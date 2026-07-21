//! Route planning: the client-side node split.
//!
//! askrene's getroutes rejects source == destination, so a circular
//! self-rebalance query is reshaped into a regular s -> t problem
//! inside a per-request layer this module owns:
//!
//!   - a fake "us_in" node id stands in for our own inbound side;
//!   - every destination channel's (peer -> us) direction gets a
//!     mirror (peer -> us_in) channel with a caller-allocated fake
//!     scid, the real direction's fee/cltv policy, and capacity set
//!     to the channel's actual receivable (local truth askrene's
//!     own gossip view cannot know);
//!   - every real (peer -> us) direction is disabled, so no flow
//!     can enter or transit the real us;
//!   - every (us -> peer) direction not named in sources is
//!     disabled, pinning the drain side;
//!   - getroutes runs source=us, destination=us_in.
//!
//! The final hop of each returned route crosses a mirror scid we
//! allocated, so we translate it back to the real return channel
//! before anyone else sees it.  The split layer is listed LAST in
//! the getroutes layers so its masks override auto.localchans (an
//! update-only layer entry ordered before the auto.localchans add
//! would silently suppress the channel instead).

use anyhow::{anyhow, Error};
use cln_rpc::ClnRpc;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::{State, XRebalanceParams};

/// The stand-in destination node id.  Not a valid curve point; must
/// merely be distinct from every real node id (byte 1 == 0x00 makes
/// a gossip collision effectively impossible).
const FAKE_US_IN: &str =
    "0200000000000000000000000000000000000000000000000000000000000000ff";

/// Mirror scids are allocated in this block: far above the real
/// chain tip for decades, so they cannot collide with gossip.
const MIRROR_BLOCK: u64 = 16_000_000;

/// The persistent layer holding learned constraints across requests.
pub const PERSISTENT_LAYER: &str = "xrebalance";

/// The outcome of planning: translated, sendpay-ready routes.
pub struct PlanResult {
    pub maxfee_msat: u64,
    pub delivered_msat: u64,
    pub fee_msat: u64,
    /// getroutes routes with final hops translated to real channels.
    pub routes: Vec<Value>,
    /// real scidd -> the scid to name in the onion instead.  An
    /// unannounced channel's peer rejects forwarding by real scid
    /// (option_scid_alias privacy); the alias it assigned us --
    /// listpeerchannels alias.remote, the same value route hints
    /// carry -- must go in the onion.  Responses and notifications
    /// keep the real scid; only the onion sees the alias.
    pub onion_scids: HashMap<String, String>,
    /// Set when the solve was infeasible (routes empty).
    pub detail: Option<String>,
}

/// Render a PlanResult as the dryrun response.
pub fn dryrun_response(params: &XRebalanceParams, plan: &PlanResult) -> Value {
    json!({
        "status": "planned",
        "label": params.label,
        "dryrun": true,
        "amount_msat": params.amount_msat,
        "maxfee_msat": plan.maxfee_msat,
        "delivered_msat": plan.delivered_msat,
        "fee_msat": plan.fee_msat,
        "routes": plan.routes,
        "detail": plan.detail,
    })
}

/// One usable channel from listpeerchannels.
struct Chan {
    peer_id: String,
    receivable_msat: u64,
    /// The peer's advertised policy toward us, if known.
    remote_update: Option<Value>,
    /// The alias the peer assigned for onions naming this channel
    /// (present and required for unannounced channels).
    onion_scid: Option<String>,
}

/// BOLT 7 direction: 0 if `from` is the lexicographically lesser id.
fn dir(from: &str, to: &str) -> u64 {
    if from < to {
        0
    } else {
        1
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_secs()
}

async fn call(rpc: &mut ClnRpc, method: &str, params: Value) -> Result<Value, Error> {
    rpc.call_raw::<Value, Value>(method, &params)
        .await
        .map_err(|e| anyhow!("{method}: {e}"))
}

/// Run the planning pipeline.
pub async fn plan(state: &State, params: &XRebalanceParams) -> Result<PlanResult, Error> {
    let mut rpc = ClnRpc::new(&state.rpc_path)
        .await
        .map_err(|e| anyhow!("connecting to lightningd rpc: {e}"))?;

    let info = call(&mut rpc, "getinfo", json!({})).await?;
    let self_id = info["id"]
        .as_str()
        .ok_or_else(|| anyhow!("getinfo: missing id"))?
        .to_owned();
    let _ = state.self_id.set(self_id.clone());

    let chans = usable_channels(&mut rpc).await?;
    for scid in params.sources.iter().chain(&params.destinations) {
        if !chans.contains_key(scid) {
            return Err(anyhow!(
                "unknown channel {scid}: not ours, or not in CHANNELD_NORMAL"
            ));
        }
    }
    for scid in &params.destinations {
        if chans[scid].remote_update.is_none() {
            return Err(anyhow!(
                "destination {scid}: peer's channel_update not yet seen; \
                 cannot mirror its policy"
            ));
        }
    }

    let maxfee_msat = match (params.maxfee_msat, params.maxfee_ppm) {
        (Some(msat), None) => msat,
        (None, Some(ppm)) => {
            u64::try_from(u128::from(params.amount_msat) * u128::from(ppm) / 1_000_000)
                .map_err(|_| anyhow!("maxfee_ppm overflow"))?
        }
        _ => unreachable!("validated by caller"),
    };

    ensure_persistent_layer(&mut rpc).await?;
    let cutoff =
        now_secs().saturating_sub(state.constraint_age.load(Ordering::Relaxed));
    call(
        &mut rpc,
        "askrene-age",
        json!({"layer": PERSISTENT_LAYER, "cutoff": cutoff}),
    )
    .await?;

    // Build the per-request split layer; whatever happens afterward,
    // remove it before returning.  The name must be unique per
    // request: concurrent requests sharing a scratch layer would
    // read each other's masks and race the removal.  The sequence
    // number disambiguates within one second, the pid across plugin
    // restarts, the timestamp across pid reuse.
    static REQ_SEQ: AtomicU64 = AtomicU64::new(0);
    let split = format!(
        "xrebalance-req-{:x}-{}-{}",
        now_secs(),
        std::process::id(),
        REQ_SEQ.fetch_add(1, Ordering::Relaxed),
    );
    call(&mut rpc, "askrene-create-layer", json!({"layer": split})).await?;
    let result = plan_in_layer(
        &mut rpc, state, &split, &self_id, &chans, params, maxfee_msat,
    )
    .await;
    // Best-effort cleanup; planning outcome takes precedence.
    let _ = call(&mut rpc, "askrene-remove-layer", json!({"layer": split})).await;
    result
}

/// Write the still-young learned overrides (overrides.rs) into a
/// request layer: policy refreshes as channel updates, failed
/// forwarders as node disables.  Best-effort per entry -- a channel
/// gone from gossip since we learned about it must not fail the
/// plan, it just stops benefiting from the override.
async fn apply_overrides(rpc: &mut ClnRpc, state: &State, layer: &str) {
    let (policies, nodes) = state
        .overrides
        .lock()
        .expect("overrides lock")
        .snapshot(now_secs());
    for (scidd, cu) in policies {
        if let Err(e) = call(
            rpc,
            "askrene-update-channel",
            json!({
                "layer": layer,
                "short_channel_id_dir": scidd,
                "enabled": cu.enabled,
                "htlc_minimum_msat": cu.htlc_minimum_msat,
                "htlc_maximum_msat": cu.htlc_maximum_msat,
                "fee_base_msat": cu.fee_base_msat,
                "fee_proportional_millionths": cu.fee_proportional_millionths,
                "cltv_expiry_delta": cu.cltv_expiry_delta,
            }),
        )
        .await
        {
            log::debug!("override {scidd}: {e}");
        }
    }
    for node in nodes {
        if let Err(e) = call(
            rpc,
            "askrene-disable-node",
            json!({"layer": layer, "node": node}),
        )
        .await
        {
            log::debug!("override disable {node}: {e}");
        }
    }
}

/// Channels we can use, keyed by scid: ours, CHANNELD_NORMAL.
async fn usable_channels(rpc: &mut ClnRpc) -> Result<HashMap<String, Chan>, Error> {
    let lpc = call(rpc, "listpeerchannels", json!({})).await?;
    let mut out = HashMap::new();
    for ch in lpc["channels"].as_array().into_iter().flatten() {
        if ch["state"].as_str() != Some("CHANNELD_NORMAL") {
            continue;
        }
        let (Some(scid), Some(peer_id)) =
            (ch["short_channel_id"].as_str(), ch["peer_id"].as_str())
        else {
            continue;
        };
        let private = ch["private"].as_bool().unwrap_or(false);
        out.insert(
            scid.to_owned(),
            Chan {
                peer_id: peer_id.to_owned(),
                receivable_msat: ch["receivable_msat"].as_u64().unwrap_or(0),
                remote_update: ch["updates"]["remote"].as_object().is_some().then(|| ch["updates"]["remote"].clone()),
                onion_scid: (private)
                    .then(|| ch["alias"]["remote"].as_str().map(str::to_owned))
                    .flatten(),
            },
        );
    }
    Ok(out)
}

async fn ensure_persistent_layer(rpc: &mut ClnRpc) -> Result<(), Error> {
    let layers = call(rpc, "askrene-listlayers", json!({})).await?;
    let exists = layers["layers"]
        .as_array()
        .into_iter()
        .flatten()
        .any(|l| l["layer"].as_str() == Some(PERSISTENT_LAYER));
    if !exists {
        call(
            rpc,
            "askrene-create-layer",
            json!({"layer": PERSISTENT_LAYER, "persistent": true}),
        )
        .await?;
    }
    Ok(())
}

async fn plan_in_layer(
    rpc: &mut ClnRpc,
    state: &State,
    split: &str,
    self_id: &str,
    chans: &HashMap<String, Chan>,
    params: &XRebalanceParams,
    maxfee_msat: u64,
) -> Result<PlanResult, Error> {
    apply_overrides(rpc, state, split).await;

    // Mirror each destination's (peer -> us) direction into us_in,
    // remembering fake scid/dir -> real scid/dir.
    let mut unsplit: HashMap<String, String> = HashMap::new();
    let mut onion_scids: HashMap<String, String> = HashMap::new();
    for (n, scid) in params.destinations.iter().enumerate() {
        let chan = &chans[scid];
        let update = chan.remote_update.as_ref().expect("validated");
        let real_scidd = format!("{scid}/{}", dir(&chan.peer_id, self_id));
        if let Some(alias) = &chan.onion_scid {
            onion_scids.insert(real_scidd.clone(), alias.clone());
        }
        let mirror_scid = format!("{MIRROR_BLOCK}x{}x0", n + 1);
        let mirror_scidd =
            format!("{mirror_scid}/{}", dir(&chan.peer_id, FAKE_US_IN));
        call(
            rpc,
            "askrene-create-channel",
            json!({
                "layer": split,
                "source": chan.peer_id,
                "destination": FAKE_US_IN,
                "short_channel_id": mirror_scid,
                // Local truth: what the channel can actually still
                // receive, not its nominal capacity.
                "capacity_msat": chan.receivable_msat,
            }),
        )
        .await?;
        call(
            rpc,
            "askrene-update-channel",
            json!({
                "layer": split,
                "short_channel_id_dir": mirror_scidd,
                "enabled": true,
                "htlc_minimum_msat": update["htlc_minimum_msat"],
                "htlc_maximum_msat": update["htlc_maximum_msat"],
                "fee_base_msat": update["fee_base_msat"],
                "fee_proportional_millionths": update["fee_proportional_millionths"],
                "cltv_expiry_delta": update["cltv_expiry_delta"],
            }),
        )
        .await?;
        // auto.localchans asserts local liquidity as an exact
        // constraint (min = max) on the real direction it covers;
        // carry that onto the mirror, since otherwise the solver
        // applies its uniform prior to the mirror's capacity.  Same
        // cap as localchans (bounded by the peer's htlc maximum).
        let known_msat = chan.receivable_msat.min(
            update["htlc_maximum_msat"].as_u64().unwrap_or(u64::MAX),
        );
        if known_msat > 0 {
            call(
                rpc,
                "askrene-inform-channel",
                json!({
                    "layer": split,
                    "short_channel_id_dir": mirror_scidd,
                    "amount_msat": known_msat,
                    "inform": "unconstrained",
                }),
            )
            .await?;
        }
        call(
            rpc,
            "askrene-inform-channel",
            json!({
                "layer": split,
                "short_channel_id_dir": mirror_scidd,
                "amount_msat": known_msat + 1,
                "inform": "constrained",
            }),
        )
        .await?;
        unsplit.insert(mirror_scidd, real_scidd);
    }

    // Mask: no flow may enter the real us (all inbound dirs off),
    // and the drain side is pinned to the named sources.
    for (scid, chan) in chans {
        call(
            rpc,
            "askrene-update-channel",
            json!({
                "layer": split,
                "short_channel_id_dir":
                    format!("{scid}/{}", dir(&chan.peer_id, self_id)),
                "enabled": false,
            }),
        )
        .await?;
        if !params.sources.contains(scid) {
            call(
                rpc,
                "askrene-update-channel",
                json!({
                    "layer": split,
                    "short_channel_id_dir":
                        format!("{scid}/{}", dir(self_id, &chan.peer_id)),
                    "enabled": false,
                }),
            )
            .await?;
        }
    }

    let mut getroutes = json!({
        "source": self_id,
        "destination": FAKE_US_IN,
        "amount_msat": params.amount_msat,
        // Split layer LAST: its masks must override auto.localchans.
        "layers": ["auto.localchans", PERSISTENT_LAYER, split],
        "maxfee_msat": maxfee_msat,
        "final_cltv": 14,
    });
    if let Some(maxparts) = params.maxparts {
        getroutes["maxparts"] = json!(maxparts);
    }
    let solved = match call(rpc, "getroutes", getroutes).await {
        Ok(v) => v,
        // Infeasible is a RESULT, not an error: zero moved.  (Real
        // infrastructure failures -- askrene absent, malformed
        // request -- also land here in this first cut; the detail
        // string tells the caller which it was.)
        Err(e) => {
            return Ok(PlanResult {
                maxfee_msat,
                delivered_msat: 0,
                fee_msat: 0,
                routes: vec![],
                onion_scids,
                detail: Some(e.to_string()),
            })
        }
    };

    // Translate final hops back to real channels: ours is the only
    // mapping in existence, because we allocated the mirror scids.
    let mut routes = solved["routes"].as_array().cloned().unwrap_or_default();
    let mut delivered: u64 = 0;
    let mut sent: u64 = 0;
    for route in &mut routes {
        let path = route["path"]
            .as_array_mut()
            .ok_or_else(|| anyhow!("getroutes: route without path"))?;
        let first = path.first().ok_or_else(|| anyhow!("empty path"))?;
        sent += first["amount_in_msat"].as_u64().unwrap_or(0);
        let last = path.last_mut().ok_or_else(|| anyhow!("empty path"))?;
        if last["node_id_out"].as_str() != Some(FAKE_US_IN) {
            return Err(anyhow!("getroutes: route does not end at the split node"));
        }
        let fake_scidd = last["short_channel_id_dir"]
            .as_str()
            .ok_or_else(|| anyhow!("final hop without scid"))?;
        let real_scidd = unsplit
            .get(fake_scidd)
            .ok_or_else(|| anyhow!("final hop over unknown mirror {fake_scidd}"))?;
        last["short_channel_id_dir"] = json!(real_scidd);
        last["node_id_out"] = json!(self_id);
        delivered += last["amount_out_msat"].as_u64().unwrap_or(0);
    }
    let fee = sent.saturating_sub(delivered);
    // Defensive: the budget is enforced at the quote by getroutes,
    // and re-checked here post-route.
    if fee > maxfee_msat {
        return Err(anyhow!(
            "planned fee {fee}msat exceeds budget {maxfee_msat}msat"
        ));
    }

    Ok(PlanResult {
        maxfee_msat,
        delivered_msat: delivered,
        fee_msat: fee,
        routes,
        onion_scids,
        detail: None,
    })
}
