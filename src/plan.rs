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
//!     own gossip view cannot know), bounded by the caller's
//!     per-destination cap, with htlc_minimum raised to the
//!     fragment floor (xrebalance-min-part-msat);
//!   - every real (peer -> us) direction is disabled, so no flow
//!     can enter or transit the real us;
//!   - every (us -> peer) direction not named in sources is
//!     disabled, pinning the drain side; a source's optional cap
//!     becomes an extra max constraint on its drain direction;
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
    /// The amount planning settled on: the request clamped to what
    /// the channels can carry under their caps -- min(amount,
    /// source bounds less the fee budget, destination bounds), each
    /// channel's bound being min(cap, live liquidity) -- and then,
    /// when no route existed at that amount, descended by the
    /// ladder to what the network could actually carry.
    pub effective_amount_msat: u64,
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
        "effective_amount_msat": plan.effective_amount_msat,
        "maxfee_msat": plan.maxfee_msat,
        "delivered_msat": plan.delivered_msat,
        "fee_msat": plan.fee_msat,
        "fee_ppm": fee_ppm(plan.fee_msat, plan.delivered_msat),
        "routes": plan.routes,
        "detail": plan.detail,
    })
}

/// One usable channel from listpeerchannels.
struct Chan {
    peer_id: String,
    spendable_msat: u64,
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

/// Effective fee rate in ppm of what was delivered; None until
/// anything is.
pub fn fee_ppm(fee_msat: u64, delivered_msat: u64) -> Option<u64> {
    if delivered_msat == 0 {
        return None;
    }
    Some((u128::from(fee_msat) * 1_000_000 / u128::from(delivered_msat)) as u64)
}

/// Whether one part's fee honors the caller's fee rate
/// (maxfee_msat / amount_msat) on the amount the part delivers.
fn part_within_rate(
    fee_msat: u64,
    delivered_msat: u64,
    maxfee_msat: u64,
    amount_msat: u64,
) -> bool {
    u128::from(fee_msat) * u128::from(amount_msat)
        <= u128::from(maxfee_msat) * u128::from(delivered_msat)
}

/// lightningd's sendpay crashes (SIGSEGV in serialize_onionpacket)
/// when a route's per-hop TLV payloads overflow the fixed 1300-byte
/// onion: create_onionpacket returns NULL and lightningd/pay.c uses
/// it unchecked.  Observed at 25 and 26 hops in production.  The
/// real limit is payload bytes, not hops; 20 leaves comfortable
/// margin.  Remove once CLN rejects the route instead of crashing.
const MAX_SAFE_HOPS: usize = 20;

/// Whether one part's route is short enough to fit the onion.
fn part_within_length(n_hops: usize) -> bool {
    n_hops <= MAX_SAFE_HOPS
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
    for spec in params.sources.iter().chain(&params.destinations) {
        let scid = &spec.scid;
        if !chans.contains_key(scid) {
            return Err(anyhow!(
                "unknown channel {scid}: not ours, or not in CHANNELD_NORMAL"
            ));
        }
    }
    for spec in &params.destinations {
        if chans[&spec.scid].remote_update.is_none() {
            return Err(anyhow!(
                "destination {}: peer's channel_update not yet seen; \
                 cannot mirror its policy",
                spec.scid,
            ));
        }
    }

    // Clamp the requested amount to what the channels can carry:
    // more than this is infeasible by local arithmetic alone, and
    // the ppm fee budget must price the movable amount, not the ask.
    let src_bound = params
        .sources
        .iter()
        .map(|s| s.capped(chans[&s.scid].spendable_msat))
        .fold(0u64, u64::saturating_add);
    let dst_bound = params
        .destinations
        .iter()
        .map(|d| d.capped(chans[&d.scid].receivable_msat))
        .fold(0u64, u64::saturating_add);
    // The source bound carries the fees too: what crosses a source
    // channel is delivered PLUS everything downstream hops charge.
    // Clamping delivery to the raw bound would go infeasible at the
    // margin, so the fee budget is set aside on the source side
    // first (worst case: every budgeted msat is spent).  The
    // destination side needs no headroom -- the last hop delivers
    // net of fees.
    let src_movable = match (params.maxfee_msat, params.maxfee_ppm) {
        (Some(msat), None) => src_bound.saturating_sub(msat),
        (None, Some(ppm)) => u64::try_from(
            u128::from(src_bound) * 1_000_000 / (1_000_000 + u128::from(ppm)),
        )
        .expect("shrunk from a u64"),
        _ => unreachable!("validated by caller"),
    };
    let amount_msat = params.amount_msat.min(src_movable).min(dst_bound);
    if amount_msat < params.amount_msat {
        log::debug!(
            "req {}: amount clamped to {}msat (requested {}, sources \
             carry {} after fee headroom, destinations carry {})",
            params.label.as_deref().unwrap_or("?"),
            crate::eng(amount_msat),
            crate::eng(params.amount_msat),
            crate::eng(src_movable),
            crate::eng(dst_bound),
        );
    }

    let maxfee_msat = match (params.maxfee_msat, params.maxfee_ppm) {
        (Some(msat), None) => msat,
        (None, Some(ppm)) => {
            u64::try_from(u128::from(amount_msat) * u128::from(ppm) / 1_000_000)
                .map_err(|_| anyhow!("maxfee_ppm overflow"))?
        }
        _ => unreachable!("validated by caller"),
    };

    if amount_msat == 0 {
        let side = if src_movable == 0 && dst_bound == 0 {
            "sources (after fee headroom) and destinations leave"
        } else if src_movable == 0 {
            "sources (after fee headroom) leave"
        } else {
            "destinations leave"
        };
        return Ok(PlanResult {
            effective_amount_msat: 0,
            maxfee_msat,
            delivered_msat: 0,
            fee_msat: 0,
            routes: vec![],
            onion_scids: HashMap::new(),
            detail: Some(format!(
                "nothing can move: the {side} nothing within their limits"
            )),
        });
    }

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
        &mut rpc, state, &split, &self_id, &chans, params, amount_msat,
        maxfee_msat,
    )
    .await;
    // Best-effort cleanup; planning outcome takes precedence.
    let _ = call(&mut rpc, "askrene-remove-layer", json!({"layer": split})).await;
    result
}

/// Write the still-young learned overrides (overrides.rs) into a
/// request layer: policy refreshes as channel updates, failed
/// forwarders as node disables, exclusions as 1msat constraints.
/// Best-effort per entry -- a channel gone from gossip since we
/// learned about it must not fail the plan, it just stops
/// benefiting from the override.
async fn apply_overrides(rpc: &mut ClnRpc, state: &State, layer: &str) {
    let snap = state
        .overrides
        .lock()
        .expect("overrides lock")
        .snapshot(now_secs());
    for (scidd, cu) in snap.policies {
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
            log::trace!("override {scidd}: {e}");
        }
    }
    for node in snap.disabled_nodes {
        if let Err(e) = call(
            rpc,
            "askrene-disable-node",
            json!({"layer": layer, "node": node}),
        )
        .await
        {
            log::trace!("override disable {node}: {e}");
        }
    }
    for scidd in snap.exclusions {
        if let Err(e) = call(
            rpc,
            "askrene-inform-channel",
            json!({
                "layer": layer,
                "short_channel_id_dir": scidd,
                "amount_msat": 1,
                "inform": "constrained",
            }),
        )
        .await
        {
            log::trace!("override exclusion {scidd}: {e}");
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
                spendable_msat: ch["spendable_msat"].as_u64().unwrap_or(0),
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

#[allow(clippy::too_many_arguments)]
async fn plan_in_layer(
    rpc: &mut ClnRpc,
    state: &State,
    split: &str,
    self_id: &str,
    chans: &HashMap<String, Chan>,
    params: &XRebalanceParams,
    amount_msat: u64,
    maxfee_msat: u64,
) -> Result<PlanResult, Error> {
    apply_overrides(rpc, state, split).await;

    // Mirror each destination's (peer -> us) direction into us_in,
    // remembering fake scid/dir -> real scid/dir.
    let mut unsplit: HashMap<String, String> = HashMap::new();
    let mut onion_scids: HashMap<String, String> = HashMap::new();
    for (n, dspec) in params.destinations.iter().enumerate() {
        let scid = &dspec.scid;
        let chan = &chans[scid];
        let update = chan.remote_update.as_ref().expect("validated");
        // Local truth bounded by the caller's cap: what the channel
        // can still receive AND this request may deliver into it.
        let receivable_msat = dspec.capped(chan.receivable_msat);
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
                "capacity_msat": receivable_msat,
            }),
        )
        .await?;
        // The fragment floor (xrebalance-min-part-msat) rides the
        // mirror's htlc_minimum_msat: every route crosses a mirror,
        // and the amount crossing it is the part's delivered
        // amount, so the solver itself refuses to plan a part
        // delivering less -- its refine stage drops sub-minimum
        // flows and re-solves the remainder over other corridors,
        // no post-pruning needed.
        let htlc_min_msat = update["htlc_minimum_msat"]
            .as_u64()
            .unwrap_or(0)
            .max(state.min_part_msat.load(Ordering::Relaxed));
        call(
            rpc,
            "askrene-update-channel",
            json!({
                "layer": split,
                "short_channel_id_dir": mirror_scidd,
                "enabled": true,
                "htlc_minimum_msat": htlc_min_msat,
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
        let known_msat = receivable_msat.min(
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
    let src_caps: HashMap<&str, Option<u64>> = params
        .sources
        .iter()
        .map(|s| (s.scid.as_str(), s.max_msat))
        .collect();
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
        let out_scidd = format!("{scid}/{}", dir(self_id, &chan.peer_id));
        match src_caps.get(scid.as_str()) {
            None => {
                call(
                    rpc,
                    "askrene-update-channel",
                    json!({
                        "layer": split,
                        "short_channel_id_dir": out_scidd,
                        "enabled": false,
                    }),
                )
                .await?;
            }
            // A binding source cap is one extra max constraint on
            // the real drain direction.  Layers intersect
            // constraints, and the solver resolves the resulting
            // min > max contradiction with auto.localchans' exact
            // spendable assertion by trusting the max ("assume min
            // is wrong", mcf.c), so the effective liquidity becomes
            // min(cap, spendable) -- reality binds when it is the
            // smaller.  A cap of 0 excludes the source outright.
            // The MCF quantizes flow into ~amount/1000 units and
            // may sit one unit past a knowledge bound, so a cap is
            // honored to routing granularity, not to the msat.
            Some(Some(cap)) if *cap < chan.spendable_msat => {
                call(
                    rpc,
                    "askrene-inform-channel",
                    json!({
                        "layer": split,
                        "short_channel_id_dir": out_scidd,
                        "amount_msat": cap + 1,
                        "inform": "constrained",
                    }),
                )
                .await?;
            }
            Some(_) => {}
        }
    }

    // The descent ladder.  getroutes is all-or-nothing at the asked
    // amount, and when the clamp binds at a bound -- especially the
    // destination side, which carries no fee slack -- the ask sits
    // exactly at the optimistic edge of what the network might
    // carry: one thin corridor fails the whole solve even though
    // most of the amount is movable.  On a no-route failure (205)
    // the solve retries at smaller amounts, probing for what IS
    // movable -- the plan-time face of "partial delivery is the
    // norm".  A ppm budget re-derives per rung (the rate is what
    // the caller fixed); an absolute msat budget stands as given.
    // 206 (a route exists but costs too much) never descends: base
    // fees weigh proportionally MORE at smaller amounts.
    const LADDER: [(u64, u64); 5] = [(1, 1), (3, 4), (1, 2), (1, 4), (1, 8)];
    let mut solved = None;
    let mut rung_amount = amount_msat;
    let mut rung_maxfee = maxfee_msat;
    let mut last_raw = String::new();
    let mut descended = false;
    let mut prev_rung = 0u64;
    for (num, den) in LADDER {
        let rung = amount_msat * num / den;
        if rung == 0 || rung == prev_rung {
            break;
        }
        prev_rung = rung;
        rung_amount = rung;
        rung_maxfee = match (params.maxfee_msat, params.maxfee_ppm) {
            (Some(msat), None) => msat,
            (None, Some(ppm)) => u64::try_from(
                u128::from(rung) * u128::from(ppm) / 1_000_000,
            )
            .expect("shrunk from a u64"),
            _ => unreachable!("validated by caller"),
        };
        let mut getroutes = json!({
            "source": self_id,
            "destination": FAKE_US_IN,
            "amount_msat": rung,
            // Split layer LAST: its masks must override auto.localchans.
            "layers": ["auto.localchans", PERSISTENT_LAYER, split],
            "maxfee_msat": rung_maxfee,
            "final_cltv": state.final_cltv.load(Ordering::Relaxed),
        });
        if let Some(maxparts) = params.maxparts {
            getroutes["maxparts"] = json!(maxparts);
        }
        match call(rpc, "getroutes", getroutes).await {
            Ok(v) => {
                solved = Some(v);
                break;
            }
            Err(e) => {
                last_raw = e.to_string();
                if !last_raw.contains("Error code 205") {
                    break;
                }
                log::debug!(
                    "req {}: no route at {} msat, descending the ladder",
                    params.label.as_deref().unwrap_or("?"),
                    crate::eng(rung),
                );
                descended = true;
            }
        }
    }
    let solved = match solved {
        Some(v) => {
            if rung_amount < amount_msat {
                log::debug!(
                    "req {}: ladder settled at {} msat (clamped ask {})",
                    params.label.as_deref().unwrap_or("?"),
                    crate::eng(rung_amount),
                    crate::eng(amount_msat),
                );
            }
            v
        }
        // Infeasible is a RESULT, not an error: zero moved.  (Real
        // infrastructure failures -- askrene absent, malformed
        // request -- also land here in this first cut; the detail
        // string tells the caller which it was.)
        None => {
            let raw = last_raw;
            // askrene's no-usable-paths diagnostic (205) names the
            // closest unusable path, which here means our own mask
            // layer and mirror scids -- internals this API never
            // otherwise exposes, and nothing a caller can act on.
            // Summarize when the message names the request layer;
            // the raw text goes to the log.
            let detail = if raw.contains(split) {
                log::trace!(
                    "req {}: getroutes: {raw}",
                    params.label.as_deref().unwrap_or("?"),
                );
                let code = raw
                    .split("Error code ")
                    .nth(1)
                    .and_then(|s| s.split(':').next())
                    .map(|c| format!(" (getroutes {c})"))
                    .unwrap_or_default();
                let nor = if descended {
                    ", nor at smaller amounts"
                } else {
                    ""
                };
                format!(
                    "no usable route from the sources to the \
                     destinations at this amount and budget{nor}{code}"
                )
            } else {
                raw
            };
            return Ok(PlanResult {
                effective_amount_msat: amount_msat,
                maxfee_msat,
                delivered_msat: 0,
                fee_msat: 0,
                routes: vec![],
                onion_scids,
                detail: Some(detail),
            })
        }
    };

    // Translate final hops back to real channels: ours is the only
    // mapping in existence, because we allocated the mirror scids.
    let solved_routes =
        solved["routes"].as_array().cloned().unwrap_or_default();
    let n_solved = solved_routes.len();
    let mut routes = Vec::with_capacity(n_solved);
    let mut delivered: u64 = 0;
    let mut sent: u64 = 0;
    let mut pruned_long: usize = 0;
    // Fee-rate-pruned parts, summarized after the loop: their ppm
    // rates (sorted for min/median/max) and foregone delivery.
    let mut pruned_rate_ppm: Vec<u64> = Vec::new();
    let mut pruned_rate_msat: u64 = 0;
    for mut route in solved_routes {
        let path = route["path"]
            .as_array_mut()
            .ok_or_else(|| anyhow!("getroutes: route without path"))?;
        if !part_within_length(path.len()) {
            log::debug!(
                "req {}: pruning part over the hop cap: {} hops > {} max \
                 (CLN sphinx crash mitigation)",
                params.label.as_deref().unwrap_or("?"),
                path.len(),
                MAX_SAFE_HOPS,
            );
            pruned_long += 1;
            continue;
        }
        let first = path.first().ok_or_else(|| anyhow!("empty path"))?;
        let route_sent = first["amount_in_msat"].as_u64().unwrap_or(0);
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
        let route_delivered = last["amount_out_msat"].as_u64().unwrap_or(0);
        let route_fee = route_sent.saturating_sub(route_delivered);
        // The quote budget is aggregate, but parts settle
        // independently: if the cheap parts fail and an expensive
        // one completes, the delivered total is priced over the
        // caller's rate.  Each part must honor the rate on its own.
        if !part_within_rate(
            route_fee,
            route_delivered,
            rung_maxfee,
            rung_amount,
        ) {
            // Per-part detail at trace; the loop exit logs one
            // summary line (an MCF solve can shed dozens of parts,
            // mostly quantization slivers priced absurdly by base
            // fees, and one line per sliver buries the log).
            log::trace!(
                "req {}: pruning part over the fee rate cap: {} msat on \
                 {} msat delivered (budget {} msat on {} msat) ({:>6} ppm)",
                params.label.as_deref().unwrap_or("?"),
                crate::eng(route_fee),
                crate::eng(route_delivered),
                crate::eng(rung_maxfee),
                crate::eng(rung_amount),
                crate::eng(fee_ppm(route_fee, route_delivered).unwrap_or(0)),
            );
            pruned_rate_ppm.push(
                fee_ppm(route_fee, route_delivered).unwrap_or(0),
            );
            pruned_rate_msat =
                pruned_rate_msat.saturating_add(route_delivered);
            continue;
        }
        sent += route_sent;
        delivered += route_delivered;
        routes.push(route);
    }
    if !pruned_rate_ppm.is_empty() {
        pruned_rate_ppm.sort_unstable();
        log::debug!(
            "req {}: pruned {} part(s) over the fee rate cap ({} msat \
             foregone): {}/{}/{} ppm min/median/max vs {} ppm budget",
            params.label.as_deref().unwrap_or("?"),
            pruned_rate_ppm.len(),
            crate::eng(pruned_rate_msat),
            crate::eng(pruned_rate_ppm[0]),
            crate::eng(pruned_rate_ppm[(pruned_rate_ppm.len() - 1) / 2]),
            crate::eng(pruned_rate_ppm[pruned_rate_ppm.len() - 1]),
            crate::eng(fee_ppm(rung_maxfee, rung_amount).unwrap_or(0)),
        );
    }
    let fee = sent.saturating_sub(delivered);
    // Defensive: the budget is enforced at the quote by getroutes,
    // per part above, and re-checked here post-route.
    if fee > rung_maxfee {
        return Err(anyhow!(
            "planned fee {fee}msat exceeds budget {rung_maxfee}msat"
        ));
    }
    let detail = if routes.is_empty() && n_solved > 0 {
        Some(if pruned_long == 0 {
            format!("all {n_solved} planned parts exceeded the fee rate cap")
        } else {
            format!(
                "all {n_solved} planned parts pruned: {pruned_long} over \
                 {MAX_SAFE_HOPS} hops, {} over the fee rate cap",
                n_solved - pruned_long
            )
        })
    } else {
        None
    };

    Ok(PlanResult {
        effective_amount_msat: rung_amount,
        maxfee_msat: rung_maxfee,
        delivered_msat: delivered,
        fee_msat: fee,
        routes,
        onion_scids,
        detail,
    })
}

#[cfg(test)]
mod tests {
    use super::{fee_ppm, part_within_length, part_within_rate};

    // 230_502msat on 50_000_000msat = 4610.04ppm, truncated.
    #[test]
    fn fee_ppm_truncates() {
        assert_eq!(fee_ppm(230_502, 50_000_000), Some(4610));
    }

    #[test]
    fn fee_ppm_none_until_delivery() {
        assert_eq!(fee_ppm(0, 0), None);
    }

    // Budget 100msat on 1_000_000msat = 100ppm; a part delivering
    // 100_000msat may charge at most 10msat.
    #[test]
    fn at_the_rate_is_within() {
        assert!(part_within_rate(10, 100_000, 100, 1_000_000));
    }

    #[test]
    fn over_the_rate_is_pruned() {
        assert!(!part_within_rate(11, 100_000, 100, 1_000_000));
    }

    #[test]
    fn zero_fee_is_within() {
        assert!(part_within_rate(0, 100_000, 0, 1_000_000));
    }

    #[test]
    fn fee_without_delivery_is_pruned() {
        assert!(!part_within_rate(1, 0, 100, 1_000_000));
    }

    #[test]
    fn at_the_hop_cap_is_within() {
        assert!(part_within_length(20));
    }

    // 25 hops crashed lightningd on 2026-07-24; anything over the
    // cap must be pruned, never sent.
    #[test]
    fn overlong_route_is_pruned() {
        assert!(!part_within_length(21));
    }

    #[test]
    fn no_overflow_at_extremes() {
        assert!(part_within_rate(
            u64::MAX,
            u64::MAX,
            u64::MAX,
            u64::MAX
        ));
        assert!(!part_within_rate(u64::MAX, 1, 1, u64::MAX));
    }
}
