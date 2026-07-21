//! Execution: turn planned routes into settled self-payment parts.
//!
//! The parts of one request are NOT an MPP set: each part is an
//! independent single payment with its OWN preimage, payment_hash,
//! and payment_secret, claimed on arrival by the htlc_accepted hook
//! (main.rs).  Sharing one hash across concurrently in-flight parts
//! would be a theft hazard: settling one part reveals its preimage
//! to every node on its path, and a node sitting on a second,
//! still-in-flight part's path could settle that part's incoming
//! HTLC without forwarding it.  Per-part preimages close the window
//! entirely -- a settled part's preimage is useless against the
//! others -- and intermediates cannot even correlate the parts as
//! one transfer.  MPP-set atomicity exists to protect a recipient
//! who must get all-or-nothing; we are the recipient, and partial
//! delivery is the semantic.
//!
//! The result channel is the xrebalance_part notification: one event
//! per part reaching a terminal state, ALWAYS emitted -- whether the
//! part resolved inside the RPC's snapshot window or long after (a
//! background watcher follows every part past the response).  The
//! RPC response is a snapshot: the plan, each part's payment_hash
//! (its handle), and whatever already resolved.  The caller's label
//! correlates the parts of one request; pollers can follow the
//! per-part hashes via waitsendpay/listsendpays.

use anyhow::{anyhow, Error};
use cln_plugin::Plugin;
use cln_rpc::ClnRpc;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::path::PathBuf;

use crate::plan::{PlanResult, PERSISTENT_LAYER};
use crate::{Claim, State, XRebalanceParams, TOPIC_PART};

/// waitsendpay's "Timed out" code: the HTLC is still in flight.
const WAITSENDPAY_TIMEOUT: i32 = 200;

/// BOLT 4 temporary_channel_failure (UPDATE|7): a liquidity failure,
/// the one failure class whose feedback belongs in the persistent
/// layer today.  Node-level failures and stale-gossip refreshes need
/// the side-store machinery (later phase); other codes teach us
/// nothing about capacity.
const WIRE_TEMPORARY_CHANNEL_FAILURE: u64 = 0x1007;

/// Claim-table entries older than this are pruned (a part cannot
/// outlive its HTLC by this much).
const CLAIM_MAX_AGE_SECS: u64 = 24 * 60 * 60;

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock before epoch")
        .as_secs()
}

/// Drop a part's claim once it can no longer arrive (terminal
/// failure; the HTLC has fully unwound).  Successful parts consume
/// their claim in the htlc_accepted hook instead.
fn drop_claim(state: &State, payment_hash: &str) {
    state
        .claims
        .lock()
        .expect("claims lock")
        .remove(payment_hash);
}

/// Convert one translated getroutes hop into sendpay's hop format.
/// `onion_scids` substitutes the peer-assigned alias for channels
/// whose real scid the forwarding peer refuses (unannounced
/// channels, option_scid_alias) -- bookkeeping keeps the real scid,
/// only the onion sees the alias.
fn hop_to_sendpay(
    hop: &Value,
    onion_scids: &std::collections::HashMap<String, String>,
) -> Result<Value, Error> {
    let scidd = hop["short_channel_id_dir"]
        .as_str()
        .ok_or_else(|| anyhow!("hop without short_channel_id_dir"))?;
    let (scid, dir) = scidd
        .split_once('/')
        .ok_or_else(|| anyhow!("malformed scidd {scidd}"))?;
    let onion_scid = onion_scids.get(scidd).map(String::as_str).unwrap_or(scid);
    Ok(json!({
        "id": hop["node_id_out"],
        "channel": onion_scid,
        "direction": dir.parse::<u32>()?,
        "amount_msat": hop["amount_out_msat"],
        "delay": hop["cltv_out"],
        "style": "tlv",
    }))
}

/// One hop of a part's route, kept for outcome feedback.
#[derive(Clone)]
struct PartHop {
    /// Real-channel scidd (post-translation for the final hop).
    scidd: String,
    /// The scid actually named in the onion (alias for unannounced
    /// channels) -- what erring_channel reports for this hop.
    onion_scid: String,
    /// The HTLC amount crossing this channel.
    amount_msat: u64,
    /// Our own channel (first hop out, return hop home): local
    /// truth belongs to auto.localchans, never the learned layer.
    ours: bool,
}

#[derive(Clone)]
struct Part {
    /// 1-based ordinal within this request (presentation only; at
    /// the sendpay level every part is a standalone payment).
    part_index: u64,
    /// This part's own payment hash -- its durable handle.
    payment_hash: String,
    first_hop: String,
    /// Real return channel (post-translation), never the alias.
    return_hop: String,
    /// Planned amounts: an HTLC settles at exactly its route's
    /// amounts, so these are authoritative once a part completes.
    planned_msat: u64,
    planned_sent_msat: u64,
    /// The route's hops, for writing outcome feedback.
    hops: Vec<PartHop>,
    status: &'static str,
    detail: Option<String>,
}

impl Part {
    fn delivered_msat(&self) -> u64 {
        if self.status == "complete" {
            self.planned_msat
        } else {
            0
        }
    }
    fn fee_msat(&self) -> u64 {
        if self.status == "complete" {
            self.planned_sent_msat.saturating_sub(self.planned_msat)
        } else {
            0
        }
    }
    fn json(&self) -> Value {
        json!({
            "part_index": self.part_index,
            "payment_hash": self.payment_hash,
            "status": self.status,
            "first_hop": self.first_hop,
            "return_hop": self.return_hop,
            "planned_msat": self.planned_msat,
            "delivered_msat": self.delivered_msat(),
            "sent_msat": self.planned_sent_msat,
            "fee_msat": self.fee_msat(),
            "detail": self.detail,
        })
    }
}

/// One best-effort inform-channel write into the persistent layer,
/// coalesced: a bound already accepted this bucket and not tightened
/// by this observation is dropped (coalesce.rs).
async fn inform(
    state: &State,
    rpc: &mut ClnRpc,
    scidd: &str,
    amount_msat: u64,
    kind: &str,
) {
    let key = format!("{scidd}|{kind}");
    let is_lower_bound = kind != "constrained";
    let Some(bucket) = state
        .coalescer
        .lock()
        .expect("coalescer lock")
        .check(&key, now_secs(), amount_msat, is_lower_bound)
    else {
        return;
    };
    match rpc
        .call_raw::<Value, Value>(
            "askrene-inform-channel",
            &json!({
                "layer": PERSISTENT_LAYER,
                "short_channel_id_dir": scidd,
                "amount_msat": amount_msat,
                "inform": kind,
            }),
        )
        .await
    {
        Ok(_) => state
            .coalescer
            .lock()
            .expect("coalescer lock")
            .record(&key, bucket, amount_msat),
        Err(e) => log::debug!("inform {kind} {scidd}: {e}"),
    }
}

/// Write a terminal part's outcome back to the persistent layer, so
/// the next request's solve knows what this one learned.
///
/// Success: every NETWORK hop demonstrably carried its amount --
/// inform unconstrained.  Our own channels are excluded (first hop
/// out and return hop home; auto.localchans owns local truth).
///
/// Liquidity failure (temporary_channel_failure at hop N): the
/// erring channel could not pass its amount -- inform constrained --
/// and every network hop BEFORE it demonstrably forwarded -- inform
/// unconstrained.  Other failure classes are not capacity knowledge
/// and are left for the side-store phase.
async fn apply_feedback(state: &State, part: &Part, fail_data: Option<&Value>) {
    let mut rpc = match ClnRpc::new(&state.rpc_path).await {
        Ok(rpc) => rpc,
        Err(e) => {
            log::warn!("feedback rpc connect: {e}");
            return;
        }
    };
    match fail_data {
        None => {
            for hop in part.hops.iter().filter(|h| !h.ours) {
                inform(
                    state, &mut rpc, &hop.scidd, hop.amount_msat,
                    "unconstrained",
                )
                .await;
            }
        }
        Some(data) => {
            if data["failcode"].as_u64() != Some(WIRE_TEMPORARY_CHANNEL_FAILURE)
            {
                return;
            }
            let (Some(chan), Some(dir)) = (
                data["erring_channel"].as_str(),
                data["erring_direction"].as_u64(),
            ) else {
                return;
            };
            // erring_channel reports the scid we named in the onion,
            // so match aliases too; the direction is node-id derived
            // and thus identical for alias and real scid.
            let erring_scidd = format!("{chan}/{dir}");
            let Some(erring_idx) = part.hops.iter().position(|h| {
                h.scidd == erring_scidd
                    || format!("{}/{dir}", h.onion_scid) == erring_scidd
            }) else {
                return;
            };
            for hop in part.hops[..erring_idx].iter().filter(|h| !h.ours) {
                inform(
                    state, &mut rpc, &hop.scidd, hop.amount_msat,
                    "unconstrained",
                )
                .await;
            }
            let erring = &part.hops[erring_idx];
            if !erring.ours {
                inform(
                    state, &mut rpc, &erring.scidd, erring.amount_msat,
                    "constrained",
                )
                .await;
            }
        }
    }
}

/// Broadcast one part's terminal state.  Best-effort: a failed
/// notification must not fail the part.
async fn notify_part(plugin: &Plugin<State>, label: &Option<String>, part: &Part) {
    let mut payload = part.json();
    payload["label"] = json!(label);
    if let Err(e) = plugin
        .send_custom_notification(TOPIC_PART.to_string(), payload)
        .await
    {
        log::warn!("could not send {TOPIC_PART} notification: {e}");
    }
}

/// Follow one still-pending part to its terminal state and emit its
/// notification.  Runs detached, past the RPC response.
async fn background_watch(
    plugin: Plugin<State>,
    rpc_path: PathBuf,
    label: Option<String>,
    mut part: Part,
) {
    let outcome = async {
        let mut rpc = ClnRpc::new(&rpc_path)
            .await
            .map_err(|e| anyhow!("rpc connect: {e}"))?;
        Ok::<_, Error>(
            rpc.call_raw::<Value, Value>(
                "waitsendpay",
                &json!({"payment_hash": part.payment_hash}),
            )
            .await,
        )
    }
    .await;
    let mut fail_data: Option<Value> = None;
    match outcome {
        Ok(Ok(_)) => part.status = "complete",
        Ok(Err(e)) => {
            part.status = "failed";
            part.detail = Some(match &e.data {
                Some(data) => format!("{} data={data}", e.message),
                None => e.message.clone(),
            });
            fail_data = e.data.clone();
            drop_claim(plugin.state(), &part.payment_hash);
        }
        Err(e) => {
            log::warn!(
                "xrebalance: lost watcher for part {} ({}): {e}",
                part.part_index,
                part.payment_hash
            );
            return;
        }
    }
    notify_part(&plugin, &label, &part).await;
    match (part.status, &fail_data) {
        ("complete", _) => apply_feedback(plugin.state(), &part, None).await,
        ("failed", Some(data)) => {
            apply_feedback(plugin.state(), &part, Some(data)).await
        }
        _ => {}
    }
}

pub async fn execute(
    plugin: &Plugin<State>,
    params: &XRebalanceParams,
    plan: &PlanResult,
) -> Result<Value, Error> {
    let state = plugin.state();
    let mut parts: Vec<Part> = Vec::new();

    if plan.routes.is_empty() {
        return Ok(render(params, plan, &parts));
    }

    // Prune stale claims once per request.
    {
        let mut claims = state.claims.lock().expect("claims lock");
        let cutoff = now_secs().saturating_sub(CLAIM_MAX_AGE_SECS);
        claims.retain(|_, c| c.created >= cutoff);
    }

    let label = format!(
        "xrebalance/{}",
        params.label.as_deref().unwrap_or("unlabeled")
    );

    let mut rpc = ClnRpc::new(&state.rpc_path)
        .await
        .map_err(|e| anyhow!("connecting to lightningd rpc: {e}"))?;

    for (i, route) in plan.routes.iter().enumerate() {
        let path = route["path"]
            .as_array()
            .ok_or_else(|| anyhow!("route without path"))?;
        let sp_route = path
            .iter()
            .map(|h| hop_to_sendpay(h, &plan.onion_scids))
            .collect::<Result<Vec<_>, _>>()?;
        let first = &path[0];
        let last = &path[path.len() - 1];

        // This part's own claim: fresh preimage and secret, so no
        // other part's settlement can be replayed against it.
        let preimage: [u8; 32] = rand::random();
        let payment_secret: [u8; 32] = rand::random();
        let payment_hash = hex::encode(Sha256::digest(preimage));
        state.claims.lock().expect("claims lock").insert(
            payment_hash.clone(),
            Claim {
                preimage: hex::encode(preimage),
                payment_secret: hex::encode(payment_secret),
                created: now_secs(),
            },
        );

        let hops = path
            .iter()
            .enumerate()
            .map(|(h, hop)| {
                let scidd = hop["short_channel_id_dir"]
                    .as_str()
                    .unwrap_or_default();
                let scid =
                    scidd.split_once('/').map(|(s, _)| s).unwrap_or_default();
                PartHop {
                    scidd: scidd.to_owned(),
                    onion_scid: plan
                        .onion_scids
                        .get(scidd)
                        .cloned()
                        .unwrap_or_else(|| scid.to_owned()),
                    amount_msat: hop["amount_out_msat"].as_u64().unwrap_or(0),
                    ours: h == 0 || h == path.len() - 1,
                }
            })
            .collect();
        let mut part = Part {
            part_index: (i + 1) as u64,
            payment_hash: payment_hash.clone(),
            first_hop: first["short_channel_id_dir"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
            return_hop: last["short_channel_id_dir"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
            planned_msat: last["amount_out_msat"].as_u64().unwrap_or(0),
            planned_sent_msat: first["amount_in_msat"].as_u64().unwrap_or(0),
            hops,
            status: "pending",
            detail: None,
        };
        // A standalone payment per part: no partid/groupid, and
        // amount_msat is this part's own delivered amount (required
        // for to-self payments).
        let sent = rpc
            .call_raw::<Value, Value>(
                "sendpay",
                &json!({
                    "route": sp_route,
                    "payment_hash": payment_hash,
                    "label": label,
                    "payment_secret": hex::encode(payment_secret),
                    "amount_msat": part.planned_msat,
                }),
            )
            .await;
        if let Err(e) = sent {
            part.status = "failed";
            part.detail = Some(format!("sendpay: {e}"));
            drop_claim(state, &part.payment_hash);
            notify_part(plugin, &params.label, &part).await;
        }
        parts.push(part);
    }

    // Snapshot window: wait (per-request part_wait, defaulting to
    // the xrebalance-part-wait option; 0 skips the wait) so fast
    // outcomes appear in the response; everything is ALSO notified.
    let wait_secs = params.part_wait.unwrap_or(state.part_wait_secs);
    let waits = parts
        .iter()
        .enumerate()
        .filter(|_| wait_secs > 0)
        .filter(|(_, p)| p.status == "pending")
        .map(|(idx, p)| {
            let rpc_path = state.rpc_path.clone();
            let payment_hash = p.payment_hash.clone();
            let timeout = wait_secs;
            async move {
                let outcome = async {
                    let mut rpc = ClnRpc::new(&rpc_path)
                        .await
                        .map_err(|e| anyhow!("rpc connect: {e}"))?;
                    Ok::<_, Error>(
                        rpc.call_raw::<Value, Value>(
                            "waitsendpay",
                            &json!({
                                "payment_hash": payment_hash,
                                "timeout": timeout,
                            }),
                        )
                        .await,
                    )
                }
                .await;
                (idx, outcome)
            }
        });
    for (idx, outcome) in futures::future::join_all(waits).await {
        let part = &mut parts[idx];
        let mut fail_data: Option<Value> = None;
        let terminal = match outcome {
            Ok(Ok(_)) => {
                part.status = "complete";
                true
            }
            Ok(Err(e)) if e.code == Some(WAITSENDPAY_TIMEOUT) => {
                part.status = "pending";
                false
            }
            Ok(Err(e)) => {
                part.status = "failed";
                part.detail = Some(match &e.data {
                    Some(data) => format!("{} data={data}", e.message),
                    None => e.message.clone(),
                });
                fail_data = e.data.clone();
                true
            }
            Err(e) => {
                part.status = "pending";
                part.detail = Some(format!("wait failed: {e}"));
                false
            }
        };
        if terminal {
            if part.status == "failed" {
                drop_claim(state, &part.payment_hash);
            }
            notify_part(plugin, &params.label, part).await;
            match (part.status, &fail_data) {
                ("complete", _) => apply_feedback(state, part, None).await,
                ("failed", Some(data)) => {
                    apply_feedback(state, part, Some(data)).await
                }
                _ => {}
            }
        }
    }

    // Every part still pending detaches: a background watcher
    // follows it to its terminal state and emits its notification.
    for part in parts.iter_mut().filter(|p| p.status == "pending") {
        if part.detail.is_none() {
            part.detail =
                Some("in flight; result follows via notification".into());
        }
        tokio::spawn(background_watch(
            plugin.clone(),
            state.rpc_path.clone(),
            params.label.clone(),
            part.clone(),
        ));
    }

    Ok(render(params, plan, &parts))
}

fn render(params: &XRebalanceParams, plan: &PlanResult, parts: &[Part]) -> Value {
    let delivered: u64 = parts.iter().map(Part::delivered_msat).sum();
    let fee: u64 = parts.iter().map(Part::fee_msat).sum();
    let pending: u64 = parts
        .iter()
        .filter(|p| p.status == "pending")
        .map(|p| p.planned_msat)
        .sum();
    json!({
        "status": "executed",
        "label": params.label,
        "amount_msat": params.amount_msat,
        "maxfee_msat": plan.maxfee_msat,
        "planned_msat": plan.delivered_msat,
        "planned_fee_msat": plan.fee_msat,
        "delivered_msat": delivered,
        "fee_msat": fee,
        "pending_msat": pending,
        "detail": plan.detail,
        "parts": parts.iter().map(Part::json).collect::<Vec<_>>(),
    })
}
