//! xrebalance: move funds between a node's own channels via
//! independent circular self-payments, using askrene for route
//! computation on unmodified Core Lightning.
//!
//! This is the executor half of rebalancing, in the spirit of xpay:
//! callers say which channels to drain, which to fill, how much, and
//! at what price; xrebalance handles the how.  Strategy -- choosing
//! channels, timing, budgets -- belongs to higher-level tools.

mod coalesce;
mod exec;
mod onion_error;
mod overrides;
mod plan;
mod spec;

use anyhow::anyhow;
use cln_plugin::options::{self, DefaultIntegerConfigOption};
use cln_plugin::{messages, Builder, Error, Plugin};
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

/// Learned constraints in the persistent xrebalance layer expire
/// after this many seconds.  Applied lazily before each request (and
/// once at init) -- askrene-age is a pure in-memory trim, so
/// per-request aging is effectively free and needs no timer.
/// Liquidity knowledge decays in hours as network traffic moves
/// balances; operators on slow networks (e.g. signet) should widen
/// this to keep accumulated knowledge longer.
///
/// All the options are dynamic (setconfig): restarting the plugin
/// to tune a knob would discard the very state being tuned -- the
/// claim table (stranding in-flight parts), the learned overrides,
/// and the coalescer cache.
///
/// Constraint knowledge should expire on the timescale of its
/// re-acquisition cost, and tenacious rounds cut re-testing a
/// corridor to one failed part.  Stale pessimism (a max bound on a
/// corridor that has since refilled) silently prunes routes until
/// it expires; stale optimism is rewritten by the first failure.
/// So age toward re-learning, but give knowledge time to be used:
/// 6 hours spans several runs of a periodic driver, so each run
/// inherits the runs before instead of restarting the map — in the
/// field, 3 hours expired corridors faster than routine traffic
/// re-probed them.
const OPT_CONSTRAINT_AGE: DefaultIntegerConfigOption = DefaultIntegerConfigOption {
    name: "xrebalance-constraint-age",
    default: 6 * 60 * 60,
    description: "seconds until learned constraints in the xrebalance layer expire",
    deprecated: false,
    dynamic: true,
    multi: false,
};

/// Learned gossip overrides (policy refreshes from onion failures,
/// node disables, channel exclusions) expire after this many
/// seconds.  A separate knob from the constraint age: overrides
/// assert what a peer enforces rather than where liquidity sits,
/// and the two decay on different clocks.
const OPT_OVERRIDE_AGE: DefaultIntegerConfigOption = DefaultIntegerConfigOption {
    name: "xrebalance-override-age",
    default: 60 * 60,
    description: "seconds until learned policy, node-disable, and \
                      channel-exclusion overrides expire",
    deprecated: false,
    dynamic: true,
    multi: false,
};

/// Snapshot window: how long the RPC waits so fast outcomes appear
/// directly in the response.  The response is only a snapshot -- the
/// authoritative result channel is the xrebalance_part notification,
/// emitted for EVERY part's terminal state, before or after the RPC
/// returns (a background watcher follows stragglers).  The wait
/// recurs every round, so the default is short; stragglers detach
/// with their amounts still counted as committed.
const OPT_PART_WAIT: DefaultIntegerConfigOption = DefaultIntegerConfigOption {
    name: "xrebalance-part-wait",
    default: 30,
    description: "default seconds the response waits for parts (per-request \
                  part_wait overrides; results always stream via the \
                  xrebalance_part notification)",
    deprecated: false,
    dynamic: true,
    multi: false,
};

/// Minimum msat a planned part must deliver.  Applied as the
/// htlc_minimum_msat of every destination mirror (plan.rs), so the
/// solver itself never plans a part below it: askrene's refine
/// stage drops sub-minimum flows and re-solves the remainder over
/// other corridors.  Fragments -- fee-free quantization slivers
/// riding truncated fee arithmetic -- carry no rebalance value at
/// any price; this floor keeps them out of the plan entirely.
/// 0 disables the floor.
const OPT_MIN_PART: DefaultIntegerConfigOption = DefaultIntegerConfigOption {
    name: "xrebalance-min-part-msat",
    default: 10_000,
    description: "minimum msat a planned part must deliver (fragment \
                  floor; 0 disables)",
    deprecated: false,
    dynamic: true,
    multi: false,
};

/// How many plan-execute rounds one request may run (the
/// per-request maxrounds overrides).  Each round replans the
/// still-unmoved remainder against everything the earlier rounds'
/// failures taught (constraints, exclusions, policy refreshes), the
/// xpay ethos: keep trying until the amount moves or the request
/// can prove it cannot.  A round that delivers nothing and learns
/// nothing ends the loop early (the stall check), so the cap is a
/// backstop for feedback bugs, not the usual exit, and the default
/// is sized for that backstop role.  maxrounds=1 requests the old
/// single-shot behavior, response shape included.
const OPT_MAX_ROUNDS: DefaultIntegerConfigOption = DefaultIntegerConfigOption {
    name: "xrebalance-max-rounds",
    default: 50,
    description: "plan-execute rounds one request may run (per-request \
                      maxrounds overrides)",
    deprecated: false,
    dynamic: true,
    multi: false,
};

/// Final-hop CLTV delta granted to each part's return leg.  The
/// htlc_accepted hook claims the HTLC the moment it arrives, but the
/// removal handshake still needs the peer to respond, and lightningd
/// force-closes a channel whose fulfilled incoming HTLC is still
/// present within ~cltv-delta/2 blocks of expiry (17 at the default
/// cltv-delta of 34).  This delta minus that buffer is how many
/// blocks a slow or flapping peer has to finish the removal before
/// the deadline check fires; a value at or below the buffer makes
/// every return leg a force-close race against the next block.
const OPT_FINAL_CLTV: DefaultIntegerConfigOption = DefaultIntegerConfigOption {
    name: "xrebalance-final-cltv",
    default: 40,
    description: "final-hop cltv delta for return legs (slack for \
                      the removal handshake before lightningd's \
                      fulfilled-HTLC close deadline)",
    deprecated: false,
    dynamic: true,
    multi: false,
};

/// Notification topic: one event per part reaching a terminal state,
/// carrying the part's own payment_hash (parts are independent
/// payments, not an MPP set), part_index, first-hop scid, real
/// return-hop scid, delivered_msat, fee_msat, and status, plus the
/// caller's label -- the request-level correlator.
pub const TOPIC_PART: &str = "xrebalance_part";

/// A registered self-payment part we will claim on arrival.  One
/// entry serves exactly one HTLC (parts are independent payments
/// with their own hashes), so the entry is CONSUMED when claimed,
/// and dropped when its part terminally fails.  The 24h prune in
/// exec.rs is the backstop for parts whose watcher was lost.
pub struct Claim {
    pub preimage: String,
    pub payment_secret: String,
    /// Msat the part's final hop delivers (the route's last
    /// amount_out); an HTLC offering less does not settle.  See
    /// try_claim.
    pub amount_msat: u64,
    pub created: u64,
}

#[derive(Clone)]
pub struct State {
    /// Path to the lightningd RPC socket (plugins start with CWD =
    /// lightning-dir, so the relative rpc_file works as-is).
    pub rpc_path: PathBuf,
    /// Seconds until learned constraints expire (dynamic; read per
    /// request).
    pub constraint_age: Arc<AtomicU64>,
    /// Bound on the synchronous part wait (dynamic; read per
    /// request).
    pub part_wait_secs: Arc<AtomicU64>,
    /// Fragment floor: least msat a planned part may deliver
    /// (dynamic; read per request).
    pub min_part_msat: Arc<AtomicU64>,
    /// Round cap for the tenacious loop (dynamic; read per
    /// request).
    pub max_rounds: Arc<AtomicU64>,
    /// Final-hop cltv delta for return legs (dynamic; read per
    /// request).
    pub final_cltv: Arc<AtomicU64>,
    /// One request at a time: concurrent requests would race each
    /// other for the same liquidity and re-learn the same failures,
    /// so non-dryrun requests queue here.
    pub request_gate: Arc<tokio::sync::Mutex<()>>,
    /// Counts every feedback write (informs, exclusions, policy
    /// stores, node disables).  A round that moves nothing and
    /// leaves this unchanged would replan identically: the loop
    /// stops instead (the stall check).
    pub feedback_writes: Arc<AtomicU64>,
    /// payment_hash (hex) -> claim, consulted by htlc_accepted.
    pub claims: Arc<Mutex<HashMap<String, Claim>>>,
    /// Suppresses redundant persistent-layer informs (coalesce.rs).
    pub coalescer: Arc<Mutex<coalesce::Coalescer>>,
    /// Learned gossip overrides, applied per request (overrides.rs).
    pub overrides: Arc<Mutex<overrides::Overrides>>,
    /// Our node id, filled by the first plan; feedback needs it to
    /// refuse disabling our own node.
    pub self_id: Arc<OnceLock<String>>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct XRebalanceParams {
    /// Channels to drain (our outgoing scids), each with an optional
    /// cap on how much this request draws from it (spec.rs).
    sources: Vec<spec::ChanSpec>,
    /// Channels to fill (our incoming scids), each with an optional
    /// cap on how much this request delivers into it.
    destinations: Vec<spec::ChanSpec>,
    /// Ceiling on the amount to move; partial delivery is the norm,
    /// zero delivered is a result rather than an error.  Clamped to
    /// what the channels can carry under their caps (plan.rs).
    amount_msat: u64,
    /// Strict whole-request fee budget: exactly one of these.
    #[serde(default)]
    maxfee_ppm: Option<u64>,
    #[serde(default)]
    maxfee_msat: Option<u64>,
    /// Caller correlation id, echoed in the response and in every
    /// xrebalance_part notification, and prefixed to this request's
    /// log lines.  Defaults to a random 8-hex-char id so concurrent
    /// requests never share a log prefix.
    #[serde(default)]
    label: Option<String>,
    /// Plan only: compute and return routes, execute nothing.
    #[serde(default)]
    dryrun: Option<bool>,
    #[serde(default)]
    maxparts: Option<u32>,
    /// Plan-execute rounds this request may run; each round replans
    /// the remainder with everything earlier failures taught.
    /// Defaults to the xrebalance-max-rounds option.
    #[serde(default)]
    maxrounds: Option<u32>,
    /// Snapshot-window override: seconds the response waits for
    /// parts, 0 to return immediately.  Defaults to the
    /// xrebalance-part-wait option.  Results stream via the
    /// xrebalance_part notification either way.
    #[serde(default)]
    part_wait: Option<u64>,
    /// Include the per-round detail (each round with its parts) in
    /// the multi-round response.  Off by default: the response then
    /// carries the request totals and the summary block, and
    /// per-part detail remains available on the xrebalance_part
    /// notification stream.  maxrounds=1's single-shot response is
    /// unchanged either way.
    #[serde(default)]
    verbose: Option<bool>,
}

/// Group digits with '_' for log readability: 10005958 -> 10_005_958.
/// Log lines only; JSON values stay plain numbers.
pub fn eng(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i) % 3 == 0 {
            out.push('_');
        }
        out.push(c);
    }
    out
}

fn main() -> Result<(), Error> {
    // The framework's logger drops records below CLN_PLUGIN_LOG
    // (default info) inside the process, so our per-request detail
    // never reaches lightningd.  Default our crate to debug: one
    // summary line per part.  The per-hop chatter sits at trace,
    // off by default; set CLN_PLUGIN_LOG=info,xrebalance=trace in
    // lightningd's environment (plugins inherit it) to get it.
    // Must happen before the runtime spawns threads.
    if std::env::var_os("CLN_PLUGIN_LOG").is_none() {
        std::env::set_var("CLN_PLUGIN_LOG", "info,xrebalance=debug");
    }
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run())
}

async fn run() -> Result<(), Error> {
    let Some(configured) = Builder::new(tokio::io::stdin(), tokio::io::stdout())
        .option(OPT_CONSTRAINT_AGE)
        .option(OPT_OVERRIDE_AGE)
        .option(OPT_PART_WAIT)
        .option(OPT_MIN_PART)
        .option(OPT_MAX_ROUNDS)
        .option(OPT_FINAL_CLTV)
        .notification(messages::NotificationTopic::new(TOPIC_PART))
        .rpcmethod(
            "xrebalance",
            "Move up to amount_msat from source channels to destination \
             channels via independent circular self-payments",
            xrebalance,
        )
        .rpcmethod(
            "xrebalance-stats",
            "Report plugin state: persistent-layer contents, learned \
             overrides, claim table, coalescer cache",
            xrebalance_stats,
        )
        .setconfig_callback(setconfig)
        .hook("htlc_accepted", htlc_accepted)
        .dynamic()
        .configure()
        .await?
    else {
        return Ok(());
    };
    let constraint_age = u64::try_from(configured.option(&OPT_CONSTRAINT_AGE)?)
        .map_err(|_| anyhow!("xrebalance-constraint-age must be positive"))?;
    let override_age = u64::try_from(configured.option(&OPT_OVERRIDE_AGE)?)
        .map_err(|_| anyhow!("xrebalance-override-age must be positive"))?;
    let part_wait = u64::try_from(configured.option(&OPT_PART_WAIT)?)
        .map_err(|_| anyhow!("xrebalance-part-wait must be positive"))?;
    let min_part = u64::try_from(configured.option(&OPT_MIN_PART)?)
        .map_err(|_| anyhow!("xrebalance-min-part-msat must not be negative"))?;
    let max_rounds = u64::try_from(configured.option(&OPT_MAX_ROUNDS)?)
        .ok()
        .filter(|&r| r >= 1)
        .ok_or_else(|| anyhow!("xrebalance-max-rounds must be at least 1"))?;
    let final_cltv = u64::try_from(configured.option(&OPT_FINAL_CLTV)?)
        .ok()
        .filter(|&c| c >= 1)
        .ok_or_else(|| anyhow!("xrebalance-final-cltv must be at least 1"))?;
    let state = State {
        rpc_path: PathBuf::from(configured.configuration().rpc_file.as_str()),
        constraint_age: Arc::new(AtomicU64::new(constraint_age)),
        part_wait_secs: Arc::new(AtomicU64::new(part_wait)),
        min_part_msat: Arc::new(AtomicU64::new(min_part)),
        max_rounds: Arc::new(AtomicU64::new(max_rounds)),
        final_cltv: Arc::new(AtomicU64::new(final_cltv)),
        request_gate: Arc::new(tokio::sync::Mutex::new(())),
        feedback_writes: Arc::new(AtomicU64::new(0)),
        claims: Arc::new(Mutex::new(HashMap::new())),
        coalescer: Arc::new(Mutex::new(coalesce::Coalescer::new(constraint_age))),
        overrides: Arc::new(Mutex::new(overrides::Overrides::new(override_age))),
        self_id: Arc::new(OnceLock::new()),
    };
    let plugin = configured.start(state).await?;
    log::info!(
        "xrebalance v{} started: constraint-age {}s, override-age {}s, \
         part-wait {}s, min-part {}msat, max-rounds {}, final-cltv {}",
        env!("CARGO_PKG_VERSION"),
        eng(constraint_age),
        eng(override_age),
        eng(part_wait),
        eng(min_part),
        eng(max_rounds),
        final_cltv,
    );
    plugin.join().await
}

/// Draw each capped spec down by the flow earlier rounds committed
/// through its channel: a per-target limit is one pot across the
/// rounds, like an absolute fee budget.  Uncapped specs stay
/// uncapped.
fn draw_down(specs: &mut [spec::ChanSpec], committed: &HashMap<String, u64>) {
    for s in specs {
        if let (Some(cap), Some(used)) = (s.max_msat, committed.get(&s.scid)) {
            s.max_msat = Some(cap.saturating_sub(*used));
        }
    }
}

async fn xrebalance(
    _plugin: Plugin<State>,
    params: serde_json::Value,
) -> Result<serde_json::Value, Error> {
    let started = std::time::Instant::now();
    let mut parsed: XRebalanceParams = serde_json::from_value(params)
        .map_err(|e| anyhow!("invalid parameters: {e} (pass parameters by keyword)"))?;
    if parsed.label.is_none() {
        parsed.label = Some(format!("{:08x}", rand::random::<u32>()));
    }

    if parsed.maxfee_ppm.is_none() == parsed.maxfee_msat.is_none() {
        return Err(anyhow!(
            "exactly one of maxfee_ppm or maxfee_msat is required"
        ));
    }
    if parsed.sources.is_empty() || parsed.destinations.is_empty() {
        return Err(anyhow!(
            "sources and destinations must each name at least one channel"
        ));
    }
    if let Some(scid) = spec::find_duplicate(&parsed.sources) {
        return Err(anyhow!("duplicate source {scid}"));
    }
    if let Some(scid) = spec::find_duplicate(&parsed.destinations) {
        return Err(anyhow!("duplicate destination {scid}"));
    }
    if parsed.maxparts == Some(0) {
        return Err(anyhow!("maxparts must be at least 1"));
    }
    if parsed.maxrounds == Some(0) {
        return Err(anyhow!("maxrounds must be at least 1"));
    }
    if parsed.amount_msat == 0 {
        return Err(anyhow!("amount_msat must be positive"));
    }

    // Log the budget in both forms, whichever one was given
    // (amount_msat is validated positive above).
    let budget_msat = parsed.maxfee_msat.unwrap_or_else(|| {
        u64::try_from(
            u128::from(parsed.amount_msat) * u128::from(parsed.maxfee_ppm.unwrap_or(0)) / 1_000_000,
        )
        .unwrap_or(u64::MAX)
    });
    let budget_ppm = u128::from(budget_msat) * 1_000_000 / u128::from(parsed.amount_msat);
    log::debug!(
        "req {}: move up to {}msat, {} sources -> {} destinations, \
         budget {}msat ({}ppm){}",
        parsed.label.as_deref().unwrap_or("?"),
        eng(parsed.amount_msat),
        parsed.sources.len(),
        parsed.destinations.len(),
        eng(budget_msat),
        eng(u64::try_from(budget_ppm).unwrap_or(u64::MAX)),
        if parsed.dryrun.unwrap_or(false) {
            " (dryrun)"
        } else {
            ""
        },
    );
    let state = _plugin.state();
    if parsed.dryrun.unwrap_or(false) {
        let planned = plan::plan(state, &parsed).await?;
        if planned.routes.is_empty() {
            log::debug!(
                "req {}: no parts: {}",
                parsed.label.as_deref().unwrap_or("?"),
                planned
                    .detail
                    .as_deref()
                    .unwrap_or("planner returned no routes"),
            );
        }
        return Ok(plan::dryrun_response(&parsed, &planned));
    }

    // The tenacious loop: replan the still-unmoved remainder each
    // round, against everything the earlier rounds' failures taught.
    // With maxrounds 1 this is exactly the old single-shot
    // request, response shape included.
    let rounds_max = parsed
        .maxrounds
        .map(u64::from)
        .unwrap_or_else(|| state.max_rounds.load(Ordering::Relaxed))
        .max(1);
    // One request at a time (see State.request_gate); the RPC
    // blocking through the rounds is what paces a sequential driver.
    let _gate = state.request_gate.lock().await;
    let req = parsed.label.clone().unwrap_or_else(|| "?".into());
    let mut rounds: Vec<serde_json::Value> = Vec::new();
    let mut delivered_total: u64 = 0;
    let mut fee_total: u64 = 0;
    let mut pending_total: u64 = 0;
    let mut pending_fee_total: u64 = 0;
    // Committed flow by target channel so far -- sent (sources,
    // fees included) and delivered (destinations) -- drawn down
    // from the per-target limits each round, so a limit bounds the
    // whole request, not each round.
    let mut source_committed: HashMap<String, u64> = HashMap::new();
    let mut dest_committed: HashMap<String, u64> = HashMap::new();
    let mut round: u64 = 0;
    let stop_reason: String;
    loop {
        round += 1;
        // Amounts still in flight count as committed: if a straggler
        // later fails, this request under-delivers rather than
        // double-spending its liquidity on a retry.
        let committed = delivered_total.saturating_add(pending_total);
        let remaining = parsed.amount_msat.saturating_sub(committed);
        if round > 1 {
            let floor = state.min_part_msat.load(Ordering::Relaxed).max(1);
            if remaining < floor {
                stop_reason = if remaining == 0 {
                    "amount fully committed".into()
                } else {
                    format!("remaining {}msat below the fragment floor", eng(remaining))
                };
                break;
            }
        }
        let mut round_params = parsed.clone();
        round_params.amount_msat = remaining;
        if let Some(pot) = parsed.maxfee_msat {
            // An absolute budget is one pot across the rounds:
            // settled and reserved (pending-part) fees come off it.
            round_params.maxfee_msat =
                Some(pot.saturating_sub(fee_total.saturating_add(pending_fee_total)));
        }
        // Per-target limits are one pot across the rounds too.
        draw_down(&mut round_params.sources, &source_committed);
        draw_down(&mut round_params.destinations, &dest_committed);
        let fb_before = state.feedback_writes.load(Ordering::Relaxed);
        let planned = match plan::plan(state, &round_params).await {
            Ok(p) => p,
            // A later round must not turn work already done into an
            // RPC error; report what happened instead.
            Err(e) if round > 1 => {
                stop_reason = format!("round {round} planning failed: {e}");
                break;
            }
            Err(e) => return Err(e),
        };
        let no_routes = planned.routes.is_empty();
        if no_routes {
            log::debug!(
                "req {req}: no parts: {}",
                planned
                    .detail
                    .as_deref()
                    .unwrap_or("planner returned no routes"),
            );
        }
        let outcome = match exec::execute(&_plugin, &round_params, &planned, started).await {
            Ok(o) => o,
            Err(e) if round > 1 => {
                stop_reason = format!("round {round} execution failed: {e}");
                break;
            }
            Err(e) => return Err(e),
        };
        delivered_total += outcome.delivered_msat;
        fee_total += outcome.fee_msat;
        pending_total += outcome.pending_msat;
        pending_fee_total += outcome.pending_fee_msat;
        for (scid, msat) in &outcome.source_committed_msat {
            *source_committed.entry(scid.clone()).or_default() += *msat;
        }
        for (scid, msat) in &outcome.dest_committed_msat {
            *dest_committed.entry(scid.clone()).or_default() += *msat;
        }
        rounds.push(outcome.response);
        if rounds_max > 1 {
            log::debug!(
                "req {req}: round {round}/{rounds_max}: delivered {} msat \
                 this round ({} total), {} msat pending, {} msat remaining",
                eng(outcome.delivered_msat),
                eng(delivered_total),
                eng(pending_total),
                eng(parsed
                    .amount_msat
                    .saturating_sub(delivered_total.saturating_add(pending_total))),
            );
        }
        if no_routes {
            stop_reason = format!(
                "no further routes: {}",
                planned.detail.as_deref().unwrap_or("planner returned none")
            );
            break;
        }
        if round >= rounds_max {
            stop_reason = "round limit".into();
            break;
        }
        if outcome.delivered_msat == 0
            && outcome.pending_msat == 0
            && state.feedback_writes.load(Ordering::Relaxed) == fb_before
        {
            // Nothing moved and nothing was learned: the next solve
            // would be identical.
            stop_reason = "stalled: a round moved nothing and learned \
                           nothing"
                .into();
            break;
        }
    }
    if rounds_max == 1 {
        // Single-shot compatibility: the old response, exactly.
        return Ok(rounds.pop().expect("round 1 always executes"));
    }
    // rounds.len(), not the loop counter: the counter has already
    // advanced for the round a top-of-loop exit never ran.
    log::debug!(
        "req {req}: finished after {} round(s): {stop_reason}",
        rounds.len()
    );
    let summary = summarize_rounds(&rounds, delivered_total, fee_total, pending_total);
    let mut response = serde_json::json!({
        "status": "executed",
        "label": parsed.label,
        "amount_msat": parsed.amount_msat,
        "rounds_run": rounds.len(),
        "stop_reason": stop_reason,
        "delivered_msat": delivered_total,
        "fee_msat": fee_total,
        "fee_ppm": plan::fee_ppm(fee_total, delivered_total),
        "pending_msat": pending_total,
        "summary": summary,
    });
    if parsed.verbose.unwrap_or(false) {
        response["rounds"] = serde_json::Value::Array(rounds);
    }
    Ok(response)
}

/// Digest a multi-round request into the few numbers a caller scans
/// for first.  The field is named "summary" because the response
/// object's keys serialize in sorted order, which lands it after
/// the bulky "rounds" array: the summary is what remains on screen
/// when the details scroll by.
fn summarize_rounds(
    rounds: &[serde_json::Value],
    delivered_msat: u64,
    fee_msat: u64,
    pending_msat: u64,
) -> serde_json::Value {
    let parts: Vec<&serde_json::Value> = rounds
        .iter()
        .filter_map(|r| r["parts"].as_array())
        .flatten()
        .collect();
    let count = |s: &str| parts.iter().filter(|p| p["status"] == s).count();
    let complete = count("complete");
    let pending = count("pending");
    let mut summary = json!({
        "rounds": rounds.len(),
        "parts": parts.len(),
        "parts_complete": complete,
        "parts_failed": count("failed"),
        "parts_pending": pending,
        "delivered_msat": delivered_msat,
        "fee_msat": fee_msat,
        "fee_ppm": plan::fee_ppm(fee_msat, delivered_msat),
        "pending_msat": pending_msat,
    });
    if complete == 0 && pending == 0 {
        // Nothing moved and nothing is in flight: report the part
        // that came nearest to landing, so the wall is visible
        // without reading the rounds.  Nearest = fewest hops left
        // uncrossed; ties go to the largest amount.
        let closest = parts
            .iter()
            .filter(|p| p["status"] == "failed")
            .filter_map(|p| p["hops_short"].as_u64().map(|h| (h, p)))
            .min_by_key(|(h, p)| (*h, u64::MAX - p["planned_msat"].as_u64().unwrap_or(0)));
        if let Some((hops_short, part)) = closest {
            summary["closest_miss"] = json!({
                "hops_short": hops_short,
                "planned_msat": part["planned_msat"],
                "erring_scidd": part["erring_scidd"],
                "failcode": part["failcode"],
                "failcode_name": part["failcode"]
                    .as_u64()
                    .map(onion_error::failcode_name),
            });
        }
    }
    summary
}

/// Report the plugin's state in one place: what the persistent
/// layer holds (constraints should be the ONLY population; nonzero
/// channel_updates / disabled_nodes / created_channels there would
/// mean something is leaking past the per-request split layers),
/// plus the in-memory stores.  Read-only; the shape is meant to
/// grow fields, not change them.
async fn xrebalance_stats(
    plugin: Plugin<State>,
    _params: serde_json::Value,
) -> Result<serde_json::Value, Error> {
    let state = plugin.state();
    let claims = state.claims.lock().expect("claims lock").len();
    let coalescer = state.coalescer.lock().expect("coalescer lock").len();
    let (policies, disabled_nodes, exclusions) =
        state.overrides.lock().expect("overrides lock").counts();

    let mut rpc = cln_rpc::ClnRpc::new(&state.rpc_path)
        .await
        .map_err(|e| anyhow!("connecting to lightningd rpc: {e}"))?;
    let listed = rpc
        .call_raw::<serde_json::Value, serde_json::Value>(
            "askrene-listlayers",
            &json!({"layer": plan::PERSISTENT_LAYER}),
        )
        .await
        .map_err(|e| anyhow!("askrene-listlayers: {e}"))?;
    let layer = match listed["layers"].as_array().and_then(|l| l.first()) {
        None => json!({"exists": false}),
        Some(l) => {
            let constraints = l["constraints"].as_array().cloned().unwrap_or_default();
            let minimums = constraints
                .iter()
                .filter(|c| c["minimum_msat"].is_u64())
                .count();
            // Breadth: distinct directions, split by bound kind
            // (a direction with a max-bound is one askrene can
            // short-circuit at plan time).  Depth: entries stacked
            // per direction.
            let mut depth: HashMap<&str, u64> = HashMap::new();
            let mut dirs_with_max: std::collections::HashSet<&str> = Default::default();
            let mut dirs_with_min: std::collections::HashSet<&str> = Default::default();
            for c in &constraints {
                let Some(d) = c["short_channel_id_dir"].as_str() else {
                    continue;
                };
                *depth.entry(d).or_insert(0) += 1;
                if c["maximum_msat"].is_u64() {
                    dirs_with_max.insert(d);
                }
                if c["minimum_msat"].is_u64() {
                    dirs_with_min.insert(d);
                }
            }
            let mut depths: Vec<u64> = depth.values().copied().collect();
            depths.sort_unstable();
            // Nearest-rank percentile, as the contrib layer-summary
            // script computes it.
            let pct = |q: f64| -> u64 {
                match depths.len() {
                    0 => 0,
                    n => depths[(q * (n - 1) as f64).round() as usize],
                }
            };
            let timestamps: Vec<u64> = constraints
                .iter()
                .filter_map(|c| c["timestamp"].as_u64())
                .collect();
            let count = |key: &str| l[key].as_array().map(|a| a.len()).unwrap_or(0);
            json!({
                "exists": true,
                "constraints": constraints.len(),
                "constraint_minimums": minimums,
                "constraint_maximums": constraints.len() - minimums,
                "constraint_scidds": depth.len(),
                "dirs_with_max": dirs_with_max.len(),
                "dirs_with_min": dirs_with_min.len(),
                "depth_min": depths.first().copied().unwrap_or(0),
                "depth_median": pct(0.5),
                "depth_p90": pct(0.9),
                "depth_max": depths.last().copied().unwrap_or(0),
                "oldest_constraint": timestamps.iter().min(),
                "newest_constraint": timestamps.iter().max(),
                "channel_updates": count("channel_updates"),
                "disabled_nodes": count("disabled_nodes"),
                "created_channels": count("created_channels"),
                "biases": count("biases"),
            })
        }
    };

    Ok(json!({
        "version": env!("CARGO_PKG_VERSION"),
        "options": {
            "constraint_age": state.constraint_age.load(Ordering::Relaxed),
            "override_age": state
                .overrides
                .lock()
                .expect("overrides lock")
                .max_age(),
            "part_wait": state.part_wait_secs.load(Ordering::Relaxed),
            "min_part_msat": state.min_part_msat.load(Ordering::Relaxed),
            "max_rounds": state.max_rounds.load(Ordering::Relaxed),
            "final_cltv": state.final_cltv.load(Ordering::Relaxed),
        },
        "claims": claims,
        "coalescer_entries": coalescer,
        "overrides": {
            "channel_updates": policies,
            "disabled_nodes": disabled_nodes,
            "exclusions": exclusions,
        },
        "layer": layer,
    }))
}

/// Apply a setconfig change to one of the dynamic options.  Values
/// take effect from the next request or layer write; an error here
/// vetoes the change and lightningd keeps the old value.  The
/// framework's own option map is updated too, so listconfigs stays
/// truthful.
async fn setconfig(
    plugin: Plugin<State>,
    v: serde_json::Value,
) -> Result<serde_json::Value, Error> {
    let name = v["config"]
        .as_str()
        .ok_or_else(|| anyhow!("setconfig: missing config name"))?
        .to_owned();
    let val = &v["val"];
    let value = val
        .as_i64()
        .or_else(|| val.as_str().and_then(|s| s.parse().ok()))
        .ok_or_else(|| anyhow!("{name}: value is not an integer"))?;
    let value = u64::try_from(value).map_err(|_| anyhow!("{name} must not be negative"))?;
    let state = plugin.state();
    match name.as_str() {
        "xrebalance-constraint-age" => {
            state.constraint_age.store(value, Ordering::Relaxed);
            state
                .coalescer
                .lock()
                .expect("coalescer lock")
                .set_aging(value);
        }
        "xrebalance-override-age" => {
            state
                .overrides
                .lock()
                .expect("overrides lock")
                .set_max_age(value);
        }
        "xrebalance-part-wait" => {
            state.part_wait_secs.store(value, Ordering::Relaxed);
        }
        "xrebalance-min-part-msat" => {
            state.min_part_msat.store(value, Ordering::Relaxed);
        }
        "xrebalance-max-rounds" => {
            if value < 1 {
                return Err(anyhow!("{name} must be at least 1"));
            }
            state.max_rounds.store(value, Ordering::Relaxed);
        }
        "xrebalance-final-cltv" => {
            if value < 1 {
                return Err(anyhow!("{name} must be at least 1"));
            }
            state.final_cltv.store(value, Ordering::Relaxed);
        }
        _ => return Err(anyhow!("unknown dynamic option {name}")),
    }
    plugin.set_option_str(&name, options::Value::Integer(value as i64))?;
    Ok(json!({}))
}

/// Outcome of matching one arriving HTLC against the claim table.
enum ClaimVerdict {
    /// Ours and whole: the entry was consumed -- each part is an
    /// independent payment, so its claim has exactly one job, and
    /// removing it closes the replay surface once the preimage
    /// becomes public along the settled path.
    Resolve { preimage: String },
    /// Ours, but the HTLC offers less than the part delivers.  The
    /// entry is kept (a whole retry can still settle); the HTLC is
    /// left to lightningd, which fails it -- no invoice exists for
    /// the hash.
    Underpaid {
        expected_msat: u64,
        incoming_msat: u64,
    },
    /// Not ours: pass it down the hook chain untouched.
    Pass,
}

/// Match an arriving HTLC against the claim table: hash AND secret
/// AND amount.  Resolving from the hook bypasses lightningd's own
/// final-hop amount check (final_incorrect_htlc_amount), and the
/// last-hop peer relays our onion intact -- secret included -- while
/// choosing the HTLC amount itself.  Settling on hash and secret
/// alone would let that peer offer less than the part delivers,
/// learn the preimage anyway, and claim the full amount from its
/// upstream, keeping the difference.  Overpayment settles: the
/// sender loses nothing by it.
fn try_claim(claims: &mut HashMap<String, Claim>, v: &serde_json::Value) -> ClaimVerdict {
    let Some(hash) = v["htlc"]["payment_hash"].as_str() else {
        return ClaimVerdict::Pass;
    };
    let Some(claim) = claims.get(hash) else {
        return ClaimVerdict::Pass;
    };
    if v["onion"]["payment_secret"].as_str() != Some(claim.payment_secret.as_str()) {
        return ClaimVerdict::Pass;
    }
    // A missing or malformed amount reads as zero: never whole.
    let incoming_msat = v["htlc"]["amount_msat"].as_u64().unwrap_or(0);
    if incoming_msat < claim.amount_msat {
        return ClaimVerdict::Underpaid {
            expected_msat: claim.amount_msat,
            incoming_msat,
        };
    }
    let claim = claims.remove(hash).expect("present above");
    ClaimVerdict::Resolve {
        preimage: claim.preimage,
    }
}

/// Claim arriving parts of our own self-payments with the registered
/// preimage; the decision is try_claim's.
async fn htlc_accepted(
    plugin: Plugin<State>,
    v: serde_json::Value,
) -> Result<serde_json::Value, Error> {
    let verdict = {
        let mut claims = plugin.state().claims.lock().expect("claims lock");
        try_claim(&mut claims, &v)
    };
    match verdict {
        ClaimVerdict::Resolve { preimage } => Ok(json!({
            "result": "resolve",
            "payment_key": preimage,
        })),
        ClaimVerdict::Underpaid {
            expected_msat,
            incoming_msat,
        } => {
            log::warn!(
                "not resolving {}: htlc offers {} msat, part delivers {}",
                v["htlc"]["payment_hash"].as_str().unwrap_or("?"),
                eng(incoming_msat),
                eng(expected_msat),
            );
            Ok(json!({"result": "continue"}))
        }
        ClaimVerdict::Pass => Ok(json!({"result": "continue"})),
    }
}

#[cfg(test)]
mod tests {
    use super::{draw_down, eng, spec::ChanSpec, summarize_rounds, try_claim, Claim, ClaimVerdict};
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn draw_down_shrinks_only_capped_specs() {
        let spec = |scid: &str, max_msat| ChanSpec {
            scid: scid.to_owned(),
            max_msat,
        };
        let mut specs = vec![
            spec("100x1x0", Some(1_000)),
            spec("200x1x0", None),
            spec("300x1x0", Some(500)),
            spec("400x1x0", Some(700)),
        ];
        let committed = HashMap::from([
            ("100x1x0".to_owned(), 400),
            ("200x1x0".to_owned(), 123),
            ("300x1x0".to_owned(), 800),
        ]);
        draw_down(&mut specs, &committed);
        assert_eq!(specs[0].max_msat, Some(600));
        // Uncapped stays uncapped, whatever flowed through it.
        assert_eq!(specs[1].max_msat, None);
        // Overshoot (routing granularity) saturates to exhausted.
        assert_eq!(specs[2].max_msat, Some(0));
        // Untouched targets keep their full cap.
        assert_eq!(specs[3].max_msat, Some(700));
    }

    #[test]
    fn summarize_counts_across_rounds() {
        let rounds = vec![
            json!({"parts": [
                {"status": "failed", "planned_msat": 100, "hops_short": 2},
                {"status": "complete", "planned_msat": 200},
            ]}),
            json!({"parts": [
                {"status": "pending", "planned_msat": 300},
            ]}),
            // A no-route round carries an empty parts array.
            json!({"parts": []}),
        ];
        let s = summarize_rounds(&rounds, 200, 3, 300);
        assert_eq!(s["rounds"], 3);
        assert_eq!(s["parts"], 3);
        assert_eq!(s["parts_complete"], 1);
        assert_eq!(s["parts_failed"], 1);
        assert_eq!(s["parts_pending"], 1);
        assert_eq!(s["delivered_msat"], 200);
        assert_eq!(s["fee_msat"], 3);
        assert_eq!(s["pending_msat"], 300);
        // Something landed: no miss to report.
        assert!(s.get("closest_miss").is_none());
    }

    #[test]
    fn summarize_closest_miss_fewest_hops_then_largest() {
        let rounds = vec![json!({"parts": [
            {"status": "failed", "planned_msat": 500_000, "hops_short": 3,
             "erring_scidd": "1x1x1/0", "failcode": 4103},
            {"status": "failed", "planned_msat": 100_000, "hops_short": 2,
             "erring_scidd": "2x2x2/1", "failcode": 4103},
            {"status": "failed", "planned_msat": 250_000, "hops_short": 2,
             "erring_scidd": "3x3x3/0", "failcode": 16394},
            // No failure geometry recorded: never the closest miss.
            {"status": "failed", "planned_msat": 900_000},
        ]})];
        let s = summarize_rounds(&rounds, 0, 0, 0);
        let miss = &s["closest_miss"];
        assert_eq!(miss["hops_short"], 2);
        assert_eq!(miss["planned_msat"], 250_000);
        assert_eq!(miss["erring_scidd"], "3x3x3/0");
        assert_eq!(miss["failcode"], 16394); // 0x400a
        assert_eq!(miss["failcode_name"], "UNKNOWN_NEXT_PEER");
    }

    #[test]
    fn summarize_no_miss_while_parts_pend() {
        let rounds = vec![json!({"parts": [
            {"status": "failed", "planned_msat": 100, "hops_short": 1},
            {"status": "pending", "planned_msat": 200},
        ]})];
        let s = summarize_rounds(&rounds, 0, 0, 200);
        assert!(s.get("closest_miss").is_none());
    }

    #[test]
    fn eng_groups_digits() {
        assert_eq!(eng(0), "0");
        assert_eq!(eng(999), "999");
        assert_eq!(eng(1000), "1_000");
        assert_eq!(eng(10005958), "10_005_958");
        assert_eq!(eng(u64::MAX), "18_446_744_073_709_551_615");
    }

    fn hash() -> String {
        "aa".repeat(32)
    }

    fn secret() -> String {
        "22".repeat(32)
    }

    fn one_claim(amount_msat: u64) -> HashMap<String, Claim> {
        let mut claims = HashMap::new();
        claims.insert(
            hash(),
            Claim {
                preimage: "11".repeat(32),
                payment_secret: secret(),
                amount_msat,
                created: 0,
            },
        );
        claims
    }

    fn htlc(hash: &str, secret: &str, amount_msat: u64) -> serde_json::Value {
        json!({
            "htlc": {"payment_hash": hash, "amount_msat": amount_msat},
            "onion": {"payment_secret": secret},
        })
    }

    #[test]
    fn whole_htlc_resolves_and_consumes_the_claim() {
        let mut claims = one_claim(1000);
        let v = htlc(&hash(), &secret(), 1000);
        match try_claim(&mut claims, &v) {
            ClaimVerdict::Resolve { preimage } => {
                assert_eq!(preimage, "11".repeat(32))
            }
            _ => panic!("expected resolve"),
        }
        assert!(claims.is_empty());
        assert!(matches!(try_claim(&mut claims, &v), ClaimVerdict::Pass));
    }

    #[test]
    fn overpaying_htlc_resolves() {
        let mut claims = one_claim(1000);
        let v = htlc(&hash(), &secret(), 1001);
        assert!(matches!(
            try_claim(&mut claims, &v),
            ClaimVerdict::Resolve { .. }
        ));
    }

    #[test]
    fn short_htlc_is_refused_and_the_claim_kept() {
        let mut claims = one_claim(1000);
        let v = htlc(&hash(), &secret(), 999);
        assert!(matches!(
            try_claim(&mut claims, &v),
            ClaimVerdict::Underpaid {
                expected_msat: 1000,
                incoming_msat: 999,
            }
        ));
        let v = htlc(&hash(), &secret(), 1000);
        assert!(matches!(
            try_claim(&mut claims, &v),
            ClaimVerdict::Resolve { .. }
        ));
    }

    #[test]
    fn missing_amount_reads_as_zero() {
        let mut claims = one_claim(1000);
        let v = json!({
            "htlc": {"payment_hash": hash()},
            "onion": {"payment_secret": secret()},
        });
        assert!(matches!(
            try_claim(&mut claims, &v),
            ClaimVerdict::Underpaid {
                incoming_msat: 0,
                ..
            }
        ));
        assert_eq!(claims.len(), 1);
    }

    #[test]
    fn wrong_secret_passes_and_the_claim_kept() {
        let mut claims = one_claim(1000);
        let v = htlc(&hash(), &"33".repeat(32), 1000);
        assert!(matches!(try_claim(&mut claims, &v), ClaimVerdict::Pass));
        assert_eq!(claims.len(), 1);
    }

    #[test]
    fn unknown_hash_passes() {
        let mut claims = one_claim(1000);
        let v = htlc(&"bb".repeat(32), &secret(), 1000);
        assert!(matches!(try_claim(&mut claims, &v), ClaimVerdict::Pass));
    }
}
