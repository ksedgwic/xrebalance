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
/// All three options are dynamic (setconfig): restarting the plugin
/// to tune a knob would discard the very state being tuned -- the
/// claim table (stranding in-flight parts), the learned overrides,
/// and the coalescer cache.
const OPT_CONSTRAINT_AGE: DefaultIntegerConfigOption =
    DefaultIntegerConfigOption {
        name: "xrebalance-constraint-age",
        default: 6 * 60 * 60,
        description:
            "seconds until learned constraints in the xrebalance layer expire",
        deprecated: false,
        dynamic: true,
        multi: false,
    };

/// Learned gossip overrides (policy refreshes from onion failures,
/// node disables) expire after this many seconds.  A separate knob
/// from the constraint age: overrides assert what a peer enforces
/// rather than where liquidity sits, and the two decay on different
/// clocks.
const OPT_OVERRIDE_AGE: DefaultIntegerConfigOption =
    DefaultIntegerConfigOption {
        name: "xrebalance-override-age",
        default: 60 * 60,
        description:
            "seconds until learned policy and node-disable overrides expire",
        deprecated: false,
        dynamic: true,
        multi: false,
    };

/// Snapshot window: how long the RPC waits so fast outcomes appear
/// directly in the response.  The response is only a snapshot -- the
/// authoritative result channel is the xrebalance_part notification,
/// emitted for EVERY part's terminal state, before or after the RPC
/// returns (a background watcher follows stragglers).
const OPT_PART_WAIT: DefaultIntegerConfigOption = DefaultIntegerConfigOption {
    name: "xrebalance-part-wait",
    default: 180,
    description: "default seconds the response waits for parts (per-request \
                  part_wait overrides; results always stream via the \
                  xrebalance_part notification)",
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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct XRebalanceParams {
    /// Channels to drain (our outgoing scids).
    sources: Vec<String>,
    /// Channels to fill (our incoming scids).
    destinations: Vec<String>,
    /// Ceiling on the amount to move; partial delivery is the norm,
    /// zero delivered is a result rather than an error.
    amount_msat: u64,
    /// Strict whole-request fee budget: exactly one of these.
    #[serde(default)]
    maxfee_ppm: Option<u64>,
    #[serde(default)]
    maxfee_msat: Option<u64>,
    /// Caller correlation id, echoed in the response and in every
    /// xrebalance_part notification.
    #[serde(default)]
    label: Option<String>,
    /// Plan only: compute and return routes, execute nothing.
    #[serde(default)]
    dryrun: Option<bool>,
    #[serde(default)]
    maxparts: Option<u32>,
    /// Snapshot-window override: seconds the response waits for
    /// parts, 0 to return immediately.  Defaults to the
    /// xrebalance-part-wait option.  Results stream via the
    /// xrebalance_part notification either way.
    #[serde(default)]
    part_wait: Option<u64>,
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
        .notification(messages::NotificationTopic::new(TOPIC_PART))
        .rpcmethod(
            "xrebalance",
            "Move up to amount_msat from source channels to destination \
             channels via independent circular self-payments",
            xrebalance,
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
    let state = State {
        rpc_path: PathBuf::from(configured.configuration().rpc_file.as_str()),
        constraint_age: Arc::new(AtomicU64::new(constraint_age)),
        part_wait_secs: Arc::new(AtomicU64::new(part_wait)),
        claims: Arc::new(Mutex::new(HashMap::new())),
        coalescer: Arc::new(Mutex::new(coalesce::Coalescer::new(
            constraint_age,
        ))),
        overrides: Arc::new(Mutex::new(overrides::Overrides::new(
            override_age,
        ))),
        self_id: Arc::new(OnceLock::new()),
    };
    let plugin = configured.start(state).await?;
    log::info!(
        "xrebalance v{} started: constraint-age {}s, override-age {}s, \
         part-wait {}s",
        env!("CARGO_PKG_VERSION"),
        constraint_age,
        override_age,
        part_wait,
    );
    plugin.join().await
}

async fn xrebalance(
    _plugin: Plugin<State>,
    params: serde_json::Value,
) -> Result<serde_json::Value, Error> {
    let parsed: XRebalanceParams = serde_json::from_value(params)
        .map_err(|e| anyhow!("invalid parameters: {e} (pass parameters by keyword)"))?;

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
    if parsed.maxparts == Some(0) {
        return Err(anyhow!("maxparts must be at least 1"));
    }
    if parsed.amount_msat == 0 {
        return Err(anyhow!("amount_msat must be positive"));
    }

    let state = _plugin.state();
    let planned = plan::plan(state, &parsed).await?;
    if parsed.dryrun.unwrap_or(false) {
        return Ok(plan::dryrun_response(&parsed, &planned));
    }
    exec::execute(&_plugin, &parsed, &planned).await
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
    let secs = val
        .as_i64()
        .or_else(|| val.as_str().and_then(|s| s.parse().ok()))
        .ok_or_else(|| anyhow!("{name}: value is not an integer"))?;
    let secs = u64::try_from(secs)
        .map_err(|_| anyhow!("{name} must not be negative"))?;
    let state = plugin.state();
    match name.as_str() {
        "xrebalance-constraint-age" => {
            state.constraint_age.store(secs, Ordering::Relaxed);
            state
                .coalescer
                .lock()
                .expect("coalescer lock")
                .set_aging(secs);
        }
        "xrebalance-override-age" => {
            state
                .overrides
                .lock()
                .expect("overrides lock")
                .set_max_age(secs);
        }
        "xrebalance-part-wait" => {
            state.part_wait_secs.store(secs, Ordering::Relaxed);
        }
        _ => return Err(anyhow!("unknown dynamic option {name}")),
    }
    plugin.set_option_str(&name, options::Value::Integer(secs as i64))?;
    Ok(json!({}))
}

/// Claim arriving parts of our own self-payments: resolve with the
/// registered preimage when hash AND secret match, otherwise pass
/// the HTLC down the hook chain untouched.  A matching entry is
/// consumed -- each part is an independent payment, so its claim
/// has exactly one job, and removing it closes the replay surface
/// once the preimage becomes public along the settled path.
async fn htlc_accepted(
    plugin: Plugin<State>,
    v: serde_json::Value,
) -> Result<serde_json::Value, Error> {
    if let Some(hash) = v["htlc"]["payment_hash"].as_str() {
        let mut claims = plugin.state().claims.lock().expect("claims lock");
        let secret_matches = claims.get(hash).is_some_and(|claim| {
            v["onion"]["payment_secret"].as_str()
                == Some(claim.payment_secret.as_str())
        });
        if secret_matches {
            let claim = claims.remove(hash).expect("checked above");
            return Ok(json!({
                "result": "resolve",
                "payment_key": claim.preimage,
            }));
        }
    }
    Ok(json!({"result": "continue"}))
}
