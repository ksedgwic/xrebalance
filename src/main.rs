//! xrebalance: move funds between a node's own channels via a
//! circular self-payment, using askrene for route computation on
//! unmodified Core Lightning.
//!
//! This is the executor half of rebalancing, in the spirit of xpay:
//! callers say which channels to drain, which to fill, how much, and
//! at what price; xrebalance handles the how.  Strategy -- choosing
//! channels, timing, budgets -- belongs to higher-level tools
//! (CLBOSS, sling, an operator at the CLI).

mod plan;

use anyhow::anyhow;
use cln_plugin::options::DefaultIntegerConfigOption;
use cln_plugin::{messages, Builder, Error, Plugin};
use serde::Deserialize;
use std::path::PathBuf;

/// Learned constraints in the persistent xrebalance layer expire
/// after this many seconds.  Applied lazily before each request (and
/// once at init) -- askrene-age is a pure in-memory trim, so
/// per-request aging is effectively free and needs no timer.
const OPT_CONSTRAINT_AGE: DefaultIntegerConfigOption =
    DefaultIntegerConfigOption::new_i64_with_default(
        "xrebalance-constraint-age",
        7 * 24 * 60 * 60,
        "seconds until learned constraints in the xrebalance layer expire",
    );

/// Notification topic: one event per part reaching a terminal state,
/// carrying (payment_hash, partid, groupid, first-hop scid, real
/// return-hop scid, delivered_msat, fee_msat, status) plus the
/// caller's label.
const TOPIC_PART: &str = "xrebalance_part";

#[derive(Clone)]
pub struct State {
    /// Path to the lightningd RPC socket (plugins start with CWD =
    /// lightning-dir, so the relative rpc_file works as-is).
    pub rpc_path: PathBuf,
    /// Seconds until learned constraints expire.
    pub constraint_age: u64,
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
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let Some(configured) = Builder::new(tokio::io::stdin(), tokio::io::stdout())
        .option(OPT_CONSTRAINT_AGE)
        .notification(messages::NotificationTopic::new(TOPIC_PART))
        .rpcmethod(
            "xrebalance",
            "Move up to amount_msat from source channels to destination \
             channels via a circular self-payment",
            xrebalance,
        )
        .dynamic()
        .configure()
        .await?
    else {
        return Ok(());
    };
    let state = State {
        rpc_path: PathBuf::from(configured.configuration().rpc_file.as_str()),
        constraint_age: u64::try_from(configured.option(&OPT_CONSTRAINT_AGE)?)
            .map_err(|_| anyhow!("xrebalance-constraint-age must be positive"))?,
    };
    let plugin = configured.start(state).await?;
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

    if !parsed.dryrun.unwrap_or(false) {
        return Err(anyhow!(
            "execution not yet implemented; pass dryrun=true to plan routes"
        ));
    }
    plan::plan(_plugin.state(), &parsed).await
}
