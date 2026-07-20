//! xrebalance: move funds between a node's own channels via a
//! circular self-payment, using askrene for route computation on
//! unmodified Core Lightning.
//!
//! This is the executor half of rebalancing, in the spirit of xpay:
//! callers say which channels to drain, which to fill, how much, and
//! at what price; xrebalance handles the how.  Strategy -- choosing
//! channels, timing, budgets -- belongs to higher-level tools
//! (CLBOSS, sling, an operator at the CLI).

use anyhow::anyhow;
use cln_plugin::options::DefaultIntegerConfigOption;
use cln_plugin::{messages, Builder, Error, Plugin};
use serde::Deserialize;
use serde_json::json;

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

#[derive(Clone, Default)]
struct State;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct XRebalanceParams {
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
    if let Some(plugin) = Builder::new(tokio::io::stdin(), tokio::io::stdout())
        .option(OPT_CONSTRAINT_AGE)
        .notification(messages::NotificationTopic::new(TOPIC_PART))
        .rpcmethod(
            "xrebalance",
            "Move up to amount_msat from source channels to destination \
             channels via a circular self-payment",
            xrebalance,
        )
        .dynamic()
        .start(State)
        .await?
    {
        plugin.join().await
    } else {
        Ok(())
    }
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

    // Scaffold: the interface is wired end-to-end; the split layer,
    // getroutes planning, and execution land next.
    Ok(json!({
        "status": "scaffold",
        "message": "interface accepted; planning and execution not yet implemented",
        "label": parsed.label,
        "sources": parsed.sources,
        "destinations": parsed.destinations,
        "amount_msat": parsed.amount_msat,
        "dryrun": parsed.dryrun.unwrap_or(false),
    }))
}
