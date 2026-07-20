# xrebalance

A Core Lightning plugin that moves funds between a node's own
channels via a circular self-payment, using
[askrene](https://docs.corelightning.org/reference/lightning-getroutes)
for route computation — on **unmodified** Core Lightning.  The
circular routing is expressed entirely through the public askrene
layer API.

**Status: pre-alpha scaffold.**  The plugin loads (dynamically) and
the RPC interface parses; planning and execution are under
construction.

## The idea

xrebalance is the *executor* half of rebalancing, in the spirit of
xpay: callers say which channels to drain, which to fill, how much,
and at what price; xrebalance handles the how.  Strategy — choosing
channels, timing, budgets — belongs to higher-level tools (CLBOSS,
sling, or an operator at the CLI).

Design points:

- **Plural sources and destinations.**  One min-cost-flow solve can
  drain several channels into several others.
- **Partial success is the semantic.**  `amount_msat` is a ceiling;
  every settled part is banked liquidity; zero delivered is a
  result, not an error.
- **Strict fees.**  The budget is enforced at the askrene quote and
  again post-route; no per-part slippage.
- **Feedback.**  Part outcomes are written back to a persistent
  askrene layer, so retries route better than first attempts.

## Interface (settling — subject to change)

    xrebalance sources=[scid,...] destinations=[scid,...]
               amount_msat=N (maxfee_ppm=N | maxfee_msat=N)
               [label=...] [dryrun=true] [maxparts=N]

One `xrebalance_part` notification is broadcast per part reaching a
terminal state, carrying the part's payment_hash/partid/groupid,
first-hop scid, real return-hop scid, delivered and fee amounts,
status, and the caller's `label` — enough for callers to keep
accurate per-channel books without polling.

Options:

    xrebalance-constraint-age=<seconds>   # expiry of learned constraints

## Build and run

    cargo build --release
    lightning-cli plugin start $PWD/target/release/xrebalance

The plugin is dynamic: it can be started, stopped, and restarted
without restarting `lightningd`.

## License

MIT
