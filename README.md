# xrebalance — the tenacious executor

A Core Lightning plugin that moves funds between a node's own channels
via independent circular self-payments, using
[askrene](https://docs.corelightning.org/reference/lightning-getroutes)
for route computation.  The circular routing is expressed entirely
through the public askrene layer API.

**Status: pre-alpha scaffold.**  The plugin loads (dynamically) and
the RPC interface parses; planning and execution are under
construction.

## The idea

xrebalance is the *executor* half of rebalancing, in the spirit of
xpay: callers say which channels to drain, which to fill, how much,
and at what price; xrebalance handles the how.  Strategy — choosing
channels, timing, budgets — belongs to higher-level tools.

Design points:

- **Plural sources and destinations.**  One min-cost-flow solve can
  drain several channels into several others.
- **Per-channel caps.**  Any source or destination can carry a limit
  on how much this request moves through it, asserted as an askrene
  constraint so the solver plans around it rather than xrebalance
  post-filtering routes.
- **Partial success is the semantic.**  `amount_msat` is a ceiling;
  every settled part is banked liquidity; zero delivered is a
  result, not an error.
- **Strict fees.**  The budget is enforced at the askrene quote and
  again post-route; no per-part slippage.
- **Feedback.**  Part outcomes are written back -- liquidity facts
  to a persistent askrene layer, policy facts (refreshes, node
  disables, channel exclusions) to a shorter-lived in-memory store
  -- so retries route better than first attempts.
- **Tenacity.**  A request may run several plan-execute rounds
  (`maxrounds`), each replanning the still-unmoved remainder
  against what the earlier rounds' failures taught, until the
  amount moves, the planner proves it cannot, or a round learns
  nothing.  Non-dryrun requests serialize: one at a time, later
  ones queue -- and the RPC blocks through the rounds, which is
  what paces a sequential driver.

## Interface (settling — subject to change)

    xrebalance sources=[src,...] destinations=[dst,...]
               amount_msat=N (maxfee_ppm=N | maxfee_msat=N)
               [label=...] [dryrun=true] [maxparts=N] [part_wait=N]
               [maxrounds=N]

Each `src`/`dst` element names one of our channels, optionally with
a cap on how much this request moves through it — at most that much
drawn from a source, at most that much delivered into a destination.
Three equivalent spellings:

    "845123x1x0"                                  no cap
    "845123x1x0:250000"                           cap in msat
    "845123x1x0:250sat"                           msat/sat suffixes
    {"scid": "845123x1x0", "max_msat": 250000}    object form

The effective per-channel bound is always the **smaller** of the cap
and the channel's live liquidity; a cap of 0 excludes the channel
from this request.  A source's cap bounds what crosses the channel —
delivered amount plus downstream fees — and the solver honors it to
its routing granularity (about 0.1% of the amount), so treat caps as
guardrails rather than exact accounting bounds.  Programs composing
JSON (CLBOSS) should prefer the object form; the string forms are
for humans at a CLI.

`amount_msat` remains the request-wide ceiling.  The planner clamps
it to what the channels can carry: the smaller of the summed source
bounds — less the fee budget, since fees ride the source channels on
top of the delivered amount — and the summed destination bounds.  A
convenient corollary: `amount_msat` larger than a source can carry
now means "drain it", rather than planning nothing.

Routing is all-or-nothing at the asked amount, so when no route
exists at the clamped amount (one thin corridor is enough, since a
clamp at the destination bound requires saturating every
destination exactly), the planner **descends a ladder** — retrying
the solve at 3/4, 1/2, 1/4, then 1/8 of the amount — and plans the
first amount the network can actually carry.  This is the plan-time
face of "partial delivery is the norm".  The amount planning
settled on (clamp, then ladder) is reported as
`effective_amount_msat`; dryruns run the identical planner, so a
dryrun's result is what execution would have pursued.

Fee budgets under the ladder: a `maxfee_ppm` budget re-derives at
each rung, so the *rate* you set holds at any size.  An absolute
`maxfee_msat` budget stands as given at every rung — at smaller
rungs it therefore permits a proportionally higher fee *rate*,
up to the full budget on an eighth of the amount.  Prefer
`maxfee_ppm` when the amount is a ceiling rather than a target;
it is the consistent form under both clamping and descent.  A
too-expensive result (getroutes 206) never descends: base fees
weigh proportionally more at smaller amounts, so cheaper routes
do not appear further down.

The parts of one request are **independent payments, not an MPP
set**: each carries its own preimage, payment_hash, and secret.
(Sharing one hash would let a node on a settled part's path steal a
still-in-flight part routed through it; per-part preimages close
that window, and intermediates cannot even correlate the parts.)

One `xrebalance_part` notification is broadcast per part reaching a
terminal state, carrying the part's own payment_hash, its
part_index, first-hop scid, real return-hop scid, delivered and fee
amounts, status, and the caller's `label` — the request-level
correlator, and enough for callers to keep accurate per-channel
books without polling.

The response is a snapshot: the plan, each part's payment_hash (its
durable handle), and whatever resolved within the snapshot window —
`part_wait` seconds (0 = return immediately), defaulting to the
`xrebalance-part-wait` option.  Parts still pending detach and keep
settling; their notifications fire when they land.

Options (all dynamic -- adjustable at runtime via `lightning-cli
setconfig`, so tuning never requires a plugin restart):

    xrebalance-constraint-age=<seconds>   # expiry of learned constraints
                                          # (10800)
    xrebalance-override-age=<seconds>     # expiry of learned overrides:
                                          # policy refreshes, node disables,
                                          # channel exclusions (3600)
    xrebalance-part-wait=<seconds>        # default snapshot window (30)
    xrebalance-min-part-msat=<msat>       # fragment floor: least msat a
                                          # part may deliver (10000)
    xrebalance-max-rounds=<n>             # plan-execute rounds per request
                                          # (50; maxrounds overrides;
                                          # ignored on dryrun)
    xrebalance-final-cltv=<blocks>        # final-hop cltv delta for return
                                          # legs (40): slack for the removal
                                          # handshake before lightningd's
                                          # fulfilled-HTLC close deadline

## Build and run

    cargo build --release
    lightning-cli plugin start $PWD/target/release/xrebalance

The plugin is dynamic: it can be started, stopped, and restarted
without restarting `lightningd`.

## Testing

Integration tests drive the real plugin binary against regtest nodes
via [pyln-testing](https://pypi.org/project/pyln-testing/); nothing
is mocked.  `lightningd` (v26.06+) and `bitcoind` must be on PATH:

    cargo build
    cd tests
    LIGHTNINGD=/path/to/lightning/lightningd/lightningd uv run pytest

## License

MIT
