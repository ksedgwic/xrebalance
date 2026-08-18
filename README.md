# xrebalance — askrene adapted for circular routing

A Core Lightning plugin that moves funds between a node's own channels
via independent circular self-payments, using
[askrene](https://docs.corelightning.org/reference/lightning-getroutes)
for route computation.  askrene only solves point-to-point, so
xrebalance adapts it by *node splitting*: each request builds a
layer that splits the local node into its two roles — the real node
originates the route, and a stand-in for its inbound side
terminates it, reachable only through copies of the chosen
destination channels.  Any route from the one to the other is, on
the wire, a circle back to the local node.  The adaptation is
expressed entirely through the public askrene layer API — no
askrene changes.  Runs on stock `lightningd`, v26.04 or later.

## Quick start

Fill a depleted channel from a few well-stocked ones, spending at
most 300 ppm in fees:

    lightning-cli -k xrebalance \
        sources='["803480x1657x1", "805961x1795x1", "806464x855x0"]' \
        destinations='["848864x3313x1"]' \
        amount_msat=1000000000 \
        maxfee_ppm=300

One solve decides how much to draw through each source; whatever
the network can actually carry moves, and the response reports what
was delivered, what it cost, and each part's fate.  Add
`dryrun=true` to see the identical plan without moving funds.

To build and load the plugin, see [Build and run](#build-and-run).

## Why xrebalance

xrebalance is a low-level rebalance executor, in the spirit of
xpay: it is meant to be driven by a high-level rebalancer that owns
the *strategy* — which channels to drain, which to fill, how much,
at what price, when — while xrebalance owns the *tactics*: routing,
splitting into parts, claiming, retrying, and learning from what
each attempt taught.

- **Strict fees.**  `maxfee_ppm` (or `maxfee_msat`) is enforced at
  the askrene quote and again post-route; no per-part slippage.
- **Batch rebalancing.**  A request considers all its sources and
  destinations at once — one min-cost-flow solve, free to split the
  amount across every pairing; per-channel caps, drawn down across
  the request, keep any single channel from over-balancing.
- **Learns the network.**  Part outcomes feed a persistent askrene
  layer and a short-lived override store; retries route better
  than first attempts, within a request and across requests.
- **Partial success is the semantic.**  `amount_msat` is a ceiling;
  every settled part is banked liquidity; zero delivered is a
  result, not an error.
- **Tenacious.**  Up to `maxrounds` plan-execute rounds replan the
  still-unmoved remainder; non-dryrun requests serialize, and the
  RPC blocks through the rounds — which is what paces a driver.
- **Independent parts.**  Each part has its own preimage and
  payment_hash — never an MPP set.  A shared hash would let a node
  routing two parts claim the second the moment the first settles;
  per-part preimages close that window.
- **A fragment floor.**  No part delivers less than
  `xrebalance-min-part-msat` — a guard against plans fragmenting
  into a spray of tiny, inefficient transfers.
- **Dryrun fidelity.**  A dryrun runs the identical planner; its
  result is what execution would have pursued.
- **Per-part notifications.**  Each part's resolution is broadcast
  as an `xrebalance_part` notification — the feedback a high-level
  rebalancer needs to keep accurate books without polling.

### What xrebalance handles

| when a request sees … | xrebalance … |
|---|---|
| a part settle | records every hop's proven liquidity in the persistent layer — success history that later plans, and later requests, build on |
| a hop report insufficient liquidity | records the bound at the blocking hop — and the proven liquidity of every hop the part cleared on the way there — then replans the remainder |
| a failure expose stale gossip — it carries the outgoing channel's current fees/CLTV | overrides the stale values so the next plan prices the hop correctly; a policy that "refreshes" to the same values is not fresher, and escalates to exclusion |
| a forwarder report the next channel gone (`unknown_next_peer`) while gossip still advertises it | temporarily excludes the channel |
| a peer charge a positive inbound fee — a surcharge the sender cannot price, so the hop fails `fee_insufficient` every time | temporarily excludes the surcharging incoming channel |
| a `fee_insufficient` with its channel_update blanked (some forwarders zero it for privacy) | cannot tell stale gossip from an inbound fee, so temporarily excludes both candidate channels — a failure must teach something, or the next plan repeats it, while over-excluding heals |
| a hop return a node-level error (`temporary_node_failure`, …) | temporarily disables the node — excluding one channel would just re-route through the same node's other channels |
| the returning HTLC offer less than the part delivered | declines to settle it — releasing the preimage against a short HTLC would let the last-hop peer claim the full amount upstream |
| no usable route at the asked amount (getroutes 205) | descends a ladder — 3/4, 1/2, 1/4, 1/8 of the amount — and plans the first amount the network can carry; a ladder run dry ends the request |
| routes only over the fee budget (getroutes 206) | stops rather than descending: base fees weigh more at smaller amounts, so cheaper routes do not appear further down |

All of this self-heals: exclusions and node disables expire after
`xrebalance-override-age`, learned liquidity after
`xrebalance-constraint-age` — as the network heals, its channels
and nodes come automatically back into the plans.  Nothing is
written off forever.

## Interface

    xrebalance sources=[src,...] destinations=[dst,...]
               amount_msat=N (maxfee_ppm=N | maxfee_msat=N)
               [label=...] [dryrun=true] [maxparts=N] [part_wait=N]
               [maxrounds=N]

Each `src`/`dst` element names one of the local node's channels,
optionally with a cap on how much this request moves through it —
at most that much
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
"Why xrebalance" above gives the security reasons (preimage replay,
short-HTLC claims); a further consequence is that intermediates
cannot even correlate the parts.  Returning parts are claimed via
the `htlc_accepted` hook — the plugin's only hook.

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
