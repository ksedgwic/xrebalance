# xrebalance — askrene adapted for circular routing

A Core Lightning plugin that rebalances a node's channels by
moving funds between them via independent circular self-payments,
using [askrene] for route computation.  askrene only solves
point-to-point, so xrebalance adapts it by *node splitting*: each
request builds a layer that splits the local node into its two
roles — the real node originates the route, and a stand-in for its
inbound side terminates it, reachable only through copies of the
chosen destination channels.  Any route from the one to the other
is, on the wire, a circle back to the local node.  The adaptation
is expressed entirely through the public askrene layer API — no
askrene changes.  Runs on stock `lightningd`, v26.04 or later.

## Quick start

Fill a depleted channel from a few well-stocked ones, spending at
most 300 ppm in fees:

    lightning-cli -k xrebalance \
        sources='["803480x1657x1", "805961x1795x1", "806464x855x0"]' \
        destinations='["848864x3313x1"]' \
        amount_msat=1000000000 \
        maxfee_ppm=300

xrebalance decides the split across the sources, replanning as
failures teach it, until the amount moves or it can prove it
cannot.  The response summarizes what was delivered and what it
cost — and, when nothing moved, where the attempt came nearest to
landing.  Add `dryrun=true` to see the identical plan without
moving funds.

To build and load the plugin, see [Installation](#installation).

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

| when a request sees …                                                                                                      | xrebalance …                                                                                                                                                                                      |
| -------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| a part settle                                                                                                              | records every hop's proven liquidity in the persistent layer — success history that later plans, and later requests, build on                                                                     |
| a hop report insufficient liquidity                                                                                        | records the bound at the blocking hop — and the proven liquidity of every hop the part cleared on the way there — then replans the remainder                                                      |
| a failure expose stale gossip — it carries the outgoing channel's current fees/CLTV                                        | overrides the stale values so the next plan prices the hop correctly; a policy that "refreshes" to the same values is not fresher, and escalates to exclusion                                     |
| a forwarder report the next channel gone (`unknown_next_peer`) while gossip still advertises it                            | temporarily excludes the channel                                                                                                                                                                  |
| a peer charge a positive inbound fee — a surcharge the sender cannot price, so the hop fails `fee_insufficient` every time | temporarily excludes the surcharging incoming channel                                                                                                                                             |
| a `fee_insufficient` with its channel_update blanked (some forwarders zero it for privacy)                                 | cannot tell stale gossip from an inbound fee, so temporarily excludes both candidate channels — a failure must teach something, or the next plan repeats it, while over-excluding heals over time |
| a hop return a node-level error (`temporary_node_failure`, …)                                                              | temporarily disables the node — excluding one channel would just re-route through the same node's other channels                                                                                  |
| the returning HTLC offer less than the part delivered                                                                      | declines to settle it — releasing the preimage against a short HTLC would let the last-hop peer dishonestly claim the full amount upstream                                                        |
| no usable route at the asked amount (getroutes 205)                                                                        | descends a ladder — 3/4, 1/2, 1/4, 1/8 of the amount — and plans the first amount the network can carry; a ladder run dry ends the request                                                        |
| routes only over the fee budget (getroutes 206)                                                                            | stops rather than descending: base fees weigh more at smaller amounts, so cheaper routes do not appear further down                                                                               |

All of this self-heals: exclusions and node disables expire after
`xrebalance-override-age`, learned liquidity after
`xrebalance-constraint-age` — as the network heals, its channels
and nodes come automatically back into the plans.  Nothing is
written off forever.

## Interface

    xrebalance sources=[src,...] destinations=[dst,...]
               amount_msat=N (maxfee_ppm=N | maxfee_msat=N)
               [label=...] [dryrun=true] [maxparts=N] [part_wait=N]
               [maxrounds=N] [verbose=true]

Each `src`/`dst` element names one of the local node's channels,
optionally with a cap on how much this request moves through it —
at most that much drawn from a source, at most that much delivered
into a destination.  A bare scid, or a cap in three equivalent
spellings:

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
means "drain it", rather than planning nothing.

askrene plans for payments, where delivering less than the asked
amount is failure, so each solve is all-or-nothing.  A rebalance
has no such floor — whatever moves is banked — so when no route
exists at the clamped amount (one thin corridor is enough, since
a clamp at the destination bound requires saturating every
destination exactly), the planner **descends a ladder** — retrying
the solve at 3/4, 1/2, 1/4, then 1/8 of the amount — and plans
the first amount the network can actually carry.  This is the
plan-time face of "partial delivery is the norm".  The amount planning
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
The security reasons appear above — preimage replay under "Why
xrebalance", short-HTLC claims under "What xrebalance handles" —
and a further consequence is that intermediates cannot even
correlate the parts.  Returning parts are claimed via
the `htlc_accepted` hook — the plugin's only hook.

One `xrebalance_part` notification is broadcast per part reaching a
terminal state, carrying the part's own payment_hash, its
part_index, first-hop scid, real return-hop scid, delivered and fee
amounts, status, and the caller's `label` — the request-level
correlator, and enough for callers to keep accurate per-channel
books without polling.

The response leads with the outcome: request totals plus a
`summary` block — round and part counts, delivered and fee totals,
and, when nothing moved, a `closest_miss` naming the failed part
that came nearest to landing.  `verbose=true` adds the per-round
detail: each round's parts with their payment_hashes (the durable
handles) and failure geometry.  Either way the response is a
snapshot bounded by the window — `part_wait` seconds (0 = return
immediately), defaulting to the `xrebalance-part-wait` option —
and parts still pending detach and keep settling; their
notifications fire when they land.

## Configuration

All options are dynamic — adjustable at runtime via `lightning-cli
setconfig`, so tuning never requires a plugin restart.

| option                                | default | meaning                                                                                                                 |
| ------------------------------------- | ------- | ----------------------------------------------------------------------------------------------------------------------- |
| `xrebalance-constraint-age=<seconds>` | 21600   | expiry of learned constraints                                                                                           |
| `xrebalance-override-age=<seconds>`   | 3600    | expiry of learned overrides: policy refreshes, node disables, channel exclusions                                        |
| `xrebalance-part-wait=<seconds>`      | 30      | default snapshot window                                                                                                 |
| `xrebalance-min-part-msat=<msat>`     | 10000   | fragment floor: the least a part may deliver                                                                            |
| `xrebalance-max-rounds=<n>`           | 50      | plan-execute rounds per request (`maxrounds` overrides; ignored on dryrun)                                              |
| `xrebalance-final-cltv=<blocks>`      | 40      | final-hop cltv delta for return legs: slack for the removal handshake before lightningd's fulfilled-HTLC close deadline |

## Installation

For general plugin installation instructions see the
[plugins repo README][plugins-install].

### Building from source

With a [rustup]-installed Rust toolchain:

    git clone https://github.com/ksedgwic/xrebalance.git
    cd xrebalance
    cargo build --release

The binary is at `target/release/xrebalance`.
`./install-versioned.sh` builds and installs it under a
git-described versioned name with a stable `xrebalance` symlink:
reverting is re-pointing the symlink, and the running version is
visible in the filename and in the plugin's log prefix.

### Loading the plugin

Start it dynamically — the plugin can be started, stopped, and
restarted without restarting `lightningd`:

    lightning-cli plugin start /path/to/xrebalance

or load it at startup with a line in your CLN config file:

    plugin=/path/to/xrebalance

If you set `xrebalance-*` options in the config file, make sure
the plugin starts automatically with CLN (a `plugin=` line or a
symlink in your plugins folder): `lightningd` refuses to start
over options no plugin claims.

## Testing

Integration tests drive the real plugin binary against regtest
nodes via [pyln-testing]; nothing is mocked.  `lightningd`
(v26.06+) and `bitcoind` must be on PATH:

    cargo build
    cd tests
    LIGHTNINGD=/path/to/lightning/lightningd/lightningd uv run pytest

## Credits

Many thanks to [Lagrang3] and [daywalker90] for guidance and
inspiration.

## License

MIT

[askrene]: https://docs.corelightning.org/reference/lightning-getroutes
[plugins-install]: https://github.com/lightningd/plugins#installation
[rustup]: https://rustup.rs
[pyln-testing]: https://pypi.org/project/pyln-testing/
[Lagrang3]: https://github.com/Lagrang3
[daywalker90]: https://github.com/daywalker90
