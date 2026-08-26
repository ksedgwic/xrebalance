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
askrene changes.  Runs on stock `lightningd`, v26.04 or later: the
askrene calls it makes have existed since v24.11 (`maxparts` since
v25.09), but only v26.04 and later have been tested.

## Quick start

Fill a depleted channel from a few well-stocked ones, spending at
most 300 ppm in fees:

    lightning-cli -k xrebalance \
        sources='["803480x1657x1", "805961x1795x1", "806464x855x0"]' \
        destinations='["848864x3313x1"]' \
        amount_msat=1000000000 \
        maxfee_ppm=300

xrebalance decides how to split the amount across the sources and
replans after failures until the amount moves or no route remains.
The response reports what was delivered and what it cost — and,
when nothing moved, the failed part that came closest.  Add
`dryrun=true` to see the same plan without moving funds.

To build and load the plugin, see [Installation](#installation).

## Why xrebalance

xrebalance is a low-level rebalance executor, in the spirit of
xpay: a high-level rebalancer decides what to move — which channels
to drain, which to fill, how much, at what price, when — and
xrebalance carries it out: routing, splitting into parts, claiming,
retrying, and using what each attempt showed.

- **Strict fees.**  `maxfee_ppm` (or `maxfee_msat`) is enforced at
  the askrene quote and again post-route; no per-part slippage.
- **Batch rebalancing.**  A request considers all its sources and
  destinations together in one min-cost-flow solve, which may split
  the amount across any pairing; per-channel caps, drawn down across
  the request, bound how much any one channel gives or receives.
- **Learns from outcomes.**  Part outcomes feed a persistent askrene
  layer and a short-lived override store, so later plans — within a
  request and across requests — use what earlier attempts found.
- **Partial delivery is normal.**  `amount_msat` is a ceiling; every
  settled part is liquidity moved; zero delivered is a result, not
  an error.
- **Multiple rounds.**  Up to `maxrounds` plan-execute rounds replan
  whatever has not yet moved; non-dryrun requests run one at a time,
  and the RPC returns when the rounds end, which paces a driver.
- **Independent parts.**  Each part has its own preimage and
  payment_hash, not an MPP set.  This allows immediate independent
  settlement without waiting for all parts.
- **A fragment floor.**  No part delivers less than
  `xrebalance-min-part-msat`, so plans do not fragment into many
  tiny transfers.
- **Dryrun.**  A dryrun runs the same planner as execution and
  returns the plan execution would have used.
- **Per-part notifications.**  Each part's resolution is broadcast
  as an `xrebalance_part` notification, so a high-level rebalancer
  can track per-channel results without polling.

### What xrebalance handles

| when a request sees …                                                                                                      | xrebalance …                                                                                                                                                            |
| -------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| a part settle                                                                                                              | records every hop's proven liquidity in the persistent layer — success history that later plans, and later requests, build on                                           |
| a hop report insufficient liquidity                                                                                        | records the bound at the blocking hop — and the proven liquidity of every hop the part cleared on the way there — then replans the remainder                            |
| a failure expose stale gossip — it carries the outgoing channel's current fees/CLTV                                        | overrides the stale values so the next plan prices the hop correctly; if the "refreshed" values equal what gossip already said, the channel is excluded instead         |
| a forwarder report the next channel gone (`unknown_next_peer`) while gossip still advertises it                            | temporarily excludes the channel                                                                                                                                        |
| a peer charge a positive inbound fee — a surcharge the sender cannot price, so the hop fails `fee_insufficient` every time | temporarily excludes the surcharging incoming channel                                                                                                                   |
| a `fee_insufficient` with its channel_update blanked (some forwarders zero it for privacy)                                 | cannot tell stale gossip from an inbound fee, so temporarily excludes both candidate channels — otherwise the next plan would repeat the failure; the exclusions expire |
| a hop return a node-level error (`temporary_node_failure`, …)                                                              | temporarily disables the node — excluding one channel would just re-route through the same node's other channels                                                        |
| the returning HTLC offer less than the part delivered                                                                      | declines to settle it — releasing the preimage against a short HTLC would let the last-hop peer dishonestly claim the full amount upstream                              |
| no usable route at the asked amount (getroutes 205)                                                                        | descends a ladder — 3/4, 1/2, 1/4, 1/8 of the amount — and plans the first amount the network can carry; a ladder run dry ends the request                              |
| routes only over the fee budget (getroutes 206)                                                                            | stops rather than descending: base fees weigh more at smaller amounts, so cheaper routes do not appear further down                                                     |

All of this expires: exclusions and node disables after
`xrebalance-override-age`, learned liquidity after
`xrebalance-constraint-age`, so channels and nodes return to the
plans as conditions change.

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
its routing granularity (about 0.1% of the amount), so caps are
approximate limits, not exact accounting bounds.  Programs composing
JSON (CLBOSS) should prefer the object form; the string forms are
for humans at a CLI.

`amount_msat` remains the request-wide ceiling.  The planner clamps
it to what the channels can carry: the smaller of the summed source
bounds — less the fee budget, since fees ride the source channels on
top of the delivered amount — and the summed destination bounds.  In
particular, an `amount_msat` larger than a source can carry drains
the source rather than planning nothing.

askrene plans for payments, where delivering less than the asked
amount is failure, so each solve is all-or-nothing.  A rebalance
has no such floor — whatever moves is kept — so when no route
exists at the clamped amount (one thin corridor is enough, since
a clamp at the destination bound requires saturating every
destination exactly), the planner **descends a ladder** — retrying
the solve at 3/4, 1/2, 1/4, then 1/8 of the amount — and plans
the first amount the network can carry.  The amount planning
settled on (clamp, then ladder) is reported as
`effective_amount_msat`; dryruns run the same planner, so a
dryrun's result is the plan execution would have used.

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
Besides letting each part settle on its own, this means a node
routing two parts cannot use the first part's preimage to claim
the second, and intermediates cannot correlate the parts.  (The
short-HTLC check is in the table above.)  Returning parts are
claimed via the `htlc_accepted` hook — the plugin's only hook.

One `xrebalance_part` notification is broadcast per part reaching a
terminal state, carrying the part's own payment_hash, its
part_index, first-hop scid, real return-hop scid, delivered and fee
amounts, status, and the caller's `label` — the request-level
correlator, and enough for callers to keep accurate per-channel
books without polling.

The response leads with the outcome: request totals plus a
`summary` block — round and part counts, delivered and fee totals,
and, when nothing moved, a `closest_miss` naming the failed part
that came closest to succeeding.  `verbose=true` adds the per-round
detail: each round's parts with their payment_hashes (the durable
identifiers) and failure details.  Either way the response is a
snapshot bounded by the window — `part_wait` seconds (0 = return
immediately), defaulting to the `xrebalance-part-wait` option —
and parts still pending continue on their own; their notifications
arrive when they resolve.

## Checking on it

    lightning-cli xrebalance-stats

reports the plugin version, the effective option values, and what
the plugin has learned.  The `layer` block summarizes the persistent
askrene layer: how many constraints it holds, split into minimums
(liquidity a part proved) and maximums (bounds a failure found), how
many channel directions they cover, how deep they stack per
direction, and the timestamps of the oldest and newest.  The
`overrides` block counts the short-lived learned overrides — policy
refreshes, node disables, channel exclusions; `claims` counts parts
still in flight; `coalescer_entries` counts the cache that
suppresses redundant layer writes.

Run it after a request, or when a plan looks wrong, to see what the
layer has learned and how old that knowledge is.  `channel_updates`,
`disabled_nodes`, and `created_channels` in the layer block belong
to the per-request layers and should read zero in the persistent
one.  The full layer contents are available from `lightning-cli
askrene-listlayers layer=xrebalance`.

## Configuration

All options are dynamic — adjustable at runtime via `lightning-cli
setconfig`, so tuning never requires a plugin restart.

| option                                | default | meaning                                                                                                                 |
| ------------------------------------- | ------- | ----------------------------------------------------------------------------------------------------------------------- |
| `xrebalance-constraint-age=<seconds>` | 21600   | expiry of learned constraints                                                                                           |
| `xrebalance-override-age=<seconds>`   | 3600    | expiry of learned overrides: policy refreshes, node disables, channel exclusions                                        |
| `xrebalance-part-wait=<seconds>`      | 30      | default snapshot window                                                                                                 |
| `xrebalance-min-part-msat=<msat>`     | 1000000 | fragment floor: the least a part may deliver                                                                            |
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
(v26.04+) and `bitcoind` must be on PATH:

    cargo build
    cd tests
    LIGHTNINGD=/path/to/lightning/lightningd/lightningd uv run pytest

## Credits

Many thanks to [Lagrang3] and [daywalker90] for guidance and
inspiration.

## License

MIT

[askrene]: https://docs.corelightning.org/reference/getroutes
[plugins-install]: https://github.com/lightningd/plugins#installation
[rustup]: https://rustup.rs
[pyln-testing]: https://pypi.org/project/pyln-testing/
[Lagrang3]: https://github.com/Lagrang3
[daywalker90]: https://github.com/daywalker90
