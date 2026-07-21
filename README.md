# xrebalance

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
               [label=...] [dryrun=true] [maxparts=N] [part_wait=N]

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

Options:

    xrebalance-constraint-age=<seconds>   # expiry of learned constraints
    xrebalance-part-wait=<seconds>        # default snapshot window (180)

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
