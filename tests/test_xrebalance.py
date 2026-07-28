"""End-to-end flow on a regtest triangle:

    l1 -> l2 -> l3 public; l3 -> l1 unannounced (the fill channel).

Covers: dryrun planning with translated final hops, the zero-budget
zero-delivered result, real execution settled via the claimer, the
authoritative xrebalance_part notifications (in-window and detached
background watcher), and success feedback landing in the persistent
layer.
"""
import pytest
from pyln.client import RpcError
from pyln.testing.utils import only_one, wait_for


def test_xrebalance_flow(node_factory, bitcoind, xrebalance_plugin,
                         part_subscriber):
    l1, l2, l3 = node_factory.line_graph(
        3, wait_for_announce=True,
        opts=[{'plugin': [xrebalance_plugin, part_subscriber]}, {}, {}])
    scid_fill, _ = l3.fundchannel(l1, announce_channel=False)

    src = only_one(
        l1.rpc.listpeerchannels(l2.info['id'])['channels'])['short_channel_id']

    # The fill peer's channel_update must arrive before we can mirror
    # its policy.
    wait_for(lambda: 'remote' in only_one(
        l1.rpc.listpeerchannels(l3.info['id'])['channels']).get('updates', {}))

    # DRYRUN: plan only.
    res = l1.rpc.xrebalance(sources=[src], destinations=[scid_fill],
                            amount_msat=100000, maxfee_msat=5000,
                            dryrun=True)
    assert res['status'] == 'planned', res
    assert res['delivered_msat'] == 100000, res
    assert res['fee_msat'] <= 5000, res

    route = only_one(res['routes'])
    path = route['path']
    # Leaves via the named source channel...
    assert path[0]['short_channel_id_dir'].startswith(src), res
    # ...and comes home over the REAL fill channel, translated back
    # from the mirror by the plugin.
    fill_dir = 0 if l3.info['id'] < l1.info['id'] else 1
    assert path[-1]['short_channel_id_dir'] == f"{scid_fill}/{fill_dir}", res
    assert path[-1]['node_id_out'] == l1.info['id'], res

    # Zero-delivered-is-a-result: an impossible budget plans nothing
    # but does not error.
    res = l1.rpc.xrebalance(sources=[src], destinations=[scid_fill],
                            amount_msat=100000, maxfee_msat=0,
                            dryrun=True)
    assert res['status'] == 'planned', res
    assert res['delivered_msat'] == 0, res
    assert res['routes'] == [], res

    # Options are dynamic: setconfig adjusts them in place, without
    # a plugin restart (which would drop claims and learned state).
    l1.rpc.setconfig('xrebalance-part-wait', 60)
    assert l1.rpc.listconfigs('xrebalance-part-wait')[
        'configs']['xrebalance-part-wait']['value_int'] == 60

    # EXECUTE: actually move the funds around the triangle.
    # maxrounds=1 pins the single-shot response shape (parts at top
    # level) that the assertions below read.
    before = only_one(
        l1.rpc.listpeerchannels(l3.info['id'])['channels'])['to_us_msat']
    res = l1.rpc.xrebalance(sources=[src], destinations=[scid_fill],
                            amount_msat=100000, maxfee_msat=5000,
                            maxrounds=1)
    assert res['status'] == 'executed', res
    part = only_one(res['parts'])
    assert part['status'] == 'complete', res
    assert res['delivered_msat'] == 100000, res
    assert part['first_hop'].startswith(src), res
    assert part['return_hop'] == f"{scid_fill}/{fill_dir}", res
    assert res['fee_msat'] <= 5000, res

    # Our side of the fill channel grew by exactly the delivered
    # amount: the self-payment settled via the htlc_accepted claimer.
    wait_for(lambda: only_one(
        l1.rpc.listpeerchannels(l3.info['id'])['channels'])['to_us_msat']
        == before + 100000)

    # The authoritative result channel: the subscriber plugin saw the
    # part's terminal notification.
    l1.daemon.wait_for_log(r"subscriber got xrebalance_part:.*'complete'")
    assert l1.daemon.is_in_log(
        r"subscriber got xrebalance_part:.*%s"
        % only_one(res['parts'])['payment_hash'])

    # Success feedback: the one NETWORK hop of the route (l2 -> l3;
    # first and return hops are ours and excluded) must now carry an
    # unconstrained record in the persistent xrebalance layer at (at
    # least) the amount that crossed it.
    chan23 = only_one([c for c in l1.rpc.listchannels(
        source=l2.info['id'])['channels']
        if c['destination'] == l3.info['id']])
    scidd23 = f"{chan23['short_channel_id']}/{chan23['direction']}"
    xlayer = only_one(l1.rpc.askrene_listlayers('xrebalance')['layers'])
    cons = [c for c in xlayer['constraints']
            if c['short_channel_id_dir'] == scidd23]
    assert cons, xlayer
    assert max(c.get('minimum_msat', 0) for c in cons) >= 100000, cons

    # part_wait=0: the snapshot returns immediately with the part
    # pending; the detached background watcher follows it and emits
    # the terminal notification when it lands.
    before2 = only_one(
        l1.rpc.listpeerchannels(l3.info['id'])['channels'])['to_us_msat']
    res = l1.rpc.xrebalance(sources=[src], destinations=[scid_fill],
                            amount_msat=50000, maxfee_msat=5000,
                            part_wait=0, maxrounds=1, label='zero-wait')
    assert res['status'] == 'executed', res
    assert only_one(res['parts'])['status'] == 'pending', res
    assert res['delivered_msat'] == 0, res
    wait_for(lambda: only_one(
        l1.rpc.listpeerchannels(l3.info['id'])['channels'])['to_us_msat']
        == before2 + 50000)
    l1.daemon.wait_for_log(r"subscriber got xrebalance_part:.*'zero-wait'")

    # The stats command summarizes the persistent layer and the
    # in-memory stores.  After the transfers above: constraints
    # recorded, nothing but constraints in the layer, no lingering
    # claims (consumed on settle).
    stats = l1.rpc.call('xrebalance-stats')
    assert stats['layer']['exists'], stats
    assert stats['layer']['constraints'] >= 1, stats
    assert stats['layer']['channel_updates'] == 0, stats
    assert stats['layer']['disabled_nodes'] == 0, stats
    assert stats['layer']['created_channels'] == 0, stats
    assert stats['claims'] == 0, stats
    assert stats['layer']['dirs_with_min'] >= 1, stats
    assert stats['layer']['depth_max'] >= 1, stats


def test_failure_feedback(node_factory, bitcoind, xrebalance_plugin,
                          part_subscriber):
    """A network hop without the liquidity the plan assumes.

    askrene knows a network channel's capacity but not its balance
    split, so after l2 pays away most of its l2 -> l3 balance the
    plan still routes through it; the part then fails there with
    temporary_channel_failure.  The failure must surface as a failed
    part, a terminal notification, and a constrained record on the
    erring direction in the persistent layer -- and the next solve
    must refuse the now-known-infeasible route.
    """
    l1, l2, l3 = node_factory.line_graph(
        3, wait_for_announce=True,
        opts=[{'plugin': [xrebalance_plugin, part_subscriber]}, {}, {}])
    scid_fill, _ = l3.fundchannel(l1, announce_channel=False)

    src = only_one(
        l1.rpc.listpeerchannels(l2.info['id'])['channels'])['short_channel_id']
    wait_for(lambda: 'remote' in only_one(
        l1.rpc.listpeerchannels(l3.info['id'])['channels']).get('updates', {}))

    # Drain l2 -> l3: after this l2 can forward well under the
    # 200_000_000 msat the rebalance will ask of it.
    l2.pay(l3, 900_000_000)
    wait_for(lambda: only_one(
        l2.rpc.listpeerchannels(l3.info['id'])['channels'])['spendable_msat']
        < 150_000_000)

    res = l1.rpc.xrebalance(sources=[src], destinations=[scid_fill],
                            amount_msat=200_000_000, maxfee_msat=1_000_000,
                            maxrounds=1, label='starved')
    assert res['status'] == 'executed', res
    part = only_one(res['parts'])
    assert part['status'] == 'failed', res
    assert 'WIRE_TEMPORARY_CHANNEL_FAILURE' in part['detail'], res
    assert res['delivered_msat'] == 0, res
    assert res['pending_msat'] == 0, res

    l1.daemon.wait_for_log(r"subscriber got xrebalance_part:.*'failed'")

    # Failure feedback: the erring direction (l2 -> l3) now carries a
    # constrained record in the persistent layer.
    chan23 = only_one([c for c in l1.rpc.listchannels(
        source=l2.info['id'])['channels']
        if c['destination'] == l3.info['id']])
    scidd23 = f"{chan23['short_channel_id']}/{chan23['direction']}"
    xlayer = only_one(l1.rpc.askrene_listlayers('xrebalance')['layers'])
    cons = [c for c in xlayer['constraints']
            if c['short_channel_id_dir'] == scidd23
            and 'maximum_msat' in c]
    assert cons, xlayer
    assert min(c['maximum_msat'] for c in cons) < 210_000_000, cons

    # The learned constraint reaches the next solve: the only route
    # is now known infeasible at the full amount, so the planner
    # descends the ladder and plans the first rung the constraint
    # admits (3/4 of the ask) instead of returning nothing.
    res = l1.rpc.xrebalance(sources=[src], destinations=[scid_fill],
                            amount_msat=200_000_000, maxfee_msat=1_000_000,
                            dryrun=True)
    assert res['status'] == 'planned', res
    assert res['effective_amount_msat'] == 150_000_000, res
    assert res['delivered_msat'] == 150_000_000, res
    assert res['routes'] != [], res


def test_scid_limits(node_factory, bitcoind, xrebalance_plugin,
                     part_subscriber):
    """Per-scid caps: both syntax forms, the fee-aware amount clamp,
    caps binding inside one multi-source solve, and the zero-cap
    result.  Fee budget below: maxfee_msat=5000, so a binding source
    bound loses exactly 5000 msat of headroom to fees.
    """
    l1, l2, l3 = node_factory.line_graph(
        3, wait_for_announce=True,
        opts=[{'plugin': [xrebalance_plugin, part_subscriber]}, {}, {}])
    scid_fill, _ = l3.fundchannel(l1, announce_channel=False)

    src = only_one(
        l1.rpc.listpeerchannels(l2.info['id'])['channels'])['short_channel_id']
    wait_for(lambda: 'remote' in only_one(
        l1.rpc.listpeerchannels(l3.info['id'])['channels']).get('updates', {}))

    # String form on the source: the cap clamps the whole request,
    # less the fee budget (the cap bounds what CROSSES the channel,
    # fees included), and the response reports the clamp.
    res = l1.rpc.xrebalance(sources=[f'{src}:60000'],
                            destinations=[scid_fill],
                            amount_msat=100000, maxfee_msat=5000,
                            dryrun=True)
    assert res['status'] == 'planned', res
    assert res['effective_amount_msat'] == 55000, res
    assert res['delivered_msat'] == 55000, res
    # The plan honors the cap on the wire: delivered plus fees stays
    # within what the source channel was allowed to carry.
    assert res['delivered_msat'] + res['fee_msat'] <= 60000, res

    # Object form on the destination: no fee headroom on that side
    # (the last hop delivers net of fees).
    res = l1.rpc.xrebalance(sources=[src],
                            destinations=[{'scid': scid_fill,
                                           'max_msat': 70000}],
                            amount_msat=100000, maxfee_msat=5000,
                            dryrun=True)
    assert res['effective_amount_msat'] == 70000, res
    assert res['delivered_msat'] == 70000, res

    # No caps and ample liquidity: nothing is clamped.
    res = l1.rpc.xrebalance(sources=[src], destinations=[scid_fill],
                            amount_msat=100000, maxfee_msat=5000,
                            dryrun=True)
    assert res['effective_amount_msat'] == 100000, res

    # A cap of 0 on the only source: zero moved is a result, not an
    # error, and the detail names the binding side.
    res = l1.rpc.xrebalance(sources=[{'scid': src, 'max_msat': 0}],
                            destinations=[scid_fill],
                            amount_msat=100000, maxfee_msat=5000,
                            dryrun=True)
    assert res['status'] == 'planned', res
    assert res['delivered_msat'] == 0, res
    assert res['routes'] == [], res
    assert 'sources' in res['detail'], res

    # A malformed limit is a parameter error.
    with pytest.raises(RpcError, match='invalid limit'):
        l1.rpc.xrebalance(sources=[f'{src}:12.5sat'],
                          destinations=[scid_fill],
                          amount_msat=100000, maxfee_msat=5000)

    # Duplicate scids would make the caps ambiguous.
    with pytest.raises(RpcError, match='duplicate source'):
        l1.rpc.xrebalance(sources=[src, f'{src}:1000'],
                          destinations=[scid_fill],
                          amount_msat=100000, maxfee_msat=5000)

    # Two sources with individual caps, one solve: the full amount is
    # delivered while each source's first-hop flow (delivered plus
    # downstream fees) stays under its own cap -- the property the
    # caps exist for, enforced by the solver rather than post-hoc.
    # The amount sits above askrene's ~1000-sat single-path
    # threshold: below it the small-amount solver requires ONE path
    # carrying everything, and no capped-below-the-amount source can
    # provide that (nor could any split honor caps < amount).
    scid_src2, _ = l1.fundchannel(l2)
    caps = {src: 1_500_000, scid_src2: 3_000_000}
    res = l1.rpc.xrebalance(
        sources=[{'scid': s, 'max_msat': c} for s, c in caps.items()],
        destinations=[scid_fill],
        amount_msat=4_000_000, maxfee_msat=50_000, dryrun=True)
    assert res['effective_amount_msat'] == 4_000_000, res
    assert res['delivered_msat'] == 4_000_000, res
    per_src = {s: 0 for s in caps}
    for route in res['routes']:
        first = route['path'][0]
        scid0 = first['short_channel_id_dir'].split('/')[0]
        assert scid0 in per_src, res
        per_src[scid0] += first['amount_in_msat']
    # askrene's MCF solves in ~amount/1000 quantization units and may
    # sit one unit past a knowledge bound, so caps are honored to
    # routing granularity rather than to the msat.
    slop = 4_000_000 // 1000
    for s, cap in caps.items():
        assert per_src[s] <= cap + slop, (per_src, caps)
    # The capped-tighter source cannot cover the amount alone, so the
    # solve genuinely split.
    assert sum(1 for v in per_src.values() if v > 0) == 2, per_src

    # EXECUTE with a source cap: the settled outcome honors it too.
    before = only_one(
        l1.rpc.listpeerchannels(l3.info['id'])['channels'])['to_us_msat']
    res = l1.rpc.xrebalance(sources=[f'{src}:60000'],
                            destinations=[scid_fill],
                            amount_msat=100000, maxfee_msat=5000,
                            maxrounds=1)
    assert res['status'] == 'executed', res
    assert res['effective_amount_msat'] == 55000, res
    assert res['delivered_msat'] == 55000, res
    wait_for(lambda: only_one(
        l1.rpc.listpeerchannels(l3.info['id'])['channels'])['to_us_msat']
        == before + 55000)


def test_min_part_floor(node_factory, bitcoind, xrebalance_plugin,
                        part_subscriber):
    """The fragment floor rides each destination mirror's
    htlc_minimum_msat, so askrene itself never plans a part
    delivering less: sub-floor flows are dropped in its refine
    stage and the remainder re-solved.  A floor above every ladder
    rung therefore yields no routes at all (the mirror is
    unusable), and a floor below the ask leaves planning intact.
    Dynamic: tuned via setconfig between requests.
    """
    l1, l2, l3 = node_factory.line_graph(
        3, wait_for_announce=True,
        opts=[{'plugin': [xrebalance_plugin, part_subscriber]}, {}, {}])
    scid_fill, _ = l3.fundchannel(l1, announce_channel=False)

    src = only_one(
        l1.rpc.listpeerchannels(l2.info['id'])['channels'])['short_channel_id']
    wait_for(lambda: 'remote' in only_one(
        l1.rpc.listpeerchannels(l3.info['id'])['channels']).get('updates', {}))

    # Floor above the ask: every descent-ladder rung sits below the
    # floor, so no part can be planned at any rung.
    l1.rpc.setconfig('xrebalance-min-part-msat', 200_000)
    res = l1.rpc.xrebalance(sources=[src], destinations=[scid_fill],
                            amount_msat=100_000, maxfee_msat=5_000,
                            dryrun=True)
    assert res['routes'] == [], res
    assert res['delivered_msat'] == 0, res

    # Floor below the ask: plans normally, and every planned part
    # delivers at least the floor.
    l1.rpc.setconfig('xrebalance-min-part-msat', 60_000)
    res = l1.rpc.xrebalance(sources=[src], destinations=[scid_fill],
                            amount_msat=100_000, maxfee_msat=5_000,
                            dryrun=True)
    assert res['routes'], res
    for route in res['routes']:
        assert route['path'][-1]['amount_out_msat'] >= 60_000, res

    # The dynamic value is visible in the stats.
    stats = l1.rpc.call('xrebalance-stats')
    assert stats['options']['min_part_msat'] == 60_000, stats


def test_maxrounds(node_factory, bitcoind, xrebalance_plugin,
                   part_subscriber):
    """The tenacious loop: an ask beyond what the channels can carry
    runs multiple rounds -- round 1 moves what fits, a later round
    replans the remainder, finds the sources and destination
    exhausted, and stops with a reason instead of an error.  The
    multi-round response carries per-round snapshots plus request
    totals; maxrounds=1 keeps the old shape.
    """
    l1, l2, l3 = node_factory.line_graph(
        3, wait_for_announce=True,
        opts=[{'plugin': [xrebalance_plugin, part_subscriber]}, {}, {}])
    scid_fill, _ = l3.fundchannel(l1, announce_channel=False)

    src = only_one(
        l1.rpc.listpeerchannels(l2.info['id'])['channels'])['short_channel_id']
    wait_for(lambda: 'remote' in only_one(
        l1.rpc.listpeerchannels(l3.info['id'])['channels']).get('updates', {}))

    # Ask for roughly twice what the ~1M-sat corridor can carry.
    res = l1.rpc.xrebalance(sources=[src], destinations=[scid_fill],
                            amount_msat=2_000_000_000,
                            maxfee_msat=100_000,
                            maxrounds=5, label='tenacious')
    assert res['status'] == 'executed', res
    assert res['rounds_run'] == len(res['rounds']), res
    assert 2 <= res['rounds_run'] <= 4, res
    assert res['delivered_msat'] >= 500_000_000, res
    assert res['stop_reason'], res
    l1.daemon.wait_for_log(r"req tenacious: round 1/5: delivered")
    l1.daemon.wait_for_log(r"req tenacious: finished after \d+ round")
