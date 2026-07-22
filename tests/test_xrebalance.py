"""End-to-end flow on a regtest triangle:

    l1 -> l2 -> l3 public; l3 -> l1 unannounced (the fill channel).

Covers: dryrun planning with translated final hops, the zero-budget
zero-delivered result, real execution settled via the claimer, the
authoritative xrebalance_part notifications (in-window and detached
background watcher), and success feedback landing in the persistent
layer.
"""
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
    before = only_one(
        l1.rpc.listpeerchannels(l3.info['id'])['channels'])['to_us_msat']
    res = l1.rpc.xrebalance(sources=[src], destinations=[scid_fill],
                            amount_msat=100000, maxfee_msat=5000)
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
                            part_wait=0, label='zero-wait')
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
                            label='starved')
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
    # is now known infeasible at this amount, so nothing is planned.
    res = l1.rpc.xrebalance(sources=[src], destinations=[scid_fill],
                            amount_msat=200_000_000, maxfee_msat=1_000_000,
                            dryrun=True)
    assert res['status'] == 'planned', res
    assert res['delivered_msat'] == 0, res
    assert res['routes'] == [], res
