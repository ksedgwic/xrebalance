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
