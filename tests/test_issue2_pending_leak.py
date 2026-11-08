"""Issue #2 reproduction: a detached part that FAILS stays reserved as
pending for the rest of the request.

Upstream ksedgwic/xrebalance issue #2 (observed in production v0.4.5):
when a round's snapshot wait closes with a part in flight, the part
detaches to background_watch and its planned amount is counted in
pending_total for the rest of the request — even after the watcher has
seen the part FAIL. Later rounds plan short, and the final response
reports parts as pending that already failed.

This test makes it deterministic on regtest: l2 holds every HTLC
(hold_fail_htlcs.py), a multi-round request sends a part per round,
then we fail all held HTLCs mid-request. Asserts the final response
reports parts as pending whose authoritative notifications (the
subscriber plugin) already recorded as failed.
"""
import pytest
from pyln.testing.utils import only_one, wait_for

from pathlib import Path

HOLD_FAIL = Path(__file__).parent / "plugins" / "hold_fail_htlcs.py"


@pytest.mark.xfail(strict=True,
                   reason="ksedgwic/xrebalance#2: a detached part that "
                          "fails stays reserved as pending for the rest "
                          "of the request; remove this marker with the fix")
def test_issue2_failed_detached_part_stays_pending(node_factory, bitcoind,
                                                   plugin_opts):
    l1, l2, l3 = node_factory.line_graph(
        3, wait_for_announce=True,
        opts=[{**plugin_opts, 'fee-base': 2500, 'fee-per-satoshi': 500},
              {'plugin': str(HOLD_FAIL)},
              {}])
    scid_fill, _ = l3.fundchannel(l1, announce_channel=False)

    src = only_one(
        l1.rpc.listpeerchannels(l2.info['id'])['channels'])['short_channel_id']

    wait_for(lambda: 'remote' in only_one(
        l1.rpc.listpeerchannels(l3.info['id'])['channels']).get('updates', {}))

    # Multi-round shape: 10M msat ask, source capped to 5M msat for the
    # request -> round 1's ~600k part detaches (held by l2), later
    # rounds keep drawing the remaining cap while the failure lands.
    amount = 10_000_000
    per_req_cap = 5_000_000

    res = {}

    def run_request():
        res['r'] = l1.rpc.xrebalance(
            sources=[{'scid': src, 'max_msat': per_req_cap}],
            destinations=[scid_fill],
            amount_msat=amount, maxfee_msat=400_000,
            part_wait=3, maxrounds=12, label='issue2repro')

    import threading
    t = threading.Thread(target=run_request)
    t.start()

    # Round 1's part detaches at ~2s; by ~5s there are ~3 held parts.
    l2.daemon.wait_for_log(r"holding htlc")
    import time
    # Fail AFTER round 1's snapshot window closes (t=3s, part detaches)
    # but before round 2's closes -> rounds 3+ run post-failure.
    time.sleep(3.7)
    failed = l2.rpc.call('fail-htlcs')
    assert failed['failed'] >= 1, failed

    t.join(timeout=120)
    assert not t.is_alive(), "request thread did not finish"
    r = res['r']
    import json
    with open('/tmp/issue2-response.json', 'w') as f:
        json.dump(r, f, indent=1, default=str)
    print("RESPONSE KEYS:", sorted(r.keys()))
    print("SUMMARY:", r.get('summary'))
    assert r['status'] == 'executed', r

    # The watcher follows the failed parts to their terminal state: the
    # subscriber sees failure notifications for them.
    l1.daemon.wait_for_log(r"subscriber got xrebalance_part:.*'failed'")

    # Nothing is in flight anywhere: l2 holds nothing, and every part
    # reached its terminal failed state per the notifications. The
    # fixed contract (#2): the response must not report parts as
    # pending. Today it does — hence xfail.
    rel = l2.rpc.call('release-htlcs')
    assert rel['released'] == 0, rel
    assert r['pending_msat'] == 0, (
        "issue #2: %d msat reported pending with nothing in flight" %
        r['pending_msat'])
    assert r['summary']['parts_pending'] == 0, r

    # And rounds that ran AFTER the failure notification still reserved
    # the failed amount (planning short): wait_for_log searches forward
    # from the failure marker found above.
    l1.daemon.wait_for_log(r"round \d+/12: .* msat pending")

    print("ISSUE2 REPRODUCED: %d parts reported pending (%d msat) after "
          "their failure notifications" % (
              r['summary']['parts_pending'], r['pending_msat']))
