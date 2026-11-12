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
import json
import re
import time

import pytest
from pyln.testing.utils import only_one, wait_for

from pathlib import Path

HOLD_FAIL = Path(__file__).parent / "plugins" / "hold_fail_htlcs.py"


def log_contains(node, pattern, timeout=30):
    """Bounded cursor-free log search — pyln's is_in_log(start=0) scans
    the whole retained log, so it cannot be defeated by an earlier
    wait_for_log having advanced the cursor past the lines this pattern
    needs (the subscriber-vs-round ordering hazard)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if node.daemon.is_in_log(pattern, start=0):
            return True
        time.sleep(0.5)
    return False


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
    # the failed amount (planning short).  Search the retained log
    # directly: the subscriber wait above advances wait_for_log's cursor
    # past round summaries, so a cursor-based wait here can time out on
    # ordering alone even when every assertion held (review finding).
    assert log_contains(l1, r"round \d+/12: delivered .* msat pending"), (
        "no round summary logged a pending amount")

    print("ISSUE2 REPRODUCED: %d parts reported pending (%d msat) after "
          "their failure notifications" % (
              r['summary']['parts_pending'], r['pending_msat']))


def test_issue2_late_completing_part_counts_and_caps(node_factory, bitcoind,
                                                     plugin_opts):
    """Review follow-up (PR #8): a detached part that COMPLETES late has
    to (a) stay committed against the per-channel caps for the whole
    request — a later round must not draw the same source allowance
    again — and (b) be counted in parts_complete at response time.

    Mirror of the failure-path test: same topology, but the held parts
    are RELEASED mid-request so they settle successfully after their
    round's snapshot window closed.
    """
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

    def src_balance():
        chans = l1.rpc.listfunds()['channels']
        return only_one(
            [c for c in chans if c['short_channel_id'] == src]
        )['our_amount_msat']

    amount = 10_000_000
    per_req_cap = 5_000_000
    before = src_balance()

    res = {}

    def run_request():
        res['r'] = l1.rpc.xrebalance(
            sources=[{'scid': src, 'max_msat': per_req_cap}],
            destinations=[scid_fill],
            amount_msat=amount, maxfee_msat=400_000,
            part_wait=3, maxrounds=12, label='issue2late', verbose=True)

    import threading
    t = threading.Thread(target=run_request)
    t.start()

    # Round windows close (parts detach) while l2 holds them; then the
    # held parts COMPLETE — the late success under test.
    l2.daemon.wait_for_log(r"holding htlc")
    time.sleep(3.7)
    rel = l2.rpc.call('release-htlcs')
    assert rel['released'] >= 1, rel

    t.join(timeout=180)
    assert not t.is_alive(), "request thread did not finish"
    r = res['r']
    with open('/tmp/issue2-late-response.json', 'w') as f:
        json.dump(r, f, indent=1, default=str)
    assert r['status'] == 'executed', r

    s = r['summary']
    # (b) reconciled successes are counted: under the review bug the
    # reconciled part vanished from all three status counts.
    assert s['parts_complete'] >= 1, s
    assert s['parts_complete'] + s['parts_failed'] + s['parts_pending'] \
        == s['parts'], s
    assert r['delivered_msat'] > 0, r

    # What the response reports pending must be exactly what is STILL
    # in flight: the last round's part can still be held by l2 at
    # response time (l2 holds every later arrival too), and that part
    # is genuinely pending — the issue-#2 contract bans PHANTOM
    # pending (nothing in flight reported as pending), not pending
    # itself.  Releasing now settles exactly the held set, so the
    # counts must match it.
    rel = l2.rpc.call('release-htlcs')
    assert s['parts_pending'] == rel['released'], (s, rel)
    assert (r['pending_msat'] == 0) == (s['parts_pending'] == 0), r

    # (a) the cap bounds the WHOLE request: total drawn from the source
    # channel (delivered + fees, settled balance delta) must stay within
    # max_msat + fees.  Under the released-reservation bug, later rounds
    # re-granted the allowance and the channel paid up to `amount`.
    drawn = before - src_balance()
    assert drawn <= per_req_cap + r['fee_msat'] + 100_000, (drawn, r)

    # Subscriber saw the late part complete (cursor-free check).
    assert log_contains(l1, r"subscriber got xrebalance_part:.*'complete'"), (
        "no completion notification observed for the late part")

    print("LATE-COMPLETE OK: parts=%d complete=%d failed=%d delivered=%d "
          "drawn=%d (cap %d)" % (
              s['parts'], s['parts_complete'], s['parts_failed'],
              r['delivered_msat'], drawn, per_req_cap))
