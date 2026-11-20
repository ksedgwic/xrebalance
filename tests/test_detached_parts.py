"""Terminal outcomes of detached parts, reconciled inside the request.

When a round's snapshot wait closes with a part in flight, the part
detaches to a background watcher and its planned amount counts as
pending.  The watcher reports the part's terminal state, and the
request loop folds that state in at the top of every round:

- a failure releases its reservation right away: a round that starts
  after the failure plans with the returned amount, and its line
  reports less pending and more remaining than the rounds before it;
- a completion keeps its share of the per-channel caps for the whole
  request -- a later round must not draw the same source allowance
  again -- and is counted in parts_complete at response time.

l2 holds every HTLC until released or failed (hold_fail_htlcs.py),
which makes the detach deterministic on regtest.
"""
import re
import threading
import time
from pathlib import Path

from pyln.testing.utils import only_one, wait_for

HOLD_FAIL = Path(__file__).parent / "plugins" / "hold_fail_htlcs.py"

ROUND_LINE = re.compile(
    r"round (\d+)/(\d+): delivered ([\d_]+) msat this round "
    r"\(([\d_]+) total\), ([\d_]+) msat pending, ([\d_]+) msat remaining")
TERMINAL_LINE = re.compile(
    r"detached part ([0-9a-f]+) reached terminal state: "
    r"pending now ([\d_]+) msat")


def read_log(node):
    node.daemon.logs_catchup()
    return "\n".join(node.daemon.logs)


def log_contains(node, pattern, timeout=30):
    """Bounded cursor-free log search: wait_for_log's cursor may
    already sit past a line that fires before the line an earlier
    wait matched (the notification precedes the fold log)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if re.search(pattern, read_log(node)):
            return True
        time.sleep(0.5)
    return False


def num(s):
    return int(s.replace("_", ""))


def line_graph(node_factory, plugin_opts):
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
    return l1, l2, l3, src, scid_fill


def start_request(l1, src, scid_fill, label):
    res = {}

    def run():
        res['r'] = l1.rpc.xrebalance(
            sources=[{'scid': src, 'max_msat': 5_000_000}],
            destinations=[scid_fill], amount_msat=10_000_000,
            maxfee_msat=400_000, part_wait=3, maxrounds=12, label=label)

    t = threading.Thread(target=run)
    t.start()
    return t, res


def test_failed_part_releases_reservation_inside_the_loop(
        node_factory, bitcoind, plugin_opts):
    l1, l2, l3, src, scid_fill = line_graph(node_factory, plugin_opts)
    t, res = start_request(l1, src, scid_fill, 'detached-fail')

    # Round 1's part is held at l2 and detaches when its window
    # closes; round 2's part is held next.  Fail ONLY the round-1
    # part, while the round-2 part is still in flight: routing stays
    # alive for later rounds, and the failure's reservation is
    # released mid-request.
    wait_for(lambda: len(l2.rpc.call('held-htlcs')['held']) >= 2)
    first = l2.rpc.call('held-htlcs')['held'][0]
    failed = l2.rpc.call('fail-htlc', {'payment_hash': first})
    assert failed['failed'] == 1, failed

    # The watcher folds the failure into the running totals before
    # the next round plans against them.
    l1.daemon.wait_for_log(r"detached part .* reached terminal state")

    t.join(timeout=120)
    assert not t.is_alive(), "request thread did not finish"
    r = res['r']
    assert r['status'] == 'executed', r

    # A round STARTED after the fold: the first round line after the
    # terminal-state line reports the failed part's amount as
    # returned -- less pending, more remaining than the last line
    # before it.
    before = after = None
    seen_terminal = False
    last = None
    for line in read_log(l1).splitlines():
        if TERMINAL_LINE.search(line):
            seen_terminal = True
            before = last
            continue
        m = ROUND_LINE.search(line)
        if m is None:
            continue
        if seen_terminal:
            after = m
            break
        last = m
    assert before is not None, "no round line logged before the fold"
    assert after is not None, "no round line logged after the fold"
    assert num(after.group(5)) < num(before.group(5)), (before, after)
    assert num(after.group(6)) > num(before.group(6)), (before, after)

    # The watcher follows the failed part to its terminal state and
    # the subscriber sees its failure notification.
    assert log_contains(l1, r"subscriber got xrebalance_part:.*'failed'"), (
        "no failure notification observed for the detached part")
    assert r['summary']['parts_failed'] >= 1, r

    # What the response reports pending is exactly what is still in
    # flight: releasing settles the held set, and the counts match.
    rel = l2.rpc.call('release-htlcs')
    assert r['summary']['parts_pending'] == rel['released'], (r, rel)
    assert (r['pending_msat'] == 0) == (r['summary']['parts_pending'] == 0), r


def test_late_completing_part_keeps_caps_and_counts(
        node_factory, bitcoind, plugin_opts):
    l1, l2, l3, src, scid_fill = line_graph(node_factory, plugin_opts)

    def src_balance():
        chans = l1.rpc.listfunds()['channels']
        return only_one(
            [c for c in chans if c['short_channel_id'] == src]
        )['our_amount_msat']

    def src_htlcs():
        return only_one(
            l1.rpc.listpeerchannels(l2.info['id'])['channels'])['htlcs']

    before = src_balance()
    t, res = start_request(l1, src, scid_fill, 'detached-late')

    # Round windows close (parts detach) while l2 holds them; then
    # the held parts COMPLETE -- the late success under test.
    l2.daemon.wait_for_log(r"holding htlc")
    time.sleep(3.7)
    rel = l2.rpc.call('release-htlcs')
    assert rel['released'] >= 1, rel

    t.join(timeout=180)
    assert not t.is_alive(), "request thread did not finish"
    r = res['r']
    assert r['status'] == 'executed', r

    s = r['summary']
    # Reconciled successes are counted: a late completion shows up in
    # parts_complete, not only in the saved round's "pending".
    assert s['parts_complete'] >= 1, s
    assert s['parts_complete'] + s['parts_failed'] + s['parts_pending'] \
        == s['parts'], s
    assert r['delivered_msat'] > 0, r

    # What the response reports pending is exactly what is STILL in
    # flight: the last round's part can still be held by l2 at
    # response time, and that part is genuinely pending.  Releasing
    # now settles exactly the held set, so the counts must match it.
    rel = l2.rpc.call('release-htlcs')
    assert s['parts_pending'] == rel['released'], (s, rel)
    assert (r['pending_msat'] == 0) == (s['parts_pending'] == 0), r

    # The cap bounds the WHOLE request: once nothing is in flight,
    # the total drawn from the source channel (fees included) must
    # stay within max_msat.  Under a released-reservation bug, later
    # rounds re-grant a completed part's allowance and the channel
    # pays beyond the cap.
    wait_for(lambda: not src_htlcs(), timeout=60)
    drawn = before - src_balance()
    assert drawn <= 5_000_000, (drawn, r)

    # Subscriber saw the late part complete.
    assert log_contains(l1, r"subscriber got xrebalance_part:.*'complete'"), (
        "no completion notification observed for the late part")
