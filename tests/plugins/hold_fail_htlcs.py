#!/usr/bin/env python3
"""Test helper for the issue-#2 reproduction: hold every incoming HTLC
until released, then FAIL them all (the upstream hold_htlcs.py releases
with continue; the pending-reservation leak needs a failure).

RPCs:
  fail-htlcs   — fail every held HTLC (temporary_channel_failure)
  release-htlcs — let every held HTLC continue (as upstream's helper)
"""
from pyln.client import Plugin

plugin = Plugin()
held = []


def on_htlc_accepted(onion, htlc, request, plugin, **kwargs):
    held.append(request)
    plugin.log("holding htlc %s" % htlc["payment_hash"])


plugin.add_hook("htlc_accepted", on_htlc_accepted, background=True)


@plugin.method("fail-htlcs")
def fail_htlcs(plugin):
    """Fail every held HTLC; returns how many."""
    n = len(held)
    while held:
        held.pop().set_result({"result": "fail",
                               "failure_message": "2002"})
    return {"failed": n}


@plugin.method("release-htlcs")
def release_htlcs(plugin):
    """Let every held HTLC continue; returns how many."""
    released = len(held)
    while held:
        held.pop().set_result({"result": "continue"})
    return {"released": released}


plugin.run()
