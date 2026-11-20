#!/usr/bin/env python3
"""Test helper: hold every incoming HTLC until it is released or one
of them is failed (the upstream hold_htlcs.py only releases; the
detached-outcome tests need a held part to FAIL).

RPCs:
  held-htlcs   — the payment hashes currently held
  fail-htlc    — fail one held HTLC by payment hash
                 (temporary_node_failure)
  release-htlcs — let every held HTLC continue (as upstream's helper)
"""
from pyln.client import Plugin

plugin = Plugin()
held = {}  # payment hash -> htlc_accepted request


def on_htlc_accepted(onion, htlc, request, plugin, **kwargs):
    held[htlc["payment_hash"]] = request
    plugin.log("holding htlc %s" % htlc["payment_hash"])


plugin.add_hook("htlc_accepted", on_htlc_accepted, background=True)


@plugin.method("held-htlcs")
def held_htlcs(plugin):
    """Return the payment hashes currently held, oldest first."""
    return {"held": list(held)}


@plugin.method("fail-htlc")
def fail_htlc(plugin, payment_hash):
    """Fail one held HTLC (temporary_node_failure); returns how many."""
    request = held.pop(payment_hash, None)
    if request is None:
        return {"failed": 0}
    request.set_result({"result": "fail", "failure_message": "2002"})
    return {"failed": 1}


@plugin.method("release-htlcs")
def release_htlcs(plugin):
    """Let every held HTLC continue; returns how many."""
    released = len(held)
    while held:
        _, request = held.popitem()
        request.set_result({"result": "continue"})
    return {"released": released}


plugin.run()
