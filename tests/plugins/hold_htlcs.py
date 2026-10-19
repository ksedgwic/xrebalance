#!/usr/bin/env python3
"""Test helper: hold every incoming HTLC until release-htlcs is
called, so a test can keep an HTLC in flight on the payer's channel
for as long as it needs."""
from pyln.client import Plugin

plugin = Plugin()
held = []


def on_htlc_accepted(onion, htlc, request, plugin, **kwargs):
    held.append(request)
    plugin.log("holding htlc %s" % htlc["payment_hash"])


# A background hook returns through the request, later, instead of
# from the handler.
plugin.add_hook("htlc_accepted", on_htlc_accepted, background=True)


@plugin.method("release-htlcs")
def release_htlcs(plugin):
    """Let every held HTLC continue; returns how many."""
    released = len(held)
    while held:
        held.pop().set_result({"result": "continue"})
    return {"released": released}


plugin.run()
