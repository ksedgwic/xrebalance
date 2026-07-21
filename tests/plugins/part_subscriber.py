#!/usr/bin/env python3
"""Test helper: subscribe to xrebalance_part and log every event, so
tests can assert notification delivery via wait_for_log."""
from pyln.client import Plugin

plugin = Plugin()


@plugin.subscribe("xrebalance_part")
def on_part(plugin, **kwargs):
    plugin.log("subscriber got xrebalance_part: %r" % (kwargs,))


plugin.run()
