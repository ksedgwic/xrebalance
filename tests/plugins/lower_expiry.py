#!/usr/bin/env python3
"""Test helper: forward every HTLC with an expiry below the one its
onion asks for, by replacing this hop's payload.  The next hop's
onion is untouched, so the receiver gets an HTLC that expires before
the block its own payload names."""
from pyln.client import Plugin

plugin = Plugin()


def truncated(value):
    """Big-endian without leading zero bytes (BOLT 1 tu32/tu64)."""
    return value.to_bytes((value.bit_length() + 7) // 8, 'big')


def tlv(tlv_type, value):
    return bytes([tlv_type, len(value)]) + value


@plugin.hook("htlc_accepted")
def on_htlc_accepted(onion, htlc, plugin, **kwargs):
    if 'short_channel_id' not in onion:
        return {"result": "continue"}
    block, txindex, outnum = (
        int(x) for x in onion['short_channel_id'].split('x'))
    scid = (block << 40 | txindex << 16 | outnum).to_bytes(8, 'big')
    asked = onion['outgoing_cltv_value']
    cltv = asked - plugin.get_option('lower-expiry-blocks')
    plugin.log("forwarding %s with expiry %d, onion asks for %d"
               % (htlc['payment_hash'], cltv, asked))
    # amt_to_forward, outgoing_cltv_value, short_channel_id.
    payload = (tlv(2, truncated(int(onion['forward_msat'])))
               + tlv(4, truncated(cltv))
               + tlv(6, scid))
    return {"result": "continue", "payload": payload.hex()}


plugin.add_option('lower-expiry-blocks', 1,
                  'Blocks to take off the expiry of each forwarded HTLC',
                  opt_type='int')
plugin.run()
