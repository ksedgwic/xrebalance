//! Interpret BOLT 4 onion failures: extract the embedded BOLT 7
//! channel_update, and attribute a FEE_INSUFFICIENT to the side of
//! the erring node actually at fault.

/// Parsed channel_update policy fields, as extracted from the
/// payload a BOLT 4 onion failure embeds: the subset
/// askrene-update-channel accepts, plus the bLIP-18 inbound-fee
/// TLV.
pub struct ChanUpdate {
    pub enabled: bool,
    pub cltv_expiry_delta: u16,
    pub htlc_minimum_msat: u64,
    pub fee_base_msat: u32,
    pub fee_proportional_millionths: u32,
    pub htlc_maximum_msat: u64,
    /// bLIP-18 inbound fees (TLV 55555): (base_msat,
    /// proportional_millionths), both signed.  None when the update
    /// carries no such TLV.
    pub inbound_fee: Option<(i32, i32)>,
}

/// Which side of the erring node a FEE_INSUFFICIENT (0x100c)
/// indicts.  The required fee at the erring node is
/// outbound_fee(outgoing channel) + inbound_fee(incoming channel),
/// but the error names and carries the policy of the OUTGOING
/// channel only.
pub enum FeeFault {
    /// The allocated fee covers the outgoing channel's advertised
    /// policy, or there is no update to compare (a plain
    /// stale-policy failure would carry one): the shortfall is the
    /// INCOMING channel's inbound fee.
    Inbound,
    /// The allocated fee falls short of the advertised outbound
    /// policy: our gossip view of the outgoing channel is stale.
    StaleOutbound,
}

/// Attribute a FEE_INSUFFICIENT.  `alloc_msat` is the fee the route
/// allocated at the erring node (amount in minus amount out);
/// `out_msat` is the amount forwarded over the outgoing channel.
pub fn classify_fee_insufficient(
    alloc_msat: u64,
    out_msat: u64,
    update: Option<&ChanUpdate>,
) -> FeeFault {
    let Some(cu) = update else {
        return FeeFault::Inbound;
    };
    // The parser bounds proportional fees at 100%, so this cannot
    // wrap for any Lightning-plausible amount.
    let required = u64::from(cu.fee_base_msat)
        + u64::from(cu.fee_proportional_millionths) * out_msat / 1_000_000;
    if alloc_msat >= required {
        FeeFault::Inbound
    } else {
        FeeFault::StaleOutbound
    }
}

/// Read a big-endian unsigned integer of 1..8 bytes.  Caller
/// ensures the read is in-bounds.
fn read_be(data: &[u8], offset: usize, nbytes: usize) -> u64 {
    let mut v = 0u64;
    for i in 0..nbytes {
        v = (v << 8) | u64::from(data[offset + i]);
    }
    v
}

/// Read a BOLT 1 BigSize at `pos`, advancing it.  None if
/// truncated.
fn read_bigsize(data: &[u8], pos: &mut usize) -> Option<u64> {
    let first = *data.get(*pos)?;
    let nbytes = match first {
        0xfd => 2,
        0xfe => 4,
        0xff => 8,
        v => {
            *pos += 1;
            return Some(u64::from(v));
        }
    };
    if *pos + 1 + nbytes > data.len() {
        return None;
    }
    let v = read_be(data, *pos + 1, nbytes);
    *pos += 1 + nbytes;
    Some(v)
}

/// Parse a BOLT 4 onion failure payload (the `raw_message` hex from
/// sendpay failure data) and extract the embedded channel_update.
/// None if the hex is malformed, the failcode does not carry a
/// channel_update, the payload is truncated, or the update carries
/// an absurd policy (proportional fee above 100% -- never a policy
/// we would pay, and bounding it here is the overflow guarantee
/// classify_fee_insufficient relies on).
///
/// Wire layout for the relevant failcodes:
///
///   2  failcode
///   X  variable per-failcode header:
///        0x1007 / 0x100e:                  0 bytes
///        0x100b / 0x100c (amount):         8 bytes htlc_msat
///        0x100d (cltv):                    4 bytes cltv_expiry
///   2  channel_update length (big-endian)
///   N  channel_update bytes
///
/// The channel_update's 2-byte type prefix 0x0102 is present in
/// CLN-issued channel_updates and absent in LND-pre-v0.18 ones;
/// both forms are accepted.
pub fn parse_chan_update(raw_message_hex: &str) -> Option<ChanUpdate> {
    let bytes = hex::decode(raw_message_hex).ok()?;
    if bytes.len() < 4 {
        return None;
    }
    let failcode = read_be(&bytes, 0, 2);
    let header = match failcode {
        0x1007 | 0x100e => 0,
        0x100b | 0x100c => 8,
        0x100d => 4,
        _ => return None,
    };
    let mut pos = 2 + header;
    if bytes.len() < pos + 2 {
        return None;
    }
    let cu_len = read_be(&bytes, pos, 2) as usize;
    pos += 2;
    if cu_len == 0 || bytes.len() < pos + cu_len {
        return None;
    }
    let mut cu = &bytes[pos..pos + cu_len];
    // Skip the optional type prefix.
    if cu.len() >= 2 && cu[0] == 0x01 && cu[1] == 0x02 {
        cu = &cu[2..];
    }
    // Fixed-layout body: 64 sig + 32 chain_hash + 8
    // short_channel_id + 4 timestamp + 1 message_flags + 1
    // channel_flags + 2 cltv_expiry_delta + 8 htlc_minimum_msat +
    // 4 fee_base_msat + 4 fee_proportional_millionths + 8
    // htlc_maximum_msat = 136.
    if cu.len() < 136 {
        return None;
    }
    let out = ChanUpdate {
        enabled: cu[109] & 0x02 == 0,
        cltv_expiry_delta: read_be(cu, 110, 2) as u16,
        htlc_minimum_msat: read_be(cu, 112, 8),
        fee_base_msat: read_be(cu, 120, 4) as u32,
        fee_proportional_millionths: read_be(cu, 124, 4) as u32,
        htlc_maximum_msat: read_be(cu, 128, 8),
        inbound_fee: parse_inbound_tlv(cu),
    };
    if out.fee_proportional_millionths > 1_000_000 {
        return None;
    }
    Some(out)
}

/// Scan the trailing TLV stream for bLIP-18 inbound fees (type
/// 55555): value is [i32 base][i32 prop], both signed.
fn parse_inbound_tlv(cu: &[u8]) -> Option<(i32, i32)> {
    let mut pos = 136;
    while pos < cu.len() {
        let ttype = read_bigsize(cu, &mut pos)?;
        let tlen = read_bigsize(cu, &mut pos)?;
        // Overflow-safe: pos <= cu.len() (guaranteed by
        // read_bigsize) so the subtraction cannot underflow,
        // whereas pos + tlen can wrap for an attacker-supplied
        // tlen and slip past a `> cu.len()` check.
        if tlen > (cu.len() - pos) as u64 {
            return None;
        }
        if ttype == 55555 && tlen == 8 {
            let base = read_be(cu, pos, 4) as u32 as i32;
            let prop = read_be(cu, pos + 4, 4) as u32 as i32;
            return Some((base, prop));
        }
        pos += tlen as usize;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn push_be(b: &mut Vec<u8>, v: u64, nbytes: usize) {
        for i in 0..nbytes {
            b.push((v >> (8 * (nbytes - 1 - i))) as u8);
        }
    }

    /// A 136-byte channel_update body (no type prefix),
    /// sig..timestamp zeroed.
    fn make_cu_body(
        disabled: bool,
        cltv: u16,
        htlc_min: u64,
        base: u32,
        prop: u32,
        htlc_max: u64,
    ) -> Vec<u8> {
        let mut b = vec![0u8; 110];
        b[109] = if disabled { 0x02 } else { 0x00 };
        push_be(&mut b, u64::from(cltv), 2);
        push_be(&mut b, htlc_min, 8);
        push_be(&mut b, u64::from(base), 4);
        push_be(&mut b, u64::from(prop), 4);
        push_be(&mut b, htlc_max, 8);
        assert_eq!(b.len(), 136);
        b
    }

    /// Wrap a channel_update in an onion failure payload: failcode
    /// + `header` zero bytes + 2-byte length + optional 0x0102
    /// type prefix + the update.
    fn make_onion_hex(
        failcode: u16,
        header: usize,
        cu: &[u8],
        type_prefix: bool,
    ) -> String {
        let mut m = Vec::new();
        push_be(&mut m, u64::from(failcode), 2);
        m.extend(std::iter::repeat(0u8).take(header));
        let mut full = Vec::new();
        if type_prefix {
            full.extend([0x01, 0x02]);
        }
        full.extend_from_slice(cu);
        push_be(&mut m, full.len() as u64, 2);
        m.extend(full);
        hex::encode(m)
    }

    /// Append a bLIP-18 inbound-fee TLV with the given length byte
    /// (a `len` other than 8 makes it malformed-for-us on
    /// purpose).
    fn push_inbound_tlv(cu: &mut Vec<u8>, len: u8, base: i32, prop: i32) {
        // type 55555 = 0xd903 needs the 0xfd bigsize form.
        cu.push(0xfd);
        push_be(cu, 55555, 2);
        cu.push(len);
        push_be(cu, base as u32 as u64, 4);
        push_be(cu, prop as u32 as u64, 4);
        cu.extend(std::iter::repeat(0u8).take((len as usize).saturating_sub(8)));
    }

    #[test]
    fn happy_path_100c_with_type_prefix() {
        let body = make_cu_body(false, 144, 1000, 1234, 567, 1_000_000_000);
        let cu = parse_chan_update(&make_onion_hex(0x100c, 8, &body, true))
            .unwrap();
        assert!(cu.enabled);
        assert_eq!(cu.cltv_expiry_delta, 144);
        assert_eq!(cu.htlc_minimum_msat, 1000);
        assert_eq!(cu.fee_base_msat, 1234);
        assert_eq!(cu.fee_proportional_millionths, 567);
        assert_eq!(cu.htlc_maximum_msat, 1_000_000_000);
        assert!(cu.inbound_fee.is_none());
    }

    #[test]
    fn no_type_prefix_via_100d() {
        let body = make_cu_body(true, 40, 1, 0, 100, 21_000_000);
        let cu = parse_chan_update(&make_onion_hex(0x100d, 4, &body, false))
            .unwrap();
        assert!(!cu.enabled);
        assert_eq!(cu.cltv_expiry_delta, 40);
        assert_eq!(cu.fee_proportional_millionths, 100);
    }

    #[test]
    fn absurd_proportional_fee_rejected() {
        let body = make_cu_body(false, 144, 0, 0, 1_000_001, 1);
        assert!(
            parse_chan_update(&make_onion_hex(0x100c, 8, &body, true))
                .is_none()
        );
        // Exactly 100% is still accepted.
        let body = make_cu_body(false, 144, 0, 0, 1_000_000, 1);
        assert!(
            parse_chan_update(&make_onion_hex(0x100c, 8, &body, true))
                .is_some()
        );
    }

    #[test]
    fn inbound_fee_tlv_signed_values() {
        let mut body = make_cu_body(false, 144, 0, 0, 0, 1);
        push_inbound_tlv(&mut body, 8, -1000, 250);
        let cu = parse_chan_update(&make_onion_hex(0x100b, 8, &body, true))
            .unwrap();
        assert_eq!(cu.inbound_fee, Some((-1000, 250)));
    }

    #[test]
    fn inbound_tlv_wrong_length_ignored() {
        let mut body = make_cu_body(false, 144, 0, 0, 0, 1);
        push_inbound_tlv(&mut body, 9, 1000, 250);
        let cu = parse_chan_update(&make_onion_hex(0x100c, 8, &body, true))
            .unwrap();
        assert!(cu.inbound_fee.is_none());
    }

    #[test]
    fn malicious_tlv_length_does_not_wrap() {
        let mut body = make_cu_body(false, 144, 0, 0, 0, 1);
        body.push(0xfd);
        push_be(&mut body, 55555, 2);
        body.push(0xff);
        push_be(&mut body, u64::MAX, 8);
        // 8 in-bounds bytes a wrapped bounds check would misread.
        push_be(&mut body, 0x1122334455667788, 8);
        let cu = parse_chan_update(&make_onion_hex(0x100c, 8, &body, true))
            .unwrap();
        assert!(cu.inbound_fee.is_none());
    }

    #[test]
    fn non_carrying_failcode_rejected() {
        let body = make_cu_body(false, 144, 0, 0, 0, 1);
        assert!(
            parse_chan_update(&make_onion_hex(0x2002, 0, &body, true))
                .is_none()
        );
    }

    #[test]
    fn truncated_and_garbage_rejected() {
        let body = make_cu_body(false, 144, 0, 0, 0, 1);
        let hex = make_onion_hex(0x100c, 8, &body, true);
        assert!(parse_chan_update(&hex[..hex.len() - 40]).is_none());
        assert!(parse_chan_update("zznothexzz").is_none());
        assert!(parse_chan_update("").is_none());
        // Zero-length channel_update.
        assert!(parse_chan_update("100c00000000000000000000").is_none());
    }

    #[test]
    fn classify_no_update_is_inbound() {
        assert!(matches!(
            classify_fee_insufficient(0, 1_000_000, None),
            FeeFault::Inbound
        ));
    }

    #[test]
    fn classify_covered_outbound_is_inbound() {
        let body = make_cu_body(false, 144, 0, 100, 500, 1);
        let cu = parse_chan_update(&make_onion_hex(0x100c, 8, &body, true))
            .unwrap();
        // required = 100 + 500 * 1_000_000 / 1_000_000 = 600.
        assert!(matches!(
            classify_fee_insufficient(600, 1_000_000, Some(&cu)),
            FeeFault::Inbound
        ));
        assert!(matches!(
            classify_fee_insufficient(599, 1_000_000, Some(&cu)),
            FeeFault::StaleOutbound
        ));
    }
}
