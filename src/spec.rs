//! Channel specs: the elements of `sources` and `destinations`.
//!
//! Each element names one of our channels and may cap how much this
//! request moves through it -- at most `max_msat` drawn from a
//! source (fees included: the cap bounds what crosses the channel),
//! at most `max_msat` delivered into a destination.  A cap bounds
//! the whole request: the round loop (main.rs) draws it down by
//! what earlier rounds committed through the channel, so partial
//! deliveries never add up past it.  Within a round the effective
//! bound is the smaller of the remaining cap and the channel's live
//! liquidity (plan.rs asserts the cap as an askrene constraint;
//! constraint intersection does the min).
//!
//! Two spellings parse to the same thing:
//!
//!   "845123x1x0"                            no cap
//!   "845123x1x0:250000"                     cap, msat
//!   "845123x1x0:250sat"                     cap, sat/msat suffixes
//!   {"scid": "845123x1x0", "max_msat": N}   object form
//!
//! The string form is for humans at a CLI; programs composing JSON
//! anyway (CLBOSS) should prefer the object form.

use serde::{Deserialize, Deserializer};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChanSpec {
    pub scid: String,
    pub max_msat: Option<u64>,
}

impl ChanSpec {
    /// The channel's usable liquidity under this spec's cap.
    pub fn capped(&self, avail_msat: u64) -> u64 {
        avail_msat.min(self.max_msat.unwrap_or(u64::MAX))
    }

    fn from_str_form(s: &str) -> Result<Self, String> {
        let Some((scid, limit)) = s.split_once(':') else {
            return Ok(ChanSpec {
                scid: s.to_owned(),
                max_msat: None,
            });
        };
        if scid.is_empty() {
            return Err(format!("\"{s}\": missing scid before ':'"));
        }
        if limit.is_empty() {
            return Err(format!("\"{s}\": missing limit after ':'"));
        }
        if limit.contains(':') {
            return Err(format!("\"{s}\": more than one ':'"));
        }
        Ok(ChanSpec {
            scid: scid.to_owned(),
            max_msat: Some(parse_limit(limit)?),
        })
    }

    fn from_value(v: &Value) -> Result<Self, String> {
        match v {
            Value::String(s) => Self::from_str_form(s),
            Value::Object(m) => {
                for k in m.keys() {
                    if k != "scid" && k != "max_msat" {
                        return Err(format!("unknown field \"{k}\" (expected scid, max_msat)"));
                    }
                }
                let scid = m
                    .get("scid")
                    .and_then(Value::as_str)
                    .ok_or("channel object needs a \"scid\" string")?
                    .to_owned();
                let max_msat = match m.get("max_msat") {
                    None | Some(Value::Null) => None,
                    Some(x) => Some(x.as_u64().ok_or_else(|| {
                        format!("{scid}: max_msat must be a non-negative integer")
                    })?),
                };
                Ok(ChanSpec { scid, max_msat })
            }
            _ => Err("each channel is a string \"scid[:limit]\" or an \
                      object {\"scid\": ..., \"max_msat\": ...}"
                .into()),
        }
    }
}

/// A limit from the string form: plain digits are msat; `msat` and
/// `sat` suffixes are accepted.
fn parse_limit(text: &str) -> Result<u64, String> {
    // Strip `msat` before `sat`: "msat" ends with "sat".
    let (digits, mult) = if let Some(d) = text.strip_suffix("msat") {
        (d, 1)
    } else if let Some(d) = text.strip_suffix("sat") {
        (d, 1000)
    } else {
        (text, 1)
    };
    let n: u64 = digits
        .parse()
        .map_err(|_| format!("invalid limit \"{text}\""))?;
    n.checked_mul(mult)
        .ok_or_else(|| format!("limit \"{text}\" overflows msat"))
}

impl<'de> Deserialize<'de> for ChanSpec {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let v = Value::deserialize(d)?;
        ChanSpec::from_value(&v).map_err(serde::de::Error::custom)
    }
}

/// The first scid appearing twice, if any.  Duplicates would make
/// the per-scid caps ambiguous, so the caller rejects them.
pub fn find_duplicate(specs: &[ChanSpec]) -> Option<&str> {
    let mut seen = std::collections::HashSet::new();
    specs
        .iter()
        .find(|s| !seen.insert(s.scid.as_str()))
        .map(|s| s.scid.as_str())
}

/// askrene routes an amount at or below this through one path
/// carrying all of it (plugins/askrene/child/mcf.c,
/// SINGLE_PATH_THRESHOLD); above it the solver works in flow units.
pub const SINGLE_PATH_THRESHOLD_MSAT: u64 = 1_000_000;

/// The flow unit askrene's multi-path solver quantizes an amount
/// into: a thousandth of it, rounded up (mcf.c, accuracy).  None at
/// or below the single-path threshold, where one flow carries the
/// whole amount and nothing is quantized.
pub fn flow_unit_msat(amount_msat: u64) -> Option<u64> {
    (amount_msat > SINGLE_PATH_THRESHOLD_MSAT).then(|| amount_msat.div_ceil(1000))
}

/// A source bound lowered to leave the solver one flow unit of
/// slack under it: the largest value at or below `bound_msat` that
/// is one msat short of a multiple of the unit.
///
/// The solver sees a source as exactly known (auto.localchans
/// asserts our spendable as min = max; a lower max written by us
/// wins, mcf.c "assume min is wrong") and packs floor(bound / unit)
/// units through it at no probability cost.  Its refine stage then
/// reserves, per part, the delivered amount plus downstream fees
/// plus a commitment-fee extra on channels we opened (172 weight
/// units at the channel's feerate), against the same bound.  An
/// arbitrary bound leaves refine only its remainder modulo the unit,
/// and a part that does not fit is logged as BROKEN on every pass
/// while the solve repeats; a bound one msat short of a unit
/// multiple stops the packing a whole unit under it, and that unit
/// takes the extras.  The slack is one unit at most: parts whose
/// extras sum past a unit still overshoot.
///
/// At or below the single-path threshold the bound is returned as
/// is: a single flow is checked without its own extra.
pub fn bound_with_unit_slack(bound_msat: u64, amount_msat: u64) -> u64 {
    match flow_unit_msat(amount_msat) {
        None => bound_msat,
        Some(unit) => (bound_msat.saturating_add(1) / unit * unit).saturating_sub(1),
    }
}

/// What flow-unit rounding can cost the source side when the summed
/// source bounds clamp the ask.  The solver draws at most floor(bound
/// / unit) units from a source, under one unit short of the bound,
/// and `bound_with_unit_slack` lowers the bound by under one more;
/// the ask itself rounds up to whole units.  An ask at least 2n + 1
/// units under the n summed bounds is feasible by local arithmetic;
/// one clamped to the bare sum is not, and fails its top rung.  Zero
/// at or below the single-path threshold.
pub fn unit_headroom_msat(sources: usize, amount_msat: u64) -> u64 {
    flow_unit_msat(amount_msat).map_or(0, |unit| unit.saturating_mul(2 * sources as u64 + 1))
}

#[cfg(test)]
mod tests {
    use super::{
        bound_with_unit_slack, find_duplicate, flow_unit_msat, unit_headroom_msat, ChanSpec,
    };

    fn parse(json: &str) -> Result<ChanSpec, String> {
        serde_json::from_str::<ChanSpec>(json).map_err(|e| e.to_string())
    }

    fn spec(scid: &str, max_msat: Option<u64>) -> ChanSpec {
        ChanSpec {
            scid: scid.to_owned(),
            max_msat,
        }
    }

    #[test]
    fn bare_scid_has_no_cap() {
        assert_eq!(parse(r#""845123x1x0""#), Ok(spec("845123x1x0", None)));
    }

    #[test]
    fn colon_limit_is_msat() {
        assert_eq!(
            parse(r#""845123x1x0:250000""#),
            Ok(spec("845123x1x0", Some(250_000)))
        );
    }

    #[test]
    fn msat_and_sat_suffixes() {
        assert_eq!(
            parse(r#""845123x1x0:250000msat""#),
            Ok(spec("845123x1x0", Some(250_000)))
        );
        assert_eq!(
            parse(r#""845123x1x0:250sat""#),
            Ok(spec("845123x1x0", Some(250_000)))
        );
    }

    #[test]
    fn zero_cap_is_allowed() {
        // An explicit 0 excludes the channel from this request;
        // mechanical callers produce it at band edges.
        assert_eq!(parse(r#""845123x1x0:0""#), Ok(spec("845123x1x0", Some(0))));
    }

    #[test]
    fn object_form() {
        assert_eq!(
            parse(r#"{"scid": "845123x1x0", "max_msat": 250000}"#),
            Ok(spec("845123x1x0", Some(250_000)))
        );
        assert_eq!(
            parse(r#"{"scid": "845123x1x0"}"#),
            Ok(spec("845123x1x0", None))
        );
    }

    #[test]
    fn bad_strings_are_rejected() {
        for bad in [
            r#""845123x1x0:""#,        // empty limit
            r#"":100000""#,            // empty scid
            r#""845123x1x0:1:2""#,     // two colons
            r#""845123x1x0:12.5sat""#, // fractional
            r#""845123x1x0:-1""#,      // negative
            r#""845123x1x0:sat""#,     // no digits
            r#""845123x1x0:99btc""#,   // unsupported unit
        ] {
            assert!(parse(bad).is_err(), "accepted {bad}");
        }
    }

    #[test]
    fn bad_objects_are_rejected() {
        for bad in [
            r#"{"max_msat": 1}"#,                          // no scid
            r#"{"scid": "845123x1x0", "max_sat": 1}"#,     // typo field
            r#"{"scid": "845123x1x0", "max_msat": -5}"#,   // negative
            r#"{"scid": "845123x1x0", "max_msat": "1x"}"#, // non-numeric
            r#"42"#,                                       // wrong type
        ] {
            assert!(parse(bad).is_err(), "accepted {bad}");
        }
    }

    #[test]
    fn overflow_limits_are_rejected() {
        assert!(parse(r#""845123x1x0:18446744073709551615sat""#).is_err());
        assert_eq!(
            parse(r#""845123x1x0:18446744073709551615msat""#),
            Ok(spec("845123x1x0", Some(u64::MAX)))
        );
    }

    #[test]
    fn capped_takes_the_smaller() {
        assert_eq!(spec("s", None).capped(7), 7);
        assert_eq!(spec("s", Some(5)).capped(7), 5);
        assert_eq!(spec("s", Some(9)).capped(7), 7);
        assert_eq!(spec("s", Some(0)).capped(7), 0);
    }

    #[test]
    fn flow_unit_is_a_thousandth_rounded_up_above_single_path() {
        assert_eq!(flow_unit_msat(1_000_000), None);
        assert_eq!(flow_unit_msat(1_000_001), Some(1_001));
        assert_eq!(flow_unit_msat(2_395_694_000), Some(2_395_694));
        assert_eq!(flow_unit_msat(2_395_694_001), Some(2_395_695));
    }

    #[test]
    fn unit_slack_bound_is_one_short_of_a_unit_multiple() {
        // A 500 M cap under a 2.4 G ask (unit 2.4 M): the solver
        // packs 207 units, and refine has 2_399_999 msat of room.
        let w = bound_with_unit_slack(500_000_000, 2_400_000_000);
        assert_eq!(w, 499_199_999);
        assert_eq!(w / 2_400_000, 207);
        assert_eq!(w % 2_400_000, 2_399_999);
        // Never above the bound, never a whole unit under it.
        for bound in [
            0u64,
            1,
            2_399_998,
            2_399_999,
            2_400_000,
            2_400_001,
            499_999_999,
            500_000_000,
            500_000_001,
        ] {
            let w = bound_with_unit_slack(bound, 2_400_000_000);
            assert!(w <= bound, "{bound}");
            assert!(bound - w < 2_400_000, "{bound}");
            assert!(w == 0 || (w + 1) % 2_400_000 == 0, "{bound}");
        }
        // A bound under one unit cannot pass a single unit: excluded.
        assert_eq!(bound_with_unit_slack(2_399_998, 2_400_000_000), 0);
        // At or below the single-path threshold nothing changes.
        assert_eq!(bound_with_unit_slack(60_000, 55_000), 60_000);
        assert_eq!(bound_with_unit_slack(60_000, 1_000_000), 60_000);
    }

    #[test]
    fn unit_slack_bound_only_tightens_down_the_ladder() {
        // Rewritten per rung from the previous rung's value, the
        // bound never rises (a looser layer constraint is ignored)
        // and stays aligned to each rung's unit.
        let mut w = 500_000_000u64;
        for rung in [
            2_400_000_000u64,
            1_800_000_000,
            1_200_000_000,
            600_000_000,
            300_000_000,
        ] {
            let next = bound_with_unit_slack(w, rung);
            assert!(next <= w, "{rung}");
            assert_eq!((next + 1) % flow_unit_msat(rung).unwrap(), 0, "{rung}");
            w = next;
        }
    }

    #[test]
    fn unit_headroom_makes_the_clamped_ask_feasible_in_units() {
        assert_eq!(unit_headroom_msat(19, 1_000_000), 0);
        assert_eq!(unit_headroom_msat(19, 2_400_000_000), 39 * 2_400_000);
        // Sources whose summed bounds clamp the ask: after the
        // headroom, the units the solver can draw cover the ask's
        // units, whatever the bounds' remainders.
        for bounds in [
            vec![500_000_000u64; 5],
            vec![499_999_999, 2_400_001, 123_456_789, 500_000_000, 7_654_321],
            vec![2_000_000_001, 1],
            vec![1_234_567; 30],
        ] {
            let sum: u64 = bounds.iter().sum();
            let ask = sum - unit_headroom_msat(bounds.len(), sum);
            let unit = flow_unit_msat(ask).unwrap();
            let drawable: u64 = bounds
                .iter()
                .map(|b| bound_with_unit_slack(*b, ask) / unit)
                .sum();
            assert!(drawable >= ask.div_ceil(unit), "{bounds:?}");
        }
    }

    #[test]
    fn duplicates_are_found() {
        let specs = [spec("a", None), spec("b", Some(1)), spec("a", Some(2))];
        assert_eq!(find_duplicate(&specs), Some("a"));
        assert_eq!(find_duplicate(&specs[..2]), None);
        assert_eq!(find_duplicate(&[]), None);
    }
}
