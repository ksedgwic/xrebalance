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

#[cfg(test)]
mod tests {
    use super::{find_duplicate, ChanSpec};

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
    fn duplicates_are_found() {
        let specs = [spec("a", None), spec("b", Some(1)), spec("a", Some(2))];
        assert_eq!(find_duplicate(&specs), Some("a"));
        assert_eq!(find_duplicate(&specs[..2]), None);
        assert_eq!(find_duplicate(&[]), None);
    }
}
