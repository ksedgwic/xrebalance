//! Coalesce redundant askrene-inform-channel writes.
//!
//! Feedback keeps re-learning the same facts: successive parts
//! crossing the same channel-dir report the same bound over and
//! over.  Keep the tightest bound emitted per (scid-dir, kind)
//! within one time bucket and emit only on a new bucket or a
//! tightening.  Dropping a dominated write is lossless: askrene
//! folds a direction's constraints down to one tightest [min,max],
//! so the dropped entry would not have changed any route.
//!
//! The bucket length is a fixed fraction (1/12) of the constraint
//! aging window (xrebalance-constraint-age), so the once-per-bucket
//! keep-alive re-emit refreshes a still-observed constraint well
//! before it can age out, whatever the configured aging value.

use std::collections::HashMap;

const WINDOW_DIVISOR: u64 = 12;

/// The tightest bound accepted by askrene for one key in its most
/// recent bucket.
pub struct InformObs {
    pub bucket: u64,
    pub tightest_msat: u64,
}

/// The coalescing decision: emit on the first observation for a
/// key, on a bucket change (the keep-alive against layer aging), or
/// when the observation tightens the recorded bound.  A lower bound
/// (unconstrained: the channel passed this much) tightens upward;
/// an upper bound (constrained: it could not) tightens downward.
pub fn should_emit(
    prior: Option<&InformObs>,
    bucket: u64,
    amount_msat: u64,
    is_lower_bound: bool,
) -> bool {
    let Some(prior) = prior else {
        return true;
    };
    if prior.bucket != bucket {
        return true;
    }
    if is_lower_bound {
        amount_msat > prior.tightest_msat
    } else {
        amount_msat < prior.tightest_msat
    }
}

pub struct Coalescer {
    bucket_secs: u64,
    cache: HashMap<String, InformObs>,
    emits: u64,
}

impl Coalescer {
    /// `aging_secs` is the constraint aging window; buckets are
    /// aging/12, floored at 1s so a pathological aging value cannot
    /// produce zero-length buckets.
    pub fn new(aging_secs: u64) -> Self {
        Coalescer {
            bucket_secs: (aging_secs / WINDOW_DIVISOR).max(1),
            cache: HashMap::new(),
            emits: 0,
        }
    }

    /// Track a changed aging window (dynamic option).  Existing
    /// cache entries keep bucket numbers from the old width; the
    /// next check sees a different bucket and re-emits, which is
    /// the safe direction.
    pub fn set_aging(&mut self, aging_secs: u64) {
        self.bucket_secs = (aging_secs / WINDOW_DIVISOR).max(1);
    }

    /// Decide whether a write for `key` goes out now.  Returns the
    /// current bucket, to hand to `record` once askrene accepts the
    /// write, or None to suppress.
    pub fn check(
        &mut self,
        key: &str,
        now_secs: u64,
        amount_msat: u64,
        is_lower_bound: bool,
    ) -> Option<u64> {
        let bucket = now_secs / self.bucket_secs;
        if !should_emit(self.cache.get(key), bucket, amount_msat, is_lower_bound)
        {
            return None;
        }
        self.emits += 1;
        if self.emits & 0xFFF == 0 {
            self.prune(bucket);
        }
        Some(bucket)
    }

    /// Record a write askrene accepted.  Recording at `check` time
    /// instead would let a rejected write leave the cache claiming
    /// the bound was written, suppressing equal-or-looser rewrites
    /// for the rest of the bucket while the layer never learned it.
    /// Two racing parts may both pass `check` and write twice;
    /// askrene folds the duplicate, which is the safe side.
    pub fn record(&mut self, key: &str, bucket: u64, amount_msat: u64) {
        self.cache.insert(
            key.to_owned(),
            InformObs {
                bucket,
                tightest_msat: amount_msat,
            },
        );
    }

    /// Drop entries idle past the aging window, so the cache does
    /// not grow with the set of channel-dirs seen over the process
    /// lifetime.  Amortised: swept once per 4096 emits.
    fn prune(&mut self, now_bucket: u64) {
        // The aging window is WINDOW_DIVISOR buckets; a few more
        // covers any still-active entry.
        const KEEP_BUCKETS: u64 = WINDOW_DIVISOR + 4;
        self.cache
            .retain(|_, obs| obs.bucket + KEEP_BUCKETS >= now_bucket);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_observation_emits() {
        assert!(should_emit(None, 5, 1000, true));
        assert!(should_emit(None, 5, 1000, false));
    }

    #[test]
    fn bucket_change_emits() {
        let prior = InformObs {
            bucket: 5,
            tightest_msat: 1000,
        };
        assert!(should_emit(Some(&prior), 6, 1000, true));
        // A clock stepping backwards still lands on != bucket.
        assert!(should_emit(Some(&prior), 4, 1000, false));
    }

    #[test]
    fn same_bucket_lower_bound_tightens_upward() {
        let prior = InformObs {
            bucket: 5,
            tightest_msat: 1000,
        };
        assert!(should_emit(Some(&prior), 5, 1001, true));
        assert!(!should_emit(Some(&prior), 5, 1000, true));
        assert!(!should_emit(Some(&prior), 5, 999, true));
    }

    #[test]
    fn same_bucket_upper_bound_tightens_downward() {
        let prior = InformObs {
            bucket: 5,
            tightest_msat: 1000,
        };
        assert!(should_emit(Some(&prior), 5, 999, false));
        assert!(!should_emit(Some(&prior), 5, 1000, false));
        assert!(!should_emit(Some(&prior), 5, 1001, false));
    }

    #[test]
    fn unrecorded_check_does_not_suppress() {
        let mut co = Coalescer::new(6 * 60 * 60);
        assert!(co.check("k", 100, 1000, true).is_some());
        // The write was rejected (no record): the retry still emits.
        let bucket = co.check("k", 100, 1000, true).unwrap();
        co.record("k", bucket, 1000);
        assert!(co.check("k", 100, 1000, true).is_none());
        assert!(co.check("k", 100, 1001, true).is_some());
    }

    #[test]
    fn keep_alive_re_emits_each_bucket() {
        let mut co = Coalescer::new(12); // 1s buckets
        let b = co.check("k", 10, 1000, true).unwrap();
        co.record("k", b, 1000);
        assert!(co.check("k", 10, 1000, true).is_none());
        assert!(co.check("k", 11, 1000, true).is_some());
    }

    #[test]
    fn set_aging_changes_bucket_width() {
        let mut co = Coalescer::new(12); // 1s buckets
        let b = co.check("k", 100, 1000, true).unwrap();
        co.record("k", b, 1000);
        assert!(co.check("k", 100, 1000, true).is_none());
        // The same instant lands in a different bucket number under
        // the new width, so the next check re-emits.
        co.set_aging(1200); // 100s buckets
        assert!(co.check("k", 100, 1000, true).is_some());
    }

    #[test]
    fn prune_drops_idle_entries() {
        let mut co = Coalescer::new(12); // 1s buckets
        let b = co.check("old", 0, 1000, true).unwrap();
        co.record("old", b, 1000);
        // Drive the amortised sweep (every 4096 emits) with fresh
        // keys well past the keep window.
        for i in 0..0x1000u64 {
            let key = format!("k{i}");
            if let Some(bk) = co.check(&key, 1000 + i, 1, true) {
                co.record(&key, bk, 1);
            }
        }
        assert!(!co.cache.contains_key("old"));
    }
}
