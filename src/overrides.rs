//! Learned gossip overrides, re-applied to each request's layer.
//!
//! askrene-age heals only constraints, so policy refreshes and node
//! disables written into the persistent layer would accumulate
//! there forever.  Channel exclusions could age there, but on the
//! wrong clock: constraint-age is liquidity-scale (hours), while an
//! exclusion asserts a policy-scale fact gossip corrects in
//! minutes.  All three live here instead -- in memory, timestamped
//! -- and each request writes the still-young entries into its own
//! split layer, which dies with the request.  A restart loses the
//! store; the next failed attempt re-learns it.

use crate::onion_error::ChanUpdate;
use std::collections::HashMap;

pub struct Overrides {
    max_age: u64,
    /// scidd -> (advertised policy, stored_at).
    policies: HashMap<String, (ChanUpdate, u64)>,
    /// node id -> stored_at.
    disabled_nodes: HashMap<String, u64>,
    /// scidd -> stored_at.
    exclusions: HashMap<String, u64>,
}

/// The still-young overrides, pruned and cloned for application to
/// one request's split layer (plan.rs).
pub struct Snapshot {
    /// Applied as askrene-update-channel.
    pub policies: Vec<(String, ChanUpdate)>,
    /// Applied as askrene-disable-node.
    pub disabled_nodes: Vec<String>,
    /// Applied as askrene-inform-channel constrained at 1msat.
    pub exclusions: Vec<String>,
}

impl Overrides {
    pub fn new(max_age: u64) -> Self {
        Overrides {
            max_age,
            policies: HashMap::new(),
            disabled_nodes: HashMap::new(),
            exclusions: HashMap::new(),
        }
    }

    /// Track a changed expiry (dynamic option); applies from the
    /// next snapshot or repeat check.
    pub fn set_max_age(&mut self, max_age: u64) {
        self.max_age = max_age;
    }

    fn young(&self, stored_at: u64, now: u64) -> bool {
        now.saturating_sub(stored_at) <= self.max_age
    }

    /// Store the policy a forwarder returned for a channel
    /// direction.
    pub fn record_policy(&mut self, scidd: &str, cu: ChanUpdate, now: u64) {
        self.policies.insert(scidd.to_owned(), (cu, now));
    }

    /// True when the freshly returned update matches the policy we
    /// already hold for this direction: the forwarder's enforcement
    /// diverges from what it signs, and another refresh would
    /// change nothing.
    pub fn is_repeat(&self, scidd: &str, cu: &ChanUpdate, now: u64) -> bool {
        self.policies
            .get(scidd)
            .is_some_and(|(stored, at)| {
                self.young(*at, now) && stored.same_policy(cu)
            })
    }

    /// Take the erring forwarder out of consideration for a while.
    pub fn record_disabled_node(&mut self, node: &str, now: u64) {
        self.disabled_nodes.insert(node.to_owned(), now);
    }

    /// Take a channel direction out of consideration for a while.
    pub fn record_exclusion(&mut self, scidd: &str, now: u64) {
        self.exclusions.insert(scidd.to_owned(), now);
    }

    /// The current expiry window (dynamic option), for stats.
    pub fn max_age(&self) -> u64 {
        self.max_age
    }

    /// Entry counts (policies, node disables, exclusions) as
    /// stored; expired entries linger until the next snapshot
    /// prunes them.
    pub fn counts(&self) -> (usize, usize, usize) {
        (
            self.policies.len(),
            self.disabled_nodes.len(),
            self.exclusions.len(),
        )
    }

    /// Prune expired entries and return the survivors for
    /// application to a request layer.
    pub fn snapshot(&mut self, now: u64) -> Snapshot {
        let max_age = self.max_age;
        self.policies
            .retain(|_, (_, at)| now.saturating_sub(*at) <= max_age);
        self.disabled_nodes
            .retain(|_, at| now.saturating_sub(*at) <= max_age);
        self.exclusions
            .retain(|_, at| now.saturating_sub(*at) <= max_age);
        Snapshot {
            policies: self
                .policies
                .iter()
                .map(|(scidd, (cu, _))| (scidd.clone(), cu.clone()))
                .collect(),
            disabled_nodes: self.disabled_nodes.keys().cloned().collect(),
            exclusions: self.exclusions.keys().cloned().collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cu(prop: u32) -> ChanUpdate {
        ChanUpdate {
            enabled: true,
            cltv_expiry_delta: 144,
            htlc_minimum_msat: 1000,
            fee_base_msat: 0,
            fee_proportional_millionths: prop,
            htlc_maximum_msat: 1_000_000,
            inbound_fee: None,
        }
    }

    #[test]
    fn snapshot_returns_young_prunes_expired() {
        let mut o = Overrides::new(100);
        o.record_policy("1x1x1/0", cu(10), 1000);
        o.record_policy("2x2x2/1", cu(20), 1090);
        o.record_disabled_node("02aa", 1000);
        o.record_disabled_node("02bb", 1150);
        o.record_exclusion("3x3x3/0", 1000);
        o.record_exclusion("4x4x4/1", 1100);
        let snap = o.snapshot(1150);
        assert_eq!(
            snap.policies
                .iter()
                .map(|(s, _)| s.as_str())
                .collect::<Vec<_>>(),
            vec!["2x2x2/1"]
        );
        assert_eq!(snap.disabled_nodes, vec!["02bb"]);
        assert_eq!(snap.exclusions, vec!["4x4x4/1"]);
        // The prune is durable, not merely a filtered view.
        assert_eq!(o.counts(), (1, 1, 1));
    }

    #[test]
    fn set_max_age_applies_to_next_snapshot() {
        let mut o = Overrides::new(1000);
        o.record_policy("1x1x1/0", cu(10), 100);
        o.record_exclusion("2x2x2/1", 100);
        let snap = o.snapshot(600);
        assert!(snap.policies.len() == 1 && snap.exclusions.len() == 1);
        o.set_max_age(100);
        let snap = o.snapshot(600);
        assert!(snap.policies.is_empty() && snap.exclusions.is_empty());
    }

    #[test]
    fn is_repeat_matches_young_identical_policy() {
        let mut o = Overrides::new(100);
        o.record_policy("1x1x1/0", cu(10), 1000);
        assert!(o.is_repeat("1x1x1/0", &cu(10), 1050));
        // Different policy: a genuine refresh, not a repeat.
        assert!(!o.is_repeat("1x1x1/0", &cu(11), 1050));
        // Nothing stored for this direction.
        assert!(!o.is_repeat("9x9x9/0", &cu(10), 1050));
        // Stored entry expired: it was not applied to the failing
        // attempt, so an identical return is not evidence of
        // divergence.
        assert!(!o.is_repeat("1x1x1/0", &cu(10), 1101));
    }

    #[test]
    fn is_repeat_ignores_inbound_fee_difference() {
        let mut o = Overrides::new(100);
        o.record_policy("1x1x1/0", cu(10), 1000);
        let mut with_inbound = cu(10);
        with_inbound.inbound_fee = Some((5000, 0));
        assert!(o.is_repeat("1x1x1/0", &with_inbound, 1050));
    }
}
