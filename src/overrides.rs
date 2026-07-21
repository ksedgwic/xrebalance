//! Learned gossip overrides, re-applied to each request's layer.
//!
//! askrene-age heals only constraints, so policy refreshes and node
//! disables written into the persistent layer would accumulate
//! there forever.  They live here instead -- in memory, timestamped
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
}

impl Overrides {
    pub fn new(max_age: u64) -> Self {
        Overrides {
            max_age,
            policies: HashMap::new(),
            disabled_nodes: HashMap::new(),
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

    /// Prune expired entries and return the survivors for
    /// application to a request layer.
    pub fn snapshot(&mut self, now: u64) -> (Vec<(String, ChanUpdate)>, Vec<String>) {
        let max_age = self.max_age;
        self.policies
            .retain(|_, (_, at)| now.saturating_sub(*at) <= max_age);
        self.disabled_nodes
            .retain(|_, at| now.saturating_sub(*at) <= max_age);
        (
            self.policies
                .iter()
                .map(|(scidd, (cu, _))| (scidd.clone(), cu.clone()))
                .collect(),
            self.disabled_nodes.keys().cloned().collect(),
        )
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
        let (policies, nodes) = o.snapshot(1150);
        assert_eq!(
            policies.iter().map(|(s, _)| s.as_str()).collect::<Vec<_>>(),
            vec!["2x2x2/1"]
        );
        assert_eq!(nodes, vec!["02bb"]);
        // The prune is durable, not merely a filtered view.
        assert!(o.policies.len() == 1 && o.disabled_nodes.len() == 1);
    }

    #[test]
    fn set_max_age_applies_to_next_snapshot() {
        let mut o = Overrides::new(1000);
        o.record_policy("1x1x1/0", cu(10), 100);
        assert_eq!(o.snapshot(600).0.len(), 1);
        o.set_max_age(100);
        assert!(o.snapshot(600).0.is_empty());
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
