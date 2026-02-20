#[cfg(test)]
pub mod strategies {
    use proptest::prelude::*;

    /// Arbitrary (low, high) watermarks where low <= high
    pub fn arb_watermarks() -> impl Strategy<Value = (i64, i64)> {
        (0..i64::MAX / 2).prop_flat_map(|low| (Just(low), low..=i64::MAX / 2))
    }

    /// (low, high, committed) triple — committed can be anywhere including
    /// below low (data loss) or above high (race condition)
    pub fn arb_watermarks_and_committed() -> impl Strategy<Value = (i64, i64, i64)> {
        arb_watermarks().prop_flat_map(|(low, high)| (Just(low), Just(high), 0..=high + 100))
    }

    /// Arbitrary non-empty cluster name (valid Prometheus label chars)
    pub fn arb_cluster_name() -> impl Strategy<Value = String> {
        "[a-zA-Z][a-zA-Z0-9_-]{0,30}"
    }

    /// Arbitrary non-empty group ID
    pub fn arb_group_id() -> impl Strategy<Value = String> {
        "[a-zA-Z][a-zA-Z0-9._-]{0,48}"
    }
}
