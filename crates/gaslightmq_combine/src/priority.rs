/// Where a lie stands in the priority order of the call: the position of its rule in that order.
///
/// The caller provides the order: the live set first, then the active scenario's mode.
/// Position 0 is the highest priority, and it wins every conflict.
/// Priority only decides between lies that conflict, and lies that do not conflict all apply.
///
/// A priority is a position, so the lies of the request pass and of the response pass order among each other.
/// Two lies with the same priority keep the order in which they came in,
/// with the lies of the request pass first.
///
/// ```
/// use gaslightmq_combine::prelude::*;
///
/// // The snapshot of armed rules is in priority order, so the position of a rule is its priority.
/// let rules = ["Blackhole", "Refuse the 3rd Produce"];
/// let priorities: Vec<Priority> = (0..rules.len()).map(Priority::new).collect();
///
/// assert_eq!(priorities[0], Priority::HIGHEST);
/// assert!(priorities[0].outranks(priorities[1]));
/// assert!(!priorities[1].outranks(priorities[1]));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Priority(usize);

impl Priority {
    /// The first position of the order, which wins every conflict.
    pub const HIGHEST: Self = Self(0);

    /// The priority of that position in the order, where 0 is the highest.
    pub const fn new(position: usize) -> Self {
        Self(position)
    }

    /// The position in the order, where 0 is the highest.
    pub const fn position(self) -> usize {
        self.0
    }

    /// Whether a lie of this priority wins a conflict with a lie of the `other`.
    pub const fn outranks(self, other: Self) -> bool {
        self.0 < other.0
    }
}

/// The priority of that position in the order.
impl From<usize> for Priority {
    fn from(position: usize) -> Self {
        Self::new(position)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_first_position_is_the_highest_priority() {
        assert_eq!(Priority::new(0), Priority::HIGHEST);
        assert_eq!(Priority::HIGHEST.position(), 0);
        assert_eq!(Priority::from(7).position(), 7);
    }

    #[test]
    fn an_earlier_position_outranks_a_later_one() {
        let (first, second) = (Priority::new(0), Priority::new(1));

        assert!(first.outranks(second));
        assert!(!second.outranks(first));
        assert!(!first.outranks(first));
    }
}
