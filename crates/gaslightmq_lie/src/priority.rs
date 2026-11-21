use std::fmt;

/// Where a lie stands when lies conflict: the position of its rule among the armed rules, counted from zero.
///
/// The first rule has the highest priority, and it wins a conflict with every rule after it.
/// Priorities sort in that order, so the smallest one is the highest.
/// The order comes from outside: the live set first, then the rules of the active scenario as its mode orders them.
///
/// Priority only decides between lies that conflict, and lies that do not conflict all apply.
/// Both evaluation passes share one order.
/// A lie of the request pass and a lie of the response pass that meet after the response compare by priority too.
///
/// ```
/// use gaslightmq_lie::prelude::*;
///
/// let first = Priority::HIGHEST;
/// let second = Priority::new(1);
///
/// assert!(first.outranks(second));
/// assert!(!second.outranks(second));
/// assert!(first < second);
/// assert_eq!(second.to_string(), "#2");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Priority(usize);

impl Priority {
    /// The priority of the first rule, which wins every conflict.
    pub const HIGHEST: Self = Self(0);

    /// The priority of the rule at that position, counted from zero.
    pub const fn new(position: usize) -> Self {
        Self(position)
    }

    /// The position among the armed rules, counted from zero.
    pub const fn position(self) -> usize {
        self.0
    }

    /// Whether a lie of this priority wins a conflict with a lie of the `other` one.
    pub const fn outranks(self, other: Self) -> bool {
        self.0 < other.0
    }
}

/// The priority of the rule at that position.
impl From<usize> for Priority {
    fn from(position: usize) -> Self {
        Self::new(position)
    }
}

/// Writes the rank as people count it: `#1` for the highest priority.
impl fmt::Display for Priority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{}", self.0.saturating_add(1))
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
        assert!(first < second);
    }

    #[test]
    fn a_priority_writes_its_rank_as_people_count_it() {
        assert_eq!(Priority::HIGHEST.to_string(), "#1");
        assert_eq!(Priority::new(1).to_string(), "#2");
        assert_eq!(Priority::new(usize::MAX).to_string(), format!("#{}", usize::MAX));
    }
}
