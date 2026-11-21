//! Budgets: a number of matches or firings that many sessions spend together, and never overspend.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::Count;

/// A number of matches or firings that all sessions share.
///
/// A budget is a handle to atomic numbers.
/// Cloning it gives a second handle to the same budget, so the engine keeps one and every session takes from it.
///
/// Taking from a budget has two steps, because a call only learns later whether its rule's action applied.
///
/// 1. [`reserve`](Budget::reserve) sets one unit aside for a call, and fails when no unit is free.
/// 2. The call settles its [`Reservation`]:
///    [`confirm`](Reservation::confirm) spends the unit for good,
///    and [`withdraw`](Reservation::withdraw) makes it free again.
///
/// The units that are spent and the units that are set aside never add up to more than the limit.
/// A budget of three firings therefore never yields a fourth, however many connections match at the same time.
/// While a unit is set aside, another call can find the budget empty, even when that unit comes free again later.
/// The budget is only [spent](Budget::is_spent) when calls confirmed every unit.
///
/// ```
/// use gaslightmq_trigger::prelude::*;
///
/// let budget = Budget::new(Count::new(2)?);
/// let first = budget.reserve().expect("two units are free");
/// let second = budget.reserve().expect("one unit is free");
/// assert!(budget.reserve().is_none());
///
/// // A rule with a higher priority shadowed the second call, so its unit comes free.
/// second.withdraw();
/// assert!(!first.confirm().is_last());
/// assert_eq!(budget.left(), 1);
///
/// // The next call takes it, and that spends the budget.
/// assert!(budget.take().expect("one unit is free").is_last());
/// assert!(budget.is_spent());
/// # Ok::<(), TriggerError>(())
/// ```
#[derive(Clone, Debug)]
pub struct Budget {
    limit: Count,
    used: Arc<Used>,
}

#[derive(Debug, Default)]
struct Used {
    /// The units that calls confirmed, plus the units that are set aside. It never passes the limit of the handle
    /// that took the unit.
    taken: AtomicU64,
    /// The units that calls confirmed.
    spent: AtomicU64,
}

impl Budget {
    /// A budget of that many units, all free.
    pub fn new(limit: Count) -> Budget {
        Budget {
            limit,
            used: Arc::default(),
        }
    }

    /// How many units the budget holds.
    pub const fn limit(&self) -> Count {
        self.limit
    }

    /// A handle to the same budget with another limit.
    ///
    /// The units that are spent and set aside stay as they are, on both handles.
    /// A parameter change on an armed rule uses this, because it resets nothing.
    #[must_use]
    pub fn with_limit(&self, limit: Count) -> Budget {
        Budget {
            limit,
            used: Arc::clone(&self.used),
        }
    }

    /// Sets one unit aside, or returns `None` when no unit is free.
    pub fn reserve(&self) -> Option<Reservation> {
        let limit = self.limit.value();
        // Each number stands alone: taking only has to be atomic on `taken` itself.
        self.used
            .taken
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |taken| {
                (taken < limit).then_some(taken + 1)
            })
            .ok()?;
        Some(Reservation {
            budget: self.clone(),
            settled: false,
        })
    }

    /// Spends one unit in one step, or returns `None` when no unit is free.
    ///
    /// A budget of matches uses this: a match counts the moment it happens.
    pub fn take(&self) -> Option<Spent> {
        self.reserve().map(Reservation::confirm)
    }

    /// How many units calls confirmed.
    pub fn spent(&self) -> u64 {
        self.used.spent.load(Ordering::Relaxed)
    }

    /// How many units are free: neither spent nor set aside.
    pub fn left(&self) -> u64 {
        self.limit
            .value()
            .saturating_sub(self.used.taken.load(Ordering::Relaxed))
    }

    /// Whether calls confirmed every unit, which ends a lifetime that counts with this budget.
    pub fn is_spent(&self) -> bool {
        self.spent() >= self.limit.value()
    }

    /// The limit and the free units, for an explanation.
    pub fn reading(&self) -> BudgetReading {
        BudgetReading {
            limit: self.limit,
            left: self.left(),
        }
    }

    /// Whether the two handles take from the same budget.
    pub fn is(&self, other: &Budget) -> bool {
        Arc::ptr_eq(&self.used, &other.used)
    }
}

/// One unit of a [`Budget`] that is set aside for a call.
///
/// Whoever holds it settles it once the call knows what happened to its rule.
/// Dropping a reservation withdraws it, so a call that ends early gives its unit back.
#[derive(Debug)]
#[must_use = "a reservation that is dropped gives its unit back"]
pub struct Reservation {
    budget: Budget,
    settled: bool,
}

impl Reservation {
    /// Spends the unit for good.
    pub fn confirm(mut self) -> Spent {
        self.settled = true;
        let spent = self.budget.used.spent.fetch_add(1, Ordering::Relaxed) + 1;
        Spent {
            last: spent == self.budget.limit.value(),
        }
    }

    /// Makes the unit free again.
    pub fn withdraw(self) {
        drop(self);
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if !self.settled {
            self.budget.used.taken.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

/// What confirming one unit of a [`Budget`] did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Spent {
    last: bool,
}

impl Spent {
    /// Whether this unit was the last of the budget.
    ///
    /// Exactly one call sees this for a budget, however many confirm at once.
    /// It is the moment a lifetime that counts with the budget ends.
    pub const fn is_last(self) -> bool {
        self.last
    }
}

/// What a [`Budget`] held at one moment, as an explanation shows it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct BudgetReading {
    limit: Count,
    left: u64,
}

impl BudgetReading {
    /// How many units the budget holds.
    pub const fn limit(self) -> Count {
        self.limit
    }

    /// How many units were free: neither spent nor set aside.
    pub const fn left(self) -> u64 {
        self.left
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn budget(limit: u64) -> Budget {
        Budget::new(Count::new(limit).unwrap())
    }

    #[test]
    fn a_new_budget_has_every_unit_free() {
        let budget = budget(3);
        assert_eq!(budget.limit().value(), 3);
        assert_eq!(budget.left(), 3);
        assert_eq!(budget.spent(), 0);
        assert!(!budget.is_spent());
    }

    #[test]
    fn a_reservation_sets_a_unit_aside_without_spending_it() {
        let budget = budget(3);
        let reservation = budget.reserve().unwrap();
        assert_eq!(budget.left(), 2);
        assert_eq!(budget.spent(), 0);
        drop(reservation);
    }

    #[test]
    fn confirming_spends_the_unit() {
        let budget = budget(3);
        assert!(!budget.reserve().unwrap().confirm().is_last());
        assert_eq!(budget.left(), 2);
        assert_eq!(budget.spent(), 1);
    }

    #[test]
    fn withdrawing_and_dropping_free_the_unit() {
        let budget = budget(1);
        budget.reserve().unwrap().withdraw();
        assert_eq!(budget.left(), 1);
        drop(budget.reserve().unwrap());
        assert_eq!(budget.left(), 1);
        assert_eq!(budget.spent(), 0);
    }

    #[test]
    fn an_empty_budget_reserves_nothing() {
        let budget = budget(2);
        let held = [budget.reserve().unwrap(), budget.reserve().unwrap()];
        assert!(budget.reserve().is_none());
        assert!(budget.take().is_none());
        assert!(!budget.is_spent(), "units that are set aside are not spent");
        drop(held);
    }

    #[test]
    fn only_the_last_unit_reports_that_it_was_the_last() {
        let budget = budget(3);
        assert!(!budget.take().unwrap().is_last());
        assert!(!budget.take().unwrap().is_last());
        assert!(budget.take().unwrap().is_last());
        assert!(budget.is_spent());
        assert!(budget.take().is_none());
    }

    #[test]
    fn clones_take_from_one_budget() {
        let engine = budget(2);
        let session = engine.clone();
        assert!(engine.is(&session));
        session.take().unwrap();
        assert_eq!(engine.left(), 1);
        engine.take().unwrap();
        assert!(session.is_spent());
        assert!(!engine.is(&budget(2)));
    }

    #[test]
    fn a_new_limit_keeps_what_was_spent() {
        let three = budget(3);
        three.take().unwrap();
        three.take().unwrap();

        let five = three.with_limit(Count::new(5).unwrap());
        assert!(five.is(&three));
        assert_eq!(five.spent(), 2);
        assert_eq!(five.left(), 3);

        let one = three.with_limit(Count::ONE);
        assert_eq!(one.left(), 0);
        assert!(one.is_spent());
        assert!(one.reserve().is_none());
    }

    #[test]
    fn a_reading_shows_the_limit_and_the_free_units() {
        let budget = budget(3);
        budget.take().unwrap();
        let reading = budget.reading();
        assert_eq!(reading.limit().value(), 3);
        assert_eq!(reading.left(), 2);
    }
}
