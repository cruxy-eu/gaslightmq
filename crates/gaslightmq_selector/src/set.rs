use std::collections::BTreeMap;
use std::collections::btree_map;

use crate::{EffectiveSelector, Selector, SelectorName, SelectorRef};

/// The selectors of a workspace by name, which resolves what a selector includes.
///
/// A set holds together:
///
/// - every name is there once,
/// - every selector includes a selector of the set,
/// - no selector includes itself, directly or through other selectors.
///
/// [`new`](Self::new) checks all three, so [`resolve`](Self::resolve) only fails for a name the set does not have.
/// A set does not change, apart from a [rename](Self::rename): build a new one from the selectors after an edit.
///
/// The set knows the references between selectors.
/// Whether a rule or a pad layout names a selector that exists is for whoever holds those to check,
/// with [`contains`](Self::contains).
///
/// ```
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_selector::prelude::*;
///
/// let order_producers = Selector::new(
///     "Order producers".parse().expect("a selector name"),
///     Conditions::new()
///         .with(Condition::is(ApiKey::PRODUCE))
///         .with(Condition::parse(Field::Topic, "orders.*").expect("a condition")),
/// );
/// let from_ingest = Selector::new(
///     "Order producers from ingest".parse().expect("a selector name"),
///     Condition::is(ClientId::new("order-ingest").expect("a client ID")).into(),
/// )
/// .including("Order producers".parse().expect("a selector name"));
///
/// let set = SelectorSet::new([order_producers, from_ingest])?;
/// let effective = set.effective(&"Order producers from ingest".parse().expect("a selector name"))?;
/// assert_eq!(
///     effective.to_string(),
///     "api is Produce and topic matches orders.* and client.id is order-ingest"
/// );
/// # Ok::<(), SelectorSetError>(())
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SelectorSet {
    by_name: BTreeMap<SelectorName, Selector>,
}

impl SelectorSet {
    /// The set of those selectors.
    ///
    /// # Errors
    ///
    /// - [`SelectorSetError::DuplicateName`] when two selectors have one name.
    /// - [`SelectorSetError::UnknownInclude`] when a selector includes a name that no selector has.
    /// - [`SelectorSetError::IncludeCycle`] when a selector includes itself, directly or through others.
    pub fn new(selectors: impl IntoIterator<Item = Selector>) -> Result<Self, SelectorSetError> {
        let mut by_name = BTreeMap::new();
        for selector in selectors {
            if let Some(twice) = by_name.insert(selector.name().clone(), selector) {
                return Err(SelectorSetError::DuplicateName {
                    name: twice.name().clone(),
                });
            }
        }
        let set = Self { by_name };
        for selector in &set {
            set.with_included(selector)?;
        }
        Ok(set)
    }

    /// The selector of this name.
    pub fn get(&self, name: &SelectorName) -> Option<&Selector> {
        self.by_name.get(name)
    }

    /// Whether a selector has this name.
    pub fn contains(&self, name: &SelectorName) -> bool {
        self.by_name.contains_key(name)
    }

    /// The selectors in the order of their names.
    pub fn iter(&self) -> btree_map::Values<'_, SelectorName, Selector> {
        self.by_name.values()
    }

    /// How many selectors there are.
    pub fn len(&self) -> usize {
        self.by_name.len()
    }

    /// Whether there is no selector.
    pub fn is_empty(&self) -> bool {
        self.by_name.is_empty()
    }

    /// Every condition that applies to the selector of this name: its own, and those of what it includes.
    ///
    /// # Errors
    ///
    /// [`SelectorSetError::UnknownSelector`] when no selector has this name.
    pub fn effective(&self, name: &SelectorName) -> Result<EffectiveSelector, SelectorSetError> {
        let selector = self
            .get(name)
            .ok_or_else(|| SelectorSetError::UnknownSelector { name: name.clone() })?;
        let chain = self.with_included(selector)?;
        Ok(chain
            .into_iter()
            .rev()
            .flat_map(|selector| selector.conditions().iter().cloned())
            .collect())
    }

    /// Every condition that applies to what `selector` references: a selector of the set, or inline conditions.
    ///
    /// # Errors
    ///
    /// [`SelectorSetError::UnknownSelector`] when the reference names a selector that the set does not have.
    pub fn resolve(&self, selector: &SelectorRef) -> Result<EffectiveSelector, SelectorSetError> {
        match selector {
            SelectorRef::Named(name) => self.effective(name),
            SelectorRef::Inline(conditions) => Ok(conditions.clone().into()),
        }
    }

    /// Renames a selector, and updates every selector that includes it.
    ///
    /// What references the selector from outside the set, such as a rule, is for whoever holds it to update.
    ///
    /// # Errors
    ///
    /// [`SelectorSetError::UnknownSelector`] when no selector is named `from`,
    /// and [`SelectorSetError::DuplicateName`] when another selector is named `to` already.
    pub fn rename(&mut self, from: &SelectorName, to: SelectorName) -> Result<(), SelectorSetError> {
        if !self.contains(from) {
            return Err(SelectorSetError::UnknownSelector { name: from.clone() });
        }
        if *from == to {
            return Ok(());
        }
        if self.contains(&to) {
            return Err(SelectorSetError::DuplicateName { name: to });
        }
        if let Some(selector) = self.by_name.remove(from) {
            self.by_name.insert(to.clone(), selector.renamed(to.clone()));
        }
        for include in self.by_name.values_mut().filter_map(Selector::include_mut) {
            if include == from {
                *include = to.clone();
            }
        }
        Ok(())
    }

    /// `selector`, the selector it includes, the one that one includes, and so on.
    fn with_included<'a>(&'a self, selector: &'a Selector) -> Result<Vec<&'a Selector>, SelectorSetError> {
        let mut chain = vec![selector];
        let mut last = selector;
        while let Some(include) = last.include() {
            let included = self.get(include).ok_or_else(|| SelectorSetError::UnknownInclude {
                selector: last.name().clone(),
                include: include.clone(),
            })?;
            if let Some(start) = chain.iter().position(|seen| seen.name() == included.name()) {
                let circle = chain[start..].iter().chain([&included]);
                return Err(SelectorSetError::IncludeCycle {
                    path: circle.map(|selector| selector.name().clone()).collect(),
                });
            }
            chain.push(included);
            last = included;
        }
        Ok(chain)
    }
}

impl<'a> IntoIterator for &'a SelectorSet {
    type Item = &'a Selector;
    type IntoIter = btree_map::Values<'a, SelectorName, Selector>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl IntoIterator for SelectorSet {
    type Item = Selector;
    type IntoIter = btree_map::IntoValues<SelectorName, Selector>;

    fn into_iter(self) -> Self::IntoIter {
        self.by_name.into_values()
    }
}

/// Why selectors are not a [`SelectorSet`], or why the set does not have what it is asked for.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum SelectorSetError {
    /// Two selectors have the same name.
    #[error("two selectors are named {:?}", .name.as_str())]
    DuplicateName { name: SelectorName },
    /// No selector of the set has this name.
    #[error("no selector is named {:?}", .name.as_str())]
    UnknownSelector { name: SelectorName },
    /// A selector includes a name that no selector of the set has.
    #[error("selector {:?} includes {:?}, and no selector has that name", .selector.as_str(), .include.as_str())]
    UnknownInclude {
        selector: SelectorName,
        include: SelectorName,
    },
    /// A selector includes itself, directly or through other selectors.
    /// The path starts and ends with the same name.
    #[error("selectors include each other in a circle: {}", circle(.path))]
    IncludeCycle { path: Vec<SelectorName> },
}

/// The names of a circle as a message writes them: each in quotes, with ` includes ` between them.
fn circle(path: &[SelectorName]) -> String {
    let names: Vec<String> = path.iter().map(|name| format!("{:?}", name.as_str())).collect();
    names.join(" includes ")
}

#[cfg(test)]
mod tests {
    use gaslightmq_field::prelude::*;
    use gaslightmq_kernel::prelude::*;

    use super::*;
    use crate::{Condition, Conditions};

    fn name(text: &str) -> SelectorName {
        text.parse().unwrap()
    }

    fn names(texts: &[&str]) -> Vec<SelectorName> {
        texts.iter().map(|text| name(text)).collect()
    }

    /// A selector without conditions, which includes `include`.
    fn link(own: &str, include: Option<&str>) -> Selector {
        let selector = Selector::new(name(own), Conditions::new());
        match include {
            Some(include) => selector.including(name(include)),
            None => selector,
        }
    }

    fn produce() -> Selector {
        Selector::new(name("Produce"), Condition::is(ApiKey::PRODUCE).into())
    }

    fn order_producers() -> Selector {
        Selector::new(
            name("Order producers"),
            Condition::parse(Field::Topic, "orders.*").unwrap().into(),
        )
        .including(name("Produce"))
    }

    fn from_ingest() -> Selector {
        Selector::new(
            name("Order producers from ingest"),
            Condition::is(ClientId::new("order-ingest").unwrap()).into(),
        )
        .including(name("Order producers"))
    }

    #[test]
    fn a_set_holds_its_selectors_by_name() {
        let set = SelectorSet::new([order_producers(), produce()]).unwrap();

        assert_eq!(set.len(), 2);
        assert!(!set.is_empty());
        assert!(set.contains(&name("Produce")));
        assert!(!set.contains(&name("produce")));
        assert_eq!(set.get(&name("Order producers")), Some(&order_producers()));
        assert_eq!(set.get(&name("Order consumers")), None);
        assert_eq!(
            set.iter().map(Selector::name).collect::<Vec<_>>(),
            [&name("Order producers"), &name("Produce")]
        );
        assert_eq!((&set).into_iter().count(), 2);
        assert_eq!(set.into_iter().collect::<Vec<_>>(), [order_producers(), produce()]);
    }

    #[test]
    fn an_empty_set_is_a_set() {
        let set = SelectorSet::new([]).unwrap();

        assert!(set.is_empty());
        assert_eq!(set, SelectorSet::default());
    }

    #[test]
    fn two_selectors_with_one_name_are_an_error() {
        assert_eq!(
            SelectorSet::new([produce(), order_producers(), link("Produce", None)]),
            Err(SelectorSetError::DuplicateName { name: name("Produce") })
        );
    }

    #[test]
    fn an_include_that_names_no_selector_is_an_error() {
        assert_eq!(
            SelectorSet::new([order_producers()]),
            Err(SelectorSetError::UnknownInclude {
                selector: name("Order producers"),
                include: name("Produce"),
            })
        );
        assert_eq!(
            SelectorSet::new([from_ingest(), order_producers()]),
            Err(SelectorSetError::UnknownInclude {
                selector: name("Order producers"),
                include: name("Produce"),
            })
        );
    }

    #[test]
    fn a_selector_that_includes_itself_is_an_error() {
        assert_eq!(
            SelectorSet::new([link("A", Some("A"))]),
            Err(SelectorSetError::IncludeCycle {
                path: names(&["A", "A"])
            })
        );
    }

    #[test]
    fn selectors_that_include_each_other_in_a_circle_are_an_error() {
        assert_eq!(
            SelectorSet::new([link("A", Some("B")), link("B", Some("A"))]),
            Err(SelectorSetError::IncludeCycle {
                path: names(&["A", "B", "A"])
            })
        );
        assert_eq!(
            SelectorSet::new([link("C", Some("A")), link("A", Some("B")), link("B", Some("C"))]),
            Err(SelectorSetError::IncludeCycle {
                path: names(&["A", "B", "C", "A"])
            })
        );
    }

    #[test]
    fn a_selector_that_leads_into_a_circle_names_the_circle_only() {
        assert_eq!(
            SelectorSet::new([link("A", Some("X")), link("X", Some("Y")), link("Y", Some("X"))]),
            Err(SelectorSetError::IncludeCycle {
                path: names(&["X", "Y", "X"])
            })
        );
    }

    #[test]
    fn several_selectors_can_include_the_same_one() {
        let set = SelectorSet::new([link("A", Some("C")), link("B", Some("C")), link("C", None)]);

        assert_eq!(set.map(|set| set.len()), Ok(3));
    }

    #[test]
    fn the_effective_selector_has_the_included_conditions_first() {
        let set = SelectorSet::new([from_ingest(), order_producers(), produce()]).unwrap();

        assert_eq!(set.effective(&name("Produce")).unwrap().to_string(), "api is Produce");
        assert_eq!(
            set.effective(&name("Order producers")).unwrap().to_string(),
            "api is Produce and topic matches orders.*"
        );
        assert_eq!(
            set.effective(&name("Order producers from ingest")).unwrap().to_string(),
            "api is Produce and topic matches orders.* and client.id is order-ingest"
        );
    }

    #[test]
    fn an_include_can_test_a_field_that_the_selector_tests_too() {
        let orders = Selector::new(
            name("Orders"),
            Condition::parse(Field::Topic, "orders.*").unwrap().into(),
        );
        let european = Selector::new(
            name("European orders"),
            Condition::parse(Field::Topic, "*.eu").unwrap().into(),
        )
        .including(name("Orders"));
        let set = SelectorSet::new([orders, european]).unwrap();

        assert_eq!(
            set.effective(&name("European orders")).unwrap().to_string(),
            "topic matches orders.* and topic matches *.eu"
        );
    }

    #[test]
    fn a_reference_resolves_to_a_selector_of_the_set_or_to_its_own_conditions() {
        let set = SelectorSet::new([order_producers(), produce()]).unwrap();
        let inline = Conditions::from(Condition::is(ApiKey::METADATA));

        assert_eq!(
            set.resolve(&name("Order producers").into()),
            set.effective(&name("Order producers"))
        );
        assert_eq!(set.resolve(&inline.clone().into()), Ok(inline.into()));
    }

    #[test]
    fn a_name_that_no_selector_has_does_not_resolve() {
        let set = SelectorSet::new([produce()]).unwrap();
        let unknown = SelectorSetError::UnknownSelector {
            name: name("Order producers"),
        };

        assert_eq!(set.effective(&name("Order producers")), Err(unknown.clone()));
        assert_eq!(set.resolve(&name("Order producers").into()), Err(unknown));
    }

    #[test]
    fn renaming_a_selector_updates_every_selector_that_includes_it() {
        let mut set = SelectorSet::new([from_ingest(), order_producers(), produce()]).unwrap();

        set.rename(&name("Order producers"), name("Order writers")).unwrap();

        assert!(!set.contains(&name("Order producers")));
        assert_eq!(
            set.get(&name("Order writers")),
            Some(&order_producers().renamed(name("Order writers")))
        );
        assert_eq!(
            set.get(&name("Order producers from ingest"))
                .and_then(Selector::include),
            Some(&name("Order writers"))
        );
        assert_eq!(set.get(&name("Produce")), Some(&produce()));
        assert_eq!(
            set.effective(&name("Order producers from ingest")).unwrap().to_string(),
            "api is Produce and topic matches orders.* and client.id is order-ingest"
        );
        assert_eq!(SelectorSet::new(set.clone()), Ok(set));
    }

    #[test]
    fn renaming_needs_a_selector_and_a_free_name() {
        let mut set = SelectorSet::new([order_producers(), produce()]).unwrap();
        let before = set.clone();

        assert_eq!(
            set.rename(&name("Order consumers"), name("Consumers")),
            Err(SelectorSetError::UnknownSelector {
                name: name("Order consumers")
            })
        );
        assert_eq!(
            set.rename(&name("Order producers"), name("Produce")),
            Err(SelectorSetError::DuplicateName { name: name("Produce") })
        );
        assert_eq!(set.rename(&name("Produce"), name("Produce")), Ok(()));
        assert_eq!(set, before);
    }
}
