use crate::{Conditions, SelectorName};

/// A named set of conditions, which can include another selector by name.
///
/// ```yaml
/// name: Order producers from ingest
/// include: Order producers
/// match:
///   client.id: order-ingest
/// ```
///
/// The conditions of the included selector combine with the selector's own with **and**.
/// A selector alone does not know them: a [`SelectorSet`](crate::SelectorSet) resolves the name,
/// and gives the [`EffectiveSelector`](crate::EffectiveSelector) that matches calls.
///
/// ```
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_selector::prelude::*;
///
/// let from_ingest = Selector::new(
///     "Order producers from ingest".parse()?,
///     Condition::is(ClientId::new("order-ingest").expect("a client ID")).into(),
/// )
/// .including("Order producers".parse()?);
///
/// assert_eq!(from_ingest.name(), "Order producers from ingest");
/// assert_eq!(from_ingest.include().map(SelectorName::as_str), Some("Order producers"));
/// assert_eq!(from_ingest.conditions().to_string(), "client.id is order-ingest");
/// # Ok::<(), SelectorNameError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(deny_unknown_fields)
)]
pub struct Selector {
    name: SelectorName,
    #[cfg_attr(feature = "serde", serde(default, skip_serializing_if = "Option::is_none"))]
    include: Option<SelectorName>,
    #[cfg_attr(
        feature = "serde",
        serde(rename = "match", default, skip_serializing_if = "Conditions::is_empty")
    )]
    conditions: Conditions,
}

impl Selector {
    /// A selector with its own conditions, which includes no other selector.
    pub fn new(name: SelectorName, conditions: Conditions) -> Self {
        Self {
            name,
            include: None,
            conditions,
        }
    }

    /// The same selector, which includes the selector named `include`.
    /// It replaces the selector that was included before.
    #[must_use]
    pub fn including(self, include: SelectorName) -> Self {
        Self {
            include: Some(include),
            ..self
        }
    }

    /// The same selector, which includes no other selector.
    #[must_use]
    pub fn not_including(self) -> Self {
        Self { include: None, ..self }
    }

    /// The same selector under another name.
    /// What references the selector by its old name does not follow:
    /// [`SelectorSet::rename`](crate::SelectorSet::rename) renames a selector with the selectors that include it.
    #[must_use]
    pub fn renamed(self, name: SelectorName) -> Self {
        Self { name, ..self }
    }

    /// The same selector with other conditions of its own.
    #[must_use]
    pub fn with_conditions(self, conditions: Conditions) -> Self {
        Self { conditions, ..self }
    }

    /// The name of the selector, unique among selectors.
    pub fn name(&self) -> &SelectorName {
        &self.name
    }

    /// The name of the selector this one includes.
    pub fn include(&self) -> Option<&SelectorName> {
        self.include.as_ref()
    }

    /// The selector's own conditions, without those it includes.
    pub fn conditions(&self) -> &Conditions {
        &self.conditions
    }

    /// The selector's own conditions, to change them in place.
    pub fn conditions_mut(&mut self) -> &mut Conditions {
        &mut self.conditions
    }

    pub(crate) fn include_mut(&mut self) -> Option<&mut SelectorName> {
        self.include.as_mut()
    }
}

/// How a rule, a filter or a subscription says which calls it considers: a selector name, or inline conditions.
///
/// ```yaml
/// selector: Order producers
/// ```
///
/// ```yaml
/// selector: { api: Metadata }
/// ```
///
/// A [`SelectorSet`](crate::SelectorSet) resolves either into an
/// [`EffectiveSelector`](crate::EffectiveSelector).
///
/// ```
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_selector::prelude::*;
///
/// let named = SelectorRef::from("Order producers".parse::<SelectorName>()?);
/// let inline = SelectorRef::from(Conditions::from(Condition::is(ApiKey::METADATA)));
///
/// assert_eq!(named.name().map(SelectorName::as_str), Some("Order producers"));
/// assert_eq!(inline.name(), None);
/// # Ok::<(), SelectorNameError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SelectorRef {
    /// The selector of this name in the set.
    Named(SelectorName),
    /// Conditions that belong to whoever writes them, and have no name.
    Inline(Conditions),
}

impl SelectorRef {
    /// The name of the selector this references, or `None` for inline conditions.
    pub fn name(&self) -> Option<&SelectorName> {
        match self {
            SelectorRef::Named(name) => Some(name),
            SelectorRef::Inline(_) => None,
        }
    }
}

impl From<SelectorName> for SelectorRef {
    fn from(name: SelectorName) -> Self {
        SelectorRef::Named(name)
    }
}

impl From<Conditions> for SelectorRef {
    fn from(conditions: Conditions) -> Self {
        SelectorRef::Inline(conditions)
    }
}

#[cfg(test)]
mod tests {
    use gaslightmq_field::prelude::*;
    use gaslightmq_kernel::prelude::*;

    use super::*;
    use crate::Condition;

    fn name(text: &str) -> SelectorName {
        text.parse().unwrap()
    }

    fn order_producers() -> Conditions {
        Conditions::new()
            .with(Condition::is(ApiKey::PRODUCE))
            .with(Condition::parse(Field::Topic, "orders.*").unwrap())
    }

    #[test]
    fn a_new_selector_includes_no_other() {
        let selector = Selector::new(name("Order producers"), order_producers());

        assert_eq!(selector.name(), "Order producers");
        assert_eq!(selector.include(), None);
        assert_eq!(selector.conditions(), &order_producers());
    }

    #[test]
    fn a_selector_includes_one_other_by_name() {
        let selector = Selector::new(name("From ingest"), Conditions::new()).including(name("Order producers"));

        assert_eq!(selector.include(), Some(&name("Order producers")));
        assert_eq!(
            selector.clone().including(name("Order consumers")).include(),
            Some(&name("Order consumers"))
        );
        assert_eq!(selector.not_including().include(), None);
    }

    #[test]
    fn a_selector_changes_one_part_at_a_time() {
        let selector = Selector::new(name("Order producers"), order_producers()).including(name("Producers"));

        let renamed = selector.clone().renamed(name("Order writers"));
        assert_eq!(renamed.name(), "Order writers");
        assert_eq!(renamed.include(), selector.include());
        assert_eq!(renamed.conditions(), selector.conditions());

        let emptied = selector.clone().with_conditions(Conditions::new());
        assert_eq!(emptied.name(), selector.name());
        assert_eq!(emptied.include(), selector.include());
        assert!(emptied.conditions().is_empty());

        let mut narrowed = selector.clone();
        narrowed.conditions_mut().insert(Condition::is(Acks::ALL));
        assert_eq!(narrowed.conditions(), &order_producers().with(Condition::is(Acks::ALL)));
    }

    #[test]
    fn a_reference_is_a_name_or_inline_conditions() {
        let named = SelectorRef::from(name("Order producers"));
        let inline = SelectorRef::from(order_producers());

        assert_eq!(named, SelectorRef::Named(name("Order producers")));
        assert_eq!(named.name(), Some(&name("Order producers")));
        assert_eq!(inline, SelectorRef::Inline(order_producers()));
        assert_eq!(inline.name(), None);
    }
}
