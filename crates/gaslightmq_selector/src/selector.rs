use crate::{Conditions, SelectorName, SelectorTitle};

/// A named set of conditions, which can include another selector by name.
///
/// ```yaml
/// name: order-producers-from-ingest
/// include: order-producers
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
///     "order-producers-from-ingest".parse()?,
///     Condition::is(ClientId::new("order-ingest").expect("a client ID")).into(),
/// )
/// .including("order-producers".parse()?);
///
/// assert_eq!(from_ingest.name(), "order-producers-from-ingest");
/// assert_eq!(from_ingest.include().map(SelectorName::as_str), Some("order-producers"));
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
    title: Option<SelectorTitle>,
    #[cfg_attr(feature = "serde", serde(default, skip_serializing_if = "Option::is_none"))]
    include: Option<SelectorName>,
    #[cfg_attr(
        feature = "serde",
        serde(rename = "match", default, skip_serializing_if = "Conditions::is_empty")
    )]
    conditions: Conditions,
}

impl Selector {
    /// A selector with its own conditions, which includes no other selector. It has no title.
    pub fn new(name: SelectorName, conditions: Conditions) -> Self {
        Self {
            name,
            title: None,
            include: None,
            conditions,
        }
    }

    /// The same selector with a title, which says which calls the selector describes.
    /// It replaces the title the selector had before.
    #[must_use]
    pub fn with_title(self, title: SelectorTitle) -> Self {
        Self {
            title: Some(title),
            ..self
        }
    }

    /// The same selector without a title.
    #[must_use]
    pub fn without_title(self) -> Self {
        Self { title: None, ..self }
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

    /// The same selector with other conditions of its own.
    #[must_use]
    pub fn with_conditions(self, conditions: Conditions) -> Self {
        Self { conditions, ..self }
    }

    /// The name of the selector, unique among selectors. It is the identity of the selector.
    pub fn name(&self) -> &SelectorName {
        &self.name
    }

    /// Which calls the selector describes, in free text, when someone wrote it down.
    pub fn title(&self) -> Option<&SelectorTitle> {
        self.title.as_ref()
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
}

/// How a rule, a filter or a subscription says which calls it considers: a selector name, or inline conditions.
///
/// ```yaml
/// selector: order-producers
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
/// let named = SelectorRef::from("order-producers".parse::<SelectorName>()?);
/// let inline = SelectorRef::from(Conditions::from(Condition::is(ApiKey::METADATA)));
///
/// assert_eq!(named.name().map(SelectorName::as_str), Some("order-producers"));
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
        let selector = Selector::new(name("order-producers"), order_producers());

        assert_eq!(selector.name(), "order-producers");
        assert_eq!(selector.include(), None);
        assert_eq!(selector.conditions(), &order_producers());
    }

    #[test]
    fn a_selector_includes_one_other_by_name() {
        let selector = Selector::new(name("from-ingest"), Conditions::new()).including(name("order-producers"));

        assert_eq!(selector.include(), Some(&name("order-producers")));
        assert_eq!(
            selector.clone().including(name("order-consumers")).include(),
            Some(&name("order-consumers"))
        );
        assert_eq!(selector.not_including().include(), None);
    }

    #[test]
    fn a_selector_changes_one_part_at_a_time() {
        let selector = Selector::new(name("order-producers"), order_producers()).including(name("producers"));

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
        let named = SelectorRef::from(name("order-producers"));
        let inline = SelectorRef::from(order_producers());

        assert_eq!(named, SelectorRef::Named(name("order-producers")));
        assert_eq!(named.name(), Some(&name("order-producers")));
        assert_eq!(inline, SelectorRef::Inline(order_producers()));
        assert_eq!(inline.name(), None);
    }

    #[test]
    fn a_selector_has_a_title_when_someone_gives_it_one() {
        let title = |text: &str| text.parse::<SelectorTitle>().unwrap();
        let selector = Selector::new(name("order-producers"), order_producers());
        let titled = selector.clone().with_title(title("Order producers"));

        assert_eq!(selector.title(), None);
        assert_eq!(titled.title(), Some(&title("Order producers")));
        assert_eq!(titled.name(), selector.name());
        assert_eq!(titled.conditions(), selector.conditions());
        assert_eq!(
            titled.clone().with_title(title("Whoever produces orders")).title(),
            Some(&title("Whoever produces orders"))
        );
        assert_eq!(titled.without_title(), selector);
    }
}
