//! The definitions that the check stores.

use gaslightmq_definition_api::Definition;

/// The definitions of one kind that [`assert_store_contract`](crate::assert_store_contract) stores.
///
/// | Sample | Is |
/// |---|---|
/// | [`definition`](Self::definition) | A definition as someone wrote it. |
/// | [`changed`](Self::changed) | The same definition after a change: it has the same name, and it differs in the rest. |
/// | [`other`](Self::other) | A definition of another name, which sorts before the first. |
///
/// This crate knows no kind of definition, so whoever runs the check hands in the samples of its kind.
/// The changed definition is the one to give every part that a store has to keep,
/// because the check asks a store to hand it back as it went in.
///
/// ```
/// use gaslightmq_definition_api::prelude::*;
/// use gaslightmq_definition_conformance::Samples;
///
/// /// What a team says when it starts its day, under a name.
/// #[derive(Debug, Clone, PartialEq)]
/// struct Greeting {
///     name: String,
///     text: String,
/// }
///
/// impl Definition for Greeting {
///     type Name = String;
///
///     const LABEL: &'static str = "greeting";
///
///     fn name(&self) -> &String {
///         &self.name
///     }
/// }
///
/// let greeting = |name: &str, text: &str| Greeting { name: name.to_owned(), text: text.to_owned() };
/// let greetings = Samples::new(
///     greeting("morning", "Good morning"),
///     greeting("morning", "Good morning, everyone"),
///     greeting("evening", "Good evening"),
/// );
///
/// assert_eq!(greetings.definition().name(), greetings.changed().name());
/// assert!(greetings.other().name() < greetings.definition().name());
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Samples<D> {
    definition: D,
    changed: D,
    other: D,
}

impl<D: Definition + PartialEq> Samples<D> {
    /// The samples of one kind of definition.
    ///
    /// # Panics
    ///
    /// When the samples are not what the check needs:
    /// `changed` has another name than `definition` or is equal to it,
    /// or the name of `other` does not sort before the name of `definition`.
    pub fn new(definition: D, changed: D, other: D) -> Self {
        assert!(
            definition.name() == changed.name(),
            "the changed definition has the name of the first"
        );
        assert!(definition != changed, "the changed definition differs from the first");
        assert!(
            other.name() < definition.name(),
            "the name of the other definition sorts before the name of the first"
        );
        Self {
            definition,
            changed,
            other,
        }
    }

    /// A definition as someone wrote it.
    pub fn definition(&self) -> &D {
        &self.definition
    }

    /// The same definition after a change: it has the same name, and everything a store has to keep.
    pub fn changed(&self) -> &D {
        &self.changed
    }

    /// A definition of another name, which sorts before the first.
    pub fn other(&self) -> &D {
        &self.other
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// What a team says when it starts its day, under a name.
    #[derive(Debug, Clone, PartialEq)]
    struct Greeting {
        name: String,
        text: String,
    }

    impl Greeting {
        fn new(name: &str, text: &str) -> Self {
            Self {
                name: name.to_owned(),
                text: text.to_owned(),
            }
        }
    }

    impl Definition for Greeting {
        type Name = String;

        const LABEL: &'static str = "greeting";

        fn name(&self) -> &String {
            &self.name
        }
    }

    #[test]
    fn samples_keep_the_three_definitions_they_were_made_of() {
        let morning = Greeting::new("morning", "Good morning");
        let louder = Greeting::new("morning", "Good morning, everyone");
        let evening = Greeting::new("evening", "Good evening");

        let greetings = Samples::new(morning.clone(), louder.clone(), evening.clone());

        assert_eq!(greetings.definition(), &morning);
        assert_eq!(greetings.changed(), &louder);
        assert_eq!(greetings.changed().text, "Good morning, everyone");
        assert_eq!(greetings.other(), &evening);
    }

    #[test]
    #[should_panic(expected = "the changed definition has the name of the first")]
    fn a_change_under_another_name_is_no_change() {
        let _ = Samples::new(
            Greeting::new("morning", "Good morning"),
            Greeting::new("noon", "Good morning, everyone"),
            Greeting::new("evening", "Good evening"),
        );
    }

    #[test]
    #[should_panic(expected = "the changed definition differs from the first")]
    fn a_change_that_changes_nothing_is_no_change() {
        let _ = Samples::new(
            Greeting::new("morning", "Good morning"),
            Greeting::new("morning", "Good morning"),
            Greeting::new("evening", "Good evening"),
        );
    }

    #[test]
    #[should_panic(expected = "the name of the other definition sorts before the name of the first")]
    fn samples_in_the_wrong_order_are_no_samples() {
        // The two greetings stand where the other one belongs.
        let _ = Samples::new(
            Greeting::new("evening", "Good evening"),
            Greeting::new("evening", "Good evening, everyone"),
            Greeting::new("morning", "Good morning"),
        );
    }
}
