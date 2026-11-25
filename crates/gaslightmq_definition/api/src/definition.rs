//! What a store holds: a definition, which has a name.

/// What a [store](crate::Store) holds: something that someone defined under a name.
///
/// A definition is the root of what it defines, and a store holds it as a whole.
/// A store only needs its name, which is its identity among the definitions of its kind.
/// Everything else of a definition is for its own crate to know.
///
/// The crate of a definition implements this trait for it: a selector is a definition in the crate of the selectors.
/// This crate knows none of them.
///
/// ```
/// use gaslightmq_definition_api::prelude::*;
///
/// /// What a team says when it starts its day, under a name.
/// #[derive(Clone)]
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
/// /// What a message calls any definition: the word for its kind, and its name.
/// fn called<D: Definition<Name = String>>(definition: &D) -> String {
///     format!("{} {:?}", D::LABEL, definition.name())
/// }
///
/// let morning = Greeting { name: "morning".to_owned(), text: "Good morning".to_owned() };
/// assert_eq!(called(&morning), "greeting \"morning\"");
/// ```
pub trait Definition: Clone + Send + Sync + 'static {
    /// The name of a definition of this kind.
    ///
    /// Names sort, and a store lists its definitions in that order.
    type Name: Clone + Ord + Send + Sync + 'static;

    /// The word that a message uses for one definition of this kind: `selector`.
    const LABEL: &'static str;

    /// The name of the definition, which no other definition of its kind in the store has.
    fn name(&self) -> &Self::Name;
}
