//! The definitions of the workspace: what the control plane lists, gets, creates, updates and deletes.

use std::fmt::{self, Debug};
use std::marker::PhantomData;

use gaslightmq_environment::{Environment, EnvironmentName};
use gaslightmq_rule::{Preset, PresetName, Rule, RuleName};
use gaslightmq_selector::{Selector, SelectorName};
use gaslightmq_surface::{LayoutName, PadLayout};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::{AnyOperation, Operation};

/// A kind of definition that the control plane lists, gets, creates, updates and deletes:
/// a [`Selector`], a [`Preset`], a [`Rule`] or a [`PadLayout`].
///
/// The operations on definitions are generic over the kind, so each one answers with the type of its kind:
/// [`GetDefinition<Rule>`] answers with a [`Rule`].
/// An environment is a definition too, and the control plane only lists and gets it, with whether it runs.
/// See [`ListEnvironments`](crate::ListEnvironments).
///
/// Nothing outside this crate implements the trait.
///
/// ```
/// use gaslightmq_control_api::prelude::*;
/// use gaslightmq_control_api::rule::Rule;
/// use gaslightmq_control_api::selector::Selector;
///
/// fn edited_through_the_control_plane<D: EditableDefinition>() {}
///
/// edited_through_the_control_plane::<Selector>();
/// edited_through_the_control_plane::<Rule>();
/// ```
pub trait EditableDefinition: Clone + Debug + PartialEq + Serialize + DeserializeOwned {
    /// The name of a definition of this kind, which every operation names it by.
    type Name: Clone + Debug + PartialEq + Serialize + DeserializeOwned + Into<DefinitionRef>;

    /// The name of the definition.
    fn name(&self) -> &Self::Name;

    /// The definition, as one of any kind.
    fn into_any(self) -> AnyDefinition;
}

/// Makes a definition type editable: the type of its name, and its variant of [`AnyDefinition`].
macro_rules! editable {
    ($($definition:ident, $name:ident => $variant:ident;)*) => {
        $(
            impl EditableDefinition for $definition {
                type Name = $name;

                fn name(&self) -> &$name {
                    $definition::name(self)
                }

                fn into_any(self) -> AnyDefinition {
                    AnyDefinition::$variant(self)
                }
            }
        )*
    };
}

editable! {
    Selector, SelectorName => Selector;
    Preset, PresetName => Preset;
    Rule, RuleName => Rule;
    PadLayout, LayoutName => Layout;
}

/// One definition of the workspace, of any kind.
///
/// A message that speaks about definitions of every kind carries one of these:
/// the [definition that changed](crate::StreamMessage::DefinitionChanged).
///
/// With `serde`, it is a map with one entry: the kind, and the form of the definition.
/// `{ "rule": { "name": "slow-order-producers", … } }`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AnyDefinition {
    /// A selector.
    Selector(Selector),
    /// A preset.
    Preset(Preset),
    /// A rule of the workspace.
    Rule(Rule),
    /// A pad layout.
    Layout(PadLayout),
    /// An environment.
    Environment(Environment),
}

impl AnyDefinition {
    /// The name of the definition, with its kind.
    pub fn name(&self) -> DefinitionRef {
        match self {
            AnyDefinition::Selector(selector) => selector.name().clone().into(),
            AnyDefinition::Preset(preset) => preset.name().clone().into(),
            AnyDefinition::Rule(rule) => rule.name().clone().into(),
            AnyDefinition::Layout(layout) => layout.name().clone().into(),
            AnyDefinition::Environment(environment) => environment.name().clone().into(),
        }
    }
}

/// The name of one definition of the workspace, with its kind: `rule "slow-order-producers"`.
///
/// A name is unique within its kind, so a selector and a rule can share one.
/// An answer that speaks about definitions of several kinds, such as the users of a selector, names each with its kind.
///
/// With `serde`, it is a map with one entry: the kind, and the name. `{ "rule": "slow-order-producers" }`
///
/// ```
/// use gaslightmq_control_api::prelude::*;
/// use gaslightmq_control_api::rule::RuleName;
/// use gaslightmq_control_api::selector::SelectorName;
///
/// let selector = DefinitionRef::from("order-producers".parse::<SelectorName>().expect("a selector name"));
/// let rule = DefinitionRef::from("order-producers".parse::<RuleName>().expect("a rule name"));
///
/// assert_ne!(selector, rule);
/// assert_eq!(rule.to_string(), "rule \"order-producers\"");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DefinitionRef {
    /// The name of a selector.
    Selector(SelectorName),
    /// The name of a preset.
    Preset(PresetName),
    /// The name of a rule.
    Rule(RuleName),
    /// The name of a pad layout.
    Layout(LayoutName),
    /// The name of an environment.
    Environment(EnvironmentName),
}

/// Writes the kind and the name in quotes: `rule "slow-order-producers"`.
impl fmt::Display for DefinitionRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (kind, name) = match self {
            DefinitionRef::Selector(name) => ("selector", name.as_str()),
            DefinitionRef::Preset(name) => ("preset", name.as_str()),
            DefinitionRef::Rule(name) => ("rule", name.as_str()),
            DefinitionRef::Layout(name) => ("layout", name.as_str()),
            DefinitionRef::Environment(name) => ("environment", name.as_str()),
        };
        write!(f, "{kind} {name:?}")
    }
}

impl From<SelectorName> for DefinitionRef {
    fn from(name: SelectorName) -> Self {
        DefinitionRef::Selector(name)
    }
}

impl From<PresetName> for DefinitionRef {
    fn from(name: PresetName) -> Self {
        DefinitionRef::Preset(name)
    }
}

impl From<RuleName> for DefinitionRef {
    fn from(name: RuleName) -> Self {
        DefinitionRef::Rule(name)
    }
}

impl From<LayoutName> for DefinitionRef {
    fn from(name: LayoutName) -> Self {
        DefinitionRef::Layout(name)
    }
}

impl From<EnvironmentName> for DefinitionRef {
    fn from(name: EnvironmentName) -> Self {
        DefinitionRef::Environment(name)
    }
}

/// Every definition of the workspace, by kind.
///
/// The [state of a process](crate::ProcessState) carries them, so a client that connects knows the workspace at once.
/// A definition travels as its file holds it. Its UUID is live state of the process, and travels in no definition.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Definitions {
    /// Every selector.
    pub selectors: Vec<Selector>,
    /// Every preset.
    pub presets: Vec<Preset>,
    /// Every rule of the workspace. Live rules belong to an environment, and are not here.
    pub rules: Vec<Rule>,
    /// Every pad layout.
    pub layouts: Vec<PadLayout>,
    /// Every environment, whether it runs or not.
    pub environments: Vec<Environment>,
}

/// List every definition of a kind in the workspace.
///
/// The answer holds each definition as its file holds it.
///
/// ```
/// use gaslightmq_control_api::prelude::*;
/// use gaslightmq_control_api::rule::Rule;
///
/// let request = AnyOperation::from(ListDefinitions::<Rule>::new());
/// assert_eq!(request, AnyOperation::ListRules(ListDefinitions::new()));
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(bound = "", deny_unknown_fields)]
pub struct ListDefinitions<D> {
    #[serde(skip)]
    kind: PhantomData<D>,
}

impl<D> ListDefinitions<D> {
    /// Ask for the definitions of the kind `D`.
    pub const fn new() -> Self {
        Self { kind: PhantomData }
    }
}

impl<D> Default for ListDefinitions<D> {
    fn default() -> Self {
        Self::new()
    }
}

impl<D: EditableDefinition> Operation for ListDefinitions<D>
where
    Self: Into<AnyOperation>,
{
    type Answer = Vec<D>;
    type Error = ListDefinitionsError;
}

/// Get the definition of a kind that has a name.
///
/// The answer is the definition as its file holds it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(bound = "", deny_unknown_fields)]
pub struct GetDefinition<D: EditableDefinition> {
    /// The name of the definition.
    pub name: D::Name,
}

impl<D: EditableDefinition> Operation for GetDefinition<D>
where
    Self: Into<AnyOperation>,
{
    type Answer = D;
    type Error = GetDefinitionError;
}

/// Create a definition in the workspace, under a name that no definition of its kind has.
///
/// The workspace gives the definition a new UUID, and keeps it in memory.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(bound = "", deny_unknown_fields)]
pub struct CreateDefinition<D: EditableDefinition> {
    /// The definition, with its name.
    pub definition: D,
}

impl<D: EditableDefinition> Operation for CreateDefinition<D>
where
    Self: Into<AnyOperation>,
{
    type Answer = ();
    type Error = CreateDefinitionError;
}

/// Replace the definition that has the name of this one: everything but the name changes, the title included.
///
/// Nothing renames a definition. The definition keeps the UUID that the workspace holds for it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(bound = "", deny_unknown_fields)]
pub struct UpdateDefinition<D: EditableDefinition> {
    /// The definition as it is to be, with the name of the one it replaces.
    pub definition: D,
}

impl<D: EditableDefinition> Operation for UpdateDefinition<D>
where
    Self: Into<AnyOperation>,
{
    type Answer = ();
    type Error = UpdateDefinitionError;
}

/// Delete the definition of a kind that has a name, when no other definition uses it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(bound = "", deny_unknown_fields)]
pub struct DeleteDefinition<D: EditableDefinition> {
    /// The name of the definition.
    pub name: D::Name,
}

impl<D: EditableDefinition> Operation for DeleteDefinition<D>
where
    Self: Into<AnyOperation>,
{
    type Answer = ();
    type Error = DeleteDefinitionError;
}

/// Why the control plane did not [list](ListDefinitions) the definitions of a kind.
///
/// ```
/// use gaslightmq_control_api::prelude::*;
///
/// let error = ListDefinitionsError::StoreFailed { reason: "permission denied".to_owned() };
/// assert_eq!(error.to_string(), "the definitions were not listed: permission denied");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
#[serde(tag = "error", rename_all = "kebab-case", deny_unknown_fields)]
pub enum ListDefinitionsError {
    /// The store of the kind could not read its definitions. `reason` is what the store said.
    #[error("the definitions were not listed: {reason}")]
    StoreFailed { reason: String },
}

/// Why the control plane did not [get](GetDefinition) a definition.
///
/// ```
/// use gaslightmq_control_api::prelude::*;
/// use gaslightmq_control_api::rule::RuleName;
///
/// let name: RuleName = "slow-order-producers".parse().expect("a rule name");
/// let error = GetDefinitionError::NotFound { name: name.into() };
/// assert_eq!(error.to_string(), "rule \"slow-order-producers\" does not exist");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
#[serde(tag = "error", rename_all = "kebab-case", deny_unknown_fields)]
pub enum GetDefinitionError {
    /// No definition of the kind has the name.
    #[error("{name} does not exist")]
    NotFound { name: DefinitionRef },
    /// The store of the kind could not read its definitions. `reason` is what the store said.
    #[error("{name} was not read: {reason}")]
    StoreFailed { name: DefinitionRef, reason: String },
}

/// Why the control plane did not [create](CreateDefinition) a definition.
///
/// A create that fails changed nothing.
///
/// ```
/// use gaslightmq_control_api::prelude::*;
/// use gaslightmq_control_api::rule::RuleName;
///
/// let name: RuleName = "slow-order-producers".parse().expect("a rule name");
/// let error = CreateDefinitionError::DoesNotFit {
///     name: name.into(),
///     reason: "rule \"slow-order-producers\" refers to selector \"order-producers\", which does not exist".to_owned(),
/// };
/// assert_eq!(
///     error.to_string(),
///     "rule \"slow-order-producers\" does not fit the workspace: \
///      rule \"slow-order-producers\" refers to selector \"order-producers\", which does not exist",
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
#[serde(tag = "error", rename_all = "kebab-case", deny_unknown_fields)]
pub enum CreateDefinitionError {
    /// A definition of the kind has the name already. Creating never overwrites it.
    #[error("{name} exists already")]
    NameTaken { name: DefinitionRef },
    /// The definitions of the workspace would not fit each other with this one.
    /// `reason` says everything that would be wrong.
    #[error("{name} does not fit the workspace: {reason}")]
    DoesNotFit { name: DefinitionRef, reason: String },
    /// A store could not read its definitions, or could not keep this one. `reason` is what the store said.
    #[error("{name} was not created: {reason}")]
    StoreFailed { name: DefinitionRef, reason: String },
}

/// Why the control plane did not [update](UpdateDefinition) a definition.
///
/// An update that fails changed nothing.
///
/// ```
/// use gaslightmq_control_api::prelude::*;
/// use gaslightmq_control_api::rule::PresetName;
///
/// let name: PresetName = "blackhole".parse().expect("a preset name");
/// let error = UpdateDefinitionError::NotFound { name: name.into() };
/// assert_eq!(error.to_string(), "preset \"blackhole\" does not exist");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
#[serde(tag = "error", rename_all = "kebab-case", deny_unknown_fields)]
pub enum UpdateDefinitionError {
    /// No definition of the kind has the name. Updating never creates one.
    #[error("{name} does not exist")]
    NotFound { name: DefinitionRef },
    /// The definitions of the workspace would not fit each other with the change:
    /// the definition itself, or the definitions that use it. `reason` says everything that would be wrong.
    #[error("{name} does not fit the workspace: {reason}")]
    DoesNotFit { name: DefinitionRef, reason: String },
    /// A store could not read its definitions, or could not keep the change. `reason` is what the store said.
    #[error("{name} was not updated: {reason}")]
    StoreFailed { name: DefinitionRef, reason: String },
}

/// Why the control plane did not [delete](DeleteDefinition) a definition.
///
/// A delete that fails changed nothing.
///
/// ```
/// use gaslightmq_control_api::prelude::*;
/// use gaslightmq_control_api::rule::RuleName;
/// use gaslightmq_control_api::selector::SelectorName;
///
/// let selector: SelectorName = "order-producers".parse().expect("a selector name");
/// let rule: RuleName = "slow-order-producers".parse().expect("a rule name");
/// let error = DeleteDefinitionError::InUse { name: selector.into(), users: vec![rule.into()] };
/// assert_eq!(error.to_string(), "selector \"order-producers\" is used by rule \"slow-order-producers\"");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
#[serde(tag = "error", rename_all = "kebab-case", deny_unknown_fields)]
pub enum DeleteDefinitionError {
    /// No definition of the kind has the name.
    #[error("{name} does not exist")]
    NotFound { name: DefinitionRef },
    /// Other definitions refer to this one, and would refer to nothing without it. `users` names each of them.
    #[error("{name} is used by {}", all(.users))]
    InUse {
        name: DefinitionRef,
        users: Vec<DefinitionRef>,
    },
    /// A store could not read its definitions, or could not remove this one. `reason` is what the store said.
    #[error("{name} was not deleted: {reason}")]
    StoreFailed { name: DefinitionRef, reason: String },
}

/// The definitions as a sentence lists them: `a`, `a and b`, or `a, b and c`.
fn all(users: &[DefinitionRef]) -> String {
    let names: Vec<String> = users.iter().map(ToString::to_string).collect();
    match names.as_slice() {
        [] => String::new(),
        [only] => only.clone(),
        [first @ .., last] => format!("{} and {last}", first.join(", ")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(name: &str) -> DefinitionRef {
        DefinitionRef::from(name.parse::<RuleName>().unwrap())
    }

    #[test]
    fn a_definition_in_use_names_every_user_as_a_sentence_does() {
        let selector = DefinitionRef::from("order-producers".parse::<SelectorName>().unwrap());
        let in_use = |users: Vec<DefinitionRef>| {
            DeleteDefinitionError::InUse {
                name: selector.clone(),
                users,
            }
            .to_string()
        };

        assert_eq!(
            in_use(vec![rule("a"), rule("b")]),
            "selector \"order-producers\" is used by rule \"a\" and rule \"b\""
        );
        assert_eq!(
            in_use(vec![rule("a"), rule("b"), rule("c")]),
            "selector \"order-producers\" is used by rule \"a\", rule \"b\" and rule \"c\""
        );
    }
}
