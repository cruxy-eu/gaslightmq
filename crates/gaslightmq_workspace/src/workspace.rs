//! The workspace: the stores of every kind, and the rules that span them.

mod reload;

pub use reload::{DefinitionChange, Reload};

use std::collections::BTreeMap;

use gaslightmq_definition_api::{Definition, Store, StoreChangeError, StoreCreateError, StoreReadError};
use gaslightmq_environment::Environment;
use gaslightmq_rule::{Preset, Rule, RuleStore};
use gaslightmq_selector::{Selector, SelectorStore};
use gaslightmq_surface::{LayoutStore, PadLayout};

use crate::contents::Contents;
use crate::kind::AnyDefinition;
use crate::{
    CreateError, DefinitionName, DeleteError, Revision, RevisionCounter, Stores, UpdateError, ValidationError,
    WorkspaceContext, WorkspaceDefinition,
};

/// Every definition of a workspace, as one model that holds together.
///
/// A workspace works with a [`WorkspaceContext`]: the [`Stores`] of the definitions, one for each kind.
/// It holds no definition itself, and it knows no files.
/// What it holds is the rules that no single store can keep, because they span the kinds:
///
/// - every reference names a definition that exists: the selector that a selector includes,
///   the selector and the preset of a rule, and the rows and the columns of a pad layout,
/// - no selectors include each other in a circle, and every rule can fire,
/// - a selector or a preset that another definition uses stays.
///
/// Every change goes through the workspace, which checks it against the definitions of every store
/// and then asks one store for it.
/// A change that would break one of the rules fails, and changes nothing.
/// Reading goes to the [stores](Self::stores) directly.
///
/// # Revisions
///
/// A workspace also holds what no store holds: the [`Revision`] of every definition, in memory.
/// It holds the [counter](Self::counter) of the process too, and every change takes its next number from there.
///
/// | When | The revision of the definition |
/// |---|---|
/// | The workspace [loads](Self::new). | The number of the load, for every definition that its stores hold. |
/// | Someone [creates](Self::create) or [updates](Self::update) the definition. | The number of that change. |
/// | Someone [deletes](Self::delete) the definition. | Forgotten. A definition that comes back under the name has a higher one. |
/// | A [reload](Self::reload) finds the definition new or changed. | The number of the reload. |
/// | A reload finds the definition as it was. | The one it had. |
/// | A reload finds the definition gone. | Forgotten. |
///
/// An update and a delete can name the revision they expect.
/// When the definition has another one, someone changed it since, and the change is refused before a store is
/// asked: see [`UpdateError::Changed`] and [`DeleteError::Changed`].
/// Without a revision, the last change wins.
///
/// A workspace is no value to copy: it holds the revisions of one process, so it has no clone.
///
/// # One change at a time
///
/// A change takes the workspace exclusively, so the check and the write of one change belong together.
/// Whoever shares a workspace between tasks puts it behind the lock of its runtime.
///
/// A store can still change behind the back of the workspace, as when someone edits a file.
/// The store goes on holding what it read, and refuses to write over the edit.
/// A [reload](Self::reload) reads every store again, and takes the edit in as one change.
///
/// ```
/// use futures::executor::block_on;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_lie::prelude::*;
/// use gaslightmq_rule::prelude::*;
/// use gaslightmq_selector::prelude::*;
/// use gaslightmq_workspace::prelude::*;
///
/// // Nothing is written anywhere.
/// let mut workspace = Workspace::new(MemoryContext::new());
///
/// let produce = Selector::new(
///     "order-producers".parse().expect("a selector name"),
///     Conditions::new().with(Condition::is(ApiKey::PRODUCE)),
/// );
/// let blackhole = Preset::new("blackhole".parse().expect("a preset name"), LieDefinition::Silence);
/// let rule = Rule::new(
///     "blackhole-order-producers".parse().expect("a rule name"),
///     produce.name().clone(),
///     blackhole.name().clone(),
/// );
///
/// block_on(async {
///     // The rule names a selector and a preset that nobody created yet.
///     let early = workspace.create(rule.clone()).await.unwrap_err();
///     assert!(matches!(early, CreateError::DoesNotFit { .. }));
///
///     workspace.create(produce.clone()).await?;
///     workspace.create(blackhole).await?;
///     workspace.create(rule).await?;
///
///     // The rule uses the selector, so the selector stays.
///     let in_use = workspace.delete(produce.name().clone(), None).await.unwrap_err();
///     assert_eq!(
///         in_use.to_string(),
///         "selector \"order-producers\" is used by rule \"blackhole-order-producers\""
///     );
///     Ok::<(), CreateError>(())
/// })?;
/// # Ok::<(), CreateError>(())
/// ```
#[derive(Debug)]
pub struct Workspace<C: WorkspaceContext> {
    context: C,
    /// The counter of the process, which gives the revision of every change.
    counter: RevisionCounter,
    /// The revision of the load, which every definition has that nobody changed since.
    loaded: Revision,
    /// The revision of every definition that someone created or updated since the load.
    changed: BTreeMap<DefinitionName, Revision>,
}

impl<C: WorkspaceContext> Workspace<C> {
    /// Loads the workspace over the stores of that context.
    ///
    /// The load is the first change of the process: it starts the [counter](Self::counter),
    /// and every definition that the stores hold has [its revision](Self::loaded).
    /// It checks nothing yet: [`validate`](Self::validate) says whether what the stores hold fits together.
    pub fn new(context: C) -> Self {
        let counter = RevisionCounter::new();
        let loaded = counter.next_revision();
        Self {
            context,
            counter,
            loaded,
            changed: BTreeMap::new(),
        }
    }

    /// The stores, to list and to get definitions.
    pub fn stores(&self) -> &C::Stores {
        self.context.stores()
    }

    /// The counter of the process, which gives the revision of every change, whoever makes it.
    ///
    /// A process has one counter, and the workspace holds it.
    /// Whoever changes something that is no definition, such as the state of a rule in an environment,
    /// takes the number of that change from a clone of it.
    pub const fn counter(&self) -> &RevisionCounter {
        &self.counter
    }

    /// The revision of the load: the one that a definition has until someone changes it.
    pub const fn loaded(&self) -> Revision {
        self.loaded
    }

    /// The revision of the definition of that kind and name, which a store of the workspace holds.
    ///
    /// It is the number of the change that last created or updated the definition,
    /// and the [revision of the load](Self::loaded) for a definition that nobody changed since.
    /// The workspace reads no store here, so it cannot say whether the definition exists:
    /// whoever asks holds the definition, and a name that no definition has gets the revision of the load too.
    pub fn revision(&self, name: impl Into<DefinitionName>) -> Revision {
        self.revision_of(&name.into())
    }

    /// Adds a definition, and returns the revision it has.
    ///
    /// A create expects nothing: it refuses a name that is taken, so it never overwrites.
    ///
    /// # Errors
    ///
    /// - [`CreateError::NameTaken`] when a definition of this kind has the name. Nothing overwrites it.
    /// - [`CreateError::DoesNotFit`] when the definitions would not fit each other with this one:
    ///   it refers to a name that does not exist, or it is a rule that can never fire.
    /// - [`CreateError::ChangedOutside`] when someone changed where the store would keep it, since the store read it.
    /// - [`CreateError::StoreFailed`] when a store could not read its definitions, or could not keep this one.
    pub async fn create<D: WorkspaceDefinition>(&mut self, definition: D) -> Result<Revision, CreateError> {
        let name: DefinitionName = definition.name().clone().into();
        self.add(definition, &name).await?;
        Ok(self.count_change_of(name))
    }

    /// Replaces the definition of the same kind and name, and returns the revision it has from then on.
    ///
    /// The definition changes in everything but its name.
    /// With `expects`, the update only changes the definition that has that revision: the one its sender saw.
    /// Without it, the last change wins.
    ///
    /// # Errors
    ///
    /// - [`UpdateError::NotFound`] when no definition of this kind has the name. Nothing is created.
    /// - [`UpdateError::Changed`] when the definition has another revision than `expects`. It says which.
    /// - [`UpdateError::DoesNotFit`] when the definitions would not fit each other with the change:
    ///   the definition refers to a name that does not exist, or a rule can never fire with it,
    ///   which can be a rule that uses the selector or the preset that changes.
    /// - [`UpdateError::ChangedOutside`] when someone changed where the store keeps it, since the store read it.
    /// - [`UpdateError::StoreFailed`] when a store could not read its definitions, or could not keep the change.
    pub async fn update<D: WorkspaceDefinition>(
        &mut self,
        definition: D,
        expects: Option<Revision>,
    ) -> Result<Revision, UpdateError> {
        let name: DefinitionName = definition.name().clone().into();
        let contents = match self.contents().await {
            Ok(contents) => contents,
            Err(Unread { reason }) => return Err(UpdateError::StoreFailed { name, reason }),
        };
        if contents.get(&name).is_none() {
            return Err(UpdateError::NotFound { name });
        }
        if let Err(revision) = self.check(&name, expects) {
            return Err(UpdateError::Changed { name, revision });
        }

        let errors = contents.errors_with(definition.clone().into_any());
        if !errors.is_empty() {
            return Err(UpdateError::DoesNotFit { name, errors });
        }

        match self.store_update(definition.into_any()).await {
            Ok(()) => Ok(self.count_change_of(name)),
            Err(StoreChangeError::NotFound) => Err(UpdateError::NotFound { name }),
            Err(StoreChangeError::ChangedOutside { reason }) => Err(UpdateError::ChangedOutside { name, reason }),
            Err(StoreChangeError::Failed { reason }) => Err(UpdateError::StoreFailed { name, reason }),
        }
    }

    /// Removes the definition of this kind and name, and returns the revision of the delete.
    ///
    /// The type of the name says the kind: a `RuleName` deletes a rule.
    /// The workspace asks the stores who uses the definition, and only those whose definitions can refer to it.
    /// With `expects`, the delete only removes the definition that has that revision: the one its sender saw.
    /// Without it, the last change wins.
    ///
    /// The delete is a change, so it takes a number, and nothing has that revision afterwards:
    /// the workspace forgets the revision of a definition that went.
    /// A definition that someone makes again under the name has a higher revision than the delete.
    ///
    /// # Errors
    ///
    /// - [`DeleteError::NotFound`] when no definition of this kind has the name.
    /// - [`DeleteError::Changed`] when the definition has another revision than `expects`. It says which.
    /// - [`DeleteError::InUse`] when other definitions refer to this one. It names them:
    ///   the selectors that include a selector, the rules that use a selector or a preset,
    ///   and the pad layouts with a row or a column for it.
    /// - [`DeleteError::ChangedOutside`] when someone changed where the store keeps it, since the store read it.
    /// - [`DeleteError::StoreFailed`] when a store could not read its definitions, or could not remove this one.
    pub async fn delete(
        &mut self,
        name: impl Into<DefinitionName>,
        expects: Option<Revision>,
    ) -> Result<Revision, DeleteError> {
        let name = name.into();
        match self.holds(&name).await {
            Ok(true) => {}
            Ok(false) => return Err(DeleteError::NotFound { name }),
            Err(Unread { reason }) => return Err(DeleteError::StoreFailed { name, reason }),
        }
        if let Err(revision) = self.check(&name, expects) {
            return Err(DeleteError::Changed { name, revision });
        }
        let users = match self.users_of(&name).await {
            Ok(users) => users,
            Err(Unread { reason }) => return Err(DeleteError::StoreFailed { name, reason }),
        };
        if !users.is_empty() {
            return Err(DeleteError::InUse { name, users });
        }

        match self.store_delete(&name).await {
            Ok(()) => {
                self.changed.remove(&name);
                Ok(self.counter.next_revision())
            }
            Err(StoreChangeError::NotFound) => Err(DeleteError::NotFound { name }),
            Err(StoreChangeError::ChangedOutside { reason }) => Err(DeleteError::ChangedOutside { name, reason }),
            Err(StoreChangeError::Failed { reason }) => Err(DeleteError::StoreFailed { name, reason }),
        }
    }

    /// Checks that the definitions of every store fit each other.
    ///
    /// Whoever opens a workspace asks this once, and again whenever a store changed behind the workspace.
    ///
    /// # Errors
    ///
    /// - [`ValidationError::Invalid`] with everything that is wrong: a reference that names nothing,
    ///   selectors that include each other in a circle, and a rule that can never fire.
    /// - [`ValidationError::StoreFailed`] when a store could not read its definitions.
    pub async fn validate(&self) -> Result<(), ValidationError> {
        let contents = match self.contents().await {
            Ok(contents) => contents,
            Err(Unread { reason }) => return Err(ValidationError::StoreFailed { reason }),
        };
        let errors = contents.errors();
        if errors.is_empty() {
            Ok(())
        } else {
            Err(ValidationError::Invalid { errors })
        }
    }

    /// The revision of the definition of that name, which a store holds.
    fn revision_of(&self, name: &DefinitionName) -> Revision {
        self.changed.get(name).copied().unwrap_or(self.loaded)
    }

    /// Whether the definition of that name, which a store holds, has the revision that a change expects.
    /// A change that expects nothing finds what it expects.
    ///
    /// # Errors
    ///
    /// The revision that the definition has, when it is another one.
    fn check(&self, name: &DefinitionName, expects: Option<Revision>) -> Result<(), Revision> {
        let revision = self.revision_of(name);
        match expects {
            Some(expected) if expected != revision => Err(revision),
            _ => Ok(()),
        }
    }

    /// Takes the next number for a change that a store took, and keeps it as the revision of the definition.
    fn count_change_of(&mut self, name: DefinitionName) -> Revision {
        let revision = self.counter.next_revision();
        self.changed.insert(name, revision);
        revision
    }

    /// Checks a new definition against what the stores hold, and asks the store of its kind to take it.
    async fn add<D: WorkspaceDefinition>(&self, definition: D, name: &DefinitionName) -> Result<(), CreateError> {
        let failed = |reason| CreateError::StoreFailed {
            name: name.clone(),
            reason,
        };
        let contents = self.contents().await.map_err(|Unread { reason }| failed(reason))?;
        if contents.get(name).is_some() {
            return Err(CreateError::NameTaken { name: name.clone() });
        }

        let errors = contents.errors_with(definition.clone().into_any());
        if !errors.is_empty() {
            return Err(CreateError::DoesNotFit {
                name: name.clone(),
                errors,
            });
        }

        match self.store_create(definition.into_any()).await {
            Ok(()) => Ok(()),
            Err(StoreCreateError::NameTaken) => Err(CreateError::NameTaken { name: name.clone() }),
            Err(StoreCreateError::ChangedOutside { reason }) => Err(CreateError::ChangedOutside {
                name: name.clone(),
                reason,
            }),
            Err(StoreCreateError::Failed { reason }) => Err(failed(reason)),
        }
    }

    /// Everything that the stores hold now.
    async fn contents(&self) -> Result<Contents, Unread> {
        let stores = self.context.stores();
        Ok(Contents::new(
            stores.selectors().list().await.map_err(Unread::by::<Selector>)?,
            stores.presets().list().await.map_err(Unread::by::<Preset>)?,
            stores.rules().list().await.map_err(Unread::by::<Rule>)?,
            stores.layouts().list().await.map_err(Unread::by::<PadLayout>)?,
            stores.environments().list().await.map_err(Unread::by::<Environment>)?,
        ))
    }

    /// Whether the store of its kind holds a definition of this name.
    async fn holds(&self, name: &DefinitionName) -> Result<bool, Unread> {
        let stores = self.context.stores();
        match name {
            DefinitionName::Selector(name) => held(stores.selectors().get(name).await),
            DefinitionName::Preset(name) => held(stores.presets().get(name).await),
            DefinitionName::Rule(name) => held(stores.rules().get(name).await),
            DefinitionName::Layout(name) => held(stores.layouts().get(name).await),
            DefinitionName::Environment(name) => held(stores.environments().get(name).await),
        }
    }

    /// The definitions that refer to the one of this name, in the order of their kinds and names.
    ///
    /// A selector that includes itself is not among them: it goes with what it includes.
    /// Nothing refers to a rule, a pad layout or an environment by name.
    async fn users_of(&self, name: &DefinitionName) -> Result<Vec<DefinitionName>, Unread> {
        let stores = self.context.stores();
        match name {
            DefinitionName::Selector(selector) => {
                let mut users = names(stores.selectors().using_selector(selector).await)?;
                users.retain(|user| user != name);
                users.extend(names(stores.rules().using_selector(selector).await)?);
                users.extend(names(stores.layouts().using_selector(selector).await)?);
                Ok(users)
            }
            DefinitionName::Preset(preset) => {
                let mut users = names(stores.rules().using_preset(preset).await)?;
                users.extend(names(stores.layouts().using_preset(preset).await)?);
                Ok(users)
            }
            DefinitionName::Rule(_) | DefinitionName::Layout(_) | DefinitionName::Environment(_) => Ok(Vec::new()),
        }
    }

    /// Asks the store of its kind to take a definition.
    async fn store_create(&self, definition: AnyDefinition) -> Result<(), StoreCreateError> {
        let stores = self.context.stores();
        match definition {
            AnyDefinition::Selector(selector) => stores.selectors().create(selector).await,
            AnyDefinition::Preset(preset) => stores.presets().create(preset).await,
            AnyDefinition::Rule(rule) => stores.rules().create(rule).await,
            AnyDefinition::Layout(layout) => stores.layouts().create(layout).await,
            AnyDefinition::Environment(environment) => stores.environments().create(environment).await,
        }
    }

    /// Asks the store of its kind to replace a definition.
    async fn store_update(&self, definition: AnyDefinition) -> Result<(), StoreChangeError> {
        let stores = self.context.stores();
        match definition {
            AnyDefinition::Selector(selector) => stores.selectors().update(selector).await,
            AnyDefinition::Preset(preset) => stores.presets().update(preset).await,
            AnyDefinition::Rule(rule) => stores.rules().update(rule).await,
            AnyDefinition::Layout(layout) => stores.layouts().update(layout).await,
            AnyDefinition::Environment(environment) => stores.environments().update(environment).await,
        }
    }

    /// Asks the store of its kind to let go of a definition.
    async fn store_delete(&self, name: &DefinitionName) -> Result<(), StoreChangeError> {
        let stores = self.context.stores();
        match name {
            DefinitionName::Selector(name) => stores.selectors().delete(name).await,
            DefinitionName::Preset(name) => stores.presets().delete(name).await,
            DefinitionName::Rule(name) => stores.rules().delete(name).await,
            DefinitionName::Layout(name) => stores.layouts().delete(name).await,
            DefinitionName::Environment(name) => stores.environments().delete(name).await,
        }
    }
}

/// A store could not read what it holds. `reason` says which store, and what it said.
struct Unread {
    reason: String,
}

impl Unread {
    /// What the store of the definitions `D` said when it could not read, with the word for its kind before it.
    fn by<D: Definition>(error: StoreReadError) -> Self {
        let StoreReadError::Failed { reason } = error;
        Self {
            reason: format!("the {} store could not be read: {reason}", D::LABEL),
        }
    }
}

/// Whether a store answered a get with a definition.
fn held<D: Definition>(got: Result<Option<D>, StoreReadError>) -> Result<bool, Unread> {
    match got {
        Ok(definition) => Ok(definition.is_some()),
        Err(error) => Err(Unread::by::<D>(error)),
    }
}

/// The names of the definitions that a store answered a question with, each with its kind.
fn names<D: WorkspaceDefinition>(answer: Result<Vec<D>, StoreReadError>) -> Result<Vec<DefinitionName>, Unread> {
    match answer {
        Ok(definitions) => Ok(definitions
            .iter()
            .map(|definition| definition.name().clone().into())
            .collect()),
        Err(error) => Err(Unread::by::<D>(error)),
    }
}
