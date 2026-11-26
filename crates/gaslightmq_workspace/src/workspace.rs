//! The workspace: the stores of every kind, and the rules that span them.

use gaslightmq_definition_api::{Definition, Store, StoreChangeError, StoreCreateError, StoreReadError};
use gaslightmq_environment::Environment;
use gaslightmq_rule::{Preset, Rule, RuleStore};
use gaslightmq_selector::{Selector, SelectorStore};
use gaslightmq_surface::{LayoutStore, PadLayout};

use crate::contents::Contents;
use crate::held::HeldUuids;
use crate::kind::{AnyDefinition, Kind};
use crate::{
    CreateError, DefinitionName, DeleteError, KeepError, LiveRuleUuid, Stores, UpdateError, UuidSource,
    ValidationError, WorkspaceContext, WorkspaceDefinition,
};

/// Every definition of a workspace, as one model that holds together.
///
/// A workspace works with a [`WorkspaceContext`]: the [`Stores`] of the definitions, one for each kind,
/// and the source of its UUIDs.
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
/// # One change at a time
///
/// A change takes the workspace exclusively, so the check and the write of one change belong together.
/// Whoever shares a workspace between tasks puts it behind the lock of its runtime.
///
/// A store can still change behind the back of the workspace, as when someone edits a file.
/// [`validate`](Self::validate) says whether the definitions still fit each other.
///
/// # UUIDs
///
/// No definition and no store holds a UUID: the UUID of a definition is live state, and the workspace keeps it in
/// memory, next to the name. [`uuid`](Self::uuid) gives it.
///
/// | When | The UUID |
/// |---|---|
/// | The workspace first holds a definition: someone creates it, or the workspace finds it in a store. | A new one. |
/// | Someone updates the definition, through the workspace or behind it. | The same. |
/// | Someone [keeps](Self::keep) a live rule. | The one the live rule had. |
/// | Someone deletes the definition. | Forgotten: a definition under that name later gets a new one. |
///
/// The workspace finds what the stores hold whenever it reads them: on every change and on every
/// [`validate`](Self::validate). A name that it did not hold yet gets a new UUID, and a name that went is forgotten.
/// The next run gives every definition a new UUID: what two runs share is the name.
///
/// The workspace reads neither a clock nor the randomness of the system: the seeds come from the [`UuidSource`]
/// of its context.
///
/// ```
/// use futures::executor::block_on;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_lie::prelude::*;
/// use gaslightmq_rule::prelude::*;
/// use gaslightmq_selector::prelude::*;
/// use gaslightmq_workspace::prelude::*;
///
/// // Nothing is written anywhere, and every UUID is known before the workspace makes it.
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
///     let uuid = workspace.create(rule.clone()).await?;
///
///     // Each definition got a UUID of its own, which the workspace keeps. The rule was the third to get one.
///     assert_eq!(String::from(uuid), KnownUuids::nth(3));
///     assert_eq!(workspace.uuid::<Rule>(rule.name()), Some(uuid));
///
///     // The rule uses the selector, so the selector stays.
///     let in_use = workspace.delete(produce.name().clone()).await.unwrap_err();
///     assert_eq!(
///         in_use.to_string(),
///         "selector \"order-producers\" is used by rule \"blackhole-order-producers\""
///     );
///     Ok::<(), CreateError>(())
/// })?;
/// # Ok::<(), CreateError>(())
/// ```
#[derive(Debug, Clone)]
pub struct Workspace<C: WorkspaceContext> {
    context: C,
    uuids: HeldUuids,
}

impl<C: WorkspaceContext> Workspace<C> {
    /// The workspace over the stores of that context, which takes the seeds of its UUIDs from it.
    ///
    /// It checks nothing yet, and holds no UUID yet: [`validate`](Self::validate) says whether what the stores hold
    /// fits together, and gives every definition that it finds its UUID.
    pub fn new(context: C) -> Self {
        Self {
            context,
            uuids: HeldUuids::default(),
        }
    }

    /// The stores, to list and to get definitions.
    pub fn stores(&self) -> &C::Stores {
        self.context.stores()
    }

    /// The UUID of the definition of the kind `D` and this name, or `None` when the workspace does not hold one.
    ///
    /// The workspace holds a definition from the moment someone creates it, or it finds it in a store,
    /// until someone deletes it.
    pub fn uuid<D: WorkspaceDefinition>(&self, name: &D::Name) -> Option<D::Uuid> {
        self.uuids.get::<D>(&name.clone().into())
    }

    /// Adds a definition, and returns the UUID that it got.
    ///
    /// The definition gets a new UUID: a copy of a definition is a definition of its own.
    ///
    /// # Errors
    ///
    /// - [`CreateError::NameTaken`] when a definition of this kind has the name. Nothing overwrites it.
    /// - [`CreateError::DoesNotFit`] when the definitions would not fit each other with this one:
    ///   it refers to a name that does not exist, or it is a rule that can never fire.
    /// - [`CreateError::StoreFailed`] when a store could not read its definitions, or could not keep this one.
    pub async fn create<D: WorkspaceDefinition>(&mut self, definition: D) -> Result<D::Uuid, CreateError> {
        let name: DefinitionName = definition.name().clone().into();
        self.add(definition, &name).await?;
        let uuid = D::new_uuid(self.context.uuids().seed());
        self.uuids.hold::<D>(name, uuid);
        Ok(uuid)
    }

    /// Adds a live rule to the workspace, under the UUID that the workspace gave it when someone made it.
    ///
    /// A live rule gets its UUID from [`live_rule_uuid`](Self::live_rule_uuid), and only from there,
    /// so a rule of the workspace never has a UUID that someone made up. Keeping spends it.
    ///
    /// # Errors
    ///
    /// A [`KeepError`] with the UUID of the live rule, which stays live, and the [`CreateError`] of what went wrong:
    ///
    /// - [`CreateError::NameTaken`] when a rule of the workspace has the name. Nothing overwrites it.
    /// - [`CreateError::DoesNotFit`] when the definitions would not fit each other with this one:
    ///   it refers to a name that does not exist, or it is a rule that can never fire.
    /// - [`CreateError::StoreFailed`] when a store could not read its definitions, or could not keep this one.
    pub async fn keep(&mut self, rule: Rule, uuid: LiveRuleUuid) -> Result<(), KeepError> {
        let name: DefinitionName = rule.name().clone().into();
        if let Err(error) = self.add(rule, &name).await {
            return Err(KeepError { error, uuid });
        }
        self.uuids.hold::<Rule>(name, uuid.uuid());
        Ok(())
    }

    /// A new UUID for a live rule, as [`create`](Self::create) gives one, and stores nothing.
    ///
    /// A live rule lives in an environment for a while, and the environment holds its UUID.
    /// It gets the UUID from here when someone makes it, and keeps it once someone [keeps](Self::keep) it.
    pub fn live_rule_uuid(&mut self) -> LiveRuleUuid {
        LiveRuleUuid::new(Rule::new_uuid(self.context.uuids().seed()))
    }

    /// Replaces the definition of the same kind and name, and returns the UUID that it keeps.
    ///
    /// The definition changes in everything but its name, and keeps its UUID.
    ///
    /// # Errors
    ///
    /// - [`UpdateError::NotFound`] when no definition of this kind has the name. Nothing is created.
    /// - [`UpdateError::DoesNotFit`] when the definitions would not fit each other with the change:
    ///   the definition refers to a name that does not exist, or a rule can never fire with it,
    ///   which can be a rule that uses the selector or the preset that changes.
    /// - [`UpdateError::StoreFailed`] when a store could not read its definitions, or could not keep the change.
    pub async fn update<D: WorkspaceDefinition>(&mut self, definition: D) -> Result<D::Uuid, UpdateError> {
        let name: DefinitionName = definition.name().clone().into();
        let contents = match self.contents().await {
            Ok(contents) => contents,
            Err(Unread { reason }) => return Err(UpdateError::StoreFailed { name, reason }),
        };
        // The workspace took in what the stores hold, so it holds a UUID for every definition that they have.
        let Some(uuid) = self.uuids.get::<D>(&name) else {
            return Err(UpdateError::NotFound { name });
        };

        let errors = contents.errors_with(definition.clone().into_any());
        if !errors.is_empty() {
            return Err(UpdateError::DoesNotFit { name, errors });
        }

        match self.store_update(definition.into_any()).await {
            Ok(()) => Ok(uuid),
            Err(StoreChangeError::NotFound) => Err(UpdateError::NotFound { name }),
            Err(StoreChangeError::Failed { reason }) => Err(UpdateError::StoreFailed { name, reason }),
        }
    }

    /// Removes the definition of this kind and name.
    ///
    /// The type of the name says the kind: a `RuleName` deletes a rule.
    /// The workspace asks the stores who uses the definition, and only those whose definitions can refer to it.
    ///
    /// # Errors
    ///
    /// - [`DeleteError::NotFound`] when no definition of this kind has the name.
    /// - [`DeleteError::InUse`] when other definitions refer to this one. It names them:
    ///   the selectors that include a selector, the rules that use a selector or a preset,
    ///   and the pad layouts with a row or a column for it.
    /// - [`DeleteError::StoreFailed`] when a store could not read its definitions, or could not remove this one.
    pub async fn delete(&mut self, name: impl Into<DefinitionName>) -> Result<(), DeleteError> {
        let name = name.into();
        match self.holds(&name).await {
            Ok(true) => {}
            Ok(false) => return Err(DeleteError::NotFound { name }),
            Err(Unread { reason }) => return Err(DeleteError::StoreFailed { name, reason }),
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
                self.uuids.forget(&name);
                Ok(())
            }
            Err(StoreChangeError::NotFound) => Err(DeleteError::NotFound { name }),
            Err(StoreChangeError::Failed { reason }) => Err(DeleteError::StoreFailed { name, reason }),
        }
    }

    /// Checks that the definitions of every store fit each other.
    ///
    /// Whoever opens a workspace asks this once, and again whenever a store changed behind the workspace.
    /// The workspace takes in what the stores hold, whether it fits or not:
    /// a definition that it finds for the first time gets its UUID, and one that went is forgotten.
    ///
    /// # Errors
    ///
    /// - [`ValidationError::Invalid`] with everything that is wrong: a reference that names nothing,
    ///   selectors that include each other in a circle, and a rule that can never fire.
    /// - [`ValidationError::StoreFailed`] when a store could not read its definitions.
    pub async fn validate(&mut self) -> Result<(), ValidationError> {
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

    /// Checks a new definition against what the stores hold, and asks the store of its kind to take it.
    async fn add<D: WorkspaceDefinition>(&mut self, definition: D, name: &DefinitionName) -> Result<(), CreateError> {
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
            Err(StoreCreateError::Failed { reason }) => Err(failed(reason)),
        }
    }

    /// Everything that the stores hold now, with a UUID for every definition of it.
    ///
    /// A definition that the workspace did not hold yet gets a new UUID, and the UUID of one that went is forgotten.
    async fn contents(&mut self) -> Result<Contents, Unread> {
        let stores = self.context.stores();
        let contents = Contents::new(
            stores.selectors().list().await.map_err(Unread::by::<Selector>)?,
            stores.presets().list().await.map_err(Unread::by::<Preset>)?,
            stores.rules().list().await.map_err(Unread::by::<Rule>)?,
            stores.layouts().list().await.map_err(Unread::by::<PadLayout>)?,
            stores.environments().list().await.map_err(Unread::by::<Environment>)?,
        );
        self.uuids.take_in(contents.names(), self.context.uuids());
        Ok(contents)
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
