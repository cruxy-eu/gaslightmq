//! The definitions of the workspace, as the control plane reads and changes them.

use gaslightmq_control_api::environment::Environment;
use gaslightmq_control_api::journal::ChangeSource;
use gaslightmq_control_api::prelude::*;
use gaslightmq_control_api::rule::{Preset, Rule};
use gaslightmq_control_api::scenario::Scenario;
use gaslightmq_control_api::selector::Selector;
use gaslightmq_control_api::surface::PadLayout;
use gaslightmq_definition_api::{Definition, Store, StoreReadError};
use gaslightmq_host::Host;
use gaslightmq_workspace::{
    CreateError, DefinitionName, DeleteError, Stores, UpdateError, Workspace, WorkspaceContext, WorkspaceDefinition,
    WorkspaceError,
};

use crate::revisions::{expected, revised, revision, run};

/// A kind of definition that the server reads from the stores of a workspace, and hands out as its store holds it.
pub(crate) trait Stored: Definition {
    /// The store of this kind among the stores of a workspace.
    fn store<S: Stores>(stores: &S) -> &impl Store<Self>;
}

/// A kind of definition that the control plane lists, gets, creates, updates and deletes.
pub(crate) trait Editable:
    Stored + EditableDefinition + WorkspaceDefinition + Definition<Name = <Self as EditableDefinition>::Name>
{
}

/// Makes a definition type one that the server reads from its store.
macro_rules! stored {
    ($($definition:ident => $store:ident;)*) => {
        $(
            impl Stored for $definition {
                fn store<S: Stores>(stores: &S) -> &impl Store<Self> {
                    stores.$store()
                }
            }
        )*
    };
}

stored! {
    Selector => selectors;
    Preset => presets;
    Rule => rules;
    Scenario => scenarios;
    PadLayout => layouts;
    Environment => environments;
}

impl Editable for Selector {}
impl Editable for Preset {}
impl Editable for Rule {}
impl Editable for PadLayout {}

/// Every definition of the kind `D`.
///
/// # Errors
///
/// What the store said when it could not read, after the word for its kind.
pub(crate) async fn list<D: Stored, C: WorkspaceContext>(workspace: &Workspace<C>) -> Result<Vec<D>, String> {
    D::store(workspace.stores()).list().await.map_err(unread::<D>)
}

/// The definition of the kind `D` with that name, or `None` when no definition has the name.
///
/// # Errors
///
/// What the store said when it could not read, after the word for its kind.
pub(crate) async fn get<D: Stored, C: WorkspaceContext>(
    workspace: &Workspace<C>,
    name: &D::Name,
) -> Result<Option<D>, String> {
    D::store(workspace.stores()).get(name).await.map_err(unread::<D>)
}

/// Every definition of the kind `D`, each with what the workspace keeps beside it.
///
/// # Errors
///
/// What the store said when it could not read, after the word for its kind.
pub(crate) async fn list_with_revisions<D: Stored + WorkspaceDefinition, C: WorkspaceContext>(
    workspace: &Workspace<C>,
) -> Result<Vec<DefinitionStatus<D>>, String> {
    let definitions = list::<D, C>(workspace).await?;
    let mut statuses = Vec::with_capacity(definitions.len());
    for definition in definitions {
        statuses.push(with_revision(workspace, definition).await?);
    }
    Ok(statuses)
}

/// The definition, with what the workspace keeps beside it: its revision, and whether a write of it would lose a
/// comment of its file.
///
/// # Errors
///
/// What the store said when it could not read, after the word for its kind.
pub(crate) async fn with_revision<D: Stored + WorkspaceDefinition, C: WorkspaceContext>(
    workspace: &Workspace<C>,
    definition: D,
) -> Result<DefinitionStatus<D>, String> {
    let loses_comment = D::store(workspace.stores())
        .would_lose_comment(definition.name())
        .await
        .map_err(unread::<D>)?;
    Ok(DefinitionStatus {
        revision: revision(workspace.revision(definition.name().clone())),
        definition,
        loses_comment,
    })
}

/// Whether a write of the definition of that name and kind would lose a comment of its file.
///
/// # Errors
///
/// What the store of its kind said when it could not read.
pub(crate) async fn loses_comment<C: WorkspaceContext>(
    workspace: &Workspace<C>,
    name: &DefinitionName,
) -> Result<bool, String> {
    let stores = workspace.stores();
    match name {
        DefinitionName::Selector(name) => Selector::store(stores)
            .would_lose_comment(name)
            .await
            .map_err(unread::<Selector>),
        DefinitionName::Preset(name) => Preset::store(stores)
            .would_lose_comment(name)
            .await
            .map_err(unread::<Preset>),
        DefinitionName::Rule(name) => Rule::store(stores)
            .would_lose_comment(name)
            .await
            .map_err(unread::<Rule>),
        DefinitionName::Scenario(name) => Scenario::store(stores)
            .would_lose_comment(name)
            .await
            .map_err(unread::<Scenario>),
        DefinitionName::Layout(name) => PadLayout::store(stores)
            .would_lose_comment(name)
            .await
            .map_err(unread::<PadLayout>),
        DefinitionName::Environment(name) => Environment::store(stores)
            .would_lose_comment(name)
            .await
            .map_err(unread::<Environment>),
    }
}

/// Every definition of the workspace, by kind, each with its revision.
///
/// # Errors
///
/// What the first store that could not read said, after the word for its kind.
pub(crate) async fn every<C: WorkspaceContext>(workspace: &Workspace<C>) -> Result<Definitions, String> {
    Ok(Definitions {
        selectors: list_with_revisions(workspace).await?,
        presets: list_with_revisions(workspace).await?,
        rules: list_with_revisions(workspace).await?,
        scenarios: list_with_revisions(workspace).await?,
        layouts: list_with_revisions(workspace).await?,
        environments: list_with_revisions(workspace).await?,
    })
}

/// The operations on the definitions of one kind, over the workspace of a host.
pub(crate) struct DefinitionOperations;

impl DefinitionOperations {
    /// Answers [`ListDefinitions`].
    pub(crate) async fn list<D: Editable, C: WorkspaceContext>(
        workspace: &Workspace<C>,
    ) -> Result<Vec<DefinitionStatus<D>>, ListDefinitionsError> {
        list_with_revisions(workspace)
            .await
            .map_err(|reason| ListDefinitionsError::StoreFailed { reason })
    }

    /// Answers [`GetDefinition`].
    pub(crate) async fn get<D: Editable, C: WorkspaceContext>(
        workspace: &Workspace<C>,
        name: <D as EditableDefinition>::Name,
    ) -> Result<DefinitionStatus<D>, GetDefinitionError> {
        let found = match get::<D, C>(workspace, &name).await {
            Ok(Some(definition)) => with_revision(workspace, definition).await.map(Some),
            Ok(None) => Ok(None),
            Err(reason) => Err(reason),
        };
        match found {
            Ok(Some(status)) => Ok(status),
            Ok(None) => Err(GetDefinitionError::NotFound {
                name: Into::<DefinitionRef>::into(name),
            }),
            Err(reason) => Err(GetDefinitionError::StoreFailed {
                name: Into::<DefinitionRef>::into(name),
                reason,
            }),
        }
    }

    /// Answers [`CreateDefinition`], through the host, which records where the change comes from and tells
    /// whoever follows it.
    pub(crate) async fn create<D: Editable, C: WorkspaceContext>(
        host: &Host<C>,
        definition: D,
        from: ChangeSource,
    ) -> Result<Revised, CreateDefinitionError> {
        use gaslightmq_host::CreateDefinitionError as Host;
        match host.create(definition, from).await {
            Ok(created) => Ok(revised(created)),
            Err(Host::NameTaken { rule, holder }) => Err(CreateDefinitionError::NameTaken {
                name: rule.into(),
                holder: Some(rule_holder(holder)),
            }),
            Err(Host::NotCreated {
                error: CreateError::NameTaken { name },
            }) => Err(CreateDefinitionError::NameTaken {
                name: reference(name),
                holder: None,
            }),
            Err(Host::NotCreated {
                error: CreateError::DoesNotFit { name, errors },
            }) => Err(CreateDefinitionError::DoesNotFit {
                name: reference(name),
                reason: each(&errors),
            }),
            Err(Host::NotCreated {
                error: CreateError::ChangedOutside { name, reason },
            }) => Err(CreateDefinitionError::ChangedOutside {
                name: reference(name),
                reason,
            }),
            Err(Host::NotCreated {
                error: CreateError::StoreFailed { name, reason },
            }) => Err(CreateDefinitionError::StoreFailed {
                name: reference(name),
                reason,
            }),
        }
    }

    /// Answers [`UpdateDefinition`], through the host, which tells whoever follows it.
    pub(crate) async fn update<D: Editable, C: WorkspaceContext>(
        host: &Host<C>,
        update: UpdateDefinition<D>,
        from: ChangeSource,
    ) -> Result<Revised, UpdateDefinitionError> {
        use gaslightmq_host::UpdateDefinitionError as Host;
        let UpdateDefinition { expects, definition } = update;
        let error = match host.update(definition, expected(expects), from).await {
            Ok(updated) => return Ok(revised(updated)),
            Err(Host::OtherRun { run: of_the_host }) => {
                return Err(UpdateDefinitionError::OtherRun { run: run(of_the_host) });
            }
            Err(Host::Unresolved {
                name,
                environment,
                rule,
                reason,
            }) => {
                return Err(UpdateDefinitionError::LiveRuleDoesNotFit {
                    name: reference(name),
                    environment,
                    rule,
                    reason,
                });
            }
            Err(Host::NotUpdated { error }) => error,
        };
        Err(match error {
            UpdateError::NotFound { name } => UpdateDefinitionError::NotFound { name: reference(name) },
            UpdateError::Changed { name, revision: now } => UpdateDefinitionError::Changed {
                name: reference(name),
                revision: revision(now),
            },
            UpdateError::DoesNotFit { name, errors } => UpdateDefinitionError::DoesNotFit {
                name: reference(name),
                reason: each(&errors),
            },
            UpdateError::ChangedOutside { name, reason } => UpdateDefinitionError::ChangedOutside {
                name: reference(name),
                reason,
            },
            UpdateError::StoreFailed { name, reason } => UpdateDefinitionError::StoreFailed {
                name: reference(name),
                reason,
            },
        })
    }

    /// Answers [`DeleteDefinition`], through the host, which tells whoever follows it.
    pub(crate) async fn delete<D: Editable, C: WorkspaceContext>(
        host: &Host<C>,
        delete: DeleteDefinition<D>,
        from: ChangeSource,
    ) -> Result<Revised, DeleteDefinitionError> {
        use gaslightmq_host::DeleteDefinitionError as Host;
        let DeleteDefinition { name, expects } = delete;
        let error = match host.delete(name, expected(expects), from).await {
            Ok(deleted) => return Ok(revised(deleted)),
            Err(Host::OtherRun { run: of_the_host }) => {
                return Err(DeleteDefinitionError::OtherRun { run: run(of_the_host) });
            }
            Err(Host::Unresolved {
                name,
                environment,
                rule,
                reason,
            }) => {
                return Err(DeleteDefinitionError::LiveRuleDoesNotFit {
                    name: reference(name),
                    environment,
                    rule,
                    reason,
                });
            }
            Err(Host::NotDeleted { error }) => error,
        };
        Err(match error {
            DeleteError::NotFound { name } => DeleteDefinitionError::NotFound { name: reference(name) },
            DeleteError::Changed { name, revision: now } => DeleteDefinitionError::Changed {
                name: reference(name),
                revision: revision(now),
            },
            DeleteError::InUse { name, users } => DeleteDefinitionError::InUse {
                name: reference(name),
                users: users.into_iter().map(reference).collect(),
            },
            DeleteError::ChangedOutside { name, reason } => DeleteDefinitionError::ChangedOutside {
                name: reference(name),
                reason,
            },
            DeleteError::StoreFailed { name, reason } => DeleteDefinitionError::StoreFailed {
                name: reference(name),
                reason,
            },
        })
    }
}

/// The definition of that name and kind, or `None` when the workspace does not have it.
///
/// # Errors
///
/// What the store of its kind said when it could not read.
pub(crate) async fn named<C: WorkspaceContext>(
    workspace: &Workspace<C>,
    name: &DefinitionName,
) -> Result<Option<AnyDefinition>, String> {
    Ok(match name {
        DefinitionName::Selector(name) => get::<Selector, C>(workspace, name).await?.map(AnyDefinition::Selector),
        DefinitionName::Preset(name) => get::<Preset, C>(workspace, name).await?.map(AnyDefinition::Preset),
        DefinitionName::Rule(name) => get::<Rule, C>(workspace, name).await?.map(AnyDefinition::Rule),
        DefinitionName::Scenario(name) => get::<Scenario, C>(workspace, name).await?.map(AnyDefinition::Scenario),
        DefinitionName::Layout(name) => get::<PadLayout, C>(workspace, name).await?.map(AnyDefinition::Layout),
        DefinitionName::Environment(name) => get::<Environment, C>(workspace, name)
            .await?
            .map(AnyDefinition::Environment),
    })
}

/// What holds a rule of a name outside the workspace, as the control plane writes it.
pub(crate) fn rule_holder(holder: gaslightmq_host::RuleHolder) -> RuleHolder {
    match holder {
        gaslightmq_host::RuleHolder::Environment(environment) => RuleHolder::Environment(environment),
        gaslightmq_host::RuleHolder::Scenario(scenario) => RuleHolder::Scenario(scenario),
    }
}

/// The name of a definition with its kind, as the control plane writes it.
pub(crate) fn reference(name: DefinitionName) -> DefinitionRef {
    match name {
        DefinitionName::Selector(name) => name.into(),
        DefinitionName::Preset(name) => name.into(),
        DefinitionName::Rule(name) => name.into(),
        DefinitionName::Scenario(name) => name.into(),
        DefinitionName::Layout(name) => name.into(),
        DefinitionName::Environment(name) => name.into(),
    }
}

/// Everything that keeps the definitions from fitting each other, in one line, with `; ` between them.
pub(crate) fn each(errors: &[WorkspaceError]) -> String {
    let messages: Vec<String> = errors.iter().map(ToString::to_string).collect();
    messages.join("; ")
}

/// What the store of the definitions `D` said when it could not read, with the word for its kind before it.
fn unread<D: Definition>(error: StoreReadError) -> String {
    let StoreReadError::Failed { reason } = error;
    format!("the {} store could not be read: {reason}", D::LABEL)
}
