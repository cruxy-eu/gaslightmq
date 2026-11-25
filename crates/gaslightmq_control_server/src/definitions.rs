//! The definitions of the workspace, as the control plane reads and changes them.

use gaslightmq_control_api::environment::Environment;
use gaslightmq_control_api::prelude::*;
use gaslightmq_control_api::rule::{Preset, Rule};
use gaslightmq_control_api::selector::Selector;
use gaslightmq_control_api::surface::PadLayout;
use gaslightmq_definition_api::{Definition, Store, StoreReadError};
use gaslightmq_workspace::{
    CreateError, DefinitionName, DeleteError, Stores, UpdateError, Workspace, WorkspaceContext, WorkspaceDefinition,
    WorkspaceError,
};

/// A kind of definition that the server reads from the stores of a workspace, and hands out without its UUID.
pub(crate) trait Stored: Definition {
    /// The store of this kind among the stores of a workspace.
    fn store<S: Stores>(stores: &S) -> &impl Store<Self>;

    /// The definition as the control plane hands it out: without its UUID, which stays inside the workspace.
    #[must_use]
    fn without_uuid(self) -> Self;
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

                fn without_uuid(self) -> Self {
                    $definition::without_uuid(self)
                }
            }
        )*
    };
}

stored! {
    Selector => selectors;
    Preset => presets;
    Rule => rules;
    PadLayout => layouts;
    Environment => environments;
}

impl Editable for Selector {}
impl Editable for Preset {}
impl Editable for Rule {}
impl Editable for PadLayout {}

/// Every definition of the kind `D`, without their UUIDs.
///
/// # Errors
///
/// What the store said when it could not read, after the word for its kind.
pub(crate) async fn list<D: Stored, C: WorkspaceContext>(workspace: &Workspace<C>) -> Result<Vec<D>, String> {
    match D::store(workspace.stores()).list().await {
        Ok(definitions) => Ok(definitions.into_iter().map(Stored::without_uuid).collect()),
        Err(error) => Err(unread::<D>(error)),
    }
}

/// The definition of the kind `D` with that name, without its UUID, or `None` when no definition has the name.
///
/// # Errors
///
/// What the store said when it could not read, after the word for its kind.
pub(crate) async fn get<D: Stored, C: WorkspaceContext>(
    workspace: &Workspace<C>,
    name: &D::Name,
) -> Result<Option<D>, String> {
    match D::store(workspace.stores()).get(name).await {
        Ok(definition) => Ok(definition.map(Stored::without_uuid)),
        Err(error) => Err(unread::<D>(error)),
    }
}

/// Every definition of the workspace, by kind, without their UUIDs.
///
/// # Errors
///
/// What the first store that could not read said, after the word for its kind.
pub(crate) async fn every<C: WorkspaceContext>(workspace: &Workspace<C>) -> Result<Definitions, String> {
    Ok(Definitions {
        selectors: list(workspace).await?,
        presets: list(workspace).await?,
        rules: list(workspace).await?,
        layouts: list(workspace).await?,
        environments: list(workspace).await?,
    })
}

/// The operations on the definitions of one kind, over the workspace of a host.
pub(crate) struct DefinitionOperations;

impl DefinitionOperations {
    /// Answers [`ListDefinitions`].
    pub(crate) async fn list<D: Editable, C: WorkspaceContext>(
        workspace: &Workspace<C>,
    ) -> Result<Vec<D>, ListDefinitionsError> {
        list(workspace)
            .await
            .map_err(|reason| ListDefinitionsError::StoreFailed { reason })
    }

    /// Answers [`GetDefinition`].
    pub(crate) async fn get<D: Editable, C: WorkspaceContext>(
        workspace: &Workspace<C>,
        name: <D as EditableDefinition>::Name,
    ) -> Result<D, GetDefinitionError> {
        match get::<D, C>(workspace, &name).await {
            Ok(Some(definition)) => Ok(definition),
            Ok(None) => Err(GetDefinitionError::NotFound {
                name: Into::<DefinitionRef>::into(name),
            }),
            Err(reason) => Err(GetDefinitionError::StoreFailed {
                name: Into::<DefinitionRef>::into(name),
                reason,
            }),
        }
    }

    /// Answers [`CreateDefinition`], and returns the definition as the workspace keeps it, without its UUID.
    pub(crate) async fn create<D: Editable, C: WorkspaceContext>(
        workspace: &mut Workspace<C>,
        definition: D,
    ) -> Result<D, CreateDefinitionError> {
        match workspace.create(definition).await {
            Ok(created) => Ok(created.without_uuid()),
            Err(CreateError::NameTaken { name }) => Err(CreateDefinitionError::NameTaken { name: reference(name) }),
            Err(CreateError::DoesNotFit { name, errors }) => Err(CreateDefinitionError::DoesNotFit {
                name: reference(name),
                reason: each(&errors),
            }),
            Err(CreateError::StoreFailed { name, reason }) => Err(CreateDefinitionError::StoreFailed {
                name: reference(name),
                reason,
            }),
        }
    }

    /// Answers [`UpdateDefinition`], and returns the definition as the workspace keeps it, without its UUID.
    pub(crate) async fn update<D: Editable, C: WorkspaceContext>(
        workspace: &mut Workspace<C>,
        definition: D,
    ) -> Result<D, UpdateDefinitionError> {
        match workspace.update(definition).await {
            Ok(updated) => Ok(updated.without_uuid()),
            Err(UpdateError::NotFound { name }) => Err(UpdateDefinitionError::NotFound { name: reference(name) }),
            Err(UpdateError::DoesNotFit { name, errors }) => Err(UpdateDefinitionError::DoesNotFit {
                name: reference(name),
                reason: each(&errors),
            }),
            Err(UpdateError::StoreFailed { name, reason }) => Err(UpdateDefinitionError::StoreFailed {
                name: reference(name),
                reason,
            }),
        }
    }

    /// Answers [`DeleteDefinition`].
    pub(crate) async fn delete<D: Editable, C: WorkspaceContext>(
        workspace: &mut Workspace<C>,
        name: <D as EditableDefinition>::Name,
    ) -> Result<(), DeleteDefinitionError> {
        match workspace.delete(name).await {
            Ok(()) => Ok(()),
            Err(DeleteError::NotFound { name }) => Err(DeleteDefinitionError::NotFound { name: reference(name) }),
            Err(DeleteError::InUse { name, users }) => Err(DeleteDefinitionError::InUse {
                name: reference(name),
                users: users.into_iter().map(reference).collect(),
            }),
            Err(DeleteError::StoreFailed { name, reason }) => Err(DeleteDefinitionError::StoreFailed {
                name: reference(name),
                reason,
            }),
        }
    }
}

/// The name of a definition with its kind, as the control plane writes it.
pub(crate) fn reference(name: DefinitionName) -> DefinitionRef {
    match name {
        DefinitionName::Selector(name) => name.into(),
        DefinitionName::Preset(name) => name.into(),
        DefinitionName::Rule(name) => name.into(),
        DefinitionName::Layout(name) => name.into(),
        DefinitionName::Environment(name) => name.into(),
    }
}

/// Everything that keeps the definitions from fitting each other, in one line, with `; ` between them.
fn each(errors: &[WorkspaceError]) -> String {
    let messages: Vec<String> = errors.iter().map(ToString::to_string).collect();
    messages.join("; ")
}

/// What the store of the definitions `D` said when it could not read, with the word for its kind before it.
fn unread<D: Definition>(error: StoreReadError) -> String {
    let StoreReadError::Failed { reason } = error;
    format!("the {} store could not be read: {reason}", D::LABEL)
}
