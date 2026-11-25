//! Where environments are kept: the store of the environments.

use gaslightmq_definition_api::{Definition, Store};

use crate::{Environment, EnvironmentName};

/// An environment is a definition: a store holds it under its [`EnvironmentName`].
impl Definition for Environment {
    type Name = EnvironmentName;

    const LABEL: &'static str = "environment";

    fn name(&self) -> &EnvironmentName {
        Environment::name(self)
    }
}

/// What keeps the environments: the files of a workspace, memory, or anything else.
///
/// A store of environments keeps the [store contract](Store), and nothing asks it more than that.
/// So a store of environments is one line: `impl EnvironmentStore for MyStore {}`.
///
/// ```
/// use gaslightmq_definition_api::prelude::*;
/// use gaslightmq_environment::prelude::*;
///
/// /// Whether someone defined an environment of this name.
/// async fn is_defined(environments: &impl EnvironmentStore, name: &EnvironmentName) -> Result<bool, StoreReadError> {
///     Ok(environments.get(name).await?.is_some())
/// }
/// ```
pub trait EnvironmentStore: Store<Environment> {}

/// A store of environments in memory is an [`EnvironmentStore`].
#[cfg(feature = "memory")]
impl EnvironmentStore for gaslightmq_definition_memory::MemoryStore<Environment> {}
