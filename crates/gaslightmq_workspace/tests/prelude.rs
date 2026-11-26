//! The prelude holds every item of the crate, and its names clash with none of the preludes next to it.

#![expect(
    clippy::unwrap_used,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

/// Imports the prelude next to the standard prelude, the modules of `std` that code imports most,
/// the prelude of the store contract, and the preludes of the definitions that a workspace holds.
///
/// All of them are glob imports, so they rank the same.
/// A name that two of them hold is ambiguous, and using it does not compile.
/// This module names every item of the prelude and of the store contract, so it only compiles while no name clashes.
mod next_to_the_others {
    #![expect(unused_imports, reason = "the imports only have to compile next to each other")]

    use std::borrow::*;
    use std::collections::*;
    use std::convert::*;
    use std::error::*;
    use std::fmt::*;
    use std::future::*;
    use std::hash::*;
    use std::io::*;
    use std::net::*;
    use std::num::*;
    use std::ops::*;
    use std::path::*;
    use std::pin::*;
    use std::prelude::rust_2024::*;
    use std::str::*;
    use std::string::*;
    use std::sync::*;
    use std::task::*;
    use std::time::*;

    use gaslightmq_definition_api::prelude::*;
    use gaslightmq_environment::prelude::*;
    use gaslightmq_field::prelude::*;
    use gaslightmq_incident::prelude::*;
    use gaslightmq_kernel::prelude::*;
    use gaslightmq_lie::prelude::*;
    use gaslightmq_rule::prelude::*;
    use gaslightmq_scenario::prelude::*;
    use gaslightmq_selector::prelude::*;
    use gaslightmq_surface::prelude::*;
    use gaslightmq_trigger::prelude::*;
    use gaslightmq_workspace::prelude::*;

    /// The word for one definition of a kind that a workspace holds, with both traits named as bounds.
    fn label_of<D: Definition + WorkspaceDefinition>() -> &'static str {
        D::LABEL
    }

    /// The names that a store holds, with the store named by its trait.
    fn names_in<D: WorkspaceDefinition>(store: &impl Store<D>) -> Vec<DefinitionName> {
        let listed = futures::executor::block_on(store.list()).unwrap();
        listed
            .iter()
            .map(|definition| definition.name().clone().into())
            .collect()
    }

    /// The names of the presets of a workspace, with its context and its stores named by their traits.
    fn presets_of<C: WorkspaceContext<Stores: Stores>>(workspace: &Workspace<C>) -> Vec<DefinitionName> {
        names_in(workspace.stores().presets())
    }

    /// One value of every type of the two preludes, each named without a path.
    pub fn every_item() -> Vec<String> {
        let name: DefinitionName = RuleName::new("slow-order-producers").unwrap().into();
        let label: &str = label_of::<Rule>();
        let mut workspace: Workspace<_> = Workspace::new(MemoryContext::new());
        let blackhole = Preset::new("blackhole".parse().unwrap(), LieDefinition::Silence);
        let counter: RevisionCounter = workspace.counter().clone();
        let created: Revision = futures::executor::block_on(workspace.create(blackhole)).unwrap();
        let held = presets_of(&workspace);

        vec![
            name.to_string(),
            label.to_owned(),
            held.iter().map(ToString::to_string).collect(),
            // The load took the first number, the create the next one, and this change takes the one after it.
            format!("{} < {created} < {}", RevisionCounter::FIRST, counter.next_revision()),
        ]
    }

    /// One error of every type of the two preludes, each named without a path.
    pub fn every_error() -> Vec<String> {
        /// What an error says. A message says everything, so the error has no source.
        fn message_of<E: std::error::Error>(error: E) -> String {
            assert!(error.source().is_none(), "{error}");
            error.to_string()
        }

        let rule: DefinitionName = RuleName::new("slow-order-producers").unwrap().into();
        let selector: DefinitionName = SelectorName::new("order-producers").unwrap().into();
        let unknown = WorkspaceError::UnknownReference {
            definition: rule.clone(),
            reference: selector.clone(),
        };
        vec![
            message_of(StoreReadError::Failed {
                reason: "permission denied".to_owned(),
            }),
            message_of(StoreCreateError::NameTaken),
            message_of(StoreChangeError::NotFound),
            message_of(unknown.clone()),
            message_of(CreateError::DoesNotFit {
                name: rule.clone(),
                errors: vec![unknown.clone()],
            }),
            message_of(UpdateError::NotFound { name: rule.clone() }),
            message_of(UpdateError::Changed {
                name: rule.clone(),
                revision: RevisionCounter::FIRST,
            }),
            message_of(DeleteError::InUse {
                name: selector,
                users: vec![rule],
            }),
            message_of(ValidationError::Invalid { errors: vec![unknown] }),
        ]
    }
}

#[test]
fn the_prelude_names_every_item_next_to_the_other_preludes() {
    assert_eq!(
        next_to_the_others::every_item(),
        [
            "rule \"slow-order-producers\"",
            "rule",
            "preset \"blackhole\"",
            "1 < 2 < 3"
        ]
    );
}

#[test]
fn the_prelude_names_the_error_of_every_type() {
    assert_eq!(
        next_to_the_others::every_error(),
        [
            "the store could not read its definitions: permission denied",
            "the store holds a definition of that name already",
            "the store holds no definition of that name",
            "rule \"slow-order-producers\" refers to selector \"order-producers\", which does not exist",
            "rule \"slow-order-producers\" does not fit the workspace: \
             rule \"slow-order-producers\" refers to selector \"order-producers\", which does not exist",
            "rule \"slow-order-producers\" does not exist",
            "rule \"slow-order-producers\" changed since: it has revision 1 now",
            "selector \"order-producers\" is used by rule \"slow-order-producers\"",
            "the definitions do not fit each other: \
             rule \"slow-order-producers\" refers to selector \"order-producers\", which does not exist",
        ]
    );
}
