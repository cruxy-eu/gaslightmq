//! The prelude holds every type, and its names clash with none of `std`
//! or of the preludes of the domains that this crate re-exports.

/// Imports the prelude next to the standard prelude, the modules of `std` that code imports most, the clock,
/// and the prelude of every domain whose types the control plane carries.
///
/// All of them are glob imports, so they rank the same.
/// A name that two of them hold is ambiguous, and using it does not compile.
/// This module names every item of the prelude, so it only compiles while no name clashes.
mod next_to_std_and_every_domain {
    #![expect(unused_imports, reason = "the imports only have to compile next to each other")]

    use std::borrow::*;
    use std::collections::*;
    use std::convert::*;
    use std::error::*;
    use std::fmt::*;
    use std::hash::*;
    use std::io::*;
    use std::net::*;
    use std::num::*;
    use std::ops::*;
    use std::path::*;
    use std::prelude::rust_2024::*;
    use std::str::*;
    use std::string::*;
    use std::sync::*;
    use std::time::*;

    use gaslightmq_control_api::call::prelude::*;
    use gaslightmq_control_api::clock::*;
    use gaslightmq_control_api::environment::prelude::*;
    use gaslightmq_control_api::field::prelude::*;
    use gaslightmq_control_api::incident::prelude::*;
    use gaslightmq_control_api::kernel::prelude::*;
    use gaslightmq_control_api::lie::prelude::*;
    use gaslightmq_control_api::prelude::*;
    use gaslightmq_control_api::rule::prelude::*;
    use gaslightmq_control_api::selector::prelude::*;
    use gaslightmq_control_api::surface::prelude::*;
    use gaslightmq_control_api::trigger::prelude::*;

    /// An operation that answers with `A` and fails with `E`. A call only compiles when both are right.
    fn answers<O: Operation<Answer = A, Error = E>, A, E>() {}

    /// A type of the prelude, named without a path.
    fn named<T>() {}

    /// A kind of definition that the control plane edits.
    fn editable<D: EditableDefinition>() {}

    /// Names every operation with what it answers and why it fails, and every other type of the prelude.
    pub fn every_item() {
        answers::<ListDefinitions<Selector>, Vec<Selector>, ListDefinitionsError>();
        answers::<GetDefinition<Preset>, Preset, GetDefinitionError>();
        answers::<CreateDefinition<Rule>, (), CreateDefinitionError>();
        answers::<UpdateDefinition<PadLayout>, (), UpdateDefinitionError>();
        answers::<DeleteDefinition<Selector>, (), DeleteDefinitionError>();
        answers::<ListEnvironments, Vec<EnvironmentStatus>, ListDefinitionsError>();
        answers::<GetEnvironment, EnvironmentStatus, GetDefinitionError>();
        answers::<StartEnvironment, (), StartEnvironmentError>();
        answers::<StopEnvironment, (), EnvironmentNotRunning>();
        answers::<ListEnvironmentRules, Vec<RuleStatus>, EnvironmentNotRunning>();
        answers::<CreateLiveRule, (), CreateLiveRuleError>();
        answers::<UpdateLiveRule, (), UpdateLiveRuleError>();
        answers::<DeleteLiveRule, (), DeleteLiveRuleError>();
        answers::<KeepRule, (), KeepRuleError>();
        answers::<ArmRule, (), RuleCommandError>();
        answers::<DisarmRule, (), RuleCommandError>();
        answers::<FireRule, (), RuleCommandError>();
        answers::<SendIntent, (), IntentError>();
        answers::<ListSessions, Vec<SessionStatus>, EnvironmentNotRunning>();
        answers::<CloseSession, (), CloseSessionError>();
        answers::<GetBackend, BackendStatus, EnvironmentNotRunning>();
        answers::<ParseSentence, ParsedSentence, EnvironmentNotRunning>();

        named::<AnyOperation>();
        editable::<Selector>();
        named::<AnyDefinition>();
        named::<DefinitionRef>();
        named::<Definitions>();
        named::<SessionIdentity>();
        named::<SessionCloser>();
        named::<BackendKind>();
        named::<WordCompletion>();
        named::<WordKind>();
        named::<UnreadWord>();
        named::<StreamMessage>();
        named::<ProcessState>();
        named::<RunningEnvironment>();
        named::<Stats>();
        named::<RuleStats>();
        named::<Subscription>();
    }
}

/// Every operation answers with what the table of the crate says, and fails with the error it names.
#[test]
fn the_prelude_names_every_type_next_to_std_and_every_domain() {
    next_to_std_and_every_domain::every_item();
}
