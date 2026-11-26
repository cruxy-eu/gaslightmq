//! The prelude holds every type, and its names clash with none of `std`
//! or of the preludes of the domains that this crate re-exports.

/// Imports the prelude next to the standard prelude, the modules of `std` that code imports most, the clock,
/// and the prelude of every domain whose types the control plane carries.
///
/// All of them are glob imports, so they rank the same.
/// A name that two of them hold is ambiguous, and using it does not compile.
/// The tests of this module name every item of the prelude, so they only compile while no name clashes.
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

    /// Every operation answers with what the table of the crate says, and fails with the error it names.
    /// A line only compiles when the type is right.
    #[test]
    fn every_operation_answers_and_fails_as_the_crate_says() {
        let _: Option<<ListDefinitions<Selector> as Operation>::Answer> = None::<Vec<Selector>>;
        let _: Option<<ListDefinitions<Selector> as Operation>::Error> = None::<ListDefinitionsError>;
        let _: Option<<GetDefinition<Preset> as Operation>::Answer> = None::<Preset>;
        let _: Option<<GetDefinition<Preset> as Operation>::Error> = None::<GetDefinitionError>;
        let _: Option<<CreateDefinition<Rule> as Operation>::Answer> = None::<()>;
        let _: Option<<CreateDefinition<Rule> as Operation>::Error> = None::<CreateDefinitionError>;
        let _: Option<<UpdateDefinition<PadLayout> as Operation>::Answer> = None::<()>;
        let _: Option<<UpdateDefinition<PadLayout> as Operation>::Error> = None::<UpdateDefinitionError>;
        let _: Option<<DeleteDefinition<Selector> as Operation>::Answer> = None::<()>;
        let _: Option<<DeleteDefinition<Selector> as Operation>::Error> = None::<DeleteDefinitionError>;
        let _: Option<<ListEnvironments as Operation>::Answer> = None::<Vec<EnvironmentStatus>>;
        let _: Option<<ListEnvironments as Operation>::Error> = None::<ListDefinitionsError>;
        let _: Option<<GetEnvironment as Operation>::Answer> = None::<EnvironmentStatus>;
        let _: Option<<GetEnvironment as Operation>::Error> = None::<GetDefinitionError>;
        let _: Option<<StartEnvironment as Operation>::Answer> = None::<()>;
        let _: Option<<StartEnvironment as Operation>::Error> = None::<StartEnvironmentError>;
        let _: Option<<StopEnvironment as Operation>::Answer> = None::<()>;
        let _: Option<<StopEnvironment as Operation>::Error> = None::<EnvironmentNotRunning>;
        let _: Option<<ListEnvironmentRules as Operation>::Answer> = None::<Vec<RuleStatus>>;
        let _: Option<<ListEnvironmentRules as Operation>::Error> = None::<EnvironmentReadError>;
        let _: Option<<CreateLiveRule as Operation>::Answer> = None::<()>;
        let _: Option<<CreateLiveRule as Operation>::Error> = None::<CreateLiveRuleError>;
        let _: Option<<UpdateLiveRule as Operation>::Answer> = None::<()>;
        let _: Option<<UpdateLiveRule as Operation>::Error> = None::<UpdateLiveRuleError>;
        let _: Option<<DeleteLiveRule as Operation>::Answer> = None::<()>;
        let _: Option<<DeleteLiveRule as Operation>::Error> = None::<DeleteLiveRuleError>;
        let _: Option<<KeepRule as Operation>::Answer> = None::<()>;
        let _: Option<<KeepRule as Operation>::Error> = None::<KeepRuleError>;
        let _: Option<<ArmRule as Operation>::Answer> = None::<()>;
        let _: Option<<ArmRule as Operation>::Error> = None::<RuleCommandError>;
        let _: Option<<DisarmRule as Operation>::Answer> = None::<()>;
        let _: Option<<DisarmRule as Operation>::Error> = None::<RuleCommandError>;
        let _: Option<<FireRule as Operation>::Answer> = None::<()>;
        let _: Option<<FireRule as Operation>::Error> = None::<RuleCommandError>;
        let _: Option<<SendIntent as Operation>::Answer> = None::<IntentEffect>;
        let _: Option<<SendIntent as Operation>::Error> = None::<IntentError>;
        let _: Option<<ListSessions as Operation>::Answer> = None::<Vec<SessionStatus>>;
        let _: Option<<ListSessions as Operation>::Error> = None::<EnvironmentNotRunning>;
        let _: Option<<CloseSession as Operation>::Answer> = None::<()>;
        let _: Option<<CloseSession as Operation>::Error> = None::<CloseSessionError>;
        let _: Option<<GetBackend as Operation>::Answer> = None::<BackendStatus>;
        let _: Option<<GetBackend as Operation>::Error> = None::<EnvironmentNotRunning>;
        let _: Option<<ParseSentence as Operation>::Answer> = None::<ParsedSentence>;
        let _: Option<<ParseSentence as Operation>::Error> = None::<EnvironmentReadError>;
    }

    /// Every other type of the prelude, named without a path.
    #[test]
    fn every_other_type_is_named_without_a_path() {
        let _: Option<<Selector as EditableDefinition>::Name> = None::<SelectorName>;
        let _: Option<AnyOperation> = None;
        let _: Option<OperationRequest> = None;
        let _: Option<RequestSource> = None;
        let _: Option<RequestSurface> = None;
        let _: Option<RequestCause> = None;
        let _: Option<RequestError> = None;
        let _: Option<AnyDefinition> = None;
        let _: Option<DefinitionRef> = None;
        let _: Option<Definitions> = None;
        let _: Option<SessionIdentity> = None;
        let _: Option<SessionCloser> = None;
        let _: Option<BackendKind> = None;
        let _: Option<WordCompletion> = None;
        let _: Option<WordKind> = None;
        let _: Option<UnreadWord> = None;
        let _: Option<StreamMessage> = None;
        let _: Option<ProcessState> = None;
        let _: Option<RunningEnvironment> = None;
        let _: Option<Stats> = None;
        let _: Option<RuleStats> = None;
        let _: Option<Subscription> = None;
    }
}
