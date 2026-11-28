//! The prelude holds every item of the crate, and its names clash with none of the preludes next to it.

#![expect(
    clippy::unwrap_used,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

/// Imports the prelude next to the standard prelude, the modules of `std` that code imports most,
/// and the preludes of the crates that code imports next to a host.
///
/// All of them are glob imports, so they rank the same.
/// A name that two of them hold is ambiguous, and using it does not compile.
/// This module names every item of the prelude, so it only compiles while no name clashes.
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
    use std::prelude::rust_2024::*;
    use std::str::*;
    use std::string::*;
    use std::sync::*;
    use std::time::*;

    use gaslightmq_backend::prelude::*;
    use gaslightmq_backend_upstream::prelude::*;
    use gaslightmq_definition_api::prelude::*;
    use gaslightmq_engine::prelude::*;
    use gaslightmq_environment::prelude::*;
    use gaslightmq_field::prelude::*;
    use gaslightmq_host::prelude::*;
    use gaslightmq_journal::prelude::*;
    use gaslightmq_kernel::prelude::*;
    use gaslightmq_lie::prelude::*;
    use gaslightmq_lie_api::prelude::*;
    use gaslightmq_quick_inject::prelude::*;
    use gaslightmq_rule::prelude::*;
    use gaslightmq_selector::prelude::*;
    use gaslightmq_trigger::prelude::*;
    use gaslightmq_workspace::prelude::*;

    /// One value of every item of the prelude that is no error, each named without a path.
    pub fn every_item() -> Vec<String> {
        let listen = Listeners::new(
            "127.0.0.1:0".parse().unwrap(),
            "127.0.0.1:19092-19191".parse().unwrap(),
            "localhost".parse().unwrap(),
        );
        let staging = Environment::new(
            "staging-proxy".parse().unwrap(),
            Instances::ProxyInFrontOfCluster {
                proxy: ProxyDefinition::new(listen.unwrap()),
                upstream: Upstream::new("kafka-1.staging:9092".parse().unwrap()),
            },
        );
        let configuration: HostConfiguration = HostConfiguration::default();
        let environment: EnvironmentConfiguration = EnvironmentConfiguration::new(&staging).unwrap();
        let run: RunId = "4be0643f1d98573b97cdca98a65347dd".parse().unwrap();
        let expects: Expects = Expects {
            run,
            revision: RevisionCounter::FIRST,
        };
        let host: Host<MemoryContext> =
            Host::new(configuration.clone(), MemoryContext::new(), Lies::of_every_build(), run);
        let reloaded: Reloaded = Reloaded {
            revision: RevisionCounter::FIRST,
            changes: Vec::new(),
        };
        vec![
            format!("{configuration:?}"),
            environment.name().to_string(),
            format!("{host:?}"),
            expects.to_string(),
            reloaded.revision.to_string(),
        ]
    }

    /// One error of every type of the prelude, each named without a path.
    pub fn every_error() -> Vec<String> {
        let staging: EnvironmentName = "staging-proxy".parse().unwrap();
        let unsupported: EnvironmentConfigurationError = EnvironmentConfigurationError::UnsupportedBroker {
            name: "orders-sim".parse().unwrap(),
        };
        let running: EnvironmentStartError = EnvironmentStartError::AlreadyRunning { name: staging.clone() };
        let not_running: NotRunningError = NotRunningError { name: staging.clone() };
        let armed: ArmRuleError = ArmRuleError::AlreadyArmed {
            environment: staging.clone(),
            rule: "blackhole".parse().unwrap(),
        };
        let not_armed: FireRuleError = FireRuleError::NotArmed {
            environment: staging.clone(),
            rule: "blackhole".parse().unwrap(),
        };
        let rules: EnvironmentRulesError = EnvironmentRulesError::NotRunning {
            environment: staging.clone(),
        };
        let taken: CreateLiveRuleError = CreateLiveRuleError::NameTaken {
            environment: staging.clone(),
            rule: "blackhole".parse().unwrap(),
            holder: Some(RuleHolder::Environment(staging.clone())),
        };
        let unchanged: UpdateLiveRuleError = UpdateLiveRuleError::NotFound {
            environment: staging.clone(),
            rule: "blackhole".parse().unwrap(),
        };
        let undeleted: DeleteLiveRuleError = DeleteLiveRuleError::NotFound {
            environment: staging.clone(),
            rule: "blackhole".parse().unwrap(),
        };
        let unkept: KeepRuleError = KeepRuleError::NotFound {
            environment: staging.clone(),
            rule: "blackhole".parse().unwrap(),
        };
        let unknown: IntentError = IntentError::UnknownLayout {
            layout: "order-desk".parse().unwrap(),
        };
        let holder: RuleHolder = RuleHolder::Environment(staging.clone());
        let uncreated: CreateDefinitionError = CreateDefinitionError::NameTaken {
            rule: "blackhole".parse().unwrap(),
            holder,
        };
        let unreloaded: ReloadWorkspaceError = ReloadWorkspaceError::NameTaken {
            rule: "blackhole".parse().unwrap(),
            environment: staging.clone(),
        };
        let unclosed: CloseSessionError = CloseSessionError::NotRunning { environment: staging };
        let missed: HostChangesError = HostChangesError::Missed { changes: 3 };
        let backlog: ChangeBacklogError = ChangeBacklogError::Zero;
        let calls: CallBacklogError = CallBacklogError::Zero;
        let followed: FollowCallsError = FollowCallsError::StoreFailed {
            reason: "the selector store could not be read: disk full".to_owned(),
        };
        let unread: ReadSentenceError = ReadSentenceError::StoreFailed {
            reason: "the preset store could not be read: disk full".to_owned(),
        };
        // A message says everything, so no error has a source.
        assert!(std::error::Error::source(&unsupported).is_none());
        assert!(std::error::Error::source(&running).is_none());
        assert!(std::error::Error::source(&not_running).is_none());
        assert!(std::error::Error::source(&armed).is_none());
        assert!(std::error::Error::source(&not_armed).is_none());
        assert!(std::error::Error::source(&unread).is_none());
        assert!(std::error::Error::source(&rules).is_none());
        assert!(std::error::Error::source(&taken).is_none());
        assert!(std::error::Error::source(&unchanged).is_none());
        assert!(std::error::Error::source(&undeleted).is_none());
        assert!(std::error::Error::source(&unkept).is_none());
        assert!(std::error::Error::source(&unknown).is_none());
        assert!(std::error::Error::source(&unclosed).is_none());
        assert!(std::error::Error::source(&missed).is_none());
        assert!(std::error::Error::source(&backlog).is_none());
        assert!(std::error::Error::source(&calls).is_none());
        assert!(std::error::Error::source(&followed).is_none());
        assert!(std::error::Error::source(&uncreated).is_none());
        assert!(std::error::Error::source(&unreloaded).is_none());
        vec![
            unsupported.to_string(),
            running.to_string(),
            not_running.to_string(),
            armed.to_string(),
            not_armed.to_string(),
            unread.to_string(),
            rules.to_string(),
            taken.to_string(),
            unchanged.to_string(),
            undeleted.to_string(),
            unkept.to_string(),
            unknown.to_string(),
            unclosed.to_string(),
            missed.to_string(),
            backlog.to_string(),
            calls.to_string(),
            followed.to_string(),
            uncreated.to_string(),
            unreloaded.to_string(),
        ]
    }

    /// One error of every type of the prelude about recordings, each named without a path.
    pub fn every_error_of_a_recording() -> Vec<String> {
        let staging: EnvironmentName = "staging-proxy".parse().unwrap();
        let not_started: StartRecordingError = StartRecordingError::NotStarted {
            environment: staging.clone(),
            reason: "disk full".to_owned(),
        };
        let not_recording: StopRecordingError = StopRecordingError::NotRecording { environment: staging };
        assert!(std::error::Error::source(&not_started).is_none());
        assert!(std::error::Error::source(&not_recording).is_none());
        vec![not_started.to_string(), not_recording.to_string()]
    }

    /// One error of every type of the prelude that a run or a revision brought, each named without a path.
    pub fn every_error_of_a_run_or_a_revision() -> Vec<String> {
        let run: RunId = "4be0643f1d98573b97cdca98a65347dd".parse().unwrap();
        let no_run: RunIdError = RunId::new("4be0").unwrap_err();
        let earlier: UpdateDefinitionError = UpdateDefinitionError::OtherRun { run };
        let gone: DeleteDefinitionError = DeleteDefinitionError::NotDeleted {
            error: DeleteError::NotFound {
                name: PresetName::new("blackhole").unwrap().into(),
            },
        };
        let disarmed: DisarmRuleError = DisarmRuleError::Changed {
            environment: "staging-proxy".parse().unwrap(),
            rule: "blackhole".parse().unwrap(),
            revision: RevisionCounter::FIRST,
        };
        // A message says everything, so no error has a source.
        assert!(std::error::Error::source(&no_run).is_none());
        assert!(std::error::Error::source(&earlier).is_none());
        assert!(std::error::Error::source(&gone).is_none());
        assert!(std::error::Error::source(&disarmed).is_none());
        vec![
            no_run.to_string(),
            earlier.to_string(),
            gone.to_string(),
            disarmed.to_string(),
        ]
    }
}

#[test]
fn the_prelude_names_every_item_next_to_the_other_preludes() {
    let items = next_to_the_others::every_item();

    assert_eq!(items.len(), 5);
    assert_eq!(items[1], "staging-proxy");
    assert_eq!(items[2], "Host { running: [], .. }");
    assert_eq!(items[3], "revision 1 of run 4be0643f1d98573b97cdca98a65347dd");
    assert_eq!(items[4], "1");
}

#[test]
fn the_prelude_names_the_error_of_every_type() {
    assert_eq!(
        next_to_the_others::every_error(),
        [
            "environment \"orders-sim\" runs a broker, and a host cannot run a broker yet",
            "environment \"staging-proxy\" runs already",
            "environment \"staging-proxy\" does not run",
            "rule \"blackhole\" is armed already in environment \"staging-proxy\"",
            "rule \"blackhole\" is not armed in environment \"staging-proxy\"",
            "the names of the workspace could not be read: the preset store could not be read: disk full",
            "environment \"staging-proxy\" does not run",
            "environment \"staging-proxy\" has a rule \"blackhole\" already",
            "environment \"staging-proxy\" has no live rule \"blackhole\"",
            "environment \"staging-proxy\" has no live rule \"blackhole\"",
            "environment \"staging-proxy\" has no live rule \"blackhole\"",
            "layout \"order-desk\" does not exist",
            "environment \"staging-proxy\" does not run",
            "the follower fell behind, and missed 3 changes of the host",
            "a change backlog holds at least one change",
            "a call backlog holds at least one call",
            "the calls were not followed: the selector store could not be read: disk full",
            "rule \"blackhole\" was not created: environment \"staging-proxy\" has a live rule of that name",
            "the workspace was not reloaded: the files hold a rule \"blackhole\", \
             and environment \"staging-proxy\" has a live rule of that name",
        ]
    );
}

#[test]
fn the_prelude_names_the_error_of_every_recording_type() {
    assert_eq!(
        next_to_the_others::every_error_of_a_recording(),
        [
            "environment \"staging-proxy\" did not start a recording: disk full",
            "environment \"staging-proxy\" records nothing",
        ]
    );
}

#[test]
fn the_prelude_names_the_error_of_a_run_and_of_a_change_that_expects_a_revision() {
    assert_eq!(
        next_to_the_others::every_error_of_a_run_or_a_revision(),
        [
            "a run ID has 32 hexadecimal digits, and not 4 characters",
            "the change expects a revision of another run than 4be0643f1d98573b97cdca98a65347dd, \
             which is the run of the process",
            "preset \"blackhole\" does not exist",
            "rule \"blackhole\" changed in environment \"staging-proxy\" since: it has revision 1 now",
        ]
    );
}
