//! The `serde` form of what travels on the control plane: requests, answers, errors and the live stream.
//!
//! The forms are JSON here, as a client and a server write them.
//! A definition inside them has the form of its own crate, which the tests of that crate show.

#![expect(
    clippy::unwrap_used,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

use std::collections::BTreeMap;
use std::fmt::Debug;

use gaslightmq_control_api::call::{Call, CallId, CallTiming, Delivered, Explanation, RuleExplanation};
use gaslightmq_control_api::clock::{Duration, Instant};
use gaslightmq_control_api::environment::{
    BrokerDefinition, Environment, EnvironmentName, Instances, Listeners, ProxyDefinition,
};
use gaslightmq_control_api::field::{ClientSoftware, ConnectionNumber, Endpoint, Field, Fields};
use gaslightmq_control_api::incident::{IncidentKind, IncidentSupport};
use gaslightmq_control_api::kernel::{ApiKey, BrokerId, ClientId};
use gaslightmq_control_api::prelude::*;
use gaslightmq_control_api::rule::{PresetName, Rule, RuleName, RuleState};
use gaslightmq_control_api::selector::{Condition, Conditions, Selector, SelectorName, SelectorRef};
use gaslightmq_control_api::surface::{Column, Intent, LayoutName, Pad, PadLayout, Row};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

/// A value next to the JSON that a client or a server writes for it.
struct Form<T>(T);

impl<T: Serialize> Form<T> {
    /// What the value writes. A value inside a message has the form of its own crate, which this crate does not
    /// spell out again.
    fn json(&self) -> Value {
        serde_json::to_value(&self.0).unwrap()
    }
}

impl<T: Serialize + DeserializeOwned + PartialEq + Debug> Form<T> {
    /// `json` is what the value writes, and what reads back as the same value.
    fn writes(&self, json: Value) {
        assert_eq!(self.json(), json);
        assert_eq!(serde_json::from_value::<T>(json).unwrap(), self.0);
    }

    /// Reading `json` as a value of this type fails, and the message holds `reason`.
    fn rejects(json: Value, reason: &str) {
        let text = json.to_string();
        let error = serde_json::from_value::<T>(json).expect_err(&text).to_string();
        assert!(error.contains(reason), "{text}: {error:?} does not hold {reason:?}");
    }
}

/// The order desk of the specs: the definitions that several tests carry, as the workspace files write them.
struct OrderDesk;

impl OrderDesk {
    /// The selector `order-producers`: Produce calls to the `orders.*` topics.
    fn selector() -> Selector {
        Selector::new(
            "order-producers".parse().unwrap(),
            Conditions::new()
                .with(Condition::is(ApiKey::PRODUCE))
                .with(Condition::parse(Field::Topic, "orders.*").unwrap()),
        )
    }

    /// The rule `slow-order-producers`: the selector `order-producers`, and the preset `delay-1s`.
    fn rule() -> Rule {
        Rule::new(
            "slow-order-producers".parse().unwrap(),
            "order-producers".parse::<SelectorName>().unwrap(),
            "delay-1s".parse::<PresetName>().unwrap(),
        )
    }

    /// `environments/orders-sim.yaml`: a proxy in front of a broker.
    fn environment() -> Environment {
        let listen = Listeners::new(
            "0.0.0.0:29092".parse().unwrap(),
            "0.0.0.0:29093-29192".parse().unwrap(),
            "localhost".parse().unwrap(),
        )
        .unwrap();
        Environment::new(
            "orders-sim".parse().unwrap(),
            Instances::ProxyInFrontOfBroker {
                proxy: ProxyDefinition::new(listen),
                broker: BrokerDefinition::new("orders-sim".parse().unwrap()),
            },
        )
    }
}

// Requests

#[test]
fn an_operation_on_a_definition_names_its_kind() {
    // Someone saves a new selector in the UI.
    Form(AnyOperation::from(CreateDefinition {
        definition: OrderDesk::selector(),
    }))
    .writes(json!({
        "operation": "create-selector",
        "definition": { "name": "order-producers", "match": { "api": "Produce", "topic": "orders.*" } },
    }));

    // A rule and a selector can share a name, and the operation says which one goes.
    Form(AnyOperation::from(DeleteDefinition::<Rule> {
        name: "order-producers".parse().unwrap(),
    }))
    .writes(json!({ "operation": "delete-rule", "name": "order-producers" }));
    Form(AnyOperation::from(DeleteDefinition::<Selector> {
        name: "order-producers".parse().unwrap(),
    }))
    .writes(json!({ "operation": "delete-selector", "name": "order-producers" }));
}

#[test]
fn a_list_says_nothing_but_its_operation() {
    Form(AnyOperation::from(ListDefinitions::<PadLayout>::new())).writes(json!({ "operation": "list-layouts" }));
    Form(AnyOperation::from(ListEnvironments {})).writes(json!({ "operation": "list-environments" }));
}

#[test]
fn an_update_carries_the_whole_definition_under_its_name() {
    let titled = OrderDesk::rule().with_title("Slow down order producers".parse().unwrap());

    Form(AnyOperation::from(UpdateDefinition { definition: titled })).writes(json!({
        "operation": "update-rule",
        "definition": {
            "name": "slow-order-producers",
            "title": "Slow down order producers",
            "selector": "order-producers",
            "action": "delay-1s",
        },
    }));
}

#[test]
fn a_command_on_something_live_names_its_environment() {
    let rule: RuleName = "slow-order-producers".parse().unwrap();

    Form(AnyOperation::from(ArmRule {
        environment: "orders-sim".parse().unwrap(),
        rule: rule.clone(),
    }))
    .writes(json!({ "operation": "arm-rule", "environment": "orders-sim", "rule": "slow-order-producers" }));
    Form(AnyOperation::from(FireRule {
        environment: "orders-sim".parse().unwrap(),
        rule,
    }))
    .writes(json!({ "operation": "fire-rule", "environment": "orders-sim", "rule": "slow-order-producers" }));
    Form(AnyOperation::from(CloseSession {
        environment: "orders-sim".parse().unwrap(),
        connection: ConnectionNumber::new(7),
    }))
    .writes(json!({ "operation": "close-session", "environment": "orders-sim", "connection": 7 }));
}

#[test]
fn the_rules_of_an_environment_are_another_list_than_the_rules_of_the_workspace() {
    Form(AnyOperation::from(ListEnvironmentRules {
        environment: "orders-sim".parse().unwrap(),
    }))
    .writes(json!({ "operation": "list-environment-rules", "environment": "orders-sim" }));
    Form(AnyOperation::from(ListDefinitions::<Rule>::new())).writes(json!({ "operation": "list-rules" }));
}

#[test]
fn a_live_rule_carries_its_definition() {
    Form(AnyOperation::from(CreateLiveRule {
        environment: "orders-sim".parse().unwrap(),
        rule: OrderDesk::rule(),
    }))
    .writes(json!({
        "operation": "create-live-rule",
        "environment": "orders-sim",
        "rule": Form(OrderDesk::rule()).json(),
    }));
}

#[test]
fn a_pad_press_sends_its_intent() {
    // Arm the rule at row 1, column 4 on layer order-ingest.
    Form(AnyOperation::from(SendIntent {
        environment: "orders-sim".parse().unwrap(),
        intent: Intent::Arm {
            pad: Pad::new(
                "order-desk".parse().unwrap(),
                "order-ingest".parse().unwrap(),
                Row::from_index(0),
                Column::from_index(3),
            ),
        },
    }))
    .writes(json!({
        "operation": "send-intent",
        "environment": "orders-sim",
        "intent": {
            "intent": "arm",
            "pad": { "layout": "order-desk", "layer": "order-ingest", "row": 0, "column": 3 },
        },
    }));
}

#[test]
fn a_sentence_is_the_text_typed_so_far() {
    Form(AnyOperation::from(ParseSentence {
        environment: "orders-sim".parse().unwrap(),
        text: "leader-flap on produce ord".to_owned(),
    }))
    .writes(
        json!({ "operation": "parse-sentence", "environment": "orders-sim", "text": "leader-flap on produce ord" }),
    );
}

#[test]
fn a_request_for_an_operation_that_does_not_exist_does_not_read() {
    Form::<AnyOperation>::rejects(
        json!({ "operation": "rename-rule", "name": "x" }),
        "unknown variant `rename-rule`",
    );
}

#[test]
fn a_request_without_what_its_operation_needs_does_not_read() {
    Form::<AnyOperation>::rejects(
        json!({ "operation": "arm-rule", "rule": "x" }),
        "missing field `environment`",
    );
}

#[test]
fn a_request_with_a_key_that_its_operation_does_not_know_does_not_read() {
    // A misspelt key would otherwise be dropped without a word.
    Form::<AnyOperation>::rejects(
        json!({ "operation": "arm-rule", "environment": "orders-sim", "rule": "slow-order-producers", "enviroment": "x" }),
        "unknown field `enviroment`",
    );
}

// Answers

#[test]
fn a_rule_of_an_environment_says_whether_it_is_live_and_its_state() {
    Form(RuleStatus {
        rule: OrderDesk::rule(),
        live: false,
        state: RuleState::Armed,
    })
    .writes(json!({ "rule": Form(OrderDesk::rule()).json(), "live": false, "state": "armed" }));
}

#[test]
fn an_environment_says_whether_it_runs() {
    let environment = OrderDesk::environment();

    Form(EnvironmentStatus {
        environment: environment.clone(),
        running: true,
    })
    .writes(json!({ "environment": Form(&environment).json(), "running": true }));
}

#[test]
fn an_anonymous_session_has_no_identity_yet() {
    let opened = SessionStatus {
        connection: ConnectionNumber::new(7),
        address: "10.0.4.27:51459".parse().unwrap(),
        endpoint: Endpoint::Broker(BrokerId::from(2)),
        opened: Instant::ZERO + Duration::from_millis(250),
        identity: None,
    };

    Form(opened.clone()).writes(json!({
        "connection": 7,
        "address": "10.0.4.27:51459",
        "endpoint": "broker 2",
        "opened": 250_000_000,
    }));

    // Its first request is an `ApiVersions` of `order-ingest`, which names its client software too.
    let identified = SessionStatus {
        identity: Some(SessionIdentity {
            client: Some(ClientId::new("order-ingest").unwrap()),
            software: Some(ClientSoftware::new("apache-kafka-java", "3.8.0").unwrap()),
        }),
        ..opened
    };
    Form(identified).writes(json!({
        "connection": 7,
        "address": "10.0.4.27:51459",
        "endpoint": "broker 2",
        "opened": 250_000_000,
        "identity": { "client": "order-ingest", "software": "apache-kafka-java 3.8.0" },
    }));
}

#[test]
fn a_session_identified_without_a_client_id_has_an_empty_identity() {
    Form(SessionIdentity::default()).writes(json!({}));
}

#[test]
fn a_backend_names_each_endpoint_with_the_address_of_its_listener() {
    let incidents = IncidentSupport::none().emulates([IncidentKind::BrokerGoesDown]);
    let backend = BackendStatus {
        kind: BackendKind::Upstream,
        endpoints: BTreeMap::from([
            (Endpoint::Bootstrap, Some("0.0.0.0:9092".parse().unwrap())),
            (
                Endpoint::Broker(BrokerId::from(1)),
                Some("0.0.0.0:19092".parse().unwrap()),
            ),
            // The port of broker 2 was taken, and the engine tries again.
            (Endpoint::Broker(BrokerId::from(2)), None),
        ]),
        incidents: incidents.clone(),
    };

    Form(backend).writes(json!({
        "kind": "upstream",
        "endpoints": { "bootstrap": "0.0.0.0:9092", "broker 1": "0.0.0.0:19092", "broker 2": null },
        "incidents": Form(&incidents).json(),
    }));
}

#[test]
fn a_sentence_gives_its_rule_its_completions_and_what_it_could_not_read() {
    let typing = ParsedSentence {
        rule: None,
        completions: vec![WordCompletion {
            kind: WordKind::Topic,
            word: "orders.eu".to_owned(),
            title: None,
            sentence: "leader-flap on produce orders.eu ".to_owned(),
        }],
        unread: vec![UnreadWord {
            word: "ord".to_owned(),
            start: 23,
            reason: "`ord` is no word of a sentence, and no name that is known".to_owned(),
        }],
    };

    Form(typing).writes(json!({
        "completions": [
            { "kind": "topic", "word": "orders.eu", "sentence": "leader-flap on produce orders.eu " },
        ],
        "unread": [
            { "word": "ord", "start": 23, "reason": "`ord` is no word of a sentence, and no name that is known" },
        ],
    }));
}

#[test]
fn a_whole_sentence_gives_a_rule_and_nothing_else() {
    Form(ParsedSentence {
        rule: Some(OrderDesk::rule()),
        completions: Vec::new(),
        unread: Vec::new(),
    })
    .writes(json!({ "rule": Form(OrderDesk::rule()).json() }));
}

#[test]
fn the_statistics_count_calls_by_the_name_of_their_api_and_rules_by_their_name() {
    let stats = Stats {
        calls: 3,
        missed_events: 0,
        apis: BTreeMap::from([(ApiKey::PRODUCE, 2), (ApiKey::FETCH, 1)]),
        rules: BTreeMap::from([(
            "slow-order-producers".parse().unwrap(),
            RuleStats {
                matches: 2,
                firings: 1,
                left: Some(2),
            },
        )]),
    };

    Form(stats).writes(json!({
        "calls": 3,
        "missed-events": 0,
        "apis": { "Produce": 2, "Fetch": 1 },
        "rules": { "slow-order-producers": { "matches": 2, "firings": 1, "left": 2 } },
    }));
}

// Errors

#[test]
fn an_error_names_its_variant() {
    let name: RuleName = "slow-order-producers".parse().unwrap();

    Form(GetDefinitionError::NotFound { name: name.into() })
        .writes(json!({ "error": "not-found", "name": { "rule": "slow-order-producers" } }));
    Form(RuleCommandError::Refused {
        environment: "orders-sim".parse().unwrap(),
        rule: "slow-order-producers".parse().unwrap(),
        reason: "an armed rule is not armed again".to_owned(),
    })
    .writes(json!({
        "error": "refused",
        "environment": "orders-sim",
        "rule": "slow-order-producers",
        "reason": "an armed rule is not armed again",
    }));
}

#[test]
fn a_store_that_could_not_read_says_what_it_said() {
    Form(RuleCommandError::StoreFailed {
        rule: "slow-order-producers".parse().unwrap(),
        reason: "the rule store could not be read: permission denied".to_owned(),
    })
    .writes(json!({
        "error": "store-failed",
        "rule": "slow-order-producers",
        "reason": "the rule store could not be read: permission denied",
    }));
    Form(EnvironmentReadError::StoreFailed {
        environment: "orders-sim".parse().unwrap(),
        reason: "the preset store could not be read: permission denied".to_owned(),
    })
    .writes(json!({
        "error": "store-failed",
        "environment": "orders-sim",
        "reason": "the preset store could not be read: permission denied",
    }));
    Form(EnvironmentReadError::NotRunning {
        environment: "orders-sim".parse().unwrap(),
    })
    .writes(json!({ "error": "not-running", "environment": "orders-sim" }));
}

#[test]
fn a_request_that_got_no_operation_says_why_in_the_same_way_for_every_operation() {
    Form(RequestError::NotServed {
        operation: "keep-rule".to_owned(),
    })
    .writes(json!({ "error": "not-served", "operation": "keep-rule" }));
    Form(RequestError::Unreadable {
        reason: "unknown variant `rename-rule`".to_owned(),
    })
    .writes(json!({ "error": "unreadable", "reason": "unknown variant `rename-rule`" }));
}

#[test]
fn an_operation_names_itself_as_its_form_does() {
    let environment: EnvironmentName = "orders-sim".parse().unwrap();
    let rule: RuleName = "slow-order-producers".parse().unwrap();
    let operations = [
        AnyOperation::from(ListDefinitions::<Selector>::new()),
        AnyOperation::from(GetDefinition::<Rule> { name: rule.clone() }),
        AnyOperation::from(CreateDefinition {
            definition: OrderDesk::rule(),
        }),
        AnyOperation::from(DeleteDefinition::<PadLayout> {
            name: "order-desk".parse().unwrap(),
        }),
        AnyOperation::from(ListEnvironments {}),
        AnyOperation::from(StartEnvironment {
            environment: environment.clone(),
        }),
        AnyOperation::from(ListEnvironmentRules {
            environment: environment.clone(),
        }),
        AnyOperation::from(KeepRule {
            environment: environment.clone(),
            rule: rule.clone(),
        }),
        AnyOperation::from(FireRule {
            environment: environment.clone(),
            rule,
        }),
        AnyOperation::from(ParseSentence {
            environment,
            text: "delay produce".to_owned(),
        }),
    ];

    for operation in operations {
        let json = Form(operation.clone()).json();
        assert_eq!(json["operation"], operation.name(), "{json}");
    }
}

#[test]
fn a_definition_in_use_names_each_user_with_its_kind() {
    let selector: SelectorName = "order-producers".parse().unwrap();
    let rule: RuleName = "slow-order-producers".parse().unwrap();
    let layout: LayoutName = "order-desk".parse().unwrap();

    Form(DeleteDefinitionError::InUse {
        name: selector.into(),
        users: vec![rule.into(), layout.into()],
    })
    .writes(json!({
        "error": "in-use",
        "name": { "selector": "order-producers" },
        "users": [{ "rule": "slow-order-producers" }, { "layout": "order-desk" }],
    }));
}

#[test]
fn an_environment_that_does_not_run_is_the_map_of_its_name() {
    Form(EnvironmentNotRunning {
        environment: "orders-sim".parse().unwrap(),
    })
    .writes(json!({ "environment": "orders-sim" }));
}

#[test]
fn an_error_of_another_operation_does_not_read() {
    Form::<CloseSessionError>::rejects(
        json!({ "error": "name-taken", "name": { "rule": "x" } }),
        "unknown variant `name-taken`",
    );
}

// The live stream

#[test]
fn an_accepted_subscription_says_what_the_client_follows() {
    Form(StreamMessage::Following {
        subscription: Subscription::Calls {
            environment: "orders-sim".parse().unwrap(),
            selector: SelectorRef::Named("order-producers".parse().unwrap()),
        },
    })
    .writes(json!({
        "message": "following",
        "subscription": { "follow": "calls", "environment": "orders-sim", "selector": "order-producers" },
    }));
    Form(StreamMessage::Following {
        subscription: Subscription::Nothing,
    })
    .writes(json!({ "message": "following", "subscription": { "follow": "nothing" } }));
}

#[test]
fn a_refused_subscription_says_why() {
    Form(StreamMessage::SubscriptionRefused {
        reason: "environment \"orders-sim\" does not run".to_owned(),
    })
    .writes(json!({ "message": "subscription-refused", "reason": "environment \"orders-sim\" does not run" }));
}

#[test]
fn a_client_that_connects_gets_the_state_of_the_process() {
    let environment = OrderDesk::environment();
    let state = ProcessState {
        definitions: Definitions {
            selectors: vec![OrderDesk::selector()],
            environments: vec![environment.clone()],
            ..Definitions::default()
        },
        environments: vec![RunningEnvironment {
            environment: "orders-sim".parse().unwrap(),
            backend: BackendStatus {
                kind: BackendKind::Simulated,
                endpoints: BTreeMap::from([(Endpoint::Bootstrap, Some("0.0.0.0:29092".parse().unwrap()))]),
                incidents: IncidentSupport::none().applies(IncidentKind::ALL),
            },
            rules: Vec::new(),
            sessions: Vec::new(),
        }],
    };

    Form(StreamMessage::Snapshot(state)).writes(json!({
        "message": "snapshot",
        "definitions": {
            "selectors": [Form(OrderDesk::selector()).json()],
            "presets": [],
            "rules": [],
            "layouts": [],
            "environments": [Form(&environment).json()],
        },
        "environments": [{
            "environment": "orders-sim",
            "backend": {
                "kind": "simulated",
                "endpoints": { "bootstrap": "0.0.0.0:29092" },
                "incidents": Form(IncidentSupport::none().applies(IncidentKind::ALL)).json(),
            },
            "rules": [],
            "sessions": [],
        }],
    }));
}

#[test]
fn a_call_travels_with_its_environment() {
    let fields = Fields::new().with(ConnectionNumber::new(7)).without_truth();
    let explanation = Explanation::new(Vec::<RuleExplanation>::new()).unwrap();
    let call = Call::new(
        CallId::FIRST,
        fields,
        Delivered::Nothing,
        explanation,
        CallTiming::arrived(Instant::ZERO),
    )
    .unwrap();

    Form(StreamMessage::Call {
        environment: "orders-sim".parse().unwrap(),
        call: call.clone(),
    })
    .writes(json!({ "message": "call", "environment": "orders-sim", "call": Form(&call).json() }));
}

#[test]
fn a_pad_press_comes_back_as_the_rule_it_changed() {
    Form(StreamMessage::RuleChanged {
        environment: "orders-sim".parse().unwrap(),
        rule: RuleStatus {
            rule: OrderDesk::rule(),
            live: true,
            state: RuleState::Armed,
        },
    })
    .writes(json!({
        "message": "rule-changed",
        "environment": "orders-sim",
        "rule": { "rule": Form(OrderDesk::rule()).json(), "live": true, "state": "armed" },
    }));
}

#[test]
fn a_change_to_the_workspace_names_the_kind_of_its_definition() {
    Form(StreamMessage::DefinitionChanged {
        definition: AnyDefinition::Selector(OrderDesk::selector()),
    })
    .writes(json!({
        "message": "definition-changed",
        "definition": { "selector": Form(OrderDesk::selector()).json() },
    }));
    Form(StreamMessage::DefinitionDeleted {
        name: "blackhole".parse::<PresetName>().unwrap().into(),
    })
    .writes(json!({ "message": "definition-deleted", "name": { "preset": "blackhole" } }));
}

#[test]
fn a_session_says_who_closed_it() {
    Form(StreamMessage::SessionClosed {
        environment: "orders-sim".parse().unwrap(),
        connection: ConnectionNumber::new(7),
        by: SessionCloser::Lie,
    })
    .writes(json!({ "message": "session-closed", "environment": "orders-sim", "connection": 7, "by": "lie" }));
}

#[test]
fn an_environment_that_stops_takes_its_name_with_it() {
    Form(StreamMessage::EnvironmentStopped {
        environment: "orders-sim".parse().unwrap(),
    })
    .writes(json!({ "message": "environment-stopped", "environment": "orders-sim" }));
}

#[test]
fn a_workspace_that_does_not_fit_says_why() {
    Form(StreamMessage::WorkspaceInvalid {
        reason: "rule \"slow-order-producers\" refers to selector \"order-producers\", which does not exist".to_owned(),
    })
    .writes(json!({
        "message": "workspace-invalid",
        "reason": "rule \"slow-order-producers\" refers to selector \"order-producers\", which does not exist",
    }));
}

#[test]
fn a_client_follows_the_calls_that_a_selector_matches() {
    Form(Subscription::Calls {
        environment: "orders-sim".parse().unwrap(),
        selector: SelectorRef::Named("order-producers".parse().unwrap()),
    })
    .writes(json!({ "follow": "calls", "environment": "orders-sim", "selector": "order-producers" }));

    // A selector without conditions matches every call.
    Form(Subscription::Calls {
        environment: "orders-sim".parse().unwrap(),
        selector: SelectorRef::Inline(Conditions::new()),
    })
    .writes(json!({ "follow": "calls", "environment": "orders-sim", "selector": {} }));

    Form(Subscription::Nothing).writes(json!({ "follow": "nothing" }));
}
