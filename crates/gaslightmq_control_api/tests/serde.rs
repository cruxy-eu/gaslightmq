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
    BrokerDefinition, BrokerListeners, Environment, EnvironmentName, Instances, Listeners, ProxyDefinition, Upstream,
};
use gaslightmq_control_api::field::{ClientSoftware, ConnectionNumber, Endpoint, Field, Fields};
use gaslightmq_control_api::incident::{IncidentKind, IncidentSupport};
use gaslightmq_control_api::journal::{
    Change, ChangeCause, ChangeSource, ChangeSurface, JournalEntry, JournalEntryId, Parameter,
};
use gaslightmq_control_api::kernel::{ApiKey, BrokerId, ClientId};
use gaslightmq_control_api::prelude::*;
use gaslightmq_control_api::rule::{PresetName, Rule, RuleName, RuleState};
use gaslightmq_control_api::scenario::{Mode, Scenario, ScenarioName};
use gaslightmq_control_api::selector::{Condition, Conditions, Selector, SelectorName, SelectorRef};
use gaslightmq_control_api::surface::{Column, Hold, Intent, LayoutName, Pad, PadLayout, PresenceId, Row};
use gaslightmq_control_api::trigger::Seed;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

/// The run of the process, as its snapshot writes it.
const RUN: &str = "4be0643f1d98573b97cdca98a65347dd";

/// The revision that a client saw beside what it changes.
const SEEN: Revision = Revision::new(7);

/// The revision that the change of that client left, or that someone else's change left before it.
const NOW: Revision = Revision::new(8);

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
fn a_request_says_where_it_comes_from_next_to_its_operation() {
    // Someone presses a pad, and the UI arms the rule of the pad in the environment it shows.
    let arm = ArmRule {
        environment: "orders-sim".parse().unwrap(),
        rule: "slow-order-producers".parse().unwrap(),
        expects: None,
    };

    Form(OperationRequest::new(RequestSurface::Pad, arm)).writes(json!({
        "from": { "surface": "pad" },
        "operation": "arm-rule",
        "environment": "orders-sim",
        "rule": "slow-order-producers",
    }));
}

#[test]
fn every_surface_that_sends_requests_has_a_name() {
    Form(RequestSurface::Pad).writes(json!("pad"));
    Form(RequestSurface::QuickInject).writes(json!("quick-inject"));
    Form(RequestSurface::Ui).writes(json!("ui"));
    Form(RequestSurface::Cli).writes(json!("cli"));
    Form(RequestSurface::Test).writes(json!("test"));
    Form(RequestSurface::Api).writes(json!("api"));
}

#[test]
fn an_undo_and_a_redo_say_their_cause() {
    // ⌘Z in a tab takes back the arming of a rule, and ⇧⌘Z arms it again.
    let environment: EnvironmentName = "orders-sim".parse().unwrap();
    let rule: RuleName = "slow-order-producers".parse().unwrap();
    let undo = RequestSource::new(RequestSurface::Ui).with_cause(RequestCause::Undo);
    let redo = RequestSource::new(RequestSurface::Ui).with_cause(RequestCause::Redo);

    Form(OperationRequest::new(
        undo,
        DisarmRule {
            environment: environment.clone(),
            rule: rule.clone(),
            expects: None,
        },
    ))
    .writes(json!({
        "from": { "surface": "ui", "cause": "undo" },
        "operation": "disarm-rule",
        "environment": "orders-sim",
        "rule": "slow-order-producers",
    }));
    let arm = ArmRule {
        environment,
        rule,
        expects: None,
    };
    Form(OperationRequest::new(redo, arm)).writes(json!({
        "from": { "surface": "ui", "cause": "redo" },
        "operation": "arm-rule",
        "environment": "orders-sim",
        "rule": "slow-order-producers",
    }));
}

#[test]
fn a_request_of_a_presence_says_which_one_and_a_hold_it_adds_ends_with_it() {
    // A tab of the UI holds a pad. Its live stream named its presence, and the request of the hold says so.
    let tab = PresenceId::from_random_bits(0x9f86_d081_884c_7d65_9a2f_eaa0_c55a_d015);
    let hold = SendIntent {
        environment: "orders-sim".parse().unwrap(),
        intent: Intent::Hold {
            pad: Pad::new(
                "order-desk".parse().unwrap(),
                "order-ingest".parse().unwrap(),
                Row::from_index(0),
                Column::from_index(3),
            ),
        },
    };

    Form(OperationRequest::new(
        RequestSource::new(RequestSurface::Pad).with_presence(tab),
        hold,
    ))
    .writes(json!({
        "from": { "surface": "pad", "presence": "9f86d081884c7d659a2feaa0c55ad015" },
        "operation": "send-intent",
        "environment": "orders-sim",
        "intent": {
            "intent": "hold",
            "pad": { "layout": "order-desk", "layer": "order-ingest", "row": 0, "column": 3 },
        },
    }));
    // An undo of the same tab says its cause and its presence.
    Form(
        RequestSource::new(RequestSurface::Ui)
            .with_presence(tab)
            .with_cause(RequestCause::Undo),
    )
    .writes(json!({ "surface": "ui", "cause": "undo", "presence": "9f86d081884c7d659a2feaa0c55ad015" }));
    // No request names a presence by anything but its ID.
    Form::<OperationRequest>::rejects(
        json!({ "from": { "surface": "pad", "presence": "tab-7" }, "operation": "list-rules" }),
        "a presence ID has 32 hexadecimal digits, and not 5 characters",
    );
}

#[test]
fn a_request_that_does_not_say_where_it_comes_from_does_not_read() {
    Form::<OperationRequest>::rejects(
        json!({ "operation": "arm-rule", "environment": "orders-sim", "rule": "slow-order-producers" }),
        "missing field `from`",
    );
    Form::<OperationRequest>::rejects(
        json!({ "from": { "cause": "undo" }, "operation": "list-rules" }),
        "missing field `surface`",
    );
}

#[test]
fn a_request_cannot_claim_a_surface_that_the_process_names_itself() {
    // A step, a lifetime and a file change things without a request.
    for surface in ["step", "lifetime", "file"] {
        Form::<OperationRequest>::rejects(
            json!({ "from": { "surface": surface }, "operation": "list-rules" }),
            &format!("unknown variant `{surface}`"),
        );
    }
}

#[test]
fn a_request_reads_its_operation_as_strictly_as_the_operation_reads_alone() {
    Form::<OperationRequest>::rejects(
        json!({ "from": { "surface": "cli" }, "operation": "rename-rule", "name": "x" }),
        "unknown variant `rename-rule`",
    );
    Form::<OperationRequest>::rejects(
        json!({ "from": { "surface": "cli" }, "operation": "arm-rule", "rule": "x" }),
        "missing field `environment`",
    );
    // A misspelt key would otherwise be dropped without a word, next to the operation or inside `from`.
    Form::<OperationRequest>::rejects(
        json!({ "from": { "surface": "cli" }, "operation": "list-rules", "form": { "surface": "cli" } }),
        "unknown field `form`",
    );
    Form::<OperationRequest>::rejects(
        json!({ "from": { "surface": "cli", "because": "undo" }, "operation": "list-rules" }),
        "unknown field `because`",
    );
}

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
        expects: None,
    }))
    .writes(json!({ "operation": "delete-rule", "name": "order-producers" }));
    Form(AnyOperation::from(DeleteDefinition::<Selector> {
        name: "order-producers".parse().unwrap(),
        expects: None,
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

    Form(AnyOperation::from(UpdateDefinition {
        expects: None,
        definition: titled,
    }))
    .writes(json!({
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
        expects: None,
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
fn a_change_says_what_it_expects_next_to_the_keys_of_its_operation() {
    // What the UI read: the run of its snapshot, and the revision beside the rule.
    let seen = Expects {
        run: RUN.parse().unwrap(),
        revision: SEEN,
    };
    let expects = json!({ "run": RUN, "revision": SEEN.number() });
    let environment: EnvironmentName = "orders-sim".parse().unwrap();
    let rule: RuleName = "slow-order-producers".parse().unwrap();

    Form(OperationRequest::new(
        RequestSurface::Ui,
        UpdateDefinition {
            expects: Some(seen),
            definition: OrderDesk::rule(),
        },
    ))
    .writes(json!({
        "from": { "surface": "ui" },
        "operation": "update-rule",
        "expects": expects,
        "definition": { "name": "slow-order-producers", "selector": "order-producers", "action": "delay-1s" },
    }));
    Form(AnyOperation::from(DeleteDefinition::<Rule> {
        name: rule.clone(),
        expects: Some(seen),
    }))
    .writes(json!({ "operation": "delete-rule", "name": "slow-order-producers", "expects": expects }));
    let on_the_rule = json!({ "environment": "orders-sim", "rule": "slow-order-producers", "expects": expects });
    let named = |operation: &str| {
        let mut form = on_the_rule.clone();
        form["operation"] = json!(operation);
        form
    };
    Form(AnyOperation::from(ArmRule {
        environment: environment.clone(),
        rule: rule.clone(),
        expects: Some(seen),
    }))
    .writes(named("arm-rule"));
    Form(AnyOperation::from(DisarmRule {
        environment: environment.clone(),
        rule: rule.clone(),
        expects: Some(seen),
    }))
    .writes(named("disarm-rule"));
    Form(AnyOperation::from(DeleteLiveRule {
        environment: environment.clone(),
        rule: rule.clone(),
        expects: Some(seen),
    }))
    .writes(named("delete-live-rule"));
    Form(AnyOperation::from(KeepRule {
        environment: environment.clone(),
        rule,
        expects: Some(seen),
    }))
    .writes(named("keep-rule"));
    Form(AnyOperation::from(UpdateLiveRule {
        environment,
        rule: OrderDesk::rule(),
        expects: Some(seen),
    }))
    .writes(json!({
        "operation": "update-live-rule",
        "environment": "orders-sim",
        "rule": Form(OrderDesk::rule()).json(),
        "expects": expects,
    }));
}

#[test]
fn a_create_and_a_firing_expect_nothing() {
    let expects = json!({ "run": RUN, "revision": SEEN.number() });

    Form::<AnyOperation>::rejects(
        json!({ "operation": "create-rule", "definition": Form(OrderDesk::rule()).json(), "expects": expects }),
        "unknown field `expects`",
    );
    Form::<AnyOperation>::rejects(
        json!({ "operation": "fire-rule", "environment": "orders-sim", "rule": "slow-order-producers", "expects": expects }),
        "unknown field `expects`",
    );
}

#[test]
fn what_a_change_expects_names_a_run_and_a_revision() {
    let arm = |expects: Value| json!({ "operation": "arm-rule", "environment": "orders-sim", "rule": "slow-order-producers", "expects": expects });

    // A revision means something only together with the run that gave it.
    Form::<AnyOperation>::rejects(arm(json!({ "revision": SEEN.number() })), "missing field `run`");
    Form::<AnyOperation>::rejects(arm(json!({ "run": RUN })), "missing field `revision`");
    Form::<AnyOperation>::rejects(
        arm(json!({ "run": "orders-sim", "revision": SEEN.number() })),
        "a run ID has 32 hexadecimal digits, and not 10 characters",
    );
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
        armed: false,
    }))
    .writes(json!({
        "operation": "create-live-rule",
        "environment": "orders-sim",
        "rule": Form(OrderDesk::rule()).json(),
    }));
}

#[test]
fn a_sentence_creates_its_live_rule_armed_in_one_request() {
    // Enter in ⌘K: the rule is there and armed, or neither.
    Form(OperationRequest::new(
        RequestSurface::QuickInject,
        CreateLiveRule {
            environment: "orders-sim".parse().unwrap(),
            rule: OrderDesk::rule(),
            armed: true,
        },
    ))
    .writes(json!({
        "from": { "surface": "quick-inject" },
        "operation": "create-live-rule",
        "environment": "orders-sim",
        "rule": Form(OrderDesk::rule()).json(),
        "armed": true,
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
fn a_release_names_the_hold_that_its_press_added() {
    Form(AnyOperation::from(SendIntent {
        environment: "orders-sim".parse().unwrap(),
        intent: Intent::Release {
            pad: Pad::new(
                "order-desk".parse().unwrap(),
                "order-ingest".parse().unwrap(),
                Row::from_index(0),
                Column::from_index(3),
            ),
            hold: Hold::new(7),
        },
    }))
    .writes(json!({
        "operation": "send-intent",
        "environment": "orders-sim",
        "intent": {
            "intent": "release",
            "pad": { "layout": "order-desk", "layer": "order-ingest", "row": 0, "column": 3 },
            "hold": 7,
        },
    }));
}

#[test]
fn a_press_answers_with_the_rule_the_revision_it_found_whether_it_made_or_changed_it_and_its_hold() {
    let armed = RuleStatus {
        rule: OrderDesk::rule(),
        live: true,
        state: RuleState::Armed,
        revision: NOW,
    };
    // The rule comes with the revision it has now, which the undo of the press expects.
    let rule = json!({
        "rule": Form(OrderDesk::rule()).json(),
        "live": true,
        "state": "armed",
        "revision": NOW.number(),
    });

    // A press in hold of an empty pad made its live rule, armed it, and added a hold.
    Form(IntentEffect {
        rule: Some(armed.clone()),
        found: None,
        made: true,
        changed: true,
        hold: Some(Hold::new(7)),
    })
    .writes(json!({ "rule": rule, "made": true, "changed": true, "hold": 7 }));
    // A press in latch that found the rule armed changed nothing, and holds nothing.
    // It found the rule at the revision that the rule still has.
    Form(IntentEffect {
        rule: Some(armed),
        found: Some(NOW),
        made: false,
        changed: false,
        hold: None,
    })
    .writes(json!({ "rule": rule, "found": NOW.number(), "made": false, "changed": false }));
    // A disarm of a pad that points at no rule finds it as it wants it.
    Form(IntentEffect {
        rule: None,
        found: None,
        made: false,
        changed: false,
        hold: None,
    })
    .writes(json!({ "rule": null, "made": false, "changed": false }));
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
fn a_rule_of_an_environment_says_whether_it_is_live_its_state_and_its_revision() {
    Form(RuleStatus {
        rule: OrderDesk::rule(),
        live: false,
        state: RuleState::Armed,
        revision: NOW,
    })
    .writes(json!({
        "rule": Form(OrderDesk::rule()).json(),
        "live": false,
        "state": "armed",
        "revision": NOW.number(),
    }));
}

#[test]
fn an_environment_says_its_revision_and_whether_it_runs() {
    let environment = OrderDesk::environment();

    Form(EnvironmentStatus {
        environment: environment.clone(),
        revision: SEEN,
        running: true,
        loses_comment: false,
    })
    .writes(json!({ "environment": Form(&environment).json(), "revision": SEEN.number(), "running": true }));
}

#[test]
fn a_definition_travels_as_its_file_holds_it_with_its_revision_beside_it() {
    let rule = OrderDesk::rule();

    // The revision is live state: it is a key next to the definition, and never a key of it.
    Form(DefinitionStatus {
        definition: rule.clone(),
        revision: SEEN,
        loses_comment: false,
    })
    .writes(json!({ "definition": Form(&rule).json(), "revision": SEEN.number() }));
    Form::<DefinitionStatus<Rule>>::rejects(
        json!({
            "definition": {
                "name": "slow-order-producers",
                "selector": "order-producers",
                "action": "delay-1s",
                "revision": SEEN.number(),
            },
            "revision": SEEN.number(),
        }),
        "unknown field `revision`",
    );
}

#[test]
fn a_definition_whose_write_would_lose_a_comment_says_so_beside_it() {
    let selector = OrderDesk::selector();

    // Someone wrote a comment inside the selector in `selectors.yaml`, and a write of it would lose that comment.
    Form(DefinitionStatus {
        definition: selector.clone(),
        revision: SEEN,
        loses_comment: true,
    })
    .writes(json!({ "definition": Form(&selector).json(), "revision": SEEN.number(), "loses-comment": true }));
    Form(StreamMessage::DefinitionChanged {
        definition: AnyDefinition::Selector(selector.clone()),
        revision: NOW,
        loses_comment: true,
    })
    .writes(json!({
        "message": "definition-changed",
        "definition": { "selector": Form(&selector).json() },
        "revision": NOW.number(),
        "loses-comment": true,
    }));
}

#[test]
fn a_reload_says_nothing_but_its_operation_and_answers_with_what_it_changed() {
    Form(OperationRequest::new(
        RequestSurface::Cli,
        ReloadWorkspace { discard_unsaved: false },
    ))
    .writes(json!({ "from": { "surface": "cli" }, "operation": "reload-workspace" }));
    // A reload that lets go of what nobody saved says so.
    Form(OperationRequest::new(
        RequestSurface::Ui,
        ReloadWorkspace { discard_unsaved: true },
    ))
    .writes(json!({ "from": { "surface": "ui" }, "operation": "reload-workspace", "discard-unsaved": true }));
    let producers: SelectorName = "order-producers".parse().unwrap();
    let slow: RuleName = "slow-order-producers".parse().unwrap();
    let blackhole: PresetName = "blackhole".parse().unwrap();

    // Someone edited a selector, wrote a new rule and took a preset away, and the reload took all of it in.
    Form(Reloaded {
        revision: NOW,
        changes: vec![
            DefinitionChange::Updated(producers.into()),
            DefinitionChange::Deleted(blackhole.into()),
            DefinitionChange::Created(slow.into()),
        ],
    })
    .writes(json!({
        "revision": NOW.number(),
        "changes": [
            { "updated": { "selector": "order-producers" } },
            { "deleted": { "preset": "blackhole" } },
            { "created": { "rule": "slow-order-producers" } },
        ],
    }));
    // A reload that changed nothing still took a number.
    Form(Reloaded {
        revision: NOW,
        changes: Vec::new(),
    })
    .writes(json!({ "revision": NOW.number() }));
}

#[test]
fn a_reload_that_changed_nothing_says_why() {
    let environment: EnvironmentName = "orders-sim".parse().unwrap();
    let rule: RuleName = "slow-order-producers".parse().unwrap();

    Form(ReloadWorkspaceError::NameTaken {
        rule: rule.clone(),
        holder: RuleHolder::Environment(environment.clone()),
    })
    .writes(json!({
        "error": "name-taken",
        "rule": "slow-order-producers",
        "holder": { "environment": "orders-sim" },
    }));
    Form(ReloadWorkspaceError::Unsaved {
        definitions: vec!["order-producers".parse::<SelectorName>().unwrap().into()],
    })
    .writes(json!({ "error": "unsaved", "definitions": [{ "selector": "order-producers" }] }));
    Form(ReloadWorkspaceError::LiveRuleDoesNotFit {
        environment,
        rule,
        reason: "rule \"slow-order-producers\": no selector is named \"order-producers\"".to_owned(),
    })
    .writes(json!({
        "error": "live-rule-does-not-fit",
        "environment": "orders-sim",
        "rule": "slow-order-producers",
        "reason": "rule \"slow-order-producers\": no selector is named \"order-producers\"",
    }));
    Form(ReloadWorkspaceError::DoesNotFit {
        reason: "rule \"slow-order-producers\" refers to preset \"delay-1s\", which does not exist".to_owned(),
    })
    .writes(json!({
        "error": "does-not-fit",
        "reason": "rule \"slow-order-producers\" refers to preset \"delay-1s\", which does not exist",
    }));
    Form(ReloadWorkspaceError::StoreFailed {
        reason: "the rule store could not be read again: rules/orders.yaml: [2].trigger.every: \
                 a count is at least one"
            .to_owned(),
    })
    .writes(json!({
        "error": "store-failed",
        "reason": "the rule store could not be read again: rules/orders.yaml: [2].trigger.every: \
                   a count is at least one",
    }));
}

#[test]
fn a_change_that_an_armed_live_rule_could_not_follow_says_which() {
    let selector: DefinitionRef = "order-producers".parse::<SelectorName>().unwrap().into();
    let reason = "rule \"hold-the-produces\": no selector is named \"order-producers\"";
    let json = json!({
        "error": "live-rule-does-not-fit",
        "name": { "selector": "order-producers" },
        "environment": "orders-sim",
        "rule": "hold-the-produces",
        "reason": reason,
    });

    Form(DeleteDefinitionError::LiveRuleDoesNotFit {
        name: selector.clone(),
        environment: "orders-sim".parse().unwrap(),
        rule: "hold-the-produces".parse().unwrap(),
        reason: reason.to_owned(),
    })
    .writes(json.clone());
    Form(UpdateDefinitionError::LiveRuleDoesNotFit {
        name: selector,
        environment: "orders-sim".parse().unwrap(),
        rule: "hold-the-produces".parse().unwrap(),
        reason: reason.to_owned(),
    })
    .writes(json);
}

#[test]
fn a_write_over_a_file_that_someone_changed_since_is_changed_outside() {
    let selector: DefinitionRef = "order-producers".parse::<SelectorName>().unwrap().into();
    let changed = "selectors.yaml changed since the store read it";

    Form(CreateDefinitionError::ChangedOutside {
        name: selector.clone(),
        reason: changed.to_owned(),
    })
    .writes(json!({ "error": "changed-outside", "name": { "selector": "order-producers" }, "reason": changed }));
    Form(UpdateDefinitionError::ChangedOutside {
        name: selector.clone(),
        reason: changed.to_owned(),
    })
    .writes(json!({ "error": "changed-outside", "name": { "selector": "order-producers" }, "reason": changed }));
    Form(DeleteDefinitionError::ChangedOutside {
        name: selector,
        reason: changed.to_owned(),
    })
    .writes(json!({ "error": "changed-outside", "name": { "selector": "order-producers" }, "reason": changed }));
    let appeared = "rules/slow-order-producers.yaml exists now, and did not when the store read its files";
    Form(KeepRuleError::ChangedOutside {
        rule: "slow-order-producers".parse().unwrap(),
        reason: appeared.to_owned(),
    })
    .writes(json!({ "error": "changed-outside", "rule": "slow-order-producers", "reason": appeared }));
}

#[test]
fn a_change_answers_with_the_revision_it_left() {
    Form(Revised { revision: NOW }).writes(json!({ "revision": NOW.number() }));
}

#[test]
fn a_revision_is_a_number_and_a_run_is_32_hexadecimal_digits_in_lower_case() {
    let run: RunId = RUN.parse().unwrap();

    Form(SEEN).writes(json!(SEEN.number()));
    Form(run).writes(json!(RUN));
    Form::<RunId>::rejects(json!(RUN.to_uppercase()), "which is no digit and no letter from a to f");
    Form::<RunId>::rejects(
        json!("4be0643f"),
        "a run ID has 32 hexadecimal digits, and not 8 characters",
    );
    Form::<Revision>::rejects(json!("7"), "invalid type");
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

    let json = json!({
        "calls": 3,
        "missed-events": 0,
        "apis": { "Produce": 2, "Fetch": 1 },
        "rules": { "slow-order-producers": { "matches": 2, "firings": 1, "left": 2 } },
    });

    Form(stats.clone()).writes(json.clone());
    // Inside a message, whose `message` key the reader looks for first, the keys of the maps read the same.
    Form(StreamMessage::Stats {
        environment: "orders-sim".parse().unwrap(),
        stats,
    })
    .writes(json!({ "message": "stats", "environment": "orders-sim", "stats": json }));
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
fn a_name_that_another_rule_has_is_taken_with_what_holds_it() {
    let name: RuleName = "slow-order-producers".parse().unwrap();

    Form(KeepRuleError::NameTaken {
        rule: name.clone(),
        holder: Some(RuleHolder::Environment("canary-proxy".parse().unwrap())),
    })
    .writes(json!({
        "error": "name-taken",
        "rule": "slow-order-producers",
        "holder": { "environment": "canary-proxy" },
    }));
    Form(CreateDefinitionError::NameTaken {
        name: name.clone().into(),
        holder: Some(RuleHolder::Scenario("leader-election".parse().unwrap())),
    })
    .writes(json!({
        "error": "name-taken",
        "name": { "rule": "slow-order-producers" },
        "holder": { "scenario": "leader-election" },
    }));
    Form(CreateLiveRuleError::NameTaken {
        environment: "orders-sim".parse().unwrap(),
        rule: name.clone(),
        holder: Some(RuleHolder::Scenario("leader-election".parse().unwrap())),
    })
    .writes(json!({
        "error": "name-taken",
        "environment": "orders-sim",
        "rule": "slow-order-producers",
        "holder": { "scenario": "leader-election" },
    }));
    // A name that the workspace has is taken without a holder.
    Form(KeepRuleError::NameTaken {
        rule: name.clone(),
        holder: None,
    })
    .writes(json!({ "error": "name-taken", "rule": "slow-order-producers" }));
    Form(CreateDefinitionError::NameTaken {
        name: name.into(),
        holder: None,
    })
    .writes(json!({ "error": "name-taken", "name": { "rule": "slow-order-producers" } }));
}

#[test]
fn a_change_that_expects_another_revision_says_which_revision_holds_now() {
    let name: RuleName = "slow-order-producers".parse().unwrap();
    let environment: EnvironmentName = "orders-sim".parse().unwrap();

    // A definition names itself with its kind, and a rule in an environment with its environment.
    let of_a_definition =
        json!({ "error": "changed", "name": { "rule": "slow-order-producers" }, "revision": NOW.number() });
    Form(UpdateDefinitionError::Changed {
        name: name.clone().into(),
        revision: NOW,
    })
    .writes(of_a_definition.clone());
    Form(DeleteDefinitionError::Changed {
        name: name.clone().into(),
        revision: NOW,
    })
    .writes(of_a_definition);
    let of_a_rule = json!({
        "error": "changed",
        "environment": "orders-sim",
        "rule": "slow-order-producers",
        "revision": NOW.number(),
    });
    Form(RuleCommandError::Changed {
        environment: environment.clone(),
        rule: name.clone(),
        revision: NOW,
    })
    .writes(of_a_rule.clone());
    Form(UpdateLiveRuleError::Changed {
        environment: environment.clone(),
        rule: name.clone(),
        revision: NOW,
    })
    .writes(of_a_rule.clone());
    Form(DeleteLiveRuleError::Changed {
        environment: environment.clone(),
        rule: name.clone(),
        revision: NOW,
    })
    .writes(of_a_rule.clone());
    let kept_late = KeepRuleError::Changed {
        environment,
        rule: name,
        revision: NOW,
    };
    assert_eq!(
        kept_late.to_string(),
        format!("rule \"slow-order-producers\" changed in environment \"orders-sim\" since: it has revision {NOW} now")
    );
    Form(kept_late).writes(of_a_rule);
}

#[test]
fn a_change_that_expects_another_run_names_the_run_of_the_process() {
    let run: RunId = RUN.parse().unwrap();
    let other_run = json!({ "error": "other-run", "run": RUN });

    Form(UpdateDefinitionError::OtherRun { run }).writes(other_run.clone());
    Form(DeleteDefinitionError::OtherRun { run }).writes(other_run.clone());
    Form(RuleCommandError::OtherRun { run }).writes(other_run.clone());
    Form(UpdateLiveRuleError::OtherRun { run }).writes(other_run.clone());
    Form(DeleteLiveRuleError::OtherRun { run }).writes(other_run.clone());
    let kept_late = KeepRuleError::OtherRun { run };
    assert_eq!(
        kept_late.to_string(),
        format!("the change expects a revision of another run than {RUN}, which is the run of the process")
    );
    Form(kept_late).writes(other_run);
}

#[test]
fn a_firing_fails_in_its_own_ways_and_never_for_a_revision() {
    Form(FireRuleError::Refused {
        environment: "orders-sim".parse().unwrap(),
        rule: "slow-order-producers".parse().unwrap(),
        reason: "it is not armed".to_owned(),
    })
    .writes(json!({
        "error": "refused",
        "environment": "orders-sim",
        "rule": "slow-order-producers",
        "reason": "it is not armed",
    }));
    Form::<FireRuleError>::rejects(
        json!({ "error": "other-run", "run": RUN }),
        "unknown variant `other-run`",
    );
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
            expects: None,
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
            expects: None,
        }),
        AnyOperation::from(FireRule {
            environment: environment.clone(),
            rule,
        }),
        AnyOperation::from(ParseSentence {
            environment: environment.clone(),
            text: "delay produce".to_owned(),
        }),
        AnyOperation::from(QueryJournal {
            environment,
            after: None,
        }),
        AnyOperation::from(ReloadWorkspace { discard_unsaved: false }),
        AnyOperation::from(SaveWorkspace {}),
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
fn the_stream_names_the_presence_of_its_client() {
    Form(StreamMessage::Present {
        presence: PresenceId::from_random_bits(0x9f86_d081_884c_7d65_9a2f_eaa0_c55a_d015),
    })
    .writes(json!({ "message": "present", "presence": "9f86d081884c7d659a2feaa0c55ad015" }));
}

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
fn the_state_of_a_process_says_how_its_workspace_writes_and_what_nobody_saved() {
    let producers: SelectorName = "order-producers".parse().unwrap();
    let slow: RuleName = "slow-order-producers".parse().unwrap();
    let state = ProcessState {
        run: RUN.parse().unwrap(),
        definitions: Definitions::default(),
        workspace: WorkspaceStatus {
            write_back: WriteBack::OnSave,
            read_only: false,
            unsaved: vec![producers.into(), slow.into()],
        },
        environments: Vec::new(),
    };

    Form(StreamMessage::Snapshot(state)).writes(json!({
        "message": "snapshot",
        "run": RUN,
        "definitions": {
            "selectors": [],
            "presets": [],
            "rules": [],
            "scenarios": [],
            "layouts": [],
            "environments": [],
        },
        "workspace": {
            "write-back": "on-save",
            "unsaved": [{ "selector": "order-producers" }, { "rule": "slow-order-producers" }],
        },
        "environments": [],
    }));
    // CI mounts the workspace read-only, and a surface reads that it takes no change.
    Form(StreamMessage::WorkspaceChanged {
        workspace: WorkspaceStatus {
            write_back: WriteBack::EachChange,
            read_only: true,
            unsaved: Vec::new(),
        },
    })
    .writes(json!({
        "message": "workspace-changed",
        "workspace": { "write-back": "each-change", "read-only": true },
    }));
}

#[test]
fn a_save_says_nothing_but_its_operation_and_answers_with_what_it_wrote() {
    Form(OperationRequest::new(RequestSurface::Ui, SaveWorkspace {}))
        .writes(json!({ "from": { "surface": "ui" }, "operation": "save-workspace" }));
    let producers: SelectorName = "order-producers".parse().unwrap();
    Form(Saved {
        saved: vec![producers.into()],
    })
    .writes(json!({ "saved": [{ "selector": "order-producers" }] }));
    // A workspace with nothing unsaved saves nothing.
    Form(Saved::default()).writes(json!({}));
    let changed = "selectors.yaml changed since the store read it; rules/slow.yaml is gone since the store read it";
    Form(SaveWorkspaceError::ChangedOutside {
        reason: changed.to_owned(),
    })
    .writes(json!({ "error": "changed-outside", "reason": changed }));
    Form(SaveWorkspaceError::StoreFailed {
        reason: "the rule store did not save: rules/slow.yaml: disk full".to_owned(),
    })
    .writes(json!({ "error": "store-failed", "reason": "the rule store did not save: rules/slow.yaml: disk full" }));
    // The renames stopped after the selector: the rule still waits.
    Form(SaveWorkspaceError::PartlySaved {
        saved: vec!["order-producers".parse::<SelectorName>().unwrap().into()],
        reason: "the rule store did not save: rules/slow.yaml: permission denied".to_owned(),
    })
    .writes(json!({
        "error": "partly-saved",
        "saved": [{ "selector": "order-producers" }],
        "reason": "the rule store did not save: rules/slow.yaml: permission denied",
    }));
}

#[test]
fn a_client_that_connects_gets_the_state_of_the_process() {
    let environment = OrderDesk::environment();
    let state = ProcessState {
        run: RUN.parse().unwrap(),
        definitions: Definitions {
            selectors: vec![DefinitionStatus {
                definition: OrderDesk::selector(),
                revision: NOW,
                loses_comment: false,
            }],
            environments: vec![DefinitionStatus {
                definition: environment.clone(),
                revision: SEEN,
                loses_comment: false,
            }],
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
        workspace: WorkspaceStatus::default(),
    };

    // The snapshot names the run, and whatever has a revision comes with it.
    Form(StreamMessage::Snapshot(state)).writes(json!({
        "message": "snapshot",
        "run": RUN,
        "definitions": {
            "selectors": [{ "definition": Form(OrderDesk::selector()).json(), "revision": NOW.number() }],
            "presets": [],
            "rules": [],
            "scenarios": [],
            "layouts": [],
            "environments": [{ "definition": Form(&environment).json(), "revision": SEEN.number() }],
        },
        "workspace": { "write-back": "each-change" },
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
fn an_environment_with_brokers_travels_inside_a_message() {
    // A proxy in front of a real cluster, with a port pinned for each broker.
    let brokers = BrokerListeners::pinned([
        (BrokerId::from(1), "0.0.0.0:19092".parse().unwrap()),
        (BrokerId::from(2), "0.0.0.0:19093".parse().unwrap()),
    ])
    .unwrap();
    let listen = Listeners::new("0.0.0.0:9092".parse().unwrap(), brokers, "localhost".parse().unwrap()).unwrap();
    let staging = Environment::new(
        "staging-proxy".parse().unwrap(),
        Instances::ProxyInFrontOfCluster {
            proxy: ProxyDefinition::new(listen),
            upstream: Upstream::new("kafka-1.staging:9092".parse().unwrap()),
        },
    );
    let running = RunningEnvironment {
        environment: staging.name().clone(),
        backend: BackendStatus {
            kind: BackendKind::Upstream,
            endpoints: BTreeMap::from([
                (Endpoint::Bootstrap, Some("127.0.0.1:9092".parse().unwrap())),
                (
                    Endpoint::Broker(BrokerId::from(1)),
                    Some("127.0.0.1:19092".parse().unwrap()),
                ),
                (Endpoint::Broker(BrokerId::from(2)), None),
            ]),
            incidents: IncidentSupport::none(),
        },
        rules: Vec::new(),
        sessions: Vec::new(),
    };
    let staging_json = json!({
        "name": "staging-proxy",
        "proxy": {
            "upstream": { "bootstrap": "kafka-1.staging:9092" },
            "listen": {
                "bootstrap": "0.0.0.0:9092",
                "brokers": { "1": "0.0.0.0:19092", "2": "0.0.0.0:19093" },
                "advertise": "localhost",
            },
        },
    });
    let running_json = json!({
        "environment": "staging-proxy",
        "backend": {
            "kind": "upstream",
            "endpoints": { "bootstrap": "127.0.0.1:9092", "broker 1": "127.0.0.1:19092", "broker 2": null },
            "incidents": Form(IncidentSupport::none()).json(),
        },
        "rules": [],
        "sessions": [],
    });

    assert_eq!(Form(&staging).json(), staging_json);
    Form(StreamMessage::Snapshot(ProcessState {
        run: RUN.parse().unwrap(),
        definitions: Definitions {
            environments: vec![DefinitionStatus {
                definition: staging.clone(),
                revision: SEEN,
                loses_comment: false,
            }],
            ..Definitions::default()
        },
        environments: vec![running.clone()],
        workspace: WorkspaceStatus::default(),
    }))
    .writes(json!({
        "message": "snapshot",
        "run": RUN,
        "definitions": {
            "selectors": [],
            "presets": [],
            "rules": [],
            "scenarios": [],
            "layouts": [],
            "environments": [{ "definition": staging_json, "revision": SEEN.number() }],
        },
        "workspace": { "write-back": "each-change" },
        "environments": [running_json],
    }));
    Form(StreamMessage::DefinitionChanged {
        definition: AnyDefinition::Environment(staging),
        revision: NOW,
        loses_comment: false,
    })
    .writes(json!({
        "message": "definition-changed",
        "definition": { "environment": staging_json },
        "revision": NOW.number(),
    }));
    Form(StreamMessage::EnvironmentStarted { environment: running })
        .writes(json!({ "message": "environment-started", "environment": running_json }));
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
            revision: NOW,
        },
    })
    .writes(json!({
        "message": "rule-changed",
        "environment": "orders-sim",
        "rule": { "rule": Form(OrderDesk::rule()).json(), "live": true, "state": "armed", "revision": NOW.number() },
    }));
}

#[test]
fn a_change_to_the_workspace_names_the_kind_of_its_definition() {
    Form(StreamMessage::DefinitionChanged {
        definition: AnyDefinition::Selector(OrderDesk::selector()),
        revision: NOW,
        loses_comment: false,
    })
    .writes(json!({
        "message": "definition-changed",
        "definition": { "selector": Form(OrderDesk::selector()).json() },
        "revision": NOW.number(),
    }));
    Form(StreamMessage::DefinitionDeleted {
        name: "blackhole".parse::<PresetName>().unwrap().into(),
    })
    .writes(json!({ "message": "definition-deleted", "name": { "preset": "blackhole" } }));
}

#[test]
fn a_scenario_travels_as_its_file_holds_it() {
    let scenario = Scenario::new(
        "flaky-orders".parse().unwrap(),
        Mode::Chain,
        Seed::new(42),
        [OrderDesk::rule().name().clone().into()],
    )
    .unwrap();

    Form(StreamMessage::DefinitionChanged {
        definition: AnyDefinition::Scenario(scenario.clone()),
        revision: NOW,
        loses_comment: false,
    })
    .writes(json!({
        "message": "definition-changed",
        "definition": { "scenario": Form(scenario).json() },
        "revision": NOW.number(),
    }));
    Form(StreamMessage::DefinitionDeleted {
        name: "flaky-orders".parse::<ScenarioName>().unwrap().into(),
    })
    .writes(json!({ "message": "definition-deleted", "name": { "scenario": "flaky-orders" } }));
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

// The journal

#[test]
fn a_client_queries_the_journal_of_an_environment_and_reads_on_after_an_entry() {
    let environment: EnvironmentName = "orders-sim".parse().unwrap();
    let whole = QueryJournal {
        environment: environment.clone(),
        after: None,
    };
    let last_read = JournalEntryId::new(41).unwrap();
    let rest = QueryJournal {
        environment,
        after: Some(last_read),
    };

    Form(OperationRequest::new(RequestSurface::Cli, whole)).writes(json!({
        "from": { "surface": "cli" },
        "operation": "query-journal",
        "environment": "orders-sim",
    }));
    Form(rest).writes(json!({ "environment": "orders-sim", "after": 41 }));
    // The entries of an environment count from one.
    Form::<QueryJournal>::rejects(
        json!({ "environment": "orders-sim", "after": 0 }),
        "a journal entry ID is at least one",
    );
}

#[test]
fn an_entry_of_the_journal_travels_with_its_environment() {
    // Someone armed a rule at a pad 4.2 seconds after the environment started, which left the rule at revision 8.
    let armed = JournalEntry {
        id: JournalEntryId::new(7).unwrap(),
        at: Instant::ZERO + Duration::from_millis(4200),
        revision: Some(NOW),
        from: ChangeSource::new(ChangeSurface::Pad),
        change: Change::RuleArmed {
            rule: "slow-order-producers".parse().unwrap(),
        },
    };

    Form(StreamMessage::JournalEntry {
        environment: "orders-sim".parse().unwrap(),
        entry: armed,
    })
    .writes(json!({
        "message": "journal-entry",
        "environment": "orders-sim",
        "entry": {
            "id": 7,
            "at": 4_200_000_000_u64,
            "revision": 8,
            "from": { "surface": "pad" },
            "change": "rule-armed",
            "rule": "slow-order-producers",
        },
    }));
}

#[test]
fn the_journal_answers_with_its_entries_the_oldest_first() {
    let rule = || "slow-order-producers".parse().unwrap();
    // Someone turned the delay of a live rule up and took that back with ⌘Z, someone fired the rule by hand,
    // and its lifetime ended.
    let turned = JournalEntry {
        id: JournalEntryId::new(3).unwrap(),
        at: Instant::ZERO + Duration::from_secs(10),
        revision: Some(SEEN),
        from: ChangeSource::new(ChangeSurface::Pad),
        change: Change::ParameterChanged {
            rule: rule(),
            parameter: Parameter::Delay,
            old: "1s".to_owned(),
            new: "1100ms".to_owned(),
        },
    };
    let taken_back = JournalEntry {
        id: JournalEntryId::new(4).unwrap(),
        at: Instant::ZERO + Duration::from_secs(12),
        revision: Some(NOW),
        from: ChangeSource::new(ChangeSurface::Ui).with_cause(ChangeCause::Undo),
        change: Change::LiveRuleChanged { rule: rule() },
    };
    let fired = JournalEntry {
        id: JournalEntryId::new(5).unwrap(),
        at: Instant::ZERO + Duration::from_secs(20),
        revision: None,
        from: ChangeSource::new(ChangeSurface::Cli),
        change: Change::RuleFiredByHand { rule: rule() },
    };
    let ended = JournalEntry {
        id: JournalEntryId::new(6).unwrap(),
        at: Instant::ZERO + Duration::from_secs(30),
        revision: Some(Revision::new(9)),
        from: ChangeSource::new(ChangeSurface::Lifetime),
        change: Change::RuleDisarmed { rule: rule() },
    };
    let answer: <QueryJournal as Operation>::Answer = vec![turned, taken_back, fired, ended];

    Form(answer).writes(json!([
        {
            "id": 3,
            "at": 10_000_000_000_u64,
            "revision": 7,
            "from": { "surface": "pad" },
            "change": "parameter-changed",
            "rule": "slow-order-producers",
            "parameter": "delay",
            "old": "1s",
            "new": "1100ms",
        },
        {
            "id": 4,
            "at": 12_000_000_000_u64,
            "revision": 8,
            "from": { "surface": "ui", "cause": "undo" },
            "change": "live-rule-changed",
            "rule": "slow-order-producers",
        },
        // A firing left no revision.
        {
            "id": 5,
            "at": 20_000_000_000_u64,
            "from": { "surface": "cli" },
            "change": "rule-fired-by-hand",
            "rule": "slow-order-producers",
        },
        // No request names the surface `lifetime`, and an entry does.
        {
            "id": 6,
            "at": 30_000_000_000_u64,
            "revision": 9,
            "from": { "surface": "lifetime" },
            "change": "rule-disarmed",
            "rule": "slow-order-producers",
        },
    ]));
}

#[test]
fn the_journal_records_the_surface_and_the_cause_of_a_request_and_not_its_presence() {
    let tab: PresenceId = "9f86d081884c7d659a2feaa0c55ad015".parse().unwrap();
    let undo_of_a_tab = RequestSource::new(RequestSurface::Pad)
        .with_cause(RequestCause::Undo)
        .with_presence(tab);

    assert_eq!(
        ChangeSource::from(undo_of_a_tab),
        ChangeSource::new(ChangeSurface::Pad).with_cause(ChangeCause::Undo)
    );
    assert_eq!(ChangeCause::from(RequestCause::Redo), ChangeCause::Redo);
    // Every surface of a request is a surface of the journal, under the same name.
    let surfaces = [
        (RequestSurface::Pad, ChangeSurface::Pad),
        (RequestSurface::QuickInject, ChangeSurface::QuickInject),
        (RequestSurface::Ui, ChangeSurface::Ui),
        (RequestSurface::Cli, ChangeSurface::Cli),
        (RequestSurface::Test, ChangeSurface::Test),
        (RequestSurface::Api, ChangeSurface::Api),
    ];
    for (of_a_request, of_the_journal) in surfaces {
        assert_eq!(ChangeSurface::from(of_a_request), of_the_journal);
        assert_eq!(Form(of_a_request).json(), Form(of_the_journal).json());
    }
}
