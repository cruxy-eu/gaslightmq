//! What someone types in the ⌘K palette, and the rule they get.
//!
//! Every table has the typed text on the left, and on the right the rule as a file writes it.

#![expect(
    clippy::unwrap_used,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

mod common;

use common::Shop;
use gaslightmq_clock::TimeSpan;
use gaslightmq_quick_inject::prelude::*;
use gaslightmq_rule::{Rule, RuleTitle};
use serde_json::json;

#[test]
fn a_sentence_says_what_happens_to_which_calls_of_whom() {
    let quick_inject = QuickInject::default();
    let known = Shop::known();

    let sentences = [
        (
            "delay produce orders.* 750ms 40% order-ingest",
            json!({
                "name": "delay-750ms-on-produce-orders-for-order-ingest",
                "title": "Delay 750ms on Produce orders.* for order-ingest",
                "selector": { "api": "Produce", "topic": "orders.*" },
                "scope": { "client.id": "order-ingest" },
                "action": { "lie": "timing", "delay": "750ms" },
                "trigger": { "chance": "40%" },
            }),
        ),
        (
            "error produce fetch NOT_LEADER_OR_FOLLOWER 30% for 20s",
            json!({
                "name": "refuse-with-not-leader-or-follower-on-produce-fetch",
                "title": "Refuse with NOT_LEADER_OR_FOLLOWER on Produce + Fetch",
                "selector": { "api": ["Produce", "Fetch"] },
                "action": { "lie": "refusal", "error": "NOT_LEADER_OR_FOLLOWER" },
                "trigger": { "chance": "30%" },
                "lifetime": "20s",
            }),
        ),
        (
            "error heartbeat REBALANCE_IN_PROGRESS analytics-etl 60%",
            json!({
                "name": "refuse-with-rebalance-in-progress-on-heartbeat-group-analytics",
                "title": "Refuse with REBALANCE_IN_PROGRESS on Heartbeat group analytics-etl",
                "selector": { "api": "Heartbeat", "group": "analytics-etl" },
                "action": { "lie": "refusal", "error": "REBALANCE_IN_PROGRESS" },
                "trigger": { "chance": "60%" },
            }),
        ),
        (
            "delay b2 2s 100%",
            json!({
                "name": "delay-2s-on-all-calls-at-broker-2",
                "title": "Delay 2s on all calls at broker 2",
                "selector": {},
                "scope": { "endpoint": "broker 2" },
                "action": { "lie": "timing", "delay": "2s" },
                "trigger": { "chance": "100%" },
            }),
        ),
        (
            "blackhole metadata for 3 firings",
            json!({
                "name": "blackhole-on-metadata",
                "title": "Blackhole on Metadata",
                "selector": { "api": "Metadata" },
                "action": { "lie": "silence" },
                "lifetime": "3 firings",
            }),
        ),
        (
            "disconnect orders.eu orders.us for 100 matches",
            json!({
                "name": "disconnect-on-orders-eu-orders-us",
                "title": "Disconnect on orders.eu + orders.us",
                "selector": { "topic": ["orders.eu", "orders.us"] },
                "action": { "lie": "hang-up" },
                "lifetime": "100 matches",
            }),
        ),
    ];

    for (typed, rule) in sentences {
        let rule: Rule = serde_json::from_value(rule).unwrap();
        let sentence = quick_inject.read(typed, &known);

        assert_eq!(sentence.rule(), Some(&rule), "{typed}");
        assert_eq!(sentence.unread(), [], "{typed}");
    }
}

#[test]
fn the_words_stand_in_any_order_and_in_any_case() {
    let quick_inject = QuickInject::default();
    let known = Shop::known();
    let rule: Rule = serde_json::from_value(json!({
        "name": "delay-750ms-on-produce-orders-for-order-ingest",
        "title": "Delay 750ms on Produce orders.* for order-ingest",
        "selector": { "api": "Produce", "topic": "orders.*" },
        "scope": { "client.id": "order-ingest" },
        "action": { "lie": "timing", "delay": "750ms" },
        "trigger": { "chance": "40%" },
    }))
    .unwrap();

    let sentences = [
        "delay produce orders.* 750ms 40% order-ingest",
        "order-ingest 40% 750ms orders.* produce delay",
        "Delay PRODUCE orders.* 750ms 40% order-ingest",
        "  delay   produce\torders.*  750ms 40%  order-ingest ",
        "delay produce on orders.* by 750ms with 40% for order-ingest",
        "delay produce topic orders.* 750ms 40% client order-ingest",
    ];

    for typed in sentences {
        let sentence = quick_inject.read(typed, &known);

        assert_eq!(sentence.rule(), Some(&rule), "{typed}");
        assert_eq!(sentence.unread(), [], "{typed}");
    }
}

#[test]
fn a_sentence_leaves_out_what_has_a_default() {
    let quick_inject = QuickInject::default();
    let known = Shop::known();

    let sentences = [
        (
            "delay",
            json!({
                "name": "delay-500ms-on-all-calls",
                "title": "Delay 500ms on all calls",
                "selector": {},
                "action": { "lie": "timing", "delay": "500ms" },
            }),
        ),
        (
            "throttle fetch",
            json!({
                "name": "throttle-2s-on-fetch",
                "title": "Throttle 2s on Fetch",
                "selector": { "api": "Fetch" },
                "action": { "lie": "timing", "throttle": "2s" },
            }),
        ),
        (
            "throttle fetch 150ms",
            json!({
                "name": "throttle-150ms-on-fetch",
                "title": "Throttle 150ms on Fetch",
                "selector": { "api": "Fetch" },
                "action": { "lie": "timing", "throttle": "150ms" },
            }),
        ),
        // An error code is a refusal, and a time span is a delay.
        (
            "request_timed_out produce 20%",
            json!({
                "name": "refuse-with-request-timed-out-on-produce",
                "title": "Refuse with REQUEST_TIMED_OUT on Produce",
                "selector": { "api": "Produce" },
                "action": { "lie": "refusal", "error": "REQUEST_TIMED_OUT" },
                "trigger": { "chance": "20%" },
            }),
        ),
        (
            "fetch 250ms",
            json!({
                "name": "delay-250ms-on-fetch",
                "title": "Delay 250ms on Fetch",
                "selector": { "api": "Fetch" },
                "action": { "lie": "timing", "delay": "250ms" },
            }),
        ),
    ];

    for (typed, rule) in sentences {
        let rule: Rule = serde_json::from_value(rule).unwrap();
        let sentence = quick_inject.read(typed, &known);

        assert_eq!(sentence.rule(), Some(&rule), "{typed}");
        assert_eq!(sentence.unread(), [], "{typed}");
    }
}

#[test]
fn the_configuration_says_what_a_sentence_leaves_out() {
    let mut configuration = QuickInjectConfiguration::default();
    configuration.default_delay = TimeSpan::from_secs(3);
    configuration.default_throttle = TimeSpan::from_millis(100);
    let quick_inject = QuickInject::new(configuration);
    let known = Shop::known();

    let sentences = [
        (
            "delay fetch",
            json!({
                "name": "delay-3s-on-fetch",
                "title": "Delay 3s on Fetch",
                "selector": { "api": "Fetch" },
                "action": { "lie": "timing", "delay": "3s" },
            }),
        ),
        (
            "throttle fetch",
            json!({
                "name": "throttle-100ms-on-fetch",
                "title": "Throttle 100ms on Fetch",
                "selector": { "api": "Fetch" },
                "action": { "lie": "timing", "throttle": "100ms" },
            }),
        ),
    ];

    for (typed, rule) in sentences {
        let rule: Rule = serde_json::from_value(rule).unwrap();

        assert_eq!(quick_inject.read(typed, &known).rule(), Some(&rule), "{typed}");
    }
}

#[test]
fn a_lie_has_the_word_of_the_palette_and_the_word_of_the_glossary() {
    let quick_inject = QuickInject::default();
    let known = Shop::known();

    let sentences = [
        ("error metadata NOT_CONTROLLER", "refuse metadata NOT_CONTROLLER"),
        ("blackhole metadata", "silence metadata"),
        ("disconnect metadata", "hang-up metadata"),
    ];

    for (palette, glossary) in sentences {
        let rule = quick_inject.read(palette, &known).into_rule();

        assert!(rule.is_some(), "{palette}");
        assert_eq!(quick_inject.read(glossary, &known).into_rule(), rule, "{glossary}");
    }
}

#[test]
fn a_name_the_system_does_not_know_stands_after_a_word_that_says_what_it_is() {
    let quick_inject = QuickInject::default();
    let known = Shop::known();

    let sentences = [
        // `to` says that `returns` is a topic, and `client` says that `billing` is a client.
        (
            "delay produce to returns by 2s for client billing",
            json!({
                "name": "delay-2s-on-produce-returns-for-billing",
                "title": "Delay 2s on Produce returns for billing",
                "selector": { "api": "Produce", "topic": "returns" },
                "scope": { "client.id": "billing" },
                "action": { "lie": "timing", "delay": "2s" },
            }),
        ),
        (
            "delay fetch from returns for billing",
            json!({
                "name": "delay-500ms-on-fetch-returns-for-billing",
                "title": "Delay 500ms on Fetch returns for billing",
                "selector": { "api": "Fetch", "topic": "returns" },
                "scope": { "client.id": "billing" },
                "action": { "lie": "timing", "delay": "500ms" },
            }),
        ),
        (
            "hang-up JOINGROUP SyncGroup group etl-* broker 1",
            json!({
                "name": "disconnect-on-joingroup-syncgroup-group-etl-at-broker-1",
                "title": "Disconnect on JoinGroup + SyncGroup group etl-* at broker 1",
                "selector": { "api": ["JoinGroup", "SyncGroup"], "group": "etl-*" },
                "scope": { "endpoint": "broker 1" },
                "action": { "lie": "hang-up" },
            }),
        ),
        (
            "delay client batch-*",
            json!({
                "name": "delay-500ms-on-all-calls-for-batch",
                "title": "Delay 500ms on all calls for batch-*",
                "selector": {},
                "scope": { "client.id": "batch-*" },
                "action": { "lie": "timing", "delay": "500ms" },
            }),
        ),
    ];

    for (typed, rule) in sentences {
        let rule: Rule = serde_json::from_value(rule).unwrap();
        let sentence = quick_inject.read(typed, &known);

        assert_eq!(sentence.rule(), Some(&rule), "{typed}");
        assert_eq!(sentence.unread(), [], "{typed}");
    }
}

#[test]
fn a_name_of_two_kinds_is_the_kind_the_word_before_it_says() {
    let quick_inject = QuickInject::default();
    let known = Shop::known();

    let sentences = [
        // A topic comes before a client.
        (
            "disconnect payments",
            json!({
                "name": "disconnect-on-payments",
                "title": "Disconnect on payments",
                "selector": { "topic": "payments" },
                "action": { "lie": "hang-up" },
            }),
        ),
        (
            "disconnect for payments",
            json!({
                "name": "disconnect-on-all-calls-for-payments",
                "title": "Disconnect on all calls for payments",
                "selector": {},
                "scope": { "client.id": "payments" },
                "action": { "lie": "hang-up" },
            }),
        ),
        (
            "disconnect fetch from payments client payments",
            json!({
                "name": "disconnect-on-fetch-payments-for-payments",
                "title": "Disconnect on Fetch payments for payments",
                "selector": { "api": "Fetch", "topic": "payments" },
                "scope": { "client.id": "payments" },
                "action": { "lie": "hang-up" },
            }),
        ),
    ];

    for (typed, rule) in sentences {
        let rule: Rule = serde_json::from_value(rule).unwrap();
        let sentence = quick_inject.read(typed, &known);

        assert_eq!(sentence.rule(), Some(&rule), "{typed}");
        assert_eq!(sentence.unread(), [], "{typed}");
    }
}

#[test]
fn a_sentence_names_a_preset_and_a_selector_of_the_workspace() {
    let quick_inject = QuickInject::default();
    let known = Shop::known();

    let sentences = [
        (
            "leader-flap on order-producers for order-ingest",
            json!({
                "name": "leader-flap-on-order-producers-for-order-ingest",
                "title": "leader-flap on order-producers for order-ingest",
                "selector": "order-producers",
                "scope": { "client.id": "order-ingest" },
                "action": "leader-flap",
            }),
        ),
        (
            "leader-election produce 1%",
            json!({
                "name": "leader-election-on-produce",
                "title": "leader-election on Produce",
                "selector": { "api": "Produce" },
                "action": "leader-election",
                "trigger": { "chance": "1%" },
            }),
        ),
        // The word for the kind takes a name that nobody knows yet, and a name that is a word of the grammar.
        (
            "preset blackhole selector heartbeats",
            json!({
                "name": "blackhole-on-heartbeats",
                "title": "blackhole on heartbeats",
                "selector": "heartbeats",
                "action": "blackhole",
            }),
        ),
    ];

    for (typed, rule) in sentences {
        let rule: Rule = serde_json::from_value(rule).unwrap();
        let sentence = quick_inject.read(typed, &known);

        assert_eq!(sentence.rule(), Some(&rule), "{typed}");
        assert_eq!(sentence.unread(), [], "{typed}");
    }
}

#[test]
fn a_word_of_the_grammar_is_a_name_after_the_word_for_its_kind() {
    let quick_inject = QuickInject::default();
    let known = Shop::known();

    let sentences = [(
        "blackhole topic fetch client delay",
        json!({
            "name": "blackhole-on-fetch-for-delay",
            "title": "Blackhole on fetch for delay",
            "selector": { "topic": "fetch" },
            "scope": { "client.id": "delay" },
            "action": { "lie": "silence" },
        }),
    )];

    for (typed, rule) in sentences {
        let rule: Rule = serde_json::from_value(rule).unwrap();
        let sentence = quick_inject.read(typed, &known);

        assert_eq!(sentence.rule(), Some(&rule), "{typed}");
        assert_eq!(sentence.unread(), [], "{typed}");
    }
}

#[test]
fn the_name_of_the_rule_is_its_title_as_a_slug() {
    let quick_inject = QuickInject::default();
    let known = Shop::known();

    let sentences = [
        (
            "delay produce orders.* 750ms order-ingest",
            "Delay 750ms on Produce orders.* for order-ingest",
            "delay-750ms-on-produce-orders-for-order-ingest",
        ),
        (
            "error produce fetch NOT_LEADER_OR_FOLLOWER",
            "Refuse with NOT_LEADER_OR_FOLLOWER on Produce + Fetch",
            "refuse-with-not-leader-or-follower-on-produce-fetch",
        ),
        // A name has at most 63 characters, so a longer one is cut, and it does not end in a hyphen.
        (
            "error heartbeat REBALANCE_IN_PROGRESS analytics-etl",
            "Refuse with REBALANCE_IN_PROGRESS on Heartbeat group analytics-etl",
            "refuse-with-rebalance-in-progress-on-heartbeat-group-analytics",
        ),
        (
            "error consumergroupheartbeat FETCH_SESSION_TOPIC_ID_ERROR order-ingest",
            "Refuse with FETCH_SESSION_TOPIC_ID_ERROR on ConsumerGroupHeartbeat for order-ingest",
            "refuse-with-fetch-session-topic-id-error-on-consumergroupheartb",
        ),
        // A name holds no letter outside `a` to `z`.
        (
            "delay client é",
            "Delay 500ms on all calls for é",
            "delay-500ms-on-all-calls-for",
        ),
    ];

    for (typed, title, name) in sentences {
        let sentence = quick_inject.read(typed, &known);
        let rule = sentence.rule().unwrap();

        assert_eq!(rule.name(), name, "{typed}");
        assert_eq!(rule.title().map(RuleTitle::as_str), Some(title), "{typed}");
    }
}

#[test]
fn a_rule_has_no_title_when_the_sentence_holds_what_no_title_does() {
    let quick_inject = QuickInject::default();
    let zero_width_space = '\u{200b}';
    let typed = format!("delay client batch{zero_width_space}1");

    let sentence = quick_inject.read(&typed, &Shop::known());
    let rule = sentence.rule().unwrap();

    assert_eq!(rule.title(), None);
    assert_eq!(rule.name(), "delay-500ms-on-all-calls-for-batch-1");
    assert_eq!(sentence.unread(), []);
}

#[test]
fn a_sentence_that_does_not_say_what_happens_is_no_rule_yet() {
    let quick_inject = QuickInject::default();
    let known = Shop::known();

    for typed in ["", "   ", "produce", "produce orders.* 40% order-ingest for 20s"] {
        let sentence = quick_inject.read(typed, &known);

        assert_eq!(sentence.rule(), None, "{typed}");
        assert_eq!(sentence.unread(), [], "{typed}");
        assert_eq!(sentence.text(), typed);
    }
}
