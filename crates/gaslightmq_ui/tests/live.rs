//! What the UI knows from the live stream: it applies every message to the snapshot, shows one environment,
//! and follows the calls of that environment that its filter lets through.
//!
//! The tests feed a [`Live`] the messages that the control plane sends, as plain values, and look at what it holds and
//! what it asks next. No browser and no server take part.

#![expect(
    clippy::unwrap_used,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use gaslightmq_control_api::call::{Call, CallId, CallTiming, Delivered, Explanation, RuleExplanation};
use gaslightmq_control_api::clock::Instant;
use gaslightmq_control_api::environment::{
    BrokerListeners, Environment, EnvironmentName, Instances, Listeners, ProxyDefinition, Upstream,
};
use gaslightmq_control_api::field::{ClientSoftware, ConnectionNumber, Endpoint, FieldValue, Fields};
use gaslightmq_control_api::incident::IncidentSupport;
use gaslightmq_control_api::kernel::{ApiKey, BrokerId, ClientId};
use gaslightmq_control_api::prelude::*;
use gaslightmq_control_api::rule::{PresetName, Rule, RuleState};
use gaslightmq_control_api::selector::{Condition, Conditions, SelectorName, SelectorRef};
use gaslightmq_ui::{Link, Live, UiConfiguration};

/// The environments of the tests, as the workspace and the control plane describe them.
/// The revision that the control plane told beside a rule or a definition. Nothing here reads it yet.
const TOLD: Revision = Revision::new(7);

/// The run of the process, as its snapshot writes it.
const RUN: &str = "4be0643f1d98573b97cdca98a65347dd";

struct Workspace;

impl Workspace {
    /// A proxy in front of a real cluster, with a port pinned for its one broker.
    fn environment(name: &str) -> Environment {
        let brokers = BrokerListeners::pinned([(BrokerId::from(1), "0.0.0.0:19092".parse().unwrap())]).unwrap();
        let listen = Listeners::new("0.0.0.0:9092".parse().unwrap(), brokers, "localhost".parse().unwrap()).unwrap();
        Environment::new(
            name.parse().unwrap(),
            Instances::ProxyInFrontOfCluster {
                proxy: ProxyDefinition::new(listen),
                upstream: Upstream::new("kafka-1.staging:9092".parse().unwrap()),
            },
        )
    }

    /// The environment as it runs: its backend, no rule and no session.
    fn running(name: &str) -> RunningEnvironment {
        RunningEnvironment {
            environment: name.parse().unwrap(),
            backend: BackendStatus {
                kind: BackendKind::Upstream,
                endpoints: BTreeMap::from([(Endpoint::Bootstrap, Some("127.0.0.1:9092".parse().unwrap()))]),
                incidents: IncidentSupport::none(),
            },
            rules: Vec::new(),
            sessions: Vec::new(),
        }
    }

    /// The snapshot of a process whose workspace has both environments, of which `running` run.
    fn snapshot(running: &[&str]) -> StreamMessage {
        let told = |definition| DefinitionStatus {
            definition,
            revision: TOLD,
        };
        StreamMessage::Snapshot(ProcessState {
            run: RUN.parse().unwrap(),
            definitions: Definitions {
                environments: vec![
                    told(Self::environment("staging-proxy")),
                    told(Self::environment("orders-sim")),
                ],
                ..Definitions::default()
            },
            environments: running.iter().map(|name| Self::running(name)).collect(),
        })
    }

    /// The rule `slow-order-producers`: the selector `order-producers`, and the preset `delay-1s`.
    fn rule() -> Rule {
        Rule::new(
            "slow-order-producers".parse().unwrap(),
            "order-producers".parse::<SelectorName>().unwrap(),
            "delay-1s".parse::<PresetName>().unwrap(),
        )
    }

    /// A Produce of `order-ingest` on connection `#7` that no rule touched.
    fn call(id: u64) -> Call {
        let fields = Fields::new()
            .with(ConnectionNumber::new(7))
            .with(ClientId::new("order-ingest").unwrap())
            .with(ApiKey::PRODUCE)
            .without_truth();
        Call::new(
            CallId::new(id).unwrap(),
            fields,
            Delivered::Nothing,
            Explanation::new(Vec::<RuleExplanation>::new()).unwrap(),
            CallTiming::arrived(Instant::ZERO),
        )
        .unwrap()
    }
}

/// Every call of the environment: a selector without conditions.
fn every_call(environment: &str) -> Subscription {
    Subscription::Calls {
        environment: environment.parse().unwrap(),
        selector: SelectorRef::Inline(Conditions::new()),
    }
}

mod following {
    use super::*;

    #[test]
    fn after_the_snapshot_the_ui_follows_every_call_of_the_first_environment_that_runs() {
        let mut live = Live::new(&UiConfiguration::default());
        live.opened();
        assert_eq!(live.next_subscription(), None, "nothing is known before the snapshot");

        live.apply(Workspace::snapshot(&["staging-proxy"]));

        assert_eq!(live.shown().map(EnvironmentName::as_str), Some("staging-proxy"));
        assert_eq!(live.next_subscription(), Some(every_call("staging-proxy")));
    }

    #[test]
    fn a_subscription_is_asked_once_and_its_calls_count_from_its_answer_on() {
        let mut live = Live::new(&UiConfiguration::default());
        live.opened();
        live.apply(Workspace::snapshot(&["staging-proxy"]));
        live.asked(every_call("staging-proxy"));
        assert_eq!(live.next_subscription(), None, "the subscription waits for its answer");

        // A call before the answer belongs to what the stream followed before.
        live.apply(StreamMessage::Call {
            environment: "staging-proxy".parse().unwrap(),
            call: Workspace::call(1),
        });
        live.apply(StreamMessage::Following {
            subscription: every_call("staging-proxy"),
        });
        live.apply(StreamMessage::Call {
            environment: "staging-proxy".parse().unwrap(),
            call: Workspace::call(2),
        });

        let shown: Vec<u64> = live.traffic().calls().map(|call| call.id().value()).collect();
        assert_eq!(shown, [2]);
        assert_eq!(live.next_subscription(), None);
    }

    #[test]
    fn another_environment_shows_its_own_calls_from_its_answer_on() {
        let mut live = Live::new(&UiConfiguration::default());
        live.opened();
        live.apply(Workspace::snapshot(&["staging-proxy", "orders-sim"]));
        live.asked(every_call("staging-proxy"));
        live.apply(StreamMessage::Following {
            subscription: every_call("staging-proxy"),
        });
        live.apply(StreamMessage::Call {
            environment: "staging-proxy".parse().unwrap(),
            call: Workspace::call(1),
        });

        live.show("orders-sim".parse().unwrap());

        assert_eq!(live.traffic().calls().len(), 0, "the calls of the other environment go");
        assert_eq!(live.next_subscription(), Some(every_call("orders-sim")));
    }

    #[test]
    fn a_value_that_someone_clicks_follows_only_the_calls_with_it() {
        let mut live = Live::new(&UiConfiguration::default());
        live.opened();
        live.apply(Workspace::snapshot(&["staging-proxy"]));
        let ingest = FieldValue::from(ClientId::new("order-ingest").unwrap());

        live.toggle(ingest.clone());

        assert_eq!(
            live.next_subscription(),
            Some(Subscription::Calls {
                environment: "staging-proxy".parse().unwrap(),
                selector: SelectorRef::Inline(Conditions::from(Condition::is(ClientId::new("order-ingest").unwrap()))),
            })
        );
        live.toggle(ingest);
        assert_eq!(live.next_subscription(), Some(every_call("staging-proxy")));
    }

    #[test]
    fn an_environment_that_stops_and_starts_again_is_followed_again() {
        let mut live = Live::new(&UiConfiguration::default());
        live.opened();
        live.apply(Workspace::snapshot(&["staging-proxy"]));
        live.asked(every_call("staging-proxy"));
        live.apply(StreamMessage::Following {
            subscription: every_call("staging-proxy"),
        });

        live.apply(StreamMessage::EnvironmentStopped {
            environment: "staging-proxy".parse().unwrap(),
        });
        assert_eq!(
            live.next_subscription(),
            None,
            "an environment that does not run has no calls"
        );
        assert_eq!(live.shown().map(EnvironmentName::as_str), Some("staging-proxy"));

        live.apply(StreamMessage::EnvironmentStarted {
            environment: Workspace::running("staging-proxy"),
        });
        assert_eq!(live.next_subscription(), Some(every_call("staging-proxy")));
    }

    #[test]
    fn a_refused_subscription_is_not_asked_again_until_its_environment_starts() {
        let mut live = Live::new(&UiConfiguration::default());
        live.opened();
        live.apply(Workspace::snapshot(&["staging-proxy"]));
        live.asked(every_call("staging-proxy"));

        live.apply(StreamMessage::SubscriptionRefused {
            reason: "environment \"staging-proxy\" does not run".to_owned(),
        });

        assert_eq!(
            live.following().refusal(),
            Some("environment \"staging-proxy\" does not run")
        );
        assert_eq!(live.next_subscription(), None);
        live.apply(StreamMessage::EnvironmentStarted {
            environment: Workspace::running("staging-proxy"),
        });
        assert_eq!(live.next_subscription(), Some(every_call("staging-proxy")));
    }

    #[test]
    fn a_new_stream_follows_nothing_until_it_asks_again() {
        let mut live = Live::new(&UiConfiguration::default());
        live.opened();
        live.apply(Workspace::snapshot(&["staging-proxy"]));
        live.asked(every_call("staging-proxy"));
        live.apply(StreamMessage::Following {
            subscription: every_call("staging-proxy"),
        });

        live.lost("the control plane stopped".to_owned());
        assert_eq!(
            live.link(),
            &Link::Lost {
                reason: "the control plane stopped".to_owned()
            }
        );
        assert_eq!(live.next_subscription(), None, "a stream that ended asks nothing");
        live.opened();
        live.apply(Workspace::snapshot(&["staging-proxy"]));

        assert_eq!(live.next_subscription(), Some(every_call("staging-proxy")));
    }
}

mod process {
    use super::*;

    #[test]
    fn a_session_is_known_by_its_connection_from_its_opening_to_its_closing() {
        let mut live = Live::new(&UiConfiguration::default());
        live.opened();
        live.apply(Workspace::snapshot(&["staging-proxy"]));
        let staging: EnvironmentName = "staging-proxy".parse().unwrap();
        let connection = ConnectionNumber::new(7);

        live.apply(StreamMessage::SessionOpened {
            environment: staging.clone(),
            session: SessionStatus {
                connection,
                address: "10.0.4.27:51459".parse().unwrap(),
                endpoint: Endpoint::Bootstrap,
                opened: Instant::ZERO,
                identity: None,
            },
        });
        live.apply(StreamMessage::SessionIdentified {
            environment: staging.clone(),
            connection,
            identity: SessionIdentity {
                client: Some(ClientId::new("order-ingest").unwrap()),
                software: None,
            },
        });
        live.apply(StreamMessage::SessionSoftwareNamed {
            environment: staging.clone(),
            connection,
            software: ClientSoftware::new("librdkafka", "2.6.1").unwrap(),
        });

        let sessions = &live.shown_running().unwrap().sessions;
        assert_eq!(sessions.len(), 1);
        assert_eq!(
            sessions[0].identity,
            Some(SessionIdentity {
                client: Some(ClientId::new("order-ingest").unwrap()),
                software: Some(ClientSoftware::new("librdkafka", "2.6.1").unwrap()),
            })
        );
        live.apply(StreamMessage::SessionClosed {
            environment: staging,
            connection,
            by: SessionCloser::Client,
        });
        assert_eq!(live.shown_running().unwrap().sessions, []);
    }

    #[test]
    fn a_rule_that_someone_creates_in_the_workspace_reaches_every_environment_disarmed() {
        let mut live = Live::new(&UiConfiguration::default());
        live.opened();
        live.apply(Workspace::snapshot(&["staging-proxy", "orders-sim"]));

        live.apply(StreamMessage::DefinitionChanged {
            definition: AnyDefinition::Rule(Workspace::rule()),
            revision: TOLD,
        });

        let process = live.process().unwrap();
        assert_eq!(process.definitions().rules, [Workspace::rule()]);
        // The revision is no part of the rule: the process keeps it beside it.
        assert_eq!(process.revision(&Workspace::rule().name().clone().into()), Some(TOLD));
        for running in process.environments() {
            assert_eq!(
                running.rules,
                [RuleStatus {
                    rule: Workspace::rule(),
                    live: false,
                    state: RuleState::Disarmed,
                    revision: TOLD,
                }],
                "{}",
                running.environment
            );
        }
    }

    #[test]
    fn a_kept_live_rule_becomes_a_rule_of_the_workspace_and_stays_armed_where_it_was() {
        let mut live = Live::new(&UiConfiguration::default());
        live.opened();
        live.apply(Workspace::snapshot(&["staging-proxy", "orders-sim"]));
        let staging: EnvironmentName = "staging-proxy".parse().unwrap();
        let armed = |live: bool| RuleStatus {
            rule: Workspace::rule(),
            live,
            state: RuleState::Armed,
            revision: TOLD,
        };
        live.apply(StreamMessage::RuleChanged {
            environment: staging.clone(),
            rule: armed(true),
        });

        // Keeping sends the three messages in this order.
        live.apply(StreamMessage::RuleRemoved {
            environment: staging.clone(),
            rule: Workspace::rule().name().clone(),
        });
        live.apply(StreamMessage::DefinitionChanged {
            definition: AnyDefinition::Rule(Workspace::rule()),
            revision: TOLD,
        });
        live.apply(StreamMessage::RuleChanged {
            environment: staging.clone(),
            rule: armed(false),
        });

        let process = live.process().unwrap();
        assert_eq!(process.environment(&staging).unwrap().rules, [armed(false)]);
        let elsewhere = process.environment(&"orders-sim".parse().unwrap()).unwrap();
        assert_eq!(elsewhere.rules[0].state, RuleState::Disarmed);
    }

    #[test]
    fn a_change_that_comes_twice_changes_nothing_the_second_time() {
        let mut live = Live::new(&UiConfiguration::default());
        live.opened();
        live.apply(Workspace::snapshot(&[]));
        let started = StreamMessage::EnvironmentStarted {
            environment: Workspace::running("orders-sim"),
        };

        live.apply(started.clone());
        let once = live.clone();
        live.apply(started);

        assert_eq!(live, once);
        assert_eq!(live.process().unwrap().environments().len(), 1);
    }
}

mod traffic {
    use super::*;

    /// A UI that follows every call of `staging-proxy`, and keeps `window` of them.
    fn following(window: usize) -> Live {
        let mut configuration = UiConfiguration::default();
        configuration.calls_kept = NonZeroUsize::new(window).unwrap();
        let mut live = Live::new(&configuration);
        live.opened();
        live.apply(Workspace::snapshot(&["staging-proxy"]));
        live.asked(every_call("staging-proxy"));
        live.apply(StreamMessage::Following {
            subscription: every_call("staging-proxy"),
        });
        live
    }

    fn arrives(live: &mut Live, id: u64) {
        live.apply(StreamMessage::Call {
            environment: "staging-proxy".parse().unwrap(),
            call: Workspace::call(id),
        });
    }

    fn shown(live: &Live) -> Vec<u64> {
        live.traffic().calls().map(|call| call.id().value()).collect()
    }

    #[test]
    fn the_window_keeps_the_newest_calls_first() {
        let mut live = following(2);

        for id in 1..=3 {
            arrives(&mut live, id);
        }

        assert_eq!(shown(&live), [3, 2]);
        assert_eq!(live.traffic().received(), 3);
    }

    #[test]
    fn calls_wait_while_the_traffic_stands_still_and_join_it_when_it_goes_on() {
        let mut live = following(10);
        arrives(&mut live, 1);

        live.pause(true);
        arrives(&mut live, 2);
        arrives(&mut live, 3);
        assert_eq!(shown(&live), [1]);
        assert_eq!(live.traffic().waiting(), 2);

        live.pause(false);
        assert_eq!(shown(&live), [3, 2, 1]);
    }

    #[test]
    fn the_call_someone_looks_at_stays_when_the_window_lets_it_go() {
        let mut live = following(1);
        arrives(&mut live, 1);

        live.inspect(Some(CallId::FIRST));
        arrives(&mut live, 2);

        assert_eq!(shown(&live), [2]);
        assert_eq!(live.inspected().map(Call::id), Some(CallId::FIRST));
    }
}
