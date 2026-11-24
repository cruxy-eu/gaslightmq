//! Build step 1: rules evaluate against hand-written calls.
//!
//! A workspace of selectors, presets and rules from the specs is armed with a fixed seed.
//! Two sessions send hand-written Produce and Fetch calls on a virtual clock.
//! Every call says which rules matched, on which target, what each trigger rolled, and what fired.
//!
//! The test stands where the pipeline will stand.
//! It resolves no conflict between lies, so it confirms every firing, as a call does when nothing shadows a rule.

#![expect(
    clippy::unwrap_used,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

use gaslightmq_clock::{Clock, Duration, Instant, TimeSpan, VirtualClock};
use gaslightmq_field::prelude::*;
use gaslightmq_incident::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_lie::prelude::*;
use gaslightmq_rule::prelude::*;
use gaslightmq_selector::prelude::*;
use gaslightmq_trigger::prelude::*;

/// The seed of the scenario, which every die derives from.
const SEED: Seed = Seed::new(42);

fn selectors() -> SelectorSet {
    let order_producers = Selector::new(
        "Order producers".parse().unwrap(),
        Conditions::new()
            .with(Condition::is(ApiKey::PRODUCE))
            .with(Condition::parse(Field::Topic, "orders.*").unwrap()),
    );
    SelectorSet::new([order_producers]).unwrap()
}

fn presets() -> PresetSet {
    let delay_1s = TimingLie::new()
        .with_delay(TimeSpan::from_secs(1))
        .with_jitter(TimeSpan::from_millis(200));
    let leader_flap = RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER);
    let leader_election = IncidentDefinition::LeaderMoves {
        partition: None,
        to: None,
    };
    PresetSet::new([
        Preset::new("Delay 1 s".parse().unwrap(), delay_1s),
        Preset::new("Leader flap".parse().unwrap(), leader_flap),
        Preset::new("Leader election".parse().unwrap(), leader_election),
    ])
    .unwrap()
}

/// The rules in the order in which they win a conflict, as a Chain scenario lists them.
fn rules() -> Vec<Rule> {
    let order_producers = || "Order producers".parse::<SelectorName>();
    let preset = |name: &str| name.parse::<PresetName>();

    // The rule of rules.md: `chance: 40%`, for one client.
    let slow_down = Rule::new(
        "Slow down order producers".parse().unwrap(),
        order_producers().unwrap(),
        preset("Delay 1 s").unwrap(),
    )
    .scoped(Scope::client(ClientId::new("order-ingest").unwrap()))
    .with_trigger(TriggerParts::new().with_chance("40%".parse().unwrap()));

    // A refusal on `orders.*`, on the 3rd matching call of each connection.
    let refuse_orders = Rule::new(
        "Refuse the 3rd call on orders".parse().unwrap(),
        Conditions::from(Condition::parse(Field::Topic, "orders.*").unwrap()),
        preset("Leader flap").unwrap(),
    )
    .with_trigger(
        TriggerParts::new()
            .with_nth(MatchNumber::new(3).unwrap())
            .with_per(Per::Session),
    );

    // The first rule of the "Leader election" scenario, with the preset that takes its partition from the call.
    let leader_moves_away = Rule::new(
        "Leader moves away".parse().unwrap(),
        order_producers().unwrap(),
        preset("Leader election").unwrap(),
    )
    .with_trigger(TriggerParts::new().with_nth(MatchNumber::new(1).unwrap()));

    // A rule that waits for the truth: it only delays a Fetch that succeeded, twice.
    let slow_fetches = Rule::new(
        "Slow down two successful fetches".parse().unwrap(),
        Conditions::new()
            .with(Condition::is(ApiKey::FETCH))
            .with(Condition::is(Outcome::Ok)),
        preset("Delay 1 s").unwrap(),
    )
    .with_lifetime("2 firings".parse().unwrap());

    vec![slow_down, refuse_orders, leader_moves_away, slow_fetches]
}

/// One connection: its fields, and the counters its pipeline keeps.
struct Session {
    connection: Fields,
    counters: SessionCounters,
}

impl Session {
    fn open(client: &str, number: u64) -> Session {
        let endpoint = Endpoint::Broker(BrokerId::from(2));
        Session {
            connection: Fields::new()
                .with(ConnectionNumber::new(number))
                .with(endpoint)
                .with(ClientId::new(client).unwrap()),
            // The text of the session key: the host, the endpoint and the ordinal of the connection.
            counters: SessionCounters::new(CounterKey::new(format!("10.0.4.27 {endpoint} #{number}")).unwrap()),
        }
    }

    fn call(&self, api: ApiKey, partitions: &[&str]) -> Fields {
        let mut call = self.connection.clone().with(api);
        for partition in partitions {
            call = call.with_part(partition.parse::<TopicPartition>().unwrap());
        }
        call
    }
}

/// One run of the scenario: the snapshot of its armed rules, and the clock of the engine.
struct Run {
    snapshot: Snapshot,
    clock: VirtualClock,
}

impl Run {
    /// Someone activates the scenario, which arms its rules, and the engine publishes the snapshot.
    fn start() -> Run {
        let (selectors, presets) = (selectors(), presets());
        let clock = VirtualClock::new();
        let mut armed = Vec::new();
        for rule in rules() {
            armed.push(rule.resolve(&selectors, &presets).unwrap().arm(SEED, clock.now()));
        }
        Run {
            snapshot: Snapshot::new(armed).unwrap(),
            clock,
        }
    }

    /// Time passes between two calls.
    fn later(&mut self) -> Instant {
        self.clock.advance(Duration::from_millis(10))
    }

    /// Runs one pass of one call, and lists it as the explanation of the call would.
    ///
    /// No lie shadows another one here, so every firing is confirmed.
    fn pass(&self, pass: Pass, call: &Fields, session: &mut Session) -> Vec<String> {
        let now = self.clock.now();
        let mut evaluation = self.snapshot.evaluate(pass, call, now, &mut session.counters);
        let lines = evaluation.iter().map(Run::line).collect();
        for evaluated in &mut evaluation {
            if evaluated.confirm().is_some_and(Confirmed::ends_lifetime) {
                assert!(evaluated.rule().arming().has_ended(now));
            }
        }
        lines
    }

    /// One rule of one call: Matched, Rolled and Fired.
    fn line(evaluated: &RuleEvaluation) -> String {
        let rolled = evaluated.rolled();
        let mut cells = vec![evaluated.name().to_string(), format!("matched {}", evaluated.target())];
        cells.extend(rolled.match_number().map(|number| format!("match {number}")));
        cells.extend(
            rolled
                .roll()
                .zip(rolled.chance())
                .map(|(roll, chance)| format!("rolled {roll} for {chance}")),
        );
        cells.extend(
            rolled
                .budget()
                .map(|budget| format!("{} of {} left", budget.left(), budget.limit())),
        );
        cells.push(format!("{:?}", rolled.outcome()));
        cells.extend(evaluated.lie().map(ToString::to_string));
        cells.extend(evaluated.incidents().iter().map(|incident| incident.kind().to_string()));
        cells.join(" | ")
    }

    /// The names of the rules that wait for the truth of a call.
    fn waiting(&self, call: &Fields) -> Vec<String> {
        let waiting = self.snapshot.waiting(call, self.clock.now());
        waiting.into_iter().map(|rule| rule.name().to_string()).collect()
    }

    fn rule(&self, name: &str) -> &ArmedRule {
        self.snapshot.get(&name.parse().unwrap()).expect("a rule that is armed")
    }

    /// How many calls a rule matched, across all connections.
    fn matches(&self, name: &str) -> u64 {
        self.rule(name).arming().counter().matches()
    }
}

#[test]
fn the_rules_of_the_request_pass_evaluate_against_hand_written_produce_calls() {
    let mut run = Run::start();
    let mut ingest = Session::open("order-ingest", 1);
    let produce = ingest.call(ApiKey::PRODUCE, &["orders.eu/3", "payments.settled/0"]);

    // 1st Produce of the ingest connection. The delay rolls over 40%, and the leader of the orders partition moves.
    let now = run.later();
    let first = run
        .snapshot
        .evaluate(Pass::Request, &produce, now, &mut ingest.counters);
    assert_eq!(first.lies().count(), 0);
    assert_eq!(
        first.incidents().cloned().collect::<Vec<_>>(),
        [Incident::LeaderMoves {
            partition: "orders.eu/3".parse().unwrap(),
            to: None
        }]
    );
    assert_eq!(
        first.iter().map(Run::line).collect::<Vec<_>>(),
        [
            "Slow down order producers | matched orders.eu/3 | match 1 | rolled 44.461% for 40% | Held",
            "Refuse the 3rd call on orders | matched orders.eu/3 | match 1 | Held",
            "Leader moves away | matched orders.eu/3 | match 1 | Fired | leader-moves",
        ]
    );
    // Nothing shadows an incident, so its firing counts once the incident is on its way.
    for mut evaluated in first {
        evaluated.confirm();
    }
    // No rule waits for the truth of a Produce, so the response pass has nothing to decide.
    assert_eq!(run.waiting(&produce), [""; 0]);
    let answered = produce.clone().with(Outcome::Ok);
    assert_eq!(run.pass(Pass::Response, &answered, &mut ingest), [""; 0]);

    // 2nd Produce. The delay rolls under 40% this time, and the leader moved already.
    run.later();
    assert_eq!(
        run.pass(Pass::Request, &produce, &mut ingest),
        [
            "Slow down order producers | matched orders.eu/3 | match 2 | rolled 11.6519% for 40% | Fired | +1 s ±200 ms",
            "Refuse the 3rd call on orders | matched orders.eu/3 | match 2 | Held",
            "Leader moves away | matched orders.eu/3 | match 2 | Held",
        ]
    );

    // A Produce of another client. The scope keeps the delay away from it, and its connection counts from one.
    let mut payments = Session::open("payments-svc", 3);
    let other = payments.call(ApiKey::PRODUCE, &["orders.us/0"]);
    assert_eq!(
        run.pass(Pass::Request, &other, &mut payments),
        [
            "Refuse the 3rd call on orders | matched orders.us/0 | match 1 | Held",
            "Leader moves away | matched orders.us/0 | match 3 | Held",
        ]
    );

    // 3rd Produce of the ingest connection. The refusal fires on the orders partition, and the rest goes through.
    run.later();
    assert_eq!(
        run.pass(Pass::Request, &produce, &mut ingest),
        [
            "Slow down order producers | matched orders.eu/3 | match 3 | rolled 73.8645% for 40% | Held",
            "Refuse the 3rd call on orders | matched orders.eu/3 | match 3 | Fired | NOT_LEADER_OR_FOLLOWER on orders.eu/3",
            "Leader moves away | matched orders.eu/3 | match 4 | Held",
        ]
    );

    // The shared counters say how many calls each rule matched, across all connections.
    assert_eq!(run.matches("Slow down order producers"), 3);
    assert_eq!(run.matches("Leader moves away"), 4);
    assert_eq!(run.matches("Slow down two successful fetches"), 0);
    // The refusal counts `per: session`, so each connection keeps its own count of it.
    let refusal = run.rule("Refuse the 3rd call on orders").arming().counter();
    assert_eq!(ingest.counters.matches(refusal), 3);
    assert_eq!(payments.counters.matches(refusal), 1);
    assert_eq!(refusal.matches(), 0);
}

#[test]
fn a_rule_that_waits_for_the_truth_evaluates_against_hand_written_fetch_calls() {
    let mut run = Run::start();
    let mut analytics = Session::open("analytics", 2);
    let fetch = analytics.call(ApiKey::FETCH, &["orders.eu/3", "orders.us/0"]);
    let answered = fetch.clone().with(Outcome::Ok).with(Latency::from_millis(14));
    assert_eq!(
        run.rule("Slow down two successful fetches").rule().pass(),
        Pass::Response
    );

    // 1st Fetch. Before forwarding, only the refusal matches, and it holds. The delay waits for the truth.
    run.later();
    assert_eq!(
        run.pass(Pass::Request, &fetch, &mut analytics),
        ["Refuse the 3rd call on orders | matched orders.eu/3, orders.us/0 | match 1 | Held"]
    );
    assert_eq!(run.waiting(&fetch), ["Slow down two successful fetches"]);
    // The backend answers, and the delay takes the first of its two firings.
    run.later();
    assert_eq!(
        run.pass(Pass::Response, &answered, &mut analytics),
        [
            "Slow down two successful fetches | matched orders.eu/3, orders.us/0 | match 1 | 1 of 2 left | Fired \
             | +1 s ±200 ms"
        ]
    );

    // 2nd Fetch. It succeeds again, and the delay takes its last firing, which ends its lifetime.
    run.later();
    assert_eq!(
        run.pass(Pass::Request, &fetch, &mut analytics),
        ["Refuse the 3rd call on orders | matched orders.eu/3, orders.us/0 | match 2 | Held"]
    );
    assert_eq!(
        run.pass(Pass::Response, &answered, &mut analytics),
        [
            "Slow down two successful fetches | matched orders.eu/3, orders.us/0 | match 2 | 0 of 2 left | Fired \
             | +1 s ±200 ms"
        ]
    );
    assert!(
        run.rule("Slow down two successful fetches")
            .arming()
            .has_ended(run.clock.now())
    );

    // 3rd Fetch. The refusal fires on both partitions, so nothing is left to forward and no truth arrives.
    run.later();
    assert_eq!(
        run.pass(Pass::Request, &fetch, &mut analytics),
        [
            "Refuse the 3rd call on orders | matched orders.eu/3, orders.us/0 | match 3 | Fired \
             | NOT_LEADER_OR_FOLLOWER on orders.eu/3, orders.us/0"
        ]
    );
    let refused = fetch.clone().without_truth();
    assert_eq!(run.waiting(&refused), [""; 0]);
    assert_eq!(run.pass(Pass::Response, &refused, &mut analytics), [""; 0]);

    // 4th Fetch. It succeeds, and the lifetime of the delay is over: the call is not counted.
    run.later();
    assert_eq!(
        run.pass(Pass::Response, &answered, &mut analytics),
        ["Slow down two successful fetches | matched orders.eu/3, orders.us/0 | 0 of 2 left | LifetimeOver"]
    );
    assert_eq!(run.matches("Slow down two successful fetches"), 2);
    // A rule that cannot fire anymore does not wait for the truth of a Fetch either.
    assert_eq!(run.waiting(&fetch), [""; 0]);
}

#[test]
fn the_same_seed_gives_the_same_firings_in_every_run() {
    let (selectors, presets) = (selectors(), presets());
    let slow_down = rules().swap_remove(0);

    // Which of eight Produce calls of one connection the rule fires on.
    let firings = |rule: &Rule, seed: Seed| -> Vec<bool> {
        let snapshot = Snapshot::new([rule.resolve(&selectors, &presets).unwrap().arm(seed, Instant::ZERO)]).unwrap();
        let mut ingest = Session::open("order-ingest", 1);
        let produce = ingest.call(ApiKey::PRODUCE, &["orders.eu/3"]);
        let mut fired = Vec::new();
        for _ in 0..8 {
            let pass = snapshot.evaluate(Pass::Request, &produce, Instant::ZERO, &mut ingest.counters);
            fired.extend(pass.iter().map(RuleEvaluation::fired));
        }
        fired
    };

    let first = firings(&slow_down, SEED);
    // The die rolls 44.461%, 11.6519%, 73.8645%, 48.5064%, 62.5689%, 34.6021%, 79.717% and 12.0951%, for `chance: 40%`.
    assert_eq!(first, [false, true, false, false, false, true, false, true]);
    assert_eq!(first, firings(&slow_down, SEED));
    assert_ne!(first, firings(&slow_down, Seed::new(7)));

    // A rule of another name rolls a die of its own.
    let other = Rule::new(
        "Slow ingest".parse().unwrap(),
        slow_down.selector().clone(),
        slow_down.action().clone(),
    )
    .with_trigger(slow_down.trigger());
    assert_ne!(first, firings(&other, SEED));
    // A new rule that takes the place of another sets `dice` to the name of the other, so it hits the same calls.
    assert_eq!(first, firings(&other.with_dice(slow_down.dice_key()), SEED));
}
