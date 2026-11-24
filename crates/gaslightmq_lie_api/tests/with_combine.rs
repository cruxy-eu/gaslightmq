//! The contract carries a kind with payload code, and the lies of a call that `gaslightmq_combine` combined.
//!
//! The tests hold two things that later crates will hold:
//!
//! - [`Refusal`], a small refusal for Produce and Heartbeat. `gaslightmq_lie_refusal` will have one for every API.
//! - [`walk`], the stages of one call. `gaslightmq_pipeline` will have them as a state machine.
//!
//! Neither names the other.
//! The walk combines the lies, asks the set for the lies that apply, and does what each effect says.

#![expect(
    clippy::unwrap_used,
    clippy::panic,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

use gaslightmq_clock::TimeSpan;
use gaslightmq_combine::prelude::*;
use gaslightmq_field::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_lie::prelude::*;
use gaslightmq_lie_api::prelude::*;
use gaslightmq_wire::messages::produce_request::{PartitionProduceData, TopicProduceData};
use gaslightmq_wire::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
use gaslightmq_wire::messages::{
    HeartbeatRequest, HeartbeatResponse, MetadataRequest, ProduceRequest, ProduceResponse, TopicName as WireTopicName,
};
use gaslightmq_wire::protocol::{Encodable, StrBytes};
use gaslightmq_wire::{ApiKey as WireApiKey, Request, Response};
use gaslightmq_wire_edit::prelude::*;

const BEFORE: Stage = Stage::BeforeForwarding;
const AFTER: Stage = Stage::AfterResponse;

/// The offset the backend of these tests gives every partition it writes.
const WRITTEN_AT: i64 = 100;

// The kind with payload code.

/// A refusal for Produce and Heartbeat.
///
/// Before forwarding, it takes its partitions out of the request, and answers itself when none is left.
/// After the response, it writes its error on its partitions, also on the ones the backend never saw.
struct Refusal;

impl Refusal {
    fn error(lie: &TargetedLie) -> Result<ErrorCode, LieError> {
        match lie.lie() {
            LieDefinition::Refusal(refusal) => Ok(refusal.error),
            other => Err(LieError::OtherKind {
                tells: LieKind::Refusal,
                lie: other.kind(),
            }),
        }
    }

    fn answer(request: &Request, body: &impl Encodable) -> Result<Effect, LieError> {
        request
            .response(body)
            .map(Effect::Answer)
            .map_err(|error| Self::not_rewritten(&error))
    }

    /// The reader or the writer of a message said `reason`.
    fn not_rewritten(reason: &impl std::fmt::Display) -> LieError {
        LieError::CallNotRewritten {
            kind: LieKind::Refusal,
            reason: reason.to_string(),
        }
    }

    /// `answered` with `error` on every partition of `asked` that `target` covers.
    fn refuse(
        asked: &ProduceRequest,
        mut answered: ProduceResponse,
        target: &Target,
        error: ErrorCode,
    ) -> ProduceResponse {
        for topic in &asked.topic_data {
            for partition in &topic.partition_data {
                if !target.covers(&part(&topic.name, partition.index)) {
                    continue;
                }
                if !answered.responses.iter().any(|response| response.name == topic.name) {
                    answered
                        .responses
                        .push(TopicProduceResponse::default().with_name(topic.name.clone()));
                }
                for response in answered
                    .responses
                    .iter_mut()
                    .filter(|response| response.name == topic.name)
                {
                    response
                        .partition_responses
                        .retain(|other| other.index != partition.index);
                    response.partition_responses.push(
                        PartitionProduceResponse::default()
                            .with_index(partition.index)
                            .with_error_code(error.value())
                            .with_base_offset(-1),
                    );
                }
            }
        }
        answered
    }
}

impl Lie for Refusal {
    fn kind(&self) -> LieKind {
        LieKind::Refusal
    }

    fn before_forwarding(&self, lie: &TargetedLie, request: &EditableRequest) -> Result<Effect, LieError> {
        let error = Self::error(lie)?;
        let request = request.request();
        match request.api().known() {
            Some(WireApiKey::Heartbeat) => {
                Self::answer(request, &HeartbeatResponse::default().with_error_code(error.value()))
            }
            Some(WireApiKey::Produce) => {
                let asked: ProduceRequest = request.body().map_err(|error| Self::not_rewritten(&error))?;
                let mut left = asked.clone();
                for topic in &mut left.topic_data {
                    let name = topic.name.clone();
                    topic
                        .partition_data
                        .retain(|partition| !lie.target().covers(&part(&name, partition.index)));
                }
                left.topic_data.retain(|topic| !topic.partition_data.is_empty());

                if left.topic_data.is_empty() {
                    let refused = Self::refuse(&asked, ProduceResponse::default(), lie.target(), error);
                    Self::answer(request, &refused)
                } else {
                    Request::new(
                        request.api().version,
                        request.correlation_id(),
                        request.client_id(),
                        &left,
                    )
                    .map(Effect::Forward)
                    .map_err(|error| Self::not_rewritten(&error))
                }
            }
            _ => Err(LieError::ApiNotSupported {
                kind: LieKind::Refusal,
                api: request.api(),
            }),
        }
    }

    fn after_response(
        &self,
        lie: &TargetedLie,
        request: &EditableRequest,
        response: &Response,
    ) -> Result<Effect, LieError> {
        let error = Self::error(lie)?;
        let request = request.request();
        match request.api().known() {
            Some(WireApiKey::Heartbeat) => {
                Self::answer(request, &HeartbeatResponse::default().with_error_code(error.value()))
            }
            Some(WireApiKey::Produce) => {
                let asked: ProduceRequest = request.body().map_err(|error| Self::not_rewritten(&error))?;
                let answered: ProduceResponse = response.body().map_err(|error| Self::not_rewritten(&error))?;
                Self::answer(request, &Self::refuse(&asked, answered, lie.target(), error))
            }
            _ => Err(LieError::ApiNotSupported {
                kind: LieKind::Refusal,
                api: request.api(),
            }),
        }
    }
}

/// The part that the codec names with a topic and a partition index.
fn part(topic: &WireTopicName, index: i32) -> Part {
    let topic = TopicName::new(topic.as_str()).unwrap();
    Part::from(TopicPartition::new(topic, PartitionNumber::try_from(index).unwrap()))
}

// The stages of one call.

/// A rule that fired on the call: its name, its lie and what its selector matched.
/// The place in the list is the priority, from the highest down.
struct Fired {
    rule: &'static str,
    lie: LieDefinition,
    matched: Target,
}

fn fired(rule: &'static str, lie: impl Into<LieDefinition>, matched: Target) -> Fired {
    Fired {
        rule,
        lie: lie.into(),
        matched,
    }
}

/// How a call ended.
#[derive(Debug)]
enum End {
    Sent(Response),
    Dropped,
    Closed,
}

/// What happened to one call.
#[derive(Debug)]
struct Walked {
    /// What the backend saw, if it saw anything.
    forwarded: Option<Request>,
    end: End,
    /// How long the answer waits, and the throttle it carries: both as combining gave them.
    wait: Delay,
    throttle: TimeSpan,
}

impl Walked {
    fn sent(&self) -> &Response {
        match &self.end {
            End::Sent(response) => response,
            other => panic!("the call ended with {other:?}"),
        }
    }
}

/// Takes one call through its stages, without a response pass.
///
/// It names no kind of lie.
/// Combining says which lies apply and where, and the set says what each of them asks for.
fn walk(lies: &Lies, fired: &[Fired], request: &Request, backend: impl Fn(&Request) -> Response) -> Walked {
    let targeted = |rule: &str, target: Target| -> TargetedLie {
        let fired = fired.iter().find(|fired| fired.rule == rule).unwrap();
        fired.lie.clone().on(target)
    };
    // The pipeline decodes a request once, and every lie reads that.
    let asked = decoded(request);
    let end = |effect: Effect| match effect {
        Effect::Drop => End::Dropped,
        Effect::Close => End::Closed,
        other => panic!("{other} does not end a call"),
    };

    let pass = RequestPass::of(fired.iter().enumerate().map(|(position, fired)| {
        let claim = Claim::try_from(&fired.lie.clone().on(fired.matched.clone())).unwrap();
        FiredLie::new(fired.rule, Priority::new(position), claim)
    }));

    // Before forwarding: a terminal lie ends the call, and every refusal changes what is forwarded.
    let before = pass.before_forwarding().clone();
    if let Some(rule) = before.terminal() {
        let effect = lies.before_forwarding(&targeted(rule, Target::Call), &asked).unwrap();
        return Walked {
            forwarded: None,
            end: end(effect),
            wait: Delay::ZERO,
            throttle: TimeSpan::ZERO,
        };
    }
    let mut forward = request.clone();
    let mut answered = None;
    let mut asked_to_forward = Vec::new();
    for (rule, target) in holders(before.refusals()) {
        let lie = targeted(rule, target);
        match lies.before_forwarding(&lie, &decoded(&forward)).unwrap() {
            Effect::Forward(other) => {
                forward = other;
                asked_to_forward.push(lie);
            }
            Effect::Answer(response) => answered = Some(response),
            Effect::Nothing => {}
            other => panic!("{lie} asked to {other}"),
        }
    }

    // The backend: it sees what is left, unless a lie answered in its place.
    let forwarded = answered.is_none().then(|| forward.clone());
    let mut response = answered.unwrap_or_else(|| backend(&forward));

    // After the response: first the lies that changed the request, then the lies of this stage.
    let call = pass.without_response_pass();
    let after = call.after_response();
    let of_this_stage = holders(after.refusals())
        .into_iter()
        .map(|(rule, target)| targeted(rule, target));
    for lie in asked_to_forward.into_iter().chain(of_this_stage) {
        match lies.after_response(&lie, &asked, &response).unwrap() {
            Effect::Answer(other) => response = other,
            Effect::Nothing => {}
            other => panic!("{lie} asked to {other}"),
        }
    }
    let end = match after.terminal() {
        Some(rule) => end(lies
            .after_response(&targeted(rule, Target::Call), &asked, &response)
            .unwrap()),
        None => End::Sent(response),
    };

    Walked {
        forwarded,
        end,
        wait: after.delay(),
        throttle: after.throttle(),
    }
}

/// Every lie that holds something, with the target it holds: the whole call, or its parts.
fn holders(winners: &Winners<&'static str>) -> Vec<(&'static str, Target)> {
    if let Some(rule) = winners.of_call() {
        return vec![(*rule, Target::Call)];
    }
    let mut rules: Vec<&'static str> = Vec::new();
    for (_, rule) in winners.parts() {
        if !rules.contains(rule) {
            rules.push(rule);
        }
    }
    rules
        .into_iter()
        .map(|rule| {
            let held = winners.parts().filter(|(_, holder)| **holder == rule);
            (rule, Target::of_parts(held.map(|(part, _)| part.clone())).unwrap())
        })
        .collect()
}

// Calls, and a backend that answers them.

/// A request as a lie gets it: decoded once, by whoever asks.
fn decoded(request: &Request) -> EditableRequest {
    EditableRequest::decode(request, &TopicNames::new()).unwrap()
}

fn every_lie() -> Lies {
    Lies::of_every_build().with(Refusal).unwrap()
}

fn parts(texts: &[&str]) -> Target {
    Target::of_parts(texts.iter().map(|text| text.parse().unwrap())).unwrap()
}

/// A Produce for those partitions, in that order.
fn produce(partitions: &[&str]) -> Request {
    let mut body = ProduceRequest::default().with_acks(-1).with_timeout_ms(30_000);
    for text in partitions {
        let partition: TopicPartition = text.parse().unwrap();
        let name = WireTopicName(StrBytes::from_string(partition.topic().as_str().to_owned()));
        let data = PartitionProduceData::default().with_index(i32::from(partition.partition()));
        if !body.topic_data.iter().any(|topic| topic.name == name) {
            body.topic_data
                .push(TopicProduceData::default().with_name(name.clone()));
        }
        for topic in body.topic_data.iter_mut().filter(|topic| topic.name == name) {
            topic.partition_data.push(data.clone());
        }
    }
    Request::new(9, 7, Some("order-ingest"), &body).unwrap()
}

fn heartbeat() -> Request {
    Request::new(4, 7, Some("order-ingest"), &HeartbeatRequest::default()).unwrap()
}

/// The truth: every partition the backend sees is written at [`WRITTEN_AT`], and every Heartbeat succeeds.
fn backend(request: &Request) -> Response {
    match request.api().known() {
        Some(WireApiKey::Produce) => {
            let asked: ProduceRequest = request.body().unwrap();
            let responses = asked
                .topic_data
                .iter()
                .map(|topic| {
                    let partitions = topic.partition_data.iter().map(|partition| {
                        PartitionProduceResponse::default()
                            .with_index(partition.index)
                            .with_base_offset(WRITTEN_AT)
                    });
                    TopicProduceResponse::default()
                        .with_name(topic.name.clone())
                        .with_partition_responses(partitions.collect())
                })
                .collect();
            request
                .response(&ProduceResponse::default().with_responses(responses))
                .unwrap()
        }
        Some(WireApiKey::Heartbeat) => request.response(&HeartbeatResponse::default()).unwrap(),
        other => panic!("the backend of these tests does not answer {other:?}"),
    }
}

/// A backend that no call may reach.
fn no_backend(request: &Request) -> Response {
    panic!("the backend saw {}", request.api())
}

/// The partitions of a Produce, in order.
fn produced(request: &Request) -> Vec<String> {
    let body: ProduceRequest = request.body().unwrap();
    let mut partitions: Vec<String> = body
        .topic_data
        .iter()
        .flat_map(|topic| {
            topic
                .partition_data
                .iter()
                .map(|partition| part(&topic.name, partition.index).to_string())
        })
        .collect();
    partitions.sort();
    partitions
}

/// What a Produce response tells about every partition, in order: `orders.eu/3 NOT_LEADER_OR_FOLLOWER` or `orders.eu/4 at 100`.
fn told(response: &Response) -> Vec<String> {
    let body: ProduceResponse = response.body().unwrap();
    let mut partitions: Vec<String> = body
        .responses
        .iter()
        .flat_map(|topic| {
            topic.partition_responses.iter().map(|partition| {
                let part = part(&topic.name, partition.index);
                match ErrorCode::from_wire(partition.error_code) {
                    Some(error) => format!("{part} {error}"),
                    None => format!("{part} at {}", partition.base_offset),
                }
            })
        })
        .collect();
    partitions.sort();
    partitions
}

fn leader_flap() -> RefusalLie {
    RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER)
}

#[test]
fn a_refusal_of_one_partition_forwards_the_rest_and_puts_its_error_into_the_response() {
    let request = produce(&["orders.eu/3", "orders.eu/4", "payments.settled/0"]);
    let rules = [fired("Leader flap", leader_flap(), parts(&["orders.eu/3"]))];

    let walked = walk(&every_lie(), &rules, &request, backend);

    // The records for the refused partition never reach the backend.
    assert_eq!(
        produced(walked.forwarded.as_ref().unwrap()),
        ["orders.eu/4", "payments.settled/0"]
    );
    // The client receives one response that covers every partition it asked for.
    assert_eq!(
        told(walked.sent()),
        [
            "orders.eu/3 NOT_LEADER_OR_FOLLOWER",
            "orders.eu/4 at 100",
            "payments.settled/0 at 100",
        ]
    );
    assert_eq!(walked.sent().correlation_id(), request.correlation_id());
}

#[test]
fn a_lie_is_asked_with_the_target_it_holds() {
    let request = produce(&["orders.eu/3", "orders.eu/4", "payments.settled/0"]);
    let rules = [
        fired("Leader flap", leader_flap(), parts(&["orders.eu/3"])),
        fired(
            "Refuse orders",
            RefusalLie::new(ErrorCode::REQUEST_TIMED_OUT),
            parts(&["orders.eu/3", "orders.eu/4"]),
        ),
    ];

    let walked = walk(&every_lie(), &rules, &request, backend);

    // "Refuse orders" lost `orders.eu/3`, so it only refuses `orders.eu/4`.
    assert_eq!(produced(walked.forwarded.as_ref().unwrap()), ["payments.settled/0"]);
    assert_eq!(
        told(walked.sent()),
        [
            "orders.eu/3 NOT_LEADER_OR_FOLLOWER",
            "orders.eu/4 REQUEST_TIMED_OUT",
            "payments.settled/0 at 100",
        ]
    );
}

#[test]
fn refusals_of_every_partition_answer_the_call_without_the_backend() {
    let request = produce(&["orders.eu/3", "orders.eu/4"]);
    let rules = [
        fired("Leader flap", leader_flap(), parts(&["orders.eu/3"])),
        fired(
            "Time out",
            RefusalLie::new(ErrorCode::REQUEST_TIMED_OUT),
            parts(&["orders.eu/4"]),
        ),
    ];

    let walked = walk(&every_lie(), &rules, &request, no_backend);

    // The first refusal asks to forward the rest, and the second finds nothing left and answers.
    // The first is then asked again, and puts its error next to the other.
    assert!(walked.forwarded.is_none());
    assert_eq!(
        told(walked.sent()),
        ["orders.eu/3 NOT_LEADER_OR_FOLLOWER", "orders.eu/4 REQUEST_TIMED_OUT"]
    );
}

#[test]
fn a_refusal_of_the_whole_call_answers_it_without_the_backend() {
    let request = heartbeat();
    let rules = [fired(
        "Rebalance",
        RefusalLie::new(ErrorCode::REBALANCE_IN_PROGRESS),
        Target::Call,
    )];

    let walked = walk(&every_lie(), &rules, &request, no_backend);

    assert!(walked.forwarded.is_none());
    let body: HeartbeatResponse = walked.sent().body().unwrap();
    assert_eq!(
        ErrorCode::from_wire(body.error_code),
        Some(ErrorCode::REBALANCE_IN_PROGRESS)
    );
}

#[test]
fn a_refusal_after_the_response_lets_the_backend_handle_the_whole_request() {
    let request = produce(&["orders.eu/3", "orders.eu/4"]);
    let lost = RefusalLie::new(ErrorCode::REQUEST_TIMED_OUT).at(AFTER);
    let rules = [fired("Lost acknowledgement", lost, parts(&["orders.eu/3"]))];

    let walked = walk(&every_lie(), &rules, &request, backend);

    // The backend writes the records, and the client is told that it failed.
    assert_eq!(
        produced(walked.forwarded.as_ref().unwrap()),
        ["orders.eu/3", "orders.eu/4"]
    );
    assert_eq!(
        told(walked.sent()),
        ["orders.eu/3 REQUEST_TIMED_OUT", "orders.eu/4 at 100"]
    );
}

#[test]
fn a_lie_that_does_not_apply_is_not_asked() {
    let request = produce(&["orders.eu/3"]);
    let rules = [
        fired("Blackhole", LieDefinition::Silence, Target::Call),
        fired("Leader flap", leader_flap(), parts(&["orders.eu/3"])),
        fired(
            "Delay 1 s",
            TimingLie::new().with_delay(TimeSpan::from_secs(1)),
            Target::Call,
        ),
    ];

    // The set holds no refusal. Asking for the shadowed one would fail, and nobody asks.
    let walked = walk(&Lies::of_every_build(), &rules, &request, no_backend);

    assert!(matches!(walked.end, End::Dropped));
    assert!(walked.forwarded.is_none());
}

#[test]
fn a_hang_up_after_the_response_closes_the_connection_once_the_backend_answered() {
    let request = produce(&["orders.eu/3"]);
    let rules = [fired("Late disconnect", HangUpLie::new().at(AFTER), Target::Call)];

    let walked = walk(&every_lie(), &rules, &request, backend);

    assert_eq!(produced(walked.forwarded.as_ref().unwrap()), ["orders.eu/3"]);
    assert!(matches!(walked.end, End::Closed));
}

#[test]
fn the_wait_of_a_stage_comes_from_combining_and_a_hold_is_one_lie_s() {
    let request = heartbeat();
    let truth = backend(&request);
    let second = TimingLie::new()
        .with_delay(TimeSpan::from_secs(1))
        .with_jitter(TimeSpan::from_millis(200))
        .with_throttle(TimeSpan::from_millis(300));
    let more = TimingLie::new()
        .with_delay(TimeSpan::from_millis(500))
        .with_throttle(TimeSpan::from_millis(500));
    let rules = [
        fired("Delay 1 s", second, Target::Call),
        fired("Background jitter", more, Target::Call),
    ];
    let lies = every_lie();

    // The walk holds the answer once, for what combining added up.
    let walked = walk(&lies, &rules, &request, backend);
    assert_eq!(walked.wait.length(), TimeSpan::from_millis(1500));
    assert_eq!(walked.wait.jitter(), TimeSpan::from_millis(200));
    assert_eq!(walked.throttle, TimeSpan::from_millis(500));

    // Each timing lie asks for its own hold, and knows nothing of the other.
    let holds: Vec<TimingLie> = rules
        .iter()
        .map(|fired| fired.lie.clone().on(Target::Call))
        .map(
            |lie| match lies.after_response(&lie, &decoded(&request), &truth).unwrap() {
                Effect::Hold(timing) => timing,
                other => panic!("{lie} asked to {other}"),
            },
        )
        .collect();
    assert_eq!(holds, [second, more]);
}

#[test]
fn a_delay_before_forwarding_asks_to_hold_the_request_back() {
    let request = heartbeat();
    let late = TimingLie::new().with_delay(TimeSpan::from_secs(45)).at(BEFORE);

    let effect = every_lie()
        .before_forwarding(&LieDefinition::from(late).on(Target::Call), &decoded(&request))
        .unwrap();

    assert_eq!(effect.to_string(), "hold +45 s before forwarding");
}

#[test]
fn a_kind_that_cannot_tell_its_lie_on_an_api_says_so() {
    let request = Request::new(9, 7, Some("order-ingest"), &MetadataRequest::default()).unwrap();
    let lie = LieDefinition::from(leader_flap()).on(Target::Call);

    let error = every_lie().before_forwarding(&lie, &decoded(&request)).unwrap_err();

    assert_eq!(
        error,
        LieError::ApiNotSupported {
            kind: LieKind::Refusal,
            api: request.api()
        }
    );
    assert_eq!(error.to_string(), "refusal cannot be told on Metadata v9");
}

#[test]
fn a_kind_that_cannot_read_the_call_says_so() {
    // A request arrives decoded, so only a response can be unreadable: this one is cut short.
    let request = produce(&["orders.eu/3"]);
    let whole = backend(&request);
    let payload = whole.frame().payload();
    let cut = Response::decode(
        request.api(),
        gaslightmq_wire::Frame::new(payload.slice(..payload.len() - 3)),
    )
    .unwrap();
    let lie = LieDefinition::from(leader_flap()).on(Target::Call);

    let error = every_lie().after_response(&lie, &decoded(&request), &cut).unwrap_err();

    assert!(
        matches!(
            error,
            LieError::CallNotRewritten {
                kind: LieKind::Refusal,
                ..
            }
        ),
        "{error}"
    );
    assert!(
        error
            .to_string()
            .starts_with("refusal cannot rewrite the call: cannot decode response body: ")
    );
}
