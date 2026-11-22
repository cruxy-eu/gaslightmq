//! One call of a role, step by step.
//!
//! A [`Call`] is a state machine. Whoever drives it reports what happened as an [`Input`],
//! and carries out the [`Step`] that comes back:
//!
//! | Step | The driver |
//! |---|---|
//! | [`Step::Exchange`] | Asks [`Call::request`] for the request, sends it on the connection the step names, and reports the response or the failure. |
//! | [`Step::Wait`] | Waits until the instant, and reports that it did. |
//! | [`Step::Done`] | Returns the result. The call is over. |

use std::collections::VecDeque;
use std::time::Duration;

use gaslightmq_clock::Instant;
use gaslightmq_wire::messages::{FindCoordinatorRequest, FindCoordinatorResponse, GroupId, MetadataResponse};
use gaslightmq_wire::protocol::Request as KafkaRequest;
use gaslightmq_wire::{Request, Response};

use crate::error::ClientError;
use crate::logic::attempts::{AfterFailure, Attempts, Failed, Limits};
use crate::logic::cluster::{Candidates, ClusterView, Destination, Route, TopicPartition, Traffic};
use crate::logic::conversation::Conversation;
use crate::logic::topics;
use crate::logic::versions::Versions;

/// The versions of `FindCoordinator` that name one group in `key`.
const FIND_COORDINATOR_VERSIONS: Versions = Versions::new(0, 3);

/// The `key_type` of a `FindCoordinator` request that asks for the coordinator of a group.
const COORDINATOR_OF_A_GROUP: i8 = 0;

/// What an operation makes of a response.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Reading<T> {
    /// The call is over, with this output.
    Done(T),
    /// The response asks for the request once more, changed by what the operation just learned.
    /// It goes out at once and uses no retry, so an operation never answers this twice in a row.
    Again,
}

/// One kind of call: the request it sends, where the request goes, and what the response means.
pub(crate) trait Operation {
    type Request: KafkaRequest;
    type Output;

    /// The versions of the API that [`request`](Self::request) and [`read`](Self::read) speak.
    const VERSIONS: Versions;

    /// Where the request has to go.
    fn route(&self) -> Route;

    /// The request, for a try that waits `within` for its response.
    fn request(&self, within: Duration) -> Self::Request;

    /// How long a try waits for its response, for a role with this request timeout.
    /// An operation that tells the broker to hold its request waits longer, and says so here.
    fn answer_within(&self, request_timeout: Duration) -> Duration {
        request_timeout
    }

    /// How long the call may take with every retry, for a role with these limits.
    /// An operation whose single try may take longer than the calls of its role says so here.
    fn call_within(&self, limits: &Limits) -> Duration {
        limits.call_timeout
    }

    /// Whether the broker may create the topic, when the call has to ask who leads a partition of it.
    /// It may, unless the role is configured otherwise, as with Kafka's own clients.
    fn allow_auto_create_topics(&self) -> bool {
        true
    }

    /// What the cluster view takes over from the response, whatever the call makes of it.
    fn learn(&self, _response: &<Self::Request as KafkaRequest>::Response, _cluster: &mut ClusterView, _now: Instant) {}

    /// What the response means.
    ///
    /// # Errors
    ///
    /// The failure the response reports. The call tries again when it is retriable.
    fn read(
        &mut self,
        response: <Self::Request as KafkaRequest>::Response,
    ) -> Result<Reading<Self::Output>, ClientError>;
}

/// Why a call sends a request.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Purpose {
    /// It is the request of the operation.
    Operation,
    /// The call has to learn who leads the partition, before the operation can go there.
    LocateLeader(TopicPartition),
    /// The call has to learn who coordinates the group, before the operation can go there.
    LocateCoordinator(GroupId),
}

/// One request and its response, on one connection.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Exchange {
    /// The connection the request belongs on.
    pub destination: Destination,
    /// How long the socket may take to connect, when the connection is not open yet.
    pub connect_within: Duration,
    /// How long the greeting on a new connection waits for its answer.
    pub greet_within: Duration,
    /// How long the request waits for its response.
    pub answer_within: Duration,
    /// The instant the call ends.
    /// The exchange is over by then, however long the request waits for its turn on the connection.
    pub not_after: Instant,
    purpose: Purpose,
    /// The connections to try when this one fails.
    fallbacks: VecDeque<Destination>,
}

/// What happened to a call. The driver reports it.
#[derive(Debug)]
pub(crate) enum Input {
    /// The call starts.
    Started,
    /// The instant of a [`Step::Wait`] came.
    Waited,
    /// The request of an exchange got this response.
    Answered(Exchange, Response),
    /// An exchange failed: its connection did not open, the request could not be sent, or no response came.
    Failed(Exchange, ClientError),
}

/// What a call needs next. The driver carries it out.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Step<T> {
    /// Send the request of this exchange, and report what came back.
    Exchange(Exchange),
    /// Do nothing until this instant.
    Wait { until: Instant },
    /// The call is over.
    Done(Result<T, ClientError>),
}

/// One call of a role: an operation, and the tries it takes to carry it out.
pub(crate) struct Call<O: Operation> {
    operation: O,
    limits: Limits,
    attempts: Attempts,
    retry_at: Option<Instant>,
}

impl<O: Operation> Call<O> {
    /// A call that starts at `now`.
    pub fn new(operation: O, limits: Limits, now: Instant) -> Self {
        let limits = Limits {
            call_timeout: operation.call_within(&limits),
            ..limits
        };
        Self {
            operation,
            limits,
            attempts: Attempts::new(&limits, now),
            retry_at: None,
        }
    }

    /// Take what happened, and decide what the call needs next.
    pub fn handle(&mut self, input: Input, cluster: &mut ClusterView, now: Instant) -> Step<O::Output> {
        match input {
            Input::Started | Input::Waited => match self.retry_at {
                Some(until) if now < until => Step::Wait { until },
                _ => self.send(cluster, now),
            },
            Input::Answered(exchange, response) => {
                cluster.answered(exchange.destination.connection);
                match self.read(&exchange, &response, cluster, now) {
                    Ok(Some(output)) => Step::Done(Ok(output)),
                    Ok(None) => self.send(cluster, now),
                    Err(error) => self.fail(&exchange, error, cluster, now),
                }
            }
            Input::Failed(mut exchange, error) => {
                if error.is_retriable() {
                    cluster.failed(exchange.destination.connection);
                    if let Some(next) = exchange.fallbacks.pop_front() {
                        exchange.destination = next;
                        return Step::Exchange(self.limited(exchange, now));
                    }
                }
                self.fail(&exchange, error, cluster, now)
            }
        }
    }

    /// The request of `exchange`, as the next request of the conversation on its connection.
    ///
    /// # Errors
    ///
    /// [`ClientError::UnsupportedVersion`] when the broker speaks no version of the API that the client speaks,
    /// and [`ClientError::Protocol`] when the request does not encode.
    pub fn request(&self, exchange: &Exchange, conversation: &mut Conversation) -> Result<Request, ClientError> {
        match &exchange.purpose {
            Purpose::Operation => conversation.request(O::VERSIONS, &self.operation.request(exchange.answer_within)),
            Purpose::LocateLeader(partition) => conversation.request(
                topics::VERSIONS,
                &topics::request(Some(&partition.topic), self.operation.allow_auto_create_topics()),
            ),
            Purpose::LocateCoordinator(group) => conversation.request(
                FIND_COORDINATOR_VERSIONS,
                &FindCoordinatorRequest::default()
                    .with_key(group.0.clone())
                    .with_key_type(COORDINATOR_OF_A_GROUP),
            ),
        }
    }

    /// The operation, as the call left it.
    pub fn into_operation(self) -> O {
        self.operation
    }

    /// Start the next exchange: the operation when the view knows where it goes,
    /// and otherwise the question that tells it.
    ///
    /// The question about a group travels with the group's traffic, and never on a connection that carries data.
    fn send(&mut self, cluster: &ClusterView, now: Instant) -> Step<O::Output> {
        self.retry_at = None;
        let (purpose, candidates) = match self.operation.route() {
            Route::AnyBroker => (Purpose::Operation, cluster.any_broker(Traffic::Data)),
            Route::Leader(partition) => match cluster.leader(&partition, now) {
                Some(leader) => (Purpose::Operation, Candidates::from(leader)),
                None => (Purpose::LocateLeader(partition), cluster.any_broker(Traffic::Data)),
            },
            Route::Coordinator(group) => match cluster.coordinator(&group) {
                Some(coordinator) => (Purpose::Operation, Candidates::from(coordinator)),
                None => (Purpose::LocateCoordinator(group), cluster.any_broker(Traffic::Group)),
            },
        };
        let exchange = Exchange {
            destination: candidates.first,
            connect_within: Duration::ZERO,
            greet_within: Duration::ZERO,
            answer_within: Duration::ZERO,
            not_after: self.attempts.deadline(),
            purpose,
            fallbacks: candidates.others,
        };
        Step::Exchange(self.limited(exchange, now))
    }

    /// Give `exchange` the time it may take: what the limits allow, and never more than the call has left.
    fn limited(&self, mut exchange: Exchange, now: Instant) -> Exchange {
        let remaining = self.attempts.remaining(now);
        let answer_within = match exchange.purpose {
            Purpose::Operation => self.operation.answer_within(self.limits.request_timeout),
            Purpose::LocateLeader(_) | Purpose::LocateCoordinator(_) => self.limits.request_timeout,
        };
        exchange.connect_within = self.limits.socket_connection_setup_timeout.min(remaining);
        exchange.greet_within = self.limits.request_timeout.min(remaining);
        exchange.answer_within = answer_within.min(remaining);
        exchange
    }

    /// Read the response of `exchange`. `Some` ends the call, and `None` asks for the next exchange.
    fn read(
        &mut self,
        exchange: &Exchange,
        response: &Response,
        cluster: &mut ClusterView,
        now: Instant,
    ) -> Result<Option<O::Output>, ClientError> {
        match &exchange.purpose {
            Purpose::Operation => {
                let body = response.body()?;
                self.operation.learn(&body, cluster, now);
                Ok(match self.operation.read(body)? {
                    Reading::Done(output) => Some(output),
                    Reading::Again => None,
                })
            }
            Purpose::LocateLeader(partition) => {
                let metadata: MetadataResponse = response.body()?;
                cluster.learn(&metadata, now);
                if cluster.leader(partition, now).is_some() {
                    return Ok(None);
                }
                Err(
                    topics::error_of(&metadata, &partition.topic).unwrap_or_else(|| ClientError::NoLeader {
                        topic: partition.topic.to_string(),
                        partition: partition.partition,
                    }),
                )
            }
            Purpose::LocateCoordinator(group) => {
                let found: FindCoordinatorResponse = response.body()?;
                if let Some(error) = ClientError::broker(found.error_code) {
                    return Err(error);
                }
                if cluster.learn_coordinator(group, found.node_id, &found.host, found.port) {
                    Ok(None)
                } else {
                    Err(ClientError::Protocol(format!(
                        "the coordinator of group {} has no address: {}:{}",
                        group.as_str(),
                        found.host,
                        found.port
                    )))
                }
            }
        }
    }

    /// Decide what follows the failure of `exchange`: another try after a wait, or the end of the call.
    ///
    /// A retriable failure of the operation makes the view forget where the request went,
    /// so the next try asks for the leader or the coordinator again.
    /// Only a failure of the operation uses one of the retries.
    fn fail(
        &mut self,
        exchange: &Exchange,
        error: ClientError,
        cluster: &mut ClusterView,
        now: Instant,
    ) -> Step<O::Output> {
        if !error.is_retriable() {
            return Step::Done(Err(error));
        }
        let failed = if exchange.purpose == Purpose::Operation {
            match self.operation.route() {
                Route::AnyBroker => {}
                Route::Leader(partition) => cluster.forget_leader(&partition),
                Route::Coordinator(group) => cluster.forget_coordinator(&group),
            }
            Failed::Operation
        } else {
            Failed::Lookup
        };
        match self.attempts.after_failure(failed, now) {
            AfterFailure::RetryAt(until) => {
                self.retry_at = Some(until);
                Step::Wait { until }
            }
            AfterFailure::OutOfRetries => Step::Done(Err(error)),
            AfterFailure::OutOfTime => Step::Done(Err(ClientError::CallTimedOut { error: Box::new(error) })),
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use gaslightmq_wire::messages::{BrokerId, HeartbeatRequest, HeartbeatResponse};
    use gaslightmq_wire::protocol::{Encodable, StrBytes};
    use gaslightmq_wire::{Api, ApiKey, ResponseError};

    use super::*;
    use crate::logic::cluster::Peer;
    use crate::logic::cluster::tests::{MAX_AGE, for_data, for_groups, metadata, partition, to_broker, topic, view};
    use crate::logic::cluster::{group_id, topic_name};
    use crate::logic::versions::BrokerVersions;

    /// A request with nothing in it, to wherever the test sends it.
    struct Ping {
        route: Route,
        asks_again: bool,
        allow_auto_create_topics: bool,
    }

    impl Ping {
        fn to(route: Route) -> Self {
            Self {
                route,
                asks_again: false,
                allow_auto_create_topics: true,
            }
        }
    }

    impl Operation for Ping {
        type Request = HeartbeatRequest;
        type Output = &'static str;

        const VERSIONS: Versions = Versions::new(0, 3);

        fn route(&self) -> Route {
            self.route.clone()
        }

        fn request(&self, _within: Duration) -> HeartbeatRequest {
            HeartbeatRequest::default()
        }

        fn allow_auto_create_topics(&self) -> bool {
            self.allow_auto_create_topics
        }

        fn read(&mut self, response: HeartbeatResponse) -> Result<Reading<&'static str>, ClientError> {
            if let Some(error) = ClientError::broker(response.error_code) {
                return Err(error);
            }
            if std::mem::take(&mut self.asks_again) {
                return Ok(Reading::Again);
            }
            Ok(Reading::Done("pong"))
        }
    }

    pub fn limits() -> Limits {
        Limits {
            call_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(5),
            socket_connection_setup_timeout: Duration::from_secs(1),
            retry_backoff: Duration::from_millis(100),
            retry_backoff_max: Duration::from_millis(100),
            retries: None,
        }
    }

    fn at(millis: u64) -> Instant {
        Instant::ZERO + Duration::from_millis(millis)
    }

    /// A conversation with a broker that speaks every API this module sends, up to version 20.
    pub fn conversation() -> Conversation {
        let apis = [ApiKey::Metadata, ApiKey::FindCoordinator, ApiKey::Heartbeat].map(|key| key as i16);
        Conversation::with(BrokerVersions::speaking(&apis, Versions::new(0, 20)))
    }

    pub fn exchange_of<T: std::fmt::Debug>(step: Step<T>) -> Exchange {
        match step {
            Step::Exchange(exchange) => exchange,
            other => panic!("expected an exchange, got {other:?}"),
        }
    }

    /// The API of the request that `exchange` sends.
    fn api_of<O: Operation>(call: &Call<O>, exchange: &Exchange) -> Api {
        call.request(exchange, &mut conversation()).unwrap().api()
    }

    /// The input for a broker that answers `exchange` with `body`.
    pub fn answered<O: Operation>(call: &Call<O>, exchange: Exchange, body: &impl Encodable) -> Input {
        let request = call.request(&exchange, &mut conversation()).unwrap();
        Input::Answered(exchange, request.response(body).unwrap())
    }

    fn pong() -> HeartbeatResponse {
        HeartbeatResponse::default()
    }

    fn refusal(error: ResponseError) -> HeartbeatResponse {
        HeartbeatResponse::default().with_error_code(error.code())
    }

    fn found(broker: i32) -> FindCoordinatorResponse {
        FindCoordinatorResponse::default()
            .with_node_id(BrokerId(broker))
            .with_host(StrBytes::from_string(format!("broker-{broker}")))
            .with_port(9092)
    }

    fn to_leader_of_orders() -> Ping {
        Ping::to(Route::Leader(partition("orders", 0)))
    }

    #[test]
    fn a_request_goes_to_the_leader_the_view_knows() {
        let mut cluster = view();
        cluster.learn(&metadata(&[1, 2], vec![topic("orders", &[2])]), at(0));
        let mut call = Call::new(to_leader_of_orders(), limits(), at(0));

        let exchange = exchange_of(call.handle(Input::Started, &mut cluster, at(0)));

        assert_eq!(exchange.destination, to_broker(2));
        assert_eq!(api_of(&call, &exchange), Api::new(ApiKey::Heartbeat, 3));
        let input = answered(&call, exchange, &pong());
        assert_eq!(call.handle(input, &mut cluster, at(5)), Step::Done(Ok("pong")));
    }

    #[test]
    fn a_call_first_asks_for_the_leader_it_does_not_know() {
        let mut cluster = view();
        let mut call = Call::new(to_leader_of_orders(), limits(), at(0));

        let asking = exchange_of(call.handle(Input::Started, &mut cluster, at(0)));

        assert_eq!(asking.destination.connection, for_data(Peer::Bootstrap(0)));
        assert_eq!(api_of(&call, &asking), Api::new(ApiKey::Metadata, 12));
        let request = call.request(&asking, &mut conversation()).unwrap();
        let asked: gaslightmq_wire::messages::MetadataRequest = request.body().unwrap();
        assert!(asked.allow_auto_topic_creation);
        let topics: Vec<_> = asked.topics.unwrap().into_iter().map(|topic| topic.name).collect();
        assert_eq!(topics, [Some(topic_name("orders"))]);

        let input = answered(&call, asking, &metadata(&[1, 2], vec![topic("orders", &[2])]));
        let sending = exchange_of(call.handle(input, &mut cluster, at(5)));

        assert_eq!(sending.destination, to_broker(2));
        assert_eq!(api_of(&call, &sending), Api::new(ApiKey::Heartbeat, 3));
    }

    #[test]
    fn a_role_that_forbids_it_asks_for_a_leader_without_creating_the_topic() {
        let mut cluster = view();
        let operation = Ping {
            allow_auto_create_topics: false,
            ..to_leader_of_orders()
        };
        let mut call = Call::new(operation, limits(), at(0));

        let asking = exchange_of(call.handle(Input::Started, &mut cluster, at(0)));

        let request = call.request(&asking, &mut conversation()).unwrap();
        let asked: gaslightmq_wire::messages::MetadataRequest = request.body().unwrap();
        assert!(!asked.allow_auto_topic_creation);
    }

    #[test]
    fn a_call_asks_again_for_a_leader_that_is_older_than_the_max_age() {
        let mut cluster = view();
        cluster.learn(&metadata(&[1, 2], vec![topic("orders", &[2])]), at(0));
        let later = at(0) + MAX_AGE + Duration::from_millis(1);
        let mut call = Call::new(to_leader_of_orders(), limits(), later);

        let asking = exchange_of(call.handle(Input::Started, &mut cluster, later));

        assert_eq!(api_of(&call, &asking).known(), Some(ApiKey::Metadata));
        let input = answered(&call, asking, &metadata(&[1, 2], vec![topic("orders", &[1])]));
        let sending = exchange_of(call.handle(input, &mut cluster, later));
        assert_eq!(sending.destination, to_broker(1));
    }

    #[test]
    fn a_partition_without_a_leader_is_asked_again_after_the_backoff() {
        let mut cluster = view();
        let mut call = Call::new(to_leader_of_orders(), limits(), at(0));
        let asking = exchange_of(call.handle(Input::Started, &mut cluster, at(0)));

        let input = answered(&call, asking, &metadata(&[1], vec![topic("orders", &[-1])]));
        let step = call.handle(input, &mut cluster, at(20));

        assert_eq!(step, Step::Wait { until: at(120) });
        assert_eq!(
            call.handle(Input::Waited, &mut cluster, at(119)),
            Step::Wait { until: at(120) }
        );
        let again = exchange_of(call.handle(Input::Waited, &mut cluster, at(120)));
        assert_eq!(api_of(&call, &again).known(), Some(ApiKey::Metadata));
    }

    #[test]
    fn a_partition_without_a_leader_fails_with_the_error_of_its_topic_when_the_time_is_over() {
        let mut cluster = view();
        let mut call = Call::new(to_leader_of_orders(), limits(), at(0));
        let asking = exchange_of(call.handle(Input::Started, &mut cluster, at(0)));
        let unknown = topic("orders", &[]).with_error_code(ResponseError::UnknownTopicOrPartition.code());

        let input = answered(&call, asking, &metadata(&[1], vec![unknown]));
        let step = call.handle(input, &mut cluster, at(9_950));

        assert_eq!(
            step,
            Step::Done(Err(ClientError::CallTimedOut {
                error: Box::new(ClientError::Broker {
                    error: ResponseError::UnknownTopicOrPartition
                })
            }))
        );
    }

    #[test]
    fn a_partition_without_a_leader_and_without_an_error_has_no_leader() {
        let mut cluster = view();
        let mut call = Call::new(to_leader_of_orders(), limits(), at(0));
        let asking = exchange_of(call.handle(Input::Started, &mut cluster, at(0)));

        let input = answered(&call, asking, &metadata(&[1], vec![topic("orders", &[-1])]));
        let step = call.handle(input, &mut cluster, at(9_950));

        assert_eq!(
            step,
            Step::Done(Err(ClientError::CallTimedOut {
                error: Box::new(ClientError::NoLeader {
                    topic: "orders".into(),
                    partition: 0
                })
            }))
        );
    }

    #[test]
    fn a_call_without_retries_still_asks_again_for_its_leader() {
        let mut cluster = view();
        let mut limits = limits();
        limits.retries = Some(0);
        let mut call = Call::new(to_leader_of_orders(), limits, at(0));

        // No leader yet, twice: asking again uses no retry.
        for asked_at in [0, 100] {
            let step = call.handle(Input::Waited, &mut cluster, at(asked_at));
            let input = answered(&call, exchange_of(step), &metadata(&[1], vec![topic("orders", &[-1])]));
            assert_eq!(
                call.handle(input, &mut cluster, at(asked_at)),
                Step::Wait {
                    until: at(asked_at + 100)
                }
            );
        }

        // The leader is there, and the one try of the operation fails for good.
        let asking = exchange_of(call.handle(Input::Waited, &mut cluster, at(200)));
        let input = answered(&call, asking, &metadata(&[1], vec![topic("orders", &[1])]));
        let sending = exchange_of(call.handle(input, &mut cluster, at(200)));
        let input = answered(&call, sending, &refusal(ResponseError::NotLeaderOrFollower));
        assert_eq!(
            call.handle(input, &mut cluster, at(200)),
            Step::Done(Err(ClientError::Broker {
                error: ResponseError::NotLeaderOrFollower
            }))
        );
    }

    #[test]
    fn a_retriable_failure_makes_the_call_ask_for_the_leader_again() {
        let mut cluster = view();
        cluster.learn(&metadata(&[1, 2], vec![topic("orders", &[2])]), at(0));
        let mut call = Call::new(to_leader_of_orders(), limits(), at(0));
        let sending = exchange_of(call.handle(Input::Started, &mut cluster, at(0)));

        let input = answered(&call, sending, &refusal(ResponseError::NotLeaderOrFollower));
        let step = call.handle(input, &mut cluster, at(10));

        assert_eq!(step, Step::Wait { until: at(110) });
        assert_eq!(cluster.leader(&partition("orders", 0), at(10)), None);
        let asking = exchange_of(call.handle(Input::Waited, &mut cluster, at(110)));
        assert_eq!(api_of(&call, &asking).known(), Some(ApiKey::Metadata));
        assert_eq!(asking.destination, to_broker(1));

        let input = answered(&call, asking, &metadata(&[1, 2], vec![topic("orders", &[1])]));
        let sending = exchange_of(call.handle(input, &mut cluster, at(115)));
        assert_eq!(sending.destination, to_broker(1));
    }

    #[test]
    fn a_connection_that_fails_is_a_retriable_failure_too() {
        let mut cluster = view();
        cluster.learn(&metadata(&[1], vec![topic("orders", &[1])]), at(0));
        let mut call = Call::new(to_leader_of_orders(), limits(), at(0));
        let sending = exchange_of(call.handle(Input::Started, &mut cluster, at(0)));

        let step = call.handle(
            Input::Failed(sending, ClientError::ConnectionFailed("reset".into())),
            &mut cluster,
            at(10),
        );

        assert_eq!(step, Step::Wait { until: at(110) });
        assert_eq!(cluster.leader(&partition("orders", 0), at(10)), None);
    }

    #[test]
    fn a_failure_that_is_not_retriable_ends_the_call() {
        let mut cluster = view();
        cluster.learn(&metadata(&[1], vec![topic("orders", &[1])]), at(0));
        let mut call = Call::new(to_leader_of_orders(), limits(), at(0));
        let sending = exchange_of(call.handle(Input::Started, &mut cluster, at(0)));

        let input = answered(&call, sending, &refusal(ResponseError::TopicAuthorizationFailed));
        let step = call.handle(input, &mut cluster, at(10));

        assert_eq!(
            step,
            Step::Done(Err(ClientError::Broker {
                error: ResponseError::TopicAuthorizationFailed
            }))
        );
        assert!(cluster.leader(&partition("orders", 0), at(10)).is_some());
    }

    #[test]
    fn a_call_that_runs_out_of_retries_fails_with_its_last_error() {
        let mut cluster = view();
        let mut limits = limits();
        limits.retries = Some(1);
        let mut call = Call::new(Ping::to(Route::AnyBroker), limits, at(0));
        let refused = |call: &Call<Ping>, exchange| answered(call, exchange, &refusal(ResponseError::RequestTimedOut));

        let first = exchange_of(call.handle(Input::Started, &mut cluster, at(0)));
        let input = refused(&call, first);
        assert_eq!(call.handle(input, &mut cluster, at(0)), Step::Wait { until: at(100) });
        let second = exchange_of(call.handle(Input::Waited, &mut cluster, at(100)));
        let input = refused(&call, second);

        assert_eq!(
            call.handle(input, &mut cluster, at(100)),
            Step::Done(Err(ClientError::Broker {
                error: ResponseError::RequestTimedOut
            }))
        );
    }

    #[test]
    fn a_call_that_runs_out_of_time_says_so_and_keeps_its_last_error() {
        let mut cluster = view();
        let mut call = Call::new(Ping::to(Route::AnyBroker), limits(), at(0));
        let sending = exchange_of(call.handle(Input::Started, &mut cluster, at(0)));

        let input = answered(&call, sending, &refusal(ResponseError::RequestTimedOut));
        let step = call.handle(input, &mut cluster, at(9_950));

        assert_eq!(
            step,
            Step::Done(Err(ClientError::CallTimedOut {
                error: Box::new(ClientError::Broker {
                    error: ResponseError::RequestTimedOut
                })
            }))
        );
    }

    #[test]
    fn a_request_for_any_broker_moves_on_to_the_next_one_at_once() {
        let mut cluster = view();
        cluster.learn(&metadata(&[1], vec![]), at(0));
        let mut call = Call::new(Ping::to(Route::AnyBroker), limits(), at(0));
        let failed = || ClientError::ConnectionFailed("refused".into());

        let first = exchange_of(call.handle(Input::Started, &mut cluster, at(0)));
        assert_eq!(first.destination, to_broker(1));
        let second = exchange_of(call.handle(Input::Failed(first, failed()), &mut cluster, at(1)));
        assert_eq!(second.destination.connection, for_data(Peer::Bootstrap(0)));
        let third = exchange_of(call.handle(Input::Failed(second, failed()), &mut cluster, at(2)));
        assert_eq!(third.destination.connection, for_data(Peer::Bootstrap(1)));

        let step = call.handle(Input::Failed(third, failed()), &mut cluster, at(3));
        assert_eq!(step, Step::Wait { until: at(103) });
    }

    #[test]
    fn a_broker_that_gave_no_response_is_asked_last_by_the_next_try() {
        let mut cluster = view();
        cluster.learn(&metadata(&[1, 2], vec![]), at(0));
        let mut call = Call::new(Ping::to(Route::AnyBroker), limits(), at(0));
        let first = exchange_of(call.handle(Input::Started, &mut cluster, at(0)));
        assert_eq!(first.destination, to_broker(1));

        let second = exchange_of(call.handle(Input::Failed(first, ClientError::RequestTimedOut), &mut cluster, at(1)));
        assert_eq!(second.destination, to_broker(2));
        let input = answered(&call, second, &refusal(ResponseError::RequestTimedOut));
        assert_eq!(call.handle(input, &mut cluster, at(1)), Step::Wait { until: at(101) });

        let third = exchange_of(call.handle(Input::Waited, &mut cluster, at(101)));
        assert_eq!(third.destination, to_broker(2));

        // A response from broker 1 puts it back in front.
        cluster.learn(&metadata(&[1, 2], vec![topic("orders", &[1])]), at(101));
        let mut other = Call::new(to_leader_of_orders(), limits(), at(101));
        let sending = exchange_of(other.handle(Input::Started, &mut cluster, at(101)));
        assert_eq!(sending.destination, to_broker(1));
        let input = answered(&other, sending, &pong());
        other.handle(input, &mut cluster, at(101));
        assert_eq!(cluster.any_broker(Traffic::Data).first, to_broker(1));
    }

    #[test]
    fn a_broker_that_answers_with_an_error_is_not_swapped_for_the_next_one() {
        let mut cluster = view();
        let mut call = Call::new(Ping::to(Route::AnyBroker), limits(), at(0));
        let sending = exchange_of(call.handle(Input::Started, &mut cluster, at(0)));

        let input = answered(&call, sending, &refusal(ResponseError::RequestTimedOut));

        assert_eq!(call.handle(input, &mut cluster, at(0)), Step::Wait { until: at(100) });
    }

    #[test]
    fn everything_a_group_needs_stays_off_the_connections_that_carry_data() {
        let mut cluster = view();
        cluster.learn(&metadata(&[1, 2], vec![topic("orders", &[2])]), at(0));
        let mut call = Call::new(Ping::to(Route::Coordinator(group_id("analytics"))), limits(), at(0));

        // The question who coordinates the group goes to broker 1, and not on the connection a Fetch would use.
        let asking = exchange_of(call.handle(Input::Started, &mut cluster, at(0)));
        assert_eq!(asking.destination.connection, for_groups(Peer::Broker(BrokerId(1))));
        assert_ne!(asking.destination.connection, to_broker(1).connection);
        assert_eq!(asking.destination.address, to_broker(1).address);
        let request = call.request(&asking, &mut conversation()).unwrap();
        assert_eq!(request.api(), Api::new(ApiKey::FindCoordinator, 3));
        assert_eq!(
            request.body::<FindCoordinatorRequest>().unwrap().key.as_str(),
            "analytics"
        );

        let input = answered(&call, asking, &found(2));
        let sending = exchange_of(call.handle(input, &mut cluster, at(5)));

        assert_eq!(sending.destination.connection, for_groups(Peer::Broker(BrokerId(2))));
        assert_eq!(sending.destination.address, to_broker(2).address);
        assert_eq!(api_of(&call, &sending).known(), Some(ApiKey::Heartbeat));
    }

    #[test]
    fn a_retriable_failure_makes_the_call_ask_for_the_coordinator_again() {
        let mut cluster = view();
        let group = group_id("analytics");
        cluster.learn_coordinator(&group, BrokerId(2), "broker-2", 9092);
        let mut call = Call::new(Ping::to(Route::Coordinator(group.clone())), limits(), at(0));
        let sending = exchange_of(call.handle(Input::Started, &mut cluster, at(0)));

        let input = answered(&call, sending, &refusal(ResponseError::NotCoordinator));
        let step = call.handle(input, &mut cluster, at(0));

        assert_eq!(step, Step::Wait { until: at(100) });
        assert_eq!(cluster.coordinator(&group), None);
        let asking = exchange_of(call.handle(Input::Waited, &mut cluster, at(100)));
        assert_eq!(api_of(&call, &asking).known(), Some(ApiKey::FindCoordinator));
    }

    #[test]
    fn a_coordinator_that_cannot_be_found_is_the_failure_of_the_call() {
        let mut cluster = view();
        let mut call = Call::new(Ping::to(Route::Coordinator(group_id("analytics"))), limits(), at(0));
        let asking = exchange_of(call.handle(Input::Started, &mut cluster, at(0)));

        let input = answered(
            &call,
            asking,
            &FindCoordinatorResponse::default().with_error_code(ResponseError::GroupAuthorizationFailed.code()),
        );

        assert_eq!(
            call.handle(input, &mut cluster, at(0)),
            Step::Done(Err(ClientError::Broker {
                error: ResponseError::GroupAuthorizationFailed
            }))
        );
    }

    #[test]
    fn an_exchange_never_takes_longer_than_the_call_has_left() {
        let mut cluster = view();
        let mut call = Call::new(Ping::to(Route::AnyBroker), limits(), at(0));

        let early = exchange_of(call.handle(Input::Started, &mut cluster, at(0)));
        assert_eq!(early.connect_within, Duration::from_secs(1));
        assert_eq!(early.greet_within, Duration::from_secs(5));
        assert_eq!(early.answer_within, Duration::from_secs(5));
        assert_eq!(early.not_after, at(10_000));

        let step = call.handle(
            Input::Failed(early, ClientError::RequestTimedOut),
            &mut cluster,
            at(9_500),
        );
        let late = exchange_of(step);
        assert_eq!(late.connect_within, Duration::from_millis(500));
        assert_eq!(late.greet_within, Duration::from_millis(500));
        assert_eq!(late.answer_within, Duration::from_millis(500));
    }

    #[test]
    fn a_call_waits_longer_after_every_failure() {
        let mut cluster = view();
        let mut limits = limits();
        limits.retry_backoff_max = Duration::from_millis(250);
        let mut call = Call::new(Ping::to(Route::AnyBroker), limits, at(0));
        let mut step = call.handle(Input::Started, &mut cluster, at(0));
        let mut waits = Vec::new();

        for _ in 0..3 {
            let input = answered(&call, exchange_of(step), &refusal(ResponseError::RequestTimedOut));
            let Step::Wait { until } = call.handle(input, &mut cluster, at(0)) else {
                panic!("the call waits before it tries again")
            };
            waits.push(until.duration_since(at(0)).as_millis());
            step = call.handle(Input::Waited, &mut cluster, until);
        }

        assert_eq!(waits, [100, 200, 250]);
    }

    #[test]
    fn an_operation_that_asks_again_sends_at_once_and_uses_no_retry() {
        let mut cluster = view();
        let mut limits = limits();
        limits.retries = Some(0);
        let operation = Ping {
            asks_again: true,
            ..Ping::to(Route::AnyBroker)
        };
        let mut call = Call::new(operation, limits, at(0));
        let first = exchange_of(call.handle(Input::Started, &mut cluster, at(0)));

        let input = answered(&call, first, &pong());
        let second = exchange_of(call.handle(input, &mut cluster, at(1)));

        let input = answered(&call, second, &pong());
        assert_eq!(call.handle(input, &mut cluster, at(2)), Step::Done(Ok("pong")));
    }

    #[test]
    fn a_request_the_broker_speaks_no_version_of_ends_the_call() {
        let mut cluster = view();
        let mut call = Call::new(Ping::to(Route::AnyBroker), limits(), at(0));
        let sending = exchange_of(call.handle(Input::Started, &mut cluster, at(0)));
        let mut conversation = Conversation::with(BrokerVersions::default());

        let unsupported = call.request(&sending, &mut conversation).unwrap_err();
        let step = call.handle(Input::Failed(sending, unsupported), &mut cluster, at(0));

        assert_eq!(
            step,
            Step::Done(Err(ClientError::UnsupportedVersion {
                api_key: ApiKey::Heartbeat as i16
            }))
        );
    }
}
