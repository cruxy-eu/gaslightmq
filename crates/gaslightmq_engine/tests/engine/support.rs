//! What the tests share, as three types.
//!
//! | Type | Is |
//! |---|---|
//! | [`ScriptedBackend`] | The backend of an engine under test: a cluster of three brokers in memory, which does what a test tells it. |
//! | [`EngineUnderTest`] | An engine in front of such a backend, with the clock that the test steps and the events it handed over. |
//! | [`ClientSocket`] | The connection of a client that does what no Kafka client does: it sends without waiting, sends bytes that are no request, and watches the proxy close the connection. |
//!
//! A test that sends a request and looks at its answer uses a real client:
//! the [`RawKafkaClient`], which sends one request at a fixed version.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use gaslightmq_backend::prelude::*;
use gaslightmq_call::prelude::*;
use gaslightmq_clock::TimeSpan;
use gaslightmq_engine::prelude::*;
use gaslightmq_environment::prelude::*;
use gaslightmq_field::prelude::*;
use gaslightmq_incident::prelude::*;
use gaslightmq_kafka_client_api::RawKafkaClient;
use gaslightmq_kernel::prelude::*;
use gaslightmq_lie_api::prelude::*;
use gaslightmq_lie_refusal::prelude::*;
use gaslightmq_rule::prelude::*;
use gaslightmq_selector::prelude::*;
use gaslightmq_wire::messages::produce_request::{PartitionProduceData, TopicProduceData};
use gaslightmq_wire::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
use gaslightmq_wire::messages::{
    HeartbeatRequest, HeartbeatResponse, MetadataRequest, MetadataResponse, ProduceRequest, ProduceResponse,
};
use gaslightmq_wire::protocol::{Request as KafkaRequest, StrBytes};
use gaslightmq_wire::{ApiKey as KnownApi, FrameDecoder, Record, RecordBatch, Request, Response};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::task::JoinHandle;

/// How long a test waits for something that has to happen. It happens at once, unless the test fails.
const PATIENCE: Duration = Duration::from_secs(10);

/// How many times a test looks for three free ports in a row before it gives up.
const RANGES_TO_TRY: usize = 20;

/// How long a test watches for something that must not happen.
const A_MOMENT: Duration = Duration::from_millis(50);

/// The backend of an engine under test: a cluster of three brokers in memory, which does what a test tells it.
///
/// It answers every request truthfully and at once, until a test tells it otherwise.
/// It also keeps what the engine did with it: the calls it started, the connections it said have ended,
/// and the addresses it said its listeners got.
#[derive(Debug)]
pub struct ScriptedBackend {
    endpoints: watch::Sender<Endpoints>,
    seen: watch::Sender<Seen>,
    /// While this is true, no call gets its answer.
    holding: watch::Sender<bool>,
    /// What every call ends with, instead of the truth.
    instead: watch::Sender<Option<BackendAnswer>>,
    /// The brokers whose listeners the backend rejects.
    rejecting: watch::Sender<Vec<BrokerId>>,
    /// While this is true, the backend never says whether it takes a listener.
    holding_listeners: watch::Sender<bool>,
    /// While this is true, the backend never lets go of a connection that ended.
    holding_connections: watch::Sender<bool>,
}

/// What a backend saw of its engine.
#[derive(Debug, Clone, Default)]
struct Seen {
    /// The calls that started, in order.
    taken: Vec<CallId>,
    /// The connections that the engine said have ended, in order.
    closed: Vec<ConnectionNumber>,
    /// Where the engine said its listeners listen, in order.
    listeners: Vec<(Endpoint, SocketAddr)>,
}

impl ScriptedBackend {
    /// A cluster of the brokers 1, 2 and 3, each with its endpoint open, next to the bootstrap endpoint.
    pub fn with_three_brokers() -> Self {
        let brokers = Self::brokers().map(Endpoint::Broker);
        let endpoints = std::iter::once(Endpoint::Bootstrap).chain(brokers);
        Self {
            endpoints: watch::Sender::new(endpoints.collect()),
            seen: watch::Sender::default(),
            holding: watch::Sender::new(false),
            instead: watch::Sender::new(None),
            rejecting: watch::Sender::default(),
            holding_listeners: watch::Sender::new(false),
            holding_connections: watch::Sender::new(false),
        }
    }

    /// The brokers of the cluster.
    pub fn brokers() -> [BrokerId; 3] {
        [1u16, 2, 3].map(BrokerId::from)
    }

    /// The offset at which the cluster says it wrote the records of every Produce.
    pub fn written_at() -> gaslightmq_wire::Offset {
        gaslightmq_wire::Offset::new(100)
    }

    /// No call gets its answer from now on, until [`release_answers`](Self::release_answers).
    pub fn hold_answers(&self) {
        self.holding.send_replace(true);
    }

    /// Every call that waits gets its answer, and so does every call from now on.
    pub fn release_answers(&self) {
        self.holding.send_replace(false);
    }

    /// Every call ends with `failure` from now on.
    pub fn fail_calls_with(&self, failure: BackendFailure) {
        self.instead.send_replace(Some(failure.into()));
    }

    /// Every call ends with an answer that is no response from now on.
    pub fn answer_with_what_is_no_response(&self) {
        let reason = "response header is cut short at 3 bytes".to_owned();
        self.instead.send_replace(Some(BackendAnswer::NotAResponse { reason }));
    }

    /// Every call ends with the truth again from now on.
    pub fn answer_truthfully(&self) {
        self.instead.send_replace(None);
    }

    /// The broker goes down, or leaves the cluster: its endpoint closes.
    pub fn take_down(&self, broker: BrokerId) {
        self.endpoints
            .send_modify(|endpoints| endpoints.close(Endpoint::Broker(broker)));
    }

    /// The broker is back: its endpoint opens.
    pub fn bring_back(&self, broker: BrokerId) {
        self.endpoints
            .send_modify(|endpoints| endpoints.open(Endpoint::Broker(broker)));
    }

    /// The broker goes down and comes back before the engine looks: its endpoint closes, and opens again.
    pub fn bounce(&self, broker: BrokerId) {
        self.endpoints.send_modify(|endpoints| {
            endpoints.close(Endpoint::Broker(broker));
            endpoints.open(Endpoint::Broker(broker));
        });
    }

    /// The backend rejects every listener of the broker from now on.
    pub fn reject_listeners_of(&self, broker: BrokerId) {
        self.rejecting.send_modify(|rejecting| rejecting.push(broker));
    }

    /// The backend hears where a listener listens from now on, and never says whether it takes it.
    pub fn hold_listeners(&self) {
        self.holding_listeners.send_replace(true);
    }

    /// The backend hears that a connection ended from now on, and never lets go of it.
    pub fn hold_connections(&self) {
        self.holding_connections.send_replace(true);
    }

    /// The backend takes the listeners of the broker again from now on.
    pub fn accept_listeners_of(&self, broker: BrokerId) {
        self.rejecting
            .send_modify(|rejecting| rejecting.retain(|rejected| *rejected != broker));
    }

    /// Waits until `count` calls started, and returns every call that did, in order.
    pub async fn calls_taken(&self, count: usize) -> anyhow::Result<Vec<CallId>> {
        let seen = self.saw(|seen| seen.taken.len() >= count).await;
        Ok(seen.context("the engine starts the calls")?.taken)
    }

    /// Every call that started so far, in order.
    pub fn calls_taken_so_far(&self) -> Vec<CallId> {
        self.seen.borrow().taken.clone()
    }

    /// Waits until the engine said of `count` connections that they ended, and returns them, in order.
    pub async fn connections_closed(&self, count: usize) -> anyhow::Result<Vec<ConnectionNumber>> {
        let seen = self.saw(|seen| seen.closed.len() >= count).await;
        Ok(seen.context("the engine says that the connections ended")?.closed)
    }

    /// The address on which the engine said last that `endpoint` listens.
    pub fn listener_of(&self, endpoint: Endpoint) -> Option<SocketAddr> {
        let seen = self.seen.borrow();
        let of_the_endpoint = seen.listeners.iter().rev().find(|(told, _)| *told == endpoint);
        of_the_endpoint.map(|(_, address)| *address)
    }

    /// Waits until the engine said `count` times where `endpoint` listens, and returns the addresses, in order.
    pub async fn listeners_of(&self, endpoint: Endpoint, count: usize) -> anyhow::Result<Vec<SocketAddr>> {
        let of_the_endpoint = |seen: &Seen| -> Vec<SocketAddr> {
            let told = seen.listeners.iter().filter(|(told, _)| *told == endpoint);
            told.map(|(_, address)| *address).collect()
        };
        let seen = self.saw(|seen| of_the_endpoint(seen).len() >= count).await;
        Ok(of_the_endpoint(
            &seen.context("the engine says where the endpoint listens")?,
        ))
    }

    /// Waits until the backend saw `enough` of its engine.
    async fn saw(&self, enough: impl FnMut(&Seen) -> bool) -> anyhow::Result<Seen> {
        let mut seen = self.seen.subscribe();
        let seen = tokio::time::timeout(PATIENCE, seen.wait_for(enough)).await??;
        Ok(seen.clone())
    }

    /// What the cluster answers `request`, when nothing is wrong with it.
    fn truth(request: &Request) -> BackendAnswer {
        let response = match request.api().known() {
            Some(KnownApi::Produce) => {
                let asked: ProduceRequest = match request.body() {
                    Ok(asked) => asked,
                    Err(error) => return Self::not_built(&error),
                };
                if asked.acks == Acks::NONE.value() {
                    return BackendAnswer::NoResponse;
                }
                let topics = asked.topic_data.iter().map(|topic| {
                    let partitions = topic.partition_data.iter().map(|partition| {
                        PartitionProduceResponse::default()
                            .with_index(partition.index)
                            .with_base_offset(i64::from(Self::written_at()))
                    });
                    TopicProduceResponse::default()
                        .with_name(topic.name.clone())
                        .with_partition_responses(partitions.collect())
                });
                request.response(&ProduceResponse::default().with_responses(topics.collect()))
            }
            Some(KnownApi::Metadata) => request.response(&MetadataResponse::default()),
            _ => request.response(&HeartbeatResponse::default()),
        };
        match response {
            Ok(response) => Truth::new(response).into(),
            Err(error) => Self::not_built(&error),
        }
    }

    /// The answer of a cluster that could not build its response, for the reason the codec gave.
    fn not_built(error: &impl std::fmt::Display) -> BackendAnswer {
        let reason = error.to_string();
        BackendFailure::ResponseNotBuilt { reason }.into()
    }
}

impl Backend for ScriptedBackend {
    async fn call(&self, request: BackendRequest) -> BackendAnswer {
        self.seen.send_modify(|seen| seen.taken.push(request.call()));
        let mut holding = self.holding.subscribe();
        // The backend holds the script for as long as it lives, so the wait only ends when the answers are let go.
        let _ = holding.wait_for(|holding| !holding).await;
        let instead = self.instead.borrow().clone();
        instead.unwrap_or_else(|| Self::truth(request.request()))
    }

    async fn close(&self, connection: ConnectionNumber) {
        self.seen.send_modify(|seen| seen.closed.push(connection));
        let mut holding = self.holding_connections.subscribe();
        // The backend holds the script for as long as it lives, so the wait only ends when the test lets go.
        let _ = holding.wait_for(|holding| !holding).await;
    }

    async fn endpoints(&self, known: &Endpoints) -> Endpoints {
        let mut endpoints = self.endpoints.subscribe();
        match endpoints.wait_for(|endpoints| endpoints != known).await {
            Ok(differing) => differing.clone(),
            // The backend holds its endpoints for as long as it lives, so they never go away.
            Err(_) => known.clone(),
        }
    }

    async fn listens(&self, endpoint: Endpoint, address: SocketAddr) -> Result<(), ListenerRejection> {
        self.seen.send_modify(|seen| seen.listeners.push((endpoint, address)));
        let mut holding = self.holding_listeners.subscribe();
        // The backend holds the script for as long as it lives, so the wait only ends when the test lets go.
        let _ = holding.wait_for(|holding| !holding).await;
        let rejected = endpoint
            .broker()
            .is_some_and(|broker| self.rejecting.borrow().contains(&broker));
        if rejected {
            return Err(ListenerRejection::Unadvertisable {
                endpoint,
                address,
                reason: "the script of the test rejects the listeners of this broker".to_owned(),
            });
        }
        Ok(())
    }

    fn incident_support(&self) -> IncidentSupport {
        IncidentSupport::none()
    }

    fn apply(&self, incident: Incident) -> impl Future<Output = Result<ExpandedIncident, IncidentRejection>> + Send {
        let unsupported = self.incident_support().check(&Requirements::from(&incident));
        std::future::ready(Err(unsupported.expect_err("the cluster supports no incident").into()))
    }
}

/// An engine under test: in front of a [`ScriptedBackend`], on a clock that the test steps.
///
/// It keeps a listener of every event from the moment the engine started, so a test reads what the sessions
/// handed over in order, and misses nothing.
pub struct EngineUnderTest {
    engine: Engine<ScriptedBackend, SteppedClock>,
    clock: SteppedClock,
    events: EveryEvent,
}

impl EngineUnderTest {
    /// The configuration of an engine whose endpoints each take a free port of this machine:
    /// the bootstrap endpoint, and the one of every broker of a [`ScriptedBackend`].
    pub fn configuration() -> anyhow::Result<EngineConfiguration> {
        let any: ListenAddress = "127.0.0.1:0".parse()?;
        let brokers = BrokerListeners::pinned(ScriptedBackend::brokers().map(|broker| (broker, any)))?;
        let listeners = Listeners::new(any, brokers, "localhost".parse()?)?;
        Ok(EngineConfiguration::new(listeners))
    }

    /// Starts an engine in front of `backend`, with [that configuration](Self::configuration).
    pub async fn start(backend: ScriptedBackend) -> anyhow::Result<Self> {
        Self::start_with(Self::configuration()?, backend).await
    }

    /// Starts an engine in front of `backend`, with `configuration`.
    ///
    /// Its sessions tell the lies of every build, and the refusal.
    pub async fn start_with(configuration: EngineConfiguration, backend: ScriptedBackend) -> anyhow::Result<Self> {
        Ok(Self::start_telling(Self::lies()?, configuration, backend).await?)
    }

    /// Starts an engine in front of a cluster of three brokers, which listen on three ports in a row,
    /// and returns the first of those ports.
    ///
    /// A range gives its ports in the order of the broker IDs, so a test knows which broker gets which port,
    /// and a broker that leaves gives the brokers behind it another one.
    /// The ports are free when the test looks, and another test can take one before the engine does.
    /// The engine then says that an address is unavailable, and the test looks for other ports.
    pub async fn start_on_a_range_of_ports() -> anyhow::Result<(Self, u16)> {
        for _ in 0..RANGES_TO_TRY {
            let Some((configuration, first_port)) = Self::free_ports_in_a_row()? else {
                continue;
            };
            let backend = ScriptedBackend::with_three_brokers();
            match Self::start_telling(Self::lies()?, configuration, backend).await {
                Ok(started) => return Ok((started, first_port)),
                Err(EngineStartError::EndpointNotOpened {
                    error: EndpointOpenError::AddressUnavailable { .. },
                }) => {}
                Err(error) => return Err(error.into()),
            }
        }
        anyhow::bail!("no three ports in a row stayed free in {RANGES_TO_TRY} tries")
    }

    /// The lies that the sessions of an engine under test tell: the ones of every build, and the refusal.
    fn lies() -> anyhow::Result<Lies> {
        Ok(Lies::of_every_build().with(Refusal)?)
    }

    /// Starts an engine whose sessions tell `lies`, on a clock that the test steps.
    async fn start_telling(
        lies: Lies,
        configuration: EngineConfiguration,
        backend: ScriptedBackend,
    ) -> Result<Self, EngineStartError> {
        let clock = SteppedClock::new();
        let engine = Engine::start(configuration, backend, clock.clone(), lies).await?;
        let events = engine.every_event();
        Ok(Self { engine, clock, events })
    }

    /// Looks for three ports in a row that are free now, and returns the configuration of an engine whose brokers
    /// listen on them, with the first of those ports. Returns `None` when the ports it looked at are taken.
    fn free_ports_in_a_row() -> anyhow::Result<Option<(EngineConfiguration, u16)>> {
        let first = std::net::TcpListener::bind("127.0.0.1:0")?;
        let port = first.local_addr()?.port();
        let Some(last) = port.checked_add(2) else {
            return Ok(None);
        };
        let rest = [port + 1, last].map(|port| std::net::TcpListener::bind(("127.0.0.1", port)));
        if rest.iter().any(Result::is_err) {
            return Ok(None);
        }
        let brokers: BrokerListeners = format!("127.0.0.1:{port}-{last}").parse()?;
        let listeners = Listeners::new("127.0.0.1:0".parse()?, brokers, "localhost".parse()?)?;
        Ok(Some((EngineConfiguration::new(listeners), port)))
    }

    /// The engine itself.
    pub fn engine(&self) -> &Engine<ScriptedBackend, SteppedClock> {
        &self.engine
    }

    /// The backend of the engine, for the test to tell it what to do and to ask it what it saw.
    pub fn backend(&self) -> &ScriptedBackend {
        self.engine.backend()
    }

    /// Arms `rule`, above every rule that is armed. The rule names no selector and no preset of a workspace.
    pub fn arm(&self, rule: &Rule) -> anyhow::Result<ArmedRule> {
        let resolved = rule.resolve(&SelectorSet::default(), &PresetSet::default())?;
        Ok(self.engine.rules().arm(resolved)?)
    }

    /// The names of the armed rules, in the order in which they win a conflict.
    pub fn armed_rules(&self) -> Vec<RuleName> {
        let snapshot = self.engine.rules().snapshot();
        snapshot.iter().map(|rule| rule.name().clone()).collect()
    }

    /// Moves the clock of the engine by `span`. No real time passes.
    pub fn advance(&self, span: TimeSpan) {
        self.clock.advance(span.duration());
    }

    /// The address on which `endpoint` listens.
    pub fn address_of(&self, endpoint: Endpoint) -> anyhow::Result<SocketAddr> {
        let address = self.engine.address_of(endpoint);
        address.with_context(|| format!("endpoint `{endpoint}` is not open"))
    }

    /// A Kafka client that sends one request at a time to `endpoint`. It connects when it first sends.
    pub fn client_on(&self, endpoint: Endpoint) -> anyhow::Result<RawKafkaClient> {
        Ok(RawKafkaClient::new(self.address_of(endpoint)?))
    }

    /// The connection of a client to `endpoint`, for what no Kafka client does.
    pub async fn socket_on(&self, endpoint: Endpoint) -> anyhow::Result<ClientSocket> {
        ClientSocket::connect(self.address_of(endpoint)?).await
    }

    /// Watches a call of a client for a moment, and says whether it is still without an answer.
    pub async fn leaves_unanswered<T>(&self, call: &mut JoinHandle<T>) -> bool {
        tokio::time::timeout(A_MOMENT, call).await.is_err()
    }

    /// Waits for the next event that the sessions handed over.
    pub async fn next_event(&mut self) -> anyhow::Result<EngineEvent> {
        let event = tokio::time::timeout(PATIENCE, self.events.next()).await;
        event
            .context("the engine hands an event over")?
            .context("the engine stopped")
    }

    /// Waits for the record of the next call, and passes over what the sessions handed over before it.
    pub async fn next_record(&mut self) -> anyhow::Result<Arc<Call>> {
        loop {
            if let EngineEvent::CallRecorded(record) = self.next_event().await? {
                return Ok(record);
            }
        }
    }

    /// Stops the engine, and waits until every session has ended.
    pub async fn stop(self) {
        self.engine.stop().await;
    }
}

/// The connection of a client that does what no Kafka client does.
///
/// | It | Where a Kafka client |
/// |---|---|
/// | Sends a request, and goes on without its answer. | Waits for the answer. |
/// | Sends bytes that are no request. | Only sends requests. |
/// | Watches the proxy close the connection. | Opens a new one. |
///
/// It builds the requests it sends itself, each at one fixed version, and counts their correlation IDs.
pub struct ClientSocket {
    stream: TcpStream,
    /// The bytes of the proxy that no frame has taken yet.
    frames: FrameDecoder,
    /// How many requests the client sent, which is the correlation ID of the last one.
    sent: i32,
}

/// A request that a [`ClientSocket`] sent, which its answer is read by.
pub struct Sent(Request);

impl ClientSocket {
    /// The version of every Heartbeat: the first one of the API.
    const HEARTBEAT_VERSION: i16 = 0;
    /// The version of every Metadata request: the first one that names the controller.
    const METADATA_VERSION: i16 = 1;
    /// The version of every Produce: the first one with record batches.
    const PRODUCE_VERSION: i16 = 3;
    /// The name under which the client sends its requests.
    const CLIENT_ID: &'static str = "order-ingest";

    /// Connects to `address`.
    pub async fn connect(address: SocketAddr) -> anyhow::Result<Self> {
        Ok(Self {
            stream: TcpStream::connect(address).await?,
            frames: FrameDecoder::new(),
            sent: 0,
        })
    }

    /// Sends a Heartbeat of a member of `group`.
    pub async fn send_heartbeat(&mut self, group: &GroupId) -> anyhow::Result<Sent> {
        let group = StrBytes::from_string(group.as_str().to_owned());
        let body = HeartbeatRequest::default().with_group_id(group.into());
        self.send(Self::HEARTBEAT_VERSION, &body).await
    }

    /// Sends a Metadata request for every topic.
    pub async fn send_metadata(&mut self) -> anyhow::Result<Sent> {
        self.send(Self::METADATA_VERSION, &MetadataRequest::default()).await
    }

    /// Sends a Produce of `record` to `partition`, which asks for `acks`.
    pub async fn send_produce(
        &mut self,
        partition: &TopicPartition,
        record: Record,
        acks: Acks,
    ) -> anyhow::Result<Sent> {
        let records = RecordBatch::new([record]).encode()?;
        let data = PartitionProduceData::default()
            .with_index(i32::from(partition.partition()))
            .with_records(Some(records));
        let topic = TopicProduceData::default()
            .with_name(StrBytes::from_string(partition.topic().as_str().to_owned()).into())
            .with_partition_data(vec![data]);
        let body = ProduceRequest::default()
            .with_acks(acks.value())
            .with_topic_data(vec![topic]);
        self.send(Self::PRODUCE_VERSION, &body).await
    }

    /// Sends the start of a frame that holds no bytes, which is no request.
    pub async fn send_frame_of_no_bytes(&mut self) -> anyhow::Result<()> {
        self.announce_frame_of(0).await
    }

    /// Sends the start of a frame that announces `bytes` bytes, and none of them.
    pub async fn announce_frame_of(&mut self, bytes: u32) -> anyhow::Result<()> {
        Ok(self.stream.write_all(&bytes.to_be_bytes()).await?)
    }

    /// Waits for the next response of the proxy, which answers `sent`.
    ///
    /// # Errors
    ///
    /// When the proxy closes the connection or sends nothing in time, and when the response answers another request.
    pub async fn receive_answer_to(&mut self, sent: &Sent) -> anyhow::Result<()> {
        let receiving = async {
            loop {
                if let Some(frame) = self.frames.next_frame()? {
                    return anyhow::Ok(frame);
                }
                let read = self.stream.read_buf(self.frames.buffer_mut()).await?;
                anyhow::ensure!(read > 0, "the proxy closed the connection before it answered");
            }
        };
        let frame = tokio::time::timeout(PATIENCE, receiving).await;
        let response = Response::decode(sent.0.api(), frame.context("the proxy answers")??)?;
        anyhow::ensure!(
            response.correlation_id() == sent.0.correlation_id(),
            "the proxy answered request {} where the answer to request {} is next",
            response.correlation_id(),
            sent.0.correlation_id()
        );
        Ok(())
    }

    /// Watches the connection for a moment, in which nothing arrives and it stays open.
    ///
    /// # Errors
    ///
    /// When the proxy sends a byte or closes the connection in that moment.
    pub async fn receive_nothing(&mut self) -> anyhow::Result<()> {
        let read = tokio::time::timeout(A_MOMENT, self.stream.read_buf(self.frames.buffer_mut())).await;
        anyhow::ensure!(read.is_err(), "the proxy did not stay silent: {read:?}");
        Ok(())
    }

    /// Waits until the proxy closed the connection.
    ///
    /// # Errors
    ///
    /// When the proxy sends a byte first, and when it leaves the connection open.
    pub async fn wait_until_closed(&mut self) -> anyhow::Result<()> {
        let read = tokio::time::timeout(PATIENCE, self.stream.read_buf(self.frames.buffer_mut())).await;
        let read = read.context("the proxy closes the connection")?;
        // A connection that was reset is as closed as one that ended.
        anyhow::ensure!(matches!(read, Ok(0) | Err(_)), "the proxy sent {read:?} bytes");
        anyhow::ensure!(
            self.frames.buffered() == 0,
            "the proxy sent bytes before it closed the connection"
        );
        Ok(())
    }

    /// Sends `body` as the next request of the client, at `version`.
    async fn send<B: KafkaRequest>(&mut self, version: i16, body: &B) -> anyhow::Result<Sent> {
        self.sent += 1;
        let request = Request::new(version, self.sent, Some(Self::CLIENT_ID), body)?;
        self.stream.write_all(&request.frame().to_bytes()).await?;
        Ok(Sent(request))
    }
}
