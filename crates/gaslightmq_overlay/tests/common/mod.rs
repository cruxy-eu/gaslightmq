//! What the tests of the overlay share: a real cluster of three brokers behind three listeners, and the wire between them.
//! A test file that includes this module uses all of it, so nothing here is dead code in any of them.
//!
//! The real brokers are `kafka-1.internal:9092`, `kafka-2.internal:9092` and `kafka-3.internal:9092`.
//! Their listeners are `gaslight.staging:19092`, `gaslight.staging:19093` and `gaslight.staging:19094`.
//!
//! No test hands the overlay a message that it built in memory.
//! Every request and every response is encoded, cut out of the bytes of a connection, and decoded again,
//! as the backend does with what a client and the cluster send.

#![expect(
    clippy::unwrap_used,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

use gaslightmq_kernel::prelude::*;
use gaslightmq_overlay::prelude::*;
use gaslightmq_wire::messages::metadata_response::MetadataResponseBroker;
use gaslightmq_wire::messages::{MetadataRequest, MetadataResponse};
use gaslightmq_wire::protocol::{Decodable, Encodable, Request as KafkaRequest, StrBytes};
use gaslightmq_wire::{FrameDecoder, Request, Response};

/// The host of every listener, as clients see it.
pub const ADVERTISED_HOST: &str = "gaslight.staging";

/// The port of every real broker.
pub const REAL_PORT: i32 = 9092;

/// The IDs of the brokers of the real cluster.
pub const BROKERS: [u16; 3] = [1, 2, 3];

/// The host of a real broker, such as `kafka-2.internal`.
pub fn real_host(broker: u16) -> String {
    format!("kafka-{broker}.internal")
}

/// The port of the listener of a broker: 19092 for broker 1, and one more for each broker after it.
pub fn advertised_port(broker: u16) -> u16 {
    19091 + broker
}

/// The address clients are told for a broker, such as `gaslight.staging:19093`.
pub fn advertised_address(broker: u16) -> BrokerAddress {
    BrokerAddress::new(ADVERTISED_HOST, advertised_port(broker)).unwrap()
}

/// A text as Kafka messages hold it.
pub fn text(text: &str) -> StrBytes {
    StrBytes::from_string(text.to_owned())
}

/// A request as the proxy reads it from the connection of a client.
pub fn from_a_client<B: KafkaRequest>(version: i16, correlation_id: i32, body: &B) -> Request {
    let sent = Request::new(version, correlation_id, Some("order-ingest"), body).unwrap();
    let mut connection = FrameDecoder::new();
    connection.extend(&sent.frame().to_bytes());
    Request::decode(connection.next_frame().unwrap().unwrap()).unwrap()
}

/// The answer to `request` as the backend reads it from its connection to the real cluster.
pub fn from_the_cluster<B: Encodable>(request: &Request, body: &B) -> Response {
    over_the_wire(&request.response(body).unwrap())
}

/// The body of `response` as a client reads it from the connection of the proxy.
pub fn read_by_a_client<B: Decodable>(response: &Response) -> B {
    over_the_wire(response).body().unwrap()
}

/// A response after one trip over a connection: its bytes, cut into a frame and decoded for the same API.
fn over_the_wire(response: &Response) -> Response {
    let mut connection = FrameDecoder::new();
    connection.extend(&response.frame().to_bytes());
    Response::decode(response.api(), connection.next_frame().unwrap().unwrap()).unwrap()
}

/// An overlay that advertises the three brokers, and that read their real addresses from the cluster.
pub fn overlay() -> Overlay {
    let advertised = BROKERS.map(|broker| (BrokerId::from(broker), advertised_address(broker)));
    let mut overlay = Overlay::new(AdvertisedAddresses::of(advertised).unwrap());

    let brokers = BROKERS.map(|broker| {
        MetadataResponseBroker::default()
            .with_node_id(i32::from(broker).into())
            .with_host(text(&real_host(broker)))
            .with_port(REAL_PORT)
    });
    let discovery = from_a_client(12, 1, &MetadataRequest::default());
    let cluster = from_the_cluster(&discovery, &MetadataResponse::default().with_brokers(brokers.to_vec()));
    overlay.learn_from(&cluster).unwrap();
    overlay
}
