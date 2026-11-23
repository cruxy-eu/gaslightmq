//! The prelude holds every item of the crate, and its names clash with none of the preludes next to it.

/// Imports the prelude next to the standard prelude, the modules of `std` that code imports most,
/// and the preludes of the fields and of the kernel.
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

    use gaslightmq_field::prelude::*;
    use gaslightmq_kernel::prelude::*;
    use gaslightmq_listener::prelude::*;

    /// A handler that reports what its transport knows, and what it receives first.
    pub struct Reporter(pub tokio::sync::mpsc::UnboundedSender<String>);

    impl ConnectionHandler for Reporter {
        async fn handle(&self, mut transport: Transport) {
            let facts = format!(
                "{} from {} to {}",
                transport.number(),
                transport.address(),
                transport.endpoint()
            );
            let received: std::result::Result<gaslightmq_wire::Frame, ListenerError> = transport.receive().await;
            let sent: std::result::Result<(), ListenerError> = transport.send(&[0, 0, 0, 1, 0]).await;
            let _reported = self.0.send(format!("{facts}: {received:?}, {sent:?}"));
        }
    }

    /// One value of every type of the prelude, each named without a path.
    pub async fn every_item() -> Vec<String> {
        let endpoint = Endpoint::Broker(BrokerId::from(2));
        let mut configuration: ListenerConfiguration =
            ListenerConfiguration::new("127.0.0.1:0".parse().expect("an address"), endpoint);
        let limit: FrameSizeLimit = FrameSizeLimit::new(1024).expect("a limit");
        configuration.socket_request_max_bytes = limit;
        let numbers: ConnectionNumbers = ConnectionNumbers::new();
        let (reports, mut reported) = tokio::sync::mpsc::unbounded_channel();
        let listener: Listener = Listener::bind(configuration.clone(), numbers.clone(), Reporter(reports))
            .await
            .expect("a free port takes a listener");
        let address = listener.address();

        // A client that connects and leaves: the handler reports the connection.
        drop(
            tokio::net::TcpStream::connect(address)
                .await
                .expect("the listener accepts"),
        );
        let report = reported.recv().await.expect("a report");
        listener.close().await;

        vec![
            format!("{configuration:?}"),
            limit.to_string(),
            numbers.assigned().to_string(),
            address.to_string(),
            report,
        ]
    }

    /// One error of every type of the prelude, each named without a path.
    ///
    /// A message says everything, so no error has a source.
    pub fn every_error() -> Vec<String> {
        /// What an error says. A message says everything, so the error has no source.
        fn message_of<E: std::error::Error>(error: E) -> String {
            assert!(error.source().is_none(), "{error}");
            error.to_string()
        }

        vec![
            message_of(FrameSizeLimitError::Zero),
            message_of(ListenerError::CannotListen {
                address: "127.0.0.1:9092".parse().expect("an address"),
                reason: "address in use".to_owned(),
            }),
            message_of(ListenerError::FrameCutShort { buffered: 6 }),
            message_of(ListenerError::ConnectionLost {
                reason: "reset by peer".to_owned(),
            }),
        ]
    }
}

#[tokio::test]
async fn the_prelude_names_every_item_next_to_the_other_preludes() {
    let items = next_to_the_others::every_item().await;

    assert_eq!(items.len(), 5);
    assert!(items.iter().all(|item| !item.is_empty()));
    assert!(items[4].starts_with("#1 from 127.0.0.1:"), "{}", items[4]);
    assert!(items[4].contains(" to broker 2: Err(ClientClosed), "), "{}", items[4]);
}

#[test]
fn the_prelude_names_the_error_of_every_type() {
    assert_eq!(
        next_to_the_others::every_error(),
        [
            "a frame size limit is at least one byte",
            "cannot listen on 127.0.0.1:9092: address in use",
            "the client closed the connection 6 bytes into a frame",
            "the connection to the client broke: reset by peer",
        ]
    );
}

#[tokio::test]
async fn the_prelude_is_enough_to_open_a_listener() {
    use gaslightmq_field::prelude::*;
    use gaslightmq_listener::prelude::*;

    struct Leave;

    impl ConnectionHandler for Leave {
        async fn handle(&self, transport: Transport) {
            transport.close().await;
        }
    }

    let configuration = ListenerConfiguration::new("127.0.0.1:0".parse().unwrap(), Endpoint::Bootstrap);
    let listener = Listener::bind(configuration, ConnectionNumbers::new(), Leave)
        .await
        .unwrap();

    assert_ne!(listener.address().port(), 0);
    listener.close().await;
}
