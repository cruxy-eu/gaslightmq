//! The client, the handler and the listener end a connection.

use std::sync::Arc;
use std::time::Duration;

use gaslightmq_clock::TimeSpan;
use gaslightmq_field::prelude::*;
use gaslightmq_listener::prelude::*;
use tokio::sync::{Notify, mpsc};

use crate::support::{Client, Echo, frame, listen, on_a_free_port, soon};

/// Answers the first frame, and closes the connection.
struct AnswerOnce;

impl ConnectionHandler for AnswerOnce {
    async fn handle(&self, mut transport: Transport) {
        if let Ok(frame) = transport.receive().await
            && transport.send(&frame.to_bytes()).await.is_ok()
        {
            transport.close().await;
        }
    }
}

/// Lets go of every connection at once.
struct Leave;

impl ConnectionHandler for Leave {
    async fn handle(&self, transport: Transport) {
        drop(transport);
    }
}

/// Waits until the client left, and then sends until the connection takes no more. It reports why.
struct SendToNobody {
    failed: mpsc::UnboundedSender<SendError>,
}

impl ConnectionHandler for SendToNobody {
    async fn handle(&self, mut transport: Transport) {
        while transport.receive().await.is_ok() {}
        let bytes = frame(b"is anybody there").to_bytes();
        loop {
            if let Err(error) = transport.send(&bytes).await {
                let _reported = self.failed.send(error);
                return;
            }
            tokio::task::yield_now().await;
        }
    }
}

/// Panics on the frame `panic`, and sends every other frame back.
struct PanicOnDemand;

impl ConnectionHandler for PanicOnDemand {
    async fn handle(&self, mut transport: Transport) {
        while let Ok(frame) = transport.receive().await {
            assert_ne!(frame.payload().as_ref(), b"panic", "the test asked for it");
            if transport.send(&frame.to_bytes()).await.is_err() {
                return;
            }
        }
    }
}

/// Reads one frame, waits for the word of the test, and then reads on. It reports everything it receives.
struct ReadLate {
    go: Notify,
    seen: mpsc::UnboundedSender<Result<Vec<u8>, ReceiveError>>,
}

impl ConnectionHandler for ReadLate {
    async fn handle(&self, mut transport: Transport) {
        let mut waited = false;
        loop {
            let received = transport.receive().await.map(|frame| frame.payload().to_vec());
            let ended = received.is_err();
            let _reported = self.seen.send(received);
            if ended {
                return;
            }
            if !waited {
                self.go.notified().await;
                waited = true;
            }
        }
    }
}

/// Sends without end, and reports why it stopped.
struct SendWithoutEnd {
    stopped: mpsc::UnboundedSender<SendError>,
}

impl ConnectionHandler for SendWithoutEnd {
    async fn handle(&self, mut transport: Transport) {
        let bytes = vec![0; 64 * 1024];
        loop {
            if let Err(error) = transport.send(&bytes).await {
                let _reported = self.stopped.send(error);
                return;
            }
        }
    }
}

/// Reads one frame, and waits for the word of the test. Then it sends its answer and closes the connection.
/// It says how far it is.
struct AnswerLateAndClose {
    go: Notify,
    answer: Vec<u8>,
    progress: mpsc::UnboundedSender<&'static str>,
}

impl ConnectionHandler for AnswerLateAndClose {
    async fn handle(&self, mut transport: Transport) {
        if transport.receive().await.is_err() {
            return;
        }
        let _said = self.progress.send("received");
        self.go.notified().await;
        if transport.send(&frame(&self.answer).to_bytes()).await.is_err() {
            return;
        }
        let _said = self.progress.send("answered");
        transport.close().await;
        let _said = self.progress.send("closed");
    }
}

/// Sends every frame back. When the listener closes, it says goodbye and closes the connection.
struct EchoAndGoodbye;

impl ConnectionHandler for EchoAndGoodbye {
    async fn handle(&self, mut transport: Transport) {
        loop {
            match transport.receive().await {
                Ok(frame) => {
                    if transport.send(&frame.to_bytes()).await.is_err() {
                        return;
                    }
                }
                Err(ReceiveError::ListenerClosed) => {
                    if transport.send(&frame(b"goodbye").to_bytes()).await.is_ok() {
                        transport.close().await;
                    }
                    return;
                }
                Err(
                    ReceiveError::ClientClosed
                    | ReceiveError::FrameCutShort { .. }
                    | ReceiveError::NotAFrame { .. }
                    | ReceiveError::ConnectionLost { .. },
                ) => return,
            }
        }
    }
}

#[tokio::test]
async fn a_client_that_closes_after_a_frame_closed_the_connection() {
    let (echo, mut endings) = Echo::new();
    let listener = listen(echo).await;
    let mut client = Client::connect(listener.address()).await;
    let address = client.address();

    client.send(b"last words").await;
    assert_eq!(client.receive().await, Some(frame(b"last words")));
    drop(client);

    let ended = endings.next().await;
    assert!(matches!(ended.why, ReceiveError::ClientClosed), "{:?}", ended.why);
    assert_eq!(ended.frames, 1);
    assert_eq!(ended.number, ConnectionNumber::new(1));
    assert_eq!(ended.address, address);
    assert_eq!(ended.endpoint, Endpoint::Bootstrap);
}

#[tokio::test]
async fn a_client_that_closes_before_its_first_frame_closed_the_connection() {
    let (echo, mut endings) = Echo::new();
    let listener = listen(echo).await;

    drop(Client::connect(listener.address()).await);

    let ended = endings.next().await;
    assert!(matches!(ended.why, ReceiveError::ClientClosed), "{:?}", ended.why);
    assert_eq!(ended.frames, 0);
}

#[tokio::test]
async fn a_client_that_closes_inside_a_frame_cut_the_frame_short() {
    let (echo, mut endings) = Echo::new();
    let listener = listen(echo).await;
    let mut client = Client::connect(listener.address()).await;
    let bytes = frame(b"never finished").to_bytes();

    client.write(&bytes[..10]).await;
    drop(client);

    let ended = endings.next().await;
    assert!(
        matches!(ended.why, ReceiveError::FrameCutShort { buffered: 10 }),
        "{:?}",
        ended.why
    );
    assert_eq!(ended.frames, 0);
}

#[tokio::test]
async fn a_handler_that_closes_ends_the_connection_after_what_it_sent() {
    let listener = listen(AnswerOnce).await;
    let mut client = Client::connect(listener.address()).await;

    client.send(b"only once").await;

    assert_eq!(client.receive().await, Some(frame(b"only once")));
    assert_eq!(client.receive().await, None);
}

#[tokio::test]
async fn a_handler_that_returns_ends_the_connection() {
    let listener = listen(Leave).await;
    let mut client = Client::connect(listener.address()).await;

    assert_eq!(client.receive().await, None);
}

#[tokio::test]
async fn sending_to_a_client_that_left_fails() {
    let (failed, mut failures) = mpsc::unbounded_channel();
    let listener = listen(SendToNobody { failed }).await;

    drop(Client::connect(listener.address()).await);

    let error = soon(failures.recv()).await.unwrap();
    assert!(matches!(error, SendError::ConnectionLost { .. }), "{error:?}");
}

#[tokio::test]
async fn a_handler_that_panics_takes_down_its_own_connection_only() {
    let listener = listen(PanicOnDemand).await;
    let mut calm = Client::connect(listener.address()).await;
    let mut doomed = Client::connect(listener.address()).await;

    doomed.send(b"panic").await;
    assert_eq!(doomed.receive().await, None);

    assert_eq!(calm.ask(b"still here").await, "still here");
    let mut later = Client::connect(listener.address()).await;
    assert_eq!(later.ask(b"and accepting").await, "and accepting");
    soon(listener.close()).await;
}

#[tokio::test]
async fn closing_the_listener_ends_its_connections() {
    let (echo, mut endings) = Echo::new();
    let listener = listen(echo).await;
    let address = listener.address();
    let mut first = Client::connect(address).await;
    let mut second = Client::connect(address).await;
    assert_eq!(first.ask(b"open").await, "open");
    assert_eq!(second.ask(b"open too").await, "open too");

    // The call returns once both handlers returned.
    soon(listener.close()).await;

    for _ in 0..2 {
        let ended = endings.next().await;
        assert!(matches!(ended.why, ReceiveError::ListenerClosed), "{:?}", ended.why);
        assert_eq!(ended.frames, 1);
    }
    assert_eq!(first.receive().await, None);
    assert_eq!(second.receive().await, None);
}

#[tokio::test]
async fn a_closed_endpoint_opens_again_on_the_address_it_had() {
    let numbers = ConnectionNumbers::new();
    let (echo, _endings) = Echo::new();
    let listener = Listener::bind(on_a_free_port(Endpoint::Bootstrap), numbers.clone(), echo)
        .await
        .unwrap();
    let address = listener.address();
    let mut before = Client::connect(address).await;
    assert_eq!(before.ask(b"before").await, "before");
    soon(listener.close()).await;
    assert_eq!(before.receive().await, None);

    let (echo, mut endings) = Echo::new();
    let echo = Arc::new(echo);
    let same_address = ListenerConfiguration::new(address, Endpoint::Bootstrap);
    // The tests run side by side, and each one takes free ports.
    // Another one may hold this port for a moment, so the test tries until the port is free again.
    let again = soon(async {
        loop {
            match Listener::bind(same_address.clone(), numbers.clone(), Arc::clone(&echo)).await {
                Ok(listener) => break listener,
                Err(ListenerError::CannotListen { .. }) => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
    })
    .await;
    let mut after = Client::connect(again.address()).await;

    assert_eq!(again.address(), address);
    assert_eq!(after.ask(b"after").await, "after");
    drop(after);
    // The numbers go on where the first listener left them.
    assert_eq!(endings.next().await.number, ConnectionNumber::new(2));
}

#[tokio::test]
async fn letting_go_of_a_listener_closes_it() {
    let (echo, mut endings) = Echo::new();
    let listener = listen(echo).await;
    let address = listener.address();
    let mut client = Client::connect(address).await;
    assert_eq!(client.ask(b"open").await, "open");

    drop(listener);

    let ended = endings.next().await;
    assert!(matches!(ended.why, ReceiveError::ListenerClosed), "{:?}", ended.why);
    assert_eq!(client.receive().await, None);
}

#[tokio::test]
async fn frames_that_arrived_before_the_listener_closed_still_come_out() {
    let (seen, mut sights) = mpsc::unbounded_channel();
    let handler = Arc::new(ReadLate {
        go: Notify::new(),
        seen,
    });
    let listener = listen(Arc::clone(&handler)).await;
    let mut client = Client::connect(listener.address()).await;
    // Both frames travel in one piece, so the second one has arrived when the first one comes out.
    client
        .write(&[frame(b"first").to_bytes(), frame(b"second").to_bytes()].concat())
        .await;
    assert_eq!(soon(sights.recv()).await.unwrap().unwrap(), b"first");

    // The handler reads on only now, and by then the listener is closed.
    handler.go.notify_one();
    soon(listener.close()).await;

    assert_eq!(soon(sights.recv()).await.unwrap().unwrap(), b"second");
    let last = soon(sights.recv()).await.unwrap();
    assert!(matches!(last, Err(ReceiveError::ListenerClosed)), "{last:?}");
}

#[tokio::test]
async fn closing_the_listener_does_not_wait_for_a_client_that_stopped_reading() {
    let (stopped, mut stops) = mpsc::unbounded_channel();
    let listener = listen(SendWithoutEnd { stopped }).await;
    let _reads_nothing = Client::connect(listener.address()).await;
    // The handler fills what the system holds for the client, and waits in `send` from then on.
    tokio::time::sleep(Duration::from_millis(100)).await;

    soon(listener.close()).await;

    let error = soon(stops.recv()).await.unwrap();
    assert!(matches!(error, SendError::ListenerClosed), "{error:?}");
}

#[tokio::test]
async fn a_handler_that_closes_loses_none_of_the_bytes_it_sent() {
    let (progress, mut said) = mpsc::unbounded_channel();
    let handler = Arc::new(AnswerLateAndClose {
        go: Notify::new(),
        // Far more than the system holds for a client that reads slowly,
        // so a part of the answer is still on the listener's side when the handler closes.
        answer: (0..=250u8).cycle().take(4 * 1024 * 1024).collect(),
        progress,
    });
    let listener = listen(Arc::clone(&handler)).await;
    let mut client = Client::connect(listener.address()).await;

    client.send(b"first").await;
    assert_eq!(soon(said.recv()).await, Some("received"));
    // A second request that the handler never reads: it is unread when the handler closes.
    client.send(b"second").await;
    handler.go.notify_one();

    let answer = client.receive_slowly().await;
    assert_eq!(answer.map(|frame| frame.payload().len()), Some(handler.answer.len()));
    assert_eq!(client.receive().await, None);
    drop(client);
    assert_eq!(soon(said.recv()).await, Some("answered"));
    assert_eq!(soon(said.recv()).await, Some("closed"));
}

#[tokio::test]
async fn a_handler_says_its_last_words_after_the_listener_closed() {
    let listener = listen(EchoAndGoodbye).await;
    let mut client = Client::connect(listener.address()).await;
    assert_eq!(client.ask(b"open").await, "open");

    let closing = tokio::spawn(listener.close());

    // The client takes the bytes, so the send does not wait, and the closed listener does not stop it.
    assert_eq!(client.receive().await, Some(frame(b"goodbye")));
    assert_eq!(client.receive().await, None);
    drop(client);
    soon(closing).await.unwrap();
}

#[tokio::test]
async fn a_close_waits_for_a_client_that_stays_no_longer_than_its_time() {
    let (progress, mut said) = mpsc::unbounded_channel();
    let handler = Arc::new(AnswerLateAndClose {
        go: Notify::new(),
        answer: b"the last response".to_vec(),
        progress,
    });
    let mut configuration = on_a_free_port(Endpoint::Bootstrap);
    configuration.close_timeout = TimeSpan::from_millis(50);
    let listener = Listener::bind(configuration, ConnectionNumbers::new(), Arc::clone(&handler))
        .await
        .unwrap();
    let mut client = Client::connect(listener.address()).await;

    client.send(b"first").await;
    handler.go.notify_one();

    // The client reads the response and the end, and keeps its side of the connection open.
    assert_eq!(client.receive().await, Some(frame(b"the last response")));
    assert_eq!(client.receive().await, None);
    assert_eq!(soon(said.recv()).await, Some("received"));
    assert_eq!(soon(said.recv()).await, Some("answered"));
    assert_eq!(soon(said.recv()).await, Some("closed"));
    // Nothing of the listener is left, although the client never closed.
    soon(listener.close()).await;
}
