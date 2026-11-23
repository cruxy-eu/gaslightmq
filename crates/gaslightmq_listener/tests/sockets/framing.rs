//! Whole frames, however the bytes of the client arrive.

use std::time::Duration;

use gaslightmq_field::prelude::*;
use gaslightmq_listener::prelude::*;
use gaslightmq_wire::WireError;

use crate::support::{Client, Echo, frame, listen, on_a_free_port, still_waits};

#[tokio::test]
async fn a_frame_that_arrives_in_pieces_is_one_frame() {
    let (echo, _endings) = Echo::new();
    let listener = listen(echo).await;
    let mut client = Client::connect(listener.address()).await;
    let bytes = frame(b"orders.eu").to_bytes();

    // The first piece ends inside the length, and the second inside the payload.
    client.write(&bytes[..2]).await;
    assert!(still_waits(client.receive()).await, "half a length is no frame");
    client.write(&bytes[2..7]).await;
    assert!(still_waits(client.receive()).await, "a part of a payload is no frame");
    client.write(&bytes[7..]).await;

    assert_eq!(client.receive().await, Some(frame(b"orders.eu")));
}

#[tokio::test]
async fn a_frame_that_trickles_in_byte_by_byte_is_one_frame() {
    let (echo, _endings) = Echo::new();
    let listener = listen(echo).await;
    let mut client = Client::connect(listener.address()).await;

    for byte in frame(b"one byte at a time").to_bytes() {
        client.write(&[byte]).await;
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    assert_eq!(client.receive().await, Some(frame(b"one byte at a time")));
}

#[tokio::test]
async fn a_frame_that_no_single_read_holds_is_one_frame() {
    let (echo, _endings) = Echo::new();
    let listener = listen(echo).await;
    let mut client = Client::connect(listener.address()).await;
    // Four mebibytes in a pattern that no power of two repeats, so a piece out of place shows.
    let payload: Vec<u8> = (0..=250u8).cycle().take(4 * 1024 * 1024).collect();

    client.send(&payload).await;

    assert_eq!(client.receive().await, Some(frame(&payload)));
}

#[tokio::test]
async fn frames_that_arrive_in_one_piece_stay_apart() {
    let (echo, _endings) = Echo::new();
    let listener = listen(echo).await;
    let mut client = Client::connect(listener.address()).await;
    let payloads: Vec<String> = (0..100).map(|index| format!("frame {index}")).collect();
    let merged: Vec<u8> = payloads
        .iter()
        .flat_map(|payload| frame(payload.as_bytes()).to_bytes())
        .collect();

    client.write(&merged).await;

    for payload in &payloads {
        assert_eq!(client.receive().await, Some(frame(payload.as_bytes())), "{payload}");
    }
}

#[tokio::test]
async fn a_piece_that_ends_inside_a_frame_keeps_the_rest_for_the_next_piece() {
    let (echo, _endings) = Echo::new();
    let listener = listen(echo).await;
    let mut client = Client::connect(listener.address()).await;
    let third = frame(b"third").to_bytes();
    let first_piece = [
        &frame(b"first").to_bytes()[..],
        &frame(b"second").to_bytes()[..],
        &third[..6],
    ]
    .concat();
    let second_piece = [&third[6..], &frame(b"fourth").to_bytes()[..]].concat();

    client.write(&first_piece).await;
    assert_eq!(client.receive().await, Some(frame(b"first")));
    assert_eq!(client.receive().await, Some(frame(b"second")));
    assert!(still_waits(client.receive()).await, "the third frame is not whole yet");
    client.write(&second_piece).await;

    assert_eq!(client.receive().await, Some(frame(b"third")));
    assert_eq!(client.receive().await, Some(frame(b"fourth")));
}

#[tokio::test]
async fn a_frame_over_the_limit_is_no_frame_before_its_payload_arrives() {
    let (echo, mut endings) = Echo::new();
    let mut configuration = on_a_free_port(Endpoint::Bootstrap);
    configuration.socket_request_max_bytes = FrameSizeLimit::new(8).unwrap();
    let listener = Listener::bind(configuration, ConnectionNumbers::new(), echo)
        .await
        .unwrap();
    let mut client = Client::connect(listener.address()).await;

    client.send(b"12345678").await;
    assert_eq!(client.receive().await, Some(frame(b"12345678")));
    // Only the length of a frame of nine bytes.
    client.write(&9u32.to_be_bytes()).await;

    let ended = endings.next().await;
    assert_eq!(ended.frames, 1);
    assert_eq!(
        ended.why,
        ListenerError::NotAFrame {
            reason: WireError::FrameTooLarge { len: 9, max: 8 }.to_string()
        }
    );
    assert_eq!(client.receive().await, None, "the handler let go of the connection");
}

#[tokio::test]
async fn a_frame_of_no_bytes_is_no_frame() {
    let (echo, mut endings) = Echo::new();
    let listener = listen(echo).await;
    let mut client = Client::connect(listener.address()).await;

    client.write(&0u32.to_be_bytes()).await;

    let ended = endings.next().await;
    assert_eq!(
        ended.why,
        ListenerError::NotAFrame {
            reason: WireError::EmptyFrame.to_string()
        }
    );
}
