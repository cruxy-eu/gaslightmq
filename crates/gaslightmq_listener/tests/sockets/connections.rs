//! Connections and listeners side by side, each with its own number.

use std::sync::Arc;

use gaslightmq_field::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_listener::prelude::*;

use crate::support::{Client, Echo, WhoAmI, listen, on_a_free_port};

#[tokio::test]
async fn a_listener_on_port_zero_names_the_port_it_got() {
    let listener = listen(WhoAmI).await;

    assert_ne!(listener.address().port(), 0);
    assert!(listener.address().ip().is_loopback());
    assert_eq!(listener.endpoint(), Endpoint::Bootstrap);
}

#[tokio::test]
async fn a_transport_knows_what_a_session_opens_with() {
    let listener = listen(WhoAmI).await;
    let mut client = Client::connect(listener.address()).await;

    let facts = client.ask(b"who am I").await;

    assert_eq!(facts, format!("#1 from {} to bootstrap", client.address()));
}

#[tokio::test]
async fn two_connections_side_by_side_have_their_own_numbers() {
    let listener = listen(WhoAmI).await;
    let mut first = Client::connect(listener.address()).await;
    let mut second = Client::connect(listener.address()).await;

    // Both connections are open, and each one answers in its own task, in whatever order the clients ask.
    let asked = [
        second.ask(b"who am I").await,
        first.ask(b"who am I").await,
        second.ask(b"who am I").await,
        first.ask(b"who am I").await,
    ];

    let first_facts = format!("#1 from {} to bootstrap", first.address());
    let second_facts = format!("#2 from {} to bootstrap", second.address());
    assert_ne!(first.address(), second.address());
    assert_eq!(
        asked,
        [second_facts.clone(), first_facts.clone(), second_facts, first_facts]
    );
}

#[tokio::test]
async fn a_connection_that_closed_does_not_give_its_number_back() {
    let (echo, mut endings) = Echo::new();
    let listener = listen(echo).await;

    for number in 1..=3 {
        drop(Client::connect(listener.address()).await);
        assert_eq!(endings.next().await.number, ConnectionNumber::new(number));
    }
}

#[tokio::test]
async fn the_listeners_of_one_instance_share_their_numbers_and_their_handler() {
    let numbers = ConnectionNumbers::new();
    let handler = Arc::new(WhoAmI);
    let broker = Endpoint::Broker(BrokerId::from(2));
    let bootstrap = Listener::bind(
        on_a_free_port(Endpoint::Bootstrap),
        numbers.clone(),
        Arc::clone(&handler),
    )
    .await
    .unwrap();
    let second_broker = Listener::bind(on_a_free_port(broker), numbers.clone(), handler)
        .await
        .unwrap();
    assert_ne!(bootstrap.address(), second_broker.address());
    assert_eq!(second_broker.endpoint(), broker);

    // Each client has its answer before the next one connects, so the order of the numbers is the order of the clients.
    let mut first = Client::connect(bootstrap.address()).await;
    let first_facts = first.ask(b"who am I").await;
    let mut second = Client::connect(second_broker.address()).await;
    let second_facts = second.ask(b"who am I").await;
    let mut third = Client::connect(bootstrap.address()).await;
    let third_facts = third.ask(b"who am I").await;

    assert_eq!(first_facts, format!("#1 from {} to bootstrap", first.address()));
    assert_eq!(second_facts, format!("#2 from {} to broker 2", second.address()));
    assert_eq!(third_facts, format!("#3 from {} to bootstrap", third.address()));
    assert_eq!(numbers.assigned(), 3);
}

#[tokio::test]
async fn listeners_with_numbers_of_their_own_count_apart() {
    let one = listen(WhoAmI).await;
    let other = listen(WhoAmI).await;
    let mut client_of_one = Client::connect(one.address()).await;
    let mut client_of_other = Client::connect(other.address()).await;

    assert!(client_of_one.ask(b"who am I").await.starts_with("#1 "));
    assert!(client_of_other.ask(b"who am I").await.starts_with("#1 "));
}

#[tokio::test]
async fn an_address_that_another_listener_holds_takes_no_listener() {
    let listener = listen(WhoAmI).await;
    let taken = ListenerConfiguration::new(listener.address(), Endpoint::Bootstrap);

    let error = Listener::bind(taken, ConnectionNumbers::new(), WhoAmI)
        .await
        .unwrap_err();

    let ListenerError::Bind { address, error: cause } = &error else {
        unreachable!("a listener error that this test does not know: {error}");
    };
    assert_eq!(*address, listener.address());
    assert_eq!(cause.kind(), std::io::ErrorKind::AddrInUse);
    assert_eq!(
        error.to_string(),
        format!("cannot listen on {}: {cause}", listener.address())
    );
}
