//! The live stream behind the UI: it opens the stream, applies what comes to what the UI knows,
//! and asks for the calls the UI shows.

use std::pin::pin;

use dioxus::prelude::*;
use futures::channel::mpsc::UnboundedReceiver;
use futures::future::{Fuse, FusedFuture};
use futures::{FutureExt, StreamExt, select_biased};
use gaslightmq_control_api::prelude::*;
use gaslightmq_control_client::{ControlClient, LiveStream};

use crate::browser;
use crate::configuration::UiConfiguration;
use crate::live::Live;

/// Something the UI changed that the live stream may need to follow: the environment it shows, or its filter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Changed;

/// Follows the live stream for as long as the UI runs, and opens a new one each time one ends.
pub async fn run(
    mut live: Signal<Live>,
    client: ControlClient,
    configuration: UiConfiguration,
    mut changes: UnboundedReceiver<Changed>,
) {
    loop {
        live.write().opening();
        let reason = match client.live_stream().await {
            Ok(mut stream) => {
                live.write().opened();
                follow(&mut stream, live, &configuration, &mut changes).await
            }
            Err(error) => error.to_string(),
        };
        live.write().lost(reason);
        browser::sleep(configuration.reconnect_after).await;
    }
}

/// Applies every message of `stream` until it ends, at most once per refresh interval, and asks for what the UI shows
/// after each change. Returns why the stream ended.
async fn follow(
    stream: &mut LiveStream,
    mut live: Signal<Live>,
    configuration: &UiConfiguration,
    changes: &mut UnboundedReceiver<Changed>,
) -> String {
    // The messages that came since the UI drew last, and the moment it draws them.
    let mut waiting: Vec<StreamMessage> = Vec::new();
    let mut refresh = pin!(Fuse::terminated());
    loop {
        select_biased! {
            message = stream.next().fuse() => match message {
                Ok(message) => {
                    if refresh.is_terminated() {
                        refresh.set(browser::sleep(configuration.refresh_interval).fuse());
                    }
                    waiting.push(message);
                    continue;
                }
                Err(error) => {
                    apply(live, &mut waiting);
                    return error.to_string();
                }
            },
            () = refresh => apply(live, &mut waiting),
            change = changes.next() => {
                if change.is_none() {
                    // Nobody sends changes once the UI is gone, and the task ends with it.
                    futures::future::pending::<()>().await;
                }
            }
        }
        // The stream may need to follow other calls now.
        let next = live.peek().next_subscription();
        if let Some(subscription) = next {
            stream.follow(subscription.clone()).await;
            live.write().asked(subscription);
        }
    }
}

/// Applies the messages that waited, in one change of what the UI knows.
fn apply(mut live: Signal<Live>, waiting: &mut Vec<StreamMessage>) {
    if waiting.is_empty() {
        return;
    }
    let mut live = live.write();
    for message in waiting.drain(..) {
        live.apply(message);
    }
}
