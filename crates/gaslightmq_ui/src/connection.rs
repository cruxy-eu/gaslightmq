//! The live stream behind the UI: it opens the stream as the presence of the tab, applies what comes to what the UI
//! knows, and asks for the calls the UI shows.

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
use crate::pads::Desk;

/// Something the UI changed that the live stream may need to follow: the environment it shows, its filter,
/// or the presence that it let go of.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Changed;

/// Follows the live stream for as long as the UI runs, and opens a new one each time one ends.
///
/// A new stream comes back as the presence that the tab is, so the pads that someone holds stay held across a
/// stream that broke. When the control plane no longer held that presence, it ended its holds, and the desk holds no
/// pad any more.
pub async fn run(
    mut live: Signal<Live>,
    mut desk: Signal<Option<Desk>>,
    client: ControlClient,
    configuration: UiConfiguration,
    mut changes: UnboundedReceiver<Changed>,
) {
    loop {
        live.write().opening();
        let comes_back_as = live.peek().presence();
        let opened = match comes_back_as {
            Some(presence) => client.live_stream_as(presence).await,
            None => client.live_stream().await,
        };
        let reason = match opened {
            Ok(mut stream) => {
                live.write().opened();
                let ended = live.write().present_as(stream.presence());
                if ended && let Some(desk) = desk.write().as_mut() {
                    desk.presence_ended();
                }
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
///
/// The stream also ends when the UI let go of its presence: the next one opens as a new presence,
/// and the control plane ends the one that nobody comes back as.
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
        if live.peek().presence().is_none() {
            return "the tab let go of its presence, and opens a new stream as a new one".to_owned();
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
