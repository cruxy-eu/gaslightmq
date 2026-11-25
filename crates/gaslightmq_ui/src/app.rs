//! The application: what every view shares, and the views by their path.

use dioxus::prelude::*;
use gaslightmq_control_api::Operation;
use gaslightmq_control_client::{ControlClient, ControlClientConfiguration};

use crate::browser;
use crate::configuration::UiConfiguration;
use crate::connection::{self, Changed};
use crate::live::Live;
use crate::views::{Pads, Record, Rules, Shell};

/// The stylesheet of every view.
const STYLE: Asset = asset!("/assets/ui.css");

/// The views of the UI, by their path. The shell holds the navigation and the environment switcher around them.
#[derive(Debug, Clone, PartialEq, Routable)]
#[rustfmt::skip]
pub enum Route {
    #[layout(Shell)]
        /// The live traffic of the environment shown.
        #[route("/")]
        Record {},
        /// Every rule of the environment shown, with its inspector.
        #[route("/rules")]
        Rules {},
        /// The pads of a layout, played live.
        #[route("/pads")]
        Pads {},
}

/// What every view shares: what the UI knows, the client of the control plane, and the notices on the screen.
#[derive(Clone)]
pub struct Ui {
    /// What the UI knows from the live stream.
    pub live: Signal<Live>,
    /// The client that sends operations to the control plane.
    pub client: ControlClient,
    /// What went wrong with what someone did, until it fades.
    pub notices: Signal<Vec<Notice>>,
    /// Tells the live stream that the UI shows something else.
    changes: Coroutine<Changed>,
    configuration: UiConfiguration,
}

/// A line on the screen that says what happened to what someone did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notice {
    /// Tells one notice from another.
    pub id: u64,
    /// What happened.
    pub text: String,
    /// Whether it went wrong.
    pub failed: bool,
}

impl Ui {
    /// The live stream may need to follow other calls: the UI shows another environment, or another filter.
    pub fn changed(&self) {
        self.changes.send(Changed);
    }

    /// Sends `operation` to the control plane, and shows a notice when it fails.
    pub fn send<O>(&self, operation: O)
    where
        O: Operation + 'static,
    {
        self.send_then(operation, |_| {});
    }

    /// Sends `operation` to the control plane, and hands its answer to `then`. A failure shows a notice.
    pub fn send_then<O>(&self, operation: O, then: impl FnOnce(O::Answer) + 'static)
    where
        O: Operation + 'static,
    {
        let client = self.client.clone();
        let ui = self.clone();
        spawn(async move {
            match client.send(operation).await {
                Ok(Ok(answer)) => then(answer),
                Ok(Err(error)) => ui.notify(error.to_string(), true),
                Err(error) => ui.notify(error.to_string(), true),
            }
        });
    }

    /// Shows `text` until it fades.
    pub fn notify(&self, text: String, failed: bool) {
        let mut notices = self.notices;
        let id = notices.peek().last().map_or(0, |last| last.id + 1);
        notices.write().push(Notice { id, text, failed });
        let fades_after = self.configuration.notice_for;
        spawn(async move {
            browser::sleep(fades_after).await;
            notices.write().retain(|notice| notice.id != id);
        });
    }
}

/// The UI: it opens the live stream, and shows the view of the path.
#[component]
pub fn App() -> Element {
    let Some(address) = browser::control_plane() else {
        return rsx! {
            document::Stylesheet { href: STYLE }
            div { class: "fatal", "The page has no address that a control plane can have." }
        };
    };
    let configuration = use_hook(UiConfiguration::default);
    let client = use_hook(|| ControlClient::new(ControlClientConfiguration::new(address)));
    let live = use_signal(|| Live::new(&configuration));
    let notices = use_signal(Vec::new);
    let changes = use_coroutine({
        let client = client.clone();
        let configuration = configuration.clone();
        move |changes| connection::run(live, client.clone(), configuration.clone(), changes)
    });
    use_context_provider(|| Ui {
        live,
        client,
        notices,
        changes,
        configuration,
    });
    rsx! {
        document::Stylesheet { href: STYLE }
        document::Title { "GaslightMQ" }
        Router::<Route> {}
    }
}
