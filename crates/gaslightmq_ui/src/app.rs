//! The application: what every view shares, and the views by their path.

use dioxus::prelude::*;
use gaslightmq_control_api::{Expects, Operation, RequestCause, RequestSource, RequestSurface, Revised};
use gaslightmq_control_client::{ControlClient, ControlClientConfiguration, OperationError, SendError};

use crate::browser;
use crate::configuration::UiConfiguration;
use crate::connection::{self, Changed};
use crate::history::{Change, Outcome, Step, Subject};
use crate::live::Live;
use crate::pads::Desk;
use crate::views::{Pads, Record, Rules, Shell};

/// The stylesheet of every view, which dx builds from `tailwind.css` into the root of the bundle.
const STYLE: &str = "/ui.css";

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
    /// The pads someone plays, once the workspace has a layout. It keeps its layer, mode and focus between views.
    pub desk: Signal<Option<Desk>>,
    /// Whether ⌘K is open.
    pub quick_inject: Signal<bool>,
    /// Tells the live stream that the UI shows something else.
    changes: Coroutine<Changed>,
    configuration: UiConfiguration,
}

impl Ui {
    /// What the UI is configured with.
    pub fn configuration(&self) -> &UiConfiguration {
        &self.configuration
    }
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

    /// Where a request of `surface` comes from: the surface, and the presence of this tab.
    pub fn source(&self, surface: RequestSurface) -> RequestSource {
        self.live.peek().source(surface)
    }

    /// Lets go of the presence of this tab, and of every pad that the tab holds as it.
    ///
    /// The live stream opens again as a new presence, and the control plane ends the one that nobody comes back
    /// as, with every hold of it: also one that the tab does not know of, because the answer to its press got lost.
    pub fn let_go_of_presence(&self) {
        let (mut live, mut desk) = (self.live, self.desk);
        live.write().let_go_of_presence();
        if let Some(desk) = desk.write().as_mut() {
            desk.presence_ended();
        }
        self.changed();
    }

    /// Sends `operation` to the control plane as a view of the UI does, and shows a notice when it fails.
    pub fn send<O>(&self, operation: O)
    where
        O: Operation + 'static,
    {
        self.send_then(operation, |_| {});
    }

    /// Sends `operation` to the control plane as a view of the UI does, and hands its answer to `then`.
    /// A failure shows a notice.
    pub fn send_then<O>(&self, operation: O, then: impl FnOnce(O::Answer) + 'static)
    where
        O: Operation + 'static,
    {
        self.send_from(RequestSurface::Ui, operation, then);
    }

    /// Sends `operation` to the control plane as the request of `surface`, and hands its answer to `then`.
    /// A failure shows a notice.
    ///
    /// The pads and ⌘K are surfaces of their own, so what they send says so.
    pub fn send_from<O>(&self, surface: RequestSurface, operation: O, then: impl FnOnce(O::Answer) + 'static)
    where
        O: Operation + 'static,
    {
        self.send_settled(surface, operation, |answer| {
            if let Some(answer) = answer {
                then(answer);
            }
        });
    }

    /// Sends `operation` to the control plane as the request of `surface`, and hands `settled` its answer,
    /// or `None` when it failed. A failure shows a notice.
    ///
    /// The request says which presence the tab is, as every request of the UI does.
    pub fn send_settled<O>(
        &self,
        surface: RequestSurface,
        operation: O,
        settled: impl FnOnce(Option<O::Answer>) + 'static,
    ) where
        O: Operation + 'static,
    {
        let client = self.client.clone();
        let ui = self.clone();
        let from = self.source(surface);
        spawn(async move {
            let failed = match client.send_from(from, operation).await {
                Ok(answer) => return settled(Some(answer)),
                Err(error) => error.to_string(),
            };
            ui.notify(failed, true);
            settled(None);
        });
    }

    /// What a change of `subject` expects: the run and the revision that the tab shows.
    /// Every change that the UI makes sends it.
    pub fn expects(&self, subject: &Subject) -> Option<Expects> {
        self.live.peek().expects(subject)
    }

    /// The control plane applied a change of this tab, and `step` takes it back.
    pub fn record(&self, step: Step) {
        let mut live = self.live;
        live.write().history_mut().record(step);
    }

    /// ⌘Z: takes back the last change of this tab. It does nothing while someone holds a pad.
    pub fn undo(&self) {
        self.go_through_history(RequestCause::Undo);
    }

    /// ⇧⌘Z: makes the last change that this tab took back again. It does nothing while someone holds a pad.
    pub fn redo(&self) {
        self.go_through_history(RequestCause::Redo);
    }

    /// Sends the undo or the redo of the history, as an ordinary operation with its cause, and says what came of it.
    fn go_through_history(&self, cause: RequestCause) {
        if self.desk.peek().as_ref().is_some_and(Desk::holding) {
            return;
        }
        let mut live = self.live;
        let change = match cause {
            RequestCause::Undo => live.write().history_mut().undo(),
            RequestCause::Redo => live.write().history_mut().redo(),
        };
        let Some(change) = change else {
            return;
        };
        // ⌘Z is a key of the UI: neither a press at the pads nor a ⌘K sentence.
        let from = self.source(RequestSurface::Ui).with_cause(cause);
        let ui = self.clone();
        spawn(async move {
            let client = &ui.client;
            let outcome = match change {
                Change::ArmRule(arm) => outcome(client.send_from(from, arm).await),
                Change::DisarmRule(disarm) => outcome(client.send_from(from, disarm).await),
                Change::CreateLiveRule(create) => outcome(client.send_from(from, create).await),
                Change::UpdateLiveRule(update) => outcome(client.send_from(from, update).await),
                Change::DeleteLiveRule(delete) => outcome(client.send_from(from, delete).await),
                Change::UpdateRule(update) => outcome(client.send_from(from, update).await),
            };
            let report = live.write().history_mut().settled(outcome);
            if let Some(report) = report {
                ui.notify(report.text, report.refused);
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

/// What came of an undo or a redo, from what the client returned.
///
/// A request that got no answer may have applied. A request that the control plane did not take, or whose answer
/// nobody can read, will not apply when it goes out again.
fn outcome<E>(sent: Result<Revised, OperationError<E>>) -> Outcome
where
    Outcome: From<E>,
{
    match sent {
        Ok(answer) => Outcome::Applied {
            revision: answer.revision,
        },
        Err(OperationError::Refused { error: refused }) => Outcome::from(refused),
        Err(OperationError::Unanswered {
            error: SendError::Unreachable { reason },
        }) => Outcome::Unanswered { reason },
        Err(OperationError::Unanswered {
            error: error @ (SendError::Rejected { .. } | SendError::Unreadable { .. }),
        }) => Outcome::Refused {
            reason: error.to_string(),
        },
    }
}

/// The UI: it opens the live stream, and shows the view of the path.
#[component]
pub fn App() -> Element {
    let Some(address) = browser::control_plane() else {
        return rsx! {
            document::Stylesheet { href: STYLE }
            div { class: "p-12 text-danger", "The page has no address that a control plane can have." }
        };
    };
    let configuration = use_hook(UiConfiguration::default);
    let client = use_hook(|| {
        // The UI says that it is the UI. Its pads and ⌘K name themselves with each request.
        let mut of_the_ui = ControlClientConfiguration::new(address);
        of_the_ui.surface = RequestSurface::Ui;
        ControlClient::new(of_the_ui)
    });
    let live = use_signal(|| Live::new(&configuration));
    let notices = use_signal(Vec::new);
    let desk = use_signal(|| None);
    let quick_inject = use_signal(|| false);
    let changes = use_coroutine({
        let client = client.clone();
        let configuration = configuration.clone();
        move |changes| connection::run(live, desk, client.clone(), configuration.clone(), changes)
    });
    use_context_provider(|| Ui {
        live,
        client,
        notices,
        desk,
        quick_inject,
        changes,
        configuration,
    });
    rsx! {
        document::Stylesheet { href: STYLE }
        document::Title { "GaslightMQ" }
        Router::<Route> {}
    }
}
