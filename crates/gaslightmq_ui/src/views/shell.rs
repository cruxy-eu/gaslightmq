//! The shell around every view: the navigation, the state of the live stream, the environment switcher and the theme.

use dioxus::prelude::*;
use gaslightmq_control_api::environment::{Environment, EnvironmentName, Instances};
use gaslightmq_control_api::prelude::*;
use gaslightmq_control_api::rule::RuleState;

use crate::app::{Route, Ui};
use crate::browser;
use crate::components::{Icon, Notices, Shape};
use crate::live::Link as StreamLink;
use crate::style::{BUTTON_GHOST, ICON_BUTTON, MENU, MENU_ITEM, MENU_SECTION, SEGMENT, SEGMENT_ON, SEGMENTS};
use crate::theme::ThemeChoice;
use crate::views::QuickInject;

/// An entry of the navigation, while its view is not shown.
const NAV: &str =
    "flex h-7 cursor-pointer items-center gap-2.25 whitespace-nowrap rounded-md px-2 text-body hover:bg-hover";

/// The entry of the view that is shown.
const NAV_ON: &str =
    "flex h-7 cursor-pointer items-center gap-2.25 whitespace-nowrap rounded-md bg-selected px-2 text-strong";

/// The sidebar and the view of the path.
#[component]
pub fn Shell() -> Element {
    let ui = use_context::<Ui>();
    let mut open = ui.quick_inject;
    use_future(move || async move {
        let mut script = browser::listen_for_quick_inject();
        while browser::quick_inject_pressed(&mut script).await {
            open.set(true);
        }
    });
    // ⌘Z and ⇧⌘Z go through the undo history of the tab. While ⌘K is open, the keys belong to its text.
    use_future(move || {
        let ui = ui.clone();
        async move {
            let mut script = browser::listen_for_history_keys();
            while let Some(cause) = browser::history_key_pressed(&mut script).await {
                if *ui.quick_inject.peek() {
                    continue;
                }
                match cause {
                    RequestCause::Undo => ui.undo(),
                    RequestCause::Redo => ui.redo(),
                }
            }
        }
    });
    rsx! {
        div { class: "relative flex h-full overflow-hidden rounded-xl border border-edge bg-app",
            Sidebar {}
            main { class: "relative my-2 mr-2 flex min-h-0 min-w-0 flex-1 flex-col overflow-hidden rounded-lg border border-edge bg-panel",
                Outlet::<Route> {}
            }
            QuickInject {}
            Notices {}
        }
    }
}

/// The navigation, the state of the live stream, the environment switcher and the theme.
#[component]
fn Sidebar() -> Element {
    let ui = use_context::<Ui>();
    let mut quick_inject = ui.quick_inject;
    let route = use_route::<Route>();
    let live = ui.live.read();
    let shown = live.shown_running();
    let calls = shown
        .and_then(|running| live.process()?.stats(&running.environment))
        .map(|stats| stats.calls);
    let armed = shown.map(|running| {
        running
            .rules
            .iter()
            .filter(|status| status.state == RuleState::Armed)
            .count()
    });
    rsx! {
        aside { class: "flex min-h-0 w-56.5 shrink-0 flex-col px-2 py-2.5",
            div { class: "mb-2.5 flex h-8 items-center gap-2 pr-1 pl-1.5",
                div { class: "size-5 shrink-0 rounded-md bg-linear-135 from-brand-from via-brand-via to-brand-to" }
                b { class: "font-semibold text-strong", "gaslightmq" }
                span { class: "flex-1" }
                button {
                    class: ICON_BUTTON,
                    title: "Quick inject ⌘K",
                    onclick: move |_| quick_inject.set(true),
                    Icon { shape: Shape::Search }
                }
            }
            NavLink { to: Route::Record {}, on: matches!(route, Route::Record {}), shape: Shape::Record, label: "Live traffic", count: calls.map(|calls| calls.to_string()) }
            NavLink { to: Route::Rules {}, on: matches!(route, Route::Rules {}), shape: Shape::Zap, label: "Rules", count: armed.map(|armed| format!("{armed} armed")) }
            NavLink { to: Route::Pads {}, on: matches!(route, Route::Pads {}), shape: Shape::Grid, label: "Pads", count: None }
            div { class: "flex items-center gap-1.5 px-2 pt-4 pb-1.25 text-caption font-medium text-subtle", "Later" }
            Later { shape: Shape::Play, label: "Scenarios" }
            Later { shape: Shape::Globe, label: "World" }
            Later { shape: Shape::Users, label: "Clients" }
            div { class: "mt-auto flex flex-col gap-2 px-0.5 pt-3.5 pb-0.5",
                Theme {}
                StreamState {}
                Switcher {}
            }
        }
    }
}

/// One entry of the navigation.
#[component]
fn NavLink(to: Route, on: bool, shape: Shape, label: &'static str, count: Option<String>) -> Element {
    rsx! {
        Link { to, class: if on { NAV_ON } else { NAV },
            span { class: if on { "text-strong" } else { "text-muted" }, Icon { shape, size: 15 } }
            "{label}"
            if let Some(count) = count {
                span { class: "ml-auto text-caption text-subtle", "{count}" }
            }
        }
    }
}

/// An entry of a view that arrives in a later step.
#[component]
fn Later(shape: Shape, label: &'static str) -> Element {
    rsx! {
        div { class: "flex h-7 items-center gap-2.25 whitespace-nowrap px-2 text-faint", title: "Arrives in a later step",
            Icon { shape, size: 15 }
            "{label}"
        }
    }
}

/// The theme: the system's, light or dark. The browser keeps the choice.
#[component]
fn Theme() -> Element {
    let mut choice = use_signal(ThemeChoice::default);
    use_future(move || async move {
        choice.set(browser::theme(None).await);
    });
    rsx! {
        div { class: SEGMENTS, title: "Theme",
            for each in ThemeChoice::ALL {
                button {
                    key: "{each.name()}",
                    class: if each == choice() { SEGMENT_ON } else { SEGMENT },
                    onclick: move |_| async move {
                        choice.set(browser::theme(Some(each)).await);
                    },
                    "{each.label()}"
                }
            }
        }
    }
}

/// Whether the live stream is open, and why not.
#[component]
fn StreamState() -> Element {
    let ui = use_context::<Ui>();
    let live = ui.live.read();
    let (dot, text) = match live.link() {
        StreamLink::Opening => (
            "size-1.5 shrink-0 rounded-full bg-pending",
            "Connecting to the control plane".to_owned(),
        ),
        StreamLink::Open => ("size-1.5 shrink-0 rounded-full bg-armed", "Live".to_owned()),
        StreamLink::Lost { reason } => (
            "size-1.5 shrink-0 rounded-full bg-danger",
            format!("Reconnecting: {reason}"),
        ),
    };
    rsx! {
        div { class: "flex min-w-0 items-center gap-1.75 px-2 font-mono text-tiny text-subtle", title: "{text}",
            i { class: dot }
            span { class: "truncate", "{text}" }
        }
    }
}

/// The environment the UI shows, and a menu with every environment of the workspace.
#[component]
fn Switcher() -> Element {
    let ui = use_context::<Ui>();
    let mut open = use_signal(|| false);
    let live = ui.live.read();
    let Some(process) = live.process() else {
        return rsx! {};
    };
    let shown = live.shown().cloned();
    let label = shown.as_ref().map_or_else(
        || "No environment".to_owned(),
        |name| {
            process
                .definitions()
                .environments
                .iter()
                .find(|environment| environment.name() == name)
                .map_or_else(|| name.to_string(), title)
        },
    );
    let runs = shown.as_ref().is_some_and(|name| process.runs(name));
    let environments = process.definitions().environments.clone();
    let running: Vec<EnvironmentName> = process
        .environments()
        .iter()
        .map(|running| running.environment.clone())
        .collect();
    rsx! {
        div { class: "relative",
            button {
                class: "flex h-8 w-full cursor-pointer items-center gap-2 rounded-lg border border-edge bg-inset px-2.5 text-compact text-body hover:border-edge-strong",
                title: "Environment",
                onclick: move |_| open.toggle(),
                i { class: if runs { "size-1.75 shrink-0 rounded-full bg-armed ring-3 ring-armed/15" } else { "size-1.75 shrink-0 rounded-full bg-subtle" } }
                span { class: "flex-1 truncate text-left", "{label}" }
                Icon { shape: Shape::Chevron, size: 11 }
            }
            if open() {
                div { class: MENU,
                    div { class: MENU_SECTION, "Environments of the workspace" }
                    if environments.is_empty() {
                        div { class: "px-2.25 py-1 text-compact text-subtle", "The workspace has no environment." }
                    }
                    for environment in environments {
                        EnvironmentEntry {
                            key: "{environment.name()}",
                            runs: running.contains(environment.name()),
                            shown: shown.as_ref() == Some(environment.name()),
                            environment: environment.clone(),
                            onpick: move |()| open.set(false),
                        }
                    }
                }
            }
        }
    }
}

/// One environment in the menu of the switcher: picking it shows it, and starts it when it does not run.
#[component]
fn EnvironmentEntry(environment: Environment, runs: bool, shown: bool, onpick: EventHandler) -> Element {
    let ui = use_context::<Ui>();
    let name = environment.name().clone();
    let label = title(&environment);
    let kind = kind(environment.instances());
    let pick = {
        let ui = ui.clone();
        let name = name.clone();
        move |_| {
            let mut live = ui.live;
            live.write().show(name.clone());
            ui.changed();
            if !runs {
                ui.send(StartEnvironment {
                    environment: name.clone(),
                });
            }
            onpick.call(());
        }
    };
    let stop = move |event: MouseEvent| {
        event.stop_propagation();
        ui.send(StopEnvironment {
            environment: name.clone(),
        });
    };
    rsx! {
        div { class: MENU_ITEM, onclick: pick,
            span { class: "w-3.5 shrink-0 text-strong",
                if shown {
                    Icon { shape: Shape::Check }
                }
            }
            div { class: "flex min-w-0 flex-col",
                span { class: "truncate text-strong", "{label}" }
                span { class: "truncate text-caption text-subtle", "{kind}" }
            }
            span { class: "flex-1" }
            if runs {
                span { class: "text-tiny text-armed", "runs" }
                button { class: BUTTON_GHOST, title: "Stop the environment", onclick: stop, "Stop" }
            } else {
                span { class: "text-caption text-subtle", "Start" }
            }
        }
    }
}

/// The title of an environment where it has one, and its name where it has none.
fn title(environment: &Environment) -> String {
    environment
        .title()
        .map_or_else(|| environment.name().to_string(), ToString::to_string)
}

/// What runs in an environment, in a few words.
fn kind(instances: &Instances) -> String {
    match instances {
        Instances::ProxyInFrontOfCluster { upstream, .. } => format!("Proxy in front of {}", upstream.bootstrap()),
        Instances::ProxyInFrontOfBroker { .. } => "Proxy in front of a broker".to_owned(),
        Instances::BrokerAlone { .. } => "Broker".to_owned(),
    }
}
