//! The shell around every view: the navigation, the state of the live stream and the environment switcher.

use dioxus::prelude::*;
use gaslightmq_control_api::environment::{Environment, EnvironmentName, Instances};
use gaslightmq_control_api::prelude::*;
use gaslightmq_control_api::rule::RuleState;

use crate::app::{Route, Ui};
use crate::components::{Icon, Notices, Shape};
use crate::live::Link as StreamLink;

/// The sidebar and the view of the path.
#[component]
pub fn Shell() -> Element {
    rsx! {
        div { class: "app",
            Sidebar {}
            main { class: "main", Outlet::<Route> {} }
            Notices {}
        }
    }
}

/// The navigation, the state of the live stream and the environment switcher.
#[component]
fn Sidebar() -> Element {
    let ui = use_context::<Ui>();
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
        aside { class: "side",
            div { class: "ws",
                div { class: "logo" }
                b { "gaslightmq" }
            }
            NavLink { to: Route::Record {}, on: matches!(route, Route::Record {}), shape: Shape::Record, label: "Live traffic", count: calls.map(|calls| calls.to_string()) }
            NavLink { to: Route::Rules {}, on: matches!(route, Route::Rules {}), shape: Shape::Zap, label: "Rules", count: armed.map(|armed| format!("{armed} armed")) }
            NavLink { to: Route::Pads {}, on: matches!(route, Route::Pads {}), shape: Shape::Grid, label: "Pads", count: None }
            div { class: "sec", "Later" }
            Later { shape: Shape::Play, label: "Scenarios" }
            Later { shape: Shape::Globe, label: "World" }
            Later { shape: Shape::Users, label: "Clients" }
            div { class: "sfoot",
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
        Link { to, class: if on { "nav on" } else { "nav" },
            Icon { shape, size: 15 }
            "{label}"
            if let Some(count) = count {
                span { class: "cnt", "{count}" }
            }
        }
    }
}

/// An entry of a view that arrives in a later step.
#[component]
fn Later(shape: Shape, label: &'static str) -> Element {
    rsx! {
        div { class: "nav off", title: "Arrives in a later step",
            Icon { shape, size: 15 }
            "{label}"
        }
    }
}

/// Whether the live stream is open, and why not.
#[component]
fn StreamState() -> Element {
    let ui = use_context::<Ui>();
    let live = ui.live.read();
    let (class, text) = match live.link() {
        StreamLink::Opening => ("link opening", "Connecting to the control plane".to_owned()),
        StreamLink::Open => ("link open", "Live".to_owned()),
        StreamLink::Lost { reason } => ("link lost", format!("Reconnecting: {reason}")),
    };
    rsx! {
        div { class, title: "{text}", i {} span { "{text}" } }
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
        div { class: "switcher",
            button {
                class: "envbtn",
                title: "Environment",
                onclick: move |_| open.toggle(),
                i { class: if runs { "dot on" } else { "dot" } }
                span { class: "nm", "{label}" }
                Icon { shape: Shape::Chevron, size: 11 }
            }
            if open() {
                div { class: "menu up",
                    div { class: "ms", "Environments of the workspace" }
                    if environments.is_empty() {
                        div { class: "mi off", "The workspace has no environment." }
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
        div { class: "mi", onclick: pick,
            span { class: "ck",
                if shown {
                    Icon { shape: Shape::Check }
                }
            }
            div { class: "two",
                span { class: "t1", "{label}" }
                span { class: "t4", "{kind}" }
            }
            span { class: "sp" }
            if runs {
                span { class: "runs", "runs" }
                button { class: "btn ghost tiny", title: "Stop the environment", onclick: stop, "Stop" }
            } else {
                span { class: "t4", "Start" }
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
