//! The rules of the environment shown, grouped by scope, with the inspector of one.

use dioxus::prelude::*;
use gaslightmq_control_api::environment::EnvironmentName;
use gaslightmq_control_api::prelude::*;
use gaslightmq_control_api::rule::{Action, ActionRef, RuleName, RuleState};
use gaslightmq_control_api::selector::{Condition, Conditions, Selector, SelectorRef};
use gaslightmq_control_api::trigger::{Chance, Lifetime};

use crate::app::Ui;
use crate::components::{ConditionChip, Icon, InspectorPanel, LieBadge, Shape, StateIcon, Tone};
use crate::history::Step;
use crate::live::Live;
use crate::process::{Process, rule_label};
use crate::rules::{
    RuleCommand, RuleOperation, RuleTab, activity, by_scope, commands, lifetimes, operation, scope_label, trigger_text,
    update, with_chance, with_lifetime,
};
use crate::style::{
    BUTTON, BUTTON_DANGER, BUTTON_PRIMARY, CRUMB, DIVIDER, EMPTY, HEADER, NOTE, PROPERTY, PROPERTY_COLUMN,
    PROPERTY_LABEL, PROPERTY_VALUES, SEGMENT, SEGMENT_ON, SEGMENTS, SWITCH_KNOB_OFF, SWITCH_KNOB_ON, SWITCH_OFF,
    SWITCH_ON, TAB, TAB_ON, TAG,
};

/// The chances the inspector offers, in percent.
const CHANCES: [u8; 6] = [100, 50, 25, 10, 5, 1];

/// Every rule of the environment shown, and the inspector of the one someone picked.
#[component]
pub fn Rules() -> Element {
    let ui = use_context::<Ui>();
    let mut tab = use_signal(RuleTab::default);
    let mut picked = use_signal(|| None::<RuleName>);
    let live = ui.live.read();
    let Some(running) = live.shown_running() else {
        return rsx! {
            div { class: HEADER, div { class: CRUMB, Icon { shape: Shape::Zap, size: 15 } "Rules" } }
            div { class: EMPTY, "{nothing_runs(&live)}" }
        };
    };
    let environment = running.environment.clone();
    let groups = by_scope(&running.rules, tab());
    let shown = picked()
        .and_then(|name| running.rules.iter().find(|status| status.rule.name() == &name))
        .or_else(|| groups.first().and_then(|group| group.rules.first()))
        .cloned();
    let counts: Vec<(RuleTab, usize)> = RuleTab::ALL
        .into_iter()
        .map(|each| (each, running.rules.iter().filter(|status| each.lists(status)).count()))
        .collect();
    let selected = shown.as_ref().map(|status| status.rule.name().clone());
    rsx! {
        div { class: HEADER,
            div { class: CRUMB, span { class: "text-muted", Icon { shape: Shape::Zap, size: 15 } } "Rules" }
            div { class: "ml-2.5 flex gap-0.5",
                for (each, count) in counts {
                    button {
                        key: "{each.name()}",
                        class: if each == tab() { TAB_ON } else { TAB },
                        onclick: move |_| tab.set(each),
                        "{each.name()} "
                        span { class: "text-caption text-subtle", "{count}" }
                    }
                }
            }
        }
        div { class: "flex min-h-0 flex-1",
            div { class: "min-h-0 min-w-0 flex-1 overflow-auto",
                if groups.is_empty() {
                    div { class: EMPTY, "No rule here yet. A pad or ⌘K makes a live rule, and the workspace holds the others." }
                }
                for group in groups {
                    div { key: "{group.scope}", class: "sticky top-0 z-1 flex h-9 items-center gap-2 border-b border-edge bg-band pr-3.5 pl-4 text-compact font-medium text-strong",
                        span { class: "text-muted", Icon { shape: if group.scope.is_all() { Shape::Globe } else { Shape::Users }, size: 13 } }
                        "{scope_label(&group.scope)}"
                        span { class: "text-small font-normal text-subtle", "{group.rules.len()}" }
                    }
                    for status in group.rules {
                        RuleRow {
                            key: "{status.rule.name()}",
                            on: selected.as_ref() == Some(status.rule.name()),
                            environment: environment.clone(),
                            onpick: move |name| picked.set(Some(name)),
                            status,
                        }
                    }
                }
            }
            InspectorPanel { width: "w-100",
                if let Some(status) = shown {
                    Inspector { environment, status }
                }
            }
        }
    }
}

/// What the view says while no environment runs that it could show.
fn nothing_runs(live: &Live) -> String {
    match live.shown() {
        Some(shown) => format!("Environment {shown} does not run. Start it in the switcher at the bottom left."),
        None => "No environment runs. Pick one in the switcher at the bottom left to start it.".to_owned(),
    }
}

/// Sends an operation on the rule `was`, expecting the revision that the view shows, and says what it did when it
/// worked. Every change that applied leaves a step in the undo history of the tab, but keep, which leaves none.
fn send(ui: &Ui, was: &RuleStatus, operation: RuleOperation) {
    let subject = operation.subject();
    let operation = match &subject {
        Some(subject) => operation.expecting(ui.expects(subject)),
        None => operation,
    };
    let (ui, was) = (ui.clone(), was.clone());
    match operation {
        RuleOperation::Arm(arm) => ui.clone().send_then(arm.clone(), move |answer| {
            ui.record(Step::armed(arm, answer));
        }),
        RuleOperation::Disarm(disarm) => ui.clone().send_then(disarm.clone(), move |answer| {
            ui.record(Step::disarmed(disarm, answer));
        }),
        RuleOperation::Fire(fire) => {
            let text = format!("Fired {} by hand", fire.rule);
            ui.clone().send_then(fire, move |()| ui.notify(text, false));
        }
        RuleOperation::Keep(keep) => {
            let text = format!("Kept {} in the workspace", keep.rule);
            ui.clone().send_then(keep, move |answer: Revised| {
                if let Some(subject) = subject {
                    let mut live = ui.live;
                    live.write().history_mut().changed(&subject, answer.revision);
                }
                ui.notify(text, false);
            });
        }
        RuleOperation::Delete(delete) => {
            let text = format!("Deleted the live rule {}", delete.rule);
            ui.clone().send_then(delete.clone(), move |answer| {
                ui.record(Step::deleted(delete, &was, answer));
                ui.notify(text, false);
            });
        }
        RuleOperation::UpdateLive(update) => ui.clone().send_then(update.clone(), move |answer| {
            ui.record(Step::updated_live(update, was.rule, answer));
        }),
        RuleOperation::UpdateWorkspace(update) => ui.clone().send_then(update.clone(), move |answer| {
            ui.record(Step::updated_rule(update, was.rule, answer));
        }),
    }
}

/// One rule in the list: its state, what it targets, what it does and how often, and a switch that arms it.
#[component]
fn RuleRow(status: RuleStatus, on: bool, environment: EnvironmentName, onpick: EventHandler<RuleName>) -> Element {
    let ui = use_context::<Ui>();
    let live = ui.live.read();
    let process = live.process();
    let rule = &status.rule;
    let label = rule_label(rule);
    let targets = process.map_or_else(String::new, |process| selector_label(process, rule.selector()));
    let (tone, action) = process.map_or((Tone::Other, String::new()), |process| {
        action_label(process, rule.action())
    });
    let trigger = trigger_text(&rule.trigger());
    let counted = process
        .and_then(|process| process.stats(&environment))
        .and_then(|stats| stats.rules.get(rule.name()).copied());
    let armed = status.state == RuleState::Armed;
    let toggle = {
        let ui = ui.clone();
        let status = status.clone();
        let environment = environment.clone();
        move |event: MouseEvent| {
            event.stop_propagation();
            let command = if armed { RuleCommand::Disarm } else { RuleCommand::Arm };
            send(&ui, &status, operation(&environment, &status, command));
        }
    };
    let name = rule.name().clone();
    let class = if on {
        "flex h-10 cursor-pointer items-center gap-3 border-b border-edge/50 bg-selected pr-4 pl-6"
    } else {
        "flex h-10 cursor-pointer items-center gap-3 border-b border-edge/50 pr-4 pl-6 hover:bg-hover"
    };
    rsx! {
        div { class, onclick: move |_| onpick.call(name.clone()),
            StateIcon { state: status.state }
            span { class: if status.live { "w-16 shrink-0 font-mono text-micro font-medium text-accent-text" } else { "w-16 shrink-0 font-mono text-micro font-medium text-subtle" },
                if status.live { "live" } else { "workspace" }
            }
            div { class: "flex min-w-0 flex-1 items-center gap-2 overflow-hidden",
                b { class: if armed { "min-w-15 truncate font-medium text-strong" } else { "min-w-15 truncate font-medium text-muted" }, "{label}" }
                span { class: TAG, "{targets}" }
            }
            LieBadge { tone, text: action }
            span { class: "w-30 shrink-0 truncate font-mono text-tiny text-muted", "{trigger}" }
            span { class: "w-17.5 shrink-0 text-right font-mono text-tiny text-subtle", title: "fired / matched",
                if let Some(counted) = counted { "{counted.firings} / {counted.matches}" }
            }
            if status.state == RuleState::Draft {
                span { class: "w-6.5 shrink-0" }
            } else {
                span { class: if armed { SWITCH_ON } else { SWITCH_OFF }, title: if armed { "Disarm" } else { "Arm" }, onclick: toggle,
                    i { class: if armed { SWITCH_KNOB_ON } else { SWITCH_KNOB_OFF } }
                }
            }
        }
    }
}

/// Everything about one rule, and what someone can do with it.
#[component]
fn Inspector(environment: EnvironmentName, status: RuleStatus) -> Element {
    let ui = use_context::<Ui>();
    let live = ui.live.read();
    let Some(process) = live.process() else {
        return rsx! {};
    };
    let rule = status.rule.clone();
    let label = rule_label(&rule);
    let name = rule.name().to_string();
    let scope = scope_label(rule.scope());
    let selector = selector_label(process, rule.selector());
    let conditions = conditions_of(process, rule.selector());
    let (tone, action) = action_label(process, rule.action());
    let trigger = rule.trigger();
    let chance = trigger.parts().map(|parts| parts.chance().unwrap_or(Chance::ALWAYS));
    let lifetime = rule.lifetime();
    let counted = process
        .stats(&environment)
        .and_then(|stats| stats.rules.get(rule.name()).copied())
        .unwrap_or_default();
    let intervals = activity(process.stats_history(&environment), rule.name());
    let most = intervals
        .iter()
        .map(|interval| interval.matches)
        .max()
        .unwrap_or(0)
        .max(1);
    let offered = commands(&status);
    let state = match status.state {
        RuleState::Armed => "Armed",
        RuleState::Disarmed => "Disarmed",
        RuleState::Draft => "Draft",
    };
    let source = if status.live {
        "Live rule · in the memory of this environment, until someone keeps it"
    } else {
        "Rule of the workspace · every environment holds it"
    };
    let offered_lifetimes = lifetimes();
    let other_lifetime = !offered_lifetimes.contains(&lifetime);
    rsx! {
        div { class: "px-5 pt-4 pb-7.5",
            div { class: "flex items-center gap-2",
                span { class: if status.live { "font-mono text-micro font-medium text-accent-text" } else { "font-mono text-micro font-medium text-subtle" }, "{source}" }
            }
            h2 { class: "mt-2.5 mb-0.5 text-title font-semibold tracking-tight text-strong", "{label}" }
            p { class: "mb-4 font-mono text-caption text-subtle", "{name}" }
            div { class: PROPERTY,
                label { class: PROPERTY_LABEL, "State" }
                div { class: PROPERTY_VALUES,
                    span { class: "inline-flex h-7 items-center gap-1.75 pr-2.25 text-compact text-body", StateIcon { state: status.state } "{state}" }
                    for command in offered {
                        CommandButton {
                            key: "{command.name()}",
                            environment: environment.clone(),
                            status: status.clone(),
                            command,
                        }
                    }
                }
            }
            div { class: PROPERTY,
                label { class: PROPERTY_LABEL, "Scope" }
                div { class: PROPERTY_VALUES, span { class: TAG, "{scope}" } }
            }
            div { class: PROPERTY,
                label { class: PROPERTY_LABEL, "Selector" }
                div { class: PROPERTY_COLUMN,
                    span { class: "text-body", "{selector}" }
                    for condition in conditions.iter() {
                        ConditionChip { key: "{condition.field()}", condition: condition.clone() }
                    }
                    if conditions.is_empty() {
                        span { class: NOTE, "No condition: every call of the scope." }
                    }
                }
            }
            div { class: DIVIDER }
            div { class: PROPERTY,
                label { class: PROPERTY_LABEL, "Action" }
                div { class: PROPERTY_VALUES, LieBadge { tone, text: action } }
            }
            div { class: PROPERTY,
                label { class: PROPERTY_LABEL, "Trigger" }
                div { class: PROPERTY_COLUMN,
                    span { class: "font-mono text-small text-body", "{trigger_text(&trigger)}" }
                    if let Some(chance) = chance {
                        div { class: SEGMENTS,
                            for percent in CHANCES {
                                ChanceButton {
                                    key: "{percent}",
                                    environment: environment.clone(),
                                    status: status.clone(),
                                    percent,
                                    on: Chance::from_percent(percent).ok() == Some(chance),
                                }
                            }
                        }
                    }
                }
            }
            div { class: PROPERTY,
                label { class: PROPERTY_LABEL, "Lifetime" }
                div { class: PROPERTY_COLUMN,
                    div { class: SEGMENTS,
                        for each in offered_lifetimes {
                            LifetimeButton {
                                key: "{each}",
                                environment: environment.clone(),
                                status: status.clone(),
                                lifetime: each,
                                on: each == lifetime,
                            }
                        }
                    }
                    if other_lifetime {
                        span { class: NOTE, "This rule lives {lifetime}." }
                    }
                }
            }
            div { class: DIVIDER }
            h3 { class: "flex items-center gap-2 text-compact font-medium text-strong",
                "Activity"
                span { class: "text-small font-normal text-subtle", "one bar per statistics interval" }
            }
            div { class: "mt-3 mb-2 flex h-11.5 items-end gap-0.5",
                for (index, interval) in intervals.iter().enumerate() {
                    div { key: "{index}", class: "relative h-full flex-1",
                        i { class: "absolute inset-x-0 bottom-0 block min-h-px rounded-xs bg-track", style: "height:{bar(interval.matches, most)}%" }
                        i { class: "absolute inset-x-0 bottom-0 block rounded-xs {tone.fill()}", style: "height:{bar(interval.firings, most)}%" }
                    }
                }
            }
            div { class: "flex gap-4.5 text-small text-muted",
                span { b { class: "font-mono font-medium text-strong", "{counted.firings}" } " fired" }
                span { b { class: "font-mono font-medium text-strong", "{counted.matches}" } " matched" }
                if let Some(left) = counted.left {
                    span { b { class: "font-mono font-medium text-strong", "{left}" } " left" }
                }
            }
            if status.live {
                p { class: "mt-4.5 {NOTE}",
                    "A live rule changes here, in this environment. Keep it to save it in the workspace under the same name."
                }
            } else {
                p { class: "mt-4.5 {NOTE}",
                    "A change of a rule of the workspace writes to the workspace, and reaches every environment."
                }
            }
        }
    }
}

/// The height of a bar of the chart, in percent of the highest.
fn bar(value: u64, most: u64) -> u64 {
    value.saturating_mul(100) / most.max(1)
}

/// A button that carries out one command on the rule.
#[component]
fn CommandButton(environment: EnvironmentName, status: RuleStatus, command: RuleCommand) -> Element {
    let ui = use_context::<Ui>();
    let class = match command {
        RuleCommand::Arm => BUTTON_PRIMARY,
        RuleCommand::Delete => BUTTON_DANGER,
        RuleCommand::Disarm | RuleCommand::Fire | RuleCommand::Keep => BUTTON,
    };
    let shape = match command {
        RuleCommand::Arm => Shape::Play,
        RuleCommand::Disarm => Shape::Pause,
        RuleCommand::Fire => Shape::Zap,
        RuleCommand::Keep => Shape::Check,
        RuleCommand::Delete => Shape::Close,
    };
    rsx! {
        button { class, onclick: move |_| send(&ui, &status, operation(&environment, &status, command)),
            Icon { shape, size: 12 }
            "{command.name()}"
        }
    }
}

/// One of the chances the inspector offers.
#[component]
fn ChanceButton(environment: EnvironmentName, status: RuleStatus, percent: u8, on: bool) -> Element {
    let ui = use_context::<Ui>();
    let pick = move |_| {
        let chance = Chance::from_percent(percent)
            .ok()
            .filter(|chance| *chance != Chance::ALWAYS);
        let changed = with_chance(&status.rule, chance);
        send(&ui, &status, update(&environment, &status, changed));
    };
    rsx! {
        button { class: if on { SEGMENT_ON } else { SEGMENT }, onclick: pick, "{percent}%" }
    }
}

/// One of the lifetimes the inspector offers.
#[component]
fn LifetimeButton(environment: EnvironmentName, status: RuleStatus, lifetime: Lifetime, on: bool) -> Element {
    let ui = use_context::<Ui>();
    let pick = move |_| {
        let changed = with_lifetime(&status.rule, lifetime);
        send(&ui, &status, update(&environment, &status, changed));
    };
    rsx! {
        button { class: if on { SEGMENT_ON } else { SEGMENT }, onclick: pick, "{lifetime}" }
    }
}

/// What a selector targets, in a few words: the title or the name of a selector of the workspace, or its conditions.
pub(crate) fn selector_label(process: &Process, selector: &SelectorRef) -> String {
    match selector {
        SelectorRef::Named(name) => process
            .definitions()
            .selectors
            .iter()
            .find(|held| held.name() == name)
            .and_then(|held| held.title().map(ToString::to_string))
            .unwrap_or_else(|| name.to_string()),
        SelectorRef::Inline(conditions) if conditions.is_empty() => "every call".to_owned(),
        SelectorRef::Inline(conditions) => conditions.to_string(),
    }
}

/// The conditions of a selector: its own, or those of the selector of the workspace that it names.
fn conditions_of(process: &Process, selector: &SelectorRef) -> Vec<Condition> {
    let conditions: Option<&Conditions> = match selector {
        SelectorRef::Named(name) => process
            .definitions()
            .selectors
            .iter()
            .find(|held| held.name() == name)
            .map(Selector::conditions),
        SelectorRef::Inline(conditions) => Some(conditions),
    };
    conditions.map_or_else(Vec::new, |conditions| conditions.iter().cloned().collect())
}

/// What an action does, in a few words, with the color of its lie: a preset by its title, or the lie itself.
pub(crate) fn action_label(process: &Process, action: &ActionRef) -> (Tone, String) {
    let (preset, action) = match action {
        ActionRef::Preset(name) => {
            let preset = process.definitions().presets.iter().find(|held| held.name() == name);
            let label = preset
                .and_then(|held| held.title().map(ToString::to_string))
                .unwrap_or_else(|| name.to_string());
            (Some(label), preset.map(|held| held.action().clone()))
        }
        ActionRef::Inline(action) => (None, Some(action.clone())),
    };
    let tone = match &action {
        Some(Action::Lie(lie)) => Tone::of(lie),
        _ => Tone::Other,
    };
    let text = match (preset, action) {
        (Some(preset), _) => preset,
        (None, Some(action)) => action.to_string(),
        (None, None) => String::new(),
    };
    (tone, text)
}
