//! ⌘K: type a sentence such as `delay produce orders.* 750ms 40% order-ingest`, and get an armed live rule.

use dioxus::prelude::*;
use gaslightmq_control_api::prelude::*;
use gaslightmq_control_api::rule::Rule;

use crate::app::{Route, Ui};
use crate::browser;
use crate::components::{Icon, LieBadge, Shape};
use crate::palette::{Jump, Palette, PaletteItem, Question};
use crate::process::{Process, rule_label};
use crate::rules::{scope_label, trigger_text};
use crate::style::{KEY, NOTE, TAG};
use crate::views::rules::{action_label, selector_label};

/// The sentence the input shows while it is empty, so the grammar shows at once.
const EXAMPLE: &str = "delay produce orders.* 750ms 40% order-ingest";

/// A word of the preview.
const TOKEN: &str =
    "inline-flex h-5.5 items-center gap-1.25 rounded-md bg-selected px-1.75 text-compact font-medium text-strong";

/// A line of the list, while it is not highlighted.
const LINE: &str = "flex h-9 cursor-pointer items-center gap-2.5 rounded-lg px-2.5 text-body";

/// The highlighted line of the list.
const LINE_ON: &str = "flex h-9 cursor-pointer items-center gap-2.5 rounded-lg bg-selected px-2.5 text-strong";

/// The palette of ⌘K, over the view, while it is open.
#[component]
pub fn QuickInject() -> Element {
    let ui = use_context::<Ui>();
    let palette = use_signal(Palette::default);
    let navigator = use_navigator();
    let mut open = ui.quick_inject;
    if !open() {
        return rsx! {};
    }
    let live = ui.live.read();
    let Some(process) = live.process().cloned() else {
        return rsx! {};
    };
    let environment = live.shown_running().map(|running| running.environment.clone());
    drop(live);
    let current = palette.read();
    let text = current.text().to_owned();
    let parsed = current.parsed().cloned();
    let whole = current.rule().is_some();
    let items = current.items();
    let highlighted = current.highlighted();
    drop(current);

    // Asks the control plane to read the sentence, once nobody typed for a moment. What ⌘K sends says that it is ⌘K.
    let ask = {
        let ui = ui.clone();
        move |question: Question| {
            let ui = ui.clone();
            let wait = ui.configuration().parse_after;
            spawn(async move {
                browser::sleep(wait).await;
                if !palette.peek().is_latest(question.number) {
                    return;
                }
                let live = ui.live.peek();
                let Some(running) = live.shown_running() else {
                    return;
                };
                let operation = question.operation(running.environment.clone());
                drop(live);
                let mut palette = palette;
                ui.send_from(RequestSurface::QuickInject, operation, move |parsed| {
                    palette.write().answered(question.number, parsed);
                });
            });
        }
    };
    let run = {
        let ui = ui.clone();
        let ask = ask.clone();
        move |item: PaletteItem| {
            let mut palette = palette;
            let mut open = open;
            match item {
                PaletteItem::Create(_) => {
                    let live = ui.live.peek();
                    let Some(running) = live.shown_running() else {
                        return;
                    };
                    let Some(creation) = palette.peek().creation(&running.environment) else {
                        return;
                    };
                    drop(live);
                    let label = rule_label(&creation.rule);
                    let done = ui.clone();
                    ui.send_from(
                        RequestSurface::QuickInject,
                        creation,
                        move |_: gaslightmq_control_api::Revised| {
                            done.notify(format!("Armed the live rule {label}"), false);
                        },
                    );
                    palette.set(Palette::default());
                    open.set(false);
                    navigator.push(Route::Rules {});
                }
                PaletteItem::Complete(completion) => {
                    let question = palette.write().type_text(completion.sentence);
                    ask(question);
                }
                PaletteItem::Jump(jump) => {
                    open.set(false);
                    navigator.push(match jump {
                        Jump::Record => Route::Record {},
                        Jump::Rules => Route::Rules {},
                        Jump::Pads => Route::Pads {},
                    });
                }
            }
        }
    };
    let typed = {
        let ask = ask.clone();
        move |event: FormEvent| {
            let mut palette = palette;
            let question = palette.write().type_text(event.value());
            ask(question);
        }
    };
    let keys = {
        let run = run.clone();
        let ask = ask.clone();
        move |event: KeyboardEvent| {
            let mut palette = palette;
            match event.key() {
                Key::ArrowDown => {
                    event.prevent_default();
                    palette.write().highlight(1);
                }
                Key::ArrowUp => {
                    event.prevent_default();
                    palette.write().highlight(-1);
                }
                Key::Enter => {
                    event.prevent_default();
                    let chosen = palette.peek().chosen();
                    if let Some(item) = chosen {
                        run(item);
                    }
                }
                Key::Tab => {
                    event.prevent_default();
                    let completion = palette.peek().completion();
                    if let Some(completion) = completion {
                        let question = palette.write().type_text(completion.sentence);
                        ask(question);
                    }
                }
                Key::Escape => {
                    event.prevent_default();
                    open.set(false);
                }
                _ => {}
            }
        }
    };
    let focus = move |event: MountedEvent| async move {
        let _ = event.set_focus(true).await;
    };
    let where_to = environment.as_ref().map_or_else(
        || "No environment runs: a sentence needs the names of one.".to_owned(),
        |environment| format!("Live rule of {environment}"),
    );
    rsx! {
        div {
            class: "absolute inset-0 z-20 flex items-start justify-center bg-scrim pt-24 backdrop-blur-xs",
            onclick: move |_| open.set(false),
            div {
                class: "w-175 overflow-hidden rounded-xl border border-edge-strong bg-popover shadow-2xl shadow-scrim",
                onclick: move |event| event.stop_propagation(),
                div { class: "flex items-center gap-1.5 px-3.5 pt-3",
                    span { class: TAG, Icon { shape: Shape::Zap, size: 11 } "Quick inject" }
                    span { class: "{TAG} text-muted", "{where_to}" }
                }
                div { class: "flex items-center gap-2.5 border-b border-edge px-4 pt-2 pb-3.5 text-muted",
                    Icon { shape: Shape::Search, size: 16 }
                    input {
                        class: "flex-1 bg-transparent text-input text-strong caret-accent outline-none placeholder:text-subtle",
                        value: "{text}",
                        autocomplete: "off",
                        spellcheck: "false",
                        placeholder: EXAMPLE,
                        oninput: typed,
                        onkeydown: keys,
                        onmounted: focus,
                    }
                }
                Preview { parsed, typed: !text.trim().is_empty(), whole, process }
                div { class: "max-h-85 overflow-auto p-1.5",
                    for (index, item) in items.into_iter().enumerate() {
                        Line {
                            key: "{index}",
                            item: item.clone(),
                            on: index == highlighted,
                            onrun: {
                                let run = run.clone();
                                move |()| run(item.clone())
                            },
                            onhover: move |()| {
                                let mut palette = palette;
                                palette.write().highlight_at(index);
                            },
                        }
                    }
                }
                div { class: "flex items-center gap-4 border-t border-edge px-3.5 py-2.25 text-caption text-subtle",
                    span { class: "inline-flex items-center gap-1.5", span { class: KEY, "↑" } span { class: KEY, "↓" } "navigate" }
                    span { class: "inline-flex items-center gap-1.5", span { class: KEY, "↵" } "create and arm" }
                    span { class: "inline-flex items-center gap-1.5", span { class: KEY, "⇥" } "complete" }
                    span { class: "flex-1" }
                    span { class: "inline-flex items-center gap-1.5", span { class: KEY, "esc" } "close" }
                }
            }
        }
    }
}

/// What the sentence gives so far: the rule, part by part, and the words that were not read.
#[component]
fn Preview(parsed: Option<ParsedSentence>, typed: bool, whole: bool, process: Process) -> Element {
    let Some(parsed) = parsed else {
        let text = if typed {
            "Reading the sentence…"
        } else {
            "Start with a lie, such as delay, or a preset."
        };
        return rsx! { div { class: "border-b border-edge px-4 py-3 text-compact text-subtle", "{text}" } };
    };
    let unread: Vec<(String, String)> = parsed
        .unread
        .iter()
        .map(|word| (word.word.clone(), word.reason.clone()))
        .collect();
    rsx! {
        div { class: "flex flex-wrap items-center gap-1.25 border-b border-edge bg-hover px-4 py-3 text-regular leading-loose text-muted",
            if let Some(rule) = parsed.rule.clone() {
                RuleTokens { rule, process }
            } else {
                span { class: "inline-flex h-5.5 items-center rounded-md border border-dashed border-edge-bold px-1.75 text-compact text-muted", "lie?" }
                span { "The sentence does not say what happens yet." }
            }
        }
        if !unread.is_empty() {
            div { class: "flex flex-col gap-1 border-b border-edge px-4 py-2.5",
                if !whole {
                    span { class: NOTE, "Every word has to read before ↵ creates the rule:" }
                }
                for (word, reason) in unread {
                    div { key: "{word}", class: "flex gap-2.5 text-small",
                        span { class: "shrink-0 font-mono text-warning", "{word}" }
                        span { class: "text-muted", "{reason}" }
                    }
                }
            }
        }
    }
}

/// The parts of a rule as tokens: what it does, to which calls, for whom, how often and for how long.
#[component]
fn RuleTokens(rule: Rule, process: Process) -> Element {
    let (tone, action) = action_label(&process, rule.action());
    let selector = selector_label(&process, rule.selector());
    let scope = scope_label(rule.scope());
    let trigger = trigger_text(&rule.trigger());
    let lifetime = rule.lifetime();
    rsx! {
        LieBadge { tone, text: action }
        span { "on" }
        span { class: TOKEN, "{selector}" }
        span { "for" }
        span { class: TOKEN, "{scope}" }
        span { "·" }
        span { class: TOKEN, "{trigger}" }
        span { "·" }
        span { class: TOKEN, "{lifetime}" }
        span { class: "ml-auto font-mono text-caption text-subtle", "{rule.name()}" }
    }
}

/// One line of the list.
#[component]
fn Line(item: PaletteItem, on: bool, onrun: EventHandler, onhover: EventHandler) -> Element {
    let (shape, label, detail, kind, key) = match &item {
        PaletteItem::Create(rule) => (
            Shape::Zap,
            format!("Create and arm {}", rule_label(rule)),
            format!("live rule · {}", scope_label(rule.scope())),
            Some("rule"),
            Some("↵"),
        ),
        PaletteItem::Complete(completion) => (
            Shape::Chevron,
            completion.word.clone(),
            completion.title.clone().unwrap_or_default(),
            Some(word_kind(completion.kind)),
            Some("⇥"),
        ),
        PaletteItem::Jump(jump) => (Shape::Play, format!("Open {}", jump.name()), String::new(), None, None),
    };
    rsx! {
        div {
            class: if on { LINE_ON } else { LINE },
            onclick: move |_| onrun.call(()),
            onmouseenter: move |_| onhover.call(()),
            span { class: "text-muted", Icon { shape } }
            span { class: "truncate", "{label}" }
            span { class: "truncate text-small text-subtle", "{detail}" }
            span { class: "ml-auto flex items-center gap-1",
                if let Some(kind) = kind {
                    span { class: "inline-flex h-4.25 items-center rounded border border-edge-strong px-1.25 text-micro text-muted", "{kind}" }
                }
                if let Some(key) = key { span { class: KEY, "{key}" } }
            }
        }
    }
}

/// The kind of a completion, as the list shows it next to the word: names are unique per kind only.
fn word_kind(kind: WordKind) -> &'static str {
    match kind {
        WordKind::Lie => "lie",
        WordKind::Preset => "preset",
        WordKind::Api => "API",
        WordKind::Selector => "selector",
        WordKind::Topic => "topic",
        WordKind::Group => "group",
        WordKind::Client => "client",
        WordKind::ErrorCode => "error code",
    }
}
