//! The live traffic of the environment shown: its sessions, its calls, and why a call came out the way it did.

use std::collections::BTreeMap;

use dioxus::prelude::*;
use gaslightmq_control_api::call::{Answer, Call, Delivered, ExplanationStatus, RuleExplanation};
use gaslightmq_control_api::environment::EnvironmentName;
use gaslightmq_control_api::field::{ConnectionNumber, Endpoint, Field, FieldGroup, FieldSource, FieldValue, Part};
use gaslightmq_control_api::kernel::{ApiKey, TopicName};
use gaslightmq_control_api::prelude::*;
use gaslightmq_control_api::selector::Condition;

use crate::app::Ui;
use crate::components::{ApiPill, ApiTone, ConditionChip, Icon, InspectorPanel, LieBadge, Shape, StateIcon, Tone};
use crate::live::{Link, Live};
use crate::process::{rule_label, rule_status};
use crate::style::{
    BUTTON, BUTTON_PRIMARY, CRUMB, EMPTY, FILTER_VALUE, FILTER_VALUE_ON, HEADER, ICON_BUTTON, KEY, NOTE, PANE_HEADER,
    PANE_HEADER_INSET,
};
use crate::text::{length, since_start, value, values};

/// A row of the traffic.
const ROW: &str = "flex h-8 cursor-pointer items-center gap-2 border-b border-edge/50 px-3 text-small hover:bg-hover";

/// The row of the call someone looks at.
const ROW_SELECTED: &str = "flex h-8 cursor-pointer items-center gap-2 border-b border-edge/50 bg-accent/12 px-3 text-small ring-1 ring-accent/40 ring-inset";

/// The live traffic of the environment shown.
#[component]
pub fn Record() -> Element {
    let ui = use_context::<Ui>();
    let live = ui.live.read();
    if let Some(waiting) = waiting(&live) {
        return waiting;
    }
    let Some(running) = live.shown_running() else {
        return not_running(&ui, &live);
    };
    let environment = running.environment.clone();
    let traffic = live.traffic();
    let paused = traffic.is_paused();
    let calls = traffic.received();
    let sessions = running.sessions.len();
    let refusal = live.following().refusal().map(ToOwned::to_owned);
    let waiting_calls = traffic.waiting();
    let selected = live.inspected().map(Call::id);
    let rows: Vec<Call> = traffic.calls().cloned().collect();
    let pause = {
        let ui = ui.clone();
        move |_| {
            let mut live = ui.live;
            live.write().pause(!paused);
        }
    };
    rsx! {
        div { class: HEADER,
            div { class: CRUMB, span { class: "text-muted", Icon { shape: Shape::Record, size: 15 } } "Live traffic" }
            span { class: "text-subtle", "·" }
            span { class: "text-small text-muted", "{calls} calls followed · {sessions} sessions" }
            if let Some(reason) = refusal {
                span { class: "max-w-2/5 truncate text-small text-danger", title: "{reason}", "Not following: {reason}" }
            }
            span { class: "flex-1" }
            button { class: BUTTON, onclick: pause,
                if paused {
                    Icon { shape: Shape::Play }
                    "Go on"
                    if waiting_calls > 0 {
                        span { class: KEY, "{waiting_calls} new" }
                    }
                } else {
                    Icon { shape: Shape::Pause }
                    "Pause"
                }
            }
        }
        FilterBar {}
        div { class: "flex min-h-0 flex-1",
            div { class: "min-h-0 w-52.5 shrink-0 overflow-auto border-r border-edge", SessionTree { environment: environment.clone() } }
            div { class: "relative min-h-0 min-w-0 flex-1 overflow-auto",
                div { class: "sticky top-0 z-2 flex h-8 items-center gap-2 border-b border-edge bg-panel px-3 text-caption text-subtle",
                    span { class: "w-21 shrink-0", "Time" }
                    span { class: "w-33 shrink-0", "Client · connection" }
                    span { class: "w-8.5 shrink-0", title: "Endpoint", "To" }
                    span { class: "w-29.5 shrink-0", "API" }
                    span { class: "min-w-25 flex-1", "Topics · details" }
                    span { class: "w-14 shrink-0 text-right", "Latency" }
                    span { class: "w-37.5 shrink-0", "Outcome" }
                }
                if rows.is_empty() {
                    div { class: EMPTY,
                        if live.filter().is_empty() {
                            "No call yet. Calls show here as clients send them."
                        } else {
                            "No call of the filter yet. Calls show here as clients send them."
                        }
                    }
                }
                for call in rows {
                    CallRow { key: "{call.id()}", selected: selected == Some(call.id()), call }
                }
            }
            if live.inspected().is_some() {
                InspectorPanel { width: "w-105", Inspector {} }
            } else {
                InspectorPanel { width: "w-67.5", ApiCounts { environment } }
            }
        }
    }
}

/// What the view shows while the UI knows nothing yet, or has no environment to show.
fn waiting(live: &Live) -> Option<Element> {
    if live.process().is_some() {
        return None;
    }
    let text = match live.link() {
        Link::Lost { reason } => format!("The control plane is unreachable: {reason}"),
        Link::Opening | Link::Open => "Connecting to the control plane…".to_owned(),
    };
    Some(rsx! {
        div { class: HEADER, div { class: CRUMB, "Live traffic" } }
        div { class: EMPTY, "{text}" }
    })
}

/// What the view shows when the environment it shows does not run.
fn not_running(ui: &Ui, live: &Live) -> Element {
    let ui = ui.clone();
    let Some(shown) = live.shown().cloned() else {
        return rsx! {
            div { class: HEADER, div { class: CRUMB, "Live traffic" } }
            div { class: EMPTY, "No environment runs. Pick one in the switcher at the bottom left to start it." }
        };
    };
    let start = {
        let shown = shown.clone();
        move |_| {
            ui.send(StartEnvironment {
                environment: shown.clone(),
            });
        }
    };
    rsx! {
        div { class: HEADER, div { class: CRUMB, "Live traffic" } }
        div { class: EMPTY,
            p { "Environment {shown} does not run." }
            button { class: BUTTON_PRIMARY, onclick: start, Icon { shape: Shape::Play } "Start {shown}" }
        }
    }
}

/// The conditions of the filter, each with a button that takes it away.
#[component]
fn FilterBar() -> Element {
    let ui = use_context::<Ui>();
    let live = ui.live.read();
    let conditions: Vec<Condition> = live.filter().conditions().iter().cloned().collect();
    rsx! {
        div { class: "flex min-h-10.5 shrink-0 flex-wrap items-center gap-1.5 border-b border-edge py-2 pr-3 pl-4",
            if conditions.is_empty() {
                span { class: "text-small text-subtle", "Every call. Click a client, a connection, an API or a topic to follow only calls like it." }
            }
            for condition in conditions {
                ConditionChip {
                    key: "{condition.field()}",
                    onremove: {
                        let ui = ui.clone();
                        let field = condition.field();
                        move |()| {
                            let mut live = ui.live;
                            live.write().clear_filter(field);
                            ui.changed();
                        }
                    },
                    condition,
                }
            }
        }
    }
}

/// A value that adds itself to the filter when someone clicks it.
#[component]
fn Value(value: FieldValue, children: Element) -> Element {
    let ui = use_context::<Ui>();
    let on = ui.live.read().filter().holds(&value);
    let toggle = move |event: MouseEvent| {
        event.stop_propagation();
        let mut live = ui.live;
        live.write().toggle(value.clone());
        ui.changed();
    };
    rsx! {
        span { class: if on { FILTER_VALUE_ON } else { FILTER_VALUE }, onclick: toggle, {children} }
    }
}

/// One call of the traffic.
#[component]
fn CallRow(call: Call, selected: bool) -> Element {
    let ui = use_context::<Ui>();
    let id = call.id();
    let time = since_start(call.timing().arrived_at());
    let client = value(&call, Field::ClientId);
    let endpoint = value(&call, Field::Endpoint).map(|endpoint| {
        let short = short_endpoint(&endpoint);
        (endpoint, short)
    });
    let api = api_of(&call);
    let version = value(&call, Field::ApiVersion).map(|version| version.to_string());
    let topics = topics(&call);
    let group = value(&call, Field::Group);
    let transaction = value(&call, Field::TransactionalId);
    let latency = call.timing().client_waited().map(length);
    let connection = FieldValue::from(call.connection());
    let inspect = move |_| {
        let mut live = ui.live;
        let already = live.peek().inspected().map(Call::id) == Some(id);
        live.write().inspect(if already { None } else { Some(id) });
    };
    rsx! {
        div { class: if selected { ROW_SELECTED } else { ROW }, onclick: inspect,
            span { class: "w-21 shrink-0 font-mono text-tiny text-subtle", "{time}" }
            div { class: "flex w-33 min-w-0 shrink-0 items-center gap-1.75 text-body",
                span { class: "truncate",
                    if let Some(client) = client {
                        Value { value: client.clone(), "{client}" }
                    } else {
                        span { class: "text-subtle", "anonymous" }
                    }
                }
                span { class: "shrink-0 font-mono text-tiny text-subtle", Value { value: connection.clone(), "{connection}" } }
            }
            span { class: "w-8.5 shrink-0 font-mono text-tiny text-subtle",
                if let Some((endpoint, short)) = endpoint {
                    Value { value: endpoint, "{short}" }
                }
            }
            div { class: "w-29.5 shrink-0",
                if let Some(api) = api {
                    Value { value: FieldValue::from(api), ApiPill { api, version } }
                }
            }
            div { class: "flex min-w-25 flex-1 items-center gap-1.5 overflow-hidden whitespace-nowrap text-muted",
                for (topic, partitions) in topics {
                    span { key: "{topic}", class: "text-body", Value { value: FieldValue::from(topic.clone()), "{topic}" } }
                    if !partitions.is_empty() {
                        span { class: "font-mono text-micro text-subtle", "[{partitions}]" }
                    }
                }
                if let Some(group) = group {
                    span { class: "text-faint", "·" }
                    Value { value: group.clone(), "group {group}" }
                }
                if let Some(transaction) = transaction {
                    span { class: "text-faint", "·" }
                    Value { value: transaction.clone(), "{transaction}" }
                }
            }
            div { class: "w-14 shrink-0 text-right font-mono text-tiny text-muted",
                if let Some(latency) = latency { "{latency}" } else { span { class: "text-subtle", "no answer" } }
            }
            Outcome { call: call.clone() }
        }
    }
}

/// What the client received: the lies that applied, or the answer of the backend.
#[component]
fn Outcome(call: Call) -> Element {
    let verdict = call.verdict();
    let lies: Vec<(Tone, String)> = verdict
        .lies()
        .iter()
        .map(|lie| (Tone::of(lie.lie()), lie.to_string()))
        .collect();
    let answer = delivered(call.delivered());
    rsx! {
        div { class: "flex w-37.5 min-w-0 shrink-0 items-center gap-1.5 overflow-hidden whitespace-nowrap text-caption",
            if lies.is_empty() {
                span { class: if answer == "ok" { "font-mono text-tiny text-faint" } else { "truncate font-mono text-tiny text-danger" }, "{answer}" }
            }
            for (tone, text) in lies {
                LieBadge { tone, text }
            }
        }
    }
}

/// What the client received, in a few words.
fn delivered(delivered: &Delivered) -> String {
    match delivered {
        Delivered::Response(response) => match response.answer() {
            Answer::Ok if response.is_ok() => "ok".to_owned(),
            Answer::Ok => {
                let failed: Vec<String> = response
                    .parts()
                    .filter_map(|(part, answer)| answer.error().map(|code| format!("{code} on {part}")))
                    .collect();
                failed.join(", ")
            }
            Answer::Error(code) => code.to_string(),
        },
        Delivered::UnreadResponse => "a response nobody could read".to_owned(),
        Delivered::Nothing => "nothing".to_owned(),
        Delivered::ConnectionClosed => "connection closed".to_owned(),
    }
}

/// An endpoint in a column of the traffic: `boot` for the bootstrap endpoint, and `b2` for broker 2.
fn short_endpoint(endpoint: &FieldValue) -> String {
    match endpoint {
        FieldValue::Endpoint(Endpoint::Bootstrap) => "boot".to_owned(),
        FieldValue::Endpoint(Endpoint::Broker(broker)) => format!("b{broker}"),
        other => other.to_string(),
    }
}

/// The API of a call.
fn api_of(call: &Call) -> Option<ApiKey> {
    match value(call, Field::Api)? {
        FieldValue::Api(api) => Some(api),
        _ => None,
    }
}

/// The topics of a call in the order of the request, each with its partitions: `("orders.eu", "0,3")`.
fn topics(call: &Call) -> Vec<(TopicName, String)> {
    let mut topics: Vec<(TopicName, Vec<String>)> = Vec::new();
    for part in call.parts() {
        let part: &Part = part.part();
        let partition = part.partition().map(|partition| partition.to_string());
        match topics.iter_mut().find(|(topic, _)| topic == part.topic()) {
            Some((_, partitions)) => partitions.extend(partition),
            None => topics.push((part.topic().clone(), partition.into_iter().collect())),
        }
    }
    topics
        .into_iter()
        .map(|(topic, partitions)| (topic, partitions.join(",")))
        .collect()
}

/// The sessions of the environment by client, and the endpoints of its backend.
#[component]
fn SessionTree(environment: EnvironmentName) -> Element {
    let ui = use_context::<Ui>();
    let live = ui.live.read();
    let Some(running) = live.shown_running() else {
        return rsx! {};
    };
    let mut clients: BTreeMap<Option<String>, Vec<&SessionStatus>> = BTreeMap::new();
    for session in &running.sessions {
        let client = session
            .identity
            .as_ref()
            .and_then(|identity| identity.client.as_ref())
            .map(ToString::to_string);
        clients.entry(client).or_default().push(session);
    }
    let groups: Vec<(Option<FieldValue>, String, Vec<SessionRow>)> = clients
        .into_iter()
        .map(|(client, sessions)| {
            let value = sessions
                .first()
                .and_then(|session| session.identity.as_ref()?.client.clone())
                .map(FieldValue::from);
            let label = client.unwrap_or_else(|| "anonymous".to_owned());
            let rows = sessions
                .into_iter()
                .map(|session| SessionRow {
                    connection: session.connection,
                    endpoint: session.endpoint.to_string(),
                    software: session
                        .identity
                        .as_ref()
                        .and_then(|identity| identity.software.as_ref())
                        .map(ToString::to_string),
                    calls: live.traffic().calls_of(session.connection),
                })
                .collect();
            (value, label, rows)
        })
        .collect();
    let endpoints: Vec<(String, String)> = running
        .backend
        .endpoints
        .iter()
        .map(|(endpoint, address)| {
            let address = address
                .as_ref()
                .map_or_else(|| "waits for its port".to_owned(), ToString::to_string);
            (endpoint.to_string(), address)
        })
        .collect();
    let kind = match running.backend.kind {
        BackendKind::Upstream => "a real cluster",
        BackendKind::Simulated => "a simulated broker",
    };
    rsx! {
        div { class: PANE_HEADER, "Sessions" span { class: "ml-auto font-normal text-subtle", "{running.sessions.len()}" } }
        if groups.is_empty() {
            div { class: "px-3.5 py-2.5 {NOTE}", "No client is connected to {environment}." }
        }
        for (value, label, rows) in groups {
            div { key: "{label}", class: "pb-1",
                div { class: "mt-1 flex h-7.5 items-center gap-2 pr-3 pl-3.5 font-medium text-strong",
                    if let Some(value) = value {
                        Value { value, "{label}" }
                    } else {
                        span { class: "text-subtle", "{label}" }
                    }
                }
                for row in rows {
                    Session { key: "{row.connection}", row }
                }
            }
        }
        div { class: "mt-2.5 flex h-8.5 items-center gap-2 whitespace-nowrap border-y border-edge px-3.5 text-small font-medium text-muted",
            "Endpoints"
            span { class: "ml-auto font-normal text-subtle", "{kind}" }
        }
        for (endpoint, address) in endpoints {
            div { key: "{endpoint}", class: "flex h-6.25 items-center gap-1.5 px-3.5 font-mono text-tiny text-muted",
                span { class: "w-17.5 shrink-0", "{endpoint}" }
                span { class: "truncate text-subtle", "{address}" }
            }
        }
    }
}

/// What the tree shows of one session.
#[derive(Debug, Clone, PartialEq)]
struct SessionRow {
    connection: ConnectionNumber,
    endpoint: String,
    software: Option<String>,
    calls: usize,
}

/// One session in the tree.
#[component]
fn Session(row: SessionRow) -> Element {
    let connection = FieldValue::from(row.connection);
    rsx! {
        div { class: "flex h-6.25 items-center gap-1.5 pr-3 pl-7.5 text-small text-muted hover:bg-hover",
            span { class: "w-8.5 shrink-0 font-mono text-tiny", Value { value: connection, "{row.connection}" } }
            span { class: "flex-1 truncate font-mono text-tiny text-subtle", "→ {row.endpoint}" }
            span { class: "font-mono text-tiny text-subtle", "{row.calls}" }
        }
        if let Some(software) = row.software {
            div { class: "-mt-0.5 truncate pr-3.5 pb-1 pl-7.5 font-mono text-micro text-subtle", "{software}" }
        }
    }
}

/// The calls of each API since the environment started, which filter the traffic when someone clicks one.
#[component]
fn ApiCounts(environment: EnvironmentName) -> Element {
    let ui = use_context::<Ui>();
    let live = ui.live.read();
    let stats = live.process().and_then(|process| process.stats(&environment));
    let Some(stats) = stats else {
        return rsx! {
            div { class: PANE_HEADER_INSET, "Calls per API" }
            div { class: "px-3.5 py-2.5 {NOTE}", "The first statistics come in a moment." }
        };
    };
    let mut apis: Vec<(ApiKey, u64)> = stats.apis.iter().map(|(api, calls)| (*api, *calls)).collect();
    apis.sort_by_key(|(_, calls)| std::cmp::Reverse(*calls));
    let most = apis.first().map_or(1, |(_, calls)| (*calls).max(1));
    let total = stats.calls;
    let missed = stats.missed_events;
    rsx! {
        div { class: PANE_HEADER_INSET, "Calls per API" span { class: "ml-auto font-normal text-subtle", "{total} since the start" } }
        if missed > 0 {
            div { class: "px-3.5 py-2.5 {NOTE}", "{missed} events went uncounted: every number is the least it can be." }
        }
        div { class: "px-3.5 py-2",
            for (api, calls) in apis {
                ApiCount { key: "{api}", api, calls, most }
            }
        }
    }
}

/// One API with its calls, as a bar.
#[component]
fn ApiCount(api: ApiKey, calls: u64, most: u64) -> Element {
    let ui = use_context::<Ui>();
    let value = FieldValue::from(api);
    let on = ui.live.read().filter().holds(&value);
    #[expect(
        clippy::cast_precision_loss,
        reason = "a bar is as wide as a share, and needs no exact count"
    )]
    let width = (calls as f64 / most as f64 * 100.0).max(2.0);
    let dot = ApiTone::of(api).dot();
    let toggle = move |_| {
        let mut live = ui.live;
        live.write().toggle(value.clone());
        ui.changed();
    };
    rsx! {
        div { class: if on { "relative -mx-1.5 flex h-6 cursor-pointer items-center gap-2 rounded px-1.5 text-small text-strong hover:bg-hover" } else { "relative -mx-1.5 flex h-6 cursor-pointer items-center gap-2 rounded px-1.5 text-small text-body hover:bg-hover" },
            onclick: toggle,
            span { class: if on { "absolute inset-y-0.75 left-0 rounded bg-accent/20" } else { "absolute inset-y-0.75 left-0 rounded bg-hover" }, style: "width:{width}%" }
            span { class: if on { "relative grid size-3 shrink-0 place-items-center rounded-sm border border-primary bg-primary text-on-primary" } else { "relative grid size-3 shrink-0 place-items-center rounded-sm border border-edge-bold" },
                if on { Icon { shape: Shape::Check, size: 10 } }
            }
            i { class: "relative {dot}" }
            span { class: "relative truncate", "{api}" }
            span { class: "relative ml-auto font-mono text-tiny text-subtle", "{calls}" }
        }
    }
}

/// Why the call someone looks at came out the way it did, and every field it has.
#[component]
fn Inspector() -> Element {
    let ui = use_context::<Ui>();
    let live = ui.live.read();
    let Some(call) = live.inspected().cloned() else {
        return rsx! {};
    };
    let running = live.shown_running().cloned();
    let verdict = call.verdict();
    let lied = !verdict.is_pass();
    let verdict_text = if lied {
        verdict.to_string()
    } else {
        "no lie applied".to_owned()
    };
    let api = api_of(&call);
    let version = value(&call, Field::ApiVersion).map(|version| version.to_string());
    let time = since_start(call.timing().arrived_at());
    let waited = call
        .timing()
        .client_waited()
        .map_or_else(|| "no answer".to_owned(), length);
    let backend = call.timing().backend_took().map(length);
    let truth = truth(&call);
    let received = delivered(call.delivered());
    let lines: Vec<RuleExplanation> = call.explanation().rules().to_vec();
    let failure = call.explanation().failure().map(ToString::to_string);
    let groups: Vec<(&'static str, Vec<(Field, String)>)> = FieldGroup::ALL
        .into_iter()
        .map(|group| {
            let fields = group
                .fields()
                .iter()
                .filter_map(|field| {
                    let values = values(&call, *field);
                    (!values.is_empty()).then(|| {
                        let text = values.iter().map(ToString::to_string).collect::<Vec<_>>().join(", ");
                        (*field, text)
                    })
                })
                .collect();
            (group_title(group), fields)
        })
        .collect();
    let close = move |_| {
        let mut live = ui.live;
        live.write().inspect(None);
    };
    rsx! {
        div { class: "border-b border-edge px-4 pt-3.5 pb-3",
            div { class: "flex items-center gap-2",
                if let Some(api) = api { ApiPill { api, version } }
                span { class: "font-mono text-caption text-subtle", "call {call.id()}" }
                span { class: "flex-1" }
                button { class: ICON_BUTTON, title: "Close", onclick: close, Icon { shape: Shape::Close } }
            }
            div { class: "mt-2 font-mono text-tiny text-subtle", "{time} · {call.connection()} · waited {waited}" }
        }
        div { class: "mx-4 mt-3.5 mb-1 flex flex-col gap-2.5 rounded-lg border border-edge-strong bg-raised px-3.5 py-3",
            h5 { class: "flex items-center gap-2 text-compact font-medium text-strong",
                span { class: if lied { "size-2 shrink-0 rounded-full bg-warning" } else { "size-2 shrink-0 rounded-full bg-armed" } }
                if lied { "Lied" } else { "Pass" }
                span { class: "truncate font-normal text-subtle", "{verdict_text}" }
            }
            div { class: "flex flex-col gap-1 text-small text-muted",
                Fact { label: "Truth", text: truth }
                Fact { label: "Delivered", text: received }
                if let Some(backend) = backend {
                    Fact { label: "Backend", text: format!("took {backend}") }
                }
            }
            if lines.is_empty() {
                div { class: "border-t border-edge pt-2.25 {NOTE}", "No armed rule matched this call." }
            }
            for line in lines {
                ExplanationLine { key: "{line.rule()}", line, running: running.clone() }
            }
            if let Some(failure) = failure {
                div { class: "border-t border-edge pt-2.25 text-caption text-danger", "GaslightMQ failed on this call: {failure}" }
            }
        }
        for (title, fields) in groups {
            if !fields.is_empty() {
                div { class: "px-4 pt-3.5 pb-1 text-tiny font-medium text-subtle", "{title}" }
                for (field, text) in fields {
                    div { key: "{field}", class: "flex min-h-6.5 items-center gap-2.5 px-4 py-0.5 font-mono text-caption",
                        span { class: "w-30 shrink-0 text-muted", "{field}" }
                        span { class: "min-w-0 break-all text-body", "{text}" }
                    }
                }
            }
        }
    }
}

/// One rule the call touched: how far it got, and what it did.
#[component]
fn ExplanationLine(line: RuleExplanation, running: Option<RunningEnvironment>) -> Element {
    let status = running.as_ref().and_then(|running| {
        let name = line.rule().as_str().parse().ok()?;
        rule_status(running, &name).cloned()
    });
    let label = status
        .as_ref()
        .map_or_else(|| line.rule().to_string(), |status| rule_label(&status.rule));
    let statuses: Vec<ExplanationStatus> = line.statuses().collect();
    let text = line.to_string();
    let lie = line.lie().and_then(|fate| {
        fate.applied()
            .is_some()
            .then(|| (Tone::of(fate.lie().lie()), fate.lie().to_string()))
    });
    rsx! {
        div { class: "flex flex-col gap-1.5 border-t border-edge pt-2.25",
            div { class: "flex min-w-0 items-center gap-1.75 text-compact font-medium text-strong",
                if let Some(status) = &status {
                    StateIcon { state: status.state, size: 12 }
                }
                span { class: "truncate", "{label}" }
                span { class: "flex-1" }
                if let Some((tone, text)) = lie {
                    LieBadge { tone, text }
                }
            }
            div { class: "flex flex-wrap items-center gap-1.25 text-caption text-muted",
                for status in statuses {
                    span { class: step_class(status), "{status.name()}" }
                }
            }
            div { class: "break-all font-mono text-micro leading-normal text-muted", "{text}" }
        }
    }
}

/// How a step of an explanation looks: an applied lie stands out, a lost one warns.
fn step_class(status: ExplanationStatus) -> &'static str {
    match status {
        ExplanationStatus::Applied => "inline-flex min-h-4.75 items-center rounded bg-armed/12 px-1.5 text-armed",
        ExplanationStatus::Shadowed | ExplanationStatus::PreEmpted => {
            "inline-flex min-h-4.75 items-center rounded bg-warning/12 px-1.5 text-warning"
        }
        ExplanationStatus::Matched | ExplanationStatus::Rolled | ExplanationStatus::Fired => {
            "inline-flex min-h-4.75 items-center rounded bg-selected px-1.5"
        }
    }
}

/// One fact of a call: a label, and its value in a monospace.
#[component]
fn Fact(label: &'static str, text: String) -> Element {
    rsx! {
        div { class: "flex gap-2.5",
            span { class: "w-17 shrink-0", "{label}" }
            span { class: "min-w-0 break-all font-mono text-caption text-body", "{text}" }
        }
    }
}

/// What the backend answered, as the outcome and the errors of the call say.
fn truth(call: &Call) -> String {
    let outcomes: Vec<String> = values(call, Field::Outcome).iter().map(ToString::to_string).collect();
    let errors: Vec<String> = values(call, Field::ErrorCode).iter().map(ToString::to_string).collect();
    match (outcomes.is_empty(), errors.is_empty()) {
        (true, _) => "none: the request did not reach the backend".to_owned(),
        (false, true) => outcomes.join(", "),
        (false, false) => format!("{} · {}", outcomes.join(", "), errors.join(", ")),
    }
}

/// The title of a group of fields.
fn group_title(group: FieldGroup) -> &'static str {
    match group {
        FieldGroup::Connection => "Connection",
        FieldGroup::Request => "Request",
        FieldGroup::Learned => "Client context · learned from other connections",
        FieldGroup::Response => "Response · the truth",
    }
}
