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
use crate::components::{ApiPill, Icon, LieBadge, Shape, StateIcon, api_color, lie_color};
use crate::live::{Link, Live};
use crate::process::{rule_label, rule_status};
use crate::text::{length, since_start, value, values};

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
        div { class: "hdr",
            div { class: "crumb", Icon { shape: Shape::Record, size: 15 } "Live traffic" }
            span { class: "t4", "·" }
            span { class: "t3 small", "{calls} calls followed · {sessions} sessions" }
            if let Some(reason) = refusal {
                span { class: "refused", title: "{reason}", "Not following: {reason}" }
            }
            span { class: "sp" }
            button { class: "btn", onclick: pause,
                if paused {
                    Icon { shape: Shape::Play }
                    "Go on"
                    if waiting_calls > 0 {
                        span { class: "kbd", "{waiting_calls} new" }
                    }
                } else {
                    Icon { shape: Shape::Pause }
                    "Pause"
                }
            }
        }
        FilterBar {}
        div { class: if selected.is_some() { "rgrid sel" } else { "rgrid" },
            div { class: "pane tree", SessionTree { environment: environment.clone() } }
            div { class: "pane",
                div { class: "srow thead",
                    span { "Time" }
                    span { "Client · connection" }
                    span { title: "Endpoint", "To" }
                    span { "API" }
                    span { "Topics · details" }
                    span { class: "right", "Latency" }
                    span { "Outcome" }
                }
                if rows.is_empty() {
                    div { class: "empty",
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
            div { class: "pane side-pane",
                if live.inspected().is_some() {
                    Inspector {}
                } else {
                    ApiCounts { environment }
                }
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
        div { class: "hdr", div { class: "crumb", "Live traffic" } }
        div { class: "empty", "{text}" }
    })
}

/// What the view shows when the environment it shows does not run.
fn not_running(ui: &Ui, live: &Live) -> Element {
    let ui = ui.clone();
    let Some(shown) = live.shown().cloned() else {
        return rsx! {
            div { class: "hdr", div { class: "crumb", "Live traffic" } }
            div { class: "empty", "No environment runs. Pick one in the switcher at the bottom left to start it." }
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
        div { class: "hdr", div { class: "crumb", "Live traffic" } }
        div { class: "empty",
            p { "Environment {shown} does not run." }
            button { class: "btn primary", onclick: start, Icon { shape: Shape::Play } "Start {shown}" }
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
        div { class: "fbar",
            if conditions.is_empty() {
                span { class: "hint", "Every call. Click a client, a connection, an API or a topic to follow only calls like it." }
            }
            for condition in conditions {
                Chip { key: "{condition.field()}", condition }
            }
        }
    }
}

/// One condition of the filter.
#[component]
fn Chip(condition: Condition) -> Element {
    let ui = use_context::<Ui>();
    let field = condition.field();
    let operator = condition.operator().name();
    let tested = match &condition {
        Condition::Is(value) => value.to_string(),
        Condition::IsAnyOf(values) | Condition::IsNot(values) => {
            values.iter().map(ToString::to_string).collect::<Vec<_>>().join(", ")
        }
        Condition::Matches(_, pattern) => pattern.to_string(),
    };
    let remove = move |_| {
        let mut live = ui.live;
        live.write().clear_filter(field);
        ui.changed();
    };
    rsx! {
        div { class: "chip",
            span { class: "f", "{field}" }
            span { class: "o", "{operator}" }
            span { class: "v", "{tested}" }
            span { class: "x", title: "Take the condition away", onclick: remove, Icon { shape: Shape::Close, size: 12 } }
        }
    }
}

/// A value that adds itself to the filter when someone clicks it.
#[component]
fn Value(value: FieldValue, class: &'static str, children: Element) -> Element {
    let ui = use_context::<Ui>();
    let on = ui.live.read().filter().holds(&value);
    let toggle = move |event: MouseEvent| {
        event.stop_propagation();
        let mut live = ui.live;
        live.write().toggle(value.clone());
        ui.changed();
    };
    rsx! {
        span { class: if on { "{class} ff on" } else { "{class} ff" }, onclick: toggle, {children} }
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
        div { class: if selected { "srow selected" } else { "srow" }, onclick: inspect,
            span { class: "mono t4", "{time}" }
            div { class: "who",
                if let Some(client) = client {
                    Value { value: client.clone(), class: "name", "{client}" }
                } else {
                    span { class: "t4", "anonymous" }
                }
                Value { value: connection.clone(), class: "mono t4", "{connection}" }
            }
            span {
                if let Some((endpoint, short)) = endpoint {
                    Value { value: endpoint, class: "mono t4", "{short}" }
                }
            }
            div {
                if let Some(api) = api {
                    Value { value: FieldValue::from(api), class: "", ApiPill { api, version } }
                }
            }
            div { class: "det",
                for (topic, partitions) in topics {
                    Value { key: "{topic}", value: FieldValue::from(topic.clone()), class: "tp", "{topic}" }
                    if !partitions.is_empty() {
                        span { class: "pp", "[{partitions}]" }
                    }
                }
                if let Some(group) = group {
                    span { class: "t5", "·" }
                    Value { value: group.clone(), class: "", "group {group}" }
                }
                if let Some(transaction) = transaction {
                    span { class: "t5", "·" }
                    Value { value: transaction.clone(), class: "", "{transaction}" }
                }
            }
            div { class: "lat",
                if let Some(latency) = latency { "{latency}" } else { span { class: "t4", "no answer" } }
            }
            Outcome { call: call.clone() }
        }
    }
}

/// What the client received: the lies that applied, or the answer of the backend.
#[component]
fn Outcome(call: Call) -> Element {
    let verdict = call.verdict();
    let lies: Vec<(&'static str, String)> = verdict
        .lies()
        .iter()
        .map(|lie| (lie_color(lie.lie()), lie.to_string()))
        .collect();
    let answer = delivered(call.delivered());
    rsx! {
        div { class: "out",
            if lies.is_empty() {
                span { class: if answer == "ok" { "ok" } else { "err" }, "{answer}" }
            }
            for (color, text) in lies {
                LieBadge { color, text }
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
        div { class: "ph", "Sessions" span { class: "t4", "{running.sessions.len()}" } }
        if groups.is_empty() {
            div { class: "note pad", "No client is connected to {environment}." }
        }
        for (value, label, rows) in groups {
            div { key: "{label}", class: "group",
                div { class: "cl",
                    if let Some(value) = value {
                        Value { value, class: "name", "{label}" }
                    } else {
                        span { class: "t4", "{label}" }
                    }
                }
                for row in rows {
                    Session { key: "{row.connection}", row }
                }
            }
        }
        div { class: "ph static", "Endpoints" span { class: "t4", "{kind}" } }
        for (endpoint, address) in endpoints {
            div { key: "{endpoint}", class: "cn endpoint",
                span { class: "mono", "{endpoint}" }
                span { class: "mono t4", "{address}" }
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
        div { class: "cn",
            Value { value: connection, class: "mono", "{row.connection}" }
            span { class: "mono t4", "→ {row.endpoint}" }
            span { class: "n", "{row.calls}" }
        }
        if let Some(software) = row.software {
            div { class: "sub", "{software}" }
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
            div { class: "ph", "Calls per API" }
            div { class: "note pad", "The first statistics come in a moment." }
        };
    };
    let mut apis: Vec<(ApiKey, u64)> = stats.apis.iter().map(|(api, calls)| (*api, *calls)).collect();
    apis.sort_by_key(|(_, calls)| std::cmp::Reverse(*calls));
    let most = apis.first().map_or(1, |(_, calls)| (*calls).max(1));
    let total = stats.calls;
    let missed = stats.missed_events;
    rsx! {
        div { class: "ph", "Calls per API" span { class: "t4", "{total} since the start" } }
        if missed > 0 {
            div { class: "note pad", "{missed} events went uncounted: every number is the least it can be." }
        }
        div { class: "facet",
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
    let color = api_color(api);
    let toggle = move |_| {
        let mut live = ui.live;
        live.write().toggle(value.clone());
        ui.changed();
    };
    rsx! {
        div { class: if on { "fv on" } else { "fv" }, onclick: toggle,
            span { class: "bar", style: "width:{width}%" }
            span { class: "ck", if on { Icon { shape: Shape::Check, size: 10 } } }
            span { class: "sq", style: "background:{color}" }
            span { class: "nm", "{api}" }
            span { class: "c", "{calls}" }
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
        div { class: "sh-h",
            div { class: "row",
                if let Some(api) = api { ApiPill { api, version } }
                span { class: "mono t4", "call {call.id()}" }
                span { class: "sp" }
                button { class: "ib", title: "Close", onclick: close, Icon { shape: Shape::Close } }
            }
            div { class: "mono t4 meta", "{time} · {call.connection()} · waited {waited}" }
        }
        div { class: "vcard",
            h5 {
                span { class: if lied { "sdot lied" } else { "sdot pass" } }
                if lied { "Lied" } else { "Pass" }
                span { class: "t4 light", "{verdict_text}" }
            }
            div { class: "tv",
                span { "Truth" }
                span { class: "mono", "{truth}" }
                span { "Delivered" }
                span { class: "mono", "{received}" }
                if let Some(backend) = backend {
                    span { "Backend" }
                    span { class: "mono", "took {backend}" }
                }
            }
            if lines.is_empty() {
                div { class: "ex note", "No armed rule matched this call." }
            }
            for line in lines {
                ExplanationLine { key: "{line.rule()}", line, running: running.clone() }
            }
            if let Some(failure) = failure {
                div { class: "ex note failed", "GaslightMQ failed on this call: {failure}" }
            }
        }
        for (title, fields) in groups {
            if !fields.is_empty() {
                div { class: "fgrp", "{title}" }
                for (field, text) in fields {
                    div { key: "{field}", class: "fld",
                        span { class: "k", "{field}" }
                        span { class: "val", "{text}" }
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
            .then(|| (lie_color(fate.lie().lie()), fate.lie().to_string()))
    });
    rsx! {
        div { class: "ex",
            div { class: "nm",
                if let Some(status) = &status {
                    StateIcon { state: status.state, size: 12 }
                }
                span { class: "ellipsis", "{label}" }
                span { class: "sp" }
                if let Some((color, text)) = lie {
                    LieBadge { color, text }
                }
            }
            div { class: "steps",
                for status in statuses {
                    span { class: step_class(status), "{status.name()}" }
                }
            }
            div { class: "mono t3 line", "{text}" }
        }
    }
}

/// How a step of an explanation looks: an applied lie stands out, a lost one warns.
fn step_class(status: ExplanationStatus) -> &'static str {
    match status {
        ExplanationStatus::Applied => "st ok",
        ExplanationStatus::Shadowed | ExplanationStatus::PreEmpted => "st sh",
        ExplanationStatus::Matched | ExplanationStatus::Rolled | ExplanationStatus::Fired => "st",
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
