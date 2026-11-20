//! The pad grid: one row per Kafka API, in catalog order.

use dioxus::prelude::*;
use gaslightmq_proxy_api::{ApiCatalogEntry, ApiRow, ApiTick, catalog::api};

use super::pad::{BlackholePadCell, DelayPadCell, ErrorPadCell};
use crate::state::{AppState, Ctl};

/// APIs the Restore workload depends on; highlighted in the grid.
const RESTORE_APIS: [i16; 3] = [api::DESCRIBE_CONFIGS, api::METADATA, api::PRODUCE];

#[component]
pub fn PadGrid() -> Element {
    let state = use_context::<Ctl>().state;
    let AppState { catalog, rows, tick, rates, .. } = state;

    let tick = tick.read();
    let seq = tick.as_ref().map_or(0, |t| t.seq);
    let rates = rates.read();
    let rows = rows.read();

    rsx! {
        section { class: "grid",
            div { class: "grid-head",
                span { "API" }
                span { class: "num", "REQ/S" }
                span { "TOPIC" }
                span { "ERROR" }
                span { "DELAY" }
                span { "BLACKHOLE" }
            }
            if catalog.read().is_empty() {
                div { class: "grid-empty", "waiting for proxy…" }
            }
            for entry in catalog.read().iter() {
                if let Some(row) = rows.iter().find(|r| r.api_key == entry.api_key) {
                    GridRow {
                        key: "{entry.api_key}",
                        entry: entry.clone(),
                        row: row.clone(),
                        tick: tick
                            .as_ref()
                            .and_then(|t| t.apis.iter().find(|a| a.api_key == entry.api_key))
                            .copied(),
                        seq,
                        rate: rates.get(&entry.api_key).copied().unwrap_or(0.0),
                    }
                }
            }
        }
    }
}

#[component]
fn GridRow(
    entry: ApiCatalogEntry,
    row: ApiRow,
    /// Activity in the latest tick, if any.
    tick: Option<ApiTick>,
    seq: u64,
    rate: f64,
) -> Element {
    let api_key = entry.api_key;
    let restore = RESTORE_APIS.contains(&api_key);
    let fired = |count: fn(&ApiTick) -> u64| tick.filter(|t| count(t) > 0).map(|_| seq);
    let active = tick.is_some_and(|t| t.requests > 0);
    let rate_label = match rate {
        r if r <= 0.0 => "·".to_string(),
        r if r < 10.0 => format!("{r:.1}"),
        r => format!("{r:.0}"),
    };

    rsx! {
        div {
            class: "grid-row",
            class: if restore { "restore" },
            class: if row.is_active() { "armed" },
            div { class: "api",
                span { class: "led", class: if rate > 0.0 { "warm" },
                    for s in active.then_some(seq) {
                        span { key: "{s}", class: "led-flash" }
                    }
                }
                span { class: "api-name", title: "API key {api_key}", "{entry.name}" }
            }
            div { class: "num rate", class: if rate <= 0.0 { "idle" }, "{rate_label}" }
            TopicField {
                api_key,
                topic: row.topic.clone(),
                supported: entry.supports_topic,
            }
            ErrorPadCell {
                api_key,
                pad: row.error,
                codes: entry.error_codes.clone(),
                supported: entry.supports_error,
                flash: fired(|t| t.errors),
            }
            DelayPadCell { api_key, pad: row.delay, flash: fired(|t| t.delays) }
            BlackholePadCell { api_key, pad: row.blackhole, flash: fired(|t| t.blackholes) }
        }
    }
}

/// Topic scope. Edits are local until committed with Enter or blur, so the
/// server's echo never fights the cursor.
#[component]
fn TopicField(api_key: i16, topic: Option<String>, supported: bool) -> Element {
    let ctl = use_context::<Ctl>();
    let mut draft = use_signal(|| None::<String>);
    let value = draft
        .read()
        .clone()
        .unwrap_or_else(|| topic.clone().unwrap_or_default());

    rsx! {
        input {
            class: "topic",
            r#type: "text",
            spellcheck: false,
            placeholder: if supported { "all topics" } else { "—" },
            disabled: !supported,
            value,
            oninput: move |e| draft.set(Some(e.value())),
            onchange: move |e| {
                let text = e.value().trim().to_string();
                let topic = (!text.is_empty()).then_some(text);
                draft.set(None);
                ctl.update_row(api_key, |r| r.topic = topic);
            },
            onkeydown: move |e| {
                if e.key() == Key::Escape {
                    draft.set(None);
                }
            },
        }
    }
}
