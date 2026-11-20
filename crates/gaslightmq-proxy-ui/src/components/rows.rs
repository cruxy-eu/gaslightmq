//! The fault table: one row per Kafka API, in catalog order.

use dioxus::prelude::*;
use gaslightmq_proxy_api::{ApiCatalogEntry, ApiRow, Percent, error_name};

use crate::state::Ctl;

#[component]
pub fn Rows() -> Element {
    let state = use_context::<Ctl>().state;
    let catalog = state.catalog.read();
    let rows = state.rows.read();

    rsx! {
        section {
            h2 { "Faults" }
            if catalog.is_empty() {
                p { "Waiting for the proxy…" }
            } else {
                table {
                    thead {
                        tr {
                            th { "API" }
                            th { "Topic" }
                            th { "Error (on, %, code)" }
                            th { "Delay (on, %, ms)" }
                            th { "Blackhole (on, %)" }
                        }
                    }
                    tbody {
                        for entry in catalog.iter() {
                            if let Some(row) = rows.iter().find(|r| r.api_key == entry.api_key) {
                                Row { key: "{entry.api_key}", entry: entry.clone(), row: row.clone() }
                            }
                        }
                    }
                }
            }
        }
    }
}

#[component]
fn Row(entry: ApiCatalogEntry, row: ApiRow) -> Element {
    let ctl = use_context::<Ctl>();
    let api_key = entry.api_key;
    let no_error = !entry.supports_error;
    let code = row.error.code;
    let code_listed = entry.error_codes.iter().any(|c| c.code == code);
    let unlisted_code = format!("{code} {}", error_name(code).unwrap_or("?"));
    let topic_placeholder = if entry.supports_topic { "all topics" } else { "n/a" };

    rsx! {
        tr { class: if row.is_active() { "active" },
            td { title: "API key {api_key}", "{entry.name}" }
            td {
                Field {
                    value: row.topic.clone().unwrap_or_default(),
                    placeholder: topic_placeholder,
                    disabled: !entry.supports_topic,
                    on_commit: move |text: String| {
                        let text = text.trim().to_string();
                        ctl.update_row(api_key, |r| r.topic = (!text.is_empty()).then_some(text));
                    },
                }
            }
            td {
                input {
                    r#type: "checkbox",
                    disabled: no_error,
                    checked: row.error.enabled,
                    onchange: move |e| ctl.update_row(api_key, |r| r.error.enabled = e.checked()),
                }
                PercentField {
                    value: row.error.probability,
                    disabled: no_error,
                    on_commit: move |p| ctl.update_row(api_key, |r| r.error.probability = p),
                }
                select {
                    disabled: no_error,
                    onchange: move |e| {
                        if let Ok(code) = e.value().parse::<i16>() {
                            ctl.update_row(api_key, |r| r.error.code = code);
                        }
                    },
                    if !code_listed {
                        option { value: "{code}", selected: true, "{unlisted_code}" }
                    }
                    for c in entry.error_codes.iter() {
                        option { value: "{c.code}", selected: c.code == code, "{c.code} {c.name}" }
                    }
                }
            }
            td {
                input {
                    r#type: "checkbox",
                    checked: row.delay.enabled,
                    onchange: move |e| ctl.update_row(api_key, |r| r.delay.enabled = e.checked()),
                }
                PercentField {
                    value: row.delay.probability,
                    on_commit: move |p| ctl.update_row(api_key, |r| r.delay.probability = p),
                }
                Field {
                    kind: "number",
                    value: row.delay.ms.to_string(),
                    on_commit: move |text: String| {
                        if let Ok(ms) = text.trim().parse::<u64>() {
                            ctl.update_row(api_key, |r| r.delay.ms = ms);
                        }
                    },
                }
            }
            td {
                input {
                    r#type: "checkbox",
                    checked: row.blackhole.enabled,
                    onchange: move |e| ctl.update_row(api_key, |r| r.blackhole.enabled = e.checked()),
                }
                PercentField {
                    value: row.blackhole.probability,
                    on_commit: move |p| ctl.update_row(api_key, |r| r.blackhole.probability = p),
                }
            }
        }
    }
}

/// A probability in percent; out-of-range input is clamped to `0..=100`.
#[component]
fn PercentField(
    value: Percent,
    #[props(default)] disabled: bool,
    on_commit: EventHandler<Percent>,
) -> Element {
    rsx! {
        Field {
            kind: "number",
            value: value.to_string(),
            disabled,
            on_commit: move |text: String| {
                if let Ok(p) = text.trim().parse::<i64>() {
                    on_commit.call(p.clamp(0, 100) as Percent);
                }
            },
        }
    }
}

/// An input that commits on change (Enter or blur). Typed text is kept as a
/// draft until then, so server echoes don't fight the cursor; after the
/// commit the field shows the model value again, which also undoes input
/// that was rejected or clamped.
#[component]
fn Field(
    value: String,
    #[props(default = "text")] kind: &'static str,
    #[props(default)] placeholder: &'static str,
    #[props(default)] disabled: bool,
    on_commit: EventHandler<String>,
) -> Element {
    let mut draft = use_signal(|| None::<String>);
    let shown = draft.read().clone().unwrap_or(value);

    rsx! {
        input {
            r#type: kind,
            class: kind,
            spellcheck: false,
            placeholder,
            disabled,
            value: shown,
            oninput: move |e| draft.set(Some(e.value())),
            onchange: move |e| {
                draft.set(None);
                on_commit.call(e.value());
            },
            onkeydown: move |e| {
                if e.key() == Key::Escape {
                    draft.set(None);
                }
            },
        }
    }
}
