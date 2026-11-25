//! The small pieces that every view draws: icons, the pill of an API, the badge of a lie, the state of a rule.

use dioxus::prelude::*;
use gaslightmq_control_api::kernel::ApiKey;
use gaslightmq_control_api::lie::{LieDefinition, LieKind};
use gaslightmq_control_api::rule::RuleState;
use gaslightmq_control_api::surface::PadState;

use gaslightmq_control_api::selector::Condition;

use crate::app::Ui;
use crate::style::{CHIP, CHIP_FIELD, CHIP_OPERATOR, CHIP_REMOVE, CHIP_VALUE};

/// The shapes of the icons, as the paths of a 16 by 16 box.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shape {
    Record,
    Zap,
    Grid,
    Users,
    Check,
    Close,
    Globe,
    Play,
    Pause,
    Chevron,
}

impl Shape {
    fn paths(self) -> &'static str {
        match self {
            Shape::Record => {
                r#"<circle cx="8" cy="8" r="5.5"/><circle cx="8" cy="8" r="2.2" fill="currentColor" stroke="none"/>"#
            }
            Shape::Zap => r#"<path d="M8.8 1.8 3.5 9h4l-.6 5.2L12.5 7h-4z"/>"#,
            Shape::Grid => {
                r#"<rect x="2.5" y="2.5" width="4.5" height="4.5" rx="1"/><rect x="9" y="2.5" width="4.5" height="4.5" rx="1"/><rect x="2.5" y="9" width="4.5" height="4.5" rx="1"/><rect x="9" y="9" width="4.5" height="4.5" rx="1"/>"#
            }
            Shape::Users => {
                r#"<circle cx="6" cy="5.5" r="2.5"/><path d="M1.8 13.5c.5-2.3 2.2-3.5 4.2-3.5s3.7 1.2 4.2 3.5"/><path d="M10.5 3.2a2.4 2.4 0 0 1 0 4.6M12 10.2c1.2.5 2 1.6 2.3 3.3"/>"#
            }
            Shape::Check => r#"<path d="m3.5 8.5 3 3 6-7"/>"#,
            Shape::Close => r#"<path d="m4.5 4.5 7 7M11.5 4.5l-7 7"/>"#,
            Shape::Globe => {
                r#"<circle cx="8" cy="8" r="5.5"/><path d="M2.5 8h11M8 2.5c1.6 1.6 2.3 3.5 2.3 5.5S9.6 11.9 8 13.5C6.4 11.9 5.7 10 5.7 8S6.4 4.1 8 2.5z"/>"#
            }
            Shape::Play => r#"<path d="M5.5 3.8v8.4l6.5-4.2z"/>"#,
            Shape::Pause => r#"<path d="M6 4v8M10 4v8"/>"#,
            Shape::Chevron => r#"<path d="m4.5 6.5 3.5 3.5 3.5-3.5"/>"#,
        }
    }
}

/// An icon in the color of the text around it.
#[component]
pub fn Icon(shape: Shape, #[props(default = 14)] size: u32) -> Element {
    rsx! {
        svg {
            class: "block shrink-0",
            width: "{size}",
            height: "{size}",
            view_box: "0 0 16 16",
            fill: "none",
            stroke: "currentColor",
            stroke_width: "1.5",
            stroke_linecap: "round",
            stroke_linejoin: "round",
            dangerous_inner_html: shape.paths(),
        }
    }
}

/// What an API is for, as its color says it: producing, fetching, metadata, groups, transactions, or the rest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiTone {
    Produce,
    Fetch,
    Metadata,
    Group,
    Transaction,
    Admin,
}

impl ApiTone {
    /// The tone of an API, by its key.
    pub fn of(api: ApiKey) -> Self {
        match api.value() {
            0 => ApiTone::Produce,
            1 | 2 => ApiTone::Fetch,
            3 | 18 | 60 | 75 => ApiTone::Metadata,
            8..=16 | 42 | 47 | 68 | 69 => ApiTone::Group,
            22..=28 => ApiTone::Transaction,
            _ => ApiTone::Admin,
        }
    }

    /// The classes of a dot in the tone.
    pub const fn dot(self) -> &'static str {
        match self {
            ApiTone::Produce => "inline-block size-1.5 shrink-0 rounded-xs bg-api-produce",
            ApiTone::Fetch => "inline-block size-1.5 shrink-0 rounded-xs bg-api-fetch",
            ApiTone::Metadata => "inline-block size-1.5 shrink-0 rounded-xs bg-api-metadata",
            ApiTone::Group => "inline-block size-1.5 shrink-0 rounded-xs bg-api-group",
            ApiTone::Transaction => "inline-block size-1.5 shrink-0 rounded-xs bg-api-transaction",
            ApiTone::Admin => "inline-block size-1.5 shrink-0 rounded-xs bg-api-admin",
        }
    }
}

/// The kind of a lie, as its color says it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tone {
    Refusal,
    Delay,
    Throttle,
    Silence,
    HangUp,
    Other,
}

impl Tone {
    /// The tone of a lie: a timing lie that only throttles has a tone of its own.
    pub fn of(lie: &LieDefinition) -> Self {
        match lie {
            LieDefinition::Timing(timing) if timing.delay().as_millis() == 0 => Tone::Throttle,
            _ => Tone::of_kind(lie.kind()),
        }
    }

    /// The tone of a kind of lie, without its parameters.
    pub fn of_kind(kind: LieKind) -> Self {
        match kind {
            LieKind::Refusal => Tone::Refusal,
            LieKind::Timing => Tone::Delay,
            LieKind::Silence => Tone::Silence,
            LieKind::HangUp => Tone::HangUp,
            _ => Tone::Other,
        }
    }

    /// The classes of a badge in the tone: its text, and a soft surface.
    pub const fn badge(self) -> &'static str {
        match self {
            Tone::Refusal => "text-lie-refusal bg-lie-refusal/13",
            Tone::Delay => "text-lie-delay bg-lie-delay/13",
            Tone::Throttle => "text-lie-throttle bg-lie-throttle/13",
            Tone::Silence => "text-lie-silence bg-lie-silence/13",
            Tone::HangUp => "text-lie-hangup bg-lie-hangup/13",
            Tone::Other => "text-lie-other bg-lie-other/13",
        }
    }

    /// The class of a surface in the tone.
    pub const fn fill(self) -> &'static str {
        match self {
            Tone::Refusal => "bg-lie-refusal",
            Tone::Delay => "bg-lie-delay",
            Tone::Throttle => "bg-lie-throttle",
            Tone::Silence => "bg-lie-silence",
            Tone::HangUp => "bg-lie-hangup",
            Tone::Other => "bg-lie-other",
        }
    }

    /// The classes of a pad in the tone, by what the pad shows.
    pub const fn pad(self, state: PadState) -> &'static str {
        match (self, state) {
            (Tone::Refusal, PadState::Empty) => "border-lie-refusal/20 bg-lie-refusal/7 hover:bg-lie-refusal/14",
            (Tone::Refusal, PadState::Disarmed) => {
                "border-dashed border-lie-refusal/50 bg-lie-refusal/7 hover:bg-lie-refusal/14"
            }
            (Tone::Refusal, PadState::Armed) => "border-lie-refusal bg-lie-refusal/30 shadow-lg shadow-lie-refusal/40",
            (Tone::Delay, PadState::Empty) => "border-lie-delay/20 bg-lie-delay/7 hover:bg-lie-delay/14",
            (Tone::Delay, PadState::Disarmed) => {
                "border-dashed border-lie-delay/50 bg-lie-delay/7 hover:bg-lie-delay/14"
            }
            (Tone::Delay, PadState::Armed) => "border-lie-delay bg-lie-delay/30 shadow-lg shadow-lie-delay/40",
            (Tone::Throttle, PadState::Empty) => "border-lie-throttle/20 bg-lie-throttle/7 hover:bg-lie-throttle/14",
            (Tone::Throttle, PadState::Disarmed) => {
                "border-dashed border-lie-throttle/50 bg-lie-throttle/7 hover:bg-lie-throttle/14"
            }
            (Tone::Throttle, PadState::Armed) => {
                "border-lie-throttle bg-lie-throttle/30 shadow-lg shadow-lie-throttle/40"
            }
            (Tone::Silence, PadState::Empty) => "border-lie-silence/20 bg-lie-silence/7 hover:bg-lie-silence/14",
            (Tone::Silence, PadState::Disarmed) => {
                "border-dashed border-lie-silence/50 bg-lie-silence/7 hover:bg-lie-silence/14"
            }
            (Tone::Silence, PadState::Armed) => "border-lie-silence bg-lie-silence/30 shadow-lg shadow-lie-silence/40",
            (Tone::HangUp, PadState::Empty) => "border-lie-hangup/20 bg-lie-hangup/7 hover:bg-lie-hangup/14",
            (Tone::HangUp, PadState::Disarmed) => {
                "border-dashed border-lie-hangup/50 bg-lie-hangup/7 hover:bg-lie-hangup/14"
            }
            (Tone::HangUp, PadState::Armed) => "border-lie-hangup bg-lie-hangup/30 shadow-lg shadow-lie-hangup/40",
            (Tone::Other, PadState::Empty) => "border-lie-other/20 bg-lie-other/7 hover:bg-lie-other/14",
            (Tone::Other, PadState::Disarmed) => {
                "border-dashed border-lie-other/50 bg-lie-other/7 hover:bg-lie-other/14"
            }
            (Tone::Other, PadState::Armed) => "border-lie-other bg-lie-other/30 shadow-lg shadow-lie-other/40",
        }
    }
}

/// The name of an API in a pill of its color, with its version when it has one.
#[component]
pub fn ApiPill(api: ApiKey, version: Option<String>) -> Element {
    let name = api.to_string();
    let dot = ApiTone::of(api).dot();
    rsx! {
        span { class: "inline-flex h-5 shrink-0 items-center gap-1.5 whitespace-nowrap rounded-md border border-edge bg-hover pr-1.75 pl-1.5 text-caption font-medium text-body",
            i { class: dot }
            "{name}"
            if let Some(version) = version {
                span { class: "font-mono text-micro font-normal text-subtle", "v{version}" }
            }
        }
    }
}

/// A badge in the color of a lie, with what it says.
#[component]
pub fn LieBadge(tone: Tone, text: String) -> Element {
    rsx! {
        span { class: "inline-flex h-5 max-w-full shrink-0 items-center gap-1.5 overflow-hidden text-ellipsis whitespace-nowrap rounded-md px-1.75 text-caption font-medium {tone.badge()}",
            i { class: "inline-block size-1.5 shrink-0 rounded-full {tone.fill()}" }
            "{text}"
        }
    }
}

/// The state of a rule as a small round icon: armed, disarmed or a draft.
#[component]
pub fn StateIcon(state: RuleState, #[props(default = 14)] size: u32) -> Element {
    let (class, paths) = match state {
        RuleState::Armed => (
            "block shrink-0 text-armed",
            r#"<circle cx="7" cy="7" r="5.5" stroke="currentColor" stroke-width="1.5"/><circle cx="7" cy="7" r="2.6" fill="currentColor"/>"#,
        ),
        RuleState::Disarmed => (
            "block shrink-0 text-muted",
            r#"<circle cx="7" cy="7" r="5.5" stroke="currentColor" stroke-width="1.5"/><path d="M5.6 5v4M8.4 5v4" stroke="currentColor" stroke-width="1.4" stroke-linecap="round"/>"#,
        ),
        RuleState::Draft => (
            "block shrink-0 text-subtle",
            r#"<circle cx="7" cy="7" r="5.5" stroke="currentColor" stroke-width="1.5" stroke-dasharray="2 2.2"/>"#,
        ),
    };
    rsx! {
        svg {
            class,
            width: "{size}",
            height: "{size}",
            view_box: "0 0 14 14",
            fill: "none",
            dangerous_inner_html: paths,
        }
    }
}

/// The notices on the screen, the newest at the bottom.
#[component]
pub fn Notices() -> Element {
    let ui = use_context::<Ui>();
    let notices = ui.notices.read();
    rsx! {
        div { class: "fixed right-5.5 bottom-5.5 z-100 flex flex-col gap-2",
            for notice in notices.iter() {
                div {
                    key: "{notice.id}",
                    class: "flex max-w-115 items-center gap-2.5 rounded-lg border border-edge-strong bg-popover px-3.5 py-2.5 text-compact text-strong shadow-xl shadow-scrim",
                    span { class: if notice.failed { "text-danger" } else { "text-armed" },
                        Icon { shape: if notice.failed { Shape::Close } else { Shape::Check } }
                    }
                    div { "{notice.text}" }
                }
            }
        }
    }
}

/// One condition of a selector, as a chip: the field, the operator and what it tests.
/// With `onremove`, the chip has a button that takes the condition away.
#[component]
pub fn ConditionChip(condition: Condition, onremove: Option<EventHandler>) -> Element {
    let field = condition.field();
    let operator = condition.operator().name();
    let tested = match &condition {
        Condition::Is(value) => value.to_string(),
        Condition::IsAnyOf(values) | Condition::IsNot(values) => {
            values.iter().map(ToString::to_string).collect::<Vec<_>>().join(", ")
        }
        Condition::Matches(_, pattern) => pattern.to_string(),
    };
    rsx! {
        div { class: CHIP,
            span { class: CHIP_FIELD, "{field}" }
            span { class: CHIP_OPERATOR, "{operator}" }
            span { class: CHIP_VALUE, "{tested}" }
            if let Some(onremove) = onremove {
                span { class: CHIP_REMOVE, title: "Take the condition away", onclick: move |_| onremove.call(()),
                    Icon { shape: Shape::Close, size: 12 }
                }
            }
        }
    }
}

/// The side panel that inspects one thing: a call in the traffic, a rule in the Rules view.
/// It scrolls on its own, on the inset surface.
#[component]
pub fn InspectorPanel(#[props(default = "w-105")] width: &'static str, children: Element) -> Element {
    rsx! {
        div { class: "min-h-0 shrink-0 overflow-auto border-l border-edge bg-inset {width}", {children} }
    }
}
