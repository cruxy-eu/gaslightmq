//! What the tests of combining share: short names for lies, and a fate as one line of text.

#![expect(
    clippy::unwrap_used,
    clippy::panic,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

use gaslightmq_clock::TimeSpan;
use gaslightmq_combine::prelude::*;
use gaslightmq_field::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_lie::prelude::*;

pub const BEFORE: Stage = Stage::BeforeForwarding;
pub const AFTER: Stage = Stage::AfterResponse;

pub fn millis(millis: u64) -> TimeSpan {
    TimeSpan::from_millis(millis)
}

pub fn part(text: &str) -> Part {
    text.parse().unwrap()
}

pub fn parts(texts: &[&str]) -> Target {
    Target::of_parts(texts.iter().map(|text| part(text))).unwrap()
}

/// The lie `id` at that position of the priority order, where 0 is the highest.
pub fn lie(id: &'static str, position: usize, claim: impl Into<Claim>) -> FiredLie<&'static str> {
    FiredLie::new(id, Priority::new(position), claim)
}

/// The claim of a lie with a definition, on what a selector matched.
pub fn claim(lie: impl Into<LieDefinition>, matched: Target) -> Claim {
    Claim::try_from(&lie.into().on(matched)).unwrap()
}

/// A refusal before forwarding.
pub fn refusal(target: Target) -> Claim {
    claim(RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER), target)
}

/// A refusal after the response.
pub fn late_refusal(target: Target) -> Claim {
    claim(RefusalLie::new(ErrorCode::REQUEST_TIMED_OUT).at(AFTER), target)
}

/// A delay after the response.
pub fn delay(delay: u64) -> TimingLie {
    TimingLie::new().with_delay(millis(delay))
}

pub fn silence() -> Claim {
    claim(LieDefinition::Silence, Target::Call)
}

/// A hang-up before forwarding.
pub fn hang_up() -> Claim {
    claim(HangUpLie::new(), Target::Call)
}

/// A hang-up after the response.
pub fn late_hang_up() -> Claim {
    claim(HangUpLie::new().at(AFTER), Target::Call)
}

/// A rewrite, which no definition writes yet.
pub fn rewrite(aspect: Aspect, target: Target) -> Claim {
    Claim::Rewrite { aspect, target }
}

/// A stale high watermark.
pub fn stale_watermark(target: Target) -> Claim {
    rewrite(Aspect::HighWatermark, target)
}

/// The fate of the lie `id` as one line: `applied on orders.eu/4; shadowed on orders.eu/3 by leader-flap`.
pub fn fate(call: &Combination<&'static str>, id: &'static str) -> String {
    told(call.fate_of(&id).unwrap_or_else(|| panic!("no lie {id:?} fired")))
}

/// A fate as one line.
pub fn told(fate: &Fate<&'static str>) -> String {
    let mut lines = Vec::new();
    if let Some(target) = fate.applied() {
        lines.push(format!("applied on {target}"));
    }
    for loss in fate.shadowed() {
        lines.push(format!("shadowed on {} by {}", loss.on(), loss.to()));
    }
    for loss in fate.pre_empted() {
        lines.push(format!("pre-empted on {} by {}", loss.on(), loss.to()));
    }
    if lines.is_empty() {
        "nothing".to_owned()
    } else {
        lines.join("; ")
    }
}

/// The identity of every lie that fired, in the order of the fates.
pub fn ids(call: &Combination<&'static str>) -> Vec<&'static str> {
    call.fates().iter().map(|fate| *fate.id()).collect()
}

/// The lies that hold parts, as `orders.eu/3: leader-flap`.
pub fn held(winners: &Winners<&'static str>) -> Vec<String> {
    winners
        .of_call()
        .map(|id| format!("call: {id}"))
        .into_iter()
        .chain(winners.parts().map(|(part, id)| format!("{part}: {id}")))
        .collect()
}
