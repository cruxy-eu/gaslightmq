use std::collections::BTreeMap;
use std::fmt;

use gaslightmq_field::prelude::*;
use gaslightmq_kernel::prelude::*;

/// What a client is told failed: the whole call, some of its parts, or both, each with an error code.
///
/// | Built with | Fails |
/// |---|---|
/// | [`none`](Self::none) | Nothing. |
/// | [`whole_call`](Self::whole_call) | Everything the request asks, with one error. |
/// | [`of_parts`](Self::of_parts) | Those parts, each with its own error. The rest of the call does not fail. |
/// | [`whole_call`](Self::whole_call) and [`with_part`](Self::with_part) | Everything, and that part with an error of its own. |
///
/// An [`EditableRequest`](crate::EditableRequest) turns failures into a response:
/// [`error_response`](crate::EditableRequest::error_response) writes them down,
/// and [`merge`](crate::EditableRequest::merge) puts them into the response of the backend.
///
/// ```
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_wire_edit::prelude::*;
///
/// let three: Part = "orders.eu/3".parse()?;
/// let four: Part = "orders.eu/4".parse()?;
///
/// // One partition fails, and the rest of the call does not.
/// let failures = Failures::none().with_part(three.clone(), ErrorCode::NOT_LEADER_OR_FOLLOWER);
/// assert_eq!(failures.of(&three), Some(ErrorCode::NOT_LEADER_OR_FOLLOWER));
/// assert_eq!(failures.of(&four), None);
///
/// // The whole call fails, and one partition keeps an error of its own.
/// let failures = Failures::whole_call(ErrorCode::REQUEST_TIMED_OUT).with_part(three.clone(), ErrorCode::NOT_LEADER_OR_FOLLOWER);
/// assert_eq!(failures.of(&three), Some(ErrorCode::NOT_LEADER_OR_FOLLOWER));
/// assert_eq!(failures.of(&four), Some(ErrorCode::REQUEST_TIMED_OUT));
/// assert_eq!(failures.to_string(), "call: REQUEST_TIMED_OUT, orders.eu/3: NOT_LEADER_OR_FOLLOWER");
/// # Ok::<(), PartError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Failures {
    call: Option<ErrorCode>,
    parts: BTreeMap<Part, ErrorCode>,
}

impl Failures {
    /// Nothing fails.
    pub fn none() -> Self {
        Self::default()
    }

    /// Everything the request asks fails with `error`.
    pub fn whole_call(error: ErrorCode) -> Self {
        Self {
            call: Some(error),
            parts: BTreeMap::new(),
        }
    }

    /// Those parts fail, each with its error. A part that comes twice keeps its last error.
    pub fn of_parts(parts: impl IntoIterator<Item = (Part, ErrorCode)>) -> Self {
        Self {
            call: None,
            parts: parts.into_iter().collect(),
        }
    }

    /// What `target` covers fails with `error`: the whole call, or each part of the target.
    pub fn of_target(target: &Target, error: ErrorCode) -> Self {
        Self::none().with_target(target, error)
    }

    /// The same failures, and `part` fails with `error`.
    /// The part keeps this error when the whole call fails with another.
    #[must_use]
    pub fn with_part(mut self, part: impl Into<Part>, error: ErrorCode) -> Self {
        self.parts.insert(part.into(), error);
        self
    }

    /// The same failures, and everything else the request asks fails with `error`.
    #[must_use]
    pub fn with_whole_call(mut self, error: ErrorCode) -> Self {
        self.call = Some(error);
        self
    }

    /// The same failures, and what `target` covers fails with `error`.
    #[must_use]
    pub fn with_target(mut self, target: &Target, error: ErrorCode) -> Self {
        if target.is_call() {
            self.call = Some(error);
        }
        for part in target.parts() {
            self.parts.insert(part.clone(), error);
        }
        self
    }

    /// The error of `part`: its own, or else the error of the whole call.
    /// `None` means that the part does not fail.
    pub fn of(&self, part: &Part) -> Option<ErrorCode> {
        self.parts.get(part).copied().or(self.call)
    }

    /// The error of the whole call.
    /// `None` means that only the [parts](Self::parts) fail.
    pub const fn of_call(&self) -> Option<ErrorCode> {
        self.call
    }

    /// The parts that have an error of their own, in order.
    pub fn parts(&self) -> impl Iterator<Item = (&Part, ErrorCode)> {
        self.parts.iter().map(|(part, error)| (part, *error))
    }

    /// Whether nothing fails.
    pub fn is_empty(&self) -> bool {
        self.call.is_none() && self.parts.is_empty()
    }
}

/// Those parts fail, each with its error.
impl FromIterator<(Part, ErrorCode)> for Failures {
    fn from_iter<I: IntoIterator<Item = (Part, ErrorCode)>>(parts: I) -> Self {
        Self::of_parts(parts)
    }
}

/// Writes `call: REQUEST_TIMED_OUT, orders.eu/3: NOT_LEADER_OR_FOLLOWER`, or `nothing`.
impl fmt::Display for Failures {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_empty() {
            return f.write_str("nothing");
        }
        let call = self.call.map(|error| format!("call: {error}"));
        let parts = self.parts.iter().map(|(part, error)| format!("{part}: {error}"));
        let all: Vec<String> = call.into_iter().chain(parts).collect();
        f.write_str(&all.join(", "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn part(text: &str) -> Part {
        text.parse().unwrap()
    }

    #[test]
    fn nothing_fails_by_default() {
        let failures = Failures::none();

        assert!(failures.is_empty());
        assert_eq!(failures.of_call(), None);
        assert_eq!(failures.of(&part("orders.eu/3")), None);
        assert_eq!(failures.to_string(), "nothing");
    }

    #[test]
    fn the_whole_call_fails_every_part() {
        let failures = Failures::whole_call(ErrorCode::REQUEST_TIMED_OUT);

        assert!(!failures.is_empty());
        assert_eq!(failures.of_call(), Some(ErrorCode::REQUEST_TIMED_OUT));
        assert_eq!(failures.of(&part("orders.eu/3")), Some(ErrorCode::REQUEST_TIMED_OUT));
        assert_eq!(failures.parts().count(), 0);
    }

    #[test]
    fn a_part_fails_alone() {
        let failures: Failures = [(part("orders.eu/3"), ErrorCode::NOT_LEADER_OR_FOLLOWER)]
            .into_iter()
            .collect();

        assert_eq!(failures.of_call(), None);
        assert_eq!(
            failures.of(&part("orders.eu/3")),
            Some(ErrorCode::NOT_LEADER_OR_FOLLOWER)
        );
        assert_eq!(failures.of(&part("orders.eu/4")), None);
        assert_eq!(failures.of(&part("orders.eu")), None);
    }

    #[test]
    fn a_part_keeps_its_own_error_when_the_whole_call_fails() {
        let failures = Failures::of_parts([(part("orders.eu/3"), ErrorCode::NOT_LEADER_OR_FOLLOWER)])
            .with_whole_call(ErrorCode::REQUEST_TIMED_OUT);

        assert_eq!(
            failures.of(&part("orders.eu/3")),
            Some(ErrorCode::NOT_LEADER_OR_FOLLOWER)
        );
        assert_eq!(failures.of(&part("orders.eu/4")), Some(ErrorCode::REQUEST_TIMED_OUT));
    }

    #[test]
    fn a_part_that_comes_twice_keeps_its_last_error() {
        let failures = Failures::none()
            .with_part(part("orders.eu/3"), ErrorCode::NOT_LEADER_OR_FOLLOWER)
            .with_part(part("orders.eu/3"), ErrorCode::REQUEST_TIMED_OUT);

        assert_eq!(failures.of(&part("orders.eu/3")), Some(ErrorCode::REQUEST_TIMED_OUT));
        assert_eq!(failures.parts().count(), 1);
    }

    #[test]
    fn a_target_fails_what_it_covers() {
        let parts = Target::of_parts([part("orders.eu/3"), part("orders.eu/4")]).unwrap();

        let of_parts = Failures::of_target(&parts, ErrorCode::NOT_LEADER_OR_FOLLOWER);
        assert_eq!(of_parts.of_call(), None);
        assert_eq!(of_parts.parts().count(), 2);

        let of_call = Failures::of_target(&Target::Call, ErrorCode::REQUEST_TIMED_OUT);
        assert_eq!(of_call, Failures::whole_call(ErrorCode::REQUEST_TIMED_OUT));

        let both = of_call.with_target(&parts, ErrorCode::NOT_LEADER_OR_FOLLOWER);
        assert_eq!(
            both.to_string(),
            "call: REQUEST_TIMED_OUT, orders.eu/3: NOT_LEADER_OR_FOLLOWER, orders.eu/4: NOT_LEADER_OR_FOLLOWER"
        );
    }
}
