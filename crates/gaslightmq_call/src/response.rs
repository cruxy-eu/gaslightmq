//! A response as a call record keeps it, and what the client received.

use std::collections::BTreeMap;
use std::fmt;

use gaslightmq_field::Part;
use gaslightmq_kernel::ErrorCode;

/// How a response answers the call, or one part of it: without an error, or with an error code.
///
/// ```
/// use gaslightmq_call::prelude::*;
/// use gaslightmq_kernel::prelude::*;
///
/// assert!(Answer::Ok.is_ok());
/// assert_eq!(Answer::from(ErrorCode::NOT_LEADER_OR_FOLLOWER).error(), Some(ErrorCode::NOT_LEADER_OR_FOLLOWER));
/// assert_eq!(Answer::from(None), Answer::Ok);
/// ```
///
/// With the `serde` feature, an answer serializes as the text `ok`, or as a map with the one key `error`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "lowercase")
)]
pub enum Answer {
    /// The response carries no error.
    Ok,
    /// The response carries this error code.
    Error(ErrorCode),
}

impl Answer {
    /// Whether the response carries no error.
    pub const fn is_ok(self) -> bool {
        matches!(self, Answer::Ok)
    }

    /// The error code, when the response carries one.
    pub const fn error(self) -> Option<ErrorCode> {
        match self {
            Answer::Ok => None,
            Answer::Error(code) => Some(code),
        }
    }
}

impl From<ErrorCode> for Answer {
    fn from(code: ErrorCode) -> Self {
        Answer::Error(code)
    }
}

/// No error code is an answer without an error.
impl From<Option<ErrorCode>> for Answer {
    fn from(code: Option<ErrorCode>) -> Self {
        code.map_or(Answer::Ok, Answer::Error)
    }
}

/// Writes `ok`, or the error code.
impl fmt::Display for Answer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Answer::Ok => f.write_str("ok"),
            Answer::Error(code) => fmt::Display::fmt(code, f),
        }
    }
}

/// A response as a call record keeps it: how it answers the call, and each part that has an answer of its own.
///
/// A record holds no frame and no payload, so this is all it says about a response.
/// A Produce response answers every partition by itself, and a `Heartbeat` response has one answer.
///
/// A call record uses it for what the client received, and for what the real cluster answered.
///
/// ```
/// use gaslightmq_call::prelude::*;
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
///
/// let [three, four]: [Part; 2] = ["orders.eu/3".parse()?, "orders.eu/4".parse()?];
/// let response = ResponseSummary::new(Answer::Ok)
///     .with_part(three.clone(), ErrorCode::NOT_LEADER_OR_FOLLOWER)
///     .with_part(four.clone(), Answer::Ok);
///
/// assert_eq!(response.part(&three), Some(Answer::Error(ErrorCode::NOT_LEADER_OR_FOLLOWER)));
/// assert_eq!(response.part(&four), Some(Answer::Ok));
/// assert!(!response.is_ok());
/// assert_eq!(response.to_string(), "ok; NOT_LEADER_OR_FOLLOWER on orders.eu/3");
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// With the `serde` feature, a response serializes as a map with `answer`, and with `parts` when it has any.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ResponseSummary {
    answer: Answer,
    #[cfg_attr(feature = "serde", serde(default, skip_serializing_if = "BTreeMap::is_empty"))]
    parts: BTreeMap<Part, Answer>,
}

impl ResponseSummary {
    /// A response that answers the call as a whole with `answer`, and no part by itself.
    pub fn new(answer: impl Into<Answer>) -> ResponseSummary {
        ResponseSummary {
            answer: answer.into(),
            parts: BTreeMap::new(),
        }
    }

    /// The response answers `part` by itself. A second answer for a part replaces the first.
    #[must_use]
    pub fn with_part(mut self, part: impl Into<Part>, answer: impl Into<Answer>) -> ResponseSummary {
        self.parts.insert(part.into(), answer.into());
        self
    }

    /// How the response answers the call as a whole.
    pub const fn answer(&self) -> Answer {
        self.answer
    }

    /// The answer `part` has of its own, or `None` when the response does not answer that part by itself.
    pub fn part(&self, part: &Part) -> Option<Answer> {
        self.parts.get(part).copied()
    }

    /// Every part with an answer of its own, in the order of the parts.
    pub fn parts(&self) -> impl Iterator<Item = (&Part, Answer)> {
        self.parts.iter().map(|(part, answer)| (part, *answer))
    }

    /// Whether the response carries no error, for the call and for every part.
    pub fn is_ok(&self) -> bool {
        self.answer.is_ok() && self.parts.values().all(|answer| answer.is_ok())
    }
}

/// Writes the answer of the call, and then every part with an error: `ok; NOT_LEADER_OR_FOLLOWER on orders.eu/3`.
impl fmt::Display for ResponseSummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.answer, f)?;
        for (part, answer) in &self.parts {
            if let Answer::Error(code) = answer {
                write!(f, "; {code} on {part}")?;
            }
        }
        Ok(())
    }
}

/// What the client actually received for a call.
///
/// | The client received | After |
/// |---|---|
/// | A response | Every call that no terminal lie ended. It is the truth, unless a lie changed it. |
/// | A response that nobody read | A response that GaslightMQ could not decode. The client got its bytes, and the record does not guess what they say. |
/// | Nothing | A silence, or a connection that ended while the call was in flight. |
/// | A closed connection | A hang-up. |
///
/// ```
/// use gaslightmq_call::prelude::*;
///
/// let delivered = Delivered::Response(ResponseSummary::new(Answer::Ok));
/// assert_eq!(delivered.response().map(ResponseSummary::is_ok), Some(true));
/// assert_eq!(Delivered::ConnectionClosed.to_string(), "connection closed");
/// ```
///
/// With the `serde` feature, it serializes as a map with the one key `response`,
/// or as the text `unread-response`, `nothing` or `connection-closed`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "kebab-case")
)]
pub enum Delivered {
    /// The client received this response.
    Response(ResponseSummary),
    /// The client received a response, and GaslightMQ could not read what it says.
    /// The record keeps no answer for it: a response that nobody read is not a response without an error.
    UnreadResponse,
    /// The client received nothing, and its connection stayed open.
    Nothing,
    /// The client found its connection closed instead of an answer.
    ConnectionClosed,
}

impl Delivered {
    /// The response, when the client received one.
    pub const fn response(&self) -> Option<&ResponseSummary> {
        match self {
            Delivered::Response(response) => Some(response),
            Delivered::UnreadResponse | Delivered::Nothing | Delivered::ConnectionClosed => None,
        }
    }
}

impl From<ResponseSummary> for Delivered {
    fn from(response: ResponseSummary) -> Self {
        Delivered::Response(response)
    }
}

/// Writes the response, `unread response`, `nothing` or `connection closed`.
impl fmt::Display for Delivered {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Delivered::Response(response) => fmt::Display::fmt(response, f),
            Delivered::UnreadResponse => f.write_str("unread response"),
            Delivered::Nothing => f.write_str("nothing"),
            Delivered::ConnectionClosed => f.write_str("connection closed"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn part(text: &str) -> Part {
        text.parse().unwrap()
    }

    #[test]
    fn an_answer_is_ok_or_an_error_code() {
        let error = Answer::from(ErrorCode::REQUEST_TIMED_OUT);

        assert!(!error.is_ok());
        assert_eq!(error.error(), Some(ErrorCode::REQUEST_TIMED_OUT));
        assert_eq!(error.to_string(), "REQUEST_TIMED_OUT");
        assert_eq!(Answer::Ok.error(), None);
        assert_eq!(Answer::Ok.to_string(), "ok");
        assert_eq!(Answer::from(Some(ErrorCode::REQUEST_TIMED_OUT)), error);
    }

    #[test]
    fn a_response_answers_the_call_and_some_parts_by_themselves() {
        let response = ResponseSummary::new(Answer::Ok).with_part(part("orders.eu/3"), Answer::Ok);

        assert_eq!(response.answer(), Answer::Ok);
        assert_eq!(response.part(&part("orders.eu/3")), Some(Answer::Ok));
        assert_eq!(response.part(&part("orders.eu/4")), None);
        assert!(response.is_ok());
        assert_eq!(response.to_string(), "ok");
    }

    #[test]
    fn a_second_answer_for_a_part_replaces_the_first() {
        let three = part("orders.eu/3");
        let response = ResponseSummary::new(Answer::Ok)
            .with_part(three.clone(), Answer::Ok)
            .with_part(three.clone(), ErrorCode::NOT_LEADER_OR_FOLLOWER);

        assert_eq!(
            response.parts().collect::<Vec<_>>(),
            [(&three, Answer::Error(ErrorCode::NOT_LEADER_OR_FOLLOWER))]
        );
        assert!(!response.is_ok());
    }

    #[test]
    fn a_response_with_an_error_for_the_call_is_not_ok() {
        let response = ResponseSummary::new(ErrorCode::COORDINATOR_NOT_AVAILABLE);

        assert!(!response.is_ok());
        assert_eq!(response.to_string(), "COORDINATOR_NOT_AVAILABLE");
    }

    #[test]
    fn only_a_response_is_a_response() {
        let response = ResponseSummary::new(Answer::Ok);

        assert_eq!(Delivered::from(response.clone()).response(), Some(&response));
        assert_eq!(Delivered::Nothing.response(), None);
        assert_eq!(Delivered::ConnectionClosed.response(), None);
        assert_eq!(Delivered::UnreadResponse.response(), None);
        assert_eq!(Delivered::UnreadResponse.to_string(), "unread response");
        assert_eq!(Delivered::Nothing.to_string(), "nothing");
        assert_eq!(Delivered::from(response).to_string(), "ok");
    }
}
