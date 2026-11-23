//! The fields of one call as its rules see them, and what a response says.

use std::collections::BTreeSet;

use gaslightmq_call::{Answer, ResponseSummary};
use gaslightmq_field::prelude::*;
use gaslightmq_wire::Response;
use gaslightmq_wire_edit::{EditableRequest, SurgeryError};

/// What a response says: about the call as a whole, and about each part it answers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Answers {
    call: Answer,
    parts: Vec<(Part, Answer)>,
}

impl Answers {
    /// Reads what `response` says about the request `asked`.
    ///
    /// The answer of the call is the error code of the response itself.
    /// An error that only stands in an entry that is no part, such as a group of a `DescribeGroups`, is not read.
    ///
    /// # Errors
    ///
    /// The [`SurgeryError`] of a response that answers another request, or that does not decode.
    pub(crate) fn read(asked: &EditableRequest, response: &Response) -> Result<Self, SurgeryError> {
        let call = Answer::from(asked.error_of(response)?);
        let parts = asked.parts_of(response)?;
        let parts = parts.iter().map(|part| {
            let answer = match part.own(Field::ErrorCode) {
                Some(Reading::Present(FieldValue::ErrorCode(code))) => Answer::Error(code),
                _ => Answer::Ok,
            };
            (part.part().clone(), answer)
        });
        Ok(Self {
            call,
            parts: parts.collect(),
        })
    }

    /// The answer of `part`, when the response answers it.
    fn of(&self, part: &Part) -> Option<Answer> {
        let answered = self.parts.iter().find(|(answered, _)| answered == part);
        answered.map(|(_, answer)| *answer)
    }

    /// The response as a call record keeps it.
    pub(crate) fn summary(&self) -> ResponseSummary {
        let summary = ResponseSummary::new(self.call);
        self.parts.iter().fold(summary, |summary, (part, answer)| {
            summary.with_part(part.clone(), *answer)
        })
    }
}

/// The fields of one call: what its session and its request say, and its parts.
///
/// The truth changes during a call, so this type gives the fields at each moment:
///
/// | Moment | Fields |
/// |---|---|
/// | The request pass | [`awaiting_truth`](Self::awaiting_truth): every response field is pending. |
/// | The response pass | [`with_truth`](Self::with_truth): the answer of the backend, for the call and for each part. |
/// | A truth that nobody could read | [`with_unread_truth`](Self::with_unread_truth): the backend answered, and no outcome is known. |
/// | A call that never reached the backend | [`without_truth`](Self::without_truth): every response field is absent. |
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CallFields {
    /// The identity of the session, the learned fields and the fields of the request. No part, and no truth.
    call: Fields,
    /// The parts of the request, in its order.
    parts: Vec<Part>,
}

impl CallFields {
    /// The fields of a call with those values and those parts.
    pub(crate) fn new(call: Fields, parts: &[Part]) -> Self {
        Self {
            call,
            parts: parts.to_vec(),
        }
    }

    /// The fields before the backend answered.
    pub(crate) fn awaiting_truth(&self) -> Fields {
        let parts = self.parts.iter().cloned().map(PartFields::new);
        parts.fold(self.call.clone(), Fields::with_part)
    }

    /// The fields of a call that never reached the backend.
    pub(crate) fn without_truth(&self) -> Fields {
        self.awaiting_truth().without_truth()
    }

    /// The fields once the backend answered with `truth`, after `latency`.
    ///
    /// A part among `withheld` never reached the backend, so it has no truth.
    /// A part that the backend did not answer by itself has the truth of the call.
    pub(crate) fn with_truth(&self, truth: &Answers, withheld: &BTreeSet<Part>, latency: Latency) -> Fields {
        let call = self.call.clone().with(latency);
        let call = match truth.call {
            Answer::Ok => call.with(Outcome::Ok),
            Answer::Error(code) => call.with(Outcome::Error).with(code),
        };
        let parts = self.parts.iter().map(|part| {
            let fields = PartFields::new(part.clone());
            match truth.of(part) {
                _ if withheld.contains(part) => fields.without_truth(),
                Some(Answer::Ok) => fields.succeeded(),
                Some(Answer::Error(code)) => fields.failed(code),
                None => fields,
            }
        });
        parts.fold(call, Fields::with_part)
    }

    /// The fields once the backend answered after `latency` with a response that nobody could read.
    ///
    /// The backend did answer, so the response fields wait no longer, and the latency is known.
    /// What the response says is not known: the call and its parts have no outcome and no error code.
    /// A response that nobody read is not a response without an error.
    pub(crate) fn with_unread_truth(&self, withheld: &BTreeSet<Part>, latency: Latency) -> Fields {
        let parts = self.parts.iter().map(|part| {
            let fields = PartFields::new(part.clone());
            if withheld.contains(part) {
                fields.without_truth()
            } else {
                fields
            }
        });
        parts.fold(self.call.clone().with(latency), Fields::with_part)
    }
}

#[cfg(test)]
mod tests {
    use gaslightmq_kernel::prelude::*;

    use super::*;

    fn part(text: &str) -> Part {
        text.parse().unwrap()
    }

    #[test]
    fn a_truth_that_nobody_read_has_a_latency_and_no_outcome() {
        let fields = CallFields::new(
            Fields::new().with(ApiKey::PRODUCE),
            &[part("orders.eu/3"), part("orders.eu/4")],
        );
        let withheld = BTreeSet::from([part("orders.eu/3")]);

        let unread = fields.with_unread_truth(&withheld, Latency::from_millis(4));

        // The backend answered, so no response field is pending, and none says how.
        assert!(unread.read(Field::Latency).is_present());
        assert!(unread.read(Field::Outcome).is_absent());
        assert!(unread.read(Field::ErrorCode).is_absent());
        for of_part in unread.parts() {
            assert!(
                unread.read_in(of_part, Field::Outcome).is_absent(),
                "{:?}",
                of_part.part()
            );
        }
    }
}
