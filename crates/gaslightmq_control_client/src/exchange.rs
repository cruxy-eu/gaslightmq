//! The form of what travels between the client and the control plane over HTTP and the WebSocket.
//!
//! Each transport moves text, and this module says what the text is and what it means.

use gaslightmq_control_api::{Operation, OperationRequest, RequestError, RequestSource, StreamMessage, Subscription};
use serde::de::DeserializeOwned;

use crate::error::{SendError, StreamError};

/// The path below the address that takes every operation, with a `POST`.
pub(crate) const OPERATIONS_PATH: &str = "api/operations";

/// The path below the address of the live stream, which a `GET` upgrades to a WebSocket.
pub(crate) const STREAM_PATH: &str = "api/stream";

/// The content type of every request and of every answer.
pub(crate) const JSON: &str = "application/json";

/// One request, as the text that travels: an operation, and where it comes from.
pub(crate) struct RequestText {
    /// The name of the operation, as its form names it: `arm-rule`. A message names it.
    pub(crate) name: &'static str,
    /// The body of the request.
    pub(crate) body: String,
}

impl RequestText {
    /// The request that asks `operation`, and comes from `from`.
    pub(crate) fn of<O: Operation>(from: RequestSource, operation: O) -> Self {
        let request = OperationRequest::new(from, operation);
        Self {
            name: request.operation.name(),
            body: serde_json::to_string(&request).expect("every request has a form"),
        }
    }

    /// Reads what the control plane answered to the request: the answer or the error of `O`,
    /// or why the control plane did not take the request.
    pub(crate) fn read<O: Operation>(&self, reply: &Reply) -> Result<Result<O::Answer, O::Error>, SendError> {
        match reply.status {
            Reply::ANSWERED => self.read_as(reply, "answer").map(Ok),
            Reply::REFUSED => self.read_as(reply, "error").map(Err),
            Reply::NOT_SERVED | Reply::NOT_READ => {
                let error: RequestError = self.read_as(reply, "rejection")?;
                Err(SendError::Rejected { error })
            }
            status => Err(SendError::Unreadable {
                reason: format!("{} got the status {status}: {}", self.name, reply.body),
            }),
        }
    }

    fn read_as<T: DeserializeOwned>(&self, reply: &Reply, what: &str) -> Result<T, SendError> {
        serde_json::from_str(&reply.body).map_err(|error| SendError::Unreadable {
            reason: format!("no {what} of {}: {error}", self.name),
        })
    }
}

/// What the control plane answered to one request: its status and its body.
pub(crate) struct Reply {
    pub(crate) status: u16,
    pub(crate) body: String,
}

impl Reply {
    /// The status of an answer, whose body is the answer of the operation.
    const ANSWERED: u16 = 200;
    /// The status of a refusal, whose body is the error of the operation.
    const REFUSED: u16 = 422;
    /// The status of an operation that the control plane does not serve, whose body is a [`RequestError`].
    const NOT_SERVED: u16 = 501;
    /// The status of a request that the control plane could not read, whose body is a [`RequestError`].
    const NOT_READ: u16 = 400;
}

/// A subscription, as the text of one message to the control plane.
pub(crate) fn subscription_text(subscription: &Subscription) -> String {
    serde_json::to_string(subscription).expect("every subscription has a form")
}

/// Reads one message of the live stream.
pub(crate) fn read_message(text: &str) -> Result<StreamMessage, StreamError> {
    serde_json::from_str(text).map_err(|error| StreamError::Unreadable {
        reason: format!("no message of the live stream: {error}"),
    })
}

/// The control plane closed the live stream with `code`, and maybe said why in `reason`.
pub(crate) fn closed(code: Option<u16>, reason: &str) -> StreamError {
    /// The code of a control plane that stops.
    const GOING_AWAY: u16 = 1001;
    let reason = match (code, reason) {
        (Some(GOING_AWAY), "") => "the control plane stopped".to_owned(),
        (_, "") => "the control plane closed the live stream".to_owned(),
        (_, reason) => format!("the control plane closed the live stream: {reason}"),
    };
    StreamError::Unreachable { reason }
}

#[cfg(test)]
mod tests {
    use gaslightmq_control_api::prelude::*;

    use super::*;

    fn arm_rule() -> ArmRule {
        ArmRule {
            environment: "orders-sim".parse().expect("an environment name"),
            rule: "delay-1s-on-order-producers".parse().expect("a rule name"),
        }
    }

    /// The source of a request of `gmq`.
    const FROM_THE_CLI: RequestSource = RequestSource::new(RequestSurface::Cli);

    #[test]
    fn a_request_is_the_form_of_its_operation_with_where_it_comes_from() {
        let request = RequestText::of(FROM_THE_CLI, arm_rule());

        assert_eq!(request.name, "arm-rule");
        let body: serde_json::Value = serde_json::from_str(&request.body).expect("json");
        assert_eq!(
            body,
            serde_json::json!({
                "from": { "surface": "cli" },
                "operation": "arm-rule",
                "environment": "orders-sim",
                "rule": "delay-1s-on-order-producers",
            })
        );
    }

    #[test]
    fn a_refusal_is_the_error_of_the_operation() {
        let request = RequestText::of(FROM_THE_CLI, arm_rule());
        let reply = Reply {
            status: Reply::REFUSED,
            body: r#"{ "error": "not-running", "environment": "orders-sim" }"#.to_owned(),
        };

        let error = request
            .read::<ArmRule>(&reply)
            .expect("an answer of the control plane")
            .expect_err("a refusal");

        assert_eq!(
            error,
            RuleCommandError::NotRunning {
                environment: "orders-sim".parse().expect("an environment name"),
            }
        );
    }

    #[test]
    fn an_answer_of_another_form_is_unreadable() {
        let request = RequestText::of(FROM_THE_CLI, arm_rule());
        let reply = Reply {
            status: Reply::ANSWERED,
            body: r#"{ "armed": true }"#.to_owned(),
        };

        let error = request.read::<ArmRule>(&reply).expect_err("no answer of arm-rule");

        assert!(matches!(error, SendError::Unreadable { .. }), "{error}");
        assert!(error.to_string().contains("no answer of arm-rule"), "{error}");
    }

    #[test]
    fn another_status_is_unreadable() {
        let request = RequestText::of(FROM_THE_CLI, arm_rule());
        let reply = Reply {
            status: 404,
            body: "not found".to_owned(),
        };

        let error = request.read::<ArmRule>(&reply).expect_err("no answer of arm-rule");

        assert_eq!(
            error,
            SendError::Unreadable {
                reason: "arm-rule got the status 404: not found".to_owned()
            }
        );
    }

    #[test]
    fn an_operation_that_the_control_plane_does_not_serve_is_rejected() {
        let request = RequestText::of(FROM_THE_CLI, arm_rule());
        let reply = Reply {
            status: Reply::NOT_SERVED,
            body: r#"{ "error": "not-served", "operation": "arm-rule" }"#.to_owned(),
        };

        let error = request.read::<ArmRule>(&reply).expect_err("no answer of arm-rule");

        assert_eq!(
            error,
            SendError::Rejected {
                error: RequestError::NotServed {
                    operation: "arm-rule".to_owned()
                }
            }
        );
    }

    #[test]
    fn a_stream_that_the_control_plane_closes_as_it_stops_says_so() {
        assert_eq!(
            closed(Some(1001), ""),
            StreamError::Unreachable {
                reason: "the control plane stopped".to_owned()
            }
        );
        assert_eq!(
            closed(Some(1011), "the workspace did not read"),
            StreamError::Unreachable {
                reason: "the control plane closed the live stream: the workspace did not read".to_owned()
            }
        );
    }
}
