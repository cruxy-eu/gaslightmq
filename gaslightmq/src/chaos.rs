use std::time::Duration;
use kafka_protocol::error::ResponseError;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ChaosAction {
    Proceed,
    Delay(Duration),
    Error(ResponseError),
    Timeout,
}

pub trait ChaosBehavior: Send + Sync {
    fn next_action(&self) -> ChaosAction;
}

pub struct Delay {
    duration: Duration,
}

impl Delay {
    pub fn new(duration: Duration) -> Self {
        Self { duration }
    }
}

impl ChaosBehavior for Delay {
    fn next_action(&self) -> ChaosAction {
        ChaosAction::Delay(self.duration)
    }
}

pub struct Timeout;

impl ChaosBehavior for Timeout {
    fn next_action(&self) -> ChaosAction {
        ChaosAction::Timeout
    }
}

pub struct ErrorResponse {
    code: ResponseError,
}

impl ErrorResponse {
    pub fn new(code: ResponseError) -> Self {
        Self { code }
    }
}

impl ChaosBehavior for ErrorResponse {
    fn next_action(&self) -> ChaosAction {
        ChaosAction::Error(self.code)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use crate::ResponseError;

    #[test]
    fn delay_returns_delay_action() {
        let behavior = Delay::new(Duration::from_millis(500));
        assert_eq!(behavior.next_action(), ChaosAction::Delay(Duration::from_millis(500)));
    }

    #[test]
    fn timeout_returns_timeout_action() {
        let behavior = Timeout;
        assert_eq!(behavior.next_action(), ChaosAction::Timeout);
    }

    #[test]
    fn error_response_returns_error_action() {
        let behavior = ErrorResponse::new(ResponseError::NotLeaderOrFollower);
        assert_eq!(
            behavior.next_action(),
            ChaosAction::Error(ResponseError::NotLeaderOrFollower)
        );
    }
}
