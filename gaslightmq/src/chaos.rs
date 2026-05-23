use std::time::Duration;
use std::sync::atomic::{AtomicUsize, Ordering};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AfterSequence {
    Loop,
    Stop,
    RepeatLast,
}

#[derive(Debug, Clone)]
pub struct Step {
    pub action: ChaosAction,
    pub repeat: usize,
}

impl Step {
    pub fn new(action: ChaosAction, repeat: usize) -> Self {
        Self { action, repeat }
    }
}

pub struct Sequence {
    steps: Vec<Step>,
    after: AfterSequence,
    counter: AtomicUsize,
    total: usize,
}

impl Sequence {
    pub fn new(steps: Vec<Step>, after: AfterSequence) -> Self {
        let total = steps.iter().map(|s| s.repeat).sum();
        Self {
            steps,
            after,
            counter: AtomicUsize::new(0),
            total,
        }
    }

    fn action_at(&self, index: usize) -> ChaosAction {
        let mut remaining = index;
        for step in &self.steps {
            if remaining < step.repeat {
                return step.action;
            }
            remaining -= step.repeat;
        }
        unreachable!()
    }
}

impl ChaosBehavior for Sequence {
    fn next_action(&self) -> ChaosAction {
        let call = self.counter.fetch_add(1, Ordering::Relaxed);

        if call < self.total {
            self.action_at(call)
        } else {
            match self.after {
                AfterSequence::Stop => ChaosAction::Proceed,
                AfterSequence::RepeatLast => self.steps.last().unwrap().action,
                AfterSequence::Loop => self.action_at(call % self.total),
            }
        }
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

    #[test]
    fn sequence_steps_through_actions() {
        let seq = Sequence::new(
            vec![
                Step::new(ChaosAction::Proceed, 2),
                Step::new(ChaosAction::Error(ResponseError::RequestTimedOut), 1),
            ],
            AfterSequence::Stop,
        );
        assert_eq!(seq.next_action(), ChaosAction::Proceed);
        assert_eq!(seq.next_action(), ChaosAction::Proceed);
        assert_eq!(seq.next_action(), ChaosAction::Error(ResponseError::RequestTimedOut));
        // After sequence, stop = proceed
        assert_eq!(seq.next_action(), ChaosAction::Proceed);
    }

    #[test]
    fn sequence_loops() {
        let seq = Sequence::new(
            vec![
                Step::new(ChaosAction::Proceed, 1),
                Step::new(ChaosAction::Timeout, 1),
            ],
            AfterSequence::Loop,
        );
        assert_eq!(seq.next_action(), ChaosAction::Proceed);
        assert_eq!(seq.next_action(), ChaosAction::Timeout);
        // Loops back
        assert_eq!(seq.next_action(), ChaosAction::Proceed);
        assert_eq!(seq.next_action(), ChaosAction::Timeout);
    }

    #[test]
    fn sequence_repeats_last() {
        let seq = Sequence::new(
            vec![
                Step::new(ChaosAction::Proceed, 1),
                Step::new(ChaosAction::Delay(Duration::from_secs(5)), 1),
            ],
            AfterSequence::RepeatLast,
        );
        assert_eq!(seq.next_action(), ChaosAction::Proceed);
        assert_eq!(seq.next_action(), ChaosAction::Delay(Duration::from_secs(5)));
        assert_eq!(seq.next_action(), ChaosAction::Delay(Duration::from_secs(5)));
        assert_eq!(seq.next_action(), ChaosAction::Delay(Duration::from_secs(5)));
    }
}
