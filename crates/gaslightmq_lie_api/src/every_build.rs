//! The three kinds of every build: timing, silence and hang-up.
//! Each is one instruction to the pipeline, and none reads a request or a response.

use gaslightmq_lie::prelude::*;
use gaslightmq_wire::Response;
use gaslightmq_wire_edit::EditableRequest;

use crate::lie::definition_of;
use crate::{Effect, Lie, LieError};

/// The behaviour of a timing lie: it asks the pipeline to [hold](Effect::Hold) the call back.
///
/// The effect carries the timing lie itself, at the stage it was asked at.
/// It reads neither the request nor the response.
///
/// The pipeline picks the wait, sets the timer and writes the throttle into the answer.
/// When several timing lies apply at one stage, their delays add up and their throttles take the maximum.
/// That is combining, so this behaviour never does it:
/// see [one lie and the lies of a call](crate#one-lie-and-the-lies-of-a-call).
///
/// ```
/// use gaslightmq_clock::TimeSpan;
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_lie::prelude::*;
/// use gaslightmq_lie_api::prelude::*;
/// use gaslightmq_wire::Request;
/// use gaslightmq_wire::messages::HeartbeatRequest;
/// use gaslightmq_wire_edit::prelude::*;
///
/// // A late Heartbeat can expire the member, and cause a real rebalance.
/// let late = TimingLie::new().with_delay(TimeSpan::from_secs(45)).at(Stage::BeforeForwarding);
/// let heartbeat = Request::new(4, 7, Some("order-ingest"), &HeartbeatRequest::default()).expect("a request");
/// let request = EditableRequest::decode(&heartbeat, &TopicNames::new()).expect("a request that decodes");
///
/// let effect = Timing.before_forwarding(&LieDefinition::from(late).on(Target::Call), &request)?;
/// assert_eq!(effect.to_string(), "hold +45 s before forwarding");
/// # Ok::<(), LieError>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Timing;

impl Timing {
    /// The hold that `lie` asks for at `stage`.
    fn hold(lie: &TargetedLie, stage: Stage) -> Result<Effect, LieError> {
        match lie.lie() {
            LieDefinition::Timing(timing) => Ok(Effect::Hold(timing.at(stage))),
            other => Err(LieError::OtherKind {
                tells: LieKind::Timing,
                lie: other.kind(),
            }),
        }
    }
}

impl Lie for Timing {
    fn kind(&self) -> LieKind {
        LieKind::Timing
    }

    /// Asks to hold the request back.
    fn before_forwarding(&self, lie: &TargetedLie, _request: &EditableRequest) -> Result<Effect, LieError> {
        Self::hold(lie, Stage::BeforeForwarding)
    }

    /// Asks to hold the answer back.
    fn after_response(
        &self,
        lie: &TargetedLie,
        _request: &EditableRequest,
        _response: &Response,
    ) -> Result<Effect, LieError> {
        Self::hold(lie, Stage::AfterResponse)
    }
}

/// The behaviour of a silence: it asks the pipeline to [drop](Effect::Drop) the call.
///
/// The request is read and never answered, and the connection stalls.
/// A silence always acts before forwarding, so this kind has no behaviour after the response.
///
/// ```
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_lie::prelude::*;
/// use gaslightmq_lie_api::prelude::*;
/// use gaslightmq_wire::Request;
/// use gaslightmq_wire::messages::HeartbeatRequest;
/// use gaslightmq_wire_edit::prelude::*;
///
/// let heartbeat = Request::new(4, 7, Some("order-ingest"), &HeartbeatRequest::default()).expect("a request");
/// let request = EditableRequest::decode(&heartbeat, &TopicNames::new()).expect("a request that decodes");
///
/// let effect = Silence.before_forwarding(&LieDefinition::Silence.on(Target::Call), &request)?;
/// assert_eq!(effect.instruction(), Instruction::Drop);
/// # Ok::<(), LieError>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Silence;

impl Lie for Silence {
    fn kind(&self) -> LieKind {
        LieKind::Silence
    }

    /// Asks to drop the call.
    fn before_forwarding(&self, lie: &TargetedLie, _request: &EditableRequest) -> Result<Effect, LieError> {
        definition_of(LieKind::Silence, lie)?;
        Ok(Effect::Drop)
    }
}

/// The behaviour of a hang-up: it asks the pipeline to [close](Effect::Close) the connection.
///
/// Before forwarding, the backend never sees the request.
/// After the response, the backend handled the request, and the connection closes before the client reads the answer.
/// The instruction is the same at both stages.
///
/// ```
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_lie::prelude::*;
/// use gaslightmq_lie_api::prelude::*;
/// use gaslightmq_wire::Request;
/// use gaslightmq_wire::messages::{HeartbeatRequest, HeartbeatResponse};
/// use gaslightmq_wire_edit::prelude::*;
///
/// let heartbeat = Request::new(4, 7, Some("order-ingest"), &HeartbeatRequest::default()).expect("a request");
/// let request = EditableRequest::decode(&heartbeat, &TopicNames::new()).expect("a request that decodes");
/// let truth = heartbeat.response(&HeartbeatResponse::default()).expect("a response");
/// let late = LieDefinition::from(HangUpLie::new().at(Stage::AfterResponse)).on(Target::Call);
///
/// let effect = HangUp.after_response(&late, &request, &truth)?;
/// assert_eq!(effect.instruction(), Instruction::Close);
/// # Ok::<(), LieError>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct HangUp;

impl Lie for HangUp {
    fn kind(&self) -> LieKind {
        LieKind::HangUp
    }

    /// Asks to close the connection instead of forwarding the request.
    fn before_forwarding(&self, lie: &TargetedLie, _request: &EditableRequest) -> Result<Effect, LieError> {
        definition_of(LieKind::HangUp, lie)?;
        Ok(Effect::Close)
    }

    /// Asks to close the connection instead of sending the answer.
    fn after_response(
        &self,
        lie: &TargetedLie,
        _request: &EditableRequest,
        _response: &Response,
    ) -> Result<Effect, LieError> {
        definition_of(LieKind::HangUp, lie)?;
        Ok(Effect::Close)
    }
}

#[cfg(test)]
mod tests {
    use gaslightmq_clock::TimeSpan;
    use gaslightmq_field::prelude::*;
    use gaslightmq_kernel::prelude::*;
    use gaslightmq_wire::Request;
    use gaslightmq_wire::messages::{HeartbeatRequest, HeartbeatResponse};
    use gaslightmq_wire_edit::TopicNames;

    use super::*;
    use crate::Instruction;

    const BEFORE: Stage = Stage::BeforeForwarding;
    const AFTER: Stage = Stage::AfterResponse;

    fn heartbeat() -> EditableRequest {
        let request = Request::new(4, 7, Some("order-ingest"), &HeartbeatRequest::default()).unwrap();
        EditableRequest::decode(&request, &TopicNames::new()).unwrap()
    }

    fn truth(request: &EditableRequest) -> Response {
        request.request().response(&HeartbeatResponse::default()).unwrap()
    }

    fn on_call(lie: impl Into<LieDefinition>) -> TargetedLie {
        lie.into().on(Target::Call)
    }

    fn delay() -> TimingLie {
        TimingLie::new()
            .with_delay(TimeSpan::from_secs(1))
            .with_jitter(TimeSpan::from_millis(200))
            .with_throttle(TimeSpan::from_millis(500))
    }

    fn held(effect: Effect) -> TimingLie {
        match effect {
            Effect::Hold(timing) => timing,
            other => panic!("{other} is no hold"),
        }
    }

    #[test]
    fn every_behaviour_tells_its_own_kind() {
        assert_eq!(Timing.kind(), LieKind::Timing);
        assert_eq!(Silence.kind(), LieKind::Silence);
        assert_eq!(HangUp.kind(), LieKind::HangUp);
    }

    #[test]
    fn a_timing_lie_asks_to_hold_the_call_as_it_says() {
        let request = heartbeat();
        let response = truth(&request);

        let after = held(Timing.after_response(&on_call(delay()), &request, &response).unwrap());
        assert_eq!(after, delay());

        let before = held(
            Timing
                .before_forwarding(&on_call(delay().at(BEFORE)), &request)
                .unwrap(),
        );
        assert_eq!(before, delay().at(BEFORE));
    }

    #[test]
    fn a_hold_names_the_stage_the_lie_was_asked_at() {
        let request = heartbeat();
        let response = truth(&request);

        // A lie of the response pass acts after the response, whatever stage it names.
        let named_before = on_call(delay().at(BEFORE));
        let after = held(Timing.after_response(&named_before, &request, &response).unwrap());
        assert_eq!(after, delay().at(AFTER));

        let named_after = on_call(delay().at(AFTER));
        let before = held(Timing.before_forwarding(&named_after, &request).unwrap());
        assert_eq!(before, delay().at(BEFORE));
    }

    #[test]
    fn a_timing_lie_that_changes_nothing_still_asks_for_its_hold() {
        let request = heartbeat();

        let effect = Timing.before_forwarding(&on_call(TimingLie::new()), &request).unwrap();
        assert_eq!(held(effect), TimingLie::new().at(BEFORE));
    }

    #[test]
    fn a_silence_asks_to_drop_the_call() {
        let request = heartbeat();

        let effect = Silence
            .before_forwarding(&on_call(LieDefinition::Silence), &request)
            .unwrap();
        assert_eq!(effect.instruction(), Instruction::Drop);
        assert!(effect.ends_call());
    }

    #[test]
    fn a_silence_has_no_behaviour_after_the_response() {
        let request = heartbeat();
        let response = truth(&request);

        assert_eq!(
            Silence
                .after_response(&on_call(LieDefinition::Silence), &request, &response)
                .unwrap_err(),
            LieError::NoBehaviourAtStage {
                kind: LieKind::Silence,
                stage: AFTER
            }
        );
    }

    #[test]
    fn a_hang_up_asks_to_close_the_connection_at_both_stages() {
        let request = heartbeat();
        let response = truth(&request);

        for stage in Stage::ALL {
            let lie = on_call(HangUpLie::new().at(stage));
            let before = HangUp.before_forwarding(&lie, &request).unwrap();
            let after = HangUp.after_response(&lie, &request, &response).unwrap();

            assert_eq!(before.instruction(), Instruction::Close);
            assert_eq!(after.instruction(), Instruction::Close);
            assert!(before.ends_call() && after.ends_call());
        }
    }

    #[test]
    fn every_effect_fits_the_class_of_its_kind() {
        let request = heartbeat();
        let lies: [(&dyn Lie, TargetedLie); 3] = [
            (&Timing, on_call(delay())),
            (&Silence, on_call(LieDefinition::Silence)),
            (&HangUp, on_call(HangUpLie::new())),
        ];

        for (behaviour, lie) in lies {
            let effect = behaviour.before_forwarding(&lie, &request).unwrap();
            assert!(effect.instruction().fits(behaviour.kind().class()), "{lie}");
            assert!(effect.instruction().allowed_at(BEFORE), "{lie}");
        }
    }

    #[test]
    fn a_behaviour_does_not_tell_a_lie_of_another_kind() {
        let request = heartbeat();
        let response = truth(&request);
        let refusal = on_call(RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER));
        let behaviours: [&dyn Lie; 3] = [&Timing, &Silence, &HangUp];

        for behaviour in behaviours {
            let other_kind = LieError::OtherKind {
                tells: behaviour.kind(),
                lie: LieKind::Refusal,
            };
            assert_eq!(behaviour.before_forwarding(&refusal, &request).unwrap_err(), other_kind);
            if behaviour.kind() != LieKind::Silence {
                assert_eq!(
                    behaviour.after_response(&refusal, &request, &response).unwrap_err(),
                    other_kind
                );
            }
        }
    }
}
