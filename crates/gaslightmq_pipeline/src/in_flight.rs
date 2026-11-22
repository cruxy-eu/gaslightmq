//! One call in flight: what the pipeline keeps for it, and the stages it goes through.

use std::collections::BTreeSet;
use std::sync::Arc;

use bytes::Bytes;
use gaslightmq_call::prelude::*;
use gaslightmq_clock::{Duration, Instant};
use gaslightmq_field::prelude::*;
use gaslightmq_incident::Incident;
use gaslightmq_kernel::prelude::*;
use gaslightmq_lie::prelude::*;
use gaslightmq_lie_api::prelude::*;
use gaslightmq_rule::Snapshot;
use gaslightmq_selector::Pass;
use gaslightmq_session::{ClosedBy, Session};
use gaslightmq_trigger::SessionCounters;
use gaslightmq_wire::{Frame, Request, Response, WireError};
use gaslightmq_wire_edit::{EditableRequest, TopicNames};

use crate::SharedBySessions;
use crate::body_fields::{BodyFields, Learned, acks_at_the_start_of, learned_from};
use crate::call_fields::{Answers, CallFields};
use crate::explain::UnexplainedRule;
use crate::rules::{RuleLines, Slot};
use crate::untold::{StageFailure, UntoldLie};

/// A lie that asked to forward what it left of the request, with the target it holds.
/// The pipeline asks it again after the response, so it makes the response fit the request of the client.
type AskAgain = Vec<(Slot, Target)>;

/// What stage 7 decided about the request of a call.
#[derive(Debug)]
pub(crate) enum Decision {
    /// A hang-up closes the connection once the request is at the head of the line.
    HangUp,
    /// The lies answered the call themselves, and the backend never sees it.
    Answer { response: Response, ask_again: AskAgain },
    /// The backend gets this request: the client's, or what the lies left of it.
    Forward {
        request: Request,
        ask_again: AskAgain,
        /// The parts of the client's request that the backend does not get.
        withheld: BTreeSet<Part>,
    },
}

/// What stage 9 decided about the answer of a call.
#[derive(Debug)]
pub(crate) enum Delivery {
    /// The client gets this response: the truth, or what the lies made of it.
    Response(Response),
    /// The call has no response to send.
    Nothing,
    /// A hang-up closes the connection once the answer is at the head of the line.
    HangUp,
    /// The backend answered with something that is no response to the request of the client.
    /// The proxy cannot answer the call, so it closes the connection once the answer would be at the head of the line.
    Unanswerable,
}

/// Where a call rests between two inputs.
#[derive(Debug)]
enum CallStep {
    /// Stage 7 decided. The request waits for its delay, and then for the requests before it.
    BeforeForwarding(Decision),
    /// The backend has the request, and the call waits for the truth.
    AtBackend {
        ask_again: AskAgain,
        withheld: BTreeSet<Part>,
    },
    /// Stage 9 decided. The answer waits for its delay, and then for the answers before it.
    AfterResponse(Delivery),
    /// A silence took the call. It never gets an answer.
    Silenced,
    /// The call left, or it is on its way through a stage.
    Gone,
}

/// What the pipeline does once a call arrived and stages 2 to 7 ran.
#[derive(Debug, Default)]
pub(crate) struct Arrival {
    /// The incidents that rules of the request pass fired.
    pub(crate) incidents: Vec<Incident>,
    /// How long the request waits before it goes on.
    pub(crate) wait: Duration,
    /// Whether a silence took the call.
    pub(crate) silenced: bool,
    /// Whether nothing says if the call gets a response, so the proxy cannot keep the answers of the connection in order.
    pub(crate) unanswerable: bool,
}

/// What the pipeline does once stage 9 ran for a call.
#[derive(Debug, Default)]
pub(crate) struct Settled {
    /// The incidents that rules of the response pass fired.
    pub(crate) incidents: Vec<Incident>,
    /// What the truth teaches the client context.
    pub(crate) learned: Vec<Learned>,
    /// How long the answer waits before it leaves.
    pub(crate) wait: Duration,
    /// Whether a lie dropped the call after the response.
    pub(crate) silenced: bool,
}

/// What happens when the request of a call is at the head of the line.
#[derive(Debug)]
pub(crate) enum AtRequestGate {
    /// The backend gets the request, and the call waits for the truth.
    Forward(Request),
    /// The call needs no truth: it is decided. The backend still gets `forward`, when there is one.
    Settled { forward: Option<Request>, settled: Settled },
    /// A hang-up closes the connection.
    HangUp,
    /// The call is not waiting at this gate.
    NotWaiting,
}

/// What happens when the answer of a call is at the head of the line.
#[derive(Debug)]
pub(crate) enum Leaving {
    /// The client gets these bytes.
    Send(Bytes),
    /// The client gets nothing for this call, and the connection stays open.
    Nothing,
    /// The connection closes instead: a hang-up closes it, or the proxy does, because it has no answer to send.
    Close(ClosedBy),
}

/// What the pipeline keeps for one call from the moment its request arrives until its answer leaves.
#[derive(Debug)]
pub(crate) struct CallInFlight {
    /// The armed rules at the moment the request arrived. The call finishes on them.
    snapshot: Arc<Snapshot>,
    /// The request as the client sent it.
    request: Request,
    /// The same request with its body decoded, or `None` when the body does not decode.
    asked: Option<EditableRequest>,
    /// The topics that were known by ID when the request arrived. The call works with them from start to finish.
    names: Arc<TopicNames>,
    /// The connection the call runs on, which a record needs whatever else it holds.
    connection: ConnectionNumber,
    /// Whether the client waits for a response. A Produce with `acks=0` has none.
    expects_response: bool,
    /// Whether nothing says if the client waits for a response: a Produce whose body ends before its `acks`.
    unanswerable: bool,
    fields: CallFields,
    /// The fields with the truth as the rules saw it, once it is settled.
    settled_fields: Option<Fields>,
    /// The truth and what it says, to read a response that no lie changed only once.
    /// It says nothing when nobody could read it.
    truth: Option<(Response, Option<Answers>)>,
    rules: RuleLines,
    step: CallStep,
    timing: CallTiming,
    failures: Vec<StageFailure>,
    recorded: bool,
}

impl CallInFlight {
    /// Stages 2 and 3 for a request that arrived at `now`: decode it, identify the session, and learn what it names.
    pub(crate) fn arrived(
        request: Request,
        session: &mut Session<Self>,
        snapshot: Arc<Snapshot>,
        shared: &SharedBySessions,
        now: Instant,
    ) -> Self {
        let identification = session.identify(&request);
        let unidentified = identification.errors().iter().cloned();
        let unidentified = unidentified.map(|error| StageFailure::Unidentified { error });

        // The call reads the client context once, and then tells it what the request names.
        let (mut call, body) = Self::read(request, session, snapshot, shared, now);
        call.failures.splice(0..0, unidentified);
        for value in body.learned() {
            value.clone().teach(session);
        }
        call
    }

    /// A request that arrived at `now` behind a silence: it is read for its record, and nothing else happens to it.
    ///
    /// It never reaches the backend and never gets an answer, so it does not identify the session,
    /// and the client context learns nothing from it.
    pub(crate) fn parked(
        request: Request,
        session: &Session<Self>,
        snapshot: Arc<Snapshot>,
        shared: &SharedBySessions,
        now: Instant,
    ) -> Self {
        Self::read(request, session, snapshot, shared, now).0
    }

    /// Stage 2: the request with its body decoded, and the fields that its body carries.
    fn read(
        request: Request,
        session: &Session<Self>,
        snapshot: Arc<Snapshot>,
        shared: &SharedBySessions,
        now: Instant,
    ) -> (Self, BodyFields) {
        let api = request.api();
        let mut failures = Vec::new();
        let names = shared.topics().names();

        let (asked, unreadable) = match EditableRequest::decode(&request, &names) {
            Ok(asked) => (Some(asked), None),
            Err(error) => (None, Some(error)),
        };
        // A body that does not decode has no fields either, and one failure says so.
        let body = asked.as_ref().map(|_| BodyFields::read(&request));
        let body = match body {
            Some(Ok(body)) => body,
            Some(Err(error)) => {
                failures.push(StageFailure::UnreadableRequestFields { api, error });
                BodyFields::default()
            }
            None => BodyFields::default(),
        };

        // Only a Produce with `acks=0` has no response. A Produce whose body does not decode still says its `acks`
        // at the start of that body. When even that is missing, nothing says whether a response comes.
        let is_produce = api.key == ApiKey::PRODUCE.value();
        let acks = body.acks().or_else(|| acks_at_the_start_of(&request));
        let expects_response = match (is_produce, acks) {
            (false, _) => Some(true),
            (true, Some(acks)) => Some(acks != Acks::NONE),
            (true, None) => None,
        };
        if let Some(error) = unreadable {
            failures.push(match expects_response {
                Some(_) => StageFailure::UnreadableRequest { error },
                None => StageFailure::UnknownAcks { error },
            });
        }

        let of_call = session.fields().with(ApiKey::new(api.key));
        let of_call = ApiVersion::try_from(api.version)
            .into_iter()
            .fold(of_call, Fields::with);
        let of_call = body.on(of_call);
        let parts = asked.as_ref().map_or(&[][..], EditableRequest::parts);

        let call = Self {
            snapshot,
            names,
            connection: session.connection().number(),
            unanswerable: expects_response.is_none(),
            expects_response: expects_response.unwrap_or(true),
            fields: CallFields::new(of_call, parts),
            settled_fields: None,
            truth: None,
            rules: RuleLines::none(),
            // Until stage 7 says otherwise, the request goes on as it arrived.
            step: CallStep::BeforeForwarding(Decision::Forward {
                request: request.clone(),
                ask_again: Vec::new(),
                withheld: BTreeSet::new(),
            }),
            request,
            asked,
            timing: CallTiming::arrived(now),
            failures,
            recorded: false,
        };
        (call, body)
    }

    /// Stages 4 to 7: evaluate the request pass, combine its lies, and decide what happens before forwarding.
    ///
    /// A request whose body does not decode goes on untouched: no rule sees it.
    pub(crate) fn evaluate(
        &mut self,
        counters: &mut SessionCounters,
        shared: &SharedBySessions,
        now: Instant,
    ) -> Arrival {
        let mut arrival = Arrival {
            unanswerable: self.unanswerable,
            ..Arrival::default()
        };
        let Some(asked) = self.asked.clone() else {
            return arrival;
        };
        let fields = self.fields.awaiting_truth();
        let pass = self.snapshot.evaluate(Pass::Request, &fields, now, counters);
        self.rules = RuleLines::of_request_pass(pass, &mut arrival.incidents);

        // A silence or a hang-up is asked first, before anything acts on the call.
        let acting = loop {
            let acting = self.rules.acting(Stage::BeforeForwarding);
            let Some(terminal) = acting.terminal else {
                break acting;
            };
            match self.ask_before(shared.lies(), terminal, Target::Call, &asked) {
                Ok(Effect::Drop) => {
                    self.rules.told(terminal);
                    self.ends_before_truth(terminal, &fields, now);
                    self.step = CallStep::Silenced;
                    arrival.silenced = true;
                    return arrival;
                }
                Ok(Effect::Close) => {
                    self.rules.asked(terminal);
                    self.ends_before_truth(terminal, &fields, now);
                    self.step = CallStep::BeforeForwarding(Decision::HangUp);
                    return arrival;
                }
                // A terminal lie that is not told ends nothing, and it takes nothing from the other lies:
                // they combine again without it. The next one that would end the call is asked in its place.
                Ok(other) => self.rules.untold_before_anything_acted(terminal, not_followed(&other)),
                Err(untold) => self.rules.untold_before_anything_acted(terminal, untold),
            }
        };

        arrival.wait = self.rules.wait(&acting.modifiers, counters);
        let decision = self.refuse_before_forwarding(shared, &asked, acting.askable);
        self.step = CallStep::BeforeForwarding(decision);
        arrival
    }

    /// The lie of `terminal` ends the call before the truth exists, so the rules that wait for the truth never see it.
    fn ends_before_truth(&mut self, terminal: Slot, fields: &Fields, now: Instant) {
        let waiting = self.snapshot.waiting(fields, now);
        let waiting = waiting.into_iter().filter_map(|rule| {
            let priority = self.snapshot.priority_of(rule.name())?;
            Some((rule.name().clone(), priority))
        });
        self.rules.ended_before_truth(terminal, waiting.collect());
    }

    /// Asks the lies that change the request, each with what the ones before it left.
    fn refuse_before_forwarding(
        &mut self,
        shared: &SharedBySessions,
        asked: &EditableRequest,
        askable: Vec<(Slot, Target)>,
    ) -> Decision {
        let names = Arc::clone(&self.names);
        let mut left = asked.clone();
        let mut answer: Option<(Slot, Response)> = None;
        let mut ask_again = Vec::new();

        for (slot, target) in askable {
            if let Some((answered, _)) = &answer {
                let by = self.rules.name_of(*answered).to_owned();
                self.rules.untold(slot, UntoldLie::AnsweredBefore { by });
                continue;
            }
            match self.ask_before(shared.lies(), slot, target.clone(), &left) {
                Ok(Effect::Forward(rest)) => match remainder_of(asked, &rest, &names) {
                    Ok(rest) => {
                        left = rest;
                        ask_again.push((slot, target));
                        self.rules.asked(slot);
                    }
                    Err(untold) => self.rules.untold(slot, untold),
                },
                Ok(Effect::Answer(response)) => match answering(asked.request(), response) {
                    Ok(response) => {
                        answer = Some((slot, response));
                        self.rules.asked(slot);
                    }
                    Err(untold) => self.rules.untold(slot, untold),
                },
                Ok(Effect::Nothing) => self.rules.asked(slot),
                Ok(other) => self.rules.untold(slot, not_followed(&other)),
                Err(untold) => self.rules.untold(slot, untold),
            }
        }

        if let Some((_, response)) = answer {
            return Decision::Answer { response, ask_again };
        }
        let forwarded: BTreeSet<&Part> = left.parts().iter().collect();
        let withheld = asked.parts().iter().filter(|part| !forwarded.contains(part));
        Decision::Forward {
            withheld: withheld.cloned().collect(),
            request: left.request().clone(),
            ask_again,
        }
    }

    /// The request is at the head of the line at `now`: stage 8 begins, or the call is decided without a backend.
    pub(crate) fn at_request_gate(
        &mut self,
        now: Instant,
        counters: &mut SessionCounters,
        shared: &SharedBySessions,
    ) -> AtRequestGate {
        let step = std::mem::replace(&mut self.step, CallStep::Gone);
        if matches!(step, CallStep::BeforeForwarding(_)) {
            // What the lies before forwarding asked for happens now.
            self.rules.took_effect();
        }
        match step {
            CallStep::BeforeForwarding(Decision::HangUp) => AtRequestGate::HangUp,
            CallStep::BeforeForwarding(Decision::Answer { response, ask_again }) => {
                let settled = if self.expects_response {
                    self.after_response(Base::Answered(response), ask_again, now, counters, shared)
                } else {
                    self.without_response()
                };
                AtRequestGate::Settled { forward: None, settled }
            }
            CallStep::BeforeForwarding(Decision::Forward {
                request,
                ask_again,
                withheld,
            }) => {
                self.at(self.timing.forwarded(now));
                if self.expects_response {
                    self.step = CallStep::AtBackend { ask_again, withheld };
                    AtRequestGate::Forward(request)
                } else {
                    AtRequestGate::Settled {
                        forward: Some(request),
                        settled: self.without_response(),
                    }
                }
            }
            other => {
                self.step = other;
                AtRequestGate::NotWaiting
            }
        }
    }

    /// The backend answered at `now`: stage 9 runs.
    /// `None` when the call does not wait for the backend.
    pub(crate) fn answered(
        &mut self,
        response: Response,
        now: Instant,
        counters: &mut SessionCounters,
        shared: &SharedBySessions,
    ) -> Option<Settled> {
        match std::mem::replace(&mut self.step, CallStep::Gone) {
            CallStep::AtBackend { ask_again, withheld } => {
                self.at(self.timing.answered(now));
                let base = Base::Truth { response, withheld };
                Some(self.after_response(base, ask_again, now, counters, shared))
            }
            other => {
                self.step = other;
                None
            }
        }
    }

    /// The backend answered at `now` with something that is no response, and `error` says why.
    /// Stage 9 has nothing to run on: the call has no answer, and the client must not get what the backend sent.
    /// `None` when the call does not wait for the backend.
    pub(crate) fn answered_unreadably(&mut self, error: String, now: Instant) -> Option<Settled> {
        match std::mem::replace(&mut self.step, CallStep::Gone) {
            CallStep::AtBackend { withheld, .. } => {
                self.at(self.timing.answered(now));
                let forwarded_at = self.timing.forwarded_at().unwrap_or(now);
                let latency = Latency::from(now.duration_since(forwarded_at));
                self.failures.push(StageFailure::UnreadableAnswer { error });
                self.settled_fields = Some(self.fields.with_unread_truth(&withheld, latency));
                self.rules.end_without_response_pass();
                self.step = CallStep::AfterResponse(Delivery::Unanswerable);
                Some(Settled::default())
            }
            other => {
                self.step = other;
                None
            }
        }
    }

    /// The session of the call ended because of `failure`, and the record of the call keeps that.
    pub(crate) fn failed(&mut self, failure: StageFailure) {
        self.failures.push(failure);
    }

    /// Whether the request waits for its delay before forwarding.
    pub(crate) const fn is_held_before_forwarding(&self) -> bool {
        matches!(self.step, CallStep::BeforeForwarding(_))
    }

    /// Whether the answer waits for its delay after the response.
    pub(crate) const fn is_held_after_response(&self) -> bool {
        matches!(self.step, CallStep::AfterResponse(_))
    }

    /// The answer is at the head of the line at `now`: stage 10.
    pub(crate) fn leave(&mut self, now: Instant) -> (Leaving, Delivered) {
        match std::mem::replace(&mut self.step, CallStep::Gone) {
            CallStep::AfterResponse(Delivery::Response(response)) => {
                // What the lies after the response asked for happens now.
                self.rules.took_effect();
                self.at(self.timing.delivered(now));
                let delivered = match self.answers_of(&response) {
                    Some(answers) => Delivered::Response(answers.summary()),
                    None => Delivered::UnreadResponse,
                };
                (Leaving::Send(response.frame().to_bytes()), delivered)
            }
            CallStep::AfterResponse(Delivery::HangUp) => {
                self.rules.took_effect();
                (Leaving::Close(ClosedBy::Lie), Delivered::ConnectionClosed)
            }
            CallStep::AfterResponse(Delivery::Unanswerable) => {
                (Leaving::Close(ClosedBy::Proxy), Delivered::ConnectionClosed)
            }
            _ => (Leaving::Nothing, Delivered::Nothing),
        }
    }

    /// Stage 11: the record of the call, once. It settles every firing of the call.
    ///
    /// `None` when the call was recorded before. A call that was not is never left without a record.
    pub(crate) fn record(&mut self, number: CallNumber, delivered: Delivered) -> Option<Call> {
        if std::mem::replace(&mut self.recorded, true) {
            return None;
        }
        let lines = self.rules.settle(&UntoldLie::CallEnded);
        let lines = lines.and_then(|lines| Explanation::new(lines).map_err(UnexplainedRule::of));
        let lines = lines.unwrap_or_else(|error| {
            self.failures.push(StageFailure::Unexplained { error });
            Explanation::none()
        });
        // A call that ended before the backend answered has no truth.
        let fields = self.settled_fields.clone();
        let fields = fields.unwrap_or_else(|| self.fields.without_truth());
        let recorded = Recorded {
            number,
            connection: self.connection,
            delivered,
            lines,
            timing: self.timing,
        };
        recorded.with(fields, std::mem::take(&mut self.failures))
    }

    /// Keeps a moment of the call, or the reason why the timing of the record lacks it.
    fn at(&mut self, moment: Result<CallTiming, CallTimingError>) {
        match moment {
            Ok(timing) => self.timing = timing,
            Err(error) => self.failures.push(StageFailure::Mistimed { error }),
        }
    }

    /// Stage 9 for a call without a response: nothing acts after the response, because there is none.
    fn without_response(&mut self) -> Settled {
        self.settled_fields = Some(self.fields.without_truth());
        self.rules.end_without_response_pass();
        self.rules.untold_at(Stage::AfterResponse, &UntoldLie::NoResponse);
        self.step = CallStep::AfterResponse(Delivery::Nothing);
        Settled::default()
    }

    /// Stage 9: read the truth, run the response pass, and ask the lies that act after the response.
    fn after_response(
        &mut self,
        base: Base,
        ask_again: AskAgain,
        now: Instant,
        counters: &mut SessionCounters,
        shared: &SharedBySessions,
    ) -> Settled {
        let mut settled = Settled::default();
        let (mut response, truth_is_read) = match base {
            Base::Truth { response, withheld } => {
                let Some(truth) = self.take_truth(&response, &withheld, now, shared, &mut settled) else {
                    // The backend sent no response to this request, so nothing can be told about it and nothing sent.
                    self.rules.end_without_response_pass();
                    self.step = CallStep::AfterResponse(Delivery::Unanswerable);
                    return settled;
                };
                let is_read = matches!(self.truth, Some((_, Some(_))));
                (truth, is_read)
            }
            Base::Answered(response) => {
                self.settled_fields = Some(self.fields.without_truth());
                (response, true)
            }
        };
        let Some(asked) = self.asked.clone() else {
            // No rule saw the request, so the truth leaves as it is.
            self.step = CallStep::AfterResponse(Delivery::Response(response));
            return settled;
        };

        match &self.settled_fields {
            // A truth that nobody could read has no outcome, so no rule that waits for the truth can test it.
            Some(fields) if truth_is_read && !self.rules.has_ended_before_truth() => {
                let pass = self.snapshot.evaluate(Pass::Response, fields, now, counters);
                self.rules.add_response_pass(pass, &mut settled.incidents);
            }
            _ => self.rules.end_without_response_pass(),
        }

        let acting = self.rules.acting(Stage::AfterResponse);
        if let Some(terminal) = acting.terminal {
            match self.ask_after(shared.lies(), terminal, Target::Call, &asked, &response) {
                Ok(Effect::Close) => {
                    self.rules.asked(terminal);
                    self.step = CallStep::AfterResponse(Delivery::HangUp);
                    return settled;
                }
                Ok(Effect::Drop) => {
                    self.rules.told(terminal);
                    self.step = CallStep::Silenced;
                    settled.silenced = true;
                    return settled;
                }
                Ok(other) => self.rules.untold(terminal, not_followed(&other)),
                Err(untold) => self.rules.untold(terminal, untold),
            }
        }

        // The lies of this stage first, and then the ones that forwarded: they add what the backend never saw,
        // so a refusal of the whole call that came with the truth does not write over it.
        for (slot, target) in acting.askable {
            let effect = self.ask_after(shared.lies(), slot, target, &asked, &response);
            match effect.and_then(|effect| self.changed_response(effect)) {
                Ok(changed) => {
                    response = changed.unwrap_or(response);
                    self.rules.asked(slot);
                }
                Err(untold) => self.rules.untold(slot, untold),
            }
        }
        for (slot, target) in ask_again {
            let effect = self.ask_after(shared.lies(), slot, target, &asked, &response);
            match effect.and_then(|effect| self.changed_response(effect)) {
                Ok(changed) => response = changed.unwrap_or(response),
                // The lie took effect when its request went on: the backend never saw what it withheld.
                // It stays told, and the call says that its response lacks what the lie was to add.
                Err(error) => {
                    let rule = self.rules.name_of(slot).to_owned();
                    self.failures.push(StageFailure::UnmergedLie { rule, error });
                }
            }
        }

        settled.wait = self.rules.wait(&acting.modifiers, counters);
        if let Some(throttle) = self.rules.unwritten_throttle() {
            let millis = throttle.as_millis();
            self.failures.push(StageFailure::ThrottleUnwritten { millis });
        }
        self.step = CallStep::AfterResponse(Delivery::Response(response));
        settled
    }

    /// Stage 8, once the backend answered: the truth with the client's correlation ID, and what it says.
    ///
    /// `None` when the answer cannot carry the API and the correlation ID of the client's request.
    /// It is then no response to that request, and the client must not get it.
    fn take_truth(
        &mut self,
        response: &Response,
        withheld: &BTreeSet<Part>,
        now: Instant,
        shared: &SharedBySessions,
        settled: &mut Settled,
    ) -> Option<Response> {
        let api = self.request.api();
        let forwarded_at = self.timing.forwarded_at().unwrap_or(now);
        let latency = Latency::from(now.duration_since(forwarded_at));
        let truth = match restored(&self.request, response) {
            Ok(truth) => truth,
            Err(error) => {
                self.failures.push(StageFailure::UnrestoredResponse { api, error });
                self.settled_fields = Some(self.fields.with_unread_truth(withheld, latency));
                return None;
            }
        };

        // The cluster names its topics here, before any lie changes the response.
        if let Err(error) = shared.topics().learn_from(&truth) {
            self.failures.push(StageFailure::UnreadableTopicNames { error });
        }
        match learned_from(&truth) {
            Ok(learned) => settled.learned = learned,
            Err(error) => self.failures.push(StageFailure::UnreadableMemberId { api, error }),
        }

        // A truth that nobody could read says nothing: it is not a truth without an error.
        let answers = match self.asked.as_ref().map(|asked| Answers::read(asked, &truth)) {
            Some(Ok(answers)) => Some(answers),
            Some(Err(error)) => {
                self.failures.push(StageFailure::UnreadableTruth { error });
                None
            }
            None => None,
        };
        self.settled_fields = Some(match &answers {
            Some(answers) => self.fields.with_truth(answers, withheld, latency),
            None => self.fields.with_unread_truth(withheld, latency),
        });
        self.truth = Some((truth.clone(), answers));
        Some(truth)
    }

    /// What `response` says, which the client receives, or `None` when nobody could read it.
    /// The truth is read once, when no lie changed it.
    fn answers_of(&mut self, response: &Response) -> Option<Answers> {
        if let Some((truth, answers)) = &self.truth
            && truth.frame() == response.frame()
        {
            return answers.clone();
        }
        match Answers::read(self.asked.as_ref()?, response) {
            Ok(answers) => Some(answers),
            Err(error) => {
                self.failures.push(StageFailure::UnreadableDelivery { error });
                None
            }
        }
    }

    /// What the lie of `slot` asks for before the backend sees `request`.
    fn ask_before(
        &self,
        lies: &Lies,
        slot: Slot,
        target: Target,
        request: &EditableRequest,
    ) -> Result<Effect, UntoldLie> {
        let lie = self.rules.lie_on(slot, target).ok_or(UntoldLie::NoLie)?;
        Ok(lies.before_forwarding(&lie, request)?)
    }

    /// What the lie of `slot` asks for once the backend answered.
    fn ask_after(
        &self,
        lies: &Lies,
        slot: Slot,
        target: Target,
        request: &EditableRequest,
        response: &Response,
    ) -> Result<Effect, UntoldLie> {
        let lie = self.rules.lie_on(slot, target).ok_or(UntoldLie::NoLie)?;
        Ok(lies.after_response(&lie, request, response)?)
    }

    /// The response that `effect` asks to send instead, or `None` when it leaves the response as it is.
    fn changed_response(&self, effect: Effect) -> Result<Option<Response>, UntoldLie> {
        match effect {
            Effect::Answer(response) => answering(&self.request, response).map(Some),
            Effect::Nothing => Ok(None),
            other => Err(not_followed(&other)),
        }
    }
}

/// What a call record holds apart from its fields and from what failed.
#[derive(Debug)]
struct Recorded {
    number: CallNumber,
    connection: ConnectionNumber,
    delivered: Delivered,
    /// The lines of the rules that the call touched.
    lines: Explanation,
    timing: CallTiming,
}

impl Recorded {
    /// The record with `fields`, and with `failures` as what went wrong while the pipeline handled the call.
    ///
    /// A call is never left without a record.
    /// When `fields` do not make one, the record says so and keeps the one field every call has: its connection.
    /// `None` only when even those do not make a record, which a connection number alone always does.
    fn with(self, fields: Fields, mut failures: Vec<StageFailure>) -> Option<Call> {
        let explained = |failures: &[StageFailure]| {
            let messages: Vec<String> = failures.iter().map(ToString::to_string).collect();
            if messages.is_empty() {
                self.lines.clone()
            } else {
                self.lines.clone().with_failure(Failure::new(messages.join("; ")))
            }
        };
        let record = |fields: Fields, failures: &[StageFailure]| {
            Call::new(
                self.number,
                fields,
                self.delivered.clone(),
                explained(failures),
                self.timing,
            )
        };
        match record(fields, &failures) {
            Ok(call) => Some(call),
            Err(error) => {
                failures.push(StageFailure::Unrecorded { error });
                record(Fields::new().with(self.connection).without_truth(), &failures).ok()
            }
        }
    }
}

/// What the lies after the response start from.
#[derive(Debug)]
enum Base {
    /// The backend answered. `withheld` are the parts of the request it never saw.
    Truth {
        response: Response,
        withheld: BTreeSet<Part>,
    },
    /// The lies before forwarding answered the call, and there is no truth.
    Answered(Response),
}

/// Why an effect that the pipeline cannot follow where it was asked for is not told.
fn not_followed(effect: &Effect) -> UntoldLie {
    UntoldLie::NotFollowed {
        instruction: effect.instruction(),
    }
}

/// `response`, when it answers `request`: same API, same correlation ID.
fn answering(request: &Request, response: Response) -> Result<Response, UntoldLie> {
    if response.api() == request.api() && response.correlation_id() == request.correlation_id() {
        Ok(response)
    } else {
        Err(UntoldLie::AnswersAnotherCall {
            api: response.api(),
            correlation_id: response.correlation_id(),
        })
    }
}

/// What a lie left of the request `asked`, decoded for the lie that is asked next.
fn remainder_of(asked: &EditableRequest, rest: &Request, names: &TopicNames) -> Result<EditableRequest, UntoldLie> {
    if rest.api() != asked.api() || rest.correlation_id() != asked.request().correlation_id() {
        return Err(UntoldLie::ForwardsAnotherCall {
            api: rest.api(),
            correlation_id: rest.correlation_id(),
        });
    }
    EditableRequest::decode(rest, names).map_err(|error| UntoldLie::UnreadableRemainder { error })
}

/// `response` with the API and the correlation ID of `request`, which is the request of the client.
///
/// A backend can number its requests anew. The correlation ID is the first four bytes of a response,
/// so the client's ID goes there, and nothing of the body is decoded.
fn restored(request: &Request, response: &Response) -> Result<Response, WireError> {
    if response.api() == request.api() && response.correlation_id() == request.correlation_id() {
        return Ok(response.clone());
    }
    let payload = response.frame().payload();
    let after_id = payload.get(size_of::<i32>()..).unwrap_or_default();
    let mut renumbered = Vec::with_capacity(payload.len());
    renumbered.extend_from_slice(&request.correlation_id().to_be_bytes());
    renumbered.extend_from_slice(after_id);
    Response::decode(request.api(), Frame::new(renumbered))
}

#[cfg(test)]
mod tests {
    use gaslightmq_lie_refusal::Refusal;
    use gaslightmq_rule::prelude::*;
    use gaslightmq_selector::prelude::*;
    use gaslightmq_session::prelude::*;
    use gaslightmq_trigger::Seed;
    use gaslightmq_wire::messages::fetch_request::{FetchPartition, FetchTopic};
    use gaslightmq_wire::messages::{FetchRequest, HeartbeatRequest, HeartbeatResponse, MetadataRequest};

    use super::*;
    use crate::KnownTopics;

    fn part(text: &str) -> Part {
        text.parse().unwrap()
    }

    #[test]
    fn a_call_works_with_the_topic_names_it_arrived_with() {
        let now = Instant::ZERO;
        let id = TopicId::new([1; 16]).unwrap();
        let lies = Lies::of_every_build().with(Refusal).unwrap();
        let shared = SharedBySessions::new(
            ClientContexts::new(Duration::from_secs(60)),
            KnownTopics::new(),
            Arc::new(lies),
        );
        shared.topics().learn(id, TopicName::new("orders.eu").unwrap());

        // "Refuse partition 3 of orders.eu", and a Fetch that names the topic by its ID.
        let partition_3 = Conditions::new()
            .with(Condition::parse(Field::Topic, "orders.eu").unwrap())
            .with(Condition::parse(Field::Partition, "3").unwrap());
        let flap = Rule::new(
            "Leader flap".parse().unwrap(),
            partition_3,
            RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER),
        );
        let armed = flap.resolve(&SelectorSet::default(), &PresetSet::default()).unwrap();
        let snapshot = Arc::new(Snapshot::new([armed.arm(Seed::new(42), now)]).unwrap());
        let partitions = [3, 4].map(|index| FetchPartition::default().with_partition(index));
        let topic = FetchTopic::default()
            .with_topic_id(vec![1; 16].try_into().unwrap())
            .with_partitions(partitions.to_vec());
        let fetch = Request::new(13, 41, None, &FetchRequest::default().with_topics(vec![topic])).unwrap();

        let connection = Connection::new(
            ConnectionNumber::new(7),
            "10.0.4.27:51459".parse().unwrap(),
            Endpoint::Bootstrap,
        );
        let mut session = Session::open(connection, ConnectionOrdinal::FIRST, shared.contexts().clone(), now);
        let mut call = CallInFlight::arrived(fetch, &mut session, snapshot, &shared, now);

        // Another session learns that the ID now belongs to another topic, between two stages of this call.
        shared.topics().learn(id, TopicName::new("orders.renamed").unwrap());
        call.evaluate(session.counters_mut(), &shared, now);

        // The call still reads what is left of its request with the names it started with:
        // partition 4 was forwarded, and only partition 3 was withheld.
        let CallStep::BeforeForwarding(Decision::Forward { withheld, .. }) = &call.step else {
            panic!("the rest of the request goes to the backend, and not {:?}", call.step);
        };
        assert_eq!(withheld, &BTreeSet::from([part("orders.eu/3")]));
    }

    #[test]
    fn a_call_whose_fields_make_no_record_still_gets_one_that_says_so() {
        let recorded = |fields: Fields| {
            let recorded = Recorded {
                number: CallNumber::FIRST,
                connection: ConnectionNumber::new(7),
                delivered: Delivered::Nothing,
                lines: Explanation::none(),
                timing: CallTiming::arrived(Instant::ZERO),
            };
            recorded.with(fields, Vec::new()).unwrap()
        };

        // Fields that make a record are kept, and nothing failed.
        let settled = Fields::new()
            .with(ConnectionNumber::new(7))
            .with(ApiKey::FETCH)
            .without_truth();
        let record = recorded(settled.clone());
        assert_eq!(record.fields(), &settled);
        assert_eq!(record.explanation().failure(), None);

        // Fields that still wait for the backend make none. The call is recorded all the same.
        let record = recorded(Fields::new().with(ConnectionNumber::new(7)).with(ApiKey::FETCH));
        assert_eq!(record.id().to_string(), "#7.1");
        assert_eq!(
            record.explanation().failure().map(Failure::message),
            Some(
                "the record keeps no field of the call: a call record is of a call that ended, \
                 and its response fields still wait for the backend"
            )
        );
    }

    fn heartbeat(correlation_id: i32) -> Request {
        Request::new(4, correlation_id, Some("order-ingest"), &HeartbeatRequest::default()).unwrap()
    }

    #[test]
    fn a_response_that_answers_the_request_stays_as_it_is() {
        let request = heartbeat(41);
        let response = request.response(&HeartbeatResponse::default()).unwrap();

        let restored = restored(&request, &response).unwrap();

        assert_eq!(restored.frame(), response.frame());
    }

    #[test]
    fn a_response_under_the_number_of_the_backend_gets_the_number_of_the_client() {
        let request = heartbeat(41);
        let body = HeartbeatResponse::default().with_error_code(27);
        let of_backend = Response::new(request.api(), 9000, &body).unwrap();

        let restored = restored(&request, &of_backend).unwrap();

        // Only the correlation ID changed: the body is the one the backend sent.
        assert_eq!(restored.correlation_id(), 41);
        assert_eq!(restored.api(), request.api());
        assert_eq!(restored.body_bytes(), of_backend.body_bytes());
        assert_eq!(restored.body::<HeartbeatResponse>().unwrap(), body);
    }

    #[test]
    fn a_response_only_answers_the_request_of_its_api_and_its_number() {
        let request = heartbeat(41);
        let answer = |request: &Request| request.response(&HeartbeatResponse::default()).unwrap();

        assert!(answering(&request, answer(&request)).is_ok());
        assert_eq!(
            answering(&request, answer(&heartbeat(42))).unwrap_err(),
            UntoldLie::AnswersAnotherCall {
                api: request.api(),
                correlation_id: 42
            }
        );
    }

    #[test]
    fn what_a_lie_leaves_of_a_request_is_a_request_of_the_same_call() {
        let request = heartbeat(41);
        let names = TopicNames::new();
        let asked = EditableRequest::decode(&request, &names).unwrap();

        assert!(remainder_of(&asked, &request, &names).is_ok());

        let other_number = remainder_of(&asked, &heartbeat(42), &names).unwrap_err();
        assert_eq!(
            other_number,
            UntoldLie::ForwardsAnotherCall {
                api: request.api(),
                correlation_id: 42
            }
        );
        let metadata = Request::new(12, 41, None, &MetadataRequest::default()).unwrap();
        assert!(matches!(
            remainder_of(&asked, &metadata, &names),
            Err(UntoldLie::ForwardsAnotherCall { .. })
        ));
    }
}
