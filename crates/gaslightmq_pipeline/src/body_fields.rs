//! The fields that the body of a request carries next to its parts, and what a response teaches a client context.

use gaslightmq_field::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_session::Session;
use gaslightmq_wire::messages::{
    AddOffsetsToTxnRequest, AddPartitionsToTxnRequest, ApiKey as KnownApi, ConsumerGroupHeartbeatRequest,
    ConsumerGroupHeartbeatResponse, EndTxnRequest, HeartbeatRequest, InitProducerIdRequest, JoinGroupRequest,
    JoinGroupResponse, LeaveGroupRequest, OffsetCommitRequest, OffsetFetchRequest, ProduceRequest, SyncGroupRequest,
    TxnOffsetCommitRequest,
};
use gaslightmq_wire::{DecodeError, Request, Response};

/// The first version of `AddPartitionsToTxn` that names several transactions.
const FIRST_VERSION_WITH_TRANSACTIONS: i16 = 4;
/// The first version of `OffsetFetch` that names several groups.
const FIRST_VERSION_WITH_GROUPS: i16 = 8;

/// A value of a learned field, as a request or a response carries it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Learned {
    Group(GroupId),
    MemberId(MemberId),
    GroupInstanceId(GroupInstanceId),
    TransactionalId(TransactionalId),
}

impl Learned {
    /// The fields with this value as one that the call itself carries.
    fn on(self, fields: Fields) -> Fields {
        match self {
            Self::Group(group) => fields.with(group),
            Self::MemberId(member) => fields.with(member),
            Self::GroupInstanceId(instance) => fields.with(instance),
            Self::TransactionalId(transactional) => fields.with(transactional),
        }
    }

    /// Tells the client context of `session` this value.
    pub(crate) fn teach<C>(self, session: &Session<C>) {
        match self {
            Self::Group(group) => session.learn(group),
            Self::MemberId(member) => session.learn(member),
            Self::GroupInstanceId(instance) => session.learn(instance),
            Self::TransactionalId(transactional) => session.learn(transactional),
        };
    }
}

/// The fields that the body of one request carries next to its parts.
///
/// | Field | Carried by |
/// |---|---|
/// | `acks` | Produce |
/// | `group` | The group protocol, `OffsetCommit`, `OffsetFetch`, `TxnOffsetCommit` and `AddOffsetsToTxn` |
/// | `member.id`, `group.instance.id` | The group protocol, `OffsetCommit` and `TxnOffsetCommit` |
/// | `transactional.id` | Produce and the transactional requests |
///
/// A value that is not one of its field, such as an empty group ID, is no value of the call.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct BodyFields {
    acks: Option<Acks>,
    learned: Vec<Learned>,
}

impl BodyFields {
    /// Reads the fields of the body of `request`. A request of an API that carries none has none.
    ///
    /// # Errors
    ///
    /// The [`DecodeError`] of a body that does not decode.
    pub(crate) fn read(request: &Request) -> Result<Self, DecodeError> {
        let mut fields = Self::default();
        let version = request.api().version;
        match request.api().known() {
            Some(KnownApi::Produce) => {
                let body: ProduceRequest = request.body()?;
                fields.acks = Some(Acks::new(body.acks));
                fields.transactional(body.transactional_id.as_ref().map(|id| id.as_str()));
            }
            Some(KnownApi::JoinGroup) => {
                let body: JoinGroupRequest = request.body()?;
                fields.membership(&body.group_id, &body.member_id, body.group_instance_id.as_deref());
            }
            Some(KnownApi::SyncGroup) => {
                let body: SyncGroupRequest = request.body()?;
                fields.membership(&body.group_id, &body.member_id, body.group_instance_id.as_deref());
            }
            Some(KnownApi::Heartbeat) => {
                let body: HeartbeatRequest = request.body()?;
                fields.membership(&body.group_id, &body.member_id, body.group_instance_id.as_deref());
            }
            Some(KnownApi::LeaveGroup) => {
                let body: LeaveGroupRequest = request.body()?;
                fields.membership(&body.group_id, &body.member_id, None);
                for member in &body.members {
                    fields.member(&member.member_id);
                    fields.instance(member.group_instance_id.as_deref());
                }
            }
            Some(KnownApi::ConsumerGroupHeartbeat) => {
                let body: ConsumerGroupHeartbeatRequest = request.body()?;
                fields.membership(&body.group_id, &body.member_id, body.instance_id.as_deref());
            }
            Some(KnownApi::OffsetCommit) => {
                let body: OffsetCommitRequest = request.body()?;
                fields.membership(&body.group_id, &body.member_id, body.group_instance_id.as_deref());
            }
            Some(KnownApi::OffsetFetch) => {
                let body: OffsetFetchRequest = request.body()?;
                if version >= FIRST_VERSION_WITH_GROUPS {
                    for group in &body.groups {
                        fields.group(&group.group_id);
                        fields.member(group.member_id.as_deref().unwrap_or_default());
                    }
                } else {
                    fields.group(&body.group_id);
                }
            }
            Some(KnownApi::InitProducerId) => {
                let body: InitProducerIdRequest = request.body()?;
                fields.transactional(body.transactional_id.as_ref().map(|id| id.as_str()));
            }
            Some(KnownApi::AddPartitionsToTxn) => {
                let body: AddPartitionsToTxnRequest = request.body()?;
                if version >= FIRST_VERSION_WITH_TRANSACTIONS {
                    for transaction in &body.transactions {
                        fields.transactional(Some(&transaction.transactional_id));
                    }
                } else {
                    fields.transactional(Some(&body.v3_and_below_transactional_id));
                }
            }
            Some(KnownApi::AddOffsetsToTxn) => {
                let body: AddOffsetsToTxnRequest = request.body()?;
                fields.transactional(Some(&body.transactional_id));
                fields.group(&body.group_id);
            }
            Some(KnownApi::EndTxn) => {
                let body: EndTxnRequest = request.body()?;
                fields.transactional(Some(&body.transactional_id));
            }
            Some(KnownApi::TxnOffsetCommit) => {
                let body: TxnOffsetCommitRequest = request.body()?;
                fields.transactional(Some(&body.transactional_id));
                fields.membership(&body.group_id, &body.member_id, body.group_instance_id.as_deref());
            }
            _ => {}
        }
        Ok(fields)
    }

    /// The `acks` of a Produce request.
    pub(crate) const fn acks(&self) -> Option<Acks> {
        self.acks
    }

    /// The values of learned fields that the request carries, for the client context to remember.
    pub(crate) fn learned(&self) -> &[Learned] {
        &self.learned
    }

    /// `fields` with every value that the body carries, as values of the call itself.
    pub(crate) fn on(&self, fields: Fields) -> Fields {
        let fields = self.acks.into_iter().fold(fields, Fields::with);
        self.learned
            .iter()
            .cloned()
            .fold(fields, |fields, value| value.on(fields))
    }

    fn membership(&mut self, group: &str, member: &str, instance: Option<&str>) {
        self.group(group);
        self.member(member);
        self.instance(instance);
    }

    fn group(&mut self, group: &str) {
        self.learned.extend(GroupId::new(group).map(Learned::Group));
    }

    /// A member that has not joined yet sends an empty member ID, which is no value.
    fn member(&mut self, member: &str) {
        self.learned
            .extend(MemberId::from_wire(member).ok().flatten().map(Learned::MemberId));
    }

    fn instance(&mut self, instance: Option<&str>) {
        self.learned
            .extend(instance.and_then(|instance| GroupInstanceId::new(instance).ok().map(Learned::GroupInstanceId)));
    }

    fn transactional(&mut self, transactional: Option<&str>) {
        self.learned.extend(
            transactional
                .and_then(|transactional| TransactionalId::new(transactional).ok().map(Learned::TransactionalId)),
        );
    }
}

/// The first version of Produce that carries a `transactional.id` in front of its `acks`.
const FIRST_PRODUCE_WITH_TRANSACTIONS: i16 = 3;
/// The first version of Produce that writes its texts with a compact length.
const FIRST_FLEXIBLE_PRODUCE: i16 = 9;

/// The `acks` of a Produce request whose body does not decode as a whole.
///
/// Whether a Produce gets a response depends on its `acks` alone, so the pipeline has to know them
/// also when the rest of the body is broken.
/// They stand at the start of the body: first up to version 2, and behind the `transactional.id` since version 3.
/// `None` when `request` is no Produce, or when its body ends before its `acks`.
pub(crate) fn acks_at_the_start_of(request: &Request) -> Option<Acks> {
    let api = request.api();
    if api.key != ApiKey::PRODUCE.value() {
        return None;
    }
    let body = request.body_bytes();
    let mut rest: &[u8] = &body;
    if api.version >= FIRST_FLEXIBLE_PRODUCE {
        // A compact text: its length and one as a varint, and zero for no text.
        let length = usize::try_from(unsigned_varint(&mut rest)?).ok()?;
        rest = rest.get(length.saturating_sub(1)..)?;
    } else if api.version >= FIRST_PRODUCE_WITH_TRANSACTIONS {
        // A text: its length in two bytes, and a negative length for no text.
        let length = i16::from_be_bytes(*rest.first_chunk()?);
        rest = rest.get(size_of::<i16>() + usize::try_from(length).unwrap_or(0)..)?;
    }
    rest.first_chunk().map(|acks| Acks::new(i16::from_be_bytes(*acks)))
}

/// Reads an unsigned varint from the start of `bytes`, and moves `bytes` past it.
fn unsigned_varint(bytes: &mut &[u8]) -> Option<u32> {
    let mut value = 0_u32;
    for shift in (0..u32::BITS).step_by(7) {
        let (&byte, rest) = bytes.split_first()?;
        *bytes = rest;
        value |= u32::from(byte & 0x7F) << shift;
        if byte & 0x80 == 0 {
            return Some(value);
        }
    }
    None
}

/// What the truth of a call teaches the client context: the member ID that a coordinator gave the client.
///
/// A response of another API teaches nothing.
///
/// # Errors
///
/// The [`DecodeError`] of a body that does not decode.
pub(crate) fn learned_from(truth: &Response) -> Result<Vec<Learned>, DecodeError> {
    let member = match truth.api().known() {
        Some(KnownApi::JoinGroup) => MemberId::from_wire(&truth.body::<JoinGroupResponse>()?.member_id),
        Some(KnownApi::ConsumerGroupHeartbeat) => {
            let body: ConsumerGroupHeartbeatResponse = truth.body()?;
            MemberId::from_wire(body.member_id.as_deref().unwrap_or_default())
        }
        _ => Ok(None),
    };
    Ok(member.ok().flatten().map(Learned::MemberId).into_iter().collect())
}

#[cfg(test)]
mod tests {
    use gaslightmq_wire::messages::leave_group_request::MemberIdentity;
    use gaslightmq_wire::messages::offset_fetch_request::OffsetFetchRequestGroup;
    use gaslightmq_wire::messages::{FetchRequest, MetadataResponse};
    use gaslightmq_wire::protocol::StrBytes;

    use super::*;

    fn text(text: &'static str) -> StrBytes {
        StrBytes::from_static_str(text)
    }

    fn group() -> Learned {
        Learned::Group(GroupId::new("analytics-etl").unwrap())
    }

    fn member(id: &str) -> Learned {
        Learned::MemberId(MemberId::new(id).unwrap())
    }

    fn transaction() -> Learned {
        Learned::TransactionalId(TransactionalId::new("payments-tx-1").unwrap())
    }

    #[test]
    fn a_produce_carries_its_acks_and_its_transaction() {
        let body = ProduceRequest::default()
            .with_acks(-1)
            .with_transactional_id(Some(text("payments-tx-1").into()));
        let request = Request::new(9, 7, None, &body).unwrap();

        let fields = BodyFields::read(&request).unwrap();

        assert_eq!(fields.acks(), Some(Acks::ALL));
        assert_eq!(fields.learned(), [transaction()]);

        // The values are the call's own, so they replace what the client context provides.
        let other = GroupId::new("another-group").unwrap();
        let of_call = fields.on(Fields::new().with_learned(other));
        let acks = of_call.read(Field::Acks).present().unwrap();
        assert_eq!(acks.to_string(), "-1");
        let transactional = of_call.read(Field::TransactionalId).present().unwrap();
        assert_eq!(transactional.origin(), Origin::Call);
    }

    #[test]
    fn a_member_that_has_not_joined_yet_names_its_group_and_no_member_id() {
        let body = JoinGroupRequest::default()
            .with_group_id(text("analytics-etl").into())
            .with_group_instance_id(Some(text("etl-0")));
        let request = Request::new(9, 7, None, &body).unwrap();

        let fields = BodyFields::read(&request).unwrap();

        let instance = Learned::GroupInstanceId(GroupInstanceId::new("etl-0").unwrap());
        assert_eq!(fields.learned(), [group(), instance]);
        assert_eq!(fields.acks(), None);
    }

    #[test]
    fn a_request_names_every_member_and_every_group_it_carries() {
        let leaving = |id| MemberIdentity::default().with_member_id(text(id));
        let leave = LeaveGroupRequest::default()
            .with_group_id(text("analytics-etl").into())
            .with_members(vec![leaving("consumer-1"), leaving("consumer-2")]);
        let leave = Request::new(5, 7, None, &leave).unwrap();
        assert_eq!(
            BodyFields::read(&leave).unwrap().learned(),
            [group(), member("consumer-1"), member("consumer-2")]
        );

        let of_group = |id| OffsetFetchRequestGroup::default().with_group_id(text(id).into());
        let fetch = OffsetFetchRequest::default().with_groups(vec![of_group("analytics-etl"), of_group("payments")]);
        let fetch = Request::new(8, 7, None, &fetch).unwrap();
        let payments = Learned::Group(GroupId::new("payments").unwrap());
        assert_eq!(BodyFields::read(&fetch).unwrap().learned(), [group(), payments]);
    }

    #[test]
    fn a_transactional_request_names_its_transaction_and_its_group() {
        let body = AddOffsetsToTxnRequest::default()
            .with_transactional_id(text("payments-tx-1").into())
            .with_group_id(text("analytics-etl").into());
        let request = Request::new(3, 7, None, &body).unwrap();

        assert_eq!(BodyFields::read(&request).unwrap().learned(), [transaction(), group()]);
    }

    #[test]
    fn the_acks_of_a_produce_stand_at_the_start_of_its_body_at_every_version() {
        use gaslightmq_wire::protocol::Message;

        let transaction = || Some(text("payments-tx-1").into());
        for version in ProduceRequest::VERSIONS.min..=ProduceRequest::VERSIONS.max {
            for acks in [Acks::NONE, Acks::LEADER, Acks::ALL] {
                // The `transactional.id` in front of the `acks` is a text, or no text.
                for transactional_id in [None, transaction()] {
                    let body = ProduceRequest::default()
                        .with_acks(acks.value())
                        .with_transactional_id(transactional_id);
                    let request = Request::new(version, 7, None, &body).unwrap();

                    assert_eq!(acks_at_the_start_of(&request), Some(acks), "Produce v{version}");
                    // They are still there when the body ends right behind them.
                    let payload = request.frame().payload();
                    let timeout_and_topics = if version >= 9 { 6 } else { 8 };
                    let cut = payload.slice(..payload.len() - timeout_and_topics);
                    let cut = Request::decode(gaslightmq_wire::Frame::new(cut)).unwrap();
                    assert!(cut.body::<ProduceRequest>().is_err());
                    assert_eq!(acks_at_the_start_of(&cut), Some(acks), "Produce v{version}, cut short");
                }
            }
        }
    }

    #[test]
    fn the_acks_of_a_produce_that_is_older_than_the_codec_knows_stand_first() {
        // A Produce of version 2: the key, the version, the correlation ID, no client ID, and then `acks=0`.
        let header: &[u8] = &[0, 0, 0, 2, 0, 0, 0, 7, 0xFF, 0xFF];
        let frame = [header, &[0, 0], &[0, 0, 0x75, 0x30]].concat();
        let request = Request::decode(gaslightmq_wire::Frame::new(frame)).unwrap();

        assert!(request.body::<ProduceRequest>().is_err());
        assert_eq!(acks_at_the_start_of(&request), Some(Acks::NONE));
    }

    #[test]
    fn a_body_that_ends_before_its_acks_does_not_say_them() {
        let request = Request::new(9, 7, None, &ProduceRequest::default().with_acks(-1)).unwrap();
        let header = request.frame().payload().len() - request.body_bytes().len();
        for kept in 0..3 {
            let cut = request.frame().payload().slice(..header + kept);
            let cut = Request::decode(gaslightmq_wire::Frame::new(cut)).unwrap();
            assert_eq!(acks_at_the_start_of(&cut), None, "{kept} bytes of body");
        }

        // Only a Produce has `acks`.
        let fetch = Request::new(13, 7, None, &FetchRequest::default()).unwrap();
        assert_eq!(acks_at_the_start_of(&fetch), None);
    }

    #[test]
    fn a_request_of_another_api_carries_none_of_these_fields() {
        let request = Request::new(13, 7, None, &FetchRequest::default()).unwrap();

        assert_eq!(BodyFields::read(&request), Ok(BodyFields::default()));
    }

    #[test]
    fn the_truth_of_joining_a_group_teaches_the_member_id() {
        let join = Request::new(9, 7, None, &JoinGroupRequest::default()).unwrap();
        let joined = JoinGroupResponse::default().with_member_id(text("consumer-1-4f2a"));
        assert_eq!(
            learned_from(&join.response(&joined).unwrap()),
            Ok(vec![member("consumer-1-4f2a")])
        );

        // A member that was refused got no ID, and a response of another API teaches nothing.
        let refused = join.response(&JoinGroupResponse::default()).unwrap();
        assert_eq!(learned_from(&refused), Ok(Vec::new()));
        let metadata = Request::new(12, 7, None, &gaslightmq_wire::messages::MetadataRequest::default()).unwrap();
        let metadata = metadata.response(&MetadataResponse::default()).unwrap();
        assert_eq!(learned_from(&metadata), Ok(Vec::new()));
    }
}
