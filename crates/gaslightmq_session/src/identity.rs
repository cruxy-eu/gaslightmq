//! The identity of a session, and what a request header changes about it.

use std::fmt;

use gaslightmq_field::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_wire::messages::ApiVersionsRequest;
use gaslightmq_wire::{Request, WireError};

use crate::{ClientInstance, Connection};

/// The first version of `ApiVersions` in which the request names the client software.
const FIRST_VERSION_WITH_SOFTWARE: i16 = 3;

/// Who is on the other end of a session: the identity fields.
///
/// | Field | Known from |
/// |---|---|
/// | `connection`, `address`, `host`, `endpoint` | The moment the client connects. |
/// | `client.id` | The first request. |
/// | `client.software` | The first `ApiVersions` request that names it. |
///
/// A field that is not known yet is absent, and so is the `client.id` of a client that sends none.
///
/// ```
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_session::prelude::*;
///
/// let connection = Connection::new(ConnectionNumber::new(7), "10.0.4.27:51459".parse()?, Endpoint::Bootstrap);
/// let identity = Identity::of(connection);
///
/// assert_eq!(identity.connection().number().to_string(), "#7");
/// assert_eq!(identity.client_id(), None);
/// assert_eq!(identity.fields().read(Field::Host).present().map(|host| host.to_string()), Some("10.0.4.27".to_owned()));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    connection: Connection,
    client_id: Option<ClientId>,
    client_software: Option<ClientSoftware>,
}

impl Identity {
    /// The identity of a session that just opened: only its connection is known.
    pub const fn of(connection: Connection) -> Self {
        Self {
            connection,
            client_id: None,
            client_software: None,
        }
    }

    /// The connection of the session, which has the fields `connection`, `address`, `host` and `endpoint`.
    pub const fn connection(&self) -> Connection {
        self.connection
    }

    /// The `client.id` of the first request, or `None` before that request and for a client that sends none.
    pub const fn client_id(&self) -> Option<&ClientId> {
        self.client_id.as_ref()
    }

    /// The client library that an `ApiVersions` request named, or `None` when no request named one.
    pub const fn client_software(&self) -> Option<&ClientSoftware> {
        self.client_software.as_ref()
    }

    /// The identity fields as the start of the fields of a call.
    ///
    /// Every call of the session has these values, whatever its request says.
    pub fn fields(&self) -> Fields {
        let fields = Fields::new()
            .with(self.connection.number())
            .with(self.connection.address())
            .with(self.connection.host())
            .with(self.connection.endpoint());
        let fields = self.client_id.iter().cloned().fold(fields, Fields::with);
        self.client_software.iter().cloned().fold(fields, Fields::with)
    }

    /// The client instance that a first request with that `client.id` identifies.
    pub(crate) fn identify(&mut self, client_id: Option<ClientId>) -> ClientInstance {
        self.client_id.clone_from(&client_id);
        ClientInstance::new(client_id, self.connection.host())
    }

    pub(crate) fn name_software(&mut self, software: ClientSoftware) {
        self.client_software = Some(software);
    }
}

/// The `client.id` in the header of `request`.
pub(crate) fn client_id_of(request: &Request) -> Result<Option<ClientId>, IdentityError> {
    request
        .client_id()
        .map(ClientId::new)
        .transpose()
        .map_err(|error| IdentityError::InvalidClientId { error })
}

/// The client software that `request` names, or `None` when it is no `ApiVersions` request that names one.
pub(crate) fn client_software_of(request: &Request) -> Result<Option<ClientSoftware>, IdentityError> {
    let api = request.api();
    if api.key != ApiKey::API_VERSIONS.value() || api.version < FIRST_VERSION_WITH_SOFTWARE {
        return Ok(None);
    }
    let body: ApiVersionsRequest = request
        .body()
        .map_err(|error| IdentityError::UnreadableClientSoftware { error })?;
    ClientSoftware::new(
        body.client_software_name.as_str(),
        body.client_software_version.as_str(),
    )
    .map(Some)
    .map_err(|error| IdentityError::InvalidClientSoftware { error })
}

/// One thing that a request changed about the identity of its session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdentityChange {
    /// The session is identified: its first request said which client instance it belongs to.
    /// The session shares the client context of that instance from now on.
    Identified(ClientInstance),
    /// An `ApiVersions` request named the client software.
    SoftwareNamed(ClientSoftware),
}

/// Writes the change as a journal would: `identified as order-ingest from 10.0.4.27`.
impl fmt::Display for IdentityChange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Identified(client) => write!(f, "identified as {client}"),
            Self::SoftwareNamed(software) => write!(f, "runs {software}"),
        }
    }
}

/// What the header of one request did to the identity of its session.
///
/// Most requests change nothing.
/// The first request identifies the session, and an `ApiVersions` request can name the client software.
///
/// A header can also say something that the session cannot keep.
/// Those are the [errors](Self::errors), and the rest of the header still counts:
/// a request with unreadable client software identifies its session all the same.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Identification {
    changes: Vec<IdentityChange>,
    errors: Vec<IdentityError>,
}

impl Identification {
    /// What the request changed, in the order it happened. Most requests change nothing.
    pub fn changes(&self) -> &[IdentityChange] {
        &self.changes
    }

    /// What the header said and the session could not keep.
    pub fn errors(&self) -> &[IdentityError] {
        &self.errors
    }

    /// Whether the request changed nothing and nothing was wrong with it.
    pub fn is_uneventful(&self) -> bool {
        self.changes.is_empty() && self.errors.is_empty()
    }

    pub(crate) fn changed(&mut self, change: IdentityChange) {
        self.changes.push(change);
    }

    pub(crate) fn failed(&mut self, error: IdentityError) {
        self.errors.push(error);
    }
}

/// Why a session cannot keep what a request says about its client.
///
/// Each variant keeps the error of the value that is wrong, and writes it after the name of the field.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum IdentityError {
    /// The `client.id` in the header is not a client ID. The session stays unidentified.
    InvalidClientId { error: ClientIdError },
    /// The body of the `ApiVersions` request, which names the client software, does not decode.
    UnreadableClientSoftware { error: WireError },
    /// The name and the version in the `ApiVersions` request are not client software.
    InvalidClientSoftware { error: ClientSoftwareError },
}

impl IdentityError {
    /// The identity field that the session could not keep.
    pub const fn field(&self) -> Field {
        match self {
            Self::InvalidClientId { .. } => Field::ClientId,
            Self::UnreadableClientSoftware { .. } | Self::InvalidClientSoftware { .. } => Field::ClientSoftware,
        }
    }
}

impl fmt::Display for IdentityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let field = self.field();
        match self {
            Self::InvalidClientId { error } => write!(f, "{field}: {error}"),
            Self::UnreadableClientSoftware { error } => write!(f, "{field}: {error}"),
            Self::InvalidClientSoftware { error } => write!(f, "{field}: {error}"),
        }
    }
}

/// The message is complete, so no variant has a source.
impl std::error::Error for IdentityError {}

/// Where a session is in its life.
///
/// ```text
/// Opened ──▶ Identified ──▶ Closed
///    └──────────────────────▲
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Lifecycle {
    /// The connection exists, and the client is anonymous: only the connection fields are known.
    Opened,
    /// The first request arrived, so the session knows its client instance.
    Identified,
    /// The session ended. It does not open again: a client that reconnects gets a new session.
    Closed,
}

impl Lifecycle {
    /// The state in one word.
    pub const fn name(self) -> &'static str {
        match self {
            Lifecycle::Opened => "opened",
            Lifecycle::Identified => "identified",
            Lifecycle::Closed => "closed",
        }
    }
}

impl fmt::Display for Lifecycle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// Who closed a session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ClosedBy {
    /// The client closed the connection, or gave up on it.
    Client,
    /// The backend went away: it lost the cluster, or the broker of the endpoint went down.
    Backend,
    /// A hang-up lie closed the connection.
    Lie,
    /// The proxy closed the connection itself, because it could not keep the answers of the connection in order:
    /// the bytes of the client were no request, or the backend answered with something that is no response.
    /// No lie did this.
    Proxy,
}

impl ClosedBy {
    /// Who closed the session, in words.
    pub const fn name(self) -> &'static str {
        match self {
            ClosedBy::Client => "the client",
            ClosedBy::Backend => "the backend",
            ClosedBy::Lie => "a lie",
            ClosedBy::Proxy => "the proxy",
        }
    }
}

impl fmt::Display for ClosedBy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// That a session closed: who closed it, and when.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Closed {
    by: ClosedBy,
    at: gaslightmq_clock::Instant,
}

impl Closed {
    pub(crate) const fn new(by: ClosedBy, at: gaslightmq_clock::Instant) -> Self {
        Self { by, at }
    }

    /// Who closed the session.
    pub const fn by(&self) -> ClosedBy {
        self.by
    }

    /// When the session closed.
    pub const fn at(&self) -> gaslightmq_clock::Instant {
        self.at
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use gaslightmq_wire::Frame;
    use gaslightmq_wire::messages::MetadataRequest;
    use gaslightmq_wire::protocol::StrBytes;

    use super::*;

    fn connection() -> Connection {
        Connection::new(
            ConnectionNumber::new(7),
            "10.0.4.27:51459".parse().unwrap(),
            Endpoint::Bootstrap,
        )
    }

    fn api_versions(version: i16, name: &'static str, software_version: &'static str) -> Request {
        let body = ApiVersionsRequest::default()
            .with_client_software_name(StrBytes::from_static_str(name))
            .with_client_software_version(StrBytes::from_static_str(software_version));
        Request::new(version, 1, Some("order-ingest"), &body).unwrap()
    }

    fn text(reading: Reading<FieldValues>) -> String {
        match reading {
            Reading::Present(values) => values.to_string(),
            Reading::Absent => "(absent)".to_owned(),
            Reading::Pending => "(pending)".to_owned(),
        }
    }

    #[test]
    fn a_new_identity_only_knows_its_connection() {
        let identity = Identity::of(connection());
        let fields = identity.fields();

        assert_eq!(identity.connection(), connection());
        assert_eq!(identity.client_id(), None);
        assert_eq!(identity.client_software(), None);
        assert_eq!(text(fields.read(Field::Connection)), "#7");
        assert_eq!(text(fields.read(Field::Address)), "10.0.4.27:51459");
        assert_eq!(text(fields.read(Field::Host)), "10.0.4.27");
        assert_eq!(text(fields.read(Field::Endpoint)), "bootstrap");
        assert_eq!(fields.read(Field::ClientId), Reading::Absent);
        assert_eq!(fields.read(Field::ClientSoftware), Reading::Absent);
    }

    #[test]
    fn an_identified_identity_has_every_connection_field() {
        let mut identity = Identity::of(connection());

        let client = identity.identify(Some(ClientId::new("order-ingest").unwrap()));
        identity.name_software(ClientSoftware::new("librdkafka", "2.6.1").unwrap());

        assert_eq!(client.to_string(), "order-ingest from 10.0.4.27");
        let fields = identity.fields();
        assert_eq!(text(fields.read(Field::ClientId)), "order-ingest");
        assert_eq!(text(fields.read(Field::ClientSoftware)), "librdkafka 2.6.1");
        for field in FieldGroup::Connection.fields() {
            assert!(fields.read(*field).is_present(), "{field}");
        }
    }

    #[test]
    fn the_header_of_a_request_carries_the_client_id_or_none() {
        let named = Request::new(12, 1, Some("order-ingest"), &MetadataRequest::default()).unwrap();
        let empty = Request::new(12, 1, Some(""), &MetadataRequest::default()).unwrap();
        let unnamed = Request::new(12, 1, None, &MetadataRequest::default()).unwrap();

        assert_eq!(client_id_of(&named), Ok(Some(ClientId::new("order-ingest").unwrap())));
        assert_eq!(client_id_of(&empty), Ok(Some(ClientId::new("").unwrap())));
        assert_eq!(client_id_of(&unnamed), Ok(None));
    }

    #[test]
    fn an_api_versions_request_names_the_client_software_from_version_3() {
        let named = api_versions(3, "apache-kafka-java", "3.8.0");
        let old = api_versions(2, "apache-kafka-java", "3.8.0");
        let metadata = Request::new(12, 1, Some("order-ingest"), &MetadataRequest::default()).unwrap();

        assert_eq!(
            client_software_of(&named),
            Ok(Some(ClientSoftware::new("apache-kafka-java", "3.8.0").unwrap()))
        );
        assert_eq!(client_software_of(&old), Ok(None));
        assert_eq!(client_software_of(&metadata), Ok(None));
    }

    #[test]
    fn client_software_that_is_no_name_and_version_is_an_error() {
        let error = client_software_of(&api_versions(3, "", "")).unwrap_err();

        assert_eq!(
            error,
            IdentityError::InvalidClientSoftware {
                error: ClientSoftwareError::Malformed { input: " ".to_owned() }
            }
        );
        assert_eq!(error.field(), Field::ClientSoftware);
        assert_eq!(
            error.to_string(),
            "client.software: client software \" \" does not have the form a name and a version, such as librdkafka 2.6.1"
        );
        assert!(error.source().is_none());
    }

    #[test]
    fn an_api_versions_body_that_does_not_decode_is_an_error() {
        let whole = api_versions(3, "librdkafka", "2.6.1");
        let payload = whole.frame().payload();
        let cut_short = Request::decode(Frame::new(payload.slice(..payload.len() - 4))).unwrap();

        let error = client_software_of(&cut_short).unwrap_err();

        assert!(matches!(error, IdentityError::UnreadableClientSoftware { .. }));
        assert_eq!(error.field(), Field::ClientSoftware);
        assert!(
            error
                .to_string()
                .starts_with("client.software: cannot decode request body: "),
            "{error}"
        );
        assert!(error.source().is_none());
    }

    #[test]
    fn an_invalid_client_id_names_its_field() {
        let error = IdentityError::InvalidClientId {
            error: ClientId::new("a".repeat(40_000)).unwrap_err(),
        };

        assert_eq!(error.field(), Field::ClientId);
        assert!(error.to_string().starts_with("client.id: "), "{error}");
        assert!(error.source().is_none());
    }

    #[test]
    fn a_change_reads_as_a_journal_writes_it() {
        let client = ClientInstance::new(
            Some(ClientId::new("order-ingest").unwrap()),
            "10.0.4.27".parse().unwrap(),
        );
        let software = ClientSoftware::new("librdkafka", "2.6.1").unwrap();

        assert_eq!(
            IdentityChange::Identified(client).to_string(),
            "identified as order-ingest from 10.0.4.27"
        );
        assert_eq!(
            IdentityChange::SoftwareNamed(software).to_string(),
            "runs librdkafka 2.6.1"
        );
    }

    #[test]
    fn an_identification_lists_changes_and_errors() {
        let mut identification = Identification::default();
        assert!(identification.is_uneventful());

        let software = ClientSoftware::new("librdkafka", "2.6.1").unwrap();
        identification.changed(IdentityChange::SoftwareNamed(software.clone()));
        assert_eq!(identification.changes(), [IdentityChange::SoftwareNamed(software)]);
        assert_eq!(identification.errors(), []);
        assert!(!identification.is_uneventful());
    }

    #[test]
    fn the_lifecycle_and_who_closed_have_names() {
        assert_eq!(
            [Lifecycle::Opened, Lifecycle::Identified, Lifecycle::Closed].map(|state| state.to_string()),
            ["opened", "identified", "closed"]
        );
        assert_eq!(
            [ClosedBy::Client, ClosedBy::Backend, ClosedBy::Lie, ClosedBy::Proxy].map(|by| by.to_string()),
            ["the client", "the backend", "a lie", "the proxy"]
        );
        assert!(Lifecycle::Opened < Lifecycle::Identified && Lifecycle::Identified < Lifecycle::Closed);
    }
}
