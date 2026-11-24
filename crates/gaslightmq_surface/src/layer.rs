use std::fmt;
use std::str::FromStr;

use gaslightmq_field::{Field, FieldValue};
use gaslightmq_kernel::{ClientId, ClientIdError};
use gaslightmq_selector::{Condition, Scope};

/// A layer: a page of the pad grid for one scope.
///
/// The same row and column hold different rules on different layers,
/// because a rule that a pad creates applies to the scope of its layer.
/// A layer is not a priority: a rule on a client layer does not win over a rule on the layer of all clients.
///
/// A layer is a scope, and no definition: it has no name, and two layers are the same when their scopes are.
/// A surface shows a layer by its text, and a [`Pad`](crate::Pad) says its layer by the layer itself.
///
/// | Layer | Text | Built with |
/// |---|---|---|
/// | All clients | `All clients` | [`all_clients`](Self::all_clients) |
/// | One client | Its `client.id`, such as `order-ingest`. | [`client`](Self::client) |
/// | Any other scope | The conditions of the scope, such as `endpoint is broker 2`. | [`new`](Self::new) |
///
/// The text of the first two reads back as the layer.
/// A `client.id` is not a name, so it is any text that a client sends.
///
/// ```
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_selector::prelude::*;
/// use gaslightmq_surface::prelude::*;
///
/// let everyone: Layer = "All clients".parse()?;
/// assert_eq!(everyone, Layer::all_clients());
/// assert!(everyone.scope().is_all());
///
/// let ingest: Layer = "order-ingest".parse()?;
/// assert_eq!(ingest.scope(), &Scope::client(ClientId::new("order-ingest").expect("a client ID")));
/// assert_eq!(ingest.to_string(), "order-ingest");
///
/// let second_broker = Layer::new(Scope::endpoint(Endpoint::Broker(BrokerId::from(2))));
/// assert_eq!(second_broker.to_string(), "endpoint is broker 2");
/// # Ok::<(), LayerError>(())
/// ```
///
/// With the `serde` feature, a layer serializes as the text that reads back as it, and else as the map of its scope:
///
/// ```yaml
/// layers:
///   - All clients
///   - order-ingest
///   - { endpoint: broker 2 }
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Layer(Scope);

impl Layer {
    /// The text of the layer of all clients.
    pub const ALL_CLIENTS: &'static str = "All clients";

    /// The layer of that scope.
    pub const fn new(scope: Scope) -> Self {
        Self(scope)
    }

    /// The layer of all clients.
    pub fn all_clients() -> Self {
        Self(Scope::all())
    }

    /// The layer of one client: every connection that carries this `client.id`.
    pub fn client(id: ClientId) -> Self {
        Self(Scope::client(id))
    }

    /// Who the rules of the layer apply to.
    pub const fn scope(&self) -> &Scope {
        &self.0
    }

    /// The client of the layer, when its scope is one client and nothing else.
    fn of_client(&self) -> Option<&ClientId> {
        let conditions = self.0.conditions();
        match conditions.get(Field::ClientId) {
            Some(Condition::Is(FieldValue::ClientId(id))) if conditions.len() == 1 => Some(id),
            _ => None,
        }
    }

    /// The text that reads back as the layer, which the layer of all clients and the layer of one client have.
    ///
    /// A client whose `client.id` is the text of the layer of all clients has none: that text reads as all clients.
    pub(crate) fn text(&self) -> Option<&str> {
        if self.0.is_all() {
            return Some(Self::ALL_CLIENTS);
        }
        self.of_client()
            .map(ClientId::as_str)
            .filter(|id| *id != Self::ALL_CLIENTS)
    }
}

impl From<Scope> for Layer {
    fn from(scope: Scope) -> Self {
        Self::new(scope)
    }
}

/// Reads `All clients` as the layer of all clients, and any other text as the layer of the client with that `client.id`.
impl FromStr for Layer {
    type Err = LayerError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        if text == Self::ALL_CLIENTS {
            return Ok(Self::all_clients());
        }
        ClientId::new(text)
            .map(Self::client)
            .map_err(|error| LayerError::InvalidClient { error })
    }
}

/// Writes the text of the layer: `All clients`, the `client.id` of its client, or else the conditions of its scope.
impl fmt::Display for Layer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.text() {
            Some(text) => f.write_str(text),
            None => self.0.fmt(f),
        }
    }
}

/// Why a text is not a [`Layer`].
///
/// ```
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_surface::prelude::*;
///
/// let too_long = "a".repeat(ClientId::MAX_LEN + 1);
/// let error = too_long.parse::<Layer>().unwrap_err();
/// assert_eq!(error.to_string(), "the layer of one client: client ID of 32768 bytes exceeds the limit of 32767");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LayerError {
    /// The text is no `client.id`, so it names no client that a layer could be for.
    #[error("the layer of one client: {error}")]
    InvalidClient { error: ClientIdError },
}

#[cfg(test)]
mod tests {
    use gaslightmq_field::Endpoint;
    use gaslightmq_kernel::prelude::*;
    use gaslightmq_selector::Conditions;

    use super::*;

    fn client(id: &str) -> ClientId {
        ClientId::new(id).unwrap()
    }

    #[test]
    fn the_layer_of_all_clients_has_no_condition() {
        let layer = Layer::all_clients();

        assert!(layer.scope().is_all());
        assert_eq!(Layer::ALL_CLIENTS.parse(), Ok(layer.clone()));
        assert_eq!(layer.to_string(), Layer::ALL_CLIENTS);
        assert_eq!(layer.text(), Some(Layer::ALL_CLIENTS));
        assert_eq!(Layer::from(Scope::all()), layer);
    }

    #[test]
    fn any_other_text_is_the_layer_of_that_client() {
        // A `client.id` is no name: a client sends any text, also an empty one.
        for id in ["order-ingest", "Order Ingest (EU)", "all clients", ""] {
            let layer: Layer = id.parse().unwrap();

            assert_eq!(layer, Layer::client(client(id)));
            assert_eq!(layer.scope(), &Scope::client(client(id)));
            assert_eq!(layer.text(), Some(id));
            assert_eq!(layer.to_string(), id);
        }
    }

    #[test]
    fn a_layer_of_any_other_scope_shows_the_conditions_of_its_scope() {
        let second_broker = Layer::new(Scope::endpoint(Endpoint::Broker(BrokerId::from(2))));
        assert_eq!(second_broker.text(), None);
        assert_eq!(second_broker.to_string(), "endpoint is broker 2");

        let instance = Layer::new(Scope::client_instance(
            client("order-ingest"),
            "10.0.4.27".parse().unwrap(),
        ));
        assert_eq!(instance.text(), None);
        assert_eq!(instance.to_string(), instance.scope().to_string());

        let every_other_client = Scope::new(Conditions::from(Condition::is_not(client("kafka-ui")))).unwrap();
        assert_eq!(Layer::new(every_other_client).text(), None);
    }

    #[test]
    fn a_client_whose_id_is_the_text_of_all_clients_has_a_layer_without_a_text() {
        let layer = Layer::client(client(Layer::ALL_CLIENTS));

        assert_ne!(layer, Layer::all_clients());
        assert_eq!(layer.text(), None);
        assert_eq!(layer.to_string(), "client.id is All clients");
    }

    #[test]
    fn two_layers_are_the_same_when_their_scopes_are() {
        let said = "order-ingest".parse::<Layer>().unwrap();
        let built = Layer::new(Scope::client(client("order-ingest")));

        assert_eq!(said, built);
        assert_ne!(said, Layer::client(client("payments-svc")));
    }

    #[test]
    fn a_text_that_is_no_client_id_is_an_error() {
        let long = "a".repeat(ClientId::MAX_LEN + 1);
        let error = long.parse::<Layer>().unwrap_err();

        assert_eq!(
            error,
            LayerError::InvalidClient {
                error: ClientId::new(&long).unwrap_err()
            }
        );
        assert_eq!(
            error.to_string(),
            "the layer of one client: client ID of 32768 bytes exceeds the limit of 32767"
        );
    }
}
