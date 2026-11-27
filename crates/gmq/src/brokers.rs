//! Where the brokers of a proxy listen, as the flag `--brokers` names it.

use gaslightmq_control_api::kernel::{BrokerId, BrokerIdError};
use gaslightmq_environment::{BrokerListeners, BrokerListenersError, ListenAddress, ListenAddressError};

/// What stands between the ID of a broker and its address in a pin: `1=0.0.0.0:19092`.
const PIN: char = '=';

/// What stands between two pins: `1=0.0.0.0:19092,2=0.0.0.0:19093`.
const PINS: char = ',';

/// The broker listeners that `text` names: a range of ports, any port, or a pin for every broker.
///
/// A range, `0.0.0.0:19092-19191`, gives the brokers that the proxy discovers a port each, in the order of their
/// IDs. Any port, `127.0.0.1:0`, gives each broker that the proxy discovers a free port that the system picks.
/// Pins, `1=0.0.0.0:19092,2=0.0.0.0:19093`, give each broker its own address, and port 0 takes a free port.
/// The text of a range or of any port is the one an environment file holds.
///
/// # Errors
///
/// [`BrokersError`] when the text is no range and no port 0, a pin is no broker ID and an address, or the pins do
/// not hold together.
pub(crate) fn broker_listeners(text: &str) -> Result<BrokerListeners, BrokersError> {
    if !text.contains(PIN) {
        return text.parse().map_err(|error| BrokersError::InvalidListeners { error });
    }
    let pins = text
        .split(PINS)
        .map(str::trim)
        .map(pinned)
        .collect::<Result<Vec<_>, _>>()?;
    BrokerListeners::pinned(pins).map_err(|error| BrokersError::InvalidListeners { error })
}

/// One broker and its address, as `1=0.0.0.0:19092` pins it.
fn pinned(pin: &str) -> Result<(BrokerId, ListenAddress), BrokersError> {
    let Some((broker, address)) = pin.split_once(PIN) else {
        return Err(BrokersError::NoPin { pin: pin.to_owned() });
    };
    let broker = broker.trim().parse().map_err(|error| BrokersError::InvalidBroker {
        pin: pin.to_owned(),
        error,
    })?;
    let address = address.trim().parse().map_err(|error| BrokersError::InvalidAddress {
        pin: pin.to_owned(),
        error,
    })?;
    Ok((broker, address))
}

/// Why the text of `--brokers` names no broker listeners.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum BrokersError {
    /// The text is no range of ports and no port 0, or the pins do not hold together: two pin one broker, or two brokers share an
    /// address.
    #[error("{error}")]
    InvalidListeners { error: BrokerListenersError },
    /// A pin among others has no `=`.
    #[error("broker pin {pin:?} is no broker ID and an address with `=` between them, such as 1=0.0.0.0:19092")]
    NoPin { pin: String },
    /// What stands before the `=` of a pin is no broker ID.
    #[error("broker pin {pin:?}: {error}")]
    InvalidBroker { pin: String, error: BrokerIdError },
    /// What stands after the `=` of a pin is no address to listen on.
    #[error("broker pin {pin:?}: {error}")]
    InvalidAddress { pin: String, error: ListenAddressError },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_range_gives_every_broker_a_port_of_it() {
        let listeners = broker_listeners("0.0.0.0:19092-19191").unwrap();

        assert_eq!(listeners, "0.0.0.0:19092-19191".parse().unwrap());
    }

    #[test]
    fn port_zero_gives_every_broker_a_free_port() {
        let listeners = broker_listeners("127.0.0.1:0").unwrap();

        assert_eq!(listeners.as_any_port(), Some("127.0.0.1".parse().unwrap()));
    }

    #[test]
    fn pins_give_each_broker_its_address() {
        let listeners = broker_listeners("1=127.0.0.1:0, 2=127.0.0.1:19093").unwrap();

        let pinned = listeners.as_pinned().unwrap();
        assert_eq!(pinned[&BrokerId::from(1)], "127.0.0.1:0".parse().unwrap());
        assert_eq!(pinned[&BrokerId::from(2)], "127.0.0.1:19093".parse().unwrap());
    }

    #[test]
    fn a_pin_without_an_equals_sign_is_no_pin() {
        let error = broker_listeners("1=127.0.0.1:0,127.0.0.1:1").unwrap_err();

        assert_eq!(
            error,
            BrokersError::NoPin {
                pin: "127.0.0.1:1".to_owned()
            }
        );
    }

    #[test]
    fn a_pin_names_a_broker_and_an_address() {
        assert!(matches!(
            broker_listeners("one=127.0.0.1:0"),
            Err(BrokersError::InvalidBroker { .. })
        ));
        assert!(matches!(
            broker_listeners("1=localhost:0"),
            Err(BrokersError::InvalidAddress { .. })
        ));
    }

    #[test]
    fn two_pins_of_one_broker_do_not_hold_together() {
        let error = broker_listeners("1=127.0.0.1:0,1=127.0.0.1:1").unwrap_err();

        assert_eq!(
            error,
            BrokersError::InvalidListeners {
                error: BrokerListenersError::PinnedTwice {
                    broker: BrokerId::from(1)
                }
            }
        );
    }
}
