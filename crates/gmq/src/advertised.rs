//! Where clients see the brokers of a proxy whose ports they reach through others, as `--advertised-listeners`
//! names it: the listeners themselves, or `-` for a line of the standard input.

use gaslightmq_environment::{AdvertisedListeners, AdvertisedListenersError};

/// What `--advertised-listeners` says for a line of the standard input.
const FROM_THE_INPUT: &str = "-";

/// The advertised listeners that the flag names, or the input that names them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AdvertisedListenersFlag {
    /// The flag names them.
    Given(AdvertisedListeners),
    /// The first line of the standard input names them, in the form of the flag. The proxy waits for it before it
    /// starts: whoever starts it learns where its ports are published only once it runs, as a container does.
    FromTheInput,
}

/// The advertised listeners that `text` names, or `-` for those of the first line of the standard input.
///
/// # Errors
///
/// [`AdvertisedListenersError`] when the text names no advertised listeners.
pub(crate) fn advertised_listeners(text: &str) -> Result<AdvertisedListenersFlag, AdvertisedListenersError> {
    if text.trim() == FROM_THE_INPUT {
        return Ok(AdvertisedListenersFlag::FromTheInput);
    }
    text.parse().map(AdvertisedListenersFlag::Given)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_dash_names_the_standard_input() {
        assert_eq!(advertised_listeners("-"), Ok(AdvertisedListenersFlag::FromTheInput));
        assert_eq!(
            advertised_listeners("19092=localhost:55002"),
            Ok(AdvertisedListenersFlag::Given("19092=localhost:55002".parse().unwrap()))
        );
        assert_eq!(
            advertised_listeners(""),
            Ok(AdvertisedListenersFlag::Given(AdvertisedListeners::none()))
        );
        assert!(advertised_listeners("19092").is_err());
    }
}
