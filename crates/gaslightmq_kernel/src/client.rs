use crate::text::{self, text_value};

text_value! {
    /// The `client.id` a client sends in every request.
    ///
    /// Kafka does not restrict a client ID, and a client may send an empty one.
    /// A client ID has at most 32,767 bytes, the longest string a Kafka message carries.
    ///
    /// A request can also carry no client ID at all.
    /// That is `None`, and not an empty `ClientId`.
    ///
    /// ```
    /// use gaslightmq_kernel::prelude::*;
    ///
    /// let client = ClientId::new("order-ingest")?;
    /// assert_eq!(client, "order-ingest");
    /// assert_eq!(ClientId::new("")?.as_str(), "");
    /// # Ok::<(), KernelError>(())
    /// ```
    ClientId, "client ID", max: text::MAX_STRING_LEN, rule: text::any
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::KernelError;

    #[test]
    fn a_client_id_takes_any_text() {
        for value in ["order-ingest", "", "consumer analytics/1 é", "."] {
            assert_eq!(ClientId::new(value).unwrap().as_str(), value);
        }
    }

    #[test]
    fn a_client_id_fits_a_kafka_string() {
        assert_eq!(ClientId::MAX_LEN, 32_767);
        assert!(ClientId::new("a".repeat(32_767)).is_ok());
        assert_eq!(
            ClientId::new("a".repeat(32_768)),
            Err(KernelError::TooLong { what: "client ID", len: 32_768, max: 32_767 })
        );
    }

    #[test]
    fn a_client_id_converts_from_and_to_text() {
        let client = ClientId::new("order-ingest").unwrap();

        assert_eq!("order-ingest".parse::<ClientId>(), Ok(client.clone()));
        assert_eq!(ClientId::try_from("order-ingest"), Ok(client.clone()));
        assert_eq!(ClientId::try_from(String::from("order-ingest")), Ok(client.clone()));
        assert_eq!(String::from(client.clone()), "order-ingest");
        assert_eq!(client.to_string(), "order-ingest");
        assert_eq!(AsRef::<str>::as_ref(&client), "order-ingest");
        assert_eq!(client, "order-ingest");
        assert!(ClientId::try_from("a".repeat(32_768)).is_err());
    }

    #[test]
    fn a_set_finds_a_client_id_by_text() {
        let clients = HashSet::from([ClientId::new("order-ingest").unwrap()]);

        assert!(clients.contains("order-ingest"));
        assert!(!clients.contains("kafka-ui"));
    }
}
