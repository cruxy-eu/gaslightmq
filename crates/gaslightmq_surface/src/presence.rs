gaslightmq_resource::random_id! {
    /// A presence is one client of a control surface that is there, such as a tab of the UI.
    ///
    /// A presence is there from the moment its client connects until the client is gone.
    /// It outlives a connection that drops while its client connects again, and it ends when the client stays away.
    /// Whoever holds the rules gives every [`Hold`](crate::Hold) to the presence whose press added it,
    /// and ends the holds of a presence that ended: a client that is gone never sends its release.
    ///
    /// Whoever lets clients connect makes the ID of each presence.
    /// A client that comes back names its presence by this ID, so no client names the presence of another one
    /// by accident, in this run of the process or in a later one.
    ///
    /// ```
    /// use gaslightmq_surface::prelude::*;
    ///
    /// let presence = PresenceId::from_random_bits(0x9f86_d081_884c_7d65_9a2f_eaa0_c55a_d015);
    /// assert_eq!(presence.to_string(), "9f86d081884c7d659a2feaa0c55ad015");
    /// assert_eq!(format!("{presence:?}"), "PresenceId(9f86d081884c7d659a2feaa0c55ad015)");
    ///
    /// // A client that comes back names the presence it had.
    /// let named: PresenceId = "9f86d081884c7d659a2feaa0c55ad015".parse()?;
    /// assert_eq!(named, presence);
    ///
    /// let error = PresenceId::new("tab-7").unwrap_err();
    /// assert_eq!(error.to_string(), "a presence ID has 32 hexadecimal digits, and not 5 characters");
    /// # Ok::<(), PresenceIdError>(())
    /// ```
    ///
    /// With the `serde` feature, a presence ID serializes as its text.
    PresenceId, PresenceIdError, "presence ID"
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The bits of the presence of the tests, and the text that writes them.
    const BITS: u128 = 0x9f86_d081_884c_7d65_9a2f_eaa0_c55a_d015;
    const TEXT: &str = "9f86d081884c7d659a2feaa0c55ad015";

    #[test]
    fn a_presence_id_writes_its_bits_as_hexadecimal_digits_and_reads_them_back() {
        let presence = PresenceId::from_random_bits(BITS);

        assert_eq!(presence.bits(), BITS);
        assert_eq!(presence.to_string(), TEXT);
        assert_eq!(TEXT.parse(), Ok(presence));
        assert_eq!(PresenceId::try_from(TEXT), Ok(presence));
        assert_eq!(PresenceId::try_from(TEXT.to_owned()), Ok(presence));
        assert_eq!(String::from(presence), TEXT);
    }

    #[test]
    fn an_id_of_few_bits_keeps_its_leading_zeros() {
        let presence = PresenceId::from_random_bits(7);

        assert_eq!(presence.to_string(), "00000000000000000000000000000007");
        assert_eq!(presence.to_string().parse(), Ok(presence));
    }

    #[test]
    fn a_text_of_another_length_is_no_presence_id() {
        let long = format!("{TEXT}0");
        for (text, length) in [("", 0), ("9f86", 4), (long.as_str(), 33)] {
            assert_eq!(PresenceId::new(text), Err(PresenceIdError::WrongLength { length }));
        }
    }

    #[test]
    fn a_character_that_is_no_hexadecimal_digit_in_lower_case_is_an_error() {
        let upper = TEXT.to_uppercase();
        let signed = format!("+{}", &TEXT[1..]);

        assert_eq!(
            PresenceId::new(&upper),
            Err(PresenceIdError::InvalidCharacter {
                input: upper.clone(),
                character: 'F',
                position: 1,
            })
        );
        assert_eq!(
            PresenceId::new(&signed),
            Err(PresenceIdError::InvalidCharacter {
                input: signed.clone(),
                character: '+',
                position: 0,
            })
        );
    }
}
