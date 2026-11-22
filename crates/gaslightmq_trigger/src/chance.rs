//! Probabilities and die rolls, both as whole parts per million.

use std::fmt;
use std::str::FromStr;

/// One whole, in parts per million.
const WHOLE: u32 = 1_000_000;
/// One percent, in parts per million.
const PERCENT: u32 = 10_000;
/// How many decimals a percentage of whole parts per million has at most.
const DECIMALS: usize = 4;

/// How often a chance trigger fires, such as `40%`.
///
/// A chance is a whole number of parts per million, from `0` to `1 000 000`.
/// `40%` is `400 000` parts per million, and the finest step is `0.0001%`.
/// No floating point takes part, so a chance means the same on every platform.
///
/// Its text is a decimal number with at most four decimals and a `%`, with an optional space between them.
/// A bare number such as `40` or `0.4` is not a chance, because it does not say which of the two it means.
///
/// A [`Roll`] fires the trigger when it is under the chance.
/// [`Chance::ALWAYS`] therefore fires on every roll, and [`Chance::NEVER`] on none.
///
/// ```
/// use gaslightmq_trigger::prelude::*;
///
/// let chance: Chance = "40%".parse()?;
/// assert_eq!(chance.ppm(), 400_000);
/// assert_eq!(chance, Chance::from_percent(40)?);
/// assert_eq!("0.5 %".parse::<Chance>()?.to_string(), "0.5%");
/// assert!("40".parse::<Chance>().is_err());
/// assert!("140%".parse::<Chance>().is_err());
/// # Ok::<(), ChanceError>(())
/// ```
///
/// With the `serde` feature, a chance serializes as its text.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Chance(u32);

impl Chance {
    /// `0%`: no roll is under it.
    pub const NEVER: Chance = Chance(0);

    /// `100%`: every roll is under it.
    pub const ALWAYS: Chance = Chance(WHOLE);

    /// Check that `ppm` is at most one million, and keep it.
    ///
    /// # Errors
    ///
    /// [`ChanceError::OutOfRange`] when `ppm` is above one million.
    pub fn from_ppm(ppm: u32) -> Result<Chance, ChanceError> {
        if ppm > WHOLE {
            return Err(ChanceError::OutOfRange {
                chance: Percentage(u64::from(ppm)).to_string(),
            });
        }
        Ok(Chance(ppm))
    }

    /// Check that `percent` is at most one hundred, and keep it.
    ///
    /// # Errors
    ///
    /// [`ChanceError::OutOfRange`] when `percent` is above one hundred.
    pub fn from_percent(percent: u8) -> Result<Chance, ChanceError> {
        Chance::from_ppm(u32::from(percent) * PERCENT)
    }

    /// The chance in parts per million.
    pub const fn ppm(self) -> u32 {
        self.0
    }

    /// The chance as a fraction between zero and one, for arithmetic that only shows a result.
    pub fn fraction(self) -> f64 {
        f64::from(self.0) / f64::from(WHOLE)
    }
}

/// Reads a percentage with at most four decimals, such as `40%`, `0.5%` or `12.5 %`.
impl FromStr for Chance {
    type Err = ChanceError;

    fn from_str(text: &str) -> Result<Chance, ChanceError> {
        let ppm = parse_percentage(text)?;
        match u32::try_from(ppm) {
            Ok(ppm) if ppm <= WHOLE => Ok(Chance(ppm)),
            _ => Err(ChanceError::OutOfRange {
                chance: text.to_owned(),
            }),
        }
    }
}

/// Writes the percentage with as few decimals as it needs, such as `40%` or `0.5%`.
///
/// [`FromStr`] reads the text back to the same chance.
impl fmt::Display for Chance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Percentage(u64::from(self.0)).fmt(f)
    }
}

/// Why a number or a text is not a [`Chance`].
///
/// ```
/// use gaslightmq_trigger::prelude::*;
///
/// let error = "40".parse::<Chance>().unwrap_err();
/// assert_eq!(error, ChanceError::NotAPercentage { input: "40".into() });
/// assert_eq!(error.to_string(), "`40` is not a percentage such as `40%`");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ChanceError {
    /// The text is not a percentage such as `40%`.
    NotAPercentage { input: String },
    /// The text has more than four decimals, and a chance is a whole number of parts per million.
    TooFine { input: String },
    /// The chance is above `100%`.
    /// `chance` is the percentage as the text wrote it, or as the number reads.
    OutOfRange { chance: String },
}

impl fmt::Display for ChanceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotAPercentage { input } => {
                write!(f, "`{input}` is not a percentage such as `40%`")
            }
            Self::TooFine { input } => {
                write!(f, "chance `{input}` has more than four decimals")
            }
            Self::OutOfRange { chance } => {
                write!(f, "chance `{chance}` is not between 0% and 100%")
            }
        }
    }
}

impl std::error::Error for ChanceError {}

/// What a die rolled for one match.
///
/// A roll is a whole number of parts per million, from `0` up to and not including one million.
/// It has the unit of a [`Chance`], so the two compare exactly, and an explanation shows both as percentages.
/// A [`Die`](crate::Die) makes rolls.
///
/// ```
/// use gaslightmq_trigger::prelude::*;
///
/// let chance: Chance = "40%".parse()?;
/// let roll = Roll::from_ppm(399_999)?;
/// assert!(roll.is_under(chance));
/// assert!(!Roll::from_ppm(400_000)?.is_under(chance));
/// assert_eq!(roll.to_string(), "39.9999%");
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Roll(u32);

impl Roll {
    /// The lowest roll. It is under every chance but [`Chance::NEVER`].
    pub const LOWEST: Roll = Roll(0);

    /// The highest roll. Only [`Chance::ALWAYS`] is above it.
    pub const HIGHEST: Roll = Roll(WHOLE - 1);

    /// Check that `ppm` is below one million, and keep it.
    ///
    /// # Errors
    ///
    /// [`RollError::OutOfRange`] when `ppm` is one million or more.
    pub fn from_ppm(ppm: u32) -> Result<Roll, RollError> {
        if ppm >= WHOLE {
            return Err(RollError::OutOfRange { ppm });
        }
        Ok(Roll(ppm))
    }

    /// Scales 64 uniform bits down to a roll.
    ///
    /// The roll is `(bits × 1 000 000) >> 64`, so it only depends on integer arithmetic.
    pub(crate) fn from_bits(bits: u64) -> Roll {
        let scaled = (u128::from(bits) * u128::from(WHOLE)) >> 64;
        // `bits` is below 2^64, so `scaled` is below one million.
        Roll(u32::try_from(scaled).unwrap_or(WHOLE - 1))
    }

    /// The roll in parts per million.
    pub const fn ppm(self) -> u32 {
        self.0
    }

    /// Whether this roll fires a trigger with that chance.
    pub const fn is_under(self, chance: Chance) -> bool {
        self.0 < chance.0
    }
}

/// Writes the roll as a percentage, such as `37.4213%`.
impl fmt::Display for Roll {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Percentage(u64::from(self.0)).fmt(f)
    }
}

/// Why a number is not a [`Roll`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum RollError {
    /// The number is one million or more, and a die never rolls a whole million.
    OutOfRange { ppm: u32 },
}

impl fmt::Display for RollError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OutOfRange { ppm } => {
                write!(f, "a roll of {ppm} parts per million is not below one million")
            }
        }
    }
}

impl std::error::Error for RollError {}

/// Parts per million as a percentage with as few decimals as it needs.
struct Percentage(u64);

impl fmt::Display for Percentage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let percent = u64::from(PERCENT);
        let (whole, rest) = (self.0 / percent, self.0 % percent);
        if rest == 0 {
            return write!(f, "{whole}%");
        }
        let decimals = format!("{rest:0DECIMALS$}");
        write!(f, "{whole}.{}%", decimals.trim_end_matches('0'))
    }
}

/// Reads a percentage as parts per million.
/// A percentage too large for a `u64` reads as `u64::MAX`, which is above every chance.
fn parse_percentage(text: &str) -> Result<u64, ChanceError> {
    let not_a_percentage = || ChanceError::NotAPercentage { input: text.to_owned() };
    let digits = |part: &str| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit());

    let number = text.strip_suffix('%').ok_or_else(not_a_percentage)?;
    let number = number.trim_end_matches(' ');
    let (whole, decimals) = match number.split_once('.') {
        Some((whole, decimals)) if digits(decimals) => (whole, decimals.trim_end_matches('0')),
        Some(_) => return Err(not_a_percentage()),
        None => (number, ""),
    };
    if !digits(whole) {
        return Err(not_a_percentage());
    }
    if decimals.len() > DECIMALS {
        return Err(ChanceError::TooFine { input: text.to_owned() });
    }

    let padded = format!("{decimals:0<DECIMALS$}");
    let rest = padded.parse::<u64>().unwrap_or(0);
    let ppm = whole
        .parse::<u64>()
        .ok()
        .and_then(|whole| whole.checked_mul(u64::from(PERCENT)))
        .and_then(|ppm| ppm.checked_add(rest));
    Ok(ppm.unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chance(text: &str) -> Chance {
        text.parse()
            .unwrap_or_else(|error| panic!("`{text}` should parse: {error}"))
    }

    fn error(text: &str) -> ChanceError {
        text.parse::<Chance>().expect_err(text)
    }

    #[test]
    fn forty_percent_is_four_hundred_thousand_parts_per_million() {
        assert_eq!(chance("40%").ppm(), 400_000);
        assert_eq!(chance("40%"), Chance::from_percent(40).unwrap());
        assert_eq!(chance("40%"), Chance::from_ppm(400_000).unwrap());
    }

    #[test]
    fn a_chance_reads_up_to_four_decimals() {
        for (text, ppm) in [
            ("0%", 0),
            ("100%", 1_000_000),
            ("50%", 500_000),
            ("0.5%", 5_000),
            ("12.5%", 125_000),
            ("0.0001%", 1),
            ("99.9999%", 999_999),
            ("33.3333%", 333_333),
            ("007%", 70_000),
            ("1.50%", 15_000),
            ("1.500000%", 15_000),
            ("100.0%", 1_000_000),
            ("40 %", 400_000),
            ("0.5  %", 5_000),
        ] {
            assert_eq!(chance(text).ppm(), ppm, "{text}");
        }
    }

    #[test]
    fn a_chance_writes_as_few_decimals_as_it_needs() {
        for (ppm, text) in [
            (0, "0%"),
            (1, "0.0001%"),
            (10, "0.001%"),
            (5_000, "0.5%"),
            (125_000, "12.5%"),
            (333_333, "33.3333%"),
            (400_000, "40%"),
            (999_999, "99.9999%"),
            (1_000_000, "100%"),
        ] {
            assert_eq!(Chance::from_ppm(ppm).unwrap().to_string(), text);
        }
    }

    #[test]
    fn the_written_text_reads_back_as_the_same_chance() {
        for ppm in [0, 1, 9, 10, 99, 9_999, 10_000, 10_001, 400_000, 999_999, 1_000_000] {
            let written = Chance::from_ppm(ppm).unwrap();
            assert_eq!(chance(&written.to_string()), written, "{ppm} ppm");
        }
    }

    #[test]
    fn a_bare_number_is_not_a_chance() {
        for text in [
            "",
            "%",
            " %",
            "40",
            "0.4",
            "40 percent",
            "-5%",
            "+5%",
            ".5%",
            "5.%",
            "4 0%",
            " 40%",
            "40% ",
            "1e1%",
            "1,5%",
            "1.2.3%",
            "forty%",
        ] {
            assert_eq!(
                error(text),
                ChanceError::NotAPercentage { input: text.into() },
                "`{text}`"
            );
        }
    }

    #[test]
    fn a_chance_is_at_most_one_hundred_percent() {
        for text in [
            "100.0001%",
            "101%",
            "140%",
            "99999999999999999999999999%",
            "1844674407370955.1616%",
        ] {
            assert_eq!(error(text), ChanceError::OutOfRange { chance: text.into() }, "`{text}`");
        }
        assert_eq!(
            Chance::from_ppm(1_000_001),
            Err(ChanceError::OutOfRange {
                chance: "100.0001%".into()
            }),
        );
        assert_eq!(
            Chance::from_percent(101),
            Err(ChanceError::OutOfRange { chance: "101%".into() }),
        );
        assert_eq!(Chance::from_percent(100), Ok(Chance::ALWAYS));
    }

    #[test]
    fn a_chance_is_no_finer_than_a_part_per_million() {
        for text in ["0.00001%", "40.12345%", "0.00005%"] {
            assert_eq!(error(text), ChanceError::TooFine { input: text.into() }, "`{text}`");
        }
    }

    #[test]
    fn a_chance_error_says_what_is_wrong() {
        let cases = [
            (
                ChanceError::NotAPercentage { input: "40".into() },
                "`40` is not a percentage such as `40%`",
            ),
            (
                ChanceError::TooFine {
                    input: "0.00001%".into(),
                },
                "chance `0.00001%` has more than four decimals",
            ),
            (
                ChanceError::OutOfRange { chance: "140%".into() },
                "chance `140%` is not between 0% and 100%",
            ),
        ];
        for (error, message) in cases {
            assert_eq!(error.to_string(), message);
        }
        assert_eq!(
            Chance::from_ppm(1_400_000).unwrap_err().to_string(),
            "chance `140%` is not between 0% and 100%",
        );
    }

    #[test]
    fn a_chance_is_a_fraction_for_display_arithmetic() {
        assert!((chance("40%").fraction() - 0.4).abs() < 1e-12);
        assert!((Chance::ALWAYS.fraction() - 1.0).abs() < 1e-12);
        assert!(Chance::NEVER.fraction().abs() < 1e-12);
    }

    #[test]
    fn a_roll_fires_when_it_is_under_the_chance() {
        let forty = chance("40%");
        assert!(Roll::LOWEST.is_under(forty));
        assert!(Roll::from_ppm(399_999).unwrap().is_under(forty));
        assert!(!Roll::from_ppm(400_000).unwrap().is_under(forty));
        assert!(!Roll::HIGHEST.is_under(forty));
    }

    #[test]
    fn always_fires_on_every_roll_and_never_on_none() {
        for roll in [Roll::LOWEST, Roll::from_ppm(500_000).unwrap(), Roll::HIGHEST] {
            assert!(roll.is_under(Chance::ALWAYS));
            assert!(!roll.is_under(Chance::NEVER));
        }
    }

    #[test]
    fn a_roll_is_below_one_million() {
        assert_eq!(Roll::from_ppm(999_999), Ok(Roll::HIGHEST));
        assert_eq!(Roll::from_ppm(1_000_000), Err(RollError::OutOfRange { ppm: 1_000_000 }));
    }

    #[test]
    fn a_roll_error_says_what_is_wrong() {
        assert_eq!(
            RollError::OutOfRange { ppm: 1_000_000 }.to_string(),
            "a roll of 1000000 parts per million is not below one million",
        );
    }

    #[test]
    fn sixty_four_bits_scale_down_to_a_roll() {
        assert_eq!(Roll::from_bits(0), Roll::LOWEST);
        assert_eq!(Roll::from_bits(u64::MAX), Roll::HIGHEST);
        assert_eq!(Roll::from_bits(1 << 63).ppm(), 500_000);
        assert_eq!(Roll::from_bits(1 << 62).ppm(), 250_000);
    }

    #[test]
    fn a_roll_writes_as_a_percentage() {
        assert_eq!(Roll::from_ppm(374_213).unwrap().to_string(), "37.4213%");
        assert_eq!(Roll::LOWEST.to_string(), "0%");
        assert_eq!(Roll::HIGHEST.to_string(), "99.9999%");
    }
}
