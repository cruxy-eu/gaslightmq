//! What the types with a text form share: conversions from and to owned text, and their `serde` form.

/// Give a type that has a text form the conversions from and to owned text, and the text form in `serde`.
///
/// The type implements `FromStr` and `Display`, and this macro builds on both.
///
/// `error` is the error of the type's `FromStr`.
macro_rules! text_form {
    ($name:ident, $error:ident) => {
        impl TryFrom<&str> for $name {
            type Error = $error;

            fn try_from(text: &str) -> Result<Self, $error> {
                text.parse()
            }
        }

        impl TryFrom<String> for $name {
            type Error = $error;

            fn try_from(text: String) -> Result<Self, $error> {
                text.parse()
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.to_string()
            }
        }

        #[cfg(feature = "serde")]
        impl serde::Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.collect_str(self)
            }
        }

        #[cfg(feature = "serde")]
        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let text = <String as serde::Deserialize>::deserialize(deserializer)?;
                text.parse().map_err(serde::de::Error::custom)
            }
        }
    };
}

pub(crate) use text_form;
