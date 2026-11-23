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

/// Give a type that keeps its text as written what a text has: the text itself, comparison with text, and
/// [`text_form`].
///
/// The type is a tuple struct around its text, which is an `Arc<str>` or a type that is a text itself,
/// and it has a `new` that checks the text.
macro_rules! kept_text {
    ($name:ident, $error:ident) => {
        impl $name {
            /// The value as text.
            pub fn as_str(&self) -> &str {
                self.0.as_ref()
            }
        }

        impl std::str::FromStr for $name {
            type Err = $error;

            fn from_str(text: &str) -> Result<Self, Self::Err> {
                Self::new(text)
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.pad(self.as_str())
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        /// A map by this text finds its entry with plain text.
        impl std::borrow::Borrow<str> for $name {
            fn borrow(&self) -> &str {
                self.as_str()
            }
        }

        impl PartialEq<str> for $name {
            fn eq(&self, other: &str) -> bool {
                self.as_str() == other
            }
        }

        impl PartialEq<&str> for $name {
            fn eq(&self, other: &&str) -> bool {
                self.as_str() == *other
            }
        }

        $crate::text::text_form!($name, $error);
    };
}

pub(crate) use {kept_text, text_form};
