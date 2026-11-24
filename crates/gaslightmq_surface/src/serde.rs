//! The `serde` forms that are not derived where their type stands.
//!
//! A layer is its text or a map, so reading needs a format that describes itself, such as YAML or JSON.

use std::fmt;

use gaslightmq_kernel::ErrorCode;
use gaslightmq_rule::PresetName;
use gaslightmq_selector::{Scope, SelectorName};
use serde::de::value::MapAccessDeserializer;
use serde::de::{self, Deserialize, Deserializer, MapAccess, Visitor};
use serde::ser::{Serialize, Serializer};

use crate::{Encoder, Layer, LayoutName, LayoutTitle, LayoutUuid, PadLayout, PadLayoutError, PlayMode};

/// A layout as a file writes it, before anyone checked that every layer and every name stands in it once.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LayoutMap {
    name: LayoutName,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    uuid: Option<LayoutUuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    title: Option<LayoutTitle>,
    rows: Vec<SelectorName>,
    columns: Vec<PresetName>,
    layers: Vec<Layer>,
}

impl TryFrom<LayoutMap> for PadLayout {
    type Error = PadLayoutError;

    fn try_from(map: LayoutMap) -> Result<Self, Self::Error> {
        let layout = PadLayout::new(map.name, map.rows, map.columns, map.layers)?;
        let layout = match map.uuid {
            Some(uuid) => layout.with_uuid(uuid),
            None => layout,
        };
        Ok(match map.title {
            Some(title) => layout.with_title(title),
            None => layout,
        })
    }
}

impl From<PadLayout> for LayoutMap {
    fn from(layout: PadLayout) -> Self {
        let uuid = layout.uuid();
        let (name, title, rows, columns, layers) = layout.into_parts();
        Self {
            name,
            uuid,
            title,
            rows,
            columns,
            layers,
        }
    }
}

/// Writes the text of the layer of all clients and of the layer of one client, and else the map of the scope.
impl Serialize for Layer {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.text() {
            Some(text) => serializer.serialize_str(text),
            None => self.scope().serialize(serializer),
        }
    }
}

/// Reads text as the layer of all clients or of one client, and a map as the layer of that scope.
impl<'de> Deserialize<'de> for Layer {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct TextOrScope;

        impl<'de> Visitor<'de> for TextOrScope {
            type Value = Layer;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("`All clients`, the `client.id` of a client, or the map of a scope")
            }

            fn visit_str<E: de::Error>(self, text: &str) -> Result<Layer, E> {
                text.parse().map_err(E::custom)
            }

            fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<Layer, A::Error> {
                Scope::deserialize(MapAccessDeserializer::new(map)).map(Layer::new)
            }
        }

        deserializer.deserialize_any(TextOrScope)
    }
}

/// Writes the name of the encoder.
impl Serialize for Encoder {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.name())
    }
}

/// Reads the name of an encoder.
impl<'de> Deserialize<'de> for Encoder {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?.parse().map_err(de::Error::custom)
    }
}

/// Writes the name of the mode.
impl Serialize for PlayMode {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.name())
    }
}

/// Reads the name of a mode.
impl<'de> Deserialize<'de> for PlayMode {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?.parse().map_err(de::Error::custom)
    }
}

/// Reads the errors of an encoder mapping, each one where it stands first.
pub(crate) fn errors_once_each<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<ErrorCode>, D::Error> {
    Vec::<ErrorCode>::deserialize(deserializer).map(crate::mapping::once_each)
}
