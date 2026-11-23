//! The resource vocabulary: the name of a resource, and the one rule that every name follows.
//!
//! A resource is a definition that someone names: a selector, a preset, a rule or an environment.
//! Whatever references a resource does so by its name, and the control plane lists it and renames it by that name.
//!
//! | Type | Is |
//! |---|---|
//! | [`ResourceName`] | The name of a resource: text that follows [the rule for names](ResourceName#the-rule-for-names). |
//! | [`ResourceNameError`] | Why a text is no name: the part of the rule it breaks. |
//! | [`name!`] | The name of one kind of resource and its error, written into the crate of that kind. |
//!
//! ```
//! use gaslightmq_resource::{ResourceName, ResourceNameError};
//!
//! let name: ResourceName = "Order producers".parse()?;
//! assert_eq!(name, "Order producers");
//!
//! // A form checks what someone types before it asks anything of the engine.
//! let error = ResourceName::new("Order producers ").unwrap_err();
//! assert_eq!(error.to_string(), "name \"Order producers \" starts or ends with white space");
//! # Ok::<(), ResourceNameError>(())
//! ```
//!
//! # A name per kind
//!
//! Every kind of resource has a name type of its own, in the crate of that kind: the name of a selector, of a rule,
//! of a preset.
//! Each one wraps a [`ResourceName`], so the name of a selector never stands where the name of a rule belongs,
//! and the rule for names is written here alone.
//!
//! Each of those types has an error of its own too, named after it, with a variant for every part of the rule.
//! Whoever names a rule reads `rule name "" is empty`, and never meets the error of this crate.
//!
//! [`name!`] writes both types.
//! The crate of a kind calls it once for each name, with the two types and the words that a message calls the name by:
//! `gaslightmq_resource::name!(SelectorName, SelectorNameError, "selector name");`
//!
//! The crate is internal, and says so in its manifest.
//! A crate that uses it shows none of its types: not in what it offers, and not in what it documents.
//! `cargo xtask internal-crates` holds every such crate to that.
//! A call of the macro passes that check, because it names no type of this crate,
//! and what the macro writes keeps the [`ResourceName`] in a private field.
//!
//! # No IO
//!
//! The crate depends on no other crate of the workspace.
//! It has no async code, no IO, no system clock and no randomness, and it compiles to WebAssembly.
//!
//! The `serde` feature maps a [`ResourceName`] to its text.

mod kind;
mod name;

pub use name::{ResourceName, ResourceNameError};
