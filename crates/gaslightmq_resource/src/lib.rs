//! The resource vocabulary: the name, the UUID and the title of a resource, with the one rule that each of them
//! follows.
//!
//! A resource is a definition that someone names: a selector, a preset, a rule or an environment.
//! A name is the identity of its resource between domains, and nothing else:
//! whatever references a resource does so by its name.
//! A resource keeps its name for as long as it exists.
//! Whoever wants another name makes a new resource.
//!
//! A UUID is the identity of a resource inside the workspace.
//! It lets the workspace track and store resources without one overwriting another, and nothing else references it.
//! Nobody writes a UUID by hand: the workspace gives a resource its UUID, and keeps it in the file of the resource.
//!
//! A title is the free text that says what a resource is for.
//! It is a part of the resource, next to its name, and never a part of the name.
//! Nothing references a title, so it changes freely.
//!
//! | Type | Is |
//! |---|---|
//! | [`ResourceName`] | The name of a resource: a slug that follows [the rule for names](ResourceName#the-rule-for-names). |
//! | [`ResourceNameError`] | Why a text is no name: the part of the rule it breaks. |
//! | [`name!`] | The name of one kind of resource and its error, written into the crate of that kind. |
//! | [`ResourceUuid`] | The UUID of a resource: a UUID of version 7 that follows [the rule for UUIDs](ResourceUuid#the-rule-for-uuids). |
//! | [`ResourceUuidError`] | Why a text is no UUID. |
//! | [`uuid!`] | The UUID of one kind of resource and its error, written into the crate of that kind. |
//! | [`ResourceTitle`] | The title of a resource: free text that follows [the rule for titles](ResourceTitle#the-rule-for-titles). |
//! | [`ResourceTitleError`] | Why a text is no title: the part of the rule it breaks. |
//! | [`title!`] | The title of one kind of resource and its error, written into the crate of that kind. |
//!
//! ```
//! use gaslightmq_resource::{ResourceName, ResourceNameError};
//!
//! let name: ResourceName = "order-producers".parse()?;
//! assert_eq!(name, "order-producers");
//!
//! // A form checks what someone types before it asks anything of the engine.
//! let error = ResourceName::new("order-producers-").unwrap_err();
//! assert_eq!(error.to_string(), "name \"order-producers-\" starts or ends with a hyphen");
//! # Ok::<(), ResourceNameError>(())
//! ```
//!
//! # A name, a UUID and a title per kind
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
//! The same holds for titles.
//! [`title!`] writes the title of one kind and its error, around a [`ResourceTitle`]:
//! `gaslightmq_resource::title!(SelectorTitle, SelectorTitleError, "selector title");`
//! The type that holds a resource has the title as an optional part: a title is never a part of a name.
//!
//! And [`uuid!`] writes the UUID of one kind and its error, around a [`ResourceUuid`]:
//! `gaslightmq_resource::uuid!(SelectorUuid, SelectorUuidError, "selector UUID");`
//! The type that holds a resource has the UUID as an optional part too:
//! a resource that someone writes by hand has none until the workspace gives it one.
//!
//! The crate is internal, and says so in its manifest.
//! A crate that uses it shows none of its types: not in what it offers, and not in what it documents.
//! `cargo xtask internal-crates` holds every such crate to that.
//! A call of the macro passes that check, because it names no type of this crate,
//! and what a macro writes keeps the [`ResourceName`], the [`ResourceUuid`] or the [`ResourceTitle`] in a private
//! field.
//!
//! # No IO
//!
//! The crate depends on no other crate of the workspace.
//! It has no async code, no IO, no system clock and no randomness, and it compiles to WebAssembly.
//!
//! A new UUID is made of the time and of random bits, and this crate reads neither.
//! [`ResourceUuid::new`] takes both from its caller, and so does the `new` of the UUID of every kind.
//! The edge reads the clock and the randomness of the system when someone creates a resource, and hands them in:
//!
//! ```
//! use gaslightmq_resource::ResourceUuid;
//!
//! // What the edge read: the milliseconds since the Unix epoch, and ten random bytes.
//! let unix_millis = 1_733_938_011_966;
//! let random = [0x0a, 0x41, 0x1c, 0x2d, 0x8e, 0x4b, 0x1f, 0x6a, 0x7d, 0x10];
//!
//! let uuid = ResourceUuid::new(unix_millis, random);
//! assert_eq!(uuid.to_string(), "0193b6c2-5f3e-7a41-9c2d-8e4b1f6a7d10");
//! // The same time and the same bits make the same UUID, so a test writes its UUIDs down.
//! assert_eq!(uuid, ResourceUuid::new(unix_millis, random));
//! ```
//!
//! The `serde` feature maps a [`ResourceName`], a [`ResourceUuid`] and a [`ResourceTitle`] to their text.

mod kind;
mod name;
mod title;
mod uuid;

pub use name::{ResourceName, ResourceNameError};
pub use title::{ResourceTitle, ResourceTitleError};
pub use uuid::{ResourceUuid, ResourceUuidError};
