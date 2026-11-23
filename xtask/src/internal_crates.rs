//! The check that no crate shows a type of an internal crate.
//!
//! `crates.md` says that an internal crate stays inside the crates that use it.
//! Its types are a detail of those crates: a crate that depends on it offers types of its own,
//! and neither what it offers nor what it documents names the internal crate.
//!
//! A crate declares itself internal in its `Cargo.toml`:
//!
//! ```toml
//! [package.metadata.gaslightmq]
//! internal = true
//! ```
//!
//! The names of an internal crate are the name of the crate, and the name of every type it exports:
//! a struct, an enum, a union, a trait or a type alias that is `pub`.
//! The check reads every crate that depends on an internal crate, without its dev-dependencies.
//! In such a crate, none of these names one of them:
//!
//! - The signature of a `pub` function or method, with its bounds.
//! - A `pub` field of a `pub` struct, with a name or in a tuple, and a field of a variant of a `pub` enum.
//! - A `pub` type alias, constant or static, and the bounds of a `pub` type.
//! - A `pub` trait: its bounds, the signatures of its methods and its associated types.
//! - An implementation of a trait: the trait, the type, the associated types and the signatures.
//!   `From<ResourceName>`, `AsRef<ResourceName>` and a `Deref` with `Target = ResourceName` all hand one out.
//! - A `pub use`, also in a prelude.
//! - A doc comment or a `#[doc]` attribute, with the code of its examples.
//!   The check reads those inside the call and the definition of a macro too.
//!
//! A private `use`, a private field, a `pub(crate)` item and the body of a function are private code, and pass.
//! So does a comment that is no doc comment.
//!
//! What the check does not catch:
//!
//! - A type behind another name. The check reads names, and not types:
//!   a private type alias, a type parameter or a private wrapper of an internal type hides it in a signature.
//! - An internal type under a new name: `use gaslightmq_resource::ResourceName as Name`.
//! - An item that a macro writes, or one inside a function body. See [`source`].
//!   Only the doc comments in a macro are read.
//! - A function, a constant or a macro of an internal crate. Its names are its types.
//!
//! What the check refuses although it is private:
//!
//! - A `pub` item in a private module that nothing exports. The check takes every `pub` item as public.
//! - An implementation of a private trait, or for a private type. The check takes every implementation as public.
//! - A type of the crate itself, or a plain word of a doc comment, that has the name of an internal type.
//!   The check knows a type by its name.

use std::collections::BTreeSet;
use std::fmt;
use std::path::PathBuf;

use proc_macro2::{Delimiter, Ident, TokenStream, TokenTree};
use syn::visit::{self, Visit};
use syn::{Attribute, Expr, Field, ImplItem, Item, ItemImpl, Lit, Macro, Meta, TraitItem, Type, UseTree, Visibility};

use crate::source::{self, SourceError, SourceFile};
use crate::workspace::{Member, Workspace};

/// The attribute that a doc comment is.
const DOC: &str = "doc";

/// An internal crate, with the types it exports.
#[derive(Debug, PartialEq)]
pub struct InternalCrate {
    /// The package name, which is its name in a path too: `gaslightmq_resource`.
    name: String,
    /// The name of every type it exports: `ResourceName`.
    types: BTreeSet<String>,
}

impl InternalCrate {
    /// The crate of this name, with the `pub` types that its source holds.
    fn read(name: &str, files: &[SourceFile]) -> Self {
        let types = files
            .iter()
            .flat_map(SourceFile::items)
            .filter_map(|item| match item {
                Item::Struct(item) if is_public(&item.vis) => Some(&item.ident),
                Item::Enum(item) if is_public(&item.vis) => Some(&item.ident),
                Item::Union(item) if is_public(&item.vis) => Some(&item.ident),
                Item::Trait(item) if is_public(&item.vis) => Some(&item.ident),
                Item::Type(item) if is_public(&item.vis) => Some(&item.ident),
                _ => None,
            })
            .map(ToString::to_string)
            .collect();
        InternalCrate {
            name: name.to_owned(),
            types,
        }
    }

    /// Whether `name` is the crate or one of its types.
    fn has(&self, name: &str) -> bool {
        self.name == name || self.types.contains(name)
    }

    /// The crate and its types, each as a name.
    fn names(&self) -> impl Iterator<Item = &str> {
        std::iter::once(self.name.as_str()).chain(self.types.iter().map(String::as_str))
    }
}

/// Every place where a crate shows a type of an internal crate it depends on.
pub fn violations(workspace: &Workspace) -> Result<Vec<InternalCrateViolation>, SourceError> {
    let internal: Vec<InternalCrate> = workspace
        .members
        .iter()
        .filter(|member| member.internal)
        .map(|member| Ok(InternalCrate::read(&member.name, &source::of(&workspace.root, member)?)))
        .collect::<Result<_, SourceError>>()?;
    let mut violations = Vec::new();
    for member in &workspace.members {
        let used = used_by(member, &internal);
        if !used.is_empty() {
            violations.extend(in_files(&source::of(&workspace.root, member)?, &used));
        }
    }
    Ok(violations)
}

/// The internal crates that a crate builds with. What only its tests need is not among them.
fn used_by<'a>(member: &Member, internal: &'a [InternalCrate]) -> Vec<&'a InternalCrate> {
    internal
        .iter()
        .filter(|internal| member.dependencies.contains(&internal.name))
        .collect()
}

/// The violations in the source of one crate, which uses these internal crates.
fn in_files(files: &[SourceFile], used: &[&InternalCrate]) -> Vec<InternalCrateViolation> {
    let mut violations = Vec::new();
    for file in files.iter().filter(|file| !file.is_test()) {
        let mut found = offered(file, used);
        let mut docs = Docs {
            used,
            found: Vec::new(),
        };
        docs.visit_file(&file.syntax);
        found.extend(docs.found.into_iter().map(|named| (Shown::Doc, named)));
        found.sort_by_key(|(_, named)| named.line);
        found.dedup();
        violations.extend(found.into_iter().map(|(shown_by, named)| InternalCrateViolation {
            path: file.path.clone(),
            line: named.line,
            shown_by,
            name: named.name,
            internal: named.internal,
        }));
    }
    violations
}

/// A name of an internal crate, where the source writes it.
#[derive(Debug, PartialEq)]
struct Named {
    line: usize,
    /// The name as written: a type of the internal crate, or the crate itself.
    name: String,
    /// The internal crate that has the name.
    internal: String,
}

/// The internal crate that has this name, as a place that writes it on `line`.
fn named(used: &[&InternalCrate], name: &str, line: usize) -> Option<Named> {
    let internal = used.iter().find(|internal| internal.has(name))?;
    Some(Named {
        line,
        name: name.to_owned(),
        internal: internal.name.clone(),
    })
}

/// What one file offers to other crates that names an internal crate.
fn offered(file: &SourceFile, used: &[&InternalCrate]) -> Vec<(Shown, Named)> {
    let mut offered = Offered {
        used,
        found: Vec::new(),
    };
    for item in file.items() {
        offered.item(item);
    }
    offered.found
}

/// Reads the items of one file, and notes what each one offers that names an internal crate.
struct Offered<'a> {
    used: &'a [&'a InternalCrate],
    found: Vec<(Shown, Named)>,
}

impl<'a> Offered<'a> {
    /// A walk over a part of an item that other crates see.
    fn paths(&self) -> Paths<'a> {
        Paths {
            used: self.used,
            found: Vec::new(),
        }
    }

    /// Notes what a walk found, as shown by one item.
    fn shows(&mut self, shown_by: &Shown, paths: Paths<'_>) {
        self.found
            .extend(paths.found.into_iter().map(|named| (shown_by.clone(), named)));
    }

    fn item(&mut self, item: &Item) {
        let mut paths = self.paths();
        match item {
            Item::Fn(item) if is_public(&item.vis) => {
                paths.visit_signature(&item.sig);
                self.shows(&Shown::Function(item.sig.ident.to_string()), paths);
            }
            Item::Struct(item) if is_public(&item.vis) => {
                let name = item.ident.to_string();
                paths.visit_generics(&item.generics);
                self.shows(&Shown::Bounds(name.clone()), paths);
                self.fields(&name, item.fields.iter().filter(|field| is_public(&field.vis)));
            }
            Item::Union(item) if is_public(&item.vis) => {
                let name = item.ident.to_string();
                paths.visit_generics(&item.generics);
                self.shows(&Shown::Bounds(name.clone()), paths);
                self.fields(&name, item.fields.named.iter().filter(|field| is_public(&field.vis)));
            }
            Item::Enum(item) if is_public(&item.vis) => {
                paths.visit_generics(&item.generics);
                self.shows(&Shown::Bounds(item.ident.to_string()), paths);
                for variant in &item.variants {
                    // A field of a variant is as public as its enum.
                    self.fields(&format!("{}::{}", item.ident, variant.ident), variant.fields.iter());
                }
            }
            Item::Type(item) if is_public(&item.vis) => {
                paths.visit_generics(&item.generics);
                paths.visit_type(&item.ty);
                self.shows(&Shown::Alias(item.ident.to_string()), paths);
            }
            Item::Const(item) if is_public(&item.vis) => {
                paths.visit_type(&item.ty);
                self.shows(&Shown::Constant(item.ident.to_string()), paths);
            }
            Item::Static(item) if is_public(&item.vis) => {
                paths.visit_type(&item.ty);
                self.shows(&Shown::Constant(item.ident.to_string()), paths);
            }
            Item::Trait(item) if is_public(&item.vis) => {
                paths.visit_generics(&item.generics);
                for bound in &item.supertraits {
                    paths.visit_type_param_bound(bound);
                }
                for inner in &item.items {
                    match inner {
                        TraitItem::Fn(function) => paths.visit_signature(&function.sig),
                        TraitItem::Type(associated) => paths.visit_trait_item_type(associated),
                        TraitItem::Const(constant) => paths.visit_type(&constant.ty),
                        _ => {}
                    }
                }
                self.shows(&Shown::Trait(item.ident.to_string()), paths);
            }
            Item::Impl(item) => self.implementation(item),
            Item::Use(item) if is_public(&item.vis) => {
                let mut exported = Vec::new();
                in_use(&item.tree, self.used, &mut exported);
                self.found
                    .extend(exported.into_iter().map(|named| (Shown::ReExport, named)));
            }
            Item::ExternCrate(item) if is_public(&item.vis) => {
                let line = item.ident.span().start().line;
                let exported = named(self.used, &item.ident.to_string(), line);
                self.found.extend(exported.map(|named| (Shown::ReExport, named)));
            }
            _ => {}
        }
    }

    /// Reads the fields of `holder` that other crates see.
    fn fields<'f>(&mut self, holder: &str, fields: impl Iterator<Item = &'f Field>) {
        for field in fields {
            let mut paths = self.paths();
            paths.visit_type(&field.ty);
            self.shows(&Shown::Field(holder.to_owned()), paths);
        }
    }

    /// Reads one `impl` block: all of an implementation of a trait, and the `pub` items of the others.
    fn implementation(&mut self, item: &ItemImpl) {
        let for_type = last_name(&item.self_ty).unwrap_or_else(|| "a type".to_owned());
        if let Some((of, _)) = &item.trait_ {
            let mut paths = self.paths();
            paths.visit_generics(&item.generics);
            paths.visit_path(of);
            paths.visit_type(&item.self_ty);
            for inner in &item.items {
                match inner {
                    ImplItem::Fn(function) => paths.visit_signature(&function.sig),
                    ImplItem::Type(associated) => paths.visit_type(&associated.ty),
                    ImplItem::Const(constant) => paths.visit_type(&constant.ty),
                    _ => {}
                }
            }
            let of = of.segments.last().map_or_default(|last| last.ident.to_string());
            self.shows(&Shown::Implementation { of, for_type }, paths);
            return;
        }
        for inner in &item.items {
            let mut paths = self.paths();
            match inner {
                ImplItem::Fn(function) if is_public(&function.vis) && !source::is_test(&function.attrs) => {
                    paths.visit_signature(&function.sig);
                    self.shows(&Shown::Function(format!("{for_type}::{}", function.sig.ident)), paths);
                }
                ImplItem::Const(constant) if is_public(&constant.vis) => {
                    paths.visit_type(&constant.ty);
                    self.shows(&Shown::Constant(format!("{for_type}::{}", constant.ident)), paths);
                }
                _ => {}
            }
        }
    }
}

/// Reads what a `pub use` exports. A path into an internal crate is one finding, whatever it ends in.
fn in_use(tree: &UseTree, used: &[&InternalCrate], found: &mut Vec<Named>) {
    let ident = match tree {
        UseTree::Path(path) => &path.ident,
        UseTree::Name(name) => &name.ident,
        UseTree::Rename(rename) => &rename.ident,
        UseTree::Group(group) => {
            for tree in &group.items {
                in_use(tree, used, found);
            }
            return;
        }
        UseTree::Glob(_) => return,
    };
    match (named(used, &ident.to_string(), ident.span().start().line), tree) {
        (Some(named), _) => found.push(named),
        (None, UseTree::Path(path)) => in_use(&path.tree, used, found),
        (None, _) => {}
    }
}

/// Whether other crates see an item: it is `pub`, and not `pub(crate)` or `pub(super)`.
fn is_public(visibility: &Visibility) -> bool {
    matches!(visibility, Visibility::Public(_))
}

/// The last name of the path that a type is written as: `SelectorName` for `crate::SelectorName<'a>`.
fn last_name(ty: &Type) -> Option<String> {
    match ty {
        Type::Path(ty) => ty.path.segments.last().map(|last| last.ident.to_string()),
        Type::Reference(reference) => last_name(&reference.elem),
        _ => None,
    }
}

/// Walks a signature, a type or a bound, and notes every path that names an internal crate.
struct Paths<'a> {
    used: &'a [&'a InternalCrate],
    found: Vec<Named>,
}

impl<'ast> Visit<'ast> for Paths<'_> {
    fn visit_path(&mut self, path: &'ast syn::Path) {
        // `gaslightmq_resource::ResourceName` is one finding, and not two.
        let first = path.segments.iter().find_map(|segment| {
            let line = segment.ident.span().start().line;
            named(self.used, &segment.ident.to_string(), line)
        });
        self.found.extend(first);
        // The types between angle brackets are paths of their own.
        visit::visit_path(self, path);
    }
}

/// Walks one file, and notes every doc comment that names an internal crate.
struct Docs<'a> {
    used: &'a [&'a InternalCrate],
    found: Vec<Named>,
}

impl Docs<'_> {
    /// Reads the text of a doc comment, which starts on `line`.
    fn text(&mut self, text: &str, line: usize) {
        for internal in self.used {
            for name in internal.names().filter(|name| holds_word(text, name)) {
                self.found.push(Named {
                    line,
                    name: name.to_owned(),
                    internal: internal.name.clone(),
                });
            }
        }
    }

    fn ident(&mut self, ident: &Ident) {
        self.found
            .extend(named(self.used, &ident.to_string(), ident.span().start().line));
    }

    /// Reads the tokens of one `#[doc]` attribute: its texts, and the names it puts together with a macro.
    fn doc(&mut self, tokens: TokenStream) {
        for tree in tokens {
            match tree {
                TokenTree::Group(group) => self.doc(group.stream()),
                TokenTree::Ident(ident) => self.ident(&ident),
                TokenTree::Literal(literal) => {
                    let line = literal.span().start().line;
                    match Lit::new(literal.clone()) {
                        Lit::Str(text) => self.text(&text.value(), line),
                        _ => self.text(&literal.to_string(), line),
                    }
                }
                TokenTree::Punct(_) => {}
            }
        }
    }

    /// Reads the tokens of a macro, which have no syntax tree, and finds the `#[doc]` attributes among them.
    /// A doc comment is such an attribute too.
    fn tokens(&mut self, tokens: TokenStream) {
        let trees: Vec<TokenTree> = tokens.into_iter().collect();
        let is_punct = |at: usize, character: char| matches!(trees.get(at), Some(TokenTree::Punct(punct)) if punct.as_char() == character);
        for (at, tree) in trees.iter().enumerate() {
            let TokenTree::Group(group) = tree else { continue };
            let after_a_hash =
                at >= 1 && is_punct(at - 1, '#') || at >= 2 && is_punct(at - 1, '!') && is_punct(at - 2, '#');
            let is_doc = group.delimiter() == Delimiter::Bracket
                && after_a_hash
                && matches!(group.stream().into_iter().next(), Some(TokenTree::Ident(first)) if first == DOC);
            if is_doc {
                self.doc(group.stream());
            } else {
                self.tokens(group.stream());
            }
        }
    }
}

impl<'ast> Visit<'ast> for Docs<'_> {
    fn visit_item(&mut self, item: &'ast Item) {
        if !source::is_test(source::attributes(item)) {
            visit::visit_item(self, item);
        }
    }

    fn visit_attribute(&mut self, attribute: &'ast Attribute) {
        if !attribute.path().is_ident(DOC) {
            return;
        }
        match &attribute.meta {
            Meta::NameValue(written) => match &written.value {
                Expr::Lit(literal) => {
                    if let Lit::Str(text) = &literal.lit {
                        self.text(&text.value(), text.span().start().line);
                    }
                }
                // `#[doc = concat!(...)]` puts its text together.
                Expr::Macro(call) => self.doc(call.mac.tokens.clone()),
                _ => {}
            },
            Meta::List(list) => self.doc(list.tokens.clone()),
            Meta::Path(_) => {}
        }
    }

    fn visit_macro(&mut self, call: &'ast Macro) {
        self.tokens(call.tokens.clone());
    }
}

/// Whether a text holds a name as a whole word: no letter, digit or `_` stands right before it or right after it.
fn holds_word(text: &str, name: &str) -> bool {
    let is_word = |character: char| character.is_alphanumeric() || character == '_';
    text.match_indices(name).any(|(at, _)| {
        let before = text[..at].chars().next_back();
        let after = text[at + name.len()..].chars().next();
        !before.is_some_and(is_word) && !after.is_some_and(is_word)
    })
}

/// What shows a type of an internal crate.
#[derive(Debug, Clone, PartialEq)]
pub enum Shown {
    /// The signature of a `pub` function, or of a `pub` method as `SelectorName::resource_name`.
    Function(String),
    /// A `pub` field of this struct, or a field of this variant of a `pub` enum.
    Field(String),
    /// The bounds of this `pub` struct, enum or union.
    Bounds(String),
    /// A `pub` type alias.
    Alias(String),
    /// A `pub` constant or static.
    Constant(String),
    /// A `pub` trait: its bounds, its methods or its associated types.
    Trait(String),
    /// An implementation of the trait `of` for the type `for_type`.
    Implementation { of: String, for_type: String },
    /// A `pub use`.
    ReExport,
    /// A doc comment or a `#[doc]` attribute.
    Doc,
}

impl fmt::Display for Shown {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Function(name) => write!(f, "the public function `{name}`"),
            Self::Field(holder) => write!(f, "a public field of `{holder}`"),
            Self::Bounds(name) => write!(f, "a bound of the public type `{name}`"),
            Self::Alias(name) => write!(f, "the public type alias `{name}`"),
            Self::Constant(name) => write!(f, "the public constant `{name}`"),
            Self::Trait(name) => write!(f, "the public trait `{name}`"),
            Self::Implementation { of, for_type } => write!(f, "an implementation of `{of}` for `{for_type}`"),
            Self::ReExport => f.write_str("a `pub use`"),
            Self::Doc => f.write_str("a doc comment"),
        }
    }
}

/// A place where a crate shows a type of an internal crate, or the crate itself.
#[derive(Debug, PartialEq)]
pub struct InternalCrateViolation {
    pub path: PathBuf,
    pub line: usize,
    pub shown_by: Shown,
    /// The name as written: `ResourceName`, or `gaslightmq_resource`.
    pub name: String,
    /// The internal crate that has the name: `gaslightmq_resource`.
    pub internal: String,
}

impl fmt::Display for InternalCrateViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            path,
            line,
            shown_by,
            name,
            internal,
        } = self;
        write!(f, "{}:{line}: {shown_by} names ", path.display())?;
        if name == internal {
            write!(f, "the internal crate `{internal}`")?;
        } else {
            write!(f, "`{name}` of the internal crate `{internal}`")?;
        }
        f.write_str("; only private code names an internal crate")
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    const RESOURCE: &str = "gaslightmq_resource";

    fn file(krate: &str, text: &str) -> SourceFile {
        SourceFile::parse(&Path::new("crates").join(krate), Path::new("name.rs"), text).unwrap()
    }

    /// The internal crate of the names of resources, as its source declares it.
    fn resource() -> InternalCrate {
        let source = "pub struct ResourceName(String);
                      pub enum ResourceNameError { Empty }";
        InternalCrate::read(RESOURCE, &[file(RESOURCE, source)])
    }

    /// The violations in one file of a crate that uses the internal crate, as the lines the check prints.
    fn lines(text: &str) -> Vec<String> {
        in_files(&[file("gaslightmq_selector", text)], &[&resource()])
            .iter()
            .map(ToString::to_string)
            .collect()
    }

    /// The line that the check prints for a name on `line` of the file.
    fn line(line: usize, shown_by: &str, name: &str) -> String {
        format!(
            "crates/gaslightmq_selector/src/name.rs:{line}: {shown_by} names `{name}` \
             of the internal crate `gaslightmq_resource`; only private code names an internal crate"
        )
    }

    #[test]
    fn an_internal_crate_exports_its_public_types() {
        let source = "pub struct ResourceName(String);
                      pub enum ResourceNameError { Empty }
                      pub trait Named {}
                      pub type Names = Vec<ResourceName>;
                      pub(crate) struct Rule;
                      struct Table;
                      pub fn check() {}
                      #[cfg(test)] pub struct Fixture;";

        let internal = InternalCrate::read(RESOURCE, &[file(RESOURCE, source)]);

        assert_eq!(
            internal.names().collect::<Vec<_>>(),
            [RESOURCE, "Named", "Names", "ResourceName", "ResourceNameError"]
        );
    }

    #[test]
    fn private_code_names_an_internal_crate() {
        let text = "\
use gaslightmq_resource::{ResourceName, ResourceNameError};
// A selector name is a ResourceName inside.
pub struct SelectorName(ResourceName);
pub struct Selector { name: ResourceName, pub(crate) other: ResourceName }
struct Kept { pub name: ResourceName }
enum Outcome { Broken(ResourceNameError) }
type Checked = Result<ResourceName, ResourceNameError>;
impl SelectorName {
    pub fn new(name: &str) -> Result<Self, SelectorNameError> {
        match ResourceName::new(name) {
            Ok(name) => Ok(Self(name)),
            Err(ResourceNameError::Empty) => Err(SelectorNameError::Empty),
        }
    }
    fn inner(&self) -> &ResourceName { &self.0 }
    pub(crate) fn into_inner(self) -> ResourceName { self.0 }
}
fn check(name: &str) -> Result<ResourceName, ResourceNameError> { ResourceName::new(name) }
pub(crate) fn checked(name: &str) -> Checked { check(name) }
";

        assert_eq!(lines(text), [""; 0]);
    }

    #[test]
    fn a_public_function_that_takes_or_returns_one_fails() {
        let text = "\
pub fn check(name: &str) -> Result<(), ResourceNameError> { Ok(()) }
pub fn named(name: impl Into<ResourceName>) {}
pub fn every<T>(names: &[T]) where T: AsRef<gaslightmq_resource::ResourceName> {}
impl SelectorName {
    pub fn resource_name(&self) -> &ResourceName { &self.0 }
}
";

        assert_eq!(
            lines(text),
            [
                line(1, "the public function `check`", "ResourceNameError"),
                line(2, "the public function `named`", "ResourceName"),
                "crates/gaslightmq_selector/src/name.rs:3: the public function `every` names \
                 the internal crate `gaslightmq_resource`; only private code names an internal crate"
                    .to_owned(),
                line(5, "the public function `SelectorName::resource_name`", "ResourceName"),
            ]
        );
    }

    #[test]
    fn a_public_field_of_one_fails() {
        let text = "\
pub struct SelectorName(pub ResourceName);
pub struct SelectorNameError {
    pub error: ResourceNameError,
}
pub union Either {
    pub name: std::mem::ManuallyDrop<ResourceName>,
}
";

        assert_eq!(
            lines(text),
            [
                line(1, "a public field of `SelectorName`", "ResourceName"),
                line(3, "a public field of `SelectorNameError`", "ResourceNameError"),
                line(6, "a public field of `Either`", "ResourceName"),
            ]
        );
    }

    #[test]
    fn a_field_of_a_variant_of_a_public_enum_fails() {
        let text = "\
pub enum SelectorNameError {
    Empty,
    Invalid { error: ResourceNameError },
    Broken(Box<ResourceNameError>),
}
";

        assert_eq!(
            lines(text),
            [
                line(3, "a public field of `SelectorNameError::Invalid`", "ResourceNameError"),
                line(4, "a public field of `SelectorNameError::Broken`", "ResourceNameError"),
            ]
        );
    }

    #[test]
    fn a_public_alias_a_public_constant_and_the_bounds_of_a_public_type_fail() {
        let text = "\
pub type SelectorName = ResourceName;
pub const DEFAULT: Option<ResourceName> = None;
pub static LAST: Option<ResourceName> = None;
pub struct Named<T: Borrow<ResourceName>>(T);
pub enum Either<T> where T: Into<ResourceName> { One(T) }
impl SelectorName {
    pub const NONE: Option<ResourceName> = None;
}
";

        assert_eq!(
            lines(text),
            [
                line(1, "the public type alias `SelectorName`", "ResourceName"),
                line(2, "the public constant `DEFAULT`", "ResourceName"),
                line(3, "the public constant `LAST`", "ResourceName"),
                line(4, "a bound of the public type `Named`", "ResourceName"),
                line(5, "a bound of the public type `Either`", "ResourceName"),
                line(7, "the public constant `SelectorName::NONE`", "ResourceName"),
            ]
        );
    }

    #[test]
    fn a_public_trait_that_names_one_fails() {
        let text = "\
pub trait Named: AsRef<ResourceName> {
    type Name: Into<ResourceName>;
    const DEFAULT: Option<ResourceName>;
    fn name(&self) -> &ResourceName;
}
trait Kept {
    fn name(&self) -> &ResourceName;
}
";

        assert_eq!(
            lines(text),
            [
                line(1, "the public trait `Named`", "ResourceName"),
                line(2, "the public trait `Named`", "ResourceName"),
                line(3, "the public trait `Named`", "ResourceName"),
                line(4, "the public trait `Named`", "ResourceName"),
            ]
        );
    }

    #[test]
    fn an_implementation_that_hands_one_out_fails() {
        let text = "\
impl From<ResourceName> for SelectorName {
    fn from(name: ResourceName) -> Self { Self(name) }
}
impl From<SelectorName> for ResourceName {
    fn from(name: SelectorName) -> Self { name.0 }
}
impl AsRef<ResourceName> for SelectorName {
    fn as_ref(&self) -> &ResourceName { &self.0 }
}
impl Borrow<ResourceName> for SelectorName {
    fn borrow(&self) -> &ResourceName { &self.0 }
}
impl Deref for SelectorName {
    type Target = ResourceName;
    fn deref(&self) -> &Self::Target { &self.0 }
}
impl FromStr for SelectorName {
    type Err = ResourceNameError;
    fn from_str(text: &str) -> Result<Self, Self::Err> { ResourceName::new(text).map(Self) }
}
";
        let from = "an implementation of `From` for `SelectorName`";
        let as_ref = "an implementation of `AsRef` for `SelectorName`";
        let borrow = "an implementation of `Borrow` for `SelectorName`";

        assert_eq!(
            lines(text),
            [
                line(1, from, "ResourceName"),
                line(2, from, "ResourceName"),
                line(4, "an implementation of `From` for `ResourceName`", "ResourceName"),
                line(7, as_ref, "ResourceName"),
                line(8, as_ref, "ResourceName"),
                line(10, borrow, "ResourceName"),
                line(11, borrow, "ResourceName"),
                line(14, "an implementation of `Deref` for `SelectorName`", "ResourceName"),
                line(
                    18,
                    "an implementation of `FromStr` for `SelectorName`",
                    "ResourceNameError"
                ),
            ]
        );
    }

    #[test]
    fn an_implementation_that_names_none_in_what_it_offers_passes() {
        let text = "\
impl FromStr for SelectorName {
    type Err = SelectorNameError;
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        ResourceName::new(text).map(Self).map_err(|_: ResourceNameError| SelectorNameError::Empty)
    }
}
impl AsRef<str> for SelectorName {
    fn as_ref(&self) -> &str { self.0.as_str() }
}
";

        assert_eq!(lines(text), [""; 0]);
    }

    #[test]
    fn a_re_export_fails_also_in_a_prelude() {
        let text = "\
pub use gaslightmq_resource::{ResourceName, ResourceNameError};
pub use gaslightmq_resource::ResourceName as Name;
pub use gaslightmq_resource::*;
pub use crate::name::{SelectorName, ResourceNameError as Broken};
pub extern crate gaslightmq_resource;
pub mod prelude {
    pub use crate::{ResourceName, SelectorName};
}
pub(crate) use gaslightmq_resource::ResourceName as Kept;
";
        let the_crate = |line: usize| {
            format!(
                "crates/gaslightmq_selector/src/name.rs:{line}: a `pub use` names \
                 the internal crate `gaslightmq_resource`; only private code names an internal crate"
            )
        };

        assert_eq!(
            lines(text),
            [
                the_crate(1),
                the_crate(2),
                the_crate(3),
                line(4, "a `pub use`", "ResourceNameError"),
                the_crate(5),
                line(7, "a `pub use`", "ResourceName"),
            ]
        );
    }

    #[test]
    fn a_doc_comment_that_names_one_fails() {
        let text = "\
//! | `SelectorName` | The text breaks [the rule](gaslightmq_resource::ResourceName#the-rule-for-names). |

/// The name of a selector, which is a [`ResourceName`].
///
/// ```
/// let error: ResourceNameError = SelectorName::new(\"\").unwrap_err().error;
/// ```
pub struct SelectorName(ResourceName);

impl SelectorName {
    /// Checks the rule of a `ResourceName`.
    fn check(&self) {}
}

#[doc = \"Why a text is no ResourceName.\"]
pub enum SelectorNameError {
    /// A [`ResourceNameError`] says the same.
    Empty,
}
";

        assert_eq!(
            lines(text),
            [
                "crates/gaslightmq_selector/src/name.rs:1: a doc comment names \
                 the internal crate `gaslightmq_resource`; only private code names an internal crate"
                    .to_owned(),
                line(1, "a doc comment", "ResourceName"),
                line(3, "a doc comment", "ResourceName"),
                line(6, "a doc comment", "ResourceNameError"),
                line(11, "a doc comment", "ResourceName"),
                line(15, "a doc comment", "ResourceName"),
                line(17, "a doc comment", "ResourceNameError"),
            ]
        );
    }

    #[test]
    fn a_doc_comment_in_a_macro_that_names_one_fails() {
        let text = "\
macro_rules! name {
    ($name:ident) => {
        /// Checks the rule of a [`ResourceName`].
        #[doc = concat!(\"A \", stringify!(ResourceNameError), \" says why.\")]
        pub struct $name(ResourceName);
    };
}

name! {
    /// The name of a rule, which is a [`ResourceName`].
    RuleName
}
";

        assert_eq!(
            lines(text),
            [
                line(3, "a doc comment", "ResourceName"),
                line(4, "a doc comment", "ResourceNameError"),
                line(10, "a doc comment", "ResourceName"),
            ]
        );
    }

    #[test]
    fn a_name_inside_another_word_is_not_that_name() {
        let text = "\
/// A `SelectorResourceName` and the `ResourceNames` of a `gaslightmq_resources` are other words.
pub fn check(name: &SelectorResourceName) {}
";

        assert_eq!(lines(text), [""; 0]);
    }

    #[test]
    fn test_code_names_an_internal_crate() {
        let text = "\
#[cfg(test)]
mod tests {
    /// A [`ResourceName`] for a test.
    pub fn name() -> ResourceName { ResourceName::new(\"x\").unwrap() }
}
";

        assert_eq!(lines(text), [""; 0]);
        assert_eq!(
            lines("#![cfg(test)]\n/// A [`ResourceName`].\npub struct Fixture(pub ResourceName);"),
            [""; 0]
        );
    }

    #[test]
    fn only_a_crate_that_builds_with_an_internal_crate_is_held_to_the_rule() {
        let internal = [resource()];
        let uses = Member {
            dependencies: BTreeSet::from([RESOURCE.to_owned()]),
            ..Member::alone("gaslightmq_selector")
        };
        let tests_with = Member {
            dev_dependencies: BTreeSet::from([RESOURCE.to_owned()]),
            ..Member::alone("gaslightmq_environment")
        };

        assert_eq!(used_by(&uses, &internal), [&internal[0]]);
        assert_eq!(used_by(&tests_with, &internal), [&internal[0]; 0]);
        assert_eq!(
            used_by(&Member::alone("gaslightmq_kernel"), &internal),
            [&internal[0]; 0]
        );
    }
}
