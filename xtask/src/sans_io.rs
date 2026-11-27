//! The check that sans-IO code stays sans-IO.
//!
//! A crate whose row in `crates.md` does not say "This crate does IO" is sans-IO as a whole.
//! A crate that does IO declares the modules it keeps sans-IO in its manifest:
//!
//! ```toml
//! [package.metadata.gaslightmq]
//! sans_io_modules = ["logic"]
//! ```
//!
//! `logic` is then `src/logic.rs` or `src/logic/mod.rs`, with every file below `src/logic/`.
//!
//! The check reads the dependencies of a sans-IO crate, and the source of all sans-IO code.
//!
//! - A sans-IO crate depends on no crate that does IO, and on no package that is async or does IO.
//!   [`IO_PACKAGES`] lists the packages the check knows.
//!   A crate that the tables plan, and the workspace does not hold yet, is held to the crates its row lists.
//! - Sans-IO code has no `async fn` with a body, no `async` block or closure, and no `.await`.
//!   A trait may declare an async method: an `async fn` without a body is a signature, and it runs nothing.
//!   [`AWAITS_STORES`] lists the code that awaits a store of definitions, and what it may have.
//!   A method of a trait whose whole body is one `async` block is an `async fn` with a body, written out
//!   so that its signature can say that its future is `Send`. The check reads it as one.
//! - Sans-IO code names nothing that does IO: no path into a package of [`IO_PACKAGES`], no `Future`,
//!   and none of the paths and names of the standard library in [`STANDARD_LIBRARY`], [`NAMES`] and [`PATHS`],
//!   which stand for sockets, files, the system clock, the process, threads and global randomness.
//!   One place may name `Future`: the return type of a method of a trait, where the trait declares the method
//!   and where a type implements it. That is how a trait declares an async method whose future is `Send`.
//!   Outside [`AWAITS_STORES`], such a method gets its future from the code it calls,
//!   because nothing else makes one.
//! - A sans-IO module names nothing of its own crate outside the sans-IO modules.
//!   A path that starts with `crate`, `self` or `super` ends inside one of them.
//!
//! What the check does not catch:
//!
//! - A method call. `path.exists()` and `instant.elapsed()` do IO, and no path names them.
//! - What a name stands for. The check reads names and does not resolve them:
//!   a renamed import hides a name, and a function of a package outside [`IO_PACKAGES`] passes whatever it does.
//! - A sans-IO module that names another crate that does IO.
//!   The check cannot tell that crate's plain types from the rest.
//! - Code that a macro writes, test code, and the dev-dependencies. See [`source`].
//! - Randomness in the order of a `HashMap`.

use std::fmt;
use std::path::PathBuf;

use proc_macro2::{TokenStream, TokenTree};
use syn::visit::{self, Visit};
use syn::{
    Block, Expr, ExprAsync, ExprAwait, ExprClosure, ImplItemFn, Item, ItemFn, ItemImpl, ItemMod, ItemUse, Macro,
    Signature, Stmt, TraitItemFn, UseTree, Visibility,
};

use crate::crate_layout::{self, CrateLayout, DOES_IO};
use crate::source::{self, SourceError, SourceFile};
use crate::workspace::{Member, Workspace};

/// The packages that are async or do IO, each with its family:
/// `tokio` stands for `tokio-util` and `tokio-stream` too, and `async` for `async-std` and `async-io`.
const IO_PACKAGES: [&str; 14] = [
    "async",
    "axum",
    "futures",
    "getrandom",
    "hyper",
    "mio",
    "notify",
    "reqwest",
    "smol",
    "socket2",
    "tokio",
    "tonic",
    "tungstenite",
    "ureq",
];

/// The crates that hold the standard library.
const STANDARD_LIBRARY_CRATES: [&str; 3] = ["std", "core", "alloc"];

/// The paths of the standard library that do IO, without the crate they start with.
/// `std::io` is not among them: its traits read and write whatever the caller hands over.
/// `std::net` is not either: an address is a plain value, and [`NAMES`] holds the sockets.
const STANDARD_LIBRARY: [(&[&str], Reach); 13] = [
    (&["env"], Reach::Process),
    (&["fs"], Reach::FileSystem),
    (&["future"], Reach::Async),
    (&["io", "stderr"], Reach::Process),
    (&["io", "stdin"], Reach::Process),
    (&["io", "stdout"], Reach::Process),
    (&["os"], Reach::Process),
    (&["process"], Reach::Process),
    (&["task"], Reach::Async),
    (&["thread"], Reach::Process),
    (&["time", "Instant"], Reach::SystemClock),
    (&["time", "SystemTime"], Reach::SystemClock),
    (&["time", "UNIX_EPOCH"], Reach::SystemClock),
];

/// The names that do IO wherever they appear in a path, whichever package they come from.
const NAMES: [(&str, Reach); 12] = [
    ("Future", Reach::Future),
    ("OsRng", Reach::Randomness),
    ("SystemTime", Reach::SystemClock),
    ("TcpListener", Reach::Socket),
    ("TcpStream", Reach::Socket),
    ("ThreadRng", Reach::Randomness),
    ("ToSocketAddrs", Reach::Socket),
    ("UdpSocket", Reach::Socket),
    ("UnixDatagram", Reach::Socket),
    ("UnixListener", Reach::Socket),
    ("UnixStream", Reach::Socket),
    ("thread_rng", Reach::Randomness),
];

/// The pairs of names that do IO wherever they follow each other in a path.
/// `Instant` alone is not among [`NAMES`], because the `Instant` of `gaslightmq_clock` comes from no system.
const PATHS: [(&[&str], Reach); 6] = [
    (&["Instant", "now"], Reach::SystemClock),
    (&["Local", "now"], Reach::SystemClock),
    (&["OffsetDateTime", "now_utc"], Reach::SystemClock),
    (&["Utc", "now"], Reach::SystemClock),
    (&["rand", "random"], Reach::Randomness),
    (&["rand", "rng"], Reach::Randomness),
];

/// The sans-IO code that awaits a store of definitions, which `crates.md` describes under "The store seam":
/// it keeps the store contract, or it calls it.
/// Each crate stands with the modules of it that await, by the first name of their path.
/// A crate without a module here awaits a store wherever it likes.
///
/// Such code may have an `async fn` with a body and an `.await`. Any other async code fails there too:
/// it makes no other future, and it names no task and no runtime, so it cannot start anything,
/// wait for a timer or block.
///
/// The crate of a definition awaits in one module: `store` holds its store trait, whose methods with a body
/// read what the store lists. Everything else in such a crate is held to the rule of every sans-IO crate.
/// The check of that trait is a crate of its own, in its directory.
/// The contract has a module `store` too: its trait answers for a store that nobody else changes,
/// in the methods with a body that such a store keeps as they are.
const AWAITS_STORES: [(&str, &[&str]); 14] = [
    ("gaslightmq_definition_api", &["store"]),
    ("gaslightmq_definition_conformance", &[]),
    ("gaslightmq_definition_memory", &[]),
    ("gaslightmq_environment_conformance", &[]),
    ("gaslightmq_recording_conformance", &[]),
    ("gaslightmq_rule", &["store"]),
    ("gaslightmq_rule_conformance", &[]),
    ("gaslightmq_scenario", &["store"]),
    ("gaslightmq_scenario_conformance", &[]),
    ("gaslightmq_selector", &["store"]),
    ("gaslightmq_selector_conformance", &[]),
    ("gaslightmq_surface", &["store"]),
    ("gaslightmq_surface_conformance", &[]),
    ("gaslightmq_workspace", &[]),
];

/// Whether a file of a sans-IO crate is code that awaits a store of definitions.
fn awaits_stores(name: &str, file: &SourceFile) -> bool {
    AWAITS_STORES.iter().any(|(awaiting, modules)| {
        let in_a_module = file
            .module
            .first()
            .is_some_and(|first| modules.contains(&first.as_str()));
        *awaiting == name && (modules.is_empty() || in_a_module)
    })
}

/// Whether a package is async or does IO, by its name in a manifest or in a path.
fn is_io_package(name: &str) -> bool {
    let name = name.replace('-', "_");
    IO_PACKAGES.iter().any(|family| {
        name.strip_prefix(family)
            .is_some_and(|rest| rest.is_empty() || rest.starts_with('_'))
    })
}

/// What a path reaches that sans-IO code stays away from, whichever crate the code is in.
fn reach_of(segments: &[&str]) -> Option<Reach> {
    if let [first, rest @ ..] = segments
        && !rest.is_empty()
    {
        if is_io_package(first) {
            return Some(Reach::Package);
        }
        if STANDARD_LIBRARY_CRATES.contains(first) {
            let known = STANDARD_LIBRARY.iter().find(|(path, _)| rest.starts_with(path));
            if let Some(&(_, reach)) = known {
                return Some(reach);
            }
        }
    }
    let in_a_row = PATHS
        .iter()
        .find(|(path, _)| segments.windows(path.len()).any(|window| window == *path));
    if let Some(&(_, reach)) = in_a_row {
        return Some(reach);
    }
    NAMES
        .iter()
        .find(|(name, _)| segments.contains(name))
        .map(|&(_, reach)| reach)
}

/// Everything in which the sans-IO code of the workspace is not sans-IO.
pub fn violations(layout: &CrateLayout, workspace: &Workspace) -> Result<Vec<SansIoViolation>, SourceError> {
    let mut violations = Vec::new();
    for member in workspace.members.iter().filter(|member| !member.is_tooling()) {
        // A crate without a row fails the check of the dependencies, and this check waits for the row.
        let Some(does_io) = layout.does_io(&member.name) else {
            continue;
        };
        if !does_io {
            violations.extend(dependency_violations(member, layout));
        }
        if !does_io || !member.sans_io_modules.is_empty() {
            let files = source::of(&workspace.root, member)?;
            violations.extend(source_violations(member, does_io, &files));
        }
    }
    violations.extend(planned_violations(layout, workspace));
    Ok(violations)
}

/// The crates that the tables plan as sans-IO, and whose rows list a crate that does IO.
/// The workspace does not hold them yet, so their rows are all the check can read.
fn planned_violations(layout: &CrateLayout, workspace: &Workspace) -> Vec<SansIoViolation> {
    let mut violations = Vec::new();
    for (name, listed) in layout.rows() {
        let exists = workspace.members.iter().any(|member| member.name == name);
        if exists || layout.does_io(name) != Some(false) {
            continue;
        }
        let doing_io = listed.filter(|dependency| layout.does_io(dependency) == Some(true));
        violations.extend(doing_io.map(|dependency| SansIoViolation::DependsOnIoCrate {
            name: name.to_owned(),
            dependency: dependency.to_owned(),
        }));
    }
    violations
}

/// The dependencies of a sans-IO crate that are async or do IO.
fn dependency_violations(member: &Member, layout: &CrateLayout) -> Vec<SansIoViolation> {
    let crates = member
        .dependencies
        .iter()
        .filter(|dependency| layout.does_io(dependency) == Some(true))
        .map(|dependency| SansIoViolation::DependsOnIoCrate {
            name: member.name.clone(),
            dependency: dependency.clone(),
        });
    let packages = member
        .outside_dependencies
        .iter()
        .filter(|package| is_io_package(package))
        .map(|package| SansIoViolation::DependsOnIoPackage {
            name: member.name.clone(),
            package: package.clone(),
        });
    crates.chain(packages).collect()
}

/// What the sans-IO source of one crate does that sans-IO code does not.
/// `does_io` says whether the crate does IO, so that only the modules it declares are sans-IO.
fn source_violations(member: &Member, does_io: bool, files: &[SourceFile]) -> Vec<SansIoViolation> {
    let name = &member.name;
    if !does_io {
        let mut violations = Vec::new();
        if !member.sans_io_modules.is_empty() {
            violations.push(SansIoViolation::DeclaresModules { name: name.clone() });
        }
        let code = Code::Crate { name: name.clone() };
        for file in files {
            let awaits = awaits_stores(name, file);
            let found = read(file, None).into_iter().filter(|(_, found)| match found {
                Found::AsyncFunction | Found::Await => !awaits,
                Found::AsyncBlock | Found::Names { .. } => true,
            });
            violations.extend(found.map(|(line, found)| in_source(file, line, &code, found)));
        }
        return violations;
    }

    let mut violations = Vec::new();
    let mut modules = Vec::new();
    for declared in &member.sans_io_modules {
        let module: Vec<String> = declared.split("::").map(str::to_owned).collect();
        if files.iter().any(|file| file.module == module) {
            modules.push(module);
        } else {
            violations.push(SansIoViolation::NoSuchModule {
                name: name.clone(),
                module: declared.clone(),
            });
        }
    }
    for file in files {
        let Some(module) = modules.iter().find(|module| file.module.starts_with(module)) else {
            continue;
        };
        let code = Code::Module {
            name: name.clone(),
            module: module.join("::"),
        };
        let found = read(file, Some(&modules));
        violations.extend(
            found
                .into_iter()
                .map(|(line, found)| in_source(file, line, &code, found)),
        );
    }
    violations
}

fn in_source(file: &SourceFile, line: usize, code: &Code, found: Found) -> SansIoViolation {
    SansIoViolation::InSource {
        path: file.path.clone(),
        line,
        code: code.clone(),
        found,
    }
}

/// What one file holds that sans-IO code does not, each with its line.
/// `sans_io_modules` are the sans-IO modules of a crate that does IO, or `None` for a crate that is sans-IO as a whole.
fn read(file: &SourceFile, sans_io_modules: Option<&[Vec<String>]>) -> Vec<(usize, Found)> {
    if file.is_test() {
        return Vec::new();
    }
    let mut reader = Reader {
        sans_io_modules,
        module: file.module.clone(),
        implements_a_trait: false,
        reads_a_trait_method: false,
        returns_of_a_trait_method: false,
        found: Vec::new(),
    };
    reader.visit_file(&file.syntax);
    reader.found.sort_by_key(|(line, _)| *line);
    reader.found.dedup();
    reader.found
}

/// Walks one file, and notes what it finds.
struct Reader<'a> {
    sans_io_modules: Option<&'a [Vec<String>]>,
    /// The module that the walk is in.
    module: Vec<String>,
    /// Whether the walk is in an `impl` of a trait, whose methods are the ones that the trait declares.
    implements_a_trait: bool,
    /// Whether the signature that the walk reads next is that of a method of a trait.
    reads_a_trait_method: bool,
    /// Whether the walk is in the return type of a method of a trait, which may name `Future`.
    returns_of_a_trait_method: bool,
    found: Vec<(usize, Found)>,
}

impl Reader<'_> {
    /// Notes a path when it reaches what sans-IO code stays away from.
    fn names(&mut self, segments: &[String], line: usize) {
        let borrowed: Vec<&str> = segments.iter().map(String::as_str).collect();
        if let Some(reach) = reach_of(&borrowed).or_else(|| self.leaves_the_sans_io_modules(&borrowed)) {
            if reach == Reach::Future && self.returns_of_a_trait_method {
                return;
            }
            let path = segments.join("::");
            self.found.push((line, Found::Names { path, reach }));
        }
    }

    /// Whether a path into the crate itself ends outside the sans-IO modules.
    fn leaves_the_sans_io_modules(&self, segments: &[&str]) -> Option<Reach> {
        let sans_io_modules = self.sans_io_modules?;
        // One name alone is `self` the value, or the `crate` of `pub(crate)`.
        let [first, rest @ ..] = segments else { return None };
        if rest.is_empty() {
            return None;
        }
        let mut target: Vec<&str> = match *first {
            "crate" => Vec::new(),
            "self" | "super" => self.module.iter().map(String::as_str).collect(),
            _ => return None,
        };
        let mut rest = rest;
        if *first == "super" {
            target.pop()?;
            while let ["super", after @ ..] = rest {
                target.pop()?;
                rest = after;
            }
        }
        target.extend(rest);
        let inside = sans_io_modules.iter().any(|module| {
            target.len() >= module.len() && module.iter().zip(&target).all(|(part, segment)| part == segment)
        });
        (!inside).then_some(Reach::RestOfTheCrate)
    }

    /// Reads the body of a method of a trait.
    ///
    /// A body that is one `async` block is the body of an `async fn`, written out so that the signature
    /// can say that the future is `Send`. It is an async function with a body, and no future made on the side.
    fn body_of_a_trait_method(&mut self, body: &Block) {
        if let [Stmt::Expr(Expr::Async(written_out), None)] = body.stmts.as_slice() {
            let line = written_out.async_token.span.start().line;
            self.found.push((line, Found::AsyncFunction));
            self.visit_block(&written_out.block);
        } else {
            self.visit_block(body);
        }
    }

    /// Reads the tokens of a macro call, which have no syntax tree.
    fn tokens(&mut self, tokens: TokenStream) {
        let trees: Vec<TokenTree> = tokens.into_iter().collect();
        let is_colon = |at: usize| matches!(trees.get(at), Some(TokenTree::Punct(punct)) if punct.as_char() == ':');
        let mut at = 0;
        while let Some(tree) = trees.get(at) {
            at += 1;
            match tree {
                TokenTree::Group(group) => self.tokens(group.stream()),
                TokenTree::Ident(ident) => {
                    let line = ident.span().start().line;
                    if ident == "async" {
                        self.found.push((line, Found::AsyncBlock));
                        continue;
                    }
                    let after_a_dot =
                        at >= 2 && matches!(&trees[at - 2], TokenTree::Punct(punct) if punct.as_char() == '.');
                    if ident == "await" && after_a_dot {
                        self.found.push((line, Found::Await));
                        continue;
                    }
                    let mut segments = vec![ident.to_string()];
                    while is_colon(at)
                        && is_colon(at + 1)
                        && let Some(TokenTree::Ident(next)) = trees.get(at + 2)
                    {
                        segments.push(next.to_string());
                        at += 3;
                    }
                    self.names(&segments, line);
                }
                TokenTree::Punct(_) | TokenTree::Literal(_) => {}
            }
        }
    }
}

impl<'ast> Visit<'ast> for Reader<'_> {
    fn visit_item(&mut self, item: &'ast Item) {
        if !source::is_test(source::attributes(item)) {
            visit::visit_item(self, item);
        }
    }

    fn visit_item_mod(&mut self, module: &'ast ItemMod) {
        self.module.push(module.ident.to_string());
        visit::visit_item_mod(self, module);
        self.module.pop();
    }

    fn visit_item_fn(&mut self, function: &'ast ItemFn) {
        if let Some(asyncness) = &function.sig.asyncness {
            self.found.push((asyncness.span.start().line, Found::AsyncFunction));
        }
        visit::visit_item_fn(self, function);
    }

    fn visit_item_impl(&mut self, implementation: &'ast ItemImpl) {
        let around = std::mem::replace(&mut self.implements_a_trait, implementation.trait_.is_some());
        visit::visit_item_impl(self, implementation);
        self.implements_a_trait = around;
    }

    fn visit_impl_item_fn(&mut self, function: &'ast ImplItemFn) {
        if source::is_test(&function.attrs) {
            return;
        }
        if let Some(asyncness) = &function.sig.asyncness {
            self.found.push((asyncness.span.start().line, Found::AsyncFunction));
        }
        for attribute in &function.attrs {
            self.visit_attribute(attribute);
        }
        self.reads_a_trait_method = self.implements_a_trait;
        self.visit_signature(&function.sig);
        if self.implements_a_trait {
            self.body_of_a_trait_method(&function.block);
        } else {
            self.visit_block(&function.block);
        }
    }

    /// An `async fn` without a body declares a method and runs nothing, so only one with a body is found.
    fn visit_trait_item_fn(&mut self, function: &'ast TraitItemFn) {
        if let (Some(asyncness), Some(_)) = (&function.sig.asyncness, &function.default) {
            self.found.push((asyncness.span.start().line, Found::AsyncFunction));
        }
        for attribute in &function.attrs {
            self.visit_attribute(attribute);
        }
        self.reads_a_trait_method = true;
        self.visit_signature(&function.sig);
        if let Some(body) = &function.default {
            self.body_of_a_trait_method(body);
        }
    }

    /// Reads a signature, and lets the return type of a method of a trait name `Future`.
    fn visit_signature(&mut self, signature: &'ast Signature) {
        let of_a_trait_method = std::mem::take(&mut self.reads_a_trait_method);
        self.visit_generics(&signature.generics);
        for input in &signature.inputs {
            self.visit_fn_arg(input);
        }
        self.returns_of_a_trait_method = of_a_trait_method;
        self.visit_return_type(&signature.output);
        self.returns_of_a_trait_method = false;
    }

    fn visit_expr_async(&mut self, block: &'ast ExprAsync) {
        self.found
            .push((block.async_token.span.start().line, Found::AsyncBlock));
        visit::visit_expr_async(self, block);
    }

    fn visit_expr_closure(&mut self, closure: &'ast ExprClosure) {
        if let Some(asyncness) = &closure.asyncness {
            self.found.push((asyncness.span.start().line, Found::AsyncBlock));
        }
        visit::visit_expr_closure(self, closure);
    }

    fn visit_expr_await(&mut self, awaited: &'ast ExprAwait) {
        self.found.push((awaited.await_token.span.start().line, Found::Await));
        visit::visit_expr_await(self, awaited);
    }

    fn visit_path(&mut self, path: &'ast syn::Path) {
        if let Some(first) = path.segments.first() {
            let segments: Vec<String> = path.segments.iter().map(|segment| segment.ident.to_string()).collect();
            self.names(&segments, first.ident.span().start().line);
        }
        // The types between angle brackets are paths of their own.
        visit::visit_path(self, path);
    }

    fn visit_item_use(&mut self, item: &'ast ItemUse) {
        let mut used = Vec::new();
        flatten(&item.tree, &mut Vec::new(), &mut used);
        for (segments, line) in used {
            self.names(&segments, line);
        }
    }

    /// `pub(crate)` and `pub(in crate::logic)` say who sees an item, and reach nothing.
    fn visit_visibility(&mut self, _: &'ast Visibility) {}

    fn visit_macro(&mut self, call: &'ast Macro) {
        self.visit_path(&call.path);
        self.tokens(call.tokens.clone());
    }
}

/// Every path that a `use` item imports, with the line of its last name.
/// A glob import ends in `*`, and `self` in braces stands for the path before the braces.
fn flatten(tree: &UseTree, prefix: &mut Vec<String>, used: &mut Vec<(Vec<String>, usize)>) {
    let mut import = |last: Option<String>, line: usize| {
        let mut segments = prefix.clone();
        segments.extend(last);
        used.push((segments, line));
    };
    match tree {
        UseTree::Path(path) => {
            prefix.push(path.ident.to_string());
            flatten(&path.tree, prefix, used);
            prefix.pop();
        }
        UseTree::Name(name) => {
            let last = (name.ident != "self" || prefix.is_empty()).then(|| name.ident.to_string());
            import(last, name.ident.span().start().line);
        }
        UseTree::Rename(rename) => {
            let last = (rename.ident != "self" || prefix.is_empty()).then(|| rename.ident.to_string());
            import(last, rename.ident.span().start().line);
        }
        UseTree::Glob(glob) => import(Some("*".to_owned()), glob.star_token.spans[0].start().line),
        UseTree::Group(group) => {
            for tree in &group.items {
                flatten(tree, prefix, used);
            }
        }
    }
}

/// The code that is sans-IO: a whole crate, or one module of a crate that does IO.
#[derive(Debug, Clone, PartialEq)]
pub enum Code {
    Crate { name: String },
    Module { name: String, module: String },
}

impl fmt::Display for Code {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Crate { name } => write!(f, "`{name}`"),
            Self::Module { name, module } => write!(f, "the module `{module}` of `{name}`"),
        }
    }
}

/// What sans-IO code holds, and should not.
#[derive(Debug, Clone, PartialEq)]
pub enum Found {
    /// An `async fn` with a body, also one that a method of a trait writes out as one `async` block.
    AsyncFunction,
    /// An `async` block or closure.
    AsyncBlock,
    /// An `.await`.
    Await,
    /// A path, as the code writes it, that reaches what sans-IO code stays away from.
    Names { path: String, reach: Reach },
}

impl fmt::Display for Found {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AsyncFunction => f.write_str("has an `async fn` with a body"),
            Self::AsyncBlock => f.write_str("has an `async` block or closure"),
            Self::Await => f.write_str("has an `.await`"),
            Self::Names { path, reach } => write!(f, "names `{path}`, which {reach}"),
        }
    }
}

/// What a path reaches.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Reach {
    /// The machinery of async code, other than `Future`.
    Async,
    /// `Future`, outside the return type of a method of a trait.
    Future,
    /// A package of [`IO_PACKAGES`].
    Package,
    Socket,
    FileSystem,
    SystemClock,
    /// The process around the code: its threads, its environment, its standard streams, and its operating system.
    Process,
    Randomness,
    /// The crate's own code outside its sans-IO modules.
    RestOfTheCrate,
}

impl fmt::Display for Reach {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Async | Self::Future => "is async code",
            Self::Package => "belongs to a package that is async or does IO",
            Self::Socket => "is a socket",
            Self::FileSystem => "reaches the file system",
            Self::SystemClock => "reads the system clock",
            Self::Process => "reaches the process, its threads or its operating system",
            Self::Randomness => "is global randomness",
            Self::RestOfTheCrate => "lies outside the sans-IO modules of the crate",
        })
    }
}

/// A way in which sans-IO code is not sans-IO.
#[derive(Debug, PartialEq)]
pub enum SansIoViolation {
    /// A sans-IO crate depends on a crate whose row says that it does IO.
    DependsOnIoCrate { name: String, dependency: String },
    /// A sans-IO crate depends on a package that is async or does IO.
    DependsOnIoPackage { name: String, package: String },
    /// A sans-IO crate declares sans-IO modules, which only a crate that does IO has a use for.
    DeclaresModules { name: String },
    /// A crate declares a sans-IO module that it does not hold.
    NoSuchModule { name: String, module: String },
    /// Sans-IO code is async, or it names something that does IO.
    InSource {
        path: PathBuf,
        line: usize,
        code: Code,
        found: Found,
    },
}

impl fmt::Display for SansIoViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let tables = crate_layout::PATH;
        match self {
            Self::DependsOnIoCrate { name, dependency } => write!(
                f,
                "`{name}` is sans-IO, and depends on `{dependency}`, which does IO; \
                 the row of a crate that does IO says \"{DOES_IO}\" in {tables}"
            ),
            Self::DependsOnIoPackage { name, package } => write!(
                f,
                "`{name}` is sans-IO, and depends on `{package}`, a package that is async or does IO; \
                 the row of a crate that does IO says \"{DOES_IO}\" in {tables}"
            ),
            Self::DeclaresModules { name } => write!(
                f,
                "`{name}` declares `sans_io_modules`, and it is sans-IO as a whole: \
                 its row in {tables} does not say \"{DOES_IO}\""
            ),
            Self::NoSuchModule { name, module } => write!(
                f,
                "`{name}` declares the sans-IO module `{module}`, and has no file for it in `src/`"
            ),
            Self::InSource {
                path,
                line,
                code,
                found,
            } => write!(f, "{}:{line}: {code} is sans-IO, and {found}", path.display()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    const TABLES: &str = "\
| Crate | Holds | Depends on |
|---|---|---|
| `trigger` | Triggers. | `clock` |
| `clock` | The clock. | — |
| `backend` | The one async trait. | — |
| `listener` | Kafka on a socket. This crate does IO. | `clock` |
| `kafka_client_native` | Our own Kafka client. This crate does IO. | `clock` |
| `test` | The test library. | `listener`, `clock` |
| `host` | One process. This crate does IO. | `listener` |
| `gaslightmq` | The facade crate. | The public crates |
";

    fn sans_io_crate() -> Member {
        Member::alone("gaslightmq_trigger")
    }

    /// A crate that does IO, and declares its module `logic` sans-IO.
    fn io_crate() -> Member {
        Member {
            sans_io_modules: vec!["logic".to_owned()],
            ..Member::alone("gaslightmq_kafka_client_native")
        }
    }

    fn file(member: &Member, in_src: &str, text: &str) -> SourceFile {
        SourceFile::parse(&member.dir, Path::new(in_src), text).unwrap()
    }

    /// What the check finds in one file of a sans-IO crate.
    fn found_in(text: &str) -> Vec<(usize, Found)> {
        read(&file(&sans_io_crate(), "lib.rs", text), None)
    }

    /// What the check finds in `src/logic/call.rs` of a crate whose module `logic` is sans-IO.
    fn found_in_logic(text: &str) -> Vec<(usize, Found)> {
        read(
            &file(&io_crate(), "logic/call.rs", text),
            Some(&[vec!["logic".to_owned()]]),
        )
    }

    fn names(path: &str, reach: Reach) -> Found {
        Found::Names {
            path: path.to_owned(),
            reach,
        }
    }

    #[test]
    fn plain_code_passes() {
        let found = found_in(
            "use std::collections::BTreeMap;
             use std::io::{self, Read, Write};
             use std::net::{IpAddr, SocketAddr};
             use std::sync::Arc;
             use std::time::Duration;
             use gaslightmq_clock::{Clock, Instant};

             pub(crate) struct Budget { left: u32, since: Instant, wait: Duration }

             impl Budget {
                 pub fn spend(&mut self, clock: &dyn Clock, now: Instant) -> Option<Instant> {
                     let later = clock.now().max(now).max(Instant::ZERO);
                     self.left = self.left.checked_sub(1)?;
                     Some(later)
                 }
             }",
        );

        assert_eq!(found, []);
    }

    #[test]
    fn async_code_fails() {
        let found = found_in(
            "async fn wait() {}
             struct Call;
             impl Call {
                 async fn send(&self) {
                     self.flush().await;
                 }
             }
             trait Backend {
                 async fn call(&self);
                 async fn close(&self) {}
             }
             fn spawn() {
                 let block = async { 1 };
                 let closure = async || 1;
             }",
        );

        assert_eq!(
            found,
            [
                (1, Found::AsyncFunction),
                (4, Found::AsyncFunction),
                (5, Found::Await),
                // Line 9 declares an async method, and runs nothing.
                (10, Found::AsyncFunction),
                (13, Found::AsyncBlock),
                (14, Found::AsyncBlock),
            ]
        );
    }

    #[test]
    fn a_trait_declares_an_async_method() {
        let text = "pub trait RuleStore: Store<Rule> {
                        async fn list(&self) -> Vec<Rule>;
                        fn using_selector(&self, selector: &SelectorName) -> impl Future<Output = Vec<Rule>> + Send;
                    }
                    impl<S: RuleStore> RuleStore for Shared<S> {
                        fn using_selector(&self, selector: &SelectorName) -> impl Future<Output = Vec<Rule>> + Send {
                            self.0.using_selector(selector)
                        }
                    }";

        assert_eq!(found_in(text), []);
        assert_eq!(found_in_logic(text), []);
    }

    #[test]
    fn the_body_of_a_method_of_a_trait_is_async_code_like_any_other() {
        let found = found_in(
            "trait Backend {
                 async fn close(&self) {}
                 fn call(&self) -> impl Future<Output = u8> + Send { async move { self.answer().await } }
                 fn wait(&self) -> impl Future<Output = ()> { tokio::time::sleep(self.wait) }
             }
             impl Backend for Broker {
                 async fn close(&self) { self.flush().await; }
                 fn call(&self) -> impl Future<Output = u8> { std::future::ready(1) }
                 fn poll(&self, answer: Pin<&mut dyn Future<Output = u8>>) {}
                 fn later(&self) -> impl Future<Output = u8> { let answer = async { 1 }; answer }
             }
             impl Broker {
                 fn call(&self) -> impl Future<Output = u8> { async { 1 } }
             }",
        );

        assert_eq!(
            found,
            [
                (2, Found::AsyncFunction),
                // A body that is one `async` block is an `async fn` written out.
                (3, Found::AsyncFunction),
                (3, Found::Await),
                (4, names("tokio::time::sleep", Reach::Package)),
                (7, Found::AsyncFunction),
                (7, Found::Await),
                (8, names("std::future::ready", Reach::Async)),
                (9, names("Future", Reach::Future)),
                // A block next to other code makes a future on the side.
                (10, Found::AsyncBlock),
                // A method of no trait names no future, and writes no `async fn` out.
                (13, names("Future", Reach::Future)),
                (13, Found::AsyncBlock),
            ]
        );
    }

    #[test]
    fn a_future_outside_the_return_type_of_a_trait_method_fails() {
        let found = found_in(
            "struct Call<F: Future> { answer: F }
             fn later() -> impl Future<Output = u8> { answer() }
             trait Backend {
                 fn drive(&self, call: impl Future<Output = u8>);
             }
             impl Future for Call {}
             use std::task::Poll;",
        );

        assert_eq!(
            found,
            [
                (1, names("Future", Reach::Future)),
                (2, names("Future", Reach::Future)),
                (4, names("Future", Reach::Future)),
                (6, names("Future", Reach::Future)),
                (7, names("std::task::Poll", Reach::Async)),
            ]
        );
    }

    #[test]
    fn a_path_into_a_runtime_fails() {
        let found = found_in(
            "#[tokio::main]
             fn main() {
                 tokio::time::sleep(wait);
                 let stream: futures_util::stream::Empty<u8> = async_std::stream::empty();
             }",
        );

        assert_eq!(
            found,
            [
                (1, names("tokio::main", Reach::Package)),
                (3, names("tokio::time::sleep", Reach::Package)),
                (4, names("futures_util::stream::Empty", Reach::Package)),
                (4, names("async_std::stream::empty", Reach::Package)),
            ]
        );
    }

    #[test]
    fn a_socket_fails_wherever_it_comes_from() {
        let found = found_in(
            "use std::net::{SocketAddr, TcpStream};
             fn open(address: SocketAddr) {
                 let listener = net::TcpListener::bind(address);
                 let socket: Option<UdpSocket> = None;
             }",
        );

        assert_eq!(
            found,
            [
                (1, names("std::net::TcpStream", Reach::Socket)),
                (3, names("net::TcpListener::bind", Reach::Socket)),
                (4, names("UdpSocket", Reach::Socket)),
            ]
        );
    }

    #[test]
    fn the_file_system_fails() {
        let found = found_in(
            "use std::fs;
             use std::fs::File as Opened;
             fn read() { let text = std::fs::read_to_string(path); }",
        );

        assert_eq!(
            found,
            [
                (1, names("std::fs", Reach::FileSystem)),
                (2, names("std::fs::File", Reach::FileSystem)),
                (3, names("std::fs::read_to_string", Reach::FileSystem)),
            ]
        );
    }

    #[test]
    fn the_system_clock_fails() {
        let found = found_in(
            "use std::time::{Duration, Instant};
             fn now() {
                 let started = time::Instant::now();
                 let wall = SystemTime::now();
                 let date = chrono::Utc::now();
             }",
        );

        assert_eq!(
            found,
            [
                (1, names("std::time::Instant", Reach::SystemClock)),
                (3, names("time::Instant::now", Reach::SystemClock)),
                (4, names("SystemTime::now", Reach::SystemClock)),
                (5, names("chrono::Utc::now", Reach::SystemClock)),
            ]
        );
    }

    #[test]
    fn the_process_fails() {
        let found = found_in(
            "fn run() {
                 std::thread::sleep(wait);
                 let home = std::env::var(name);
                 let input = std::io::stdin();
                 std::process::exit(1);
             }",
        );

        assert_eq!(
            found,
            [
                (2, names("std::thread::sleep", Reach::Process)),
                (3, names("std::env::var", Reach::Process)),
                (4, names("std::io::stdin", Reach::Process)),
                (5, names("std::process::exit", Reach::Process)),
            ]
        );
    }

    #[test]
    fn global_randomness_fails() {
        let found = found_in(
            "fn roll() -> u8 {
                 let mut rng = rand::rng();
                 let seeded = rand::rngs::StdRng::seed_from_u64(seed);
                 rand::random()
             }",
        );

        assert_eq!(
            found,
            [
                (2, names("rand::rng", Reach::Randomness)),
                (4, names("rand::random", Reach::Randomness)),
            ]
        );
    }

    #[test]
    fn the_inside_of_a_macro_call_is_read_too() {
        let found = found_in(
            "fn run() {
                 let all = vec![std::fs::read(path), other];
                 select! { answer = socket.read().await => answer, later = async { 1 } => later }
             }",
        );

        assert_eq!(
            found,
            [
                (2, names("std::fs::read", Reach::FileSystem)),
                (3, Found::Await),
                (3, Found::AsyncBlock),
            ]
        );
    }

    #[test]
    fn comments_and_text_name_nothing() {
        let found = found_in(
            "/// Never calls `std::fs::read`, and is not async.
             fn plain() -> &'static str {
                 // tokio::spawn would be wrong here.
                 \"std::thread::sleep and .await\"
             }",
        );

        assert_eq!(found, []);
    }

    #[test]
    fn test_code_passes() {
        let found = found_in(
            "#[cfg(test)]
             mod tests {
                 #[tokio::test]
                 async fn waits() { tokio::time::sleep(wait).await; }
             }
             #[test]
             fn reads() { std::fs::read(path); }",
        );

        assert_eq!(found, []);
    }

    #[test]
    fn a_sans_io_module_stays_inside_the_sans_io_modules() {
        let found = found_in_logic(
            "use crate::logic::cluster::Route;
             use crate::logic;
             use super::versions::{self, Versions};
             use self::steps::Step;
             pub(crate) fn decide(route: &Route) -> super::Decision { crate::logic::wire_millis(route) }
             pub(in crate::logic) mod steps { use super::super::versions::*; }",
        );

        assert_eq!(found, []);
    }

    #[test]
    fn a_sans_io_module_that_names_the_rest_of_its_crate_fails() {
        let found = found_in_logic(
            "use crate::io::clock::MonotonicClock;
             use crate::ClientError;
             use super::super::configuration::*;
             fn decide() { let connections = super::super::io::Connections::new(); }
             mod steps { use super::super::super::address::BrokerAddress; }",
        );

        assert_eq!(
            found,
            [
                (1, names("crate::io::clock::MonotonicClock", Reach::RestOfTheCrate)),
                (2, names("crate::ClientError", Reach::RestOfTheCrate)),
                (3, names("super::super::configuration::*", Reach::RestOfTheCrate)),
                (4, names("super::super::io::Connections::new", Reach::RestOfTheCrate)),
                (
                    5,
                    names("super::super::super::address::BrokerAddress", Reach::RestOfTheCrate)
                ),
            ]
        );
    }

    #[test]
    fn a_crate_that_is_sans_io_as_a_whole_names_all_of_itself() {
        assert_eq!(found_in("use crate::dice::Die; use super::count::Count;"), []);
    }

    #[test]
    fn a_sans_io_module_is_held_to_the_same_names() {
        let found = found_in_logic("async fn send() { tokio::time::sleep(wait).await; }");

        assert_eq!(
            found,
            [
                (1, Found::AsyncFunction),
                (1, Found::Await),
                (1, names("tokio::time::sleep", Reach::Package)),
            ]
        );
    }

    #[test]
    fn only_the_declared_modules_of_a_crate_that_does_io_are_read() {
        let member = io_crate();
        let files = [
            file(&member, "lib.rs", "mod io; mod logic;"),
            file(
                &member,
                "io/connection.rs",
                "async fn connect() { tokio::net::TcpStream::connect(to).await; }",
            ),
            file(&member, "logic/mod.rs", "pub(crate) mod call;"),
            file(&member, "logic/call.rs", "use std::time::SystemTime;"),
        ];

        assert_eq!(
            source_violations(&member, true, &files),
            [SansIoViolation::InSource {
                path: PathBuf::from("crates/gaslightmq_kafka_client_native/src/logic/call.rs"),
                line: 1,
                code: Code::Module {
                    name: "gaslightmq_kafka_client_native".to_owned(),
                    module: "logic".to_owned()
                },
                found: names("std::time::SystemTime", Reach::SystemClock),
            }]
        );
    }

    #[test]
    fn a_module_in_one_file_is_a_sans_io_module_too() {
        let member = Member {
            sans_io_modules: vec!["error".to_owned(), "logic::cluster".to_owned()],
            ..io_crate()
        };
        let files = [
            file(&member, "error.rs", "use crate::logic::cluster::Route;"),
            file(&member, "logic/mod.rs", "use tokio::sync::Mutex;"),
            file(
                &member,
                "logic/cluster.rs",
                "use crate::error::ClientError; use super::call::Call;",
            ),
        ];

        let violations = source_violations(&member, true, &files);

        let lines: Vec<String> = violations.iter().map(ToString::to_string).collect();
        assert_eq!(
            lines,
            ["crates/gaslightmq_kafka_client_native/src/logic/cluster.rs:1: \
              the module `logic::cluster` of `gaslightmq_kafka_client_native` is sans-IO, \
              and names `super::call::Call`, which lies outside the sans-IO modules of the crate"]
        );
    }

    #[test]
    fn a_declared_module_without_a_file_fails() {
        let member = Member {
            sans_io_modules: vec!["logic".to_owned(), "decisions".to_owned()],
            ..io_crate()
        };
        let files = [file(&member, "lib.rs", "mod logic;"), file(&member, "logic.rs", "")];

        assert_eq!(
            source_violations(&member, true, &files),
            [SansIoViolation::NoSuchModule {
                name: "gaslightmq_kafka_client_native".to_owned(),
                module: "decisions".to_owned()
            }]
        );
    }

    #[test]
    fn a_sans_io_crate_that_declares_modules_fails() {
        let member = Member {
            sans_io_modules: vec!["logic".to_owned()],
            ..sans_io_crate()
        };

        assert_eq!(
            source_violations(&member, false, &[]),
            [SansIoViolation::DeclaresModules {
                name: "gaslightmq_trigger".to_owned()
            }]
        );
    }

    #[test]
    fn every_file_of_a_sans_io_crate_is_read() {
        let member = sans_io_crate();
        let files = [
            file(&member, "lib.rs", "mod dice;"),
            file(&member, "dice.rs", "fn roll() {\n    rand::random()\n}"),
        ];

        let violations = source_violations(&member, false, &files);

        let lines: Vec<String> = violations.iter().map(ToString::to_string).collect();
        assert_eq!(
            lines,
            [
                "crates/gaslightmq_trigger/src/dice.rs:2: `gaslightmq_trigger` is sans-IO, \
              and names `rand::random`, which is global randomness"
            ]
        );
    }

    #[test]
    fn no_crate_is_named_for_the_async_methods_that_its_traits_declare() {
        let text = "pub trait Backend {
                        async fn call(&self, request: Request) -> Answer;
                        fn close(&self) -> impl Future<Output = ()> + Send;
                    }
                    impl<B: Backend> Backend for Arc<B> {
                        fn close(&self) -> impl Future<Output = ()> + Send { (**self).close() }
                    }
                    async fn drive(backend: &impl Backend) { backend.close().await; }";

        // The contract of the backends, the contract of the stores, a crate of definitions, and any other crate.
        for name in [
            "gaslightmq_backend",
            "gaslightmq_definition_api",
            "gaslightmq_rule",
            "gaslightmq_trigger",
        ] {
            let member = Member::alone(name);
            let violations = source_violations(&member, false, &[file(&member, "lib.rs", text)]);

            let found: Vec<(usize, &Found)> = violations
                .iter()
                .filter_map(|violation| match violation {
                    SansIoViolation::InSource { line, found, .. } => Some((*line, found)),
                    _ => None,
                })
                .collect();
            assert_eq!(found, [(8, &Found::AsyncFunction), (8, &Found::Await)], "{name}");
        }
    }

    #[test]
    fn code_that_awaits_a_store_has_async_functions_and_no_other_async_code() {
        let text = "impl<D> Store<D> for MemoryStore<D> {
                        async fn list(&self) -> Vec<D> { self.definitions() }
                    }
                    pub async fn validate(store: &impl Store<Rule>) -> usize { store.list().await.len() }
                    fn ready(store: &Memory) -> impl Future<Output = usize> { std::future::ready(store.len()) }
                    fn later(store: &Memory) { let listing = async { store.list().await }; }
                    async fn nap() { tokio::time::sleep(wait).await; }
                    pub trait RuleStore: Store<Rule> {
                        fn count(&self) -> impl Future<Output = usize> + Send { async move { self.list().await.len() } }
                    }";
        let found_in_file = |name: &str, in_src: &str| {
            let member = Member::alone(name);
            let violations = source_violations(&member, false, &[file(&member, in_src, text)]);
            violations
                .into_iter()
                .filter_map(|violation| match violation {
                    SansIoViolation::InSource { line, found, .. } => Some((line, found)),
                    _ => None,
                })
                .collect::<Vec<(usize, Found)>>()
        };

        for (name, modules) in AWAITS_STORES {
            // A crate without a module awaits wherever it likes, and its root stands for it here.
            let in_src = modules
                .first()
                .map_or("lib.rs".to_owned(), |module| format!("{module}.rs"));

            assert_eq!(
                found_in_file(name, &in_src),
                [
                    (5, names("Future", Reach::Future)),
                    (5, names("std::future::ready", Reach::Async)),
                    (6, Found::AsyncBlock),
                    (7, names("tokio::time::sleep", Reach::Package)),
                ],
                "{name}"
            );
        }
        // The same code in any other sans-IO crate: four async functions and four awaits more.
        assert_eq!(found_in_file("gaslightmq_trigger", "lib.rs").len(), 12);
        // The crate of a definition awaits in the module of its store trait, and nowhere else.
        assert_eq!(found_in_file("gaslightmq_rule", "store.rs").len(), 4);
        assert_eq!(found_in_file("gaslightmq_rule", "snapshot.rs").len(), 12);
        // A store of environments is asked nothing more than the contract, so their crate awaits nowhere.
        assert_eq!(found_in_file("gaslightmq_environment", "store.rs").len(), 12);
        // The check of a store trait is a crate of its own, and awaits wherever it likes.
        assert_eq!(found_in_file("gaslightmq_rule_conformance", "samples.rs").len(), 4);
    }

    #[test]
    fn a_sans_io_crate_depends_on_nothing_that_does_io() {
        let layout = CrateLayout::parse(TABLES).unwrap();
        let owned = |names: &[&str]| names.iter().map(|&name| name.to_owned()).collect();
        let member = Member {
            dependencies: owned(&["gaslightmq_clock", "gaslightmq_listener"]),
            outside_dependencies: owned(&["serde", "tokio", "tokio-util", "futures-core", "async-trait", "rand"]),
            dev_dependencies: owned(&["tokio-test"]),
            ..sans_io_crate()
        };

        let violations = dependency_violations(&member, &layout);

        let named: Vec<&str> = violations
            .iter()
            .map(|violation| match violation {
                SansIoViolation::DependsOnIoCrate { dependency, .. } => dependency.as_str(),
                SansIoViolation::DependsOnIoPackage { package, .. } => package.as_str(),
                _ => "",
            })
            .collect();
        assert_eq!(
            named,
            [
                "gaslightmq_listener",
                "async-trait",
                "futures-core",
                "tokio",
                "tokio-util"
            ]
        );
    }

    #[test]
    fn a_planned_sans_io_crate_lists_nothing_that_does_io() {
        let layout = CrateLayout::parse(TABLES).unwrap();
        let workspace = Workspace {
            root: PathBuf::from("/repo"),
            members: vec![sans_io_crate(), Member::alone("gaslightmq_clock")],
        };

        assert_eq!(
            planned_violations(&layout, &workspace),
            [SansIoViolation::DependsOnIoCrate {
                name: "gaslightmq_test".to_owned(),
                dependency: "gaslightmq_listener".to_owned()
            }]
        );
    }

    #[test]
    fn a_crate_that_exists_is_held_to_its_manifest_and_not_to_its_row() {
        let layout = CrateLayout::parse(TABLES).unwrap();
        let workspace = Workspace {
            root: PathBuf::from("/repo"),
            members: vec![Member::alone("gaslightmq_test")],
        };

        assert_eq!(planned_violations(&layout, &workspace), []);
    }

    #[test]
    fn a_package_is_known_by_its_family() {
        assert!(is_io_package("tokio"));
        assert!(is_io_package("tokio-util"));
        assert!(is_io_package("tokio_util"));
        assert!(!is_io_package("tokiometer"));
        assert!(!is_io_package("serde"));
    }

    #[test]
    fn a_violation_says_where_and_what() {
        let depends = SansIoViolation::DependsOnIoPackage {
            name: "gaslightmq_trigger".to_owned(),
            package: "tokio".to_owned(),
        };
        let missing = SansIoViolation::NoSuchModule {
            name: "gaslightmq_kafka_client_native".to_owned(),
            module: "logic".to_owned(),
        };

        assert_eq!(
            depends.to_string(),
            "`gaslightmq_trigger` is sans-IO, and depends on `tokio`, a package that is async or does IO; \
             the row of a crate that does IO says \"This crate does IO\" in docs/specs/architecture/crates.md"
        );
        assert_eq!(
            missing.to_string(),
            "`gaslightmq_kafka_client_native` declares the sans-IO module `logic`, and has no file for it in `src/`"
        );
    }
}
