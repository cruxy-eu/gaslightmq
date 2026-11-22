//! The workspace's own checks: rules of `crates.md` and `CLAUDE.md` that the compiler and clippy cannot see.
//!
//! `cargo xtask [check ...]` runs the named checks, or every check without a name.
//! It prints one line per violation, and fails when it finds one.
//!
//! `cargo xtask --wasm-packages` runs no check.
//! It prints the crates that compile to WebAssembly, as the `--package` arguments of a cargo command.

mod crate_layout;
mod crate_names;
mod manifest;
mod sans_io;
mod source;
mod table;
mod wasm_crates;
mod workspace;
mod workspace_rules;
mod wrapped_errors;

use std::process::ExitCode;

use crate::crate_layout::CrateLayout;
use crate::wasm_crates::WasmCrates;
use crate::workspace::Workspace;

/// The argument that asks for the crates that compile to WebAssembly, instead of a check.
const WASM_PACKAGES: &str = "--wasm-packages";

/// One check, by the name it has on the command line.
#[derive(Clone, Copy)]
enum Check {
    /// Every crate depends on what its row in `crates.md` lists.
    Dependencies,
    /// Every crate name starts with `gaslightmq_`, and its directory carries it.
    CrateNames,
    /// No crate that compiles to WebAssembly reaches the Kafka codec.
    WasmBoundary,
    /// Every crate builds with edition 2024, takes its dependencies from the workspace, and leaves `async-trait` out.
    WorkspaceRules,
    /// Sans-IO code is not async, and reaches no runtime, socket, file or system clock.
    SansIo,
    /// An error type keeps an error it holds in a field `error`, and its `source()` returns `None`.
    WrappedErrors,
}

impl Check {
    const ALL: [Check; 6] = [
        Check::Dependencies,
        Check::CrateNames,
        Check::WasmBoundary,
        Check::WorkspaceRules,
        Check::SansIo,
        Check::WrappedErrors,
    ];

    fn name(self) -> &'static str {
        match self {
            Check::Dependencies => "dependencies",
            Check::CrateNames => "crate-names",
            Check::WasmBoundary => "wasm-boundary",
            Check::WorkspaceRules => "workspace-rules",
            Check::SansIo => "sans-io",
            Check::WrappedErrors => "wrapped-errors",
        }
    }

    fn named(name: &str) -> Option<Check> {
        Check::ALL.into_iter().find(|check| check.name() == name)
    }

    /// The violations it finds, one line each, or the reason it could not look.
    fn run(self, workspace: &Workspace) -> Result<Vec<String>, String> {
        match self {
            Check::Dependencies => {
                let layout = CrateLayout::load(&workspace.root).map_err(|error| error.to_string())?;
                Ok(lines(&layout.violations(workspace)))
            }
            Check::CrateNames => Ok(lines(&crate_names::violations(workspace))),
            Check::WasmBoundary => {
                let layout = CrateLayout::load(&workspace.root).map_err(|error| error.to_string())?;
                let wasm_crates = WasmCrates::load(&workspace.root).map_err(|error| error.to_string())?;
                Ok(lines(&wasm_crates.violations(&layout, workspace)))
            }
            Check::WorkspaceRules => {
                let violations = workspace_rules::violations(workspace).map_err(|error| error.to_string())?;
                Ok(lines(&violations))
            }
            Check::SansIo => {
                let layout = CrateLayout::load(&workspace.root).map_err(|error| error.to_string())?;
                let violations = sans_io::violations(&layout, workspace).map_err(|error| error.to_string())?;
                Ok(lines(&violations))
            }
            Check::WrappedErrors => {
                let violations = wrapped_errors::violations(workspace).map_err(|error| error.to_string())?;
                Ok(lines(&violations))
            }
        }
    }
}

/// The crates of the WebAssembly table that exist, each as a `--package` argument of a cargo command.
fn wasm_packages() -> Result<Vec<String>, String> {
    let workspace = Workspace::load().map_err(|error| error.to_string())?;
    let wasm_crates = WasmCrates::load(&workspace.root).map_err(|error| error.to_string())?;
    let packages = wasm_crates.existing(&workspace);
    if packages.is_empty() {
        // Without a package, cargo would build the whole workspace.
        return Err(format!(
            "the workspace holds no crate that {} lists as compiling to WebAssembly",
            crate_layout::PATH
        ));
    }
    Ok(packages
        .into_iter()
        .map(|member| format!("--package {}", member.name))
        .collect())
}

fn lines(violations: &[impl ToString]) -> Vec<String> {
    violations.iter().map(ToString::to_string).collect()
}

#[expect(
    clippy::print_stdout,
    clippy::print_stderr,
    reason = "a check reports on the terminal"
)]
fn main() -> ExitCode {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    if arguments == [WASM_PACKAGES] {
        return match wasm_packages() {
            Ok(packages) => {
                println!("{}", packages.join("\n"));
                ExitCode::SUCCESS
            }
            Err(reason) => {
                eprintln!("{reason}");
                ExitCode::from(2)
            }
        };
    }

    let mut checks = Vec::new();
    for name in arguments {
        let Some(check) = Check::named(&name) else {
            let names: Vec<&str> = Check::ALL.iter().map(|check| check.name()).collect();
            eprintln!("there is no check `{name}`; the checks are {}", names.join(", "));
            return ExitCode::from(2);
        };
        checks.push(check);
    }
    if checks.is_empty() {
        checks = Check::ALL.to_vec();
    }

    let workspace = match Workspace::load() {
        Ok(workspace) => workspace,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::from(2);
        }
    };

    let mut passed = true;
    for check in checks {
        match check.run(&workspace) {
            Ok(violations) if violations.is_empty() => println!("✓ {}", check.name()),
            Ok(violations) => {
                passed = false;
                for violation in violations {
                    println!("✗ {}: {violation}", check.name());
                }
            }
            Err(reason) => {
                passed = false;
                println!("✗ {}: {reason}", check.name());
            }
        }
    }
    if passed { ExitCode::SUCCESS } else { ExitCode::FAILURE }
}
