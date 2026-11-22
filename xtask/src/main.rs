//! The workspace's own checks: rules of `crates.md` and `CLAUDE.md` that the compiler and clippy cannot see.
//!
//! `cargo xtask [check ...]` runs the named checks, or every check without a name.
//! It prints one line per violation, and fails when it finds one.

mod crate_layout;
mod crate_names;
mod workspace;

use std::process::ExitCode;

use crate::crate_layout::CrateLayout;
use crate::workspace::Workspace;

/// One check, by the name it has on the command line.
#[derive(Clone, Copy)]
enum Check {
    /// Every crate depends on what its row in `crates.md` lists.
    Dependencies,
    /// Every crate name starts with `gaslightmq_`, and its directory carries it.
    CrateNames,
}

impl Check {
    const ALL: [Check; 2] = [Check::Dependencies, Check::CrateNames];

    fn name(self) -> &'static str {
        match self {
            Check::Dependencies => "dependencies",
            Check::CrateNames => "crate-names",
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
        }
    }
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
    let mut checks = Vec::new();
    for name in std::env::args().skip(1) {
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
