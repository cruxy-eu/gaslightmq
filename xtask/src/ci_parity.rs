//! The check that CI runs what `just ci` runs, and that each of it blocks a merge.
//!
//! The justfile says what `just ci` runs, and the workflow of CI says what runs on GitHub.
//! The check reads both, and fails when the workflow leaves something out:
//!
//! - **A check without a job.** Every check is a job of its own: a matrix whose entries come from the list that
//!   `cargo xtask --list` prints. The check follows the matrix to the output of the job that it needs, and that output
//!   to a step that prints the list.
//! - **A recipe without a job.** Every recipe that `just ci` depends on has a job with a step that runs `just` and
//!   that recipe. The recipe `checks` is the exception: the matrix of checks runs it.
//! - **A job that does not block a merge.** Branch protection requires the aggregate job alone, so its `needs`
//!   names every other job.
//!
//! What the check does not catch:
//!
//! - A matrix job that does not run the check of its entry. The check sees where the entries come from, and not
//!   what the job does with them.
//! - A step that names `just` and a recipe without running it, in a string or a comment of its script. The check
//!   reads the script as words, and not as a shell does.
//! - A job that never runs, because of its `if`, or a workflow that does not run on a pull request.
//! - A recipe that `just ci` runs in its body, rather than as a dependency.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::Path;

use serde::Deserialize;

/// The workflow of CI, relative to the workspace root.
pub const WORKFLOW: &str = ".github/workflows/ci.yml";

/// The justfile, relative to the workspace root.
pub const JUSTFILE: &str = "justfile";

/// The recipe of the justfile that runs everything CI runs.
const CI_RECIPE: &str = "ci";

/// The recipe that runs the checks, which the matrix of checks replaces in CI.
const CHECKS_RECIPE: &str = "checks";

/// The job that branch protection requires, and that passes only when every other job passed.
const AGGREGATE_JOB: &str = "ci-success";

/// The name of the binary of the checks, which a step may run instead of `cargo xtask`.
const BINARY: &str = "gaslightmq_checks";

/// The argument that prints the list of checks.
const LIST: &str = "--list";

/// What the workflow leaves out, for the checks named `checks`.
pub fn violations(root: &Path, checks: &[&str]) -> Result<Vec<ParityViolation>, CiFilesError> {
    let read = |path: &'static str| {
        std::fs::read_to_string(root.join(path)).map_err(|error| CiFilesError::Unreadable {
            path,
            reason: error.to_string(),
        })
    };
    let workflow = Workflow::parse(&read(WORKFLOW)?)?;
    let recipes = ci_recipes(&read(JUSTFILE)?).ok_or(CiFilesError::NoCiRecipe)?;
    Ok(of(&workflow, &recipes, checks))
}

/// What `workflow` leaves out of the recipes of `just ci` and of the checks.
fn of(workflow: &Workflow, recipes: &[String], checks: &[&str]) -> Vec<ParityViolation> {
    let mut violations = Vec::new();
    if !workflow.has_check_matrix() {
        violations.extend(checks.iter().map(|&check| ParityViolation::CheckWithoutJob {
            check: check.to_owned(),
        }));
    }
    let without_job = recipes
        .iter()
        .filter(|recipe| *recipe != CHECKS_RECIPE && !workflow.runs_recipe(recipe))
        .map(|recipe| ParityViolation::RecipeWithoutJob { recipe: recipe.clone() });
    violations.extend(without_job);
    match workflow.jobs.get(AGGREGATE_JOB) {
        None => violations.push(ParityViolation::NoAggregateJob),
        Some(aggregate) => {
            let needed = aggregate.needs.names();
            let unneeded = workflow
                .jobs
                .keys()
                .filter(|job| *job != AGGREGATE_JOB && !needed.contains(job.as_str()))
                .map(|job| ParityViolation::JobNotNeeded { job: job.clone() });
            violations.extend(unneeded);
        }
    }
    violations
}

/// The recipes that the recipe `ci` depends on, in their order, or `None` without a recipe `ci`.
///
/// A dependency with arguments, `(checks "wasm-boundary")`, counts as its recipe.
fn ci_recipes(justfile: &str) -> Option<Vec<String>> {
    justfile.lines().find_map(|line| {
        let (head, dependencies) = line.split_once(':')?;
        let mut head = head.split_whitespace();
        if line.starts_with(char::is_whitespace) || head.next() != Some(CI_RECIPE) || dependencies.starts_with('=') {
            return None;
        }
        let dependencies = dependencies.split('#').next().unwrap_or_default();
        let mut recipes = Vec::new();
        let mut in_arguments = false;
        for word in dependencies.split_whitespace() {
            if let Some(recipe) = word.strip_prefix('(') {
                recipes.push(recipe.trim_end_matches(')').to_owned());
                in_arguments = !word.ends_with(')');
            } else if in_arguments {
                in_arguments = !word.ends_with(')');
            } else if word != "&&" {
                recipes.push(word.to_owned());
            }
        }
        Some(recipes)
    })
}

/// The part of a workflow that the check reads.
#[derive(Debug, Deserialize)]
struct Workflow {
    #[serde(default)]
    jobs: BTreeMap<String, Job>,
}

#[derive(Debug, Deserialize)]
struct Job {
    #[serde(default)]
    needs: Needs,
    #[serde(default)]
    outputs: BTreeMap<String, String>,
    strategy: Option<Strategy>,
    #[serde(default)]
    steps: Vec<Step>,
}

/// The jobs that a job needs: one name, or a list of them.
#[derive(Debug, Default, Deserialize)]
#[serde(untagged)]
enum Needs {
    #[default]
    None,
    One(String),
    Many(Vec<String>),
}

impl Needs {
    fn names(&self) -> BTreeSet<&str> {
        match self {
            Needs::None => BTreeSet::new(),
            Needs::One(name) => BTreeSet::from([name.as_str()]),
            Needs::Many(names) => names.iter().map(String::as_str).collect(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct Strategy {
    /// Each dimension of the matrix, by its name: a list of entries, or an expression that makes the list.
    #[serde(default)]
    matrix: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct Step {
    id: Option<String>,
    run: Option<String>,
}

impl Workflow {
    fn parse(text: &str) -> Result<Self, CiFilesError> {
        serde_saphyr::from_str(text).map_err(|error| CiFilesError::NotAWorkflow {
            reason: error.to_string(),
        })
    }

    /// Whether a job has a matrix whose entries come from the list of checks.
    ///
    /// The matrix reads `${{ fromJSON(needs.<job>.outputs.<output>) }}`, the job it needs sets that output to
    /// `${{ steps.<step>.outputs.<name> }}`, and that step runs `cargo xtask --list` or the binary with `--list`.
    fn has_check_matrix(&self) -> bool {
        self.jobs.values().any(|job| {
            let Some(strategy) = &job.strategy else {
                return false;
            };
            strategy
                .matrix
                .values()
                .filter_map(serde_json::Value::as_str)
                .any(|dimension| self.lists_checks(job, dimension))
        })
    }

    /// Whether `dimension`, a dimension of the matrix of `job`, is the list of checks.
    fn lists_checks(&self, job: &Job, dimension: &str) -> bool {
        let Some(["needs", needed, "outputs", output]) = expression(dimension, "fromJSON") else {
            return false;
        };
        let Some(needed_job) = self.jobs.get(needed).filter(|_| job.needs.names().contains(needed)) else {
            return false;
        };
        let Some(["steps", step, "outputs", _]) =
            needed_job.outputs.get(output).and_then(|value| expression(value, ""))
        else {
            return false;
        };
        needed_job
            .steps
            .iter()
            .filter(|candidate| candidate.id.as_deref() == Some(step))
            .filter_map(|candidate| candidate.run.as_deref())
            .any(prints_list)
    }

    /// Whether a step of a job runs `just <recipe>`.
    fn runs_recipe(&self, recipe: &str) -> bool {
        self.jobs
            .values()
            .flat_map(|job| &job.steps)
            .filter_map(|step| step.run.as_deref())
            .any(|run| words(run).windows(2).any(|pair| pair == ["just", recipe]))
    }
}

/// The four parts of the path in `${{ <function>(<path>) }}`, or in `${{ <path> }}` when `function` is empty.
///
/// `${{ fromJSON(needs.xtask.outputs.checks) }}` is `["needs", "xtask", "outputs", "checks"]`.
/// GitHub reads the name of a function in any case.
fn expression<'a>(text: &'a str, function: &str) -> Option<[&'a str; 4]> {
    let inner = text.trim().strip_prefix("${{")?.strip_suffix("}}")?.trim();
    let path = if function.is_empty() {
        inner
    } else {
        let (name, rest) = inner.split_at_checked(function.len())?;
        if !name.eq_ignore_ascii_case(function) {
            return None;
        }
        rest.trim().strip_prefix('(')?.strip_suffix(')')?.trim()
    };
    path.split('.').collect::<Vec<_>>().try_into().ok()
}

/// Whether a script runs the checks with `--list`: `cargo xtask --list`, or the binary of the checks.
fn prints_list(run: &str) -> bool {
    let words = words(run);
    let runs_checks = |at: usize| {
        let word = words[at];
        word == BINARY
            || word.ends_with(&format!("/{BINARY}"))
            || (word == "xtask" && at > 0 && words[at - 1] == "cargo")
    };
    words
        .iter()
        .enumerate()
        .skip(1)
        .any(|(at, word)| *word == LIST && runs_checks(at - 1))
}

/// The words of a script, without the quotes and the command substitutions around them.
fn words(run: &str) -> Vec<&str> {
    run.split_whitespace()
        .map(|word| word.trim_matches(|c| matches!(c, '"' | '\'' | '$' | '(' | ')' | '`')))
        .filter(|word| !word.is_empty())
        .collect()
}

/// What the workflow of CI leaves out.
#[derive(Debug, PartialEq)]
pub enum ParityViolation {
    /// No matrix job comes from the list of checks, so this check has no job.
    CheckWithoutJob { check: String },
    /// No job runs this recipe of `just ci`.
    RecipeWithoutJob { recipe: String },
    /// The workflow has no aggregate job.
    NoAggregateJob,
    /// The aggregate job does not need this job, so its failure does not block a merge.
    JobNotNeeded { job: String },
}

impl fmt::Display for ParityViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CheckWithoutJob { check } => write!(
                f,
                "{WORKFLOW}: the check `{check}` has no job; \
                 no matrix of the workflow takes its entries from `cargo xtask {LIST}`"
            ),
            Self::RecipeWithoutJob { recipe } => write!(
                f,
                "{WORKFLOW}: `just {CI_RECIPE}` runs the recipe `{recipe}`, and no job runs `just {recipe}`"
            ),
            Self::NoAggregateJob => write!(
                f,
                "{WORKFLOW}: there is no job `{AGGREGATE_JOB}`, which branch protection requires \
                 and which needs every other job"
            ),
            Self::JobNotNeeded { job } => write!(
                f,
                "{WORKFLOW}: `{AGGREGATE_JOB}` does not need the job `{job}`, so a failure of it does not block a merge"
            ),
        }
    }
}

/// Why the workflow or the justfile could not be read.
#[derive(Debug, thiserror::Error)]
pub enum CiFilesError {
    #[error("{path} cannot be read: {reason}")]
    Unreadable { path: &'static str, reason: String },
    #[error("{WORKFLOW} is no workflow the checks can read: {reason}")]
    NotAWorkflow { reason: String },
    #[error("{JUSTFILE} has no recipe `{CI_RECIPE}`")]
    NoCiRecipe,
}

#[cfg(test)]
mod tests {
    use super::*;

    const CHECKS: [&str; 2] = ["dyn-errors", "sans-io"];

    const JUSTFILE: &str = "\
set shell := [\"bash\", \"-c\"]

# Everything CI runs.
ci: fmt-check checks test # cheapest first
    @echo done

checks *names:
    cargo xtask {{names}}
";

    /// A workflow that runs every recipe and every check, and whose aggregate job needs every job.
    const COMPLETE: &str = r#"
name: CI
on:
  pull_request:
jobs:
  fmt:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v7
      - run: just fmt-check
  xtask:
    runs-on: ubuntu-latest
    outputs:
      checks: ${{ steps.list.outputs.checks }}
    steps:
      - run: cargo build --release --package gaslightmq_checks
      - id: list
        run: echo "checks=$(target/release/gaslightmq_checks --list)" >> "$GITHUB_OUTPUT"
  check:
    name: "Check: ${{ matrix.check.title }}"
    needs: xtask
    strategy:
      fail-fast: false
      matrix:
        check: ${{ fromJSON(needs.xtask.outputs.checks) }}
    steps:
      - run: gaslightmq_checks "$CHECK"
  test:
    steps:
      - run: just test
  ci-success:
    name: CI
    if: always()
    needs: [fmt, xtask, check, test]
    steps:
      - run: exit 0
"#;

    fn lines(workflow: &str) -> Vec<String> {
        let workflow = Workflow::parse(workflow).unwrap();
        let recipes = ci_recipes(JUSTFILE).unwrap();
        of(&workflow, &recipes, &CHECKS)
            .iter()
            .map(ToString::to_string)
            .collect()
    }

    #[test]
    fn a_workflow_that_runs_everything_passes() {
        assert_eq!(lines(COMPLETE), [""; 0]);
    }

    #[test]
    fn the_recipes_of_ci_are_its_dependencies() {
        let justfile = "ci: fmt-check (checks \"wasm-boundary\" \"sans-io\") clippy && doc\n    @echo done\n";

        assert_eq!(ci_recipes(justfile).unwrap(), ["fmt-check", "checks", "clippy", "doc"]);
        assert_eq!(ci_recipes(JUSTFILE).unwrap(), ["fmt-check", "checks", "test"]);
    }

    #[test]
    fn a_justfile_without_a_recipe_ci_has_no_recipes() {
        assert_eq!(ci_recipes("cid: test\nchecks:\n    cargo xtask\n"), None);
    }

    #[test]
    fn a_matrix_that_lists_the_checks_itself_fails_for_every_check() {
        let workflow = COMPLETE.replace(
            "check: ${{ fromJSON(needs.xtask.outputs.checks) }}",
            "check: [dyn-errors]",
        );

        assert_eq!(
            lines(&workflow),
            [
                ".github/workflows/ci.yml: the check `dyn-errors` has no job; \
                 no matrix of the workflow takes its entries from `cargo xtask --list`",
                ".github/workflows/ci.yml: the check `sans-io` has no job; \
                 no matrix of the workflow takes its entries from `cargo xtask --list`",
            ]
        );
    }

    #[test]
    fn a_matrix_from_an_output_of_another_step_fails() {
        let workflow = COMPLETE.replace("--list", "--wasm-packages");

        assert_eq!(lines(&workflow).len(), CHECKS.len());
    }

    #[test]
    fn a_matrix_from_a_job_it_does_not_need_fails() {
        let workflow = COMPLETE.replace("    needs: xtask\n", "");

        assert_eq!(lines(&workflow).len(), CHECKS.len());
    }

    #[test]
    fn a_matrix_from_cargo_xtask_passes() {
        let workflow = COMPLETE.replace("$(target/release/gaslightmq_checks --list)", "$(cargo xtask --list)");

        assert_eq!(lines(&workflow), [""; 0]);
    }

    #[test]
    fn a_recipe_without_a_job_fails() {
        let workflow = COMPLETE.replace("just test", "cargo test --workspace");

        assert_eq!(
            lines(&workflow),
            [".github/workflows/ci.yml: `just ci` runs the recipe `test`, and no job runs `just test`"]
        );
    }

    #[test]
    fn a_recipe_with_a_longer_name_is_another_recipe() {
        let workflow = COMPLETE.replace("just test", "just test-kafka");

        assert_eq!(
            lines(&workflow),
            [".github/workflows/ci.yml: `just ci` runs the recipe `test`, and no job runs `just test`"]
        );
    }

    #[test]
    fn a_job_that_the_aggregate_does_not_need_fails() {
        let workflow = COMPLETE.replace("needs: [fmt, xtask, check, test]", "needs: [fmt, xtask, test]");

        assert_eq!(
            lines(&workflow),
            [".github/workflows/ci.yml: `ci-success` does not need the job `check`, \
              so a failure of it does not block a merge"]
        );
    }

    #[test]
    fn an_aggregate_that_needs_one_job_fails_for_the_others() {
        let workflow = COMPLETE.replace("needs: [fmt, xtask, check, test]", "needs: test");

        assert_eq!(lines(&workflow).len(), 3);
    }

    #[test]
    fn a_workflow_without_an_aggregate_job_fails() {
        let workflow = COMPLETE.replace("  ci-success:", "  all-passed:");

        assert_eq!(
            lines(&workflow),
            [
                ".github/workflows/ci.yml: there is no job `ci-success`, which branch protection requires \
                 and which needs every other job"
            ]
        );
    }

    #[test]
    fn a_file_that_is_no_workflow_cannot_be_read() {
        let error = Workflow::parse("jobs: [fmt").unwrap_err();

        assert!(matches!(error, CiFilesError::NotAWorkflow { .. }), "{error}");
    }
}
