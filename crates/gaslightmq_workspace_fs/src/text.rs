//! What the text of one file reads as, and the one written form of a definition.

use crate::document::{Document, Item};
use crate::kind::FileDefinition;

/// What the text of one file holds: its layout, and the definition of each of its items.
#[derive(Debug, Clone)]
pub(crate) struct Read<D> {
    pub(crate) document: Document,
    /// One for each item of the document, in its order.
    pub(crate) definitions: Vec<D>,
}

/// Reads the text of a file of the kind `D`.
///
/// # Errors
///
/// Everything that is wrong, each as one line: the path of the key that failed, and what is wrong with the value
/// there. An item of a list starts its path with its place in the list: `[0].trigger.every`.
pub(crate) fn read<D: FileDefinition>(text: &str) -> Result<Read<D>, Vec<String>> {
    let document = Document::parse(text).map_err(|unreadable| vec![unreadable.reason])?;
    let lists = D::PLACE.holds_lists();
    match (&document, lists) {
        (Document::One(_), true) => return Err(vec![format!("the file holds no list of {}s", D::LABEL)]),
        (Document::List(_), false) => {
            return Err(vec![format!("the file holds a list, and not one {}", D::LABEL)]);
        }
        (Document::Nothing, false) => return Err(vec![format!("the file holds no {}", D::LABEL)]),
        _ => {}
    }

    let mut definitions = Vec::new();
    let mut errors = Vec::new();
    for (index, item) in document.items().iter().enumerate() {
        let at = lists.then(|| format!("[{index}]"));
        match read_item::<D>(text, item) {
            Ok(definition) => definitions.push(definition),
            Err(Misread { path, message }) => errors.push(located(at.as_deref(), path.as_deref(), &message)),
        }
    }
    if errors.is_empty() {
        Ok(Read { document, definitions })
    } else {
        Err(errors)
    }
}

/// The one written form of a definition: a mapping at column zero, its keys in the order of its type.
///
/// # Errors
///
/// What the writer said, when it could not write the definition.
pub(crate) fn written<D: FileDefinition>(definition: &D) -> Result<String, String> {
    serde_saphyr::to_string(definition).map_err(|error| error.to_string())
}

/// Why one item does not read: the path of the key that failed, when the reader knows it, and what went wrong.
struct Misread {
    path: Option<String>,
    message: String,
}

/// Reads one item alone, with the lines and the columns it has in its file, as every text of a definition is read.
fn read_item<D: FileDefinition>(text: &str, item: &Item) -> Result<D, Misread> {
    gaslightmq_definition_text::read(&item.alone(text)).map_err(|error| Misread {
        path: error.path,
        message: error.reason,
    })
}

/// The path of a key in a file, then what is wrong there: `[0].trigger.every: a count is at least one`.
///
/// `at` is the place of the item in its list. A path that names no key, as `.` for the whole value, adds nothing.
fn located(at: Option<&str>, path: Option<&str>, message: &str) -> String {
    match (at, path) {
        (Some(at), Some(key)) => format!("{at}.{key}: {message}"),
        (Some(at), None) => format!("{at}: {message}"),
        (None, Some(key)) => format!("{key}: {message}"),
        (None, None) => message.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use gaslightmq_environment::Environment;
    use gaslightmq_rule::Rule;
    use gaslightmq_selector::Selector;

    use super::*;

    #[test]
    fn an_error_names_the_item_and_the_key_and_says_what_the_value_said() {
        let text = "\
- name: blackhole-fetches
  selector: fetches
  action: { lie: silence }
- name: slow
  selector: fetches
  action: { lie: silence }
  trigger:
    every: 0
";
        let errors = read::<Rule>(text).expect_err("a count of zero");

        assert_eq!(errors, ["[1].trigger.every: a count is at least one"]);
    }

    #[test]
    fn every_item_that_does_not_read_is_named() {
        let text = "\
- name: a
  uuid: 4b6c
  match:
    api: Fetch
- name: b
  match:
    api: Nope
";
        let errors = read::<Selector>(text).expect_err("two items do not read");

        assert_eq!(errors.len(), 2);
        assert!(errors[0].starts_with("[0].uuid: unknown field `uuid`"), "{}", errors[0]);
        assert_eq!(errors[1], "[1].match.api: api: unknown API key: \"Nope\"");
    }

    #[test]
    fn a_file_of_one_definition_names_the_key_alone() {
        let text = "\
name: staging
proxy:
  upstream:
    bootstrap: kafka:9092
    sasl:
      mechanism: PLAIN
      username: gaslight
      password: hunter2
  listen:
    bootstrap: 0.0.0.0:9092
    brokers: 0.0.0.0:19092-19094
    advertise: localhost
";
        let errors = read::<Environment>(text).expect_err("a secret is a reference");

        assert_eq!(
            errors,
            [
                "proxy.upstream.sasl.password: a secret is a reference and never the value: \
              write { file: … } or { env: … }"
            ]
        );
    }

    #[test]
    fn a_name_that_yaml_1_1_took_for_a_boolean_stays_a_name() {
        let read = read::<Selector>("- name: on\n  match:\n    api: Fetch\n").expect("a selector named on");

        assert_eq!(read.definitions[0].name().as_str(), "on");
    }

    #[test]
    fn a_file_of_the_wrong_shape_says_what_belongs_there() {
        assert_eq!(
            read::<Selector>("name: a\n").expect_err("no list").as_slice(),
            ["the file holds no list of selectors"]
        );
        assert_eq!(
            read::<Environment>("").expect_err("nothing").as_slice(),
            ["the file holds no environment"]
        );
        assert_eq!(
            read::<Environment>("- name: shop\n").expect_err("a list").as_slice(),
            ["the file holds a list, and not one environment"]
        );
    }

    #[test]
    fn a_definition_has_one_written_form_with_its_keys_in_order() {
        let text = "- name: fetches\n  title: Fetches\n  match:\n    api: Fetch\n    topic: orders.*\n";
        let read = read::<Selector>(text).expect("a selector");
        let selector = &read.definitions[0];

        let written = written(selector).expect("a selector writes");

        assert_eq!(
            written,
            "name: fetches\ntitle: Fetches\nmatch:\n  api: Fetch\n  topic: orders.*\n"
        );
    }
}
