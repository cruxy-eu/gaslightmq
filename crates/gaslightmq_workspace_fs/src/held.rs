//! What a store of files holds: every file that it read or wrote, and the definitions in them.
//!
//! Nothing here reads or writes a file. The store hands in the bytes it found, and writes the text it gets back.

use std::collections::BTreeMap;
use std::hash::{DefaultHasher, Hasher};
use std::path::{Path, PathBuf};

use gaslightmq_definition_api::{Definition, StoreChangeError, StoreCreateError};

use crate::document::{Document, InFlow, Item};
use crate::error::FileError;
use crate::kind::{FileDefinition, Place};
use crate::text::{self, Read};

/// Every file of one kind that a store read or wrote, and every definition in them.
#[derive(Debug)]
pub(crate) struct Held<D: FileDefinition> {
    /// By the path below the directory of the workspace, with the files that the store found missing.
    files: BTreeMap<PathBuf, Known>,
    definitions: BTreeMap<D::Name, Entry<D>>,
}

/// What a store knows of one file: the hash of its bytes and their layout, or that the file is not there.
#[derive(Debug, Clone)]
struct Known {
    /// `None` when the file is not there.
    fingerprint: Option<Fingerprint>,
    document: Document,
}

/// One definition, and the item of the file that holds it.
#[derive(Debug, Clone)]
struct Entry<D> {
    definition: D,
    file: PathBuf,
    item: usize,
}

/// The hash of the bytes of a file, which says whether someone changed the file since.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Fingerprint(u64);

impl Fingerprint {
    pub(crate) fn of(bytes: &[u8]) -> Self {
        let mut hasher = DefaultHasher::new();
        hasher.write(bytes);
        Self(hasher.finish())
    }
}

/// One file below the directory of the workspace, with its bytes, or `None` when it is not there.
pub(crate) type Found = (PathBuf, Option<Vec<u8>>);

/// One change that a store makes to one file.
#[derive(Debug, Clone)]
pub(crate) enum Edit<D: Definition> {
    Create(D),
    Update(D),
    Delete(D::Name),
}

/// What a file holds after a change.
#[derive(Debug, Clone)]
pub(crate) enum After<D> {
    /// The new text, and what it reads as.
    Text { text: String, read: Read<D> },
    /// The file goes: it held the one definition that the change deleted, or nothing else.
    Gone,
}

impl<D: FileDefinition> Held<D> {
    /// What a store holds before it read anything: no definition, and no file.
    pub(crate) fn new() -> Self {
        Self {
            files: BTreeMap::new(),
            definitions: BTreeMap::new(),
        }
    }

    /// What the files of the kind hold.
    ///
    /// `found` holds every file of the kind with its bytes, and `None` for a file of the kind that is not there.
    ///
    /// # Errors
    ///
    /// Every file that does not read, with what is wrong in it: its text, or a name that an earlier file or item has.
    pub(crate) fn read(found: Vec<Found>) -> Result<Self, Vec<FileError>> {
        let mut held = Self::new();
        let mut errors = Vec::new();
        for (file, bytes) in found {
            let Some(bytes) = bytes else {
                held.files.insert(file, Known::missing());
                continue;
            };
            let Ok(text) = std::str::from_utf8(&bytes) else {
                errors.push(FileError {
                    file,
                    reason: "the file is no UTF-8 text".to_owned(),
                });
                continue;
            };
            let read = match text::read::<D>(text) {
                Ok(read) => read,
                Err(reasons) => {
                    errors.extend(reasons.into_iter().map(|reason| FileError {
                        file: file.clone(),
                        reason,
                    }));
                    continue;
                }
            };
            for (item, definition) in read.definitions.into_iter().enumerate() {
                if let Some(first) = held.definitions.get(definition.name()) {
                    let at = if D::PLACE.holds_lists() {
                        format!("[{item}].name")
                    } else {
                        "name".to_owned()
                    };
                    errors.push(FileError {
                        file: file.clone(),
                        reason: format!(
                            "{at}: {} {:?} is in {} already",
                            D::LABEL,
                            definition.name().as_ref(),
                            first.file.display()
                        ),
                    });
                    continue;
                }
                let entry = Entry {
                    definition,
                    file: file.clone(),
                    item,
                };
                held.definitions.insert(entry.definition.name().clone(), entry);
            }
            held.files.insert(
                file,
                Known {
                    fingerprint: Some(Fingerprint::of(&bytes)),
                    document: read.document,
                },
            );
        }
        if errors.is_empty() { Ok(held) } else { Err(errors) }
    }

    /// Every definition, in the order of their names.
    pub(crate) fn list(&self) -> Vec<D> {
        self.definitions
            .values()
            .map(|entry| entry.definition.clone())
            .collect()
    }

    /// The definition of this name.
    pub(crate) fn get(&self, name: &D::Name) -> Option<D> {
        self.definitions.get(name).map(|entry| entry.definition.clone())
    }

    /// Whether the text of the definition of this name holds a comment that a write would lose.
    pub(crate) fn would_lose_comment(&self, name: &D::Name) -> bool {
        self.definitions.get(name).is_some_and(|entry| {
            let items = self.files.get(&entry.file).map(|known| known.document.items());
            items
                .and_then(|items| items.get(entry.item))
                .is_some_and(Item::commented)
        })
    }

    /// The file that a new definition goes to.
    ///
    /// # Errors
    ///
    /// - [`StoreCreateError::NameTaken`] when a definition has the name.
    /// - [`StoreCreateError::Failed`] when the file of its name holds another definition.
    pub(crate) fn file_for(&self, definition: &D) -> Result<PathBuf, StoreCreateError> {
        let name = definition.name();
        if self.definitions.contains_key(name) {
            return Err(StoreCreateError::NameTaken);
        }
        let file = D::PLACE.file_for(name.as_ref());
        if let Place::Each(_) = D::PLACE
            && let Some(holder) = self.definitions.values().find(|entry| entry.file == file)
        {
            return Err(StoreCreateError::Failed {
                reason: format!(
                    "{} holds {} {:?}",
                    file.display(),
                    D::LABEL,
                    holder.definition.name().as_ref()
                ),
            });
        }
        Ok(file)
    }

    /// The file that holds the definition of this name.
    ///
    /// # Errors
    ///
    /// [`StoreChangeError::NotFound`] when no definition has the name.
    pub(crate) fn file_holding(&self, name: &D::Name) -> Result<PathBuf, StoreChangeError> {
        match self.definitions.get(name) {
            Some(entry) => Ok(entry.file.clone()),
            None => Err(StoreChangeError::NotFound),
        }
    }

    /// What the store holds of a file: the hash of its bytes, or `None` when it holds that the file is not there.
    /// A file that the store never saw is not there either.
    pub(crate) fn fingerprint(&self, file: &Path) -> Option<Fingerprint> {
        self.files.get(file).and_then(|known| known.fingerprint)
    }

    /// What a file holds after a change, from `text`: the text of the file now, which the store read.
    ///
    /// The change replaces the text of the one definition that changes, or adds it after the others,
    /// or takes it away, and leaves every other byte as it was.
    /// The text it gives reads back as every definition of the file, with the change.
    ///
    /// # Errors
    ///
    /// Why the store does not make the change: the file holds a list in flow style, or the written form does not
    /// read back as the definition.
    pub(crate) fn after(&self, file: &Path, text: &str, edit: &Edit<D>) -> Result<After<D>, String> {
        let document = self.files.get(file).map_or(&Document::Nothing, |known| &known.document);
        let mut expected = self.definitions_of(file);
        let in_flow = |InFlow| {
            format!(
                "{} holds its {}s in a list in flow style, `[…]`, which a write does not edit",
                file.display(),
                D::LABEL
            )
        };
        let new_text = match edit {
            Edit::Create(definition) => {
                expected.push(definition.clone());
                let written = text::written(definition)?;
                if D::PLACE.holds_lists() {
                    document.append(text, &written).map_err(in_flow)?
                } else {
                    written
                }
            }
            Edit::Update(definition) => {
                let index = self.item_of(definition.name());
                expected[index] = definition.clone();
                let written = text::written(definition)?;
                document.replace(text, index, &written).map_err(in_flow)?
            }
            Edit::Delete(name) => {
                let index = self.item_of(name);
                expected.remove(index);
                if !D::PLACE.holds_lists() {
                    return Ok(After::Gone);
                }
                document.remove(text, index).map_err(in_flow)?
            }
        };
        if expected.is_empty() && new_text.trim().is_empty() {
            return Ok(After::Gone);
        }

        let unread = || match edit {
            Edit::Create(definition) | Edit::Update(definition) => format!(
                "the written form of {} {:?} does not read back as it",
                D::LABEL,
                definition.name().as_ref()
            ),
            Edit::Delete(name) => format!(
                "{} does not read without {} {:?}",
                file.display(),
                D::LABEL,
                name.as_ref()
            ),
        };
        let read = text::read::<D>(&new_text).map_err(|_| unread())?;
        if read.definitions != expected {
            return Err(unread());
        }
        Ok(After::Text { text: new_text, read })
    }

    /// Holds what a file holds after a change that the store wrote.
    pub(crate) fn take(&mut self, file: PathBuf, after: After<D>) {
        self.definitions.retain(|_, entry| entry.file != file);
        match after {
            After::Gone => {
                self.files.insert(file, Known::missing());
            }
            After::Text { text, read } => {
                for (item, definition) in read.definitions.into_iter().enumerate() {
                    let entry = Entry {
                        definition,
                        file: file.clone(),
                        item,
                    };
                    self.definitions.insert(entry.definition.name().clone(), entry);
                }
                self.files.insert(
                    file,
                    Known {
                        fingerprint: Some(Fingerprint::of(text.as_bytes())),
                        document: read.document,
                    },
                );
            }
        }
    }

    /// The place of the definition of this name among the items of its file. The store holds the name.
    fn item_of(&self, name: &D::Name) -> usize {
        self.definitions.get(name).map_or(0, |entry| entry.item)
    }

    /// The definitions of a file, in the order of its items.
    fn definitions_of(&self, file: &Path) -> Vec<D> {
        let mut entries: Vec<&Entry<D>> = self.definitions.values().filter(|entry| entry.file == file).collect();
        entries.sort_by_key(|entry| entry.item);
        entries.into_iter().map(|entry| entry.definition.clone()).collect()
    }
}

impl Known {
    fn missing() -> Self {
        Self {
            fingerprint: None,
            document: Document::Nothing,
        }
    }
}

/// What changed in a file since the store read it, or `None` when it holds the bytes that the store read.
///
/// `expected` is what the store holds of the file, and `found` what is there now. `None` is a file that is not there.
pub(crate) fn changed_outside(file: &Path, expected: Option<Fingerprint>, found: Option<&[u8]>) -> Option<String> {
    let file = file.display();
    match (expected, found) {
        (Some(_), None) => Some(format!("{file} is gone since the store read it")),
        (None, Some(_)) => Some(format!("{file} exists now, and did not when the store read its files")),
        (Some(expected), Some(bytes)) if Fingerprint::of(bytes) != expected => {
            Some(format!("{file} changed since the store read it"))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use gaslightmq_selector::Selector;

    use super::*;

    const SELECTORS: &str = "\
- name: fetches
  match:
    api: Fetch   # every fetch

- name: produces
  match:
    api: Produce
";

    fn held() -> Held<Selector> {
        let found = vec![(PathBuf::from("selectors.yaml"), Some(SELECTORS.as_bytes().to_vec()))];
        Held::read(found).expect("the selectors read")
    }

    #[test]
    fn a_store_says_which_definition_holds_a_comment() {
        let held = held();
        let [fetches, produces] = held.list().try_into().expect("two selectors");

        assert!(held.would_lose_comment(fetches.name()));
        assert!(!held.would_lose_comment(produces.name()));
    }

    #[test]
    fn a_name_that_an_earlier_item_has_does_not_read() {
        let twice = format!("{SELECTORS}- name: fetches\n  match:\n    api: Metadata\n");
        let found = vec![(PathBuf::from("selectors.yaml"), Some(twice.into_bytes()))];

        let errors = Held::<Selector>::read(found).expect_err("a name twice");

        assert_eq!(
            errors,
            [FileError {
                file: "selectors.yaml".into(),
                reason: "[2].name: selector \"fetches\" is in selectors.yaml already".to_owned(),
            }]
        );
    }

    #[test]
    fn a_file_counts_as_changed_when_its_bytes_or_its_presence_differ() {
        let file = Path::new("selectors.yaml");
        let read = Fingerprint::of(SELECTORS.as_bytes());

        assert_eq!(changed_outside(file, Some(read), Some(SELECTORS.as_bytes())), None);
        assert_eq!(changed_outside(file, None, None), None);
        assert_eq!(
            changed_outside(file, Some(read), Some(b"[]\n")),
            Some("selectors.yaml changed since the store read it".to_owned())
        );
        assert_eq!(
            changed_outside(file, Some(read), None),
            Some("selectors.yaml is gone since the store read it".to_owned())
        );
        assert_eq!(
            changed_outside(file, None, Some(b"[]\n")),
            Some("selectors.yaml exists now, and did not when the store read its files".to_owned())
        );
    }
}
