use std::collections::BTreeMap;
use std::collections::btree_map;

use crate::address::ListenAddress;
use crate::environment::Environment;
use crate::name::EnvironmentName;

/// The environments of a workspace by name.
///
/// A set holds together: every name is there once.
///
/// Two environments of a set can name the same port, as long as no process starts both.
/// [`to_start`](Self::to_start) checks the ones that a process starts together.
///
/// ```
/// use gaslightmq_environment::prelude::*;
///
/// let name = |name: &str| name.parse::<EnvironmentName>().expect("an environment name");
/// let broker = |environment: &str, bootstrap: &str, brokers: &str| {
///     let bootstrap = bootstrap.parse().expect("a listen address");
///     let brokers = brokers.parse().expect("broker listeners");
///     let listen = Listeners::new(bootstrap, brokers, "localhost".parse().expect("a host name"))
///         .expect("a bootstrap port outside the ports of the brokers");
///     let broker = BrokerDefinition::new("shop".parse().expect("a world name"));
///     Environment::new(name(environment), Instances::BrokerAlone { broker, listen })
/// };
/// let set = EnvironmentSet::new([
///     broker("shop", "0.0.0.0:9092", "0.0.0.0:19092-19094"),
///     broker("tests", "0.0.0.0:29092", "0.0.0.0:29093-29095"),
///     broker("demo", "0.0.0.0:9092", "0.0.0.0:39093-39095"),
/// ])?;
/// let [shop, tests, demo] = ["shop", "tests", "demo"].map(name);
///
/// // `shop` and `tests` listen on different ports, so one process starts both.
/// assert_eq!(set.to_start([&shop, &tests])?.len(), 2);
///
/// // `shop` and `demo` both listen on port 9092.
/// let error = set.to_start([&shop, &demo]).unwrap_err();
/// assert_eq!(error.to_string(), "environments \"shop\" and \"demo\" both listen on 0.0.0.0:9092");
/// # Ok::<(), EnvironmentSetError>(())
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EnvironmentSet {
    by_name: BTreeMap<EnvironmentName, Environment>,
}

impl EnvironmentSet {
    /// The set of those environments.
    ///
    /// # Errors
    ///
    /// [`EnvironmentSetError::DuplicateName`] when two environments have one name.
    pub fn new(environments: impl IntoIterator<Item = Environment>) -> Result<Self, EnvironmentSetError> {
        let mut by_name = BTreeMap::new();
        for environment in environments {
            if let Some(twice) = by_name.insert(environment.name().clone(), environment) {
                return Err(EnvironmentSetError::DuplicateName {
                    name: twice.name().clone(),
                });
            }
        }
        Ok(Self { by_name })
    }

    /// The environment of this name.
    pub fn get(&self, name: &EnvironmentName) -> Option<&Environment> {
        self.by_name.get(name)
    }

    /// Whether an environment has this name.
    pub fn contains(&self, name: &EnvironmentName) -> bool {
        self.by_name.contains_key(name)
    }

    /// The environments in the order of their names.
    pub fn iter(&self) -> btree_map::Values<'_, EnvironmentName, Environment> {
        self.by_name.values()
    }

    /// How many environments there are.
    pub fn len(&self) -> usize {
        self.by_name.len()
    }

    /// Whether there is no environment.
    pub fn is_empty(&self) -> bool {
        self.by_name.is_empty()
    }

    /// The environments of these names, which one process can start together.
    ///
    /// The environments come in the order of `names`, and a name that is given twice is one environment.
    /// A process that already runs some environments names those too, next to the one it is about to start.
    ///
    /// # Errors
    ///
    /// - [`EnvironmentSetError::UnknownEnvironment`] when no environment has one of the names.
    /// - [`EnvironmentSetError::SharedListener`] when two of the environments would listen on the same port of the
    ///   same address.
    pub fn to_start<'a>(
        &self,
        names: impl IntoIterator<Item = &'a EnvironmentName>,
    ) -> Result<Vec<&Environment>, EnvironmentSetError> {
        let mut together: Vec<&Environment> = Vec::new();
        for name in names {
            let environment = self
                .get(name)
                .ok_or_else(|| EnvironmentSetError::UnknownEnvironment { name: name.clone() })?;
            if together.iter().any(|started| started.name() == name) {
                continue;
            }
            for started in &together {
                if let Some(address) = started.shared_listener(environment) {
                    return Err(EnvironmentSetError::SharedListener {
                        first: started.name().clone(),
                        second: name.clone(),
                        address,
                    });
                }
            }
            together.push(environment);
        }
        Ok(together)
    }
}

impl<'a> IntoIterator for &'a EnvironmentSet {
    type Item = &'a Environment;
    type IntoIter = btree_map::Values<'a, EnvironmentName, Environment>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl IntoIterator for EnvironmentSet {
    type Item = Environment;
    type IntoIter = btree_map::IntoValues<EnvironmentName, Environment>;

    fn into_iter(self) -> Self::IntoIter {
        self.by_name.into_values()
    }
}

/// Why environments are not an [`EnvironmentSet`], or why a set cannot do what was asked.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EnvironmentSetError {
    /// Two environments have this name.
    #[error("two environments are named {:?}", .name.as_str())]
    DuplicateName { name: EnvironmentName },
    /// No environment has this name.
    #[error("no environment is named {:?}", .name.as_str())]
    UnknownEnvironment { name: EnvironmentName },
    /// Two environments that start together would listen on the same port of the same address.
    #[error("environments {:?} and {:?} both listen on {address}", .first.as_str(), .second.as_str())]
    SharedListener {
        first: EnvironmentName,
        second: EnvironmentName,
        address: ListenAddress,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::environment::{BrokerDefinition, Instances};
    use crate::listeners::Listeners;
    use crate::name::WorldName;

    fn name(text: &str) -> EnvironmentName {
        text.parse().unwrap()
    }

    fn world(text: &str) -> WorldName {
        text.parse().unwrap()
    }

    /// A broker alone, on a bootstrap port and the three ports after it.
    fn environment(text: &str, port: u16) -> Environment {
        let listen = Listeners::new(
            format!("0.0.0.0:{port}").parse().unwrap(),
            format!("0.0.0.0:{}-{}", port + 1, port + 3).parse().unwrap(),
            "localhost".parse().unwrap(),
        )
        .unwrap();
        let broker = BrokerDefinition::new(world(text));
        Environment::new(name(text), Instances::BrokerAlone { broker, listen })
    }

    fn set() -> EnvironmentSet {
        EnvironmentSet::new([
            environment("shop", 9092),
            environment("staging", 19092),
            environment("demo", 9094),
        ])
        .unwrap()
    }

    fn names(environments: &[&Environment]) -> Vec<String> {
        environments
            .iter()
            .map(|environment| environment.name().to_string())
            .collect()
    }

    #[test]
    fn a_set_finds_its_environments_by_name() {
        let set = set();

        assert_eq!(set.len(), 3);
        assert!(!set.is_empty());
        assert!(set.contains(&name("shop")));
        assert_eq!(set.get(&name("staging")), Some(&environment("staging", 19092)));
        assert_eq!(set.get(&name("tests")), None);
        assert_eq!(names(&set.iter().collect::<Vec<_>>()), ["demo", "shop", "staging"]);
        assert_eq!((&set).into_iter().count(), 3);
        assert_eq!(set.into_iter().count(), 3);
        assert!(EnvironmentSet::default().is_empty());
    }

    #[test]
    fn two_environments_with_one_name_are_an_error() {
        let error = EnvironmentSet::new([environment("shop", 9092), environment("shop", 19092)]).unwrap_err();

        assert_eq!(error, EnvironmentSetError::DuplicateName { name: name("shop") });
        assert_eq!(error.to_string(), "two environments are named \"shop\"");
    }

    #[test]
    fn a_process_starts_the_environments_whose_listeners_do_not_meet() {
        let set = set();
        let (shop, staging, demo) = (name("shop"), name("staging"), name("demo"));

        assert_eq!(names(&set.to_start([&staging, &shop]).unwrap()), ["staging", "shop"]);
        assert_eq!(names(&set.to_start([&demo, &staging]).unwrap()), ["demo", "staging"]);
        assert_eq!(names(&set.to_start([&shop]).unwrap()), ["shop"]);
        assert_eq!(names(&set.to_start([]).unwrap()), Vec::<String>::new());
        // A name that is given twice is one environment, and it does not share its ports with itself.
        assert_eq!(
            names(&set.to_start([&shop, &staging, &shop]).unwrap()),
            ["shop", "staging"]
        );
    }

    #[test]
    fn a_process_refuses_two_environments_that_listen_on_the_same_port() {
        let set = set();
        let (shop, staging, demo) = (name("shop"), name("staging"), name("demo"));

        // `shop` has its brokers on 9093 to 9095, and `demo` has them on 9095 to 9097.
        let error = set.to_start([&staging, &shop, &demo]).unwrap_err();
        assert_eq!(
            error,
            EnvironmentSetError::SharedListener {
                first: shop,
                second: demo,
                address: "0.0.0.0:9095".parse().unwrap()
            }
        );
        assert_eq!(
            error.to_string(),
            "environments \"shop\" and \"demo\" both listen on 0.0.0.0:9095"
        );
    }

    #[test]
    fn a_name_of_no_environment_is_an_error() {
        let error = set().to_start([&name("shop"), &name("tests")]).unwrap_err();

        assert_eq!(error, EnvironmentSetError::UnknownEnvironment { name: name("tests") });
        assert_eq!(error.to_string(), "no environment is named \"tests\"");
    }
}
