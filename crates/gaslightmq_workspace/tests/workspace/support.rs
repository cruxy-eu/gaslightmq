//! What the tests share: the definitions of one workspace, and stores that fail with the context over them.

use std::future::ready;

use gaslightmq_clock::TimeSpan;
use gaslightmq_definition_api::prelude::*;
use gaslightmq_definition_memory::{KnownUuids, MemoryStore};
use gaslightmq_environment::prelude::*;
use gaslightmq_field::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_lie::prelude::*;
use gaslightmq_rule::prelude::*;
use gaslightmq_selector::prelude::*;
use gaslightmq_surface::prelude::*;
use gaslightmq_workspace::prelude::*;

/// The workspace of an order desk, as someone wrote it: no definition has a UUID yet.
pub struct OrderDesk;

impl OrderDesk {
    /// The selector `order-producers`: every Produce to a topic of orders.
    pub fn order_producers() -> Selector {
        let conditions = Conditions::new()
            .with(Condition::is(ApiKey::PRODUCE))
            .with(Condition::parse(Field::Topic, "orders.*").unwrap());
        Selector::new("order-producers".parse().unwrap(), conditions).with_title("Order producers".parse().unwrap())
    }

    /// The selector `order-producers-from-ingest`: it includes `order-producers`, for one client.
    pub fn order_producers_from_ingest() -> Selector {
        let ingest = Condition::is(ClientId::new("order-ingest").unwrap());
        Selector::new("order-producers-from-ingest".parse().unwrap(), ingest.into())
            .including("order-producers".parse().unwrap())
    }

    /// The selector `answered`: every call that the backend answered without an error.
    /// A rule with it only knows its calls once the truth is there.
    pub fn answered() -> Selector {
        Selector::new("answered".parse().unwrap(), Self::answered_conditions())
    }

    /// The conditions of [`answered`](Self::answered).
    pub fn answered_conditions() -> Conditions {
        Conditions::new().with(Condition::parse(Field::Outcome, "ok").unwrap())
    }

    /// The preset `delay-1s`: a delay of one second.
    pub fn delay_1s() -> Preset {
        let delay = TimingLie::new()
            .with_delay(TimeSpan::from_secs(1))
            .with_jitter(TimeSpan::from_millis(200));
        Preset::new("delay-1s".parse().unwrap(), delay).with_title("Delay 1 s".parse().unwrap())
    }

    /// The preset `blackhole`: a silence, which acts before the backend answers.
    pub fn blackhole() -> Preset {
        Preset::new("blackhole".parse().unwrap(), LieDefinition::Silence)
    }

    /// The rule `slow-order-producers`: the selector `order-producers` with the preset `delay-1s`.
    pub fn slow_order_producers() -> Rule {
        Rule::new(
            "slow-order-producers".parse().unwrap(),
            "order-producers".parse::<SelectorName>().unwrap(),
            "delay-1s".parse::<PresetName>().unwrap(),
        )
        .with_title("Slow down order producers".parse().unwrap())
    }

    /// The rule `blackhole-metadata`, which needs no other definition:
    /// its selector and its action stand in the rule itself.
    pub fn blackhole_metadata() -> Rule {
        Rule::new(
            "blackhole-metadata".parse().unwrap(),
            Conditions::new().with(Condition::is(ApiKey::METADATA)),
            LieDefinition::Silence,
        )
    }

    /// The pad layout `order-desk`: a row for each selector of orders, and a column for each preset.
    pub fn layout() -> PadLayout {
        PadLayout::new(
            "order-desk".parse().unwrap(),
            [
                "order-producers".parse().unwrap(),
                "order-producers-from-ingest".parse().unwrap(),
            ],
            ["delay-1s".parse().unwrap(), "blackhole".parse().unwrap()],
            [
                Layer::all_clients(),
                Layer::client(ClientId::new("order-ingest").unwrap()),
            ],
        )
        .unwrap()
        .with_title("Order desk".parse().unwrap())
    }

    /// The environment `orders-sim`: a proxy in front of a broker.
    pub fn environment() -> Environment {
        let listen = Listeners::new(
            "0.0.0.0:29092".parse().unwrap(),
            "0.0.0.0:29093-29192".parse().unwrap(),
            "localhost".parse().unwrap(),
        )
        .unwrap();
        Environment::new(
            "orders-sim".parse().unwrap(),
            Instances::ProxyInFrontOfBroker {
                proxy: ProxyDefinition::new(listen),
                broker: BrokerDefinition::new("orders-sim".parse().unwrap()),
            },
        )
    }
}

/// The context of a workspace over the stores of a test, with UUIDs that the test knows before they are made.
///
/// A test whose stores all work takes the context of the memory crate.
/// This one is for the stores below, of which one fails.
pub struct Context<S> {
    stores: S,
    uuids: KnownUuids,
}

impl<S: Stores> Context<S> {
    /// The context over those stores, which has made no UUID yet.
    pub fn over(stores: S) -> Self {
        Self {
            stores,
            uuids: KnownUuids::new(),
        }
    }
}

impl<S: Stores> WorkspaceContext for Context<S> {
    type Stores = S;
    type Uuids = KnownUuids;

    fn stores(&self) -> &S {
        &self.stores
    }

    fn uuids(&mut self) -> &mut KnownUuids {
        &mut self.uuids
    }
}

/// The stores of a workspace whose presets are on a medium that takes no change. Every other store is in memory.
pub struct ReadOnlyPresets {
    selectors: MemoryStore<Selector>,
    presets: ReadOnly<Preset>,
    rules: MemoryStore<Rule>,
    layouts: MemoryStore<PadLayout>,
    environments: MemoryStore<Environment>,
}

impl ReadOnlyPresets {
    /// The stores with those presets on the medium, and no other definition.
    pub fn holding(presets: MemoryStore<Preset>) -> Self {
        Self {
            selectors: MemoryStore::new(),
            presets: ReadOnly(presets),
            rules: MemoryStore::new(),
            layouts: MemoryStore::new(),
            environments: MemoryStore::new(),
        }
    }
}

impl Stores for ReadOnlyPresets {
    type Selectors = MemoryStore<Selector>;
    type Presets = ReadOnly<Preset>;
    type Rules = MemoryStore<Rule>;
    type Layouts = MemoryStore<PadLayout>;
    type Environments = MemoryStore<Environment>;

    fn selectors(&self) -> &MemoryStore<Selector> {
        &self.selectors
    }

    fn presets(&self) -> &ReadOnly<Preset> {
        &self.presets
    }

    fn rules(&self) -> &MemoryStore<Rule> {
        &self.rules
    }

    fn layouts(&self) -> &MemoryStore<PadLayout> {
        &self.layouts
    }

    fn environments(&self) -> &MemoryStore<Environment> {
        &self.environments
    }
}

/// The stores of a workspace whose rules cannot be read. Every other store is in memory, and holds nothing.
#[derive(Default)]
pub struct UnreadableRules {
    selectors: MemoryStore<Selector>,
    presets: MemoryStore<Preset>,
    rules: Unreadable,
    layouts: MemoryStore<PadLayout>,
    environments: MemoryStore<Environment>,
}

impl UnreadableRules {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Stores for UnreadableRules {
    type Selectors = MemoryStore<Selector>;
    type Presets = MemoryStore<Preset>;
    type Rules = Unreadable;
    type Layouts = MemoryStore<PadLayout>;
    type Environments = MemoryStore<Environment>;

    fn selectors(&self) -> &MemoryStore<Selector> {
        &self.selectors
    }

    fn presets(&self) -> &MemoryStore<Preset> {
        &self.presets
    }

    fn rules(&self) -> &Unreadable {
        &self.rules
    }

    fn layouts(&self) -> &MemoryStore<PadLayout> {
        &self.layouts
    }

    fn environments(&self) -> &MemoryStore<Environment> {
        &self.environments
    }
}

/// A store whose definitions are on a medium that takes no change: it reads, and it keeps nothing new.
pub struct ReadOnly<D: Definition>(MemoryStore<D>);

impl<D: Definition> ReadOnly<D> {
    /// What the medium says to every change.
    pub const REASON: &'static str = "the directory is read-only";
}

impl<D: Definition> Store<D> for ReadOnly<D> {
    async fn list(&self) -> Result<Vec<D>, StoreReadError> {
        self.0.list().await
    }

    async fn get(&self, name: &D::Name) -> Result<Option<D>, StoreReadError> {
        self.0.get(name).await
    }

    // The medium answers at once, so the changes return a future that is ready.
    fn create(&self, _definition: D) -> impl Future<Output = Result<(), StoreCreateError>> + Send {
        ready(Err(StoreCreateError::Failed {
            reason: Self::REASON.to_owned(),
        }))
    }

    fn update(&self, _definition: D) -> impl Future<Output = Result<(), StoreChangeError>> + Send {
        ready(Err(StoreChangeError::Failed {
            reason: Self::REASON.to_owned(),
        }))
    }

    fn delete(&self, _name: &D::Name) -> impl Future<Output = Result<(), StoreChangeError>> + Send {
        ready(Err(StoreChangeError::Failed {
            reason: Self::REASON.to_owned(),
        }))
    }
}

impl PresetStore for ReadOnly<Preset> {}

/// A store of rules that cannot read what it holds, so nobody knows what is in it.
#[derive(Default)]
pub struct Unreadable;

impl Unreadable {
    /// What stands in the way of every read.
    pub const REASON: &'static str = "permission denied";

    fn failed() -> StoreReadError {
        StoreReadError::Failed {
            reason: Self::REASON.to_owned(),
        }
    }
}

impl Store<Rule> for Unreadable {
    fn list(&self) -> impl Future<Output = Result<Vec<Rule>, StoreReadError>> + Send {
        ready(Err(Self::failed()))
    }

    fn get(&self, _name: &RuleName) -> impl Future<Output = Result<Option<Rule>, StoreReadError>> + Send {
        ready(Err(Self::failed()))
    }

    fn create(&self, _rule: Rule) -> impl Future<Output = Result<(), StoreCreateError>> + Send {
        ready(Err(StoreCreateError::Failed {
            reason: Self::REASON.to_owned(),
        }))
    }

    fn update(&self, _rule: Rule) -> impl Future<Output = Result<(), StoreChangeError>> + Send {
        ready(Err(StoreChangeError::Failed {
            reason: Self::REASON.to_owned(),
        }))
    }

    fn delete(&self, _name: &RuleName) -> impl Future<Output = Result<(), StoreChangeError>> + Send {
        ready(Err(StoreChangeError::Failed {
            reason: Self::REASON.to_owned(),
        }))
    }
}

impl RuleStore for Unreadable {
    fn using_selector(
        &self,
        _selector: &SelectorName,
    ) -> impl Future<Output = Result<Vec<Rule>, StoreReadError>> + Send {
        ready(Err(Self::failed()))
    }

    fn using_preset(&self, _preset: &PresetName) -> impl Future<Output = Result<Vec<Rule>, StoreReadError>> + Send {
        ready(Err(Self::failed()))
    }
}
