use gaslightmq_rule::{Preset, PresetName, PresetSet, PresetTitle, Rule, RuleName, RuleTitle};
use gaslightmq_selector::{Selector, SelectorName, SelectorSet, SelectorTitle};

use crate::{Column, Layer, LayoutName, LayoutTitle, Pad, Row};

/// A pad layout: a grid that maps rows to selectors, columns to presets and layers to scopes.
///
/// | Part | Meaning | When left out |
/// |---|---|---|
/// | Name | The identity of the layout: unique among layouts. A [`Pad`] names its layout by it. | |
/// | Title | What the layout is for, in free text. | No title. |
/// | Rows | Selectors, by name. | |
/// | Columns | Presets, by name. | |
/// | Layers | Scopes. Each [`Layer`] is a page of the grid. | |
///
/// ```yaml
/// name: order-desk
/// title: Order desk
/// rows: [order-producers, fetch, metadata]
/// columns: [error-retriable, delay-1s, blackhole]
/// layers: [All clients, order-ingest]
/// ```
///
/// A layout is a definition of the workspace, and the only state a surface owns.
/// It only maps positions to names, and it holds no rule.
/// It can name a selector or a preset that does not exist, and whoever holds the whole workspace checks that.
///
/// # What always holds
///
/// - The layout has at least one layer, so a surface always shows one.
/// - No layer, no selector of a row and no preset of a column stands in the layout twice.
///
/// A layout has as many rows and columns as it lists.
/// A surface with a grid of a fixed size shows the part that fits.
///
/// ```
/// use gaslightmq_lie::prelude::*;
/// use gaslightmq_rule::prelude::*;
/// use gaslightmq_selector::prelude::*;
/// use gaslightmq_surface::prelude::*;
///
/// // A workspace has selectors and presets, each with a name and a title.
/// let order_producers = Selector::new("order-producers".parse().expect("a selector name"), Conditions::new())
///     .with_title("Order producers".parse().expect("a selector title"));
/// let blackhole = Preset::new("blackhole".parse().expect("a preset name"), LieDefinition::Silence)
///     .with_title("Blackhole".parse().expect("a preset title"));
/// let ingest: Layer = "order-ingest".parse().expect("a layer");
///
/// let layout = PadLayout::new(
///     "order-desk".parse().expect("a layout name"),
///     [order_producers.name().clone()],
///     [blackhole.name().clone()],
///     [Layer::all_clients(), ingest.clone()],
/// )?
/// .with_title("Order desk".parse().expect("a layout title"));
///
/// // An empty pad creates a live rule from its row, its column and its layer.
/// let pad = layout
///     .pad(&ingest, Row::from_index(0), Column::from_index(0))
///     .expect("the layout has this pad");
/// let selectors = SelectorSet::new([order_producers.clone()]).expect("a selector set");
/// let presets = PresetSet::new([blackhole.clone()]).expect("a preset set");
/// let rule = layout.rule(&pad, &selectors, &presets).expect("the layout has this pad");
///
/// assert_eq!(rule.name(), "blackhole-on-order-producers-for-order-ingest");
/// assert_eq!(rule.title().map(RuleTitle::as_str), Some("Blackhole on Order producers for order-ingest"));
/// assert_eq!(rule.selector(), &SelectorRef::Named(order_producers.name().clone()));
/// assert_eq!(rule.action(), &ActionRef::Preset(blackhole.name().clone()));
/// assert_eq!(rule.scope(), ingest.scope());
/// # Ok::<(), PadLayoutError>(())
/// ```
///
/// With the `serde` feature, a layout serializes as the map above.
/// Writing leaves out a title that the layout does not have, and reading rejects a key it does not know.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(try_from = "crate::serde::LayoutMap", into = "crate::serde::LayoutMap")
)]
pub struct PadLayout {
    name: LayoutName,
    title: Option<LayoutTitle>,
    rows: Vec<SelectorName>,
    columns: Vec<PresetName>,
    layers: Vec<Layer>,
}

impl PadLayout {
    /// A layout of those rows, columns and layers, each in the order a surface shows them. It has no title.
    ///
    /// # Errors
    ///
    /// - [`PadLayoutError::NoLayer`] when `layers` is empty.
    /// - [`PadLayoutError::DuplicateLayer`] when two layers have the same scope.
    /// - [`PadLayoutError::DuplicateRow`] when two rows name the same selector.
    /// - [`PadLayoutError::DuplicateColumn`] when two columns name the same preset.
    pub fn new(
        name: LayoutName,
        rows: impl IntoIterator<Item = SelectorName>,
        columns: impl IntoIterator<Item = PresetName>,
        layers: impl IntoIterator<Item = Layer>,
    ) -> Result<Self, PadLayoutError> {
        let rows: Vec<SelectorName> = rows.into_iter().collect();
        let columns: Vec<PresetName> = columns.into_iter().collect();
        let layers: Vec<Layer> = layers.into_iter().collect();

        if layers.is_empty() {
            return Err(PadLayoutError::NoLayer { layout: name });
        }
        if let Some(layer) = first_repeated(&layers) {
            return Err(PadLayoutError::DuplicateLayer {
                layout: name,
                layer: layer.clone(),
            });
        }
        if let Some(selector) = first_repeated(&rows) {
            return Err(PadLayoutError::DuplicateRow {
                layout: name,
                selector: selector.clone(),
            });
        }
        if let Some(preset) = first_repeated(&columns) {
            return Err(PadLayoutError::DuplicateColumn {
                layout: name,
                preset: preset.clone(),
            });
        }
        Ok(Self {
            name,
            title: None,
            rows,
            columns,
            layers,
        })
    }

    /// The same layout with a title, which says what the layout is for.
    /// It replaces the title the layout had before.
    #[must_use]
    pub fn with_title(self, title: LayoutTitle) -> Self {
        Self {
            title: Some(title),
            ..self
        }
    }

    /// The same layout without a title.
    #[must_use]
    pub fn without_title(self) -> Self {
        Self { title: None, ..self }
    }

    /// The name of the layout: its identity, unique among layouts.
    pub const fn name(&self) -> &LayoutName {
        &self.name
    }

    /// What the layout is for, in free text, when someone wrote it down.
    pub const fn title(&self) -> Option<&LayoutTitle> {
        self.title.as_ref()
    }

    /// The selector of every row, from the first row on.
    pub fn rows(&self) -> &[SelectorName] {
        &self.rows
    }

    /// The preset of every column, from the first column on.
    pub fn columns(&self) -> &[PresetName] {
        &self.columns
    }

    /// The layers, in the order a surface offers them. There is at least one.
    pub fn layers(&self) -> &[Layer] {
        &self.layers
    }

    /// The layer a surface shows first.
    pub fn first_layer(&self) -> &Layer {
        match self.layers.first() {
            Some(layer) => layer,
            None => unreachable!("a layout has at least one layer"),
        }
    }

    /// Whether the layout has that layer.
    pub fn has_layer(&self, layer: &Layer) -> bool {
        self.layers.contains(layer)
    }

    /// The selector of that row, or `None` when the layout has no such row.
    pub fn selector(&self, row: Row) -> Option<&SelectorName> {
        self.rows.get(row.index())
    }

    /// The preset of that column, or `None` when the layout has no such column.
    pub fn preset(&self, column: Column) -> Option<&PresetName> {
        self.columns.get(column.index())
    }

    /// The row of that selector, or `None` when no row names it.
    pub fn row_of(&self, selector: &SelectorName) -> Option<Row> {
        self.rows.iter().position(|row| row == selector).map(Row::from_index)
    }

    /// The column of that preset, or `None` when no column names it.
    pub fn column_of(&self, preset: &PresetName) -> Option<Column> {
        self.columns
            .iter()
            .position(|column| column == preset)
            .map(Column::from_index)
    }

    /// The pad at that row and column on that layer, or `None` when the layout has no pad there.
    pub fn pad(&self, layer: &Layer, row: Row, column: Column) -> Option<Pad> {
        let pad = Pad::new(self.name.clone(), layer.clone(), row, column);
        self.holds(&pad).then_some(pad)
    }

    /// Whether the pad is one of this layout: it names the layout, and stands on a layer, a row and a column it has.
    pub fn holds(&self, pad: &Pad) -> bool {
        self.parts(pad).is_some()
    }

    /// The name of the rule that the pad points at, or `None` when the pad is none of this layout.
    ///
    /// The name is generated from the preset of the column, the selector of the row and the text of the layer:
    /// `delay-1s-on-order-producers-for-order-ingest`.
    /// A rule keeps its name for as long as it exists, so the pad finds its rule by it, whatever its title becomes.
    ///
    /// The name follows the rule for names.
    /// The text of a layer goes into it as a name can hold it: its letters in lower case and its digits,
    /// with one hyphen where the text has anything else.
    /// A name that is too long is cut, so two pads whose names only differ after the cut point at the same rule.
    pub fn rule_name(&self, pad: &Pad) -> Option<RuleName> {
        let (selector, preset, layer) = self.parts(pad)?;
        let mut name = format!("{preset}-on-{selector}-for-{}", as_a_name_holds(&layer.to_string()));
        // Every character of a name is one byte.
        name.truncate(RuleName::MAX_LENGTH);
        match RuleName::new(name.trim_end_matches('-')) {
            Ok(name) => Some(name),
            Err(error) => unreachable!("the start of names with hyphens between them is a name: {error}"),
        }
    }

    /// The live rule that pressing the empty pad creates, or `None` when the pad is none of this layout.
    ///
    /// The rule has [the name of the pad](Self::rule_name).
    /// It names the selector of the row and the preset of the column, and it applies to the scope of the layer.
    /// It fires on every matching call, and stays armed until someone disarms it.
    ///
    /// Its title is generated too: *Delay 1 s on Order producers for order-ingest*.
    /// `selectors` and `presets` give the titles of the selector and of the preset.
    /// One that has no title, or that they do not hold, stands in the title of the rule with its name.
    /// The rule has no title when the text of the layer makes one that breaks the rule for titles.
    /// Someone can change the title afterwards, and the name stays.
    pub fn rule(&self, pad: &Pad, selectors: &SelectorSet, presets: &PresetSet) -> Option<Rule> {
        let (selector, preset, layer) = self.parts(pad)?;
        let rule = Rule::new(self.rule_name(pad)?, selector.clone(), preset.clone()).scoped(layer.scope().clone());

        let of_selector = selectors
            .get(selector)
            .and_then(Selector::title)
            .map_or(selector.as_str(), SelectorTitle::as_str);
        let of_preset = presets
            .get(preset)
            .and_then(Preset::title)
            .map_or(preset.as_str(), PresetTitle::as_str);
        Some(
            match RuleTitle::new(format!("{of_preset} on {of_selector} for {layer}")) {
                Ok(title) => rule.with_title(title),
                Err(_) => rule,
            },
        )
    }

    /// The selector, the preset and the layer that the pad stands on, when the pad is one of this layout.
    fn parts<'a>(&'a self, pad: &'a Pad) -> Option<(&'a SelectorName, &'a PresetName, &'a Layer)> {
        if pad.layout() != &self.name || !self.has_layer(pad.layer()) {
            return None;
        }
        Some((self.selector(pad.row())?, self.preset(pad.column())?, pad.layer()))
    }

    /// The parts of the layout, as a file writes them.
    #[cfg(feature = "serde")]
    pub(crate) fn into_parts(
        self,
    ) -> (
        LayoutName,
        Option<LayoutTitle>,
        Vec<SelectorName>,
        Vec<PresetName>,
        Vec<Layer>,
    ) {
        (self.name, self.title, self.rows, self.columns, self.layers)
    }
}

/// The first item that stands in the list a second time.
fn first_repeated<T: Ord>(items: &[T]) -> Option<&T> {
    let mut seen = std::collections::BTreeSet::new();
    items.iter().find(|item| !seen.insert(*item))
}

/// The text as a name can hold it: its letters in lower case and its digits,
/// with one hyphen where the text has anything else, and none at either end.
fn as_a_name_holds(text: &str) -> String {
    let mut held = String::new();
    for character in text.chars() {
        if character.is_ascii_alphanumeric() {
            held.push(character.to_ascii_lowercase());
        } else if !held.is_empty() && !held.ends_with('-') {
            held.push('-');
        }
    }
    held.truncate(held.trim_end_matches('-').len());
    held
}

/// Why rows, columns and layers are not a [`PadLayout`].
///
/// ```
/// use gaslightmq_surface::prelude::*;
///
/// let name: LayoutName = "order-desk".parse().expect("a layout name");
/// let error = PadLayout::new(name.clone(), [], [], []).unwrap_err();
/// assert_eq!(error, PadLayoutError::NoLayer { layout: name });
/// assert_eq!(error.to_string(), "layout \"order-desk\" has no layer");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PadLayoutError {
    /// The layout has no layer, so a surface would have no page to show.
    #[error("layout {:?} has no layer", .layout.as_str())]
    NoLayer { layout: LayoutName },
    /// Two layers have the same scope, so a pad on one would be a pad on the other.
    #[error("layout {:?} has the layer {:?} twice", .layout.as_str(), .layer.to_string())]
    DuplicateLayer { layout: LayoutName, layer: Layer },
    /// Two rows name the same selector, so their pads would create the same rules.
    #[error("layout {:?} has two rows for selector {:?}", .layout.as_str(), .selector.as_str())]
    DuplicateRow { layout: LayoutName, selector: SelectorName },
    /// Two columns name the same preset, so their pads would create the same rules.
    #[error("layout {:?} has two columns for preset {:?}", .layout.as_str(), .preset.as_str())]
    DuplicateColumn { layout: LayoutName, preset: PresetName },
}

#[cfg(test)]
mod tests {
    use gaslightmq_field::Endpoint;
    use gaslightmq_kernel::prelude::*;
    use gaslightmq_lie::LieDefinition;
    use gaslightmq_rule::ActionRef;
    use gaslightmq_selector::{Conditions, Scope, SelectorRef};
    use gaslightmq_trigger::Lifetime;

    use super::*;

    /// The layout of an order desk, with a few of the rows, columns and layers of the layout in the spec.
    struct OrderDesk;

    impl OrderDesk {
        const NAME: &'static str = "order-desk";
        const ROWS: [&'static str; 3] = ["order-producers", "fetch", "metadata"];
        const COLUMNS: [&'static str; 4] = ["error-retriable", "delay-100ms", "delay-1s", "blackhole"];
        const CLIENTS: [&'static str; 2] = ["order-ingest", "payments-svc"];

        fn name() -> LayoutName {
            Self::NAME.parse().unwrap()
        }

        fn rows() -> Vec<SelectorName> {
            Self::ROWS.iter().map(|row| row.parse().unwrap()).collect()
        }

        fn columns() -> Vec<PresetName> {
            Self::COLUMNS.iter().map(|column| column.parse().unwrap()).collect()
        }

        fn layers() -> Vec<Layer> {
            let clients = Self::CLIENTS.iter().map(|client| client.parse().unwrap());
            std::iter::once(Layer::all_clients()).chain(clients).collect()
        }

        fn layout() -> PadLayout {
            PadLayout::new(Self::name(), Self::rows(), Self::columns(), Self::layers()).unwrap()
        }

        fn order_producers() -> SelectorName {
            Self::ROWS[0].parse().unwrap()
        }

        fn delay_1s() -> PresetName {
            Self::COLUMNS[2].parse().unwrap()
        }

        fn order_ingest() -> Layer {
            Self::CLIENTS[0].parse().unwrap()
        }

        /// The selectors of the workspace: `order-producers` has a title, and the others have none.
        fn selectors() -> SelectorSet {
            let titled = Selector::new(Self::order_producers(), Conditions::new())
                .with_title("Order producers".parse().unwrap());
            let untitled = Self::rows()
                .into_iter()
                .skip(1)
                .map(|name| Selector::new(name, Conditions::new()));
            SelectorSet::new(std::iter::once(titled).chain(untitled)).unwrap()
        }

        /// The presets of the workspace: `delay-1s` has a title, and the others are not there.
        fn presets() -> PresetSet {
            let titled = Preset::new(Self::delay_1s(), LieDefinition::Silence).with_title("Delay 1 s".parse().unwrap());
            PresetSet::new([titled]).unwrap()
        }

        /// The pad of `delay-1s` on `order-producers`, on that layer.
        fn slow(layer: &Layer) -> Pad {
            let layout = Self::layout();
            let row = layout.row_of(&Self::order_producers()).unwrap();
            let column = layout.column_of(&Self::delay_1s()).unwrap();
            Pad::new(Self::name(), layer.clone(), row, column)
        }

        /// The pad of `delay-1s` on `order-producers`, on the layer of `order-ingest`.
        fn slow_ingest() -> Pad {
            Self::slow(&Self::order_ingest())
        }

        /// The layout with one more layer, and the pad of `delay-1s` on `order-producers` on that layer.
        fn with_layer(layer: &Layer) -> (PadLayout, Pad) {
            let layers = Self::layers().into_iter().chain([layer.clone()]);
            let layout = PadLayout::new(Self::name(), Self::rows(), Self::columns(), layers).unwrap();
            (layout, Self::slow(layer))
        }
    }

    #[test]
    fn a_layout_keeps_its_rows_columns_and_layers_in_order() {
        let layout = OrderDesk::layout();

        assert_eq!(layout.name(), OrderDesk::NAME);
        assert_eq!(layout.title(), None);
        assert_eq!(layout.rows(), OrderDesk::rows());
        assert_eq!(layout.columns(), OrderDesk::columns());
        assert_eq!(layout.layers(), OrderDesk::layers());
        assert_eq!(layout.first_layer(), &Layer::all_clients());
    }

    #[test]
    fn a_layout_has_a_title_when_someone_gives_it_one() {
        let title = |text: &str| text.parse::<LayoutTitle>().unwrap();
        let titled = OrderDesk::layout().with_title(title("Order desk"));

        assert_eq!(titled.title(), Some(&title("Order desk")));
        assert_eq!(titled.name(), OrderDesk::layout().name());
        assert_eq!(
            titled.clone().with_title(title("The desk of the orders")).title(),
            Some(&title("The desk of the orders"))
        );
        assert_eq!(titled.without_title(), OrderDesk::layout());
    }

    #[test]
    fn a_layout_finds_a_line_by_its_name_and_a_name_by_its_line() {
        let layout = OrderDesk::layout();

        for (index, selector) in OrderDesk::rows().iter().enumerate() {
            assert_eq!(layout.row_of(selector), Some(Row::from_index(index)));
            assert_eq!(layout.selector(Row::from_index(index)), Some(selector));
        }
        for (index, preset) in OrderDesk::columns().iter().enumerate() {
            assert_eq!(layout.column_of(preset), Some(Column::from_index(index)));
            assert_eq!(layout.preset(Column::from_index(index)), Some(preset));
        }
        for layer in OrderDesk::layers() {
            assert!(layout.has_layer(&layer));
        }

        assert_eq!(layout.selector(Row::from_index(OrderDesk::ROWS.len())), None);
        assert_eq!(layout.preset(Column::from_index(OrderDesk::COLUMNS.len())), None);
        assert_eq!(layout.row_of(&"heartbeat".parse().unwrap()), None);
        assert_eq!(layout.column_of(&"throttle".parse().unwrap()), None);
        assert!(!layout.has_layer(&"kafka-ui".parse().unwrap()));
    }

    #[test]
    fn an_empty_pad_creates_a_rule_from_its_row_its_column_and_its_layer() {
        let rule = OrderDesk::layout()
            .rule(
                &OrderDesk::slow_ingest(),
                &OrderDesk::selectors(),
                &OrderDesk::presets(),
            )
            .unwrap();

        assert_eq!(rule.name(), "delay-1s-on-order-producers-for-order-ingest");
        assert_eq!(
            rule.title().map(RuleTitle::as_str),
            Some("Delay 1 s on Order producers for order-ingest")
        );
        assert_eq!(rule.selector(), &SelectorRef::Named(OrderDesk::order_producers()));
        assert_eq!(rule.action(), &ActionRef::Preset(OrderDesk::delay_1s()));
        assert_eq!(
            rule.scope(),
            &Scope::client(ClientId::new(OrderDesk::CLIENTS[0]).unwrap())
        );
        assert!(rule.trigger().is_always());
        assert_eq!(rule.lifetime(), Lifetime::UntilDisarmed);
        assert_eq!(rule.dice(), None);
    }

    #[test]
    fn the_same_position_on_the_layer_of_all_clients_creates_a_rule_for_all_clients() {
        let everyone = OrderDesk::slow(&Layer::all_clients());

        let rule = OrderDesk::layout()
            .rule(&everyone, &OrderDesk::selectors(), &OrderDesk::presets())
            .unwrap();
        assert_eq!(rule.name(), "delay-1s-on-order-producers-for-all-clients");
        assert_eq!(
            rule.title().map(RuleTitle::as_str),
            Some("Delay 1 s on Order producers for All clients")
        );
        assert!(rule.scope().is_all());
    }

    #[test]
    fn a_selector_or_a_preset_without_a_title_stands_in_the_title_with_its_name() {
        let layout = OrderDesk::layout();
        let pad = OrderDesk::slow_ingest();

        // The selector has a title, and no preset of this name is there.
        let rule = layout
            .rule(&pad, &OrderDesk::selectors(), &PresetSet::default())
            .unwrap();
        assert_eq!(
            rule.title().map(RuleTitle::as_str),
            Some("delay-1s on Order producers for order-ingest")
        );

        // The selector of the second row is there, and it has no title.
        let fetch = layout.pad(pad.layer(), Row::from_index(1), pad.column()).unwrap();
        let rule = layout
            .rule(&fetch, &OrderDesk::selectors(), &OrderDesk::presets())
            .unwrap();
        assert_eq!(
            rule.title().map(RuleTitle::as_str),
            Some("Delay 1 s on fetch for order-ingest")
        );
    }

    #[test]
    fn the_name_of_a_rule_does_not_depend_on_a_title() {
        let layout = OrderDesk::layout();
        let pad = OrderDesk::slow_ingest();

        let with_titles = layout
            .rule(&pad, &OrderDesk::selectors(), &OrderDesk::presets())
            .unwrap();
        let without = layout
            .rule(&pad, &SelectorSet::default(), &PresetSet::default())
            .unwrap();

        assert_eq!(layout.rule_name(&pad).as_ref(), Some(with_titles.name()));
        assert_eq!(with_titles.name(), without.name());
        assert_eq!(with_titles.without_title(), without.without_title());
    }

    #[test]
    fn the_text_of_a_layer_goes_into_the_name_as_a_name_can_hold_it() {
        let named = |layer: Layer| {
            let (layout, pad) = OrderDesk::with_layer(&layer);
            layout.rule_name(&pad).unwrap()
        };
        let client = |id: &str| Layer::client(ClientId::new(id).unwrap());

        assert_eq!(
            named(client("Order Ingest (EU)")),
            "delay-1s-on-order-producers-for-order-ingest-eu"
        );
        assert_eq!(
            named(client("  kafka_ui.v2")),
            "delay-1s-on-order-producers-for-kafka-ui-v2"
        );
        assert_eq!(
            named(Layer::new(Scope::endpoint(Endpoint::Broker(BrokerId::from(2))))),
            "delay-1s-on-order-producers-for-endpoint-is-broker-2"
        );
        // A `client.id` without a letter or a digit leaves nothing for the name.
        assert_eq!(named(client("…")), "delay-1s-on-order-producers-for");
    }

    #[test]
    fn a_name_that_is_too_long_is_cut() {
        let preset: PresetName = "delay-1s".parse().unwrap();
        let named = |selector: &str| {
            let layout = PadLayout::new(
                OrderDesk::name(),
                [selector.parse().unwrap()],
                [preset.clone()],
                [OrderDesk::order_ingest()],
            )
            .unwrap();
            let pad = layout
                .pad(&OrderDesk::order_ingest(), Row::from_index(0), Column::from_index(0))
                .unwrap();
            layout.rule_name(&pad).unwrap()
        };

        // The longest name of a selector leaves no room for the layer, and not for all of the selector.
        let start = format!("{preset}-on-");
        let longest = "a".repeat(SelectorName::MAX_LENGTH);
        let cut = named(&longest);
        assert_eq!(cut.as_str().len(), RuleName::MAX_LENGTH);
        assert_eq!(
            cut,
            format!("{start}{}", "a".repeat(RuleName::MAX_LENGTH - start.len())).as_str()
        );

        // A cut that would end on a hyphen ends before it.
        let up_to_the_layer = "a".repeat(RuleName::MAX_LENGTH - format!("{start}-for-").len());
        assert_eq!(
            named(&up_to_the_layer),
            format!("{start}{up_to_the_layer}-for").as_str()
        );
    }

    #[test]
    fn a_layer_whose_text_breaks_the_rule_for_titles_gives_a_rule_without_a_title() {
        let ends_with_a_space = Layer::client(ClientId::new("order-ingest ").unwrap());
        let (layout, pad) = OrderDesk::with_layer(&ends_with_a_space);

        let rule = layout
            .rule(&pad, &OrderDesk::selectors(), &OrderDesk::presets())
            .unwrap();

        assert_eq!(rule.title(), None);
        assert_eq!(rule.name(), "delay-1s-on-order-producers-for-order-ingest");
    }

    #[test]
    fn every_pad_of_a_layout_with_short_names_points_at_a_rule_of_its_own() {
        let layout = OrderDesk::layout();
        let mut names = std::collections::BTreeSet::new();

        for layer in layout.layers() {
            for row in 0..layout.rows().len() {
                for column in 0..layout.columns().len() {
                    let pad = layout
                        .pad(layer, Row::from_index(row), Column::from_index(column))
                        .unwrap();
                    assert!(names.insert(layout.rule_name(&pad).unwrap()), "{pad}");
                }
            }
        }
        assert_eq!(
            names.len(),
            OrderDesk::ROWS.len() * OrderDesk::COLUMNS.len() * layout.layers().len()
        );
    }

    #[test]
    fn a_layout_has_no_pad_outside_its_rows_columns_and_layers_or_of_another_layout() {
        let layout = OrderDesk::layout();
        let pad = OrderDesk::slow_ingest();
        let past_the_rows = Row::from_index(OrderDesk::ROWS.len());
        let past_the_columns = Column::from_index(OrderDesk::COLUMNS.len());
        let no_layer: Layer = "kafka-ui".parse().unwrap();

        assert!(layout.holds(&pad));
        assert_eq!(layout.pad(pad.layer(), past_the_rows, pad.column()), None);
        assert_eq!(layout.pad(pad.layer(), pad.row(), past_the_columns), None);
        assert_eq!(layout.pad(&no_layer, pad.row(), pad.column()), None);

        let outside = [
            Pad::new(OrderDesk::name(), pad.layer().clone(), past_the_rows, pad.column()),
            Pad::new(OrderDesk::name(), pad.layer().clone(), pad.row(), past_the_columns),
            Pad::new(OrderDesk::name(), no_layer, pad.row(), pad.column()),
            Pad::new(
                "payments-desk".parse().unwrap(),
                pad.layer().clone(),
                pad.row(),
                pad.column(),
            ),
        ];
        for pad in outside {
            assert!(!layout.holds(&pad), "{pad}");
            assert_eq!(layout.rule_name(&pad), None, "{pad}");
            assert_eq!(
                layout.rule(&pad, &OrderDesk::selectors(), &OrderDesk::presets()),
                None,
                "{pad}"
            );
        }
    }

    #[test]
    fn a_layout_without_rows_or_columns_is_a_layout_without_pads() {
        let layout = PadLayout::new(OrderDesk::name(), [], [], OrderDesk::layers()).unwrap();

        assert!(layout.rows().is_empty() && layout.columns().is_empty());
        assert_eq!(
            layout.pad(&Layer::all_clients(), Row::from_index(0), Column::from_index(0)),
            None
        );
    }

    #[test]
    fn a_layout_needs_a_layer() {
        assert_eq!(
            PadLayout::new(OrderDesk::name(), OrderDesk::rows(), OrderDesk::columns(), []),
            Err(PadLayoutError::NoLayer {
                layout: OrderDesk::name()
            })
        );
    }

    #[test]
    fn a_layout_takes_every_name_once() {
        let rows_with_one_twice = [OrderDesk::rows(), vec![OrderDesk::rows()[0].clone()]].concat();
        let columns_with_one_twice = [OrderDesk::columns(), vec![OrderDesk::columns()[0].clone()]].concat();

        let error = PadLayout::new(
            OrderDesk::name(),
            rows_with_one_twice,
            OrderDesk::columns(),
            OrderDesk::layers(),
        )
        .unwrap_err();
        assert_eq!(
            error,
            PadLayoutError::DuplicateRow {
                layout: OrderDesk::name(),
                selector: OrderDesk::rows()[0].clone(),
            }
        );
        assert_eq!(
            error.to_string(),
            "layout \"order-desk\" has two rows for selector \"order-producers\""
        );

        let error = PadLayout::new(
            OrderDesk::name(),
            OrderDesk::rows(),
            columns_with_one_twice,
            OrderDesk::layers(),
        )
        .unwrap_err();
        assert_eq!(
            error,
            PadLayoutError::DuplicateColumn {
                layout: OrderDesk::name(),
                preset: OrderDesk::columns()[0].clone(),
            }
        );
        assert_eq!(
            error.to_string(),
            "layout \"order-desk\" has two columns for preset \"error-retriable\""
        );
    }

    #[test]
    fn a_layout_takes_every_layer_once_however_someone_says_it() {
        // The text of a client, and the scope of the same client.
        let said = OrderDesk::order_ingest();
        let built = Layer::new(Scope::client(ClientId::new(OrderDesk::CLIENTS[0]).unwrap()));

        let error = PadLayout::new(
            OrderDesk::name(),
            OrderDesk::rows(),
            OrderDesk::columns(),
            [Layer::all_clients(), said.clone(), built],
        )
        .unwrap_err();
        assert_eq!(
            error,
            PadLayoutError::DuplicateLayer {
                layout: OrderDesk::name(),
                layer: said,
            }
        );
        assert_eq!(
            error.to_string(),
            "layout \"order-desk\" has the layer \"order-ingest\" twice"
        );
    }
}
