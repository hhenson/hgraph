#include <hgraph/lib/std/operators/impl/table_impl.h>

#include <hgraph/types/metadata/type_realization.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/time_series/ts_input/bundle_view.h>
#include <hgraph/types/time_series/ts_input/dict_view.h>
#include <hgraph/types/time_series/ts_output/dict_view.h>
#include <hgraph/types/value/compact_storage.h>
#include <hgraph/types/value/specialized_views.h>
#include <hgraph/types/value/value_builder.h>

#include <arrow/table.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>

#include <fmt/format.h>

#include <algorithm>
#include <charconv>
#include <memory>
#include <numeric>
#include <span>
#include <unordered_map>
#include <unordered_set>

namespace
{
    std::unordered_map<const hgraph::TSValueTypeMetaData *, const hgraph::TableTypeOps *>
        g_table_type_ops_overrides;
}

namespace hgraph::stdlib
{
    namespace table_ts_detail
    {
        [[nodiscard]] const TableTypeOps &builtin_table_type_ops(
            const TSValueTypeMetaData *schema);

        namespace
        {
            inline constexpr std::string_view RECORDING_PROJECTION_VERSION_KEY{
                "hgraph.recording.projection.version"};
            inline constexpr std::string_view RECORDING_PROJECTION_COUNT_KEY{
                "hgraph.recording.projection.count"};
            inline constexpr std::string_view RECORDING_PROJECTION_COLUMN_PREFIX{
                "hgraph.recording.projection.column."};
            inline constexpr std::string_view RECORDING_PROJECTION_PRESENT_PREFIX{
                "hgraph.recording.projection.present."};
            inline constexpr std::string_view RECORDING_PROJECTION_VERSION{"1"};

            [[nodiscard]] std::string projection_column_key(std::size_t column)
            {
                return std::string{RECORDING_PROJECTION_COLUMN_PREFIX} +
                       std::to_string(column);
            }

            [[nodiscard]] std::string projection_present_key(std::size_t column)
            {
                return std::string{RECORDING_PROJECTION_PRESENT_PREFIX} +
                       std::to_string(column);
            }

            void set_metadata(arrow::KeyValueMetadata &metadata, std::string key,
                              std::string value)
            {
                const arrow::Status status = metadata.Set(std::move(key), std::move(value));
                if (!status.ok())
                {
                    throw std::runtime_error("record: cannot encode projection metadata: " +
                                             status.ToString());
                }
            }

            /** The writer-declared projection, or nullopt for a legacy or
                hand-built frame. The expected count bounds parsing before any
                allocation and detects a projection for a different layout. */
            [[nodiscard]] std::optional<std::vector<std::optional<std::string>>>
            recording_projection(const Frame &frame, std::size_t expected_columns)
            {
                const auto &metadata = frame.table->schema()->metadata();
                if (metadata == nullptr ||
                    metadata->FindKey(std::string{RECORDING_PROJECTION_VERSION_KEY}) < 0)
                {
                    return std::nullopt;
                }

                const std::string version =
                    metadata->Get(std::string{RECORDING_PROJECTION_VERSION_KEY}).ValueOr("");
                if (version != RECORDING_PROJECTION_VERSION)
                {
                    throw std::runtime_error("replay: unsupported recording projection metadata "
                                             "version '" +
                                             version + "'");
                }

                const std::string count_text =
                    metadata->Get(std::string{RECORDING_PROJECTION_COUNT_KEY}).ValueOr("");
                std::size_t count{0};
                const auto [end, error] =
                    std::from_chars(count_text.data(), count_text.data() + count_text.size(), count);
                if (error != std::errc{} || end != count_text.data() + count_text.size())
                {
                    throw std::runtime_error(
                        "replay: recording projection metadata has an invalid column count");
                }
                if (count != expected_columns)
                {
                    throw std::runtime_error(
                        fmt::format("replay: recording projection describes {} layout columns, "
                                    "but the requested type requires {}",
                                    count, expected_columns));
                }

                std::vector<std::optional<std::string>> result;
                result.reserve(count);
                for (std::size_t column = 0; column < count; ++column)
                {
                    const std::string presence_key = projection_present_key(column);
                    const std::string presence = metadata->Get(presence_key).ValueOr("");
                    if (presence == "0")
                    {
                        result.emplace_back(std::nullopt);
                        continue;
                    }
                    if (presence != "1")
                    {
                        throw std::runtime_error("replay: recording projection metadata is "
                                                 "missing entry '" +
                                                 presence_key + "'");
                    }
                    const std::string name_key = projection_column_key(column);
                    const auto        value = metadata->Get(name_key);
                    if (!value.ok())
                    {
                        throw std::runtime_error("replay: recording projection metadata is "
                                                 "missing entry '" +
                                                 name_key + "'");
                    }
                    result.emplace_back(*value);
                }
                return result;
            }

            // ---------------------------------------------------------------
            // Layout synthesis (interned; NO locks - the OperatorRegistry
            // precedent: wiring and evaluation are single-threaded).
            // ---------------------------------------------------------------

            struct LayoutKey
            {
                const TSValueTypeMetaData *meta{nullptr};
                std::string                date_key{};
                std::string                as_of_key{};

                bool operator==(const LayoutKey &) const = default;
            };

            struct LayoutKeyHash
            {
                std::size_t operator()(const LayoutKey &key) const noexcept
                {
                    std::size_t seed = std::hash<const TSValueTypeMetaData *>{}(key.meta);
                    seed ^= std::hash<std::string>{}(key.date_key) + 0x9e3779b9 + (seed << 6) +
                            (seed >> 2);
                    seed ^= std::hash<std::string>{}(key.as_of_key) + 0x9e3779b9 + (seed << 6) +
                            (seed >> 2);
                    return seed;
                }
            };

            std::unordered_map<LayoutKey, std::unique_ptr<TsTableLayout>, LayoutKeyHash> g_layouts;
            /** The registry generation ``g_layouts`` was built against. */
            std::uint64_t g_layouts_generation{0};

            /** Drop the cache when the registry that owns its keys has been
                reset: the schema pointers it interns by are freed by that
                reset, and the next interning can hand the same address to a
                DIFFERENT type - at which point a stale entry answers for it.
                register_table_operators() also clears, but only a caller that
                registers operators reaches it, and building a layout needs
                nothing but the type registry. */
            void discard_stale_layouts()
            {
                const std::uint64_t generation = TypeRegistry::instance().reset_generation();
                if (generation == g_layouts_generation)
                {
                    return;
                }
                g_layouts.clear();
                g_table_type_ops_overrides.clear();
                g_layouts_generation = generation;
            }

            /** Flatten a VALUE schema to (suffix, leaf, path) triples. Bundles
                flatten by field name (dotted), tuples by index; the suffix is
                empty for an atomic root. */
            template <typename Sink>
            void flatten_value(const ValueTypeMetaData *meta, const std::string &suffix,
                               std::vector<std::size_t> path, Sink &&sink)
            {
                if (meta != nullptr && meta->value_kind() == ValueTypeKind::Atomic)
                {
                    sink(suffix, meta, std::move(path));
                    return;
                }
                if (meta != nullptr && (meta->value_kind() == ValueTypeKind::Bundle ||
                                        meta->value_kind() == ValueTypeKind::Tuple))
                {
                    for (std::size_t i = 0; i < meta->field_count; ++i)
                    {
                        const auto &field = meta->fields[i];
                        std::string name =
                            meta->value_kind() == ValueTypeKind::Bundle && field.name != nullptr
                                ? std::string{field.name}
                                : fmt::format("{}", i);
                        auto child_path = path;
                        child_path.push_back(i);
                        flatten_value(field.type, suffix.empty() ? name : suffix + "." + name,
                                      std::move(child_path), sink);
                    }
                    return;
                }
                throw std::invalid_argument(
                    fmt::format("to_table: unsupported value kind for column '{}' ('{}')",
                                suffix.empty() ? "value" : suffix,
                                meta != nullptr && !meta->name().empty() ? meta->name()
                                                                         : std::string_view{"?"}));
            }

            void emit_plain(const TableLayout &layout, const TSInputView &ts, Int mode,
                            DateTime now, DateTime as_of, bool emit_removals,
                            const TableRowSink &sink, bool reject_empty_frames);
            void emit_partitioned(const TableLayout &layout, const TSInputView &ts, Int mode,
                                  DateTime now, DateTime as_of, bool emit_removals,
                                  const TableRowSink &sink, bool reject_empty_frames);
            void emit_frame(const TableLayout &layout, const TSInputView &ts, Int mode,
                            DateTime now, DateTime as_of, bool emit_removals,
                            const TableRowSink &sink, bool reject_empty_frames);
            void apply_plain(const TableLayout &layout, const TableRowSource &source,
                             const TSOutputView &out);
            void apply_partitioned(const TableLayout &layout, const TableRowSource &source,
                                   const TSOutputView &out);
            void apply_frame(const TableLayout &layout, const TableRowSource &source,
                             const TSOutputView &out);

            void begin_leaf(TableLayout &layout, const TSValueTypeMetaData *schema)
            {
                if (layout.leaf_ts == nullptr)
                {
                    layout.leaf_ts = schema;
                    layout.value_col_start = layout.keys.size();
                }
            }

            /** Compose an exact child override into the structural plan.

                Built-in children describe directly into their parent as
                before. A registered child is first built as its own canonical
                layout, then its value columns are projected into the parent.
                Evaluation can consequently invoke the already-selected child
                ops without consulting the registry or assuming its value
                representation. */
            void describe_child(TableLayout &layout, const TSValueTypeMetaData *schema,
                                std::string prefix, std::vector<std::size_t> ts_path,
                                std::size_t level_no)
            {
                const TableTypeOps &selected = table_type_ops(schema);
                const TableTypeOps &builtin = builtin_table_type_ops(schema);
                if (&selected == &builtin)
                {
                    selected.describe(layout, schema, std::move(prefix), std::move(ts_path),
                                      level_no);
                    return;
                }

                const TsTableLayout &child =
                    ts_table_layout(schema, layout.date_key, layout.as_of_key);
                if (child.multi())
                {
                    throw std::invalid_argument(
                        "to_table: a registered multi-row child strategy cannot be embedded "
                        "inside a structural table layout");
                }
                if (child.keys.size() < 2 || child.keys.size() != child.col_metas.size())
                {
                    throw std::logic_error(
                        "to_table: registered child strategy produced an invalid table layout");
                }

                begin_leaf(layout, schema);
                TableLayout::ChildPlan child_plan{
                    .layout = &child,
                    .ts_path = std::move(ts_path),
                    .columns = std::vector<std::size_t>(child.keys.size(), TableLayout::no_column),
                };
                // Every canonical layout starts with its bitemporal pair. They
                // map to the parent's pair for apply, while nested emission
                // suppresses the child's duplicate delivery.
                child_plan.columns[0] = 0;
                child_plan.columns[1] = 1;
                for (std::size_t column = 2; column < child.keys.size(); ++column)
                {
                    const std::string &local_name = child.keys[column];
                    const std::string name =
                        prefix.empty()
                            ? local_name
                            : local_name == "value" ? prefix : prefix + "." + local_name;
                    child_plan.columns[column] = layout.keys.size();
                    layout.keys.push_back(name);
                    layout.col_metas.push_back(child.col_metas[column]);
                }
                // Payload nullability does not encode whether the child
                // emitted a row: a valid strategy may have no value columns,
                // or may deliberately emit an all-null row. Persist that
                // distinction explicitly so nested apply observes the same
                // row presence as root apply.
                child_plan.presence_column = layout.keys.size();
                layout.keys.push_back(
                    fmt::format("__hgraph_child_{}_present__", layout.child_plans.size()));
                layout.col_metas.push_back(TypeRegistry::instance().value_type("bool"));
                layout.child_plans.push_back(std::move(child_plan));
            }

            void describe_tsd(TableLayout &layout, const TSValueTypeMetaData *schema, std::string,
                              std::vector<std::size_t>, std::size_t           level_no)
            {
                TableLayout::Level level;
                level.key_meta = schema->key_type();
                level.removed_col = layout.keys.size();
                const std::string removed_name = fmt::format("__key_{}_removed__", level_no);
                layout.keys.push_back(removed_name);
                layout.col_metas.push_back(TypeRegistry::instance().value_type("bool"));
                layout.removed_keys.push_back(removed_name);

                level.first_key_col = layout.keys.size();
                flatten_value(level.key_meta, "", {},
                              [&](const std::string &suffix, const ValueTypeMetaData *leaf,
                                  std::vector<std::size_t> path) {
                                  const std::string name =
                                      suffix.empty()
                                          ? fmt::format("__key_{}__", level_no)
                                          : fmt::format("__key_{}_{}__", level_no, suffix);
                                  layout.keys.push_back(name);
                                  layout.col_metas.push_back(leaf);
                                  layout.partition_keys.push_back(name);
                                  level.key_paths.push_back(std::move(path));
                              });
                layout.levels.push_back(std::move(level));
                const auto *child = schema->element_ts();
                describe_child(layout, child, "", {}, level_no + 1);
            }

            void describe_tsb(TableLayout &layout, const TSValueTypeMetaData *schema,
                              std::string prefix, std::vector<std::size_t> ts_path,
                              std::size_t level_no)
            {
                begin_leaf(layout, schema);
                const auto *bundle = schema->value_schema;
                for (std::size_t i = 0; i < schema->field_count(); ++i)
                {
                    const auto *child = schema->fields()[i].type;
                    const char *name = bundle->fields[i].name;
                    auto        child_path = ts_path;
                    child_path.push_back(i);
                    const std::string child_prefix =
                        prefix.empty() ? std::string{name != nullptr ? name : ""}
                                       : prefix + "." + (name != nullptr ? name : "");
                    describe_child(layout, child, child_prefix, std::move(child_path), level_no);
                }
            }

            void describe_ts(TableLayout &layout, const TSValueTypeMetaData *schema,
                             std::string prefix, std::vector<std::size_t> ts_path, std::size_t)
            {
                begin_leaf(layout, schema);
                flatten_value(schema->value_schema, prefix, {},
                              [&](const std::string &suffix, const ValueTypeMetaData *leaf,
                                  std::vector<std::size_t> value_path) {
                                  TableLayout::Column entry;
                                  entry.name = suffix.empty() ? "value" : suffix;
                                  entry.leaf = leaf;
                                  entry.column = layout.keys.size();
                                  entry.ts_path = ts_path;
                                  entry.value_path = std::move(value_path);
                                  layout.keys.push_back(entry.name);
                                  layout.col_metas.push_back(leaf);
                                  layout.value_cols.push_back(std::move(entry));
                              });
            }

            void describe_frame(TableLayout &layout, const TSValueTypeMetaData *schema,
                                std::string prefix, std::vector<std::size_t> ts_path, std::size_t)
            {
                if (!prefix.empty() || !ts_path.empty() || !layout.value_cols.empty())
                {
                    throw std::invalid_argument("to_table: a Frame leaf must be the complete "
                                                "value below any TSD levels");
                }
                begin_leaf(layout, schema);
                layout.is_multi_row = true;
                const auto *columns = schema->value_schema->element_type;
                if (columns == nullptr)
                {
                    throw std::invalid_argument(
                        "to_table: an untyped Frame input cannot derive table columns "
                        "(use Frame[Schema])");
                }
                layout.frame_converter =
                    &table_converter(columns, layout.date_key, layout.as_of_key);
                for (const auto &column : layout.frame_converter->columns)
                {
                    TableLayout::Column entry;
                    entry.name = column.name;
                    entry.leaf = column.leaf_meta;
                    entry.column = layout.keys.size();
                    entry.value_path = column.path;
                    layout.keys.push_back(column.name);
                    layout.col_metas.push_back(column.leaf_meta);
                    layout.value_cols.push_back(std::move(entry));
                }
            }

            [[nodiscard]] const TableTypeOps &tsd_ops()
            {
                static const TableTypeOps ops{&describe_tsd, &emit_partitioned, &apply_partitioned};
                return ops;
            }

            [[nodiscard]] const TableTypeOps &tsb_ops()
            {
                static const TableTypeOps ops{&describe_tsb, &emit_plain, &apply_plain};
                return ops;
            }

            [[nodiscard]] const TableTypeOps &ts_ops()
            {
                static const TableTypeOps ops{&describe_ts, &emit_plain, &apply_plain};
                return ops;
            }

            [[nodiscard]] const TableTypeOps &frame_ops()
            {
                static const TableTypeOps ops{&describe_frame, &emit_frame, &apply_frame};
                return ops;
            }

            [[nodiscard]] const TsTableLayout *build_layout(const TSValueTypeMetaData *ts,
                                                            std::string_view           date_key,
                                                            std::string_view           as_of_key)
            {
                auto layout = std::make_unique<TsTableLayout>();
                layout->ts_schema = ts;
                layout->date_key = std::string{date_key};
                layout->as_of_key = std::string{as_of_key};

                const auto *datetime_meta = TypeRegistry::instance().value_type("datetime");
                layout->keys.push_back(layout->date_key);
                layout->col_metas.push_back(datetime_meta);
                layout->keys.push_back(layout->as_of_key);
                layout->col_metas.push_back(datetime_meta);
                layout->ops = &table_type_ops(ts);
                layout->ops->describe(*layout, ts, "", {}, 1);

                auto &registry = TypeRegistry::instance();
                layout->row_meta = registry.tuple(layout->col_metas);
                layout->rows_meta = registry.list(layout->row_meta, 0, /*variadic_tuple=*/true);
                layout->output_ts =
                    registry.ts(layout->multi() ? layout->rows_meta : layout->row_meta);

                const auto *raw = layout.get();
                g_layouts.emplace(LayoutKey{ts, std::string{date_key}, std::string{as_of_key}},
                                  std::move(layout));
                return raw;
            }
        }  // namespace

        const TableTypeOps &builtin_table_type_ops(const TSValueTypeMetaData *schema)
        {
            if (schema == nullptr)
            {
                return empty_table_type_ops();
            }
            switch (schema->kind)
            {
            case TSTypeKind::TSD:
                return tsd_ops();
            case TSTypeKind::TSB:
                return tsb_ops();
            case TSTypeKind::TS:
                return TypeRegistry::instance().is_frame(schema->value_schema) ? frame_ops()
                                                                               : ts_ops();
            default:
                return empty_table_type_ops();
            }
        }

        const TsTableLayout &ts_table_layout(const TSValueTypeMetaData *ts,
                                             std::string_view date_key, std::string_view as_of_key)
        {
            discard_stale_layouts();
            const LayoutKey key{ts, std::string{date_key}, std::string{as_of_key}};
            if (const auto it = g_layouts.find(key); it != g_layouts.end())
            {
                return *it->second;
            }
            return *build_layout(ts, date_key, as_of_key);
        }

        void clear_ts_table_layouts() noexcept
        {
            g_layouts.clear();
            g_layouts_generation = TypeRegistry::instance().reset_generation();
        }

        const ValueTypeMetaData *to_table_mode_meta()
        {
            return scalar_descriptor<ToTableMode>::value_meta();
        }

        namespace
        {
            constexpr Int kModeTick = static_cast<Int>(ToTableMode::Tick);
            constexpr Int kModeSnap = static_cast<Int>(ToTableMode::Snap);

            // ---------------------------------------------------------------
            // Row building
            // ---------------------------------------------------------------

            [[nodiscard]] ValueTypeRef checked_binding(const ValueTypeMetaData *meta,
                                                       const char              *what)
            {
                const auto binding = value_type_for_active_realization(meta);
                if (binding == nullptr)
                {
                    throw std::logic_error(fmt::format("{}: schema has no value binding", what));
                }
                return binding;
            }

            /** Assign a semantic cell through the selected target strategy.
                Cell schemas may be compatible without being identical (for
                example a concrete CompoundScalar leaf assigned to its closed
                base realization), so ``ValueView::copy_from`` is deliberately
                too narrow here. */
            void assign_cell(const ValueView &destination,
                             const ValueView &source)
            {
                if (!destination.has_value() || !source.has_value())
                {
                    throw std::invalid_argument(
                        "table cell assignment requires live values");
                }
                const auto target = destination.binding();
                target.ops_ref().copy_assign_from(
                    target, destination.mutable_data(), source.binding(),
                    source.data());
            }

            /** A fresh borrow over the same binding + memory (the read views
                are move-only; ``at`` results are const).

                That constness is deliberate rather than incidental - see the
                note on ``IndexedValueView::at`` - so this is the supported way
                to get an owned ``ValueView`` out of an accessor, not a
                workaround to be optimised away. */
            [[nodiscard]] ValueView reborrow(const ValueView &view)
            {
                return ValueView{view.binding(), view.data()};
            }

            /** Walk composite VALUE fields (bundle or tuple) along ``path``;
                an unset field yields an invalid view. */
            [[nodiscard]] ValueView walk_value(ValueView view, std::span<const std::size_t> path)
            {
                for (const std::size_t index : path)
                {
                    if (!view.has_value())
                    {
                        return view;
                    }
                    if (view.is_tuple())
                    {
                        auto tuple = view.as_tuple();
                        view = reborrow(tuple.at(index));
                    }
                    else
                    {
                        auto bundle = view.as_bundle();
                        view = reborrow(bundle.at(index));
                    }
                }
                return view;
            }

            /** Mutable walk: marks each step live and returns writable leaf
                memory (composite mutable_element_at semantics). */
            [[nodiscard]] ValueView walk_mutable(ValueView view, std::span<const std::size_t> path)
            {
                for (const std::size_t index : path)
                {
                    if (view.is_tuple())
                    {
                        auto mut = view.as_tuple().begin_mutation();
                        view = mut.at(index);
                    }
                    else
                    {
                        auto mut = view.as_bundle().begin_mutation();
                        view = mut.at(index);
                    }
                }
                return view;
            }

            /**
             * The keys of the levels above the row being emitted, threaded
             * through the recursion rather than copied into a buffer.
             *
             * A row is emitted at whatever depth it belongs to - a removal
             * above the leaf, a value row at it - and delivering the chain
             * gives exactly that row's key columns. There is nothing to clear
             * afterwards, because nothing was written speculatively.
             */
            struct KeyChain
            {
                const KeyChain             *parent{nullptr};
                const TsTableLayout::Level *level{nullptr};
                const ValueView            *key{nullptr};
                bool                        removed{false};
            };

            /** The scalars a row needs that are not part of the source data. */
            struct RowScalars
            {
                Value now;
                Value as_of;
                Value removed_true{Bool{true}};
                Value removed_false{Bool{false}};

                RowScalars(DateTime now_, DateTime as_of_) : now{now_}, as_of{as_of_} {}
            };

            struct RowBuffer
            {
                explicit RowBuffer(const TsTableLayout &layout)
                    : value{checked_binding(layout.row_meta, "to_table")}
                {
                }

                RowBuffer(const RowBuffer &other) : value{Value{other.value.view()}} {}
                RowBuffer(RowBuffer &&) noexcept = default;
                RowBuffer &operator=(RowBuffer &&) noexcept = default;

                void set_cell(std::size_t index, const ValueView &leaf)
                {
                    if (!leaf.has_value())
                    {
                        return;
                    }  // unset cell = None
                    ValueView root = value.view();
                    auto      mut = root.as_tuple().begin_mutation();
                    ValueView cell = mut.at(index);
                    assign_cell(cell, leaf);
                }

                template <typename T>
                void set_scalar(std::size_t index, T scalar)
                {
                    Value cell{std::move(scalar)};
                    set_cell(index, cell.view());
                }

                Value value;
            };

            /** Deliver the key columns of every level above this row, outermost
                first, along with each level's removed flag. */
            void deliver_keys(const RowSink &sink, const RowScalars &scalars, const KeyChain *chain)
            {
                if (chain == nullptr)
                {
                    return;
                }
                deliver_keys(sink, scalars, chain->parent);
                const auto &level = *chain->level;
                sink.cell(sink.context, level.removed_col,
                          chain->removed ? scalars.removed_true.view()
                                         : scalars.removed_false.view());
                for (std::size_t i = 0; i < level.key_paths.size(); ++i)
                {
                    sink.cell(sink.context, level.first_key_col + i,
                              walk_value(reborrow(*chain->key), level.key_paths[i]));
                }
            }

            /** Write the leaf value columns of ``row`` from the leaf TS view.
                Tick mode writes modified nodes only; Sample/Snap write every
                valid node. */
            void write_value_cells(const RowSink &sink, const TsTableLayout &layout,
                                   const TSInputView &leaf, Int mode)
            {
                for (std::size_t i = 0; i < layout.value_cols.size(); ++i)
                {
                    const auto &column = layout.value_cols[i];
                    TSInputView node = leaf.borrowed_ref();
                    for (const std::size_t index : column.ts_path)
                    {
                        auto bundle = node.as_bundle();
                        node = bundle.at(index);
                    }
                    if (mode == kModeTick && !node.modified())
                    {
                        continue;
                    }
                    if (!node.valid())
                    {
                        continue;
                    }
                    sink.cell(sink.context, column.column,
                              walk_value(node.value(), column.value_path));
                }
            }

            [[nodiscard]] TSInputView child_input(const TSInputView &root,
                                                  std::span<const std::size_t> path)
            {
                TSInputView child = root.borrowed_ref();
                for (const std::size_t index : path)
                {
                    auto bundle = child.as_bundle();
                    child = bundle.at(index);
                }
                return child;
            }

            /** Capture one registered child's local row before projecting it
                into its structural parent. Capturing keeps an invalid custom
                multi-row implementation from partially mutating the parent's
                row before the contract violation is diagnosed. */
            struct ChildRowCapture
            {
                const TableLayout::ChildPlan *plan{nullptr};
                std::vector<Value>             cells{};
                std::vector<bool>              written{};
                std::size_t                    rows{0};

                explicit ChildRowCapture(const TableLayout::ChildPlan &plan_)
                    : plan{&plan_}, cells(plan_.columns.size()),
                      written(plan_.columns.size(), false)
                {
                }

                static void cell(void *context, std::size_t column, const ValueView &value)
                {
                    auto &self = *static_cast<ChildRowCapture *>(context);
                    if (self.rows != 0)
                    {
                        throw std::invalid_argument(
                            "to_table: a registered child strategy emitted more than one row");
                    }
                    if (column >= self.cells.size())
                    {
                        throw std::out_of_range(
                            "to_table: a registered child strategy emitted an unknown column");
                    }
                    if (column < 2)
                    {
                        return;  // the structural parent owns the bitemporal pair
                    }
                    if (self.written[column])
                    {
                        throw std::invalid_argument(
                            "to_table: a registered child strategy emitted a column twice");
                    }
                    self.cells[column] = value.has_value() ? Value{value} : Value{};
                    self.written[column] = true;
                }

                static void end_row(void *context)
                {
                    auto &self = *static_cast<ChildRowCapture *>(context);
                    ++self.rows;
                    if (self.rows > 1)
                    {
                        throw std::invalid_argument(
                            "to_table: a registered child strategy emitted more than one row");
                    }
                }

                [[nodiscard]] RowSink sink()
                {
                    return RowSink{.context = this, .cell = &cell, .end_row = &end_row};
                }

                void deliver(const RowSink &sink) const
                {
                    if (rows != 1)
                    {
                        throw std::invalid_argument(
                            "to_table: a registered child strategy must emit exactly one row");
                    }
                    for (std::size_t column = 2; column < cells.size(); ++column)
                    {
                        const std::size_t parent_column = plan->columns[column];
                        if (parent_column == TableLayout::no_column || !cells[column].has_value())
                        {
                            continue;
                        }
                        sink.cell(sink.context, parent_column, cells[column].view());
                    }
                    const Value present{Bool{true}};
                    sink.cell(sink.context, plan->presence_column, present.view());
                }
            };

            void write_child_cells(const RowSink &sink, const TsTableLayout &layout,
                                   const TSInputView &leaf, Int mode, DateTime now,
                                   DateTime as_of, bool emit_removals,
                                   bool reject_empty_frames)
            {
                for (const TableLayout::ChildPlan &plan : layout.child_plans)
                {
                    TSInputView child = child_input(leaf, plan.ts_path);
                    if ((mode == kModeTick && !child.modified()) || !child.valid())
                    {
                        continue;
                    }
                    ChildRowCapture capture{plan};
                    const RowSink   child_sink = capture.sink();
                    plan.layout->ops->emit(*plan.layout, child, mode, now, as_of, emit_removals,
                                           child_sink, reject_empty_frames);
                    capture.deliver(sink);
                }
            }

            void emit_frame_rows(const TsTableLayout &layout, const TSInputView &leaf,
                                 const RowScalars &scalars, const KeyChain *chain,
                                 const RowSink &sink, bool reject_empty_frames)
            {
                const ValueView view = leaf.value();
                const Frame    &frame = view.checked_as<Frame>();
                if (!frame.has_value())
                {
                    return;
                }
                if (frame_rows(frame) == 0 && reject_empty_frames)
                {
                    throw std::invalid_argument("record: zero-row Frame ticks cannot be recorded");
                }
                for (std::int64_t r = 0; r < frame_rows(frame); ++r)
                {
                    sink.cell(sink.context, 0, scalars.now.view());
                    sink.cell(sink.context, 1, scalars.as_of.view());
                    deliver_keys(sink, scalars, chain);
                    for (std::size_t i = 0; i < layout.value_cols.size(); ++i)
                    {
                        const auto &column = layout.value_cols[i];
                        // Read just the Arrow leaf requested by the sink. A
                        // complete row Value would immediately be flattened
                        // again by the recorder and doubles the hot-path copy.
                        const Value cell = frame_cell(frame, column.name, column.leaf, r);
                        if (cell.has_value())
                        {
                            sink.cell(sink.context, column.column, cell.view());
                        }
                    }
                    sink.end_row(sink.context);
                }
            }

            /** Recursive TSD walk: modified subtrees first, then removals
                (Python's row order); Snap iterates the full current state
                and emits no removals. */
            void emit_partition_rows(const TsTableLayout &layout, std::size_t level_index,
                                     const TSInputView &ts, Int mode, const RowScalars &scalars,
                                     const KeyChain *chain, bool emit_removals, const RowSink &sink,
                                     bool reject_empty_frames)
            {
                if (level_index == layout.levels.size())
                {
                    if (layout.is_multi_row)
                    {
                        emit_frame_rows(layout, ts, scalars, chain, sink, reject_empty_frames);
                        return;
                    }
                    sink.cell(sink.context, 0, scalars.now.view());
                    sink.cell(sink.context, 1, scalars.as_of.view());
                    deliver_keys(sink, scalars, chain);
                    write_value_cells(sink, layout, ts, mode);
                    write_child_cells(sink, layout, ts, mode,
                                      scalars.now.view().checked_as<DateTime>(),
                                      scalars.as_of.view().checked_as<DateTime>(), emit_removals,
                                      reject_empty_frames);
                    sink.end_row(sink.context);
                    return;
                }

                const auto &level = layout.levels[level_index];
                auto        dict = const_cast<TSInputView &>(ts).as_dict();

                if (mode == kModeSnap)
                {
                    for (auto &&[key, child] : dict.valid_items())
                    {
                        const KeyChain link{
                            .parent = chain, .level = &level, .key = &key, .removed = false};
                        emit_partition_rows(layout, level_index + 1, child, mode, scalars, &link,
                                            emit_removals, sink, reject_empty_frames);
                    }
                    return;
                }

                for (auto &&[key, child] : dict.modified_items())
                {
                    const KeyChain link{
                        .parent = chain, .level = &level, .key = &key, .removed = false};
                    emit_partition_rows(layout, level_index + 1, child, mode, scalars, &link,
                                        emit_removals, sink, reject_empty_frames);
                }
                if (!emit_removals)
                {
                    return;
                }
                for (const ValueView key : dict.removed_keys())
                {
                    // A removal is emitted HERE, so the row carries the keys
                    // down to this level and no value columns at all - which
                    // needs no clearing, because nothing below was written.
                    const KeyChain link{
                        .parent = chain, .level = &level, .key = &key, .removed = true};
                    sink.cell(sink.context, 0, scalars.now.view());
                    sink.cell(sink.context, 1, scalars.as_of.view());
                    deliver_keys(sink, scalars, &link);
                    sink.end_row(sink.context);
                }
            }

            /** The ``to_table`` sink: materialise the row, because its output
                IS a row-valued time-series. A recording sink appends the same
                cells into Arrow builders instead (RFC 0019, step 2). */
            struct MaterialisingSink
            {
                const TsTableLayout     *layout{nullptr};
                std::vector<Value>      *rows{nullptr};
                std::optional<RowBuffer> row{};

                RowBuffer &current()
                {
                    if (!row.has_value())
                    {
                        row.emplace(*layout);
                    }
                    return *row;
                }

                static void cell(void *context, std::size_t column, const ValueView &value)
                {
                    static_cast<MaterialisingSink *>(context)->current().set_cell(column, value);
                }

                static void end_row(void *context)
                {
                    auto *self = static_cast<MaterialisingSink *>(context);
                    self->rows->push_back(std::move(self->current().value));
                    // A fresh row next time: a column this row never delivered
                    // stays unset, which is what None means here.
                    self->row.reset();
                }

                [[nodiscard]] RowSink sink()
                {
                    return RowSink{.context = this, .cell = &cell, .end_row = &end_row};
                }
            };

            [[nodiscard]] Value build_rows_value(const TsTableLayout &layout,
                                                 std::vector<Value>   rows)
            {
                const auto &row_binding = checked_binding(layout.row_meta, "to_table");
                const auto &rows_binding = checked_binding(layout.rows_meta, "to_table");
                ListBuilder builder{row_binding, *layout.rows_meta};
                for (const Value &row : rows)
                {
                    builder.push_back(row.view());
                }
                return builder.build(rows_binding);
            }
        }  // namespace

        RecordingColumns recording_columns(const TsTableLayout         &layout,
                                           const TableRecordingOptions &options)
        {
            const std::size_t flattened_keys =
                std::accumulate(layout.levels.begin(), layout.levels.end(), std::size_t{0},
                                [](std::size_t total, const TsTableLayout::Level &level) {
                                    return total + level.key_paths.size();
                                });
            if (!options.partition_names.empty() &&
                options.partition_names.size() != flattened_keys)
            {
                throw std::invalid_argument(
                    fmt::format("table recording: {} partition names for {} flattened key columns",
                                options.partition_names.size(), flattened_keys));
            }
            if (!options.removed_names.empty() &&
                options.removed_names.size() != layout.levels.size())
            {
                throw std::invalid_argument(
                    fmt::format("table recording: {} removed names for {} levels",
                                options.removed_names.size(), layout.levels.size()));
            }

            const bool track_removes = options.removes == TableRecordingOptions::Removes::Track;
            const bool keep_as_of = options.as_of != TableRecordingOptions::AsOf::Omit;

            RecordingColumns columns;
            const auto       add = [&](std::size_t source, std::string name) {
                columns.names.push_back(std::move(name));
                columns.metas.push_back(layout.col_metas[source]);
                columns.source.push_back(source);
            };

            add(0, options.date_key.empty() ? layout.date_key : options.date_key);
            if (keep_as_of)
            {
                add(1, options.as_of_key.empty() ? layout.as_of_key : options.as_of_key);
            }

            std::size_t key_column = 0;
            for (std::size_t level_index = 0; level_index < layout.levels.size(); ++level_index)
            {
                const auto &level = layout.levels[level_index];
                if (track_removes)
                {
                    add(level.removed_col, options.removed_names.empty()
                                               ? layout.keys[level.removed_col]
                                               : options.removed_names[level_index]);
                }
                for (std::size_t i = 0; i < level.key_paths.size(); ++i, ++key_column)
                {
                    add(level.first_key_col + i, options.partition_names.empty()
                                                     ? layout.keys[level.first_key_col + i]
                                                     : options.partition_names[key_column]);
                }
            }

            for (std::size_t i = layout.value_col_start; i < layout.keys.size(); ++i)
            {
                // The prefix applies to an expanded frame's columns; a plain
                // value column has nothing to disambiguate from.
                add(i,
                    layout.is_multi_row ? options.frame_prefix + layout.keys[i] : layout.keys[i]);
            }

            std::unordered_set<std::string_view> seen;
            for (const auto &name : columns.names)
            {
                if (!seen.insert(name).second)
                {
                    throw std::invalid_argument(
                        fmt::format("table recording: duplicate column '{}' - rename it or "
                                    "set a frame prefix",
                                    name));
                }
            }
            return columns;
        }

        Frame annotate_recording_projection(
            Frame frame, std::span<const std::optional<std::string>> stored_names)
        {
            if (!frame.has_value())
            {
                throw std::invalid_argument(
                    "record: cannot annotate the projection of an empty frame");
            }
            auto metadata = frame.table->schema()->metadata() != nullptr
                                ? frame.table->schema()->metadata()->Copy()
                                : arrow::key_value_metadata({}, {});
            set_metadata(*metadata, std::string{RECORDING_PROJECTION_VERSION_KEY},
                         std::string{RECORDING_PROJECTION_VERSION});
            set_metadata(*metadata, std::string{RECORDING_PROJECTION_COUNT_KEY},
                         std::to_string(stored_names.size()));
            for (std::size_t column = 0; column < stored_names.size(); ++column)
            {
                set_metadata(*metadata, projection_present_key(column),
                             stored_names[column].has_value() ? "1" : "0");
                if (stored_names[column].has_value())
                {
                    set_metadata(*metadata, projection_column_key(column), *stored_names[column]);
                }
            }
            frame.table = frame.table->ReplaceSchemaMetadata(std::move(metadata));
            return frame;
        }

        namespace
        {
            void emit_plain(const TableLayout &layout, const TSInputView &ts, Int mode,
                            DateTime now, DateTime as_of, bool emit_removals,
                            const TableRowSink &sink, bool reject_empty_frames)
            {
                const RowScalars scalars{now, as_of};
                sink.cell(sink.context, 0, scalars.now.view());
                sink.cell(sink.context, 1, scalars.as_of.view());
                write_value_cells(sink, layout, ts, mode);
                write_child_cells(sink, layout, ts, mode, now, as_of, emit_removals,
                                  reject_empty_frames);
                sink.end_row(sink.context);
            }

            void emit_partitioned(const TableLayout &layout, const TSInputView &ts, Int mode,
                                  DateTime now, DateTime as_of, bool emit_removals,
                                  const TableRowSink &sink, bool reject_empty_frames)
            {
                const RowScalars scalars{now, as_of};
                emit_partition_rows(layout, 0, ts, mode, scalars, nullptr, emit_removals, sink,
                                    reject_empty_frames);
            }

            void emit_frame(const TableLayout &layout, const TSInputView &ts, Int, DateTime now,
                            DateTime as_of, bool, const TableRowSink &sink,
                            bool reject_empty_frames)
            {
                const RowScalars scalars{now, as_of};
                emit_frame_rows(layout, ts, scalars, nullptr, sink, reject_empty_frames);
            }
        }  // namespace

        void emit_rows_to(const TsTableLayout &layout, const TSInputView &ts, Int mode,
                          DateTime now, DateTime as_of, bool emit_removals, const RowSink &sink,
                          bool reject_empty_frames)
        {
            layout.ops->emit(layout, ts, mode, now, as_of, emit_removals, sink,
                             reject_empty_frames);
        }

        void emit_rows(const TsTableLayout &layout, const TSInputView &ts, Int mode, DateTime now,
                       DateTime as_of, const TSOutputView &out)
        {
            std::vector<Value> rows;
            MaterialisingSink  materialise{.layout = &layout, .rows = &rows};
            const RowSink      sink = materialise.sink();
            // to_table always reports removals: they are part of the row stream
            // it produces, whatever a recording later chooses to keep.
            emit_rows_to(layout, ts, mode, now, as_of, true, sink);

            if (!layout.multi())
            {
                apply_current_value(out, rows.front().view());
                return;
            }
            if (rows.empty())
            {
                return;
            }
            Value value = build_rows_value(layout, std::move(rows));
            apply_current_value(out, value.view());
        }

        namespace
        {
            // ---------------------------------------------------------------
            // Row application (from_table)
            // ---------------------------------------------------------------

            [[nodiscard]] Value read_key(const TsTableLayout::Level &level, const TupleView &row)
            {
                const auto &binding = checked_binding(level.key_meta, "from_table");
                Value       key{binding};
                if (level.key_paths.size() == 1 && level.key_paths.front().empty())
                {
                    // Atomic key: the single key cell IS the key value.
                    ValueView dest = key.view().begin_mutation();
                    assign_cell(dest, row.at(level.first_key_col));
                    return key;
                }
                for (std::size_t i = 0; i < level.key_paths.size(); ++i)
                {
                    const ValueView &cell = row.at(level.first_key_col + i);
                    if (!cell.has_value())
                    {
                        continue;
                    }
                    assign_cell(
                        walk_mutable(key.view().begin_mutation(),
                                     level.key_paths[i]),
                        cell);
                }
                return key;
            }

            [[nodiscard]] TSOutputView child_output(const TSOutputView &root,
                                                    std::span<const std::size_t> path)
            {
                TSOutputView child = root.borrowed_ref();
                for (const std::size_t index : path)
                {
                    child = child.indexed_child_at(index);
                }
                return child;
            }

            struct ChildRowSource
            {
                const TableLayout::ChildPlan *plan{nullptr};
                const TupleView              *row{nullptr};

                static Value cell(const void *context, std::size_t row_index,
                                  std::size_t column)
                {
                    const auto &self = *static_cast<const ChildRowSource *>(context);
                    if (row_index != 0 || column >= self.plan->columns.size())
                    {
                        throw std::out_of_range(
                            "from_table: registered child row or column is out of range");
                    }
                    const std::size_t parent_column = self.plan->columns[column];
                    if (parent_column == TableLayout::no_column)
                    {
                        return {};
                    }
                    const ValueView value = self.row->at(parent_column);
                    return value.has_value() ? Value{value} : Value{};
                }

                [[nodiscard]] TableRowSource source() const
                {
                    return TableRowSource{.context = this, .rows = 1, .cell = &cell};
                }
            };

            void apply_child_plans(const TsTableLayout &layout, const TupleView &row,
                                   const TSOutputView &out)
            {
                for (const TableLayout::ChildPlan &plan : layout.child_plans)
                {
                    const ValueView &present = row.at(plan.presence_column);
                    if (!present.has_value() || !present.checked_as<Bool>())
                    {
                        continue;
                    }

                    const ChildRowSource projection{.plan = &plan, .row = &row};
                    const TableRowSource source = projection.source();
                    TSOutputView child = child_output(out, plan.ts_path);
                    plan.layout->ops->apply(*plan.layout, source, child);
                }
            }

            void apply_leaf_row(const TsTableLayout &layout, const TupleView &row,
                                const TSOutputView &out)
            {
                const auto *leaf_ts = layout.leaf_ts;
                if (leaf_ts->kind == TSTypeKind::TS && layout.value_cols.size() == 1 &&
                    layout.value_cols.front().value_path.empty() &&
                    layout.value_cols.front().ts_path.empty())
                {
                    const ValueView &cell = row.at(layout.value_cols.front().column);
                    if (cell.has_value())
                    {
                        apply_current_value(out, cell);
                    }
                    return;
                }

                // Compound leaf: rebuild a (possibly partial) value and apply
                // it as the tick's delta (unset fields stay unset).
                if (!layout.value_cols.empty())
                {
                    Value value{checked_binding(leaf_ts->value_schema, "from_table")};
                    bool  any = false;
                    for (std::size_t i = 0; i < layout.value_cols.size(); ++i)
                    {
                        const auto      &column = layout.value_cols[i];
                        const ValueView &cell = row.at(column.column);
                        if (!cell.has_value())
                        {
                            continue;
                        }
                        std::vector<std::size_t> full_path = column.ts_path;
                        full_path.insert(full_path.end(), column.value_path.begin(),
                                         column.value_path.end());
                        assign_cell(
                            walk_mutable(value.view().begin_mutation(),
                                         full_path),
                            cell);
                        any = true;
                    }
                    if (any)
                    {
                        apply_delta(out, value.view());
                    }
                }
                apply_child_plans(layout, row, out);
            }

            void apply_partition_row(const TsTableLayout &layout, std::size_t level_index,
                                     const TupleView &row, const TSOutputView &out)
            {
                if (level_index == layout.levels.size())
                {
                    apply_leaf_row(layout, row, out);
                    return;
                }
                const auto &level = layout.levels[level_index];
                auto        dict = out.as_dict();
                auto        mutation = dict.begin_mutation(out.evaluation_time());
                Value       key = read_key(level, row);

                const ValueView &removed = row.at(level.removed_col);
                if (removed.has_value() && removed.checked_as<Bool>())
                {
                    static_cast<void>(mutation.erase(key.view()));
                    return;
                }
                auto element = mutation.at(key.view());
                apply_partition_row(layout, level_index + 1, row,
                                    TSOutputView{out.output(), element, out.evaluation_time()});
            }

            [[nodiscard]] bool same_partition_key(const TsTableLayout &layout, const TupleView &lhs,
                                                  const TupleView &rhs)
            {
                for (const auto &level : layout.levels)
                {
                    for (std::size_t i = 0; i < level.key_paths.size(); ++i)
                    {
                        const ValueView &left = lhs.at(level.first_key_col + i);
                        const ValueView &right = rhs.at(level.first_key_col + i);
                        if (left.has_value() != right.has_value())
                        {
                            return false;
                        }
                        if (left.has_value() && !left.equals(right))
                        {
                            return false;
                        }
                    }
                }
                return true;
            }

            void apply_frame_rows_value(const TsTableLayout       &layout,
                                        std::span<const ValueView> rows, const TSOutputView &out)
            {
                Frame frame = frame_from_rows(*layout.frame_converter, rows,
                                              /*first_column=*/layout.value_col_start);
                Value boxed{checked_binding(out.schema()->value_schema, "from_table")};
                *static_cast<Frame *>(const_cast<void *>(boxed.view().data())) = std::move(frame);
                apply_current_value(out, boxed.view());
            }

            [[nodiscard]] std::size_t apply_partitioned_frame_rows(const TsTableLayout &layout,
                                                                   std::span<const ValueView> rows,
                                                                   std::size_t                first,
                                                                   std::size_t         level_index,
                                                                   const TSOutputView &out)
            {
                const TupleView row = rows[first].as_tuple();
                if (level_index == layout.levels.size())
                {
                    std::size_t count = 1;
                    while (first + count < rows.size() &&
                           same_partition_key(layout, row, rows[first + count].as_tuple()))
                    {
                        ++count;
                    }
                    apply_frame_rows_value(layout, rows.subspan(first, count), out);
                    return count;
                }

                const auto &level = layout.levels[level_index];
                auto        dict = out.as_dict();
                auto        mutation = dict.begin_mutation(out.evaluation_time());
                Value       key = read_key(level, row);

                const ValueView &removed = row.at(level.removed_col);
                if (removed.has_value() && removed.checked_as<Bool>())
                {
                    static_cast<void>(mutation.erase(key.view()));
                    return 1;
                }
                auto element = mutation.at(key.view());
                return apply_partitioned_frame_rows(
                    layout, rows, first, level_index + 1,
                    TSOutputView{out.output(), element, out.evaluation_time()});
            }
        }  // namespace

        namespace
        {
            /** ``assemble_from_paths`` at one nesting depth.
                ``paths``/``leaves`` are the entries that reach this node, and
                ``flatten_value`` emitted them in field order, so the entries
                for one field are contiguous. */
            [[nodiscard]] Value assemble_at(const ValueTypeMetaData                  *meta,
                                            std::span<const std::vector<std::size_t>> paths,
                                            std::span<const Value> leaves, std::size_t depth)
            {
                if (meta == nullptr)
                {
                    throw std::invalid_argument("replay: cannot rebuild a key with no schema");
                }
                if (meta->value_kind() == ValueTypeKind::Atomic)
                {
                    // An atomic node is reached by exactly one column, so the
                    // uncompounded key falls out of the same walk.
                    if (leaves.size() != 1 || !leaves.front().has_value())
                    {
                        throw std::invalid_argument(
                            "replay: recorded key column is empty, so the key cannot be rebuilt");
                    }
                    return Value{leaves.front().view()};
                }

                BundleBuilder builder{ValuePlanFactory::instance().type_for(meta)};
                std::size_t   i = 0;
                while (i < paths.size())
                {
                    const std::size_t field = paths[i][depth];
                    std::size_t       j = i;
                    while (j < paths.size() && paths[j][depth] == field)
                    {
                        ++j;
                    }
                    builder.set(field,
                                assemble_at(meta->fields[field].type, paths.subspan(i, j - i),
                                            leaves.subspan(i, j - i), depth + 1));
                    i = j;
                }
                return builder.build();
            }
        }  // namespace


        std::vector<int> resolve_replay_columns(const Frame &frame, const TsTableLayout &layout,
                                                std::span<const std::string> stored_names,
                                                bool as_of_named, bool removes_named)
        {
            if (!frame.has_value())
            {
                throw std::invalid_argument("replay: cannot resolve columns of an empty frame");
            }
            // The layout's row always opens with the bitemporal pair, so the
            // as-of is column 1 (``build_layout`` pushes date then as-of before
            // any level).
            constexpr std::size_t as_of_column = 1;
            const auto is_removed_column = [&](std::size_t column) {
                return std::any_of(layout.levels.begin(), layout.levels.end(),
                                   [column](const TsTableLayout::Level &level) {
                                       return column == level.removed_col;
                                   });
            };
            const auto optional_unnamed = [&](std::size_t column) {
                if (column == as_of_column) { return !as_of_named; }
                return is_removed_column(column) && !removes_named;
            };

            const auto      &schema = *frame.table->schema();
            const auto       recorded = recording_projection(frame, layout.keys.size());
            std::vector<int> columns(layout.keys.size(), -1);
            for (std::size_t column = 0; column < layout.keys.size(); ++column)
            {
                const std::string &canonical = layout.keys[column];
                const std::string &requested =
                    column < stored_names.size() && !stored_names[column].empty()
                        ? stored_names[column]
                        : canonical;

                std::string_view stored = requested;
                if (recorded.has_value())
                {
                    const std::optional<std::string> &declared = (*recorded)[column];
                    if (!declared.has_value())
                    {
                        if (optional_unnamed(column)) { continue; }
                        throw std::runtime_error(
                            "replay: recording omits column '" + requested + "' for '" +
                            canonical +
                            "'; the projection supplied to replay does not match the recording");
                    }
                    if (*declared != requested)
                    {
                        throw std::runtime_error(
                            "replay: recording has no column '" + requested + "' for '" +
                            canonical + "'; its stored projection names that column '" + *declared +
                            "'; the projection supplied to replay must match the one used to record");
                    }
                    stored = *declared;
                }

                const int index = schema.GetFieldIndex(std::string{stored});
                if (index < 0)
                {
                    // Only an unannotated legacy or hand-built frame reaches
                    // this compatibility case. Current hgraph recordings say
                    // explicitly whether the optional column was omitted.
                    if (!recorded.has_value() && optional_unnamed(column)) { continue; }
                    throw std::runtime_error(
                        "replay: recording has no column '" + std::string{stored} + "' for '" +
                        canonical +
                        "'; the projection supplied to replay must match the one used to record");
                }
                columns[column] = index;
            }

            // Two layout columns resolving to one stored column would read the
            // same data under two meanings. A projection should never produce
            // that, and a hand-built table cannot be trusted not to.
            for (std::size_t lhs = 0; lhs < columns.size(); ++lhs)
            {
                if (columns[lhs] < 0) { continue; }
                for (std::size_t rhs = lhs + 1; rhs < columns.size(); ++rhs)
                {
                    if (columns[lhs] == columns[rhs])
                    {
                        throw std::runtime_error("replay: '" + layout.keys[lhs] + "' and '" +
                                                 layout.keys[rhs] +
                                                 "' both resolve to stored column '" +
                                                 schema.field(columns[lhs])->name() + "'");
                    }
                }
            }
            return columns;
        }

        Value assemble_from_paths(const ValueTypeMetaData                  *meta,
                                  std::span<const std::vector<std::size_t>> paths,
                                  std::span<const Value>                    leaves)
        {
            if (paths.size() != leaves.size())
            {
                throw std::invalid_argument(
                    "replay: key column count does not match the key layout");
            }
            return assemble_at(meta, paths, leaves, 0);
        }

        namespace
        {
            struct ValueRowSource
            {
                const TableLayout *layout{nullptr};
                const ValueView   *value{nullptr};

                static Value cell(const void *context, std::size_t row, std::size_t column)
                {
                    const auto &self = *static_cast<const ValueRowSource *>(context);
                    // Both arms must yield the SAME type. `at` returns a const ValueView by
                    // value and `reborrow` a non-const one; a conditional mixing the two has
                    // to convert one prvalue to the other's type, which needs the copy
                    // constructor ValueView deletes. Clang, GCC and MSVC 14.51 accept it
                    // regardless; MSVC 14.44 rejects it with C2280, so the wheel did not build
                    // under VS 2022 17.14. Reborrowing both arms is the idiom this file
                    // already uses for `at` results (see walk_value) and needs no constructor
                    // at all - same-type prvalues elide.
                    const ValueView row_value = self.layout->multi()
                                                    ? reborrow(self.value->as_list().at(row))
                                                    : reborrow(*self.value);
                    const ValueView item = row_value.as_tuple().at(column);
                    return item.has_value() ? Value{item} : Value{};
                }

                [[nodiscard]] TableRowSource source() const
                {
                    return TableRowSource{.context = this,
                                          .rows = layout->multi() ? value->as_list().size()
                                                                  : std::size_t{1},
                                          .cell = &cell};
                }
            };

            [[nodiscard]] Value materialize_row(const TableLayout    &layout,
                                                const TableRowSource &source, std::size_t row)
            {
                Value value{checked_binding(layout.row_meta, "from_table")};
                auto  tuple = value.as_tuple().begin_mutation();
                for (std::size_t column = 0; column < layout.keys.size(); ++column)
                {
                    Value cell = source.cell(source.context, row, column);
                    if (cell.has_value())
                    {
                        assign_cell(tuple.at(column), cell.view());
                    }
                }
                return value;
            }

            void apply_plain(const TableLayout &layout, const TableRowSource &source,
                             const TSOutputView &out)
            {
                if (source.rows == 0)
                {
                    return;
                }
                Value row = materialize_row(layout, source, 0);
                apply_leaf_row(layout, row.view().as_tuple(), out);
            }

            void apply_partitioned(const TableLayout &layout, const TableRowSource &source,
                                   const TSOutputView &out)
            {
                std::vector<Value> owned_rows;
                owned_rows.reserve(source.rows);
                std::vector<ValueView> rows;
                rows.reserve(source.rows);
                for (std::size_t row = 0; row < source.rows; ++row)
                {
                    owned_rows.push_back(materialize_row(layout, source, row));
                    rows.push_back(owned_rows.back().view());
                }

                if (layout.is_multi_row)
                {
                    std::size_t first = 0;
                    while (first < rows.size())
                    {
                        first += apply_partitioned_frame_rows(
                            layout, std::span<const ValueView>{rows}, first, 0, out);
                    }
                    return;
                }
                for (ValueView &row : rows)
                {
                    apply_partition_row(layout, 0, row.as_tuple(), out);
                }
            }

            void apply_frame(const TableLayout &layout, const TableRowSource &source,
                             const TSOutputView &out)
            {
                std::vector<Value> owned_rows;
                owned_rows.reserve(source.rows);
                std::vector<ValueView> rows;
                rows.reserve(source.rows);
                for (std::size_t row = 0; row < source.rows; ++row)
                {
                    owned_rows.push_back(materialize_row(layout, source, row));
                    rows.push_back(owned_rows.back().view());
                }
                apply_frame_rows_value(layout, std::span<const ValueView>{rows}, out);
            }
        }  // namespace

        void apply_rows(const TsTableLayout &layout, const ValueView &value,
                        const TSOutputView &out)
        {
            const ValueRowSource rows{.layout = &layout, .value = &value};
            const TableRowSource source = rows.source();
            layout.ops->apply(layout, source, out);
        }
    }  // namespace table_ts_detail

    void register_table_operators()
    {
        // Layouts intern by TS-schema POINTER (the plan-registries rule).
        // ts_table_layout() drops the cache itself when the registry
        // generation moves, so this is belt-and-braces rather than the only
        // guard - registration is not on the path a layout build must take.
        table_ts_detail::clear_ts_table_layouts();
        static_cast<void>(table_ts_detail::to_table_mode_meta());  // register the mode enum
        register_overload<to_table, to_table_rows_impl>();
        register_overload<from_table, from_table_rows_impl>();
        register_overload<from_table_const, from_table_const_impl>();
    }
}  // namespace hgraph::stdlib

namespace hgraph
{
    namespace
    {
        void unsupported_describe(TableLayout &, const TSValueTypeMetaData *schema, std::string,
                                  std::vector<std::size_t>, std::size_t)
        {
            throw std::invalid_argument(fmt::format("to_table: unsupported time-series schema '{}'",
                                                    schema != nullptr && !schema->name().empty()
                                                        ? schema->name()
                                                        : std::string_view{"?"}));
        }

        void unsupported_emit(const TableLayout &, const TSInputView &, Int, DateTime, DateTime,
                              bool, const TableRowSink &, bool)
        {
            throw std::logic_error("table layout has no emit operation");
        }

        void unsupported_apply(const TableLayout &, const TableRowSource &, const TSOutputView &)
        {
            throw std::logic_error("table layout has no apply operation");
        }
    }  // namespace

    const TableTypeOps &empty_table_type_ops() noexcept
    {
        static const TableTypeOps ops{&unsupported_describe, &unsupported_emit, &unsupported_apply};
        return ops;
    }

    void register_table_type_ops(const TSValueTypeMetaData *schema, const TableTypeOps &ops)
    {
        if (schema == nullptr)
        {
            throw std::invalid_argument("table type ops require a schema");
        }
        if (ops.describe == nullptr || ops.emit == nullptr || ops.apply == nullptr)
        {
            throw std::invalid_argument("table type ops must provide describe, emit and apply");
        }
        g_table_type_ops_overrides[schema] = &ops;
        stdlib::table_ts_detail::clear_ts_table_layouts();
    }

    const TableTypeOps &table_type_ops(const TSValueTypeMetaData *schema)
    {
        if (const auto it = g_table_type_ops_overrides.find(schema);
            it != g_table_type_ops_overrides.end())
        {
            return *it->second;
        }
        return stdlib::table_ts_detail::builtin_table_type_ops(schema);
    }

    void clear_table_type_ops() noexcept
    {
        g_table_type_ops_overrides.clear();
        stdlib::table_ts_detail::clear_ts_table_layouts();
    }
}  // namespace hgraph
