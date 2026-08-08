#include <hgraph/lib/std/operators/impl/table_impl.h>

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/time_series/ts_input/bundle_view.h>
#include <hgraph/types/time_series/ts_input/dict_view.h>
#include <hgraph/types/time_series/ts_output/dict_view.h>
#include <hgraph/types/value/compact_storage.h>
#include <hgraph/types/value/specialized_views.h>
#include <hgraph/types/value/value_builder.h>

#include <fmt/format.h>

#include <memory>
#include <span>
#include <unordered_map>

namespace hgraph::stdlib
{
    namespace table_ts_detail
    {
        namespace
        {
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
                    seed ^= std::hash<std::string>{}(key.date_key) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
                    seed ^= std::hash<std::string>{}(key.as_of_key) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
                    return seed;
                }
            };

            std::unordered_map<LayoutKey, std::unique_ptr<TsTableLayout>, LayoutKeyHash> g_layouts;

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
                if (meta != nullptr &&
                    (meta->value_kind() == ValueTypeKind::Bundle || meta->value_kind() == ValueTypeKind::Tuple))
                {
                    for (std::size_t i = 0; i < meta->field_count; ++i)
                    {
                        const auto &field = meta->fields[i];
                        std::string name  = meta->value_kind() == ValueTypeKind::Bundle && field.name != nullptr
                                                ? std::string{field.name}
                                                : fmt::format("{}", i);
                        auto        child_path = path;
                        child_path.push_back(i);
                        flatten_value(field.type, suffix.empty() ? name : suffix + "." + name,
                                      std::move(child_path), sink);
                    }
                    return;
                }
                throw std::invalid_argument(
                    fmt::format("to_table: unsupported value kind for column '{}' ('{}')",
                                suffix.empty() ? "value" : suffix,
                                meta != nullptr && !meta->name().empty() ? meta->name() : std::string_view{"?"}));
            }

            /** Flatten the LEAF time-series (below all TSD levels): TSB
                recurses over child time-series, a plain TS flattens its
                value schema. */
            template <typename Sink>
            void flatten_leaf_ts(const TSValueTypeMetaData *ts, const std::string &suffix,
                                 std::vector<std::size_t> ts_path, Sink &&sink)
            {
                if (ts == nullptr) { throw std::invalid_argument("to_table: null time-series schema"); }
                if (ts->kind == TSTypeKind::TSB)
                {
                    const auto *bundle = ts->value_schema;
                    for (std::size_t i = 0; i < ts->field_count(); ++i)
                    {
                        const auto *child = ts->fields()[i].type;
                        const char *name  = bundle->fields[i].name;
                        auto        child_ts_path = ts_path;
                        child_ts_path.push_back(i);
                        flatten_leaf_ts(child,
                                        suffix.empty() ? std::string{name != nullptr ? name : ""}
                                                       : suffix + "." + (name != nullptr ? name : ""),
                                        std::move(child_ts_path), sink);
                    }
                    return;
                }
                if (ts->kind == TSTypeKind::TS)
                {
                    flatten_value(ts->value_schema, suffix, {},
                                  [&](const std::string &value_suffix, const ValueTypeMetaData *leaf,
                                      std::vector<std::size_t> value_path) {
                                      sink(value_suffix, leaf, ts_path, std::move(value_path));
                                  });
                    return;
                }
                throw std::invalid_argument(
                    fmt::format("to_table: unsupported leaf time-series kind for '{}'",
                                ts != nullptr && !ts->name().empty() ? ts->name() : std::string_view{"?"}));
            }

            [[nodiscard]] bool is_frame_meta(const ValueTypeMetaData *meta)
            {
                return TypeRegistry::instance().is_frame(meta);
            }

            [[nodiscard]] const TsTableLayout *build_layout(const TSValueTypeMetaData *ts,
                                                            std::string_view           date_key,
                                                            std::string_view           as_of_key)
            {
                auto layout       = std::make_unique<TsTableLayout>();
                layout->ts_schema = ts;
                layout->date_key  = std::string{date_key};
                layout->as_of_key = std::string{as_of_key};

                const auto *datetime_meta = TypeRegistry::instance().value_type("datetime");
                const auto *bool_meta     = TypeRegistry::instance().value_type("bool");

                layout->keys.push_back(layout->date_key);
                layout->col_metas.push_back(datetime_meta);
                layout->keys.push_back(layout->as_of_key);
                layout->col_metas.push_back(datetime_meta);

                // TSD nesting levels contribute removed + key columns.
                const TSValueTypeMetaData *cursor   = ts;
                std::size_t                level_no = 1;
                while (cursor->kind == TSTypeKind::TSD)
                {
                    TsTableLayout::Level level;
                    level.key_meta    = cursor->key_type();
                    level.removed_col = layout->keys.size();
                    const std::string removed_name = fmt::format("__key_{}_removed__", level_no);
                    layout->keys.push_back(removed_name);
                    layout->col_metas.push_back(bool_meta);
                    layout->removed_keys.push_back(removed_name);

                    level.first_key_col = layout->keys.size();
                    flatten_value(level.key_meta, "", {},
                                  [&](const std::string &suffix, const ValueTypeMetaData *leaf,
                                      std::vector<std::size_t> path) {
                                      const std::string name =
                                          suffix.empty() ? fmt::format("__key_{}__", level_no)
                                                         : fmt::format("__key_{}_{}__", level_no, suffix);
                                      layout->keys.push_back(name);
                                      layout->col_metas.push_back(leaf);
                                      layout->partition_keys.push_back(name);
                                      level.key_paths.push_back(std::move(path));
                                  });
                    layout->levels.push_back(std::move(level));
                    cursor = cursor->element_ts();
                    ++level_no;
                }
                layout->leaf_ts = cursor;

                layout->value_col_start = layout->keys.size();
                if (cursor->kind == TSTypeKind::TS && is_frame_meta(cursor->value_schema))
                {
                    // Frame payload: one output row per frame row.
                    if (!layout->levels.empty())
                    {
                        throw std::invalid_argument(
                            "to_table: Frame payloads under a TSD are not supported yet");
                    }
                    layout->is_multi_row = true;
                    // The typed Frame carries its column bundle on
                    // element_type (TypeRegistry::frame); an untyped frame
                    // cannot name columns.
                    const auto *columns = cursor->value_schema->element_type;
                    if (columns == nullptr)
                    {
                        throw std::invalid_argument(
                            "to_table: an untyped Frame input cannot derive table columns "
                            "(use Frame[Schema])");
                    }
                    layout->frame_converter = &table_converter(columns, date_key, as_of_key);
                    for (const auto &column : layout->frame_converter->columns)
                    {
                        TsTableLayout::Column entry;
                        entry.name       = column.name;
                        entry.leaf       = column.leaf_meta;
                        entry.value_path = column.path;
                        layout->keys.push_back(column.name);
                        layout->col_metas.push_back(column.leaf_meta);
                        layout->value_cols.push_back(std::move(entry));
                    }
                }
                else
                {
                    flatten_leaf_ts(cursor, "", {},
                                    [&](const std::string &suffix, const ValueTypeMetaData *leaf,
                                        std::vector<std::size_t> ts_path,
                                        std::vector<std::size_t> value_path) {
                                        TsTableLayout::Column entry;
                                        entry.name       = suffix.empty() ? "value" : suffix;
                                        entry.leaf       = leaf;
                                        entry.ts_path    = std::move(ts_path);
                                        entry.value_path = std::move(value_path);
                                        layout->keys.push_back(entry.name);
                                        layout->col_metas.push_back(leaf);
                                        layout->value_cols.push_back(std::move(entry));
                                    });
                }

                auto &registry    = TypeRegistry::instance();
                layout->row_meta  = registry.tuple(layout->col_metas);
                layout->rows_meta = registry.list(layout->row_meta, 0, /*variadic_tuple=*/true);
                layout->output_ts = registry.ts(layout->multi() ? layout->rows_meta : layout->row_meta);

                const auto *raw = layout.get();
                g_layouts.emplace(LayoutKey{ts, std::string{date_key}, std::string{as_of_key}},
                                  std::move(layout));
                return raw;
            }
        }  // namespace

        const TsTableLayout &ts_table_layout(const TSValueTypeMetaData *ts, std::string_view date_key,
                                             std::string_view as_of_key)
        {
            const LayoutKey key{ts, std::string{date_key}, std::string{as_of_key}};
            if (const auto it = g_layouts.find(key); it != g_layouts.end()) { return *it->second; }
            return *build_layout(ts, date_key, as_of_key);
        }

        void clear_ts_table_layouts() noexcept { g_layouts.clear(); }

        const ValueTypeMetaData *to_table_mode_meta()
        {
            return TypeRegistry::instance().enum_type("ToTableMode",
                                                      {{"Tick", 1}, {"Sample", 2}, {"Snap", 3}});
        }

        Value to_table_mode_value(Int member)
        {
            const auto *meta    = to_table_mode_meta();
            const auto binding = ValuePlanFactory::instance().type_for(meta);
            if (binding == nullptr) { throw std::logic_error("ToTableMode has no value binding"); }
            Value value{binding};
            *static_cast<Int *>(const_cast<void *>(value.view().data())) = member;
            return value;
        }

        namespace
        {
            constexpr Int kModeTick = 1;
            constexpr Int kModeSnap = 3;

            // ---------------------------------------------------------------
            // Row building
            // ---------------------------------------------------------------

            [[nodiscard]] ValueTypeRef checked_binding(const ValueTypeMetaData *meta,
                                                                  const char              *what)
            {
                const auto binding = ValuePlanFactory::instance().type_for(meta);
                if (binding == nullptr)
                {
                    throw std::logic_error(fmt::format("{}: schema has no value binding", what));
                }
                return binding;
            }

            /** A fresh borrow over the same binding + memory (the read views
                are move-only; ``at`` results are const). */
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
                    if (!view.has_value()) { return view; }
                    if (view.is_tuple())
                    {
                        auto tuple = view.as_tuple();
                        view       = reborrow(tuple.at(index));
                    }
                    else
                    {
                        auto bundle = view.as_bundle();
                        view        = reborrow(bundle.at(index));
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
                        view     = mut.at(index);
                    }
                    else
                    {
                        auto mut = view.as_bundle().begin_mutation();
                        view     = mut.at(index);
                    }
                }
                return view;
            }

            struct RowBuffer
            {
                explicit RowBuffer(const TsTableLayout &layout)
                    : value{checked_binding(layout.row_meta, "to_table")}
                {
                }

                RowBuffer(const RowBuffer &other) : value{Value{other.value.view()}} {}
                RowBuffer(RowBuffer &&) noexcept            = default;
                RowBuffer &operator=(RowBuffer &&) noexcept = default;

                void set_cell(std::size_t index, const ValueView &leaf)
                {
                    if (!leaf.has_value()) { return; }   // unset cell = None
                    ValueView root = value.view();
                    auto      mut  = root.as_tuple().begin_mutation();
                    ValueView cell = mut.at(index);
                    cell.copy_from(leaf);
                }

                template <typename T>
                void set_scalar(std::size_t index, T scalar)
                {
                    Value cell{std::move(scalar)};
                    set_cell(index, cell.view());
                }

                Value value;
            };

            [[nodiscard]] RowBuffer make_row(const TsTableLayout &layout, DateTime now, DateTime as_of)
            {
                RowBuffer row{layout};
                row.set_scalar(0, now);
                row.set_scalar(1, as_of);
                return row;
            }

            void write_level_keys(RowBuffer &row, const TsTableLayout::Level &level, const ValueView &key)
            {
                for (std::size_t i = 0; i < level.key_paths.size(); ++i)
                {
                    row.set_cell(level.first_key_col + i, walk_value(reborrow(key), level.key_paths[i]));
                }
            }

            /** Write the leaf value columns of ``row`` from the leaf TS view.
                Tick mode writes modified nodes only; Sample/Snap write every
                valid node. */
            void write_value_cells(RowBuffer &row, const TsTableLayout &layout, const TSInputView &leaf,
                                   Int mode)
            {
                for (std::size_t i = 0; i < layout.value_cols.size(); ++i)
                {
                    const auto &column = layout.value_cols[i];
                    TSInputView node   = leaf.borrowed_ref();
                    for (const std::size_t index : column.ts_path)
                    {
                        auto bundle = node.as_bundle();
                        node        = bundle.at(index);
                    }
                    if (mode == kModeTick && !node.modified()) { continue; }
                    if (!node.valid()) { continue; }
                    row.set_cell(layout.value_col_start + i, walk_value(node.value(), column.value_path));
                }
            }

            void emit_frame_rows(const TsTableLayout &layout, const TSInputView &leaf, DateTime now,
                                 DateTime as_of, std::vector<Value> &rows)
            {
                const ValueView view  = leaf.value();
                const Frame    &frame = view.checked_as<Frame>();
                if (!frame.has_value()) { return; }
                for (std::int64_t r = 0; r < frame_rows(frame); ++r)
                {
                    Value     row_value = read_row(*layout.frame_converter, frame, r);
                    RowBuffer row       = make_row(layout, now, as_of);
                    for (std::size_t i = 0; i < layout.value_cols.size(); ++i)
                    {
                        row.set_cell(layout.value_col_start + i,
                                     walk_value(row_value.view(), layout.value_cols[i].value_path));
                    }
                    rows.push_back(std::move(row.value));
                }
            }

            /** Recursive TSD walk: modified subtrees first, then removals
                (Python's row order); Snap iterates the full current state
                and emits no removals. */
            void emit_partition_rows(const TsTableLayout &layout, std::size_t level_index,
                                     const TSInputView &ts, Int mode, DateTime now, DateTime as_of,
                                     const RowBuffer &prototype, std::vector<Value> &rows)
            {
                if (level_index == layout.levels.size())
                {
                    RowBuffer row = prototype;
                    write_value_cells(row, layout, ts, mode);
                    rows.push_back(std::move(row.value));
                    return;
                }

                const auto &level = layout.levels[level_index];
                auto        dict  = const_cast<TSInputView &>(ts).as_dict();

                if (mode == kModeSnap)
                {
                    for (auto &&[key, child] : dict.valid_items())
                    {
                        RowBuffer row = prototype;
                        row.set_scalar(level.removed_col, Bool{false});
                        write_level_keys(row, level, key);
                        emit_partition_rows(layout, level_index + 1, child, mode, now, as_of, row, rows);
                    }
                    return;
                }

                for (auto &&[key, child] : dict.modified_items())
                {
                    RowBuffer row = prototype;
                    row.set_scalar(level.removed_col, Bool{false});
                    write_level_keys(row, level, key);
                    emit_partition_rows(layout, level_index + 1, child, mode, now, as_of, row, rows);
                }
                for (const ValueView key : dict.removed_keys())
                {
                    RowBuffer row = prototype;
                    row.set_scalar(level.removed_col, Bool{true});
                    write_level_keys(row, level, key);
                    rows.push_back(std::move(row.value));
                }
            }

            [[nodiscard]] Value build_rows_value(const TsTableLayout &layout, std::vector<Value> rows)
            {
                const auto &row_binding  = checked_binding(layout.row_meta, "to_table");
                const auto &rows_binding = checked_binding(layout.rows_meta, "to_table");
                ListBuilder builder{row_binding};
                for (const Value &row : rows) { builder.push_back_copy(row.view().data()); }
                Value out{rows_binding};
                *static_cast<ListStorage *>(const_cast<void *>(out.view().data())) =
                    builder.build_storage();
                return out;
            }
        }  // namespace

        void emit_rows(const TsTableLayout &layout, const TSInputView &ts, Int mode, DateTime now,
                       DateTime as_of, const TSOutputView &out)
        {
            if (!layout.multi())
            {
                RowBuffer row = make_row(layout, now, as_of);
                write_value_cells(row, layout, ts, mode);
                apply_current_value(out, row.value.view());
                return;
            }

            std::vector<Value> rows;
            if (layout.partitioned())
            {
                const RowBuffer prototype = make_row(layout, now, as_of);
                emit_partition_rows(layout, 0, ts, mode, now, as_of, prototype, rows);
            }
            else { emit_frame_rows(layout, ts, now, as_of, rows); }
            if (rows.empty()) { return; }
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
                    dest.copy_from(row.at(level.first_key_col));
                    return key;
                }
                for (std::size_t i = 0; i < level.key_paths.size(); ++i)
                {
                    const ValueView &cell = row.at(level.first_key_col + i);
                    if (!cell.has_value()) { continue; }
                    walk_mutable(key.view().begin_mutation(), level.key_paths[i]).copy_from(cell);
                }
                return key;
            }

            void apply_leaf_row(const TsTableLayout &layout, const TupleView &row, const TSOutputView &out)
            {
                const auto *leaf_ts = layout.leaf_ts;
                if (leaf_ts->kind == TSTypeKind::TS && layout.value_cols.size() == 1 &&
                    layout.value_cols.front().value_path.empty() &&
                    layout.value_cols.front().ts_path.empty())
                {
                    const ValueView &cell = row.at(layout.value_col_start);
                    if (cell.has_value()) { apply_current_value(out, cell); }
                    return;
                }

                // Compound leaf: rebuild a (possibly partial) value and apply
                // it as the tick's delta (unset fields stay unset).
                Value value{checked_binding(leaf_ts->value_schema, "from_table")};
                bool  any = false;
                for (std::size_t i = 0; i < layout.value_cols.size(); ++i)
                {
                    const auto     &column = layout.value_cols[i];
                    const ValueView &cell  = row.at(layout.value_col_start + i);
                    if (!cell.has_value()) { continue; }
                    std::vector<std::size_t> full_path = column.ts_path;
                    full_path.insert(full_path.end(), column.value_path.begin(),
                                     column.value_path.end());
                    walk_mutable(value.view().begin_mutation(), full_path).copy_from(cell);
                    any = true;
                }
                if (!any) { return; }
                apply_delta(out, value.view());
            }

            void apply_partition_row(const TsTableLayout &layout, std::size_t level_index,
                                     const TupleView &row, const TSOutputView &out)
            {
                if (level_index == layout.levels.size())
                {
                    apply_leaf_row(layout, row, out);
                    return;
                }
                const auto &level    = layout.levels[level_index];
                auto        dict     = out.as_dict();
                auto        mutation = dict.begin_mutation(out.evaluation_time());
                Value       key      = read_key(level, row);

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
        }  // namespace

        void apply_rows(const TsTableLayout &layout, const ValueView &value, const TSOutputView &out)
        {
            if (layout.is_multi_row)
            {
                // Frame output: rebuild the tick's frame from its rows.
                std::vector<ValueView> row_views;
                auto                   list = value.as_list();
                for (ValueView row : list) { row_views.push_back(std::move(row)); }
                Frame frame = frame_from_rows(*layout.frame_converter,
                                              std::span<const ValueView>{row_views},
                                              /*first_column=*/layout.value_col_start);
                // Box over the OUTPUT's (typed) frame schema - the base
                // Frame{} scalar meta would not match a Frame[Schema] output.
                Value boxed{checked_binding(out.schema()->value_schema, "from_table")};
                *static_cast<Frame *>(const_cast<void *>(boxed.view().data())) = std::move(frame);
                apply_current_value(out, boxed.view());
                return;
            }
            if (layout.partitioned())
            {
                auto list = value.as_list();
                for (ValueView row : list)
                {
                    apply_partition_row(layout, 0, row.as_tuple(), out);
                }
                return;
            }
            apply_leaf_row(layout, value.as_tuple(), out);
        }
    }  // namespace table_ts_detail

    void register_table_operators()
    {
        // Layouts intern by TS-schema POINTER (the plan-registries rule):
        // registration follows every registry reset, so clearing here keeps
        // the cache generation-consistent without a types->stdlib reset hook.
        table_ts_detail::clear_ts_table_layouts();
        static_cast<void>(table_ts_detail::to_table_mode_meta());   // register the mode enum
        register_overload<to_table, to_table_rows_impl>();
        register_overload<from_table, from_table_rows_impl>();
        register_overload<from_table_const, from_table_const_impl>();
    }
}  // namespace hgraph::stdlib
