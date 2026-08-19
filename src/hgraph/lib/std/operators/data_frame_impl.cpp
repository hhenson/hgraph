#include <hgraph/lib/std/operators/impl/data_frame_impl.h>

#include <hgraph/lib/std/operators/container.h>
#include <hgraph/lib/std/operators/conversion.h>
#include <hgraph/types/metadata/type_realization.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/table_config.h>
#include <hgraph/types/time_series/ts_input/bundle_view.h>
#include <hgraph/types/time_series/ts_input/dict_view.h>
#include <hgraph/types/time_series/ts_output/dict_view.h>
#include <hgraph/types/value/specialized_views.h>

#include <arrow/api.h>
#include <arrow/acero/api.h>
#include <arrow/compute/api.h>
#include <arrow/table.h>
#include <fmt/format.h>

#include <algorithm>
#include <memory>
#include <unordered_map>

namespace hgraph::stdlib
{
    namespace data_frame_detail
    {
        namespace
        {
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

            /** Assign a cell through the selected destination strategy.
                Compatible realized values need not have identical schemas or
                storage plans. */
            void assign_cell(const ValueView &destination,
                             const ValueView &source)
            {
                if (!destination.has_value() || !source.has_value())
                {
                    throw std::invalid_argument(
                        "data-frame cell assignment requires live values");
                }
                const auto target = destination.binding();
                target.ops_ref().copy_assign_from(
                    target, destination.mutable_data(), source.binding(),
                    source.data());
            }

            [[nodiscard]] const ValueTypeMetaData *datetime_meta()
            {
                // Generation-cached: read_dt runs per replayed ROW, and the
                // name lookup took the registry mutex plus a string
                // allocation every call (lock-free per-tick ruling).
                return TypeRegistry::instance().scalar_type<DateTime>().schema();
            }

            [[nodiscard]] DateTime read_dt(const Frame &frame, const std::string &dt_col,
                                           std::int64_t row)
            {
                const Value cell = frame_cell(frame, dt_col, datetime_meta(), row);
                if (!cell.has_value())
                {
                    throw std::invalid_argument("from_data_frame: null value in the date column");
                }
                return cell.view().checked_as<DateTime>();
            }

            [[nodiscard]] const ValueTypeMetaData *frame_columns_schema(const ValueTypeMetaData *frame_meta,
                                                                        const char              *what)
            {
                if (frame_meta == nullptr || frame_meta->element_type == nullptr)
                {
                    throw std::invalid_argument(
                        fmt::format("{}: an untyped Frame cannot name columns (use Frame[Schema])", what));
                }
                return frame_meta->element_type;
            }

            void set_bundle_field(Value &row, std::size_t index, const ValueView &cell)
            {
                if (!cell.has_value()) { return; }
                ValueView root = row.view().begin_mutation();
                auto      mut  = root.as_bundle().begin_mutation();
                ValueView dest = mut.at(index);
                assign_cell(dest, cell);
            }

            void set_bundle_field(Value &row, std::size_t index, const Value &cell)
            {
                if (!cell.has_value()) { return; }
                set_bundle_field(row, index, cell.view());
            }

            [[nodiscard]] std::size_t field_index_of(const ValueTypeMetaData *bundle,
                                                     std::string_view name, const char *what)
            {
                for (std::size_t i = 0; i < bundle->field_count; ++i)
                {
                    if (bundle->fields[i].name != nullptr && bundle->fields[i].name == name) { return i; }
                }
                throw std::invalid_argument(fmt::format("{}: no column matches field '{}'", what, name));
            }

            [[nodiscard]] bool key_equal(const ValueView &lhs, const ValueView &rhs)
            {
                return lhs.binding() == rhs.binding() &&
                       lhs.binding().ops_ref().equals(lhs.data(), rhs.data());
            }
        }  // namespace

        // -----------------------------------------------------------------
        // from_data_frame
        // -----------------------------------------------------------------

        namespace
        {
            void apply_from_frame_root(const FromFramePlan &plan,
                                       std::int64_t row,
                                       const TSOutputView &out);
            void apply_from_frame_dict(const FromFramePlan &plan,
                                       std::int64_t row,
                                       const TSOutputView &out);
        }

        void missing_from_frame_row_application(const FromFramePlan &,
                                                std::int64_t,
                                                const TSOutputView &)
        {
            throw std::logic_error("from_data_frame row application strategy was not selected");
        }

        void load_from_frame(const Frame &frame, std::string_view dt_col,
                             std::string_view key_col, std::string_view value_col,
                             TimeDelta offset, DateTime start_time,
                             const TSOutputView &out, FromFramePlan *&plan_out)
        {
            auto plan      = std::make_unique<FromFramePlan>();
            plan->frame    = frame;
            plan->dt_col   = std::string{dt_col};
            plan->offset   = offset;

            const auto add_bundle_fields = [&](const ValueTypeMetaData *bundle, const char *what) {
                plan->bundle_meta = bundle;
                for (std::size_t i = 0; i < bundle->field_count; ++i)
                {
                    const auto &field = bundle->fields[i];
                    if (field.type->value_kind() != ValueTypeKind::Atomic)
                    {
                        throw std::invalid_argument(
                            fmt::format("{}: non-atomic field '{}' is not supported", what,
                                        field.name != nullptr ? field.name : "?"));
                    }
                    plan->fields.push_back(
                        FieldRead{field.name != nullptr ? field.name : "", field.type, i});
                }
            };

            switch (out.schema()->kind)
            {
                case TSTypeKind::TS:
                    plan->apply_row = &apply_from_frame_root;
                    plan->fields.push_back(
                        FieldRead{std::string{value_col}, out.schema()->value_schema, 0});
                    break;
                case TSTypeKind::TSB:
                    plan->apply_row = &apply_from_frame_root;
                    add_bundle_fields(out.schema()->value_schema, "from_data_frame");
                    break;
                case TSTypeKind::TSD: {
                    plan->apply_row = &apply_from_frame_dict;
                    plan->key_meta = out.schema()->key_type();
                    plan->key_col  = std::string{key_col};
                    const auto *child = out.schema()->element_ts();
                    if (child->kind == TSTypeKind::TSB)
                    {
                        add_bundle_fields(child->value_schema, "from_data_frame");
                    }
                    else if (child->kind == TSTypeKind::TS)
                    {
                        // The value column is "the remaining column" (the
                        // upstream k_v rule): first frame column that is
                        // neither the date nor the key column.
                        std::string column{value_col};
                        for (const auto &name : frame_column_names(frame))
                        {
                            if (name != plan->dt_col && name != plan->key_col)
                            {
                                column = name;
                                break;
                            }
                        }
                        plan->fields.push_back(FieldRead{std::move(column), child->value_schema, 0});
                    }
                    else
                    {
                        throw std::invalid_argument(
                            "from_data_frame: unsupported TSD element time-series kind");
                    }
                    break;
                }
                default: throw std::invalid_argument("from_data_frame: unsupported output kind");
            }

            const auto rows = plan->frame.has_value() ? frame_rows(plan->frame) : 0;
            while (plan->row < rows &&
                   read_dt(plan->frame, plan->dt_col, plan->row) + plan->offset <
                       start_time)
            {
                ++plan->row;
            }
            plan_out = plan.release();
        }

        void start_from_frame(const Frame &frame, std::string_view dt_col,
                              std::string_view key_col,
                              std::string_view value_col, TimeDelta offset,
                              DateTime start_time, const TSOutputView &out,
                              SingleShotScheduler &sched,
                              FromFramePlan *&plan_out)
        {
            load_from_frame(frame, dt_col, key_col, value_col, offset,
                            start_time, out, plan_out);
            const auto rows = plan_out->frame.has_value()
                                  ? frame_rows(plan_out->frame)
                                  : std::int64_t{0};
            if (plan_out->row < rows)
            {
                sched.schedule(read_dt(plan_out->frame, plan_out->dt_col,
                                       plan_out->row) +
                               plan_out->offset);
            }
        }

        namespace
        {
            void apply_from_frame_leafish(const FromFramePlan &plan, std::int64_t row,
                                          const TSOutputView &out)
            {
                if (plan.bundle_meta != nullptr)
                {
                    Value value{checked_binding(plan.bundle_meta, "from_data_frame")};
                    bool  any = false;
                    for (const auto &field : plan.fields)
                    {
                        const Value cell = frame_cell(plan.frame, field.column, field.leaf, row);
                        if (!cell.has_value()) { continue; }   // nulls do not tick
                        set_bundle_field(value, field.field_index, cell);
                        any = true;
                    }
                    if (any) { apply_delta(out, value.view()); }
                    return;
                }
                const auto &field = plan.fields.front();
                const Value cell  = frame_cell(plan.frame, field.column, field.leaf, row);
                if (cell.has_value()) { apply_current_value(out, cell.view()); }
            }

            void apply_from_frame_root(const FromFramePlan &plan,
                                       std::int64_t row,
                                       const TSOutputView &out)
            {
                apply_from_frame_leafish(plan, row, out);
            }

            void apply_from_frame_dict(const FromFramePlan &plan,
                                       std::int64_t row,
                                       const TSOutputView &out)
            {
                auto        dict     = out.as_dict();
                auto        mutation = dict.begin_mutation(out.evaluation_time());
                const Value key = frame_cell(plan.frame, plan.key_col, plan.key_meta, row);
                auto        element = mutation.at(key.view());
                apply_from_frame_leafish(
                    plan, row, TSOutputView{out.output(), element, out.evaluation_time()});
            }
        }  // namespace

        void eval_from_frame(FromFramePlan &plan, DateTime now, NodeScheduler &sched,
                             const TSOutputView &out)
        {
            const auto rows = plan.frame.has_value() ? frame_rows(plan.frame) : 0;
            while (plan.row < rows && read_dt(plan.frame, plan.dt_col, plan.row) + plan.offset == now)
            {
                plan.apply_row(plan, plan.row, out);
                ++plan.row;
            }
            if (plan.row < rows)
            {
                sched.schedule(read_dt(plan.frame, plan.dt_col, plan.row) + plan.offset);
            }
        }

        // -----------------------------------------------------------------
        // replay_data_frame
        // -----------------------------------------------------------------

        namespace
        {
            struct ReplayCandidate
            {
                DateTime           when{};
                DateTime           as_of{};
                std::int64_t        row{0};
            };

            struct ReplayGroup
            {
                DateTime                    when{};
                DateTime                    as_of{};
                std::vector<Value>          keys{};
                std::vector<ReplayCandidate> rows{};
            };

            [[nodiscard]] std::size_t combine_hash(std::size_t seed,
                                                   std::size_t value) noexcept
            {
                return seed ^ (value + 0x9e3779b97f4a7c15ULL + (seed << 6) +
                               (seed >> 2));
            }

            [[nodiscard]] std::size_t candidate_hash(
                DateTime when, const std::vector<Value> &keys)
            {
                std::size_t seed = std::hash<std::int64_t>{}(
                    when.time_since_epoch().count());
                for (const Value &key : keys)
                {
                    seed = combine_hash(seed, key.hash());
                }
                return seed;
            }

            [[nodiscard]] bool same_partition(const ReplayGroup &group,
                                              DateTime when,
                                              const std::vector<Value> &keys)
            {
                if (group.when != when || group.keys.size() != keys.size())
                {
                    return false;
                }
                for (std::size_t index = 0; index < keys.size(); ++index)
                {
                    if (!group.keys[index].equals(keys[index])) { return false; }
                }
                return true;
            }

            [[nodiscard]] Frame take_replay_rows(
                const Frame &frame, std::span<const ReplayCandidate> selected)
            {
                arrow::Int64Builder builder;
                for (const ReplayCandidate &candidate : selected)
                {
                    const auto status = builder.Append(candidate.row);
                    if (!status.ok())
                    {
                        throw std::runtime_error(
                            "replay_data_frame: failed to append row selection: " +
                            status.ToString());
                    }
                }
                std::shared_ptr<arrow::Array> indices;
                const auto finish = builder.Finish(&indices);
                if (!finish.ok())
                {
                    throw std::runtime_error(
                        "replay_data_frame: failed to finish row selection: " +
                        finish.ToString());
                }
                const auto result = arrow::compute::Take(
                    arrow::Datum{frame.table}, arrow::Datum{std::move(indices)});
                if (!result.ok())
                {
                    throw std::runtime_error(
                        "replay_data_frame: failed to select rows: " +
                        result.status().ToString());
                }
                return Frame{result->table()};
            }

            [[nodiscard]] Value read_table_row(
                const table_ts_detail::TsTableLayout &layout,
                const Frame &frame, std::span<const int> columns, std::int64_t row)
            {
                Value value{checked_binding(layout.row_meta, "replay_data_frame")};
                auto  tuple = value.as_tuple().begin_mutation();
                for (std::size_t column = 0; column < layout.keys.size(); ++column)
                {
                    // ``-1`` is a column the recording does not carry; a missing
                    // REQUIRED column was already refused when the projection
                    // was resolved.
                    if (columns[column] < 0) { continue; }
                    Value cell = frame_cell_at(frame, columns[column],
                                               layout.col_metas[column], row);
                    if (cell.has_value())
                    {
                        assign_cell(tuple.at(column), cell.view());
                    }
                }
                return value;
            }

            [[nodiscard]] Value build_table_rows(
                const table_ts_detail::TsTableLayout &layout,
                const Frame &frame, std::span<const int> columns,
                std::span<const ReplayCandidate> candidates)
            {
                if (!layout.multi())
                {
                    return read_table_row(layout, frame, columns, candidates.front().row);
                }

                const auto row_binding =
                    checked_binding(layout.row_meta, "replay_data_frame");
                ListBuilder builder{row_binding, *layout.rows_meta};
                for (const ReplayCandidate &candidate : candidates)
                {
                    Value row = read_table_row(layout, frame, columns, candidate.row);
                    builder.push_back(row.view());
                }
                return builder.build(
                    checked_binding(layout.rows_meta, "replay_data_frame"));
            }
        }  // namespace

        Frame select_replay_frame(const Frame &frame,
                                  const table_ts_detail::TsTableLayout &layout,
                                  std::span<const int> columns,
                                  DateTime as_of_time, DateTime start_time)
        {
            if (!frame.has_value() || frame_rows(frame) == 0) { return frame; }

            Frame normalized = frame;
            const auto combined = frame.table->CombineChunks();
            if (!combined.ok())
            {
                throw std::invalid_argument(
                    "replay_data_frame: failed to combine input chunks: " +
                    combined.status().ToString());
            }
            normalized.table = *combined;

            const int  as_of_column = columns[1];
            const bool has_as_of = as_of_column >= 0;
            std::vector<ReplayGroup> groups;
            std::unordered_map<std::size_t, std::vector<std::size_t>> buckets;

            for (std::int64_t row = 0; row < frame_rows(normalized); ++row)
            {
                const Value when_cell = frame_cell_at(
                    normalized, columns[0],
                    scalar_descriptor<DateTime>::value_meta(), row);
                if (!when_cell.has_value())
                {
                    throw std::invalid_argument(
                        "replay_data_frame: date column must not contain nulls");
                }
                const DateTime when = when_cell.view().checked_as<DateTime>();
                if (when < start_time) { continue; }

                DateTime revision = MIN_DT;
                if (has_as_of)
                {
                    const Value as_of_cell = frame_cell_at(
                        normalized, as_of_column,
                        scalar_descriptor<DateTime>::value_meta(), row);
                    if (!as_of_cell.has_value())
                    {
                        throw std::invalid_argument(
                            "replay_data_frame: as-of column must not contain nulls");
                    }
                    revision = as_of_cell.view().checked_as<DateTime>();
                    if (revision > as_of_time) { continue; }
                }

                std::vector<Value> keys;
                keys.reserve(layout.partition_keys.size());
                // A removal row ends at the removed TSD level: key columns
                // below that level are deliberately null because there is no
                // descendant partition to name. Select revisions by the
                // populated key prefix, matching apply_recorded_row's walk.
                for (const auto &level : layout.levels)
                {
                    for (std::size_t key_index = 0;
                         key_index < level.key_paths.size(); ++key_index)
                    {
                        const std::size_t column = level.first_key_col + key_index;
                        Value value = frame_cell_at(normalized, columns[column],
                                                    layout.col_metas[column], row);
                        if (!value.has_value())
                        {
                            throw std::invalid_argument(
                                "replay_data_frame: populated partition key prefix "
                                "must not contain nulls");
                        }
                        keys.push_back(std::move(value));
                    }

                    const int removed_index = columns[level.removed_col];
                    if (removed_index >= 0 &&
                        !normalized.table->column(removed_index)->chunk(0)->IsNull(row))
                    {
                        const Value removed = frame_cell_at(
                            normalized, removed_index,
                            layout.col_metas[level.removed_col], row);
                        if (removed.has_value() &&
                            removed.view().checked_as<Bool>())
                        {
                            break;
                        }
                    }
                }

                ReplayCandidate candidate{when, revision, row};
                const std::size_t hash = candidate_hash(when, keys);
                auto &bucket = buckets[hash];
                const auto existing = std::find_if(
                    bucket.begin(), bucket.end(), [&](std::size_t index) {
                        return same_partition(groups[index], when, keys);
                    });
                if (existing == bucket.end())
                {
                    bucket.push_back(groups.size());
                    groups.push_back(ReplayGroup{
                        when, revision, std::move(keys), {std::move(candidate)}});
                }
                else
                {
                    auto &group = groups[*existing];
                    if (revision > group.as_of)
                    {
                        group.as_of = revision;
                        group.rows.clear();
                        group.rows.push_back(std::move(candidate));
                    }
                    else if (layout.is_multi_row && revision == group.as_of)
                    {
                        group.rows.push_back(std::move(candidate));
                    }
                }
            }

            std::vector<ReplayCandidate> selected;
            for (auto &group : groups)
            {
                for (auto &candidate : group.rows)
                {
                    selected.push_back(std::move(candidate));
                }
            }
            std::stable_sort(
                selected.begin(), selected.end(),
                [](const ReplayCandidate &lhs, const ReplayCandidate &rhs) {
                    if (lhs.when != rhs.when) { return lhs.when < rhs.when; }
                    return lhs.row < rhs.row;
                });
            return take_replay_rows(normalized, selected);
        }

        void start_replay_data_frame(const Frame &frame, DateTime as_of_time,
                                     DateTime start_time, GlobalStateView gs,
                                     const TSOutputView &out,
                                     SingleShotScheduler &sched,
                                     ReplayDataFramePlan *&plan_out)
        {
            auto plan = std::make_unique<ReplayDataFramePlan>();
            const auto config = table::config(gs);
            plan->layout = &table_ts_detail::ts_table_layout(
                out.schema(), config.date_key, config.as_of_key);

            if (!frame.has_value() || frame_rows(frame) == 0)
            {
                plan_out = plan.release();
                return;
            }

            const DateTime cutoff = as_of_time == MAX_DT
                                        ? config.as_of.value_or(start_time)
                                        : as_of_time;
            // The raw path has no caller-supplied projection: the frame is
            // handed straight to replay, so its columns must already carry the
            // layout's own names. Resolving with an empty projection applies
            // exactly the same strictness - a required column that is absent
            // is an error rather than a silently empty cell.
            const auto  columns = table_ts_detail::resolve_replay_columns(frame, *plan->layout, {});
            const Frame normalized =
                select_replay_frame(frame, *plan->layout, columns, cutoff, start_time);
            std::vector<ReplayCandidate> candidates;
            for (std::int64_t row = 0; row < frame_rows(normalized); ++row)
            {
                const Value when_cell = frame_cell_at(
                    normalized, columns[0],
                    scalar_descriptor<DateTime>::value_meta(), row);
                if (!when_cell.has_value())
                {
                    throw std::invalid_argument(
                        "replay_data_frame: date column must not contain nulls");
                }
                const DateTime when = when_cell.view().checked_as<DateTime>();
                candidates.push_back(ReplayCandidate{when, MIN_DT, row});
            }

            for (std::size_t begin = 0; begin < candidates.size();)
            {
                std::size_t end = begin + 1;
                while (end < candidates.size() &&
                       candidates[end].when == candidates[begin].when)
                {
                    ++end;
                }
                plan->ticks.push_back(ReplayFrameTick{
                    candidates[begin].when,
                    build_table_rows(*plan->layout, normalized, columns,
                                     std::span<const ReplayCandidate>{candidates}.subspan(
                                         begin, end - begin))});
                begin = end;
            }

            if (!plan->ticks.empty()) { sched.schedule(plan->ticks.front().when); }
            plan_out = plan.release();
        }

        void eval_replay_data_frame(ReplayDataFramePlan &plan, DateTime now,
                                    NodeScheduler &sched,
                                    const TSOutputView &out)
        {
            while (plan.tick < plan.ticks.size() &&
                   plan.ticks[plan.tick].when == now)
            {
                table_ts_detail::apply_rows(*plan.layout,
                                            plan.ticks[plan.tick].rows.view(), out);
                ++plan.tick;
            }
            if (plan.tick < plan.ticks.size())
            {
                sched.schedule(plan.ticks[plan.tick].when);
            }
        }

        // -----------------------------------------------------------------
        // to_data_frame
        // -----------------------------------------------------------------

        const TSValueTypeMetaData *resolve_to_frame_output(const TSValueTypeMetaData *ts,
                                                           std::string_view dt_col,
                                                           std::string_view key_col,
                                                           std::string_view value_col)
        {
            auto       &registry = TypeRegistry::instance();
            const auto *schema   = registry.dereference(ts);
            std::vector<std::pair<std::string, const ValueTypeMetaData *>> fields;
            fields.emplace_back(std::string{dt_col}, datetime_meta());

            const auto *leaf = schema;
            if (schema->kind == TSTypeKind::TSD)
            {
                if (schema->key_type()->value_kind() != ValueTypeKind::Atomic) { return nullptr; }
                fields.emplace_back(std::string{key_col}, schema->key_type());
                leaf = schema->element_ts();
            }
            if (leaf->kind == TSTypeKind::TS)
            {
                if (leaf->value_schema->value_kind() != ValueTypeKind::Atomic) { return nullptr; }
                fields.emplace_back(std::string{value_col}, leaf->value_schema);
            }
            else if (leaf->kind == TSTypeKind::TSB)
            {
                const auto *bundle = leaf->value_schema;
                for (std::size_t i = 0; i < bundle->field_count; ++i)
                {
                    const auto &field = bundle->fields[i];
                    if (field.type->value_kind() != ValueTypeKind::Atomic) { return nullptr; }
                    fields.emplace_back(field.name != nullptr ? field.name : "", field.type);
                }
            }
            else { return nullptr; }

            return registry.ts(registry.frame(registry.un_named_bundle(fields)));
        }

        void start_to_frame(const TSInputView &ts, std::string_view dt_col, std::string_view key_col,
                            std::string_view value_col, const TSOutputView &out, ToFramePlan *&plan_out)
        {
            auto        plan     = std::make_unique<ToFramePlan>();
            const auto *columns  = frame_columns_schema(out.schema()->value_schema, "to_data_frame");
            const auto *schema   = TypeRegistry::instance().dereference(ts.schema());
            plan->row_meta       = columns;
            plan->converter      = &table_converter(columns);
            plan->dict           = schema->kind == TSTypeKind::TSD;

            const auto *leaf = plan->dict ? schema->element_ts() : schema;
            const auto *leaf_bundle =
                leaf->kind == TSTypeKind::TSB ? leaf->value_schema : nullptr;

            for (std::size_t i = 0; i < columns->field_count; ++i)
            {
                const char            *name = columns->fields[i].name;
                const std::string_view column{name != nullptr ? name : ""};
                ToFramePlan::Column    entry;
                if (column == dt_col) { entry.source = ToFramePlan::Source::Date; }
                else if (plan->dict && column == key_col) { entry.source = ToFramePlan::Source::Key; }
                else if (leaf_bundle != nullptr)
                {
                    entry.source   = ToFramePlan::Source::Field;
                    entry.ts_field = field_index_of(leaf_bundle, column, "to_data_frame");
                }
                else
                {
                    // A plain TS leaf: the (single) value column, whatever
                    // its name (value_col by convention).
                    static_cast<void>(value_col);
                    entry.source = ToFramePlan::Source::Whole;
                }
                plan->columns.push_back(entry);
            }
            plan_out = plan.release();
        }

        namespace
        {
            [[nodiscard]] Value snapshot_row(const ToFramePlan &plan, DateTime now, const ValueView *key,
                                             const TSInputView &leaf)
            {
                Value row{checked_binding(plan.row_meta, "to_data_frame")};
                for (std::size_t i = 0; i < plan.columns.size(); ++i)
                {
                    const auto &column = plan.columns[i];
                    switch (column.source)
                    {
                        case ToFramePlan::Source::Date: {
                            Value cell{now};
                            set_bundle_field(row, i, cell);
                            break;
                        }
                        case ToFramePlan::Source::Key:
                            if (key != nullptr) { set_bundle_field(row, i, *key); }
                            break;
                        case ToFramePlan::Source::Field: {
                            auto bundle = leaf.as_bundle();
                            auto child  = bundle.at(column.ts_field);
                            if (child.valid()) { set_bundle_field(row, i, child.value()); }
                            break;
                        }
                        case ToFramePlan::Source::ValueField: {
                            if (!leaf.valid()) { break; }
                            const ValueView value = leaf.value();
                            auto            bundle = value.as_bundle();
                            set_bundle_field(row, i, bundle.at(plan.columns[i].ts_field));
                            break;
                        }
                        case ToFramePlan::Source::Whole:
                            if (leaf.valid()) { set_bundle_field(row, i, leaf.value()); }
                            break;
                    }
                }
                return row;
            }
        }  // namespace

        void eval_to_frame(const ToFramePlan &plan, const TSInputView &ts, DateTime now,
                           const TSOutputView &out)
        {
            std::vector<Value> rows;
            if (plan.dict)
            {
                auto dict = const_cast<TSInputView &>(ts).as_dict();
                for (auto &&[key, child] : dict.valid_items())
                {
                    rows.push_back(snapshot_row(plan, now, &key, child));
                }
            }
            else { rows.push_back(snapshot_row(plan, now, nullptr, ts)); }

            Frame frame = frame_from_values(*plan.converter, rows);
            Value boxed{checked_binding(out.schema()->value_schema, "to_data_frame")};
            *static_cast<Frame *>(const_cast<void *>(boxed.view().data())) = std::move(frame);
            apply_current_value(out, boxed.view());
        }

        // -----------------------------------------------------------------
        // group_by
        // -----------------------------------------------------------------

        const TSValueTypeMetaData *resolve_group_by_output(const TSValueTypeMetaData *ts,
                                                           const ValueView          &by)
        {
            if (ts->kind != TSTypeKind::TS) { return nullptr; }
            const auto *columns = ts->value_schema->element_type;
            if (columns == nullptr) { return nullptr; }
            auto &registry = TypeRegistry::instance();

            const auto field_meta = [&](std::string_view name) -> const ValueTypeMetaData * {
                for (std::size_t i = 0; i < columns->field_count; ++i)
                {
                    if (columns->fields[i].name != nullptr && columns->fields[i].name == name)
                    {
                        return columns->fields[i].type;
                    }
                }
                return nullptr;
            };

            const ValueTypeMetaData *key_meta = nullptr;
            if (by.schema()->value_kind() == ValueTypeKind::Atomic)
            {
                key_meta = field_meta(by.checked_as<Str>());
            }
            else
            {
                std::vector<const ValueTypeMetaData *> parts;
                auto                                   list = by.as_indexed_view();
                for (std::size_t i = 0; i < list.size(); ++i)
                {
                    const auto *part = field_meta(std::string{list.at(i).checked_as<Str>()});
                    if (part == nullptr) { return nullptr; }
                    parts.push_back(part);
                }
                key_meta = registry.tuple(parts);
            }
            if (key_meta == nullptr) { return nullptr; }
            return registry.tsd(key_meta, registry.ts(ts->value_schema));
        }

        void start_group_by(const TSInputView &ts, const ValueView &by, const TSOutputView &out,
                            GroupByPlan *&plan_out)
        {
            static_cast<void>(out);
            auto        plan    = std::make_unique<GroupByPlan>();
            const auto *columns = frame_columns_schema(ts.schema()->value_schema, "group_by");
            plan->converter     = &table_converter(columns);

            const auto add_key = [&](std::string_view name) {
                const std::size_t index = field_index_of(columns, name, "group_by");
                plan->key_cols.push_back(
                    FieldRead{std::string{name}, columns->fields[index].type, index});
            };

            if (by.schema()->value_kind() == ValueTypeKind::Atomic)
            {
                add_key(by.checked_as<Str>());
                plan->key_meta = plan->key_cols.front().leaf;
            }
            else
            {
                plan->tuple_key = true;
                std::vector<const ValueTypeMetaData *> parts;
                auto                                   list = by.as_indexed_view();
                for (std::size_t i = 0; i < list.size(); ++i)
                {
                    add_key(std::string{list.at(i).checked_as<Str>()});
                    parts.push_back(plan->key_cols.back().leaf);
                }
                plan->key_meta = TypeRegistry::instance().tuple(parts);
            }
            plan_out = plan.release();
        }

        void eval_group_by(const GroupByPlan &plan, const TSInputView &ts, const TSOutputView &out)
        {
            const ValueView view  = ts.value();
            const Frame    &frame = view.checked_as<Frame>();
            const auto      rows  = frame.has_value() ? frame_rows(frame) : 0;

            std::vector<std::pair<Value, std::vector<Value>>> buckets;
            for (std::int64_t r = 0; r < rows; ++r)
            {
                Value key{checked_binding(plan.key_meta, "group_by")};
                if (plan.tuple_key)
                {
                    for (std::size_t i = 0; i < plan.key_cols.size(); ++i)
                    {
                        const Value cell =
                            frame_cell(frame, plan.key_cols[i].column, plan.key_cols[i].leaf, r);
                        if (!cell.has_value()) { continue; }
                        ValueView root = key.view().begin_mutation();
                        auto      mut  = root.as_tuple().begin_mutation();
                        ValueView dest = mut.at(i);
                        assign_cell(dest, cell.view());
                    }
                }
                else
                {
                    const Value cell =
                        frame_cell(frame, plan.key_cols.front().column, plan.key_cols.front().leaf, r);
                    if (!cell.has_value()) { continue; }
                    ValueView dest = key.view().begin_mutation();
                    assign_cell(dest, cell.view());
                }

                auto bucket = std::find_if(buckets.begin(), buckets.end(), [&](const auto &entry) {
                    return key_equal(entry.first.view(), key.view());
                });
                if (bucket == buckets.end())
                {
                    buckets.emplace_back(std::move(key), std::vector<Value>{});
                    bucket = std::prev(buckets.end());
                }
                bucket->second.push_back(read_row(*plan.converter, frame, r));
            }

            auto dict     = out.as_dict();
            auto mutation = dict.begin_mutation(out.evaluation_time());

            // Removals first: collect current keys, drop the ones absent
            // from this tick's buckets.
            std::vector<Value> stale;
            for (ValueView key : dict.keys())
            {
                const bool present =
                    std::any_of(buckets.begin(), buckets.end(), [&](const auto &entry) {
                        return key_equal(entry.first.view(), key);
                    });
                if (!present) { stale.emplace_back(key); }
            }
            for (const Value &key : stale) { static_cast<void>(mutation.erase(key.view())); }

            const auto *child_schema = out.schema()->element_ts();
            for (auto &[key, bucket_rows] : buckets)
            {
                Frame sub = frame_from_values(*plan.converter, bucket_rows);
                Value boxed{checked_binding(child_schema->value_schema, "group_by")};
                *static_cast<Frame *>(const_cast<void *>(boxed.view().data())) = std::move(sub);
                auto element = mutation.at(key.view());
                apply_current_value(TSOutputView{out.output(), element, out.evaluation_time()},
                                    boxed.view());
            }
        }

        Frame sort_frame(const Frame &frame, std::string_view by, bool descending)
        {
            if (!frame.has_value() || frame_rows(frame) < 2) { return frame; }

            const auto column = frame.table->GetColumnByName(std::string{by});
            if (column == nullptr)
            {
                throw std::invalid_argument("sorted_: frame has no column named '" + std::string{by} + "'");
            }
            auto indices = arrow::compute::SortIndices(
                *column, descending ? arrow::compute::SortOrder::Descending
                                    : arrow::compute::SortOrder::Ascending);
            if (!indices.ok())
            {
                throw std::runtime_error("sorted_: arrow sort failed: " + indices.status().ToString());
            }
            auto sorted = arrow::compute::Take(arrow::Datum{frame.table}, arrow::Datum{*indices});
            if (!sorted.ok())
            {
                throw std::runtime_error("sorted_: arrow take failed: " + sorted.status().ToString());
            }
            return Frame{sorted->table()->ReplaceSchemaMetadata(
                frame.table->schema()->metadata())};
        }

        Frame concat_frames(const Frame &lhs, const Frame &rhs)
        {
            if (!lhs.has_value()) { return rhs; }
            if (!rhs.has_value()) { return lhs; }
            if (!frame_metadata_equal(lhs, rhs))
            {
                throw std::invalid_argument("concat: frame metadata must be equal");
            }

            auto result = arrow::ConcatenateTables({lhs.table, rhs.table});
            if (!result.ok())
            {
                throw std::runtime_error("concat: arrow concatenate failed: " + result.status().ToString());
            }
            return Frame{(*result)->ReplaceSchemaMetadata(
                lhs.table->schema()->metadata())};
        }

        namespace
        {
            [[nodiscard]] std::vector<std::string> join_keys(const ValueView &on)
            {
                if (on.schema() == scalar_descriptor<Str>::value_meta())
                {
                    return {std::string{on.checked_as<Str>()}};
                }
                if (on.schema() != nullptr &&
                    (on.schema()->value_kind() == ValueTypeKind::Tuple ||
                     on.schema()->value_kind() == ValueTypeKind::List))
                {
                    std::vector<std::string> keys;
                    const auto values = on.as_indexed_view();
                    keys.reserve(values.size());
                    for (std::size_t index = 0; index < values.size(); ++index)
                    {
                        keys.emplace_back(values.at(index).checked_as<Str>());
                    }
                    if (!keys.empty()) { return keys; }
                }
                throw std::invalid_argument(
                    "join: 'on' must be a column name or a non-empty tuple of column names");
            }

            [[nodiscard]] bool contains_name(const std::vector<std::string> &names,
                                             std::string_view name)
            {
                return std::find(names.begin(), names.end(), name) != names.end();
            }

            [[nodiscard]] arrow::acero::JoinType join_type(std::string_view how)
            {
                using arrow::acero::JoinType;
                if (how == "inner") { return JoinType::INNER; }
                if (how == "left" || how == "left outer") { return JoinType::LEFT_OUTER; }
                if (how == "right" || how == "right outer") { return JoinType::RIGHT_OUTER; }
                if (how == "full" || how == "outer" || how == "full outer")
                {
                    return JoinType::FULL_OUTER;
                }
                if (how == "semi" || how == "left semi") { return JoinType::LEFT_SEMI; }
                if (how == "anti" || how == "left anti") { return JoinType::LEFT_ANTI; }
                if (how == "right semi") { return JoinType::RIGHT_SEMI; }
                if (how == "right anti") { return JoinType::RIGHT_ANTI; }
                throw std::invalid_argument("join: unsupported join type '" + std::string{how} + "'");
            }

            [[nodiscard]] const ValueTypeMetaData *field_type(
                const ValueTypeMetaData *row, std::string_view name)
            {
                for (std::size_t index = 0; index < row->field_count; ++index)
                {
                    const auto &field = row->fields[index];
                    if (field.name != nullptr && field.name == name) { return field.type; }
                }
                return nullptr;
            }
        }  // namespace

        const ValueTypeMetaData *resolve_join_row(const TSValueTypeMetaData *lhs,
                                                  const TSValueTypeMetaData *rhs,
                                                  const ValueView &on,
                                                  std::string_view how,
                                                  std::string_view suffix)
        {
            if (!ts_value_is_frame(lhs) || !ts_value_is_frame(rhs)) { return nullptr; }
            const auto *left = frame_columns_schema(lhs->value_schema, "join");
            const auto *right = frame_columns_schema(rhs->value_schema, "join");
            const auto keys = join_keys(on);
            for (const std::string &key : keys)
            {
                const auto *left_type = field_type(left, key);
                const auto *right_type = field_type(right, key);
                if (left_type == nullptr || right_type == nullptr)
                {
                    throw std::invalid_argument("join: key column '" + key +
                                                "' must exist in both frames");
                }
                if (left_type != right_type)
                {
                    throw std::invalid_argument("join: key column '" + key +
                                                "' has different scalar types");
                }
            }

            const auto type = join_type(how);
            if (type == arrow::acero::JoinType::RIGHT_SEMI ||
                type == arrow::acero::JoinType::RIGHT_ANTI)
            {
                return right;
            }

            std::vector<std::pair<std::string, const ValueTypeMetaData *>> fields;
            fields.reserve(left->field_count + right->field_count);
            for (std::size_t index = 0; index < left->field_count; ++index)
            {
                const auto &field = left->fields[index];
                fields.emplace_back(field.name != nullptr ? field.name : "", field.type);
            }
            if (type != arrow::acero::JoinType::LEFT_SEMI &&
                type != arrow::acero::JoinType::LEFT_ANTI)
            {
                for (std::size_t index = 0; index < right->field_count; ++index)
                {
                    const auto &field = right->fields[index];
                    const std::string name{field.name != nullptr ? field.name : ""};
                    if (contains_name(keys, name)) { continue; }
                    const bool collision = field_type(left, name) != nullptr;
                    fields.emplace_back(collision ? name + std::string{suffix} : name, field.type);
                }
            }
            return TypeRegistry::instance().un_named_bundle(fields);
        }

        Frame join_frames(const Frame &lhs, const Frame &rhs, const ValueView &on,
                          std::string_view how, std::string_view suffix)
        {
            if (!lhs.has_value() || !rhs.has_value()) { return {}; }
            const auto keys = join_keys(on);
            std::vector<arrow::FieldRef> left_keys;
            std::vector<arrow::FieldRef> right_keys;
            std::vector<arrow::FieldRef> left_output;
            std::vector<arrow::FieldRef> right_output;
            left_keys.reserve(keys.size());
            right_keys.reserve(keys.size());
            for (const std::string &key : keys)
            {
                left_keys.emplace_back(key);
                right_keys.emplace_back(key);
            }

            const auto type = join_type(how);
            if (type == arrow::acero::JoinType::RIGHT_SEMI ||
                type == arrow::acero::JoinType::RIGHT_ANTI)
            {
                for (const auto &field : rhs.table->schema()->fields())
                {
                    right_output.emplace_back(field->name());
                }
            }
            else
            {
                for (const auto &field : lhs.table->schema()->fields())
                {
                    left_output.emplace_back(field->name());
                }
                if (type != arrow::acero::JoinType::LEFT_SEMI &&
                    type != arrow::acero::JoinType::LEFT_ANTI)
                {
                    for (const auto &field : rhs.table->schema()->fields())
                    {
                        if (!contains_name(keys, field->name()))
                        {
                            right_output.emplace_back(field->name());
                        }
                    }
                }
            }

            arrow::acero::Declaration left{
                "table_source", arrow::acero::TableSourceNodeOptions{lhs.table}};
            arrow::acero::Declaration right{
                "table_source", arrow::acero::TableSourceNodeOptions{rhs.table}};
            std::vector<arrow::acero::Declaration::Input> inputs;
            inputs.emplace_back(std::move(left));
            inputs.emplace_back(std::move(right));
            arrow::acero::HashJoinNodeOptions options{
                type, std::move(left_keys), std::move(right_keys),
                std::move(left_output), std::move(right_output), arrow::compute::literal(true),
                "", std::string{suffix}};
            arrow::acero::Declaration declaration{
                "hashjoin", std::move(inputs), std::move(options)};
            auto result = arrow::acero::DeclarationToTable(std::move(declaration), false);
            if (!result.ok())
            {
                throw std::runtime_error("join: Arrow hash join failed: " +
                                         result.status().ToString());
            }
            return Frame{std::move(*result)};
        }

        namespace
        {
            struct FramePredicate
            {
                std::string              name;
                const ValueTypeMetaData *meta;
                ValueView                value;
            };

            [[nodiscard]] Frame filter_with_predicates(
                const Frame &frame, std::span<const FramePredicate> predicates)
            {
                if (!frame.has_value() || predicates.empty()) { return frame; }

                arrow::Datum mask;
                bool has_mask = false;
                for (const auto &predicate : predicates)
                {
                    const auto column = frame.table->GetColumnByName(predicate.name);
                    if (column == nullptr)
                    {
                        throw std::invalid_argument(
                            "filter_frame: frame has no column named '" + predicate.name + "'");
                    }
                    auto equal = arrow::compute::CallFunction(
                        "equal", {arrow::Datum{column},
                                  arrow_scalar(predicate.value, predicate.meta)});
                    if (!equal.ok())
                    {
                        throw std::runtime_error("filter_frame: Arrow comparison failed: " +
                                                 equal.status().ToString());
                    }
                    if (!has_mask)
                    {
                        mask = std::move(*equal);
                        has_mask = true;
                    }
                    else
                    {
                        auto combined = arrow::compute::CallFunction(
                            "and_kleene", {std::move(mask), std::move(*equal)});
                        if (!combined.ok())
                        {
                            throw std::runtime_error(
                                "filter_frame: Arrow predicate combination failed: " +
                                combined.status().ToString());
                        }
                        mask = std::move(*combined);
                    }
                }
                auto result = arrow::compute::Filter(arrow::Datum{frame.table}, mask);
                if (!result.ok())
                {
                    throw std::runtime_error("filter_frame: Arrow filter failed: " +
                                             result.status().ToString());
                }
                return Frame{result->table()->ReplaceSchemaMetadata(
                    frame.table->schema()->metadata())};
            }
        }  // namespace

        Frame filter_frame_by_bundle(const Frame &frame, TSInputView &predicate)
        {
            if (predicate.schema()->kind != TSTypeKind::TSB)
            {
                throw std::invalid_argument("filter_frame: predicate must be a TSB");
            }
            std::vector<FramePredicate> predicates;
            auto bundle = predicate.as_bundle();
            for (std::size_t index = 0; index < predicate.schema()->field_count(); ++index)
            {
                auto child = bundle.at(index);
                if (!child.valid()) { continue; }
                const auto &field = predicate.schema()->fields()[index];
                const auto *meta = child.schema()->value_schema;
                if (meta == nullptr || meta->value_kind() != ValueTypeKind::Atomic)
                {
                    throw std::invalid_argument(
                        "filter_frame: structural predicates must be scalar TS fields");
                }
                predicates.push_back(FramePredicate{
                    field.name != nullptr ? field.name : "", meta, child.value()});
            }
            return filter_with_predicates(frame, predicates);
        }

        Frame filter_frame_by_value(const Frame &frame, const ValueView &predicate)
        {
            if (predicate.schema() == nullptr ||
                predicate.schema()->value_kind() != ValueTypeKind::Bundle)
            {
                throw std::invalid_argument(
                    "filter_cs: predicate must be a compound scalar value");
            }
            std::vector<FramePredicate> predicates;
            const auto bundle = predicate.as_bundle();
            for (std::size_t index = 0; index < predicate.schema()->field_count; ++index)
            {
                const ValueView child = bundle.at(index);
                if (!child.has_value()) { continue; }
                const auto &field = predicate.schema()->fields[index];
                if (field.type->value_kind() != ValueTypeKind::Atomic)
                {
                    throw std::invalid_argument(
                        "filter_cs: predicate fields must be scalar values");
                }
                predicates.push_back(FramePredicate{
                    field.name != nullptr ? field.name : "", field.type,
                    ValueView{child.binding(), child.data()}});
            }
            return filter_with_predicates(frame, predicates);
        }

        namespace
        {
            [[nodiscard]] std::vector<std::string> key_column_names(
                const ValueView &key_col)
            {
                if (key_col.schema() == scalar_descriptor<Str>::value_meta())
                {
                    return {std::string{key_col.checked_as<Str>()}};
                }
                if (key_col.schema() != nullptr &&
                    (key_col.schema()->value_kind() == ValueTypeKind::Tuple ||
                     key_col.schema()->value_kind() == ValueTypeKind::List))
                {
                    std::vector<std::string> names;
                    const auto values = key_col.as_indexed_view();
                    names.reserve(values.size());
                    for (std::size_t index = 0; index < values.size(); ++index)
                    {
                        names.emplace_back(values.at(index).checked_as<Str>());
                    }
                    if (!names.empty()) { return names; }
                }
                throw std::invalid_argument(
                    "ungroup: key_col must be a column name or non-empty tuple of names");
            }

            [[nodiscard]] std::vector<const ValueTypeMetaData *> key_parts(
                const ValueTypeMetaData *key)
            {
                if (key != nullptr && key->value_kind() == ValueTypeKind::Tuple)
                {
                    std::vector<const ValueTypeMetaData *> parts;
                    parts.reserve(key->field_count);
                    for (std::size_t index = 0; index < key->field_count; ++index)
                    {
                        parts.push_back(key->fields[index].type);
                    }
                    return parts;
                }
                return {key};
            }

            [[nodiscard]] std::shared_ptr<arrow::ChunkedArray> repeated_column(
                const ValueView &value, const ValueTypeMetaData *meta, std::int64_t count)
            {
                const auto scalar = arrow_scalar(value, meta);
                std::unique_ptr<arrow::ArrayBuilder> builder;
                auto status = arrow::MakeBuilder(arrow::default_memory_pool(), scalar.type(),
                                                 &builder);
                if (!status.ok())
                {
                    throw std::runtime_error(
                        "data frame: Arrow column builder failed: " + status.ToString());
                }
                status = builder->AppendScalar(*scalar.scalar(), count);
                if (!status.ok())
                {
                    throw std::runtime_error(
                        "data frame: Arrow scalar broadcast failed: " + status.ToString());
                }
                std::shared_ptr<arrow::Array> array;
                status = builder->Finish(&array);
                if (!status.ok())
                {
                    throw std::runtime_error(
                        "data frame: Arrow column finish failed: " + status.ToString());
                }
                return std::make_shared<arrow::ChunkedArray>(std::move(array));
            }

            [[nodiscard]] std::shared_ptr<arrow::Table> append_key_columns(
                std::shared_ptr<arrow::Table> table, const ValueView &key,
                const ValueTypeMetaData *key_meta, const std::vector<std::string> &names)
            {
                const auto parts = key_parts(key_meta);
                if (parts.size() != names.size())
                {
                    throw std::invalid_argument(
                        "ungroup: key column count does not match the key shape");
                }
                const bool tuple_key = parts.size() > 1;
                for (std::size_t index = 0; index < names.size(); ++index)
                {
                    if (table->GetColumnByName(names[index]) != nullptr)
                    {
                        throw std::invalid_argument(
                            "ungroup: key column '" + names[index] + "' already exists");
                    }
                    ValueView value{key.binding(), key.data()};
                    if (tuple_key)
                    {
                        const auto key_values = key.as_indexed_view();
                        const auto &part = key_values.at(index);
                        value = ValueView{part.binding(), part.data()};
                    }
                    auto column = repeated_column(value, parts[index], table->num_rows());
                    if (column == nullptr)
                    {
                        throw std::logic_error(
                            "ungroup: key column construction returned null");
                    }
                    auto column_type = column->type();
                    auto added = table->AddColumn(
                        table->num_columns(), arrow::field(names[index], std::move(column_type)),
                        std::move(column));
                    if (!added.ok())
                    {
                        throw std::runtime_error(
                            "ungroup: Arrow key column append failed: " +
                            added.status().ToString());
                    }
                    table = std::move(*added);
                }
                return table;
            }

            [[nodiscard]] std::shared_ptr<arrow::Table> project_columns(
                const std::shared_ptr<arrow::Table> &table,
                const ValueTypeMetaData *output_row, std::string_view what)
            {
                std::vector<int> indices;
                indices.reserve(output_row->field_count);
                for (std::size_t index = 0; index < output_row->field_count; ++index)
                {
                    const std::string name{
                        output_row->fields[index].name != nullptr
                            ? output_row->fields[index].name
                            : ""};
                    const int column = table->schema()->GetFieldIndex(name);
                    if (column < 0)
                    {
                        throw std::invalid_argument(std::string{what} +
                                                    ": output column '" + name +
                                                    "' has no source");
                    }
                    indices.push_back(column);
                }
                auto projected = table->SelectColumns(indices);
                if (!projected.ok())
                {
                    throw std::runtime_error(std::string{what} +
                                             ": Arrow projection failed: " +
                                             projected.status().ToString());
                }
                return std::move(*projected);
            }
        }  // namespace

        const ValueTypeMetaData *resolve_ungroup_row(const TSValueTypeMetaData *ts,
                                                     const ValueView *key_col)
        {
            ts = time_series_schema_as<AnyTSD>(ts);
            if (ts == nullptr) { return nullptr; }
            const auto *child = ts->element_ts();
            const ValueTypeMetaData *row = nullptr;
            if (ts_value_is_frame(child)) { row = child->value_schema->element_type; }
            else
            {
                const auto *value = ts_value_schema(child);
                if (value != nullptr && value->value_kind() == ValueTypeKind::Bundle)
                {
                    row = value;
                }
            }
            if (row == nullptr || key_col == nullptr) { return row; }

            const auto names = key_column_names(*key_col);
            const auto parts = key_parts(ts->key_type());
            if (names.size() != parts.size())
            {
                throw std::invalid_argument(
                    "ungroup: key column count does not match the key shape");
            }
            std::vector<std::pair<std::string, const ValueTypeMetaData *>> fields;
            fields.reserve(row->field_count + names.size());
            for (std::size_t index = 0; index < row->field_count; ++index)
            {
                const auto &field = row->fields[index];
                fields.emplace_back(field.name != nullptr ? field.name : "", field.type);
            }
            for (std::size_t index = 0; index < names.size(); ++index)
            {
                if (field_type(row, names[index]) != nullptr)
                {
                    throw std::invalid_argument(
                        "ungroup: key column '" + names[index] + "' already exists");
                }
                fields.emplace_back(names[index], parts[index]);
            }
            return TypeRegistry::instance().un_named_bundle(fields);
        }

        Frame ungroup_frames(TSInputView &ts, const ValueView *key_col,
                             const ValueTypeMetaData *output_row)
        {
            auto dict = ts.as_dict();
            const auto names = key_col != nullptr ? key_column_names(*key_col)
                                                  : std::vector<std::string>{};
            std::vector<std::shared_ptr<arrow::Table>> tables;
            for (auto &&[key, child] : dict.valid_items())
            {
                const auto frame_value = child.value();
                const auto &frame = frame_value.checked_as<Frame>();
                if (!frame.has_value() || frame.table->num_rows() == 0) { continue; }
                auto table = frame.table;
                if (key_col != nullptr)
                {
                    table = append_key_columns(std::move(table), key, ts.schema()->key_type(),
                                               names);
                }
                tables.push_back(project_columns(table, output_row, "ungroup"));
            }
            if (tables.empty()) { return {}; }
            auto result = arrow::ConcatenateTables(tables);
            if (!result.ok())
            {
                throw std::runtime_error(
                    "ungroup: Arrow concatenate failed: " + result.status().ToString());
            }
            return Frame{std::move(*result)};
        }

        Frame ungroup_items(TSInputView &ts, const ValueTypeMetaData *output_row)
        {
            auto dict = ts.as_dict();
            std::vector<Value> rows;
            for (auto &&[key, child] : dict.valid_items())
            {
                static_cast<void>(key);
                rows.emplace_back(child.value());
            }
            if (rows.empty()) { return {}; }
            return frame_from_values(table_converter(output_row), rows);
        }

        Frame replace_frame_columns(const Frame &frame, TSInputView &columns,
                                    const ValueTypeMetaData *output_row)
        {
            if (!frame.has_value()) { return {}; }
            if (columns.schema()->kind != TSTypeKind::TSB)
            {
                throw std::invalid_argument("with_columns: columns must be a TSB");
            }
            auto bundle = columns.as_bundle();
            arrow::FieldVector fields;
            std::vector<std::shared_ptr<arrow::ChunkedArray>> values;
            fields.reserve(output_row->field_count);
            values.reserve(output_row->field_count);
            for (std::size_t out_index = 0; out_index < output_row->field_count;
                 ++out_index)
            {
                const auto &out_field = output_row->fields[out_index];
                const std::string name{out_field.name != nullptr ? out_field.name : ""};
                std::shared_ptr<arrow::ChunkedArray> column;
                for (std::size_t index = 0; index < columns.schema()->field_count(); ++index)
                {
                    const auto &field = columns.schema()->fields()[index];
                    if (field.name == nullptr || field.name != name) { continue; }
                    auto child = bundle.at(index);
                    if (!child.valid())
                    {
                        throw std::invalid_argument(
                            "with_columns: replacement column '" + name + "' is invalid");
                    }
                    const auto *meta = child.schema()->value_schema;
                    if (TypeRegistry::instance().is_series(meta))
                    {
                        if (meta->element_type != out_field.type)
                        {
                            throw std::invalid_argument(
                                "with_columns: Series column '" + name +
                                "' has the wrong element type");
                        }
                        const auto series_value = child.value();
                        const auto &series = series_value.checked_as<Series>();
                        if (!series.has_value() ||
                            series.array->length() != frame.table->num_rows())
                        {
                            throw std::invalid_argument(
                                "with_columns: Series column '" + name +
                                "' must match the frame row count");
                        }
                        column = std::make_shared<arrow::ChunkedArray>(series.array);
                    }
                    else
                    {
                        if (meta != out_field.type)
                        {
                            throw std::invalid_argument(
                                "with_columns: replacement column '" + name +
                                "' has the wrong scalar type");
                        }
                        column = repeated_column(child.value(), meta,
                                                 frame.table->num_rows());
                    }
                    break;
                }
                if (column == nullptr) { column = frame.table->GetColumnByName(name); }
                if (column == nullptr)
                {
                    throw std::invalid_argument(
                        "with_columns: output column '" + name + "' has no source");
                }
                fields.push_back(arrow::field(name, column->type()));
                values.push_back(std::move(column));
            }
            return Frame{arrow::Table::Make(
                arrow::schema(std::move(fields), frame.table->schema()->metadata()),
                std::move(values), frame.table->num_rows())};
        }
        // -----------------------------------------------------------------
        // convert / combine frame targets
        // -----------------------------------------------------------------

        bool value_is_frame(const ValueTypeMetaData *meta)
        {
            return TypeRegistry::instance().is_frame(meta);
        }

        bool ts_value_is_frame(const TSValueTypeMetaData *ts)
        {
            return ts != nullptr && ts->kind == TSTypeKind::TS && value_is_frame(ts->value_schema);
        }

        std::string mapping_entry(const ValueView &mapping, std::string_view key)
        {
            if (!mapping.has_value() || !mapping.is_map()) { return {}; }
            auto        map = mapping.as_map();
            const Value probe{Str{std::string{key}}};
            if (!map.contains(probe.view())) { return {}; }
            return std::string{map.at(probe.view()).checked_as<Str>()};
        }

        void start_convert_tsd_frame(const TSInputView &ts, const ValueView &mapping,
                                     const TSOutputView &out, ToFramePlan *&plan_out)
        {
            auto        plan   = std::make_unique<ToFramePlan>();
            plan->dict         = true;
            const auto *child  = ts.schema()->element_ts();
            const auto *fields = child->value_schema;
            if (fields == nullptr || fields->value_kind() != ValueTypeKind::Bundle)
            {
                throw std::invalid_argument("convert: TSD to Frame needs compound-valued elements");
            }
            const std::string key_col = mapping_entry(mapping, "key_col");

            const ValueTypeMetaData *columns = out.schema()->value_schema->element_type;
            if (columns == nullptr)
            {
                // Untyped Frame target: columns are the element's fields
                // (plus the key column when requested).
                std::vector<std::pair<std::string, const ValueTypeMetaData *>> spec;
                if (!key_col.empty()) { spec.emplace_back(key_col, ts.schema()->key_type()); }
                for (std::size_t i = 0; i < fields->field_count; ++i)
                {
                    spec.emplace_back(fields->fields[i].name != nullptr ? fields->fields[i].name : "",
                                      fields->fields[i].type);
                }
                columns = TypeRegistry::instance().un_named_bundle(spec);
            }
            plan->row_meta  = columns;
            plan->converter = &table_converter(columns);
            for (std::size_t i = 0; i < columns->field_count; ++i)
            {
                const char            *name = columns->fields[i].name;
                const std::string_view column{name != nullptr ? name : ""};
                ToFramePlan::Column    entry;
                if (!key_col.empty() && column == key_col) { entry.source = ToFramePlan::Source::Key; }
                else
                {
                    // The TSD child is a TS over a compound VALUE: cells walk
                    // the value's fields, not time-series children.
                    entry.source   = ToFramePlan::Source::ValueField;
                    entry.ts_field = field_index_of(fields, column, "convert");
                }
                plan->columns.push_back(entry);
            }
            plan_out = plan.release();
        }

        void start_convert_value_frame(const TSInputView &ts, const TSOutputView &out,
                                       ToFramePlan *&plan_out)
        {
            auto        plan  = std::make_unique<ToFramePlan>();
            const auto *value = ts.schema()->value_schema;
            const auto *element =
                value->value_kind() == ValueTypeKind::List ? value->element_type : value;
            if (element == nullptr || element->value_kind() != ValueTypeKind::Bundle)
            {
                throw std::invalid_argument("convert: value to Frame needs a compound payload");
            }
            const ValueTypeMetaData *columns = out.schema()->value_schema->element_type;
            if (columns == nullptr) { columns = element; }
            plan->row_meta  = columns;
            plan->converter = &table_converter(columns);
            for (std::size_t i = 0; i < columns->field_count; ++i)
            {
                const char *name = columns->fields[i].name;
                ToFramePlan::Column entry;
                entry.source   = ToFramePlan::Source::Field;
                entry.ts_field = field_index_of(element, name != nullptr ? name : "", "convert");
                plan->columns.push_back(entry);
            }
            plan_out = plan.release();
        }

        namespace
        {
            [[nodiscard]] Value value_row(const ToFramePlan &plan, const ValueView &element)
            {
                Value row{checked_binding(plan.row_meta, "convert")};
                for (std::size_t i = 0; i < plan.columns.size(); ++i)
                {
                    auto      bundle = element.as_bundle();
                    ValueView cell   = ValueView{bundle.at(plan.columns[i].ts_field).binding(),
                                                 bundle.at(plan.columns[i].ts_field).data()};
                    set_bundle_field(row, i, cell);
                }
                return row;
            }

            void publish_frame(const ToFramePlan &plan, std::vector<Value> rows,
                               const TSOutputView &out)
            {
                Frame frame = frame_from_values(*plan.converter, rows);
                Value boxed{checked_binding(out.schema()->value_schema, "convert")};
                *static_cast<Frame *>(const_cast<void *>(boxed.view().data())) = std::move(frame);
                apply_current_value(out, boxed.view());
            }
        }  // namespace

        void eval_convert_value_frame(const ToFramePlan &plan, const TSInputView &ts,
                                      const TSOutputView &out)
        {
            std::vector<Value> rows;
            const ValueView    value = ts.value();
            if (value.schema()->value_kind() == ValueTypeKind::List)
            {
                auto list = value.as_list();
                for (ValueView element : list) { rows.push_back(value_row(plan, element)); }
            }
            else { rows.push_back(value_row(plan, value)); }
            publish_frame(plan, std::move(rows), out);
        }

        void eval_convert_frame_frame(const ValueView &mapping, const TSInputView &ts,
                                      const TSOutputView &out)
        {
            Frame frame = ts.value().checked_as<Frame>();
            if (mapping.has_value() && mapping.is_map() && mapping.as_map().size() > 0)
            {
                std::vector<std::pair<std::string, std::string>> renames;
                auto map = mapping.as_map();
                for (auto &&[key, value] : map.items())
                {
                    renames.emplace_back(std::string{key.checked_as<Str>()},
                                         std::string{value.checked_as<Str>()});
                }
                frame = frame_rename_columns(frame, renames);
            }
            Value boxed{checked_binding(out.schema()->value_schema, "convert")};
            *static_cast<Frame *>(const_cast<void *>(boxed.view().data())) = std::move(frame);
            apply_current_value(out, boxed.view());
        }

        void start_combine_frame(const TSInputView &ts, const TSOutputView &out, ToFramePlan *&plan_out)
        {
            auto        plan    = std::make_unique<ToFramePlan>();
            const auto *columns = frame_columns_schema(out.schema()->value_schema, "combine");
            const auto *bundle  = ts.schema()->value_schema;   // the structural TSB
            plan->row_meta      = columns;
            plan->converter     = &table_converter(columns);
            for (std::size_t i = 0; i < columns->field_count; ++i)
            {
                const char *name = columns->fields[i].name;
                ToFramePlan::Column entry;
                entry.source   = ToFramePlan::Source::Field;
                entry.ts_field = field_index_of(bundle, name != nullptr ? name : "", "combine");
                plan->columns.push_back(entry);
            }
            plan_out = plan.release();
        }

        void eval_combine_frame(const ToFramePlan &plan, const TSInputView &ts, const TSOutputView &out)
        {
            // Each input field is a TS[tuple[T, ...]] COLUMN; rows zip them.
            auto        bundle = const_cast<TSInputView &>(ts).as_bundle();
            std::size_t rows_n = 0;
            {
                auto first = bundle.at(plan.columns.front().ts_field);
                if (!first.valid()) { return; }
                rows_n = first.value().as_indexed_view().size();
            }
            std::vector<Value> rows;
            for (std::size_t r = 0; r < rows_n; ++r)
            {
                Value row{checked_binding(plan.row_meta, "combine")};
                for (std::size_t i = 0; i < plan.columns.size(); ++i)
                {
                    auto child = bundle.at(plan.columns[i].ts_field);
                    if (!child.valid()) { continue; }
                    auto column = child.value().as_indexed_view();
                    if (r >= column.size()) { continue; }
                    const ValueView &cell = column.at(r);
                    set_bundle_field(row, i, cell);
                }
                rows.push_back(std::move(row));
            }
            publish_frame(plan, std::move(rows), out);
        }
    }  // namespace data_frame_detail

    void register_data_frame_operators()
    {
        register_overload<len_, len_frame_impl>();
        register_overload<from_data_frame, from_data_frame_impl>();
        register_overload<from_data_frame_batches, from_data_frame_batches_impl>();
        register_overload<replay_data_frame, replay_data_frame_impl>();
        register_overload<to_data_frame, to_data_frame_tsd_impl>();
        register_overload<to_data_frame, to_data_frame_impl>();
        register_overload<group_by, group_by_impl>();
        register_overload<sorted_, sorted_frame_impl>();
        register_overload<sorted_, sorted_metadata_frame_impl>();
        register_overload<concat, concat_frame_impl>();
        register_overload<concat, concat_metadata_frame_impl>();
        register_overload<data_frame::join, join_frame_impl>();
        register_overload<data_frame::filter_frame, filter_frame_impl>();
        register_overload<data_frame::filter_frame, filter_metadata_frame_impl>();
        register_overload<data_frame::filter_cs, filter_cs_impl>();
        register_overload<data_frame::filter_cs, filter_cs_metadata_frame_impl>();
        register_overload<data_frame::ungroup, ungroup_frame_impl>();
        register_overload<data_frame::ungroup_with_keys, ungroup_frame_with_keys_impl>();
        register_overload<data_frame::ungroup, ungroup_items_impl>();
        register_overload<data_frame::with_columns, with_columns_impl>();
        register_overload<data_frame::with_columns, with_columns_metadata_frame_impl>();
        register_overload<convert, convert_tsd_to_frame_impl>();
        register_overload<convert, convert_frame_to_frame_impl>();
        register_overload<convert, convert_value_to_frame_impl>();
        register_overload<combine, combine_frame_impl>();
    }
}  // namespace hgraph::stdlib
