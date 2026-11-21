#ifndef HGRAPH_PERSISTENCE_PARTITION_WITHDRAWALS_H
#define HGRAPH_PERSISTENCE_PARTITION_WITHDRAWALS_H

#include <algorithm>
#include <bit>
#include <unordered_map>
#include <utility>

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/table.h>
#include <arrow/util/key_value_metadata.h>
#include <hgraph/lib/std/operators/table_rows.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_output.h>

namespace hgraph::persistence::record_replay_frame_detail
{
    // A sparse frame's null cells normally mean "no field update". A file that
    // actually contains a withdrawal declares its additional column explicitly.
    inline constexpr char WITHDRAWAL_COLUMN[] = "hgraph.recording.withdrawals.v1.column";

    inline void check_arrow(const arrow::Status &status)
    {
        if (!status.ok()) { throw std::runtime_error(status.ToString()); }
    }

    using FieldPath = std::vector<std::int32_t>;
    using PartitionLayout = stdlib::table_ts_detail::TsTableLayout;

    inline bool supports_partition_withdrawals(const PartitionLayout &layout)
    {
        if (!layout.partitioned() || layout.is_multi_row || !layout.child_plans.empty()) { return false; }
        // Only the structural projection of a dictionary chain names these
        // fields. A registered multi-row or embedded child strategy owns
        // its own interpretation of omitted cells.
        const auto *schema = layout.ts_schema;
        for (const auto &level : layout.levels) {
            if (!schema || schema->kind != TSTypeKind::TSD || level.key_meta != schema->key_type()) {
                return false;
            }
            schema = schema->element_ts();
        }
        return schema && schema == layout.leaf_ts &&
               (schema->kind == TSTypeKind::TSB || (layout.levels.size() > 1 && schema->kind == TSTypeKind::TS));
    }

    template <typename Cell>
    Value partition_key(const PartitionLayout::Level &level, const Cell &cell)
    {
        if (level.key_paths.size() == 1 && level.key_paths.front().empty()) {
            return cell(level.first_key_col);
        }
        std::vector<Value> leaves;
        leaves.reserve(level.key_paths.size());
        for (std::size_t index = 0; index < level.key_paths.size(); ++index) {
            leaves.push_back(cell(level.first_key_col + index));
        }
        return stdlib::table_ts_detail::assemble_from_paths(level.key_meta, level.key_paths, leaves);
    }

    // Collect only rows with withdrawals. Ordinary recordings keep their exact
    // existing Arrow schema and the accumulator starts afresh for each segment.
    using WithdrawnRows = std::vector<std::pair<std::int64_t, std::vector<FieldPath>>>;

    struct PartitionObservation
    {
        struct KeyHash {
            std::size_t operator()(const std::vector<Value> &key) const {
                std::size_t hash = 0;
                for (const auto &part : key) { hash = std::rotl(hash, 7) ^ part.hash(); }
                return hash;
            }
        };

        const PartitionLayout &layout;
        std::unordered_map<std::vector<Value>, std::vector<FieldPath>, KeyHash> pending;
        std::vector<Value> row_key;
        bool removed{false};

        explicit PartitionObservation(const PartitionLayout &layout_) : layout{layout_} {}

        struct Key {
            const Key *parent;
            const PartitionLayout::Level &level;
            const ValueView &value;
        };

        static void append_key(std::vector<Value> &parts, const Key &key)
        {
            if (key.parent) { append_key(parts, *key.parent); }
            for (const auto &path : key.level.key_paths) {
                auto part = ValueView{key.value.binding(), key.value.data()};
                for (auto field : path) {
                    const auto &next = part.as_indexed_view().at(field);
                    part = ValueView{next.binding(), next.data()};
                }
                parts.emplace_back(part);
            }
        }

        // A sparse cell can mean an untouched field. Only an owned field
        // cleared in this evaluation contributes a withdrawal.
        void inspect_fields(const TSDataView &node, DateTime now, const Key &key, FieldPath &fields)
        {
            if (!node.valid()) { return; }
            if (node.tracking().cleared_at(now)) {
                std::vector<Value> parts;
                append_key(parts, key);
                auto &paths = pending[std::move(parts)];
                if (std::find(paths.begin(), paths.end(), fields) == paths.end()) {
                    paths.push_back(fields);
                }
            } else if (node.schema()->kind == TSTypeKind::TSB && node.has_current_value()) {
                const auto bundle = node.as_bundle();
                for (std::size_t index = 0; index < node.schema()->field_count(); ++index) {
                    fields.push_back(static_cast<std::int32_t>(index));
                    inspect_fields(bundle.at(index), now, key, fields);
                    fields.pop_back();
                }
            }
        }

        void collect(const TSDataView &owner, std::size_t depth, DateTime now, const Key *parent,
                     FieldPath &fields)
        {
            const auto dict = owner.as_dict();
            const auto inspect = [&](const ValueView &part, const TSDataView &child) {
                const Key key{parent, layout.levels[depth], part};
                if (depth + 1 == layout.levels.size()) {
                    if (depth > 0 || child.has_current_value()) { inspect_fields(child, now, key, fields); }
                } else if (child.has_current_value()) {
                    collect(child, depth + 1, now, &key, fields);
                }
            };
            for (const auto &[key, child] : dict.modified_items(now)) { inspect(key, child); }
            for (const auto key : dict.removed_keys()) {
                if (dict.contains(key)) { inspect(key, dict.at(key)); }
            }
        }
        void cell(std::size_t column, const ValueView &value)
        {
            if (column < 2 || column >= layout.value_col_start) { return; }
            for (const auto &level : layout.levels) {
                if (column == level.removed_col) {
                    removed |= value.checked_as<Bool>();
                    return;
                }
            }
            row_key.emplace_back(value);
        }

        void finish_row(WithdrawnRows &rows, std::int64_t row)
        {
            if (!std::exchange(removed, false)) {
                if (auto it = pending.find(row_key); it != pending.end()) {
                    rows.emplace_back(row, std::move(it->second));
                    pending.erase(it);
                }
            }
            row_key.clear();
        }

        // Snap emits valid dictionary items. A newly withdrawn leaf still
        // owns its key, so give that otherwise omitted partition its own row.
        void finish_tick(DateTime now, DateTime as_of, const stdlib::table_ts_detail::RowSink &sink)
        {
            const Value date{now}, revision{as_of}, retained{false};
            while (!pending.empty()) {
                const auto &keys = pending.begin()->first;
                sink.cell(sink.context, 0, date.view());
                sink.cell(sink.context, 1, revision.view());
                std::size_t index = 0;
                for (const auto &level : layout.levels) {
                    sink.cell(sink.context, level.removed_col, retained.view());
                    for (std::size_t part = 0; part < level.key_paths.size(); ++part) {
                        sink.cell(sink.context, level.first_key_col + part, keys.at(index++).view());
                    }
                }
                sink.end_row(sink.context);
            }
        }
    };

    inline Frame annotate_withdrawals(Frame frame, WithdrawnRows &rows)
    {
        if (rows.empty()) { return frame; }
        auto fields = std::make_shared<arrow::Int32Builder>();
        arrow::ListBuilder lists{arrow::default_memory_pool(), fields};
        for (const auto &[row, paths] : rows) {
            check_arrow(lists.AppendNulls(row - lists.length()));
            check_arrow(lists.Append());
            for (const auto &path : paths) {
                check_arrow(fields->Append(static_cast<std::int32_t>(path.size())));
                check_arrow(fields->AppendValues(path));
            }
        }
        check_arrow(lists.AppendNulls(frame.table->num_rows() - lists.length()));
        std::shared_ptr<arrow::Array> array;
        check_arrow(lists.Finish(&array));
        std::string name = "__hgraph_withdrawn_fields__";
        while (frame.table->schema()->GetFieldByName(name)) { name += '_'; }
        auto extended = frame.table->AddColumn(frame.table->num_columns(), arrow::field(name, array->type()),
                                                std::make_shared<arrow::ChunkedArray>(array));
        check_arrow(extended.status());
        auto metadata = frame.table->schema()->metadata()->Copy();
        metadata->Append(WITHDRAWAL_COLUMN, name);
        frame.table = (*extended)->ReplaceSchemaMetadata(std::move(metadata));
        rows.clear();
        return frame;
    }

    inline int withdrawal_column(const Frame &frame)
    {
        const auto metadata = frame.table->schema()->metadata();
        const int entry = metadata ? metadata->FindKey(WITHDRAWAL_COLUMN) : -1;
        if (entry < 0) { return -1; }
        const int column = frame.table->schema()->GetFieldIndex(metadata->value(entry));
        if (column < 0 || !frame.table->field(column)->type()->Equals(arrow::list(arrow::int32()))) {
            throw std::invalid_argument("replay: missing or incompatible declared withdrawal column");
        }
        return column;
    }

    template <typename Cell>
    void apply_partition_withdrawals(const Frame &frame, int column, std::int64_t row,
                                     const PartitionLayout &layout, const TSOutputView &out, const Cell &cell)
    {
        const auto marker = frame.table->column(column)->GetScalar(row);
        check_arrow(marker.status());
        if (!(*marker)->is_valid) { return; }
        const auto &flat = static_cast<const arrow::Int32Array &>(
            *static_cast<const arrow::ListScalar &>(**marker).value);
        auto target = out.data_view().borrowed_ref();
        for (const auto &level : layout.levels) {
            auto key = partition_key(level, cell);
            target = target.as_dict().begin_mutation(out.evaluation_time()).at(key.view());
        }
        for (std::int64_t at = 0; at < flat.length();) {
            const auto depth = flat.IsNull(at) ? -1 : flat.Value(at);
            ++at;
            if (depth < 0 || depth > flat.length() - at) {
                throw std::invalid_argument("replay: malformed withdrawal field path");
            }
            auto field = target.borrowed_ref();
            for (std::int32_t part = 0; part < depth; ++part) {
                const auto index = flat.IsNull(at) ? -1 : flat.Value(at);
                ++at;
                if (field.schema()->kind != TSTypeKind::TSB || index < 0 ||
                    static_cast<std::size_t>(index) >= field.schema()->field_count()) {
                    throw std::invalid_argument("replay: withdrawal path names an unknown bundle field");
                }
                auto bundle = field.as_bundle();
                field = bundle.at(static_cast<std::size_t>(index));
            }
            static_cast<void>(field.begin_mutation(out.evaluation_time()).invalidate());
        }
    }
}
#endif
