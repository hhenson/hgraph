#ifndef HGRAPH_PERSISTENCE_DICTIONARY_VALIDITY_RECORDING_H
#define HGRAPH_PERSISTENCE_DICTIONARY_VALIDITY_RECORDING_H

#include "partition_withdrawals.h"

#include <arrow/compute/api_scalar.h>

namespace hgraph::persistence::record_replay_frame_detail
{
    inline std::string validity_metadata(std::size_t depth)
    {
        return "hgraph.recording.dictionary_validity.v1.level." + std::to_string(depth + 1);
    }

    template <typename T> T checked(arrow::Result<T> result)
    {
        check_arrow(result.status());
        return std::move(result).ValueUnsafe();
    }

    // A directly addressable owner has a complete prefix but no descendant
    // key. The validity flag at the last populated level terminates this row.
    inline void write_dictionary_validity(const PartitionLayout &layout,
            const std::vector<Value> &parts, bool valid, DateTime now, DateTime as_of,
            const stdlib::table_ts_detail::RowSink &sink, TableRecorder &recorder,
            std::size_t first_validity)
    {
        sink.cell(sink.context, 0, Value{now}.view());
        sink.cell(sink.context, 1, Value{as_of}.view());
        std::size_t index = 0;
        for (std::size_t depth = 0; depth + 1 < layout.levels.size(); ++depth) {
            const auto &level = layout.levels[depth];
            sink.cell(sink.context, level.removed_col, Value{false}.view());
            for (std::size_t field = 0; field < level.key_paths.size(); ++field) {
                sink.cell(sink.context, level.first_key_col + field, parts.at(index++).view());
            }
            if (index == parts.size()) {
                recorder.append_cell(first_validity + depth, Value{valid}.view());
                sink.end_row(sink.context);
                return;
            }
        }
        throw std::logic_error("record: invalid intermediate dictionary key");
    }

    // Feed the existing as-of/time selector a prefix terminator like that of
    // a removal. Keep this private flag separate from the stored removal so
    // that applying the selected row retains the owner's dictionary key.
    inline std::vector<int> resolve_dictionary_validity(Frame &frame, const PartitionLayout &layout,
                                                        std::vector<int> &columns)
    {
        auto table = frame.table;
        const auto &meta = table->schema()->metadata();
        std::vector<int> validity;
        for (std::size_t depth = 0; depth + 1 < layout.levels.size(); ++depth) {
            const auto key = validity_metadata(depth);
            const int index = meta ? meta->FindKey(key) : -1;
            if (index < 0) { continue; }
            const int flag = table->schema()->GetFieldIndex(meta->value(index));
            if (flag < 0 || table->field(flag)->type()->id() != arrow::Type::BOOL ||
                std::find(columns.begin(), columns.end(), flag) != columns.end() ||
                std::find(validity.begin(), validity.end(), flag) != validity.end()) {
                throw std::invalid_argument("replay: missing or incompatible dictionary validity column");
            }
            const auto &states = table->column(flag);
            if (states->null_count() == states->length()) { continue; }
            validity.resize(depth + 1, -1);
            validity[depth] = flag;
            auto terminal = checked(arrow::compute::IsValid(states));
            int &removed = columns[layout.levels[depth].removed_col];
            if (removed >= 0) {
                terminal = checked(arrow::compute::IfElse(terminal, arrow::Datum{true}, table->column(removed)));
            }
            removed = table->num_columns();
            table = checked(table->AddColumn(removed, arrow::field("", arrow::boolean()), terminal.chunked_array()));
        }
        frame.table = std::move(table);
        return validity;
    }

    inline std::optional<std::pair<std::size_t, bool>> dictionary_validity_at(
            const Frame &frame, const std::vector<int> &columns, std::int64_t row)
    {
        std::optional<std::pair<std::size_t, bool>> result;
        for (std::size_t depth = 0; depth < columns.size(); ++depth) {
            if (columns[depth] < 0) { continue; }
            const auto state = checked(frame.table->column(columns[depth])->GetScalar(row));
            if (!state->is_valid) { continue; }
            if (result) { throw std::invalid_argument("replay: multiple validity levels on one dictionary row"); }
            result.emplace(depth, static_cast<const arrow::BooleanScalar &>(*state).value);
        }
        return result;
    }

    template <typename Cell>
    void apply_dictionary_validity(const PartitionLayout &layout, const TSOutputView &out,
                                   std::size_t depth, bool valid, const Cell &cell)
    {
        auto owner = out.data_view().borrowed_ref();
        for (std::size_t i = 0; i <= depth; ++i) {
            const auto key = partition_key(layout.levels[i], cell);
            owner = owner.as_dict().begin_mutation(out.evaluation_time()).at(key.view());
        }
        if (valid) { owner.as_dict().begin_mutation(out.evaluation_time()).touch(); }
        else { static_cast<void>(owner.begin_mutation(out.evaluation_time()).invalidate()); }
    }
}
#endif
