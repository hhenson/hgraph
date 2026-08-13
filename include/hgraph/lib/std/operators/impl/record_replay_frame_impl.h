#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_RECORD_REPLAY_FRAME_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_RECORD_REPLAY_FRAME_IMPL_H

#include <hgraph/lib/std/operators/impl/data_frame_impl.h>
#include <hgraph/lib/std/operators/impl/table_impl.h>

#include <arrow/array.h>
#include <arrow/table.h>
#include <hgraph/lib/std/operators/io.h>
#include <hgraph/runtime/node_scheduler.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/record_replay.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/value/compact_storage.h>
#include <hgraph/types/value/table_codec.h>
#include <hgraph/types/value/value_builder.h>

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace hgraph::stdlib
{
    namespace record_replay_frame_detail
    {
        [[nodiscard]] inline std::string frame_key(const TraitsView &traits, std::string_view recordable_id,
                                                   std::string_view key)
        {
            return record_replay::fq_recordable_id(traits, recordable_id) + "." + std::string{key};
        }

        /** An EMPTY ``tuple[str, ...]``, as the rename arguments' default.
            ``Value{*meta}`` will not do: that is an UNSET value, which the
            operator registry rejects outright ("scalar default value must not
            be empty") because an unset default is Python's ``None`` and only
            means anything for a time-series parameter. This is a real, empty
            list value - the shape that says "rename nothing". */
        [[nodiscard]] inline Value empty_names()
        {
            const auto *meta = scalar_descriptor<HomogeneousTuple<Str>>::value_meta();
            const auto  binding =
                ValuePlanFactory::instance().type_for(scalar_descriptor<Str>::value_meta());
            ListBuilder  builder{binding};
            ListStorage  storage = builder.build_storage();
            return Value{compact_list_type(binding, *meta), &storage};
        }

        /** Read a ``tuple[str, ...]`` wiring argument. Empty means "keep the
            layout's own names", so an unconfigured call renames nothing. */
        [[nodiscard]] inline std::vector<std::string> column_names(const ValueView &value)
        {
            std::vector<std::string> out;
            const auto               list = value.as_indexed_view();
            out.reserve(list.size());
            for (std::size_t i = 0; i < list.size(); ++i)
            {
                out.emplace_back(list.at(i).checked_as<Str>());
            }
            return out;
        }

        /** Rename compatibility projections back to the canonical layout.

            The replay machinery has one semantic column vocabulary. A
            release/0.5 store may use configured partition/removal names and
            a frame-valued recording may prefix its payload columns; normalize
            those names once at start instead of teaching every row reader
            about aliases. Missing optional as-of/removal columns remain
            missing and are handled by replay selection/application. */
        [[nodiscard]] inline Frame canonical_replay_frame(
            Frame frame, const table_ts_detail::TsTableLayout &layout,
            std::span<const std::string> stored_names)
        {
            std::vector<std::string> names = frame.table->ColumnNames();
            for (std::size_t column = 0; column < layout.keys.size(); ++column)
            {
                const std::string &stored = stored_names[column];
                const std::string &canonical = layout.keys[column];
                if (stored.empty() || stored == canonical) { continue; }

                const int source = frame.table->schema()->GetFieldIndex(stored);
                if (source < 0) { continue; }
                const int existing = frame.table->schema()->GetFieldIndex(canonical);
                if (existing >= 0 && existing != source)
                {
                    throw std::runtime_error(
                        "replay: column projection '" + stored + "' -> '" +
                        canonical + "' collides with an existing column");
                }
                names[static_cast<std::size_t>(source)] = canonical;
            }
            for (std::size_t lhs = 0; lhs < names.size(); ++lhs)
            {
                for (std::size_t rhs = lhs + 1; rhs < names.size(); ++rhs)
                {
                    if (names[lhs] == names[rhs])
                    {
                        throw std::runtime_error(
                            "replay: projected column name is not unique: '" +
                            names[lhs] + "'");
                    }
                }
            }
            auto renamed = frame.table->RenameColumns(names);
            if (!renamed.ok())
            {
                throw std::runtime_error(
                    "replay: failed to normalize stored columns: " +
                    renamed.status().ToString());
            }
            frame.table = *renamed;
            return frame;
        }

        /**
         * Fold the call site's choices over the wiring-time default.
         *
         * ``Inherit`` is what makes the configuration local WITH a global
         * default: an unconfigured call records exactly as it did before, and
         * two calls in one graph differ by being called differently rather
         * than through a registry keyed on their name.
         *
         * A global ``Config::as_of`` names a fixed as-of for the run, so it
         * selects ``Fixed`` unless the call site omits the column outright.
         */
        [[nodiscard]] inline table_ts_detail::TableRecordingOptions recording_options(
            RecordAsOf as_of, RecordRemoves removes, const ValueView &partition_names,
            const ValueView &removed_names, std::string_view frame_prefix,
            const record_replay::Config &config)
        {
            using Options = table_ts_detail::TableRecordingOptions;
            Options options;
            options.partition_names = column_names(partition_names);
            options.removed_names   = column_names(removed_names);
            options.frame_prefix    = std::string{frame_prefix};
            switch (as_of)
            {
            case RecordAsOf::Omit: options.as_of = Options::AsOf::Omit; break;
            case RecordAsOf::Track: options.as_of = Options::AsOf::Track; break;
            case RecordAsOf::Inherit:
                if (config.as_of.has_value())
                {
                    options.as_of       = Options::AsOf::Fixed;
                    options.as_of_value = config.as_of;
                }
                break;
            }
            switch (removes)
            {
            case RecordRemoves::Omit: options.removes = Options::Removes::Omit; break;
            case RecordRemoves::Track: options.removes = Options::Removes::Track; break;
            case RecordRemoves::Inherit: break;
            }
            return options;
        }

        /** Heap recorder handle owned across start/eval/stop via node State. */
        struct RecorderHandle
        {
            TableRecorder recorder;
            std::string   fq_key;
            /** Output column per layout column; ``npos`` for one the recording
                omits, so the sink can drop a cell rather than shift the rest. */
            std::vector<std::size_t> output_of_layout_column{};
            bool                     emit_removals{false};
            /** Present only for an inherited fixed as-of. Explicit Track
                always follows evaluation time even when the graph default is fixed. */
            std::optional<DateTime>  fixed_as_of{};

            static constexpr std::size_t dropped = static_cast<std::size_t>(-1);
        };

        /** Feeds emitted cells into the recorder, mapping layout columns to the
            recording's own. */
        struct RecordingSink
        {
            RecorderHandle *handle{nullptr};

            static void cell(void *context, std::size_t column, const ValueView &value)
            {
                auto *self = static_cast<RecordingSink *>(context);
                if (column >= self->handle->output_of_layout_column.size()) { return; }
                const std::size_t output = self->handle->output_of_layout_column[column];
                if (output == RecorderHandle::dropped) { return; }
                self->handle->recorder.append_cell(output, value);
            }

            static void end_row(void *context)
            {
                static_cast<RecordingSink *>(context)->handle->recorder.end_row();
            }

            [[nodiscard]] table_ts_detail::RowSink sink()
            {
                return table_ts_detail::RowSink{.context = this, .cell = &cell, .end_row = &end_row};
            }
        };

        /** Heap replay cursor owned across start/eval/stop via node State. */
        struct ReplayHandle
        {
            const TableConverter                   *converter{nullptr};
            /** Set when one recorded row is not one whole value, so the row
                cannot be read straight through the converter: partitioned (a
                row is a key plus a value) or frame-valued (a TICK is a run of
                rows). Null for the plain TS and TSB cases. */
            const table_ts_detail::TsTableLayout   *layout{nullptr};
            Frame                                   frame{};
            std::int64_t                            row{0};
            /** Stored column name per canonical layout column. Empty means
                the recording omitted that column. */
            std::vector<std::string>                 stored_names{};
        };

        /** Apply one recorded row, descending the TSD levels it names.
            The inverse of the emission walk: read this level's key, step into
            it, and recurse; at the leaf read the value columns by name. */
        inline void apply_recorded_row(const table_ts_detail::TsTableLayout &layout,
                                       const TableConverter &leaf_converter, const Frame &frame,
                                       std::int64_t row, std::size_t level_index,
                                       const TSOutputView &out, DateTime now,
                                       std::span<const std::string> stored_names)
        {
            if (level_index == layout.levels.size())
            {
                // By name, so the key and bitemporal columns are ignored.
                const Value value = read_row(leaf_converter, frame, row);
                apply_current_value(out, value.view());
                return;
            }

            const auto &level = layout.levels[level_index];
            // One cell per flattened key column, then rebuilt through the same
            // paths the recording flattened the key down. An atomic key is the
            // one-column case of this, not a separate path.
            std::vector<Value> key_leaves;
            key_leaves.reserve(level.key_paths.size());
            for (std::size_t i = 0; i < level.key_paths.size(); ++i)
            {
                const std::string &name       = stored_names[level.first_key_col + i];
                const auto         key_column = frame.table->GetColumnByName(name);
                if (key_column == nullptr)
                {
                    throw std::runtime_error("replay: recording is missing key column '" + name + "'");
                }
                key_leaves.push_back(read_table_cell(layout.col_metas[level.first_key_col + i],
                                                     *key_column->chunk(0), *frame.table->schema(), row));
            }
            const Value key = table_ts_detail::assemble_from_paths(level.key_meta, level.key_paths, key_leaves);

            auto dict_out = out.as_dict();
            auto mutation = dict_out.begin_mutation(now);

            // A removed column is only present when the recording tracked
            // removals; without it every row is a value.
            const std::string &removed_name = stored_names[level.removed_col];
            const auto removed_column = removed_name.empty() ? nullptr : frame.table->GetColumnByName(removed_name);
            if (removed_column != nullptr && !removed_column->chunk(0)->IsNull(row))
            {
                const Value removed = read_table_cell(scalar_descriptor<Bool>::value_meta(),
                                                      *removed_column->chunk(0),
                                                      *frame.table->schema(), row);
                if (removed.has_value() && removed.view().checked_as<Bool>())
                {
                    static_cast<void>(mutation.erase(key.view()));
                    return;
                }
            }

            auto child = mutation.at(key.view());
            apply_recorded_row(layout, leaf_converter, frame, row, level_index + 1,
                               TSOutputView{out.output(), child, now}, now, stored_names);
        }
    }  // namespace record_replay_frame_detail

    /** Node-State payloads carrying the heap handles (start-lifecycle pattern). */
    struct FrameRecorderState
    {
        record_replay_frame_detail::RecorderHandle *handle{nullptr};
    };

    struct FrameReplayState
    {
        record_replay_frame_detail::ReplayHandle *handle{nullptr};
    };
}  // namespace hgraph::stdlib

namespace hgraph::static_schema_detail
{
    template <>
    struct scalar_name<stdlib::FrameRecorderState>
    {
        static constexpr std::string_view value{"FrameRecorderState"};
    };

    template <>
    struct scalar_name<stdlib::FrameReplayState>
    {
        static constexpr std::string_view value{"FrameReplayState"};
    };
}  // namespace hgraph::static_schema_detail

namespace hgraph::stdlib
{
    /**
     * The Arrow data-frame record/replay backend (design record, step 4;
     * model ``record_replay::DATA_FRAME``). ``record`` appends one bitemporal
     * row per tick straight into Arrow builders (the fused P4 path) and
     * writes the finished frame to the registered frame store (P6) at
     * ``stop`` under ``fq_recordable_id.key``; ``replay`` reads the frame and
     * re-emits each row at its recorded value time.
     */
    struct record_frame_impl
    {
        static constexpr auto name = "record";

        static std::vector<std::pair<std::string_view, Value>> defaults()
        {
            return {{"recordable_id", Value{Str{}}},
                    {"as_of", Value{RecordAsOf::Inherit}},
                    {"removes", Value{RecordRemoves::Inherit}},
                    {"partition_names", record_replay_frame_detail::empty_names()},
                    {"removed_names", record_replay_frame_detail::empty_names()},
                    {"frame_prefix", Value{Str{}}}};
        }

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return record_replay::model_is(context.global_state, record_replay::DATA_FRAME);
        }

        static void start(In<"ts", TsVar<"S">> ts, Scalar<"key", Str> key,
                          Scalar<"recordable_id", Str> recordable_id, Scalar<"as_of", RecordAsOf> as_of,
                          Scalar<"removes", RecordRemoves> removes,
                          Scalar<"partition_names", ScalarVar<"PN", HomogeneousTuple<Str>>> partition_names,
                          Scalar<"removed_names", ScalarVar<"RN", HomogeneousTuple<Str>>>   removed_names,
                          Scalar<"frame_prefix", Str> frame_prefix, TraitsView traits, GlobalStateView gs,
                          State<FrameRecorderState> state)
        {
            using record_replay_frame_detail::RecorderHandle;
            using namespace table_ts_detail;

            const auto  config = record_replay::config(gs);
            // The LAYOUT, not the value schema: a value schema cannot describe
            // a TSD's key columns or removed flags, which is why recording a
            // partitioned series used to fail here (RFC 0019).
            const auto &layout =
                ts_table_layout(ts.base().schema(), config.date_key, config.as_of_key);
            const TableRecordingOptions options = record_replay_frame_detail::recording_options(
                as_of.value(), removes.value(), partition_names.value(), removed_names.value(),
                frame_prefix.value(), config);
            const RecordingColumns columns = recording_columns(layout, options);

            std::vector<std::size_t> output_of_layout_column(layout.keys.size(), RecorderHandle::dropped);
            for (std::size_t output = 0; output < columns.source.size(); ++output)
            {
                output_of_layout_column[columns.source[output]] = output;
            }

            auto handle = std::make_unique<RecorderHandle>(RecorderHandle{
                TableRecorder{columns.names, columns.metas},
                record_replay_frame_detail::frame_key(traits, recordable_id.value(), key.value()),
                std::move(output_of_layout_column),
                options.removes == TableRecordingOptions::Removes::Track,
                options.as_of == TableRecordingOptions::AsOf::Fixed ? options.as_of_value
                                                                    : std::optional<DateTime>{}});
            state.set(FrameRecorderState{handle.release()});   // owned by node State until stop
        }

        static void eval(In<"ts", TsVar<"S">> ts, Scalar<"key", Str> key,
                         Scalar<"recordable_id", Str> recordable_id, Scalar<"as_of", RecordAsOf> as_of,
                         Scalar<"removes", RecordRemoves> removes,
                         Scalar<"partition_names", ScalarVar<"PN", HomogeneousTuple<Str>>> partition_names,
                         Scalar<"removed_names", ScalarVar<"RN", HomogeneousTuple<Str>>>   removed_names,
                         Scalar<"frame_prefix", Str> frame_prefix, State<FrameRecorderState> state,
                         GlobalStateView gs, DateTime now)
        {
            static_cast<void>(key);
            static_cast<void>(recordable_id);
            // All five were resolved into the recorder's shape at start; the
            // row walk reads that shape from the handle, not from the arguments.
            static_cast<void>(as_of);
            static_cast<void>(removes);
            static_cast<void>(partition_names);
            static_cast<void>(removed_names);
            static_cast<void>(frame_prefix);
            const auto as_of_cell = state.get().handle->fixed_as_of.value_or(now);
            using namespace table_ts_detail;
            auto *handle = state.get().handle;
            const auto &layout =
                ts_table_layout(ts.base().schema(), record_replay::config(gs).date_key,
                                record_replay::config(gs).as_of_key);
            record_replay_frame_detail::RecordingSink recording{.handle = handle};
            emit_rows_to(layout, ts.base(), kToTableModeTick, now, as_of_cell, handle->emit_removals,
                         recording.sink());
        }

        static void stop(State<FrameRecorderState> state, GlobalStateView gs)
        {
            // Take ownership first so a throwing store write cannot leak.
            std::unique_ptr<record_replay_frame_detail::RecorderHandle> handle{state.get().handle};
            state.set(FrameRecorderState{});
            if (handle == nullptr) { return; }
            record_replay::store_write(gs, handle->fq_key, handle->recorder.finish());
        }
    };

    struct replay_frame_impl
    {
        static constexpr auto name = "replay";

        static std::vector<std::pair<std::string_view, Value>> defaults()
        {
            return {{"recordable_id", Value{Str{}}},
                    {"partition_names", record_replay_frame_detail::empty_names()},
                    {"removed_names", record_replay_frame_detail::empty_names()},
                    {"frame_prefix", Value{Str{}}}};
        }

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return record_replay::model_is(context.global_state, record_replay::DATA_FRAME);
        }

        static void start(Scalar<"key", Str> key, Scalar<"recordable_id", Str> recordable_id,
                          Scalar<"partition_names", ScalarVar<"PN", HomogeneousTuple<Str>>> partition_names,
                          Scalar<"removed_names", ScalarVar<"RN", HomogeneousTuple<Str>>> removed_names,
                          Scalar<"frame_prefix", Str> frame_prefix, TraitsView traits,
                          GlobalStateView gs, DateTime now, State<FrameReplayState> state,
                          SingleShotScheduler sched, Out<TsVar<"O">> out)
        {
            using record_replay_frame_detail::ReplayHandle;
            const auto  fq_key = record_replay_frame_detail::frame_key(traits, recordable_id.value(), key.value());
            Frame       frame  = record_replay::store_read(gs, fq_key);
            if (!frame.has_value())
            {
                throw std::runtime_error("replay: no recorded frame under '" + fq_key + "'");
            }
            const auto &erased = static_cast<const TSOutputView &>(out);
            const auto  config = record_replay::config(gs);
            const auto &layout =
                table_ts_detail::ts_table_layout(erased.schema(), config.date_key, config.as_of_key);
            table_ts_detail::TableRecordingOptions replay_options;
            replay_options.removes         = table_ts_detail::TableRecordingOptions::Removes::Track;
            replay_options.partition_names = record_replay_frame_detail::column_names(partition_names.value());
            replay_options.removed_names   = record_replay_frame_detail::column_names(removed_names.value());
            replay_options.frame_prefix    = frame_prefix.value();
            const auto recorded_columns = table_ts_detail::recording_columns(layout, replay_options);
            std::vector<std::string> stored_names(layout.keys.size());
            for (std::size_t i = 0; i < recorded_columns.source.size(); ++i)
            {
                stored_names[recorded_columns.source[i]] = recorded_columns.names[i];
            }
            frame = record_replay_frame_detail::canonical_replay_frame(
                std::move(frame), layout, stored_names);
            stored_names = layout.keys;
            frame = data_frame_detail::select_replay_frame(
                frame, layout, config.as_of.value_or(MAX_DT), now);
            // The leaf's value schema for the value columns; the key columns
            // are read separately, since no value schema names them. A
            // frame-valued leaf has no value schema to convert at all - its
            // columns come from the frame's own converter.
            const auto &converter =
                layout.is_multi_row
                    ? *layout.frame_converter
                    : table_converter(layout.partitioned() ? layout.leaf_ts->value_schema
                                                           : erased.schema()->value_schema,
                                      config.date_key, config.as_of_key);
            auto handle = std::make_unique<ReplayHandle>(
                ReplayHandle{&converter, layout.multi() ? &layout : nullptr, std::move(frame), 0,
                             std::move(stored_names)});
            if (frame_rows(handle->frame) > 0)
            {
                sched.schedule(frame_value_time(converter, handle->frame, 0));
            }
            state.set(FrameReplayState{handle.release()});   // owned by node State until stop
        }

        static void eval(Scalar<"key", Str> key, Scalar<"recordable_id", Str> recordable_id,
                         Scalar<"partition_names", ScalarVar<"PN", HomogeneousTuple<Str>>> partition_names,
                         Scalar<"removed_names", ScalarVar<"RN", HomogeneousTuple<Str>>> removed_names,
                         Scalar<"frame_prefix", Str> frame_prefix, State<FrameReplayState> state,
                         NodeScheduler sched, DateTime now, Out<TsVar<"O">> out)
        {
            static_cast<void>(key);
            static_cast<void>(recordable_id);
            static_cast<void>(partition_names);
            static_cast<void>(removed_names);
            static_cast<void>(frame_prefix);
            auto      *handle = state.get().handle;
            const auto rows   = frame_rows(handle->frame);
            if (handle->layout != nullptr && handle->layout->is_multi_row)
            {
                // A frame-valued leaf recorded one row per FRAME row, so this
                // tick is the whole RUN of rows at ``now`` - consumed as one
                // frame rather than applied row by row.
                const std::int64_t first = handle->row;
                while (handle->row < rows &&
                       frame_value_time(*handle->converter, handle->frame, handle->row) == now)
                {
                    ++handle->row;
                }
                if (handle->row > first)
                {
                    table_ts_detail::apply_recorded_frame_rows(*handle->layout, handle->frame, first,
                                                               handle->row - first,
                                                               static_cast<const TSOutputView &>(out));
                }
                if (handle->row < rows)
                {
                    sched.schedule(frame_value_time(*handle->converter, handle->frame, handle->row));
                }
                return;
            }
            while (handle->row < rows && frame_value_time(*handle->converter, handle->frame, handle->row) == now)
            {
                if (handle->layout != nullptr)
                {
                    record_replay_frame_detail::apply_recorded_row(
                        *handle->layout, *handle->converter, handle->frame, handle->row, 0,
                        static_cast<const TSOutputView &>(out), now, handle->stored_names);
                }
                else
                {
                    Value value = read_row(*handle->converter, handle->frame, handle->row);
                    apply_delta(out, value.view());
                }
                ++handle->row;
            }
            if (handle->row < rows)
            {
                sched.schedule(frame_value_time(*handle->converter, handle->frame, handle->row));
            }
        }

        static void stop(State<FrameReplayState> state)
        {
            std::unique_ptr<record_replay_frame_detail::ReplayHandle> handle{state.get().handle};
            state.set(FrameReplayState{});
        }
    };

    /**
     * ``compare`` — the backtesting comparison sink (the Q-compare ruling):
     * per tick, records whether ``lhs`` and ``rhs`` hold equal values into a
     * bitemporal ``equal`` frame written through the REGISTERED frame store
     * (P6) at stop, under ``fq.__compare__``. Model-independent — the store
     * is the pluggable seam, so a single implementation serves every model.
     */
    struct compare_impl
    {
        static constexpr auto name = "compare";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return !record_replay::model_is(
                context.global_state, record_replay::IN_MEMORY);
        }

        static std::vector<std::pair<std::string_view, Value>> defaults()
        {
            return {{"recordable_id", Value{Str{}}}};
        }

        static void start(Scalar<"recordable_id", Str> recordable_id, TraitsView traits,
                          GlobalStateView gs, State<FrameRecorderState> state)
        {
            using record_replay_frame_detail::RecorderHandle;
            const auto config = record_replay::config(gs);
            // Bitemporal plus one boolean: described directly rather than
            // through a converter, so there is one recorder in the tree.
            const std::vector<std::string> names{config.date_key, config.as_of_key, "value"};
            const std::vector<const ValueTypeMetaData *> metas{
                scalar_descriptor<DateTime>::value_meta(), scalar_descriptor<DateTime>::value_meta(),
                scalar_descriptor<Bool>::value_meta()};
            auto handle = std::make_unique<RecorderHandle>(RecorderHandle{
                TableRecorder{names, metas},
                record_replay::fq_recordable_id(traits, recordable_id.value()) + ".__compare__",
                {},
                false});
            state.set(FrameRecorderState{handle.release()});   // owned by node State until stop
        }

        static void eval(In<"lhs", TsVar<"S">, InputValidity::Unchecked> lhs,
                         In<"rhs", TsVar<"S">, InputValidity::Unchecked> rhs,
                         Scalar<"recordable_id", Str> recordable_id, State<FrameRecorderState> state,
                         GlobalStateView gs, DateTime now)
        {
            static_cast<void>(recordable_id);
            // Activation means at least one side ticked: a one-sided value IS
            // a mismatch (one series produced where the other did not).
            const bool equal = lhs.valid() && rhs.valid() && lhs.value().equals(rhs.value());
            const auto as_of = record_replay::config(gs).as_of.value_or(now);
            auto       &recorder = state.get().handle->recorder;
            const Value when{now};
            const Value as_of_value{as_of};
            const Value result{Bool{equal}};
            recorder.append_cell(0, when.view());
            recorder.append_cell(1, as_of_value.view());
            recorder.append_cell(2, result.view());
            recorder.end_row();
        }

        static void stop(State<FrameRecorderState> state, GlobalStateView gs)
        {
            std::unique_ptr<record_replay_frame_detail::RecorderHandle> handle{state.get().handle};
            state.set(FrameRecorderState{});
            if (handle == nullptr) { return; }
            record_replay::store_write(gs, handle->fq_key, handle->recorder.finish());
        }
    };

    /**
     * ``replay_const`` — const-evaluable (the const_fn ruling, P1). The
     * eager kernel reads the last recorded value from the frame store
     * (``recordable_id`` MUST be explicit — no graph traits exist outside a
     * graph, matching Python's eager-call contract); the wired form emits
     * the recovered value once at start, resolving the id through graph
     * traits like every other record/replay node.
     */
    struct replay_const_impl
    {
        static constexpr auto name              = "replay_const";
        static constexpr bool schedule_on_start = true;

        static std::vector<std::pair<std::string_view, Value>> defaults()
        {
            return {{"recordable_id", Value{Str{}}}, {"tm", Value{MAX_DT}}};
        }

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return record_replay::model_is(context.global_state, record_replay::DATA_FRAME);
        }

        static Value const_eval(const TSValueTypeMetaData *resolved_output, OperatorCallContext context)
        {
            const auto *key           = context.scalar_as<Str>("key");
            const auto *recordable_id = context.scalar_as<Str>("recordable_id");
            const auto *tm            = context.scalar_as<DateTime>("tm");
            if (recordable_id == nullptr || recordable_id->empty())
            {
                throw std::invalid_argument(
                    "replay_const: an explicit recordable_id is required for the eager (const) call");
            }
            const auto config = record_replay::config(context.global_state);
            return record_replay::replay_const_value(
                context.global_state,
                *recordable_id + "." + (key != nullptr ? *key : Str{}), resolved_output->value_schema,
                tm != nullptr ? *tm : MAX_DT,
                config.as_of.value_or(MAX_DT));
        }

        static void eval(Scalar<"key", Str> key, Scalar<"recordable_id", Str> recordable_id,
                         Scalar<"tm", DateTime> tm, TraitsView traits, GlobalStateView gs,
                         DateTime now, Out<TsVar<"O">> out)
        {
            const auto &erased = static_cast<const TSOutputView &>(out);
            const auto  cutoff = tm.value() == MAX_DT ? now : tm.value();
            Value       value  = record_replay::replay_const_value(
                gs,
                record_replay::fq_recordable_id(traits, recordable_id.value()) + "." + key.value(),
                erased.schema()->value_schema, cutoff,
                record_replay::config(gs).as_of.value_or(MAX_DT));
            if (value.has_value()) { out.apply(value.view()); }
        }
    };

    /** Register the data-frame record/replay backend overloads. */
    void register_record_replay_frame_operators();
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_RECORD_REPLAY_FRAME_IMPL_H
