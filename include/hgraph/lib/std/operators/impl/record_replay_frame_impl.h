#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_RECORD_REPLAY_FRAME_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_RECORD_REPLAY_FRAME_IMPL_H

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
            const ValueView &removed_names, const record_replay::Config &config)
        {
            using Options = table_ts_detail::TableRecordingOptions;
            Options options;
            options.partition_names = column_names(partition_names);
            options.removed_names   = column_names(removed_names);
            switch (as_of)
            {
            case RecordAsOf::Omit: options.as_of = Options::AsOf::Omit; break;
            case RecordAsOf::Track: options.as_of = Options::AsOf::Track; break;
            case RecordAsOf::Inherit: break;
            }
            if (options.as_of != Options::AsOf::Omit && config.as_of.has_value())
            {
                options.as_of       = Options::AsOf::Fixed;
                options.as_of_value = config.as_of;
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
            /** Set when the recording is partitioned: a row is then a key and a
                value rather than a whole value, so it cannot be read straight
                through the converter. */
            const table_ts_detail::TsTableLayout   *layout{nullptr};
            Frame                                   frame{};
            std::int64_t                            row{0};
        };

        /** Apply one recorded row, descending the TSD levels it names.
            The inverse of the emission walk: read this level's key, step into
            it, and recurse; at the leaf read the value columns by name. */
        inline void apply_recorded_row(const table_ts_detail::TsTableLayout &layout,
                                       const TableConverter &leaf_converter, const Frame &frame,
                                       std::int64_t row, std::size_t level_index,
                                       const TSOutputView &out, DateTime now)
        {
            if (level_index == layout.levels.size())
            {
                // By name, so the key and bitemporal columns are ignored.
                const Value value = read_row(leaf_converter, frame, row);
                apply_current_value(out, value.view());
                return;
            }

            const auto &level = layout.levels[level_index];
            if (level.key_paths.size() != 1)
            {
                throw std::runtime_error(
                    "replay: compound TSD keys are not yet reassembled from their flattened columns");
            }
            const auto key_column = frame.table->GetColumnByName(layout.keys[level.first_key_col]);
            if (key_column == nullptr)
            {
                throw std::runtime_error("replay: recording is missing key column '" +
                                         layout.keys[level.first_key_col] + "'");
            }
            const Value key = read_table_cell(level.key_meta, *key_column->chunk(0), *frame.table->schema(), row);

            auto dict_out = out.as_dict();
            auto mutation = dict_out.begin_mutation(now);

            // A removed column is only present when the recording tracked
            // removals; without it every row is a value.
            const auto removed_column = frame.table->GetColumnByName(layout.keys[level.removed_col]);
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
                               TSOutputView{out.output(), child, now}, now);
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
                    {"removed_names", record_replay_frame_detail::empty_names()}};
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
                          TraitsView traits, GlobalStateView gs, State<FrameRecorderState> state)
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
                as_of.value(), removes.value(), partition_names.value(), removed_names.value(), config);
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
                options.removes == TableRecordingOptions::Removes::Track});
            state.set(FrameRecorderState{handle.release()});   // owned by node State until stop
        }

        static void eval(In<"ts", TsVar<"S">> ts, Scalar<"key", Str> key,
                         Scalar<"recordable_id", Str> recordable_id, Scalar<"as_of", RecordAsOf> as_of,
                         Scalar<"removes", RecordRemoves> removes,
                         Scalar<"partition_names", ScalarVar<"PN", HomogeneousTuple<Str>>> partition_names,
                         Scalar<"removed_names", ScalarVar<"RN", HomogeneousTuple<Str>>>   removed_names,
                         State<FrameRecorderState> state, GlobalStateView gs, DateTime now)
        {
            static_cast<void>(key);
            static_cast<void>(recordable_id);
            // All four were resolved into the recorder's shape at start; the
            // row walk reads that shape from the handle, not from the arguments.
            static_cast<void>(as_of);
            static_cast<void>(removes);
            static_cast<void>(partition_names);
            static_cast<void>(removed_names);
            const auto as_of_cell = record_replay::config(gs).as_of.value_or(now);
            using namespace table_ts_detail;
            auto *handle = state.get().handle;
            const auto &layout =
                ts_table_layout(ts.base().schema(), record_replay::config(gs).date_key,
                                record_replay::config(gs).as_of_key);
            record_replay_frame_detail::RecordingSink recording{.handle = handle};
            emit_rows_to(layout, ts.base(), kToTableModeTick, now, as_of_cell, handle->emit_removals,
                         recording.sink());
        }

        static void stop(State<FrameRecorderState> state)
        {
            // Take ownership first so a throwing store write cannot leak.
            std::unique_ptr<record_replay_frame_detail::RecorderHandle> handle{state.get().handle};
            state.set(FrameRecorderState{});
            if (handle == nullptr) { return; }
            record_replay::store_write(handle->fq_key, handle->recorder.finish());
        }
    };

    struct replay_frame_impl
    {
        static constexpr auto name = "replay";

        static std::vector<std::pair<std::string_view, Value>> defaults()
        {
            return {{"recordable_id", Value{Str{}}}};
        }

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return record_replay::model_is(context.global_state, record_replay::DATA_FRAME);
        }

        static void start(Scalar<"key", Str> key, Scalar<"recordable_id", Str> recordable_id, TraitsView traits,
                          GlobalStateView gs, State<FrameReplayState> state,
                          SingleShotScheduler sched, Out<TsVar<"O">> out)
        {
            using record_replay_frame_detail::ReplayHandle;
            const auto  fq_key = record_replay_frame_detail::frame_key(traits, recordable_id.value(), key.value());
            Frame       frame  = record_replay::store_read(fq_key);
            if (!frame.has_value())
            {
                throw std::runtime_error("replay: no recorded frame under '" + fq_key + "'");
            }
            const auto &erased = static_cast<const TSOutputView &>(out);
            const auto  config = record_replay::config(gs);
            const auto &layout =
                table_ts_detail::ts_table_layout(erased.schema(), config.date_key, config.as_of_key);
            // The leaf's value schema for the value columns; the key columns
            // are read separately, since no value schema names them.
            const auto &converter = table_converter(
                layout.partitioned() ? layout.leaf_ts->value_schema : erased.schema()->value_schema,
                config.date_key, config.as_of_key);
            auto handle = std::make_unique<ReplayHandle>(
                ReplayHandle{&converter, layout.partitioned() ? &layout : nullptr, std::move(frame), 0});
            if (frame_rows(handle->frame) > 0)
            {
                sched.schedule(frame_value_time(converter, handle->frame, 0));
            }
            state.set(FrameReplayState{handle.release()});   // owned by node State until stop
        }

        static void eval(Scalar<"key", Str> key, Scalar<"recordable_id", Str> recordable_id,
                         State<FrameReplayState> state, NodeScheduler sched, DateTime now, Out<TsVar<"O">> out)
        {
            static_cast<void>(key);
            static_cast<void>(recordable_id);
            auto      *handle = state.get().handle;
            const auto rows   = frame_rows(handle->frame);
            while (handle->row < rows && frame_value_time(*handle->converter, handle->frame, handle->row) == now)
            {
                if (handle->layout != nullptr)
                {
                    record_replay_frame_detail::apply_recorded_row(
                        *handle->layout, *handle->converter, handle->frame, handle->row, 0,
                        static_cast<const TSOutputView &>(out), now);
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

        static void stop(State<FrameRecorderState> state)
        {
            std::unique_ptr<record_replay_frame_detail::RecorderHandle> handle{state.get().handle};
            state.set(FrameRecorderState{});
            if (handle == nullptr) { return; }
            record_replay::store_write(handle->fq_key, handle->recorder.finish());
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
