#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_RECORD_REPLAY_FRAME_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_RECORD_REPLAY_FRAME_IMPL_H

#include <hgraph/lib/std/operators/impl/data_frame_impl.h>
#include <hgraph/lib/std/operators/impl/table_impl.h>

#include <arrow/array.h>
#include <arrow/table.h>
#include <hgraph/lib/std/operators/io.h>
#include <hgraph/runtime/node_scheduler.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/record_replay.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/types/value/compact_storage.h>
#include <hgraph/types/value/table_codec.h>
#include <hgraph/types/value/value_builder.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace hgraph::stdlib
{
    namespace record_replay_frame_detail
    {
        [[nodiscard]] inline std::string frame_key(const TraitsView &traits,
                                                   std::string_view  recordable_id,
                                                   std::string_view  key)
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
            ListBuilder builder{binding};
            ListStorage storage = builder.build_storage();
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
                if (stored.empty() || stored == canonical)
                {
                    continue;
                }

                const int source = frame.table->schema()->GetFieldIndex(stored);
                if (source < 0)
                {
                    continue;
                }
                const int existing = frame.table->schema()->GetFieldIndex(canonical);
                if (existing >= 0 && existing != source)
                {
                    throw std::runtime_error("replay: column projection '" + stored + "' -> '" +
                                             canonical + "' collides with an existing column");
                }
                names[static_cast<std::size_t>(source)] = canonical;
            }

            // A frame prefix is a recording-side disambiguation aid, not
            // part of the value schema.  Legacy replay therefore did not
            // require callers to repeat it: frame payload columns were read
            // positionally through the frame converter.  Preserve that
            // contract while normalising the stored frame for the canonical
            // row-source path.  Recorder projections always append expanded
            // frame fields last and in schema order.
            if (layout.is_multi_row && !layout.value_cols.empty())
            {
                if (names.size() < layout.value_cols.size())
                {
                    throw std::runtime_error(
                        "replay: recording has fewer columns than the frame schema");
                }
                const std::size_t first_payload = names.size() - layout.value_cols.size();
                for (std::size_t i = 0; i < layout.value_cols.size(); ++i)
                {
                    const std::string &canonical = layout.keys[layout.value_col_start + i];
                    if (std::find(names.begin(), names.end(), canonical) == names.end())
                    {
                        names[first_payload + i] = canonical;
                    }
                }
            }
            for (std::size_t lhs = 0; lhs < names.size(); ++lhs)
            {
                for (std::size_t rhs = lhs + 1; rhs < names.size(); ++rhs)
                {
                    if (names[lhs] == names[rhs])
                    {
                        throw std::runtime_error("replay: projected column name is not unique: '" +
                                                 names[lhs] + "'");
                    }
                }
            }
            auto renamed = frame.table->RenameColumns(names);
            if (!renamed.ok())
            {
                throw std::runtime_error("replay: failed to normalize stored columns: " +
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
            const ValueView &removed_names, std::string_view date_key, std::string_view as_of_key,
            std::string_view frame_prefix, const record_replay::Config &config)
        {
            using Options = table_ts_detail::TableRecordingOptions;
            Options options;
            options.partition_names = column_names(partition_names);
            options.removed_names = column_names(removed_names);
            options.date_key = std::string{date_key};
            options.as_of_key = std::string{as_of_key};
            options.frame_prefix = std::string{frame_prefix};
            switch (as_of)
            {
            case RecordAsOf::Omit:
                options.as_of = Options::AsOf::Omit;
                break;
            case RecordAsOf::Track:
                options.as_of = Options::AsOf::Track;
                break;
            case RecordAsOf::Inherit:
                if (config.as_of.has_value())
                {
                    options.as_of = Options::AsOf::Fixed;
                    options.as_of_value = config.as_of;
                }
                break;
            }
            switch (removes)
            {
            case RecordRemoves::Omit:
                options.removes = Options::Removes::Omit;
                break;
            case RecordRemoves::Track:
                options.removes = Options::Removes::Track;
                break;
            case RecordRemoves::Inherit:
                break;
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
            /** Fixed wiring-time ToTableMode value (Tick/Sample/Snap). */
            ToTableMode mode{ToTableMode::Tick};
            /** Present only for an inherited fixed as-of. Explicit Track
                always follows evaluation time even when the graph default is fixed. */
            std::optional<DateTime> fixed_as_of{};
            /** Set before rejecting an unrepresentable tick, so stop does not
                persist a partial recording after evaluation fails. */
            bool discard{false};
            /** Native-only immutable segment policy. Zero disables each trigger. */
            Int         flush_rows{0};
            TimeDelta   flush_interval{};
            DateTime    last_flush{};
            std::size_t next_segment{0};
            std::size_t total_rows{0};
            bool        segmented{false};

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
                if (column >= self->handle->output_of_layout_column.size())
                {
                    return;
                }
                const std::size_t output = self->handle->output_of_layout_column[column];
                if (output == RecorderHandle::dropped)
                {
                    return;
                }
                self->handle->recorder.append_cell(output, value);
            }

            static void end_row(void *context)
            {
                static_cast<RecordingSink *>(context)->handle->recorder.end_row();
            }

            [[nodiscard]] table_ts_detail::RowSink sink()
            {
                return table_ts_detail::RowSink{
                    .context = this, .cell = &cell, .end_row = &end_row};
            }
        };

        /** Heap replay cursor owned across start/eval/stop via node State. */
        struct ReplayHandle
        {
            /** Always set: projection/filter description for every segment. */
            const table_ts_detail::TsTableLayout *recording_layout{nullptr};
            Frame                                 frame{};
            std::int64_t                          row{0};
            /** Projection supplied by the caller, retained for every segment. */
            std::vector<std::string> source_names{};
            std::string              fq_key{};
            std::size_t              next_segment{0};
            DateTime                 as_of{MAX_DT};
            DateTime                 start_time{MIN_ST};
            bool                     segmented{false};
        };

        struct RecordedFrameRows
        {
            const table_ts_detail::TsTableLayout *layout{nullptr};
            const Frame                          *frame{nullptr};
            std::int64_t                          first{0};
            std::int64_t                          count{0};

            static Value cell(const void *context, std::size_t row, std::size_t column)
            {
                const auto &self = *static_cast<const RecordedFrameRows *>(context);
                if (column >= self.layout->keys.size())
                {
                    throw std::out_of_range("replay: table column index is out of range");
                }
                const std::string &name = self.layout->keys[column];
                if (self.frame->table->GetColumnByName(name) == nullptr)
                {
                    return {};
                }
                return frame_cell(*self.frame, name, self.layout->col_metas[column],
                                  self.first + static_cast<std::int64_t>(row));
            }

            [[nodiscard]] TableRowSource source() const
            {
                return TableRowSource{
                    .context = this, .rows = static_cast<std::size_t>(count), .cell = &cell};
            }
        };

        [[nodiscard]] inline DateTime recorded_value_time(
            const table_ts_detail::TsTableLayout &layout, const Frame &frame, std::int64_t row)
        {
            return frame_cell(frame, layout.date_key, scalar_descriptor<DateTime>::value_meta(),
                              row)
                .view()
                .checked_as<DateTime>();
        }

        inline void flush_segment(RecorderHandle &handle, GlobalStateView gs)
        {
            const std::int64_t rows = handle.recorder.rows();
            if (rows == 0)
            {
                return;
            }
            record_replay::store_write(
                gs, record_replay::segment_key(handle.fq_key, handle.next_segment),
                handle.recorder.finish());
            ++handle.next_segment;
            handle.total_rows += static_cast<std::size_t>(rows);
        }

        [[nodiscard]] inline Frame prepare_replay_segment(const ReplayHandle &handle, Frame frame)
        {
            frame = canonical_replay_frame(std::move(frame), *handle.recording_layout,
                                           handle.source_names);
            return data_frame_detail::select_replay_frame(frame, *handle.recording_layout,
                                                          handle.as_of, handle.start_time);
        }

        [[nodiscard]] inline bool load_next_replay_segment(ReplayHandle &handle, GlobalStateView gs)
        {
            while (true)
            {
                Frame next = record_replay::store_read(
                    gs, record_replay::segment_key(handle.fq_key, handle.next_segment));
                if (!next.has_value())
                {
                    handle.frame = Frame{};
                    handle.row = 0;
                    return false;
                }
                ++handle.next_segment;
                handle.frame = prepare_replay_segment(handle, std::move(next));
                handle.row = 0;
                if (frame_rows(handle.frame) != 0)
                {
                    return true;
                }
            }
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
                    {"date_key", Value{Str{}}},
                    {"as_of_key", Value{Str{}}},
                    {"frame_prefix", Value{Str{}}},
                    {"mode", Value{ToTableMode::Tick}},
                    {"flush_rows", Value{Int{0}}},
                    {"flush_interval", Value{TimeDelta{0}}}};
        }

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return record_replay::model_is(context.global_state, record_replay::DATA_FRAME);
        }

        static void start(
            In<"ts", TsVar<"S">> ts, Scalar<"key", Str> key,
            Scalar<"recordable_id", Str> recordable_id, Scalar<"as_of", RecordAsOf> as_of,
            Scalar<"removes", RecordRemoves>                                  removes,
            Scalar<"partition_names", ScalarVar<"PN", HomogeneousTuple<Str>>> partition_names,
            Scalar<"removed_names", ScalarVar<"RN", HomogeneousTuple<Str>>>   removed_names,
            Scalar<"date_key", Str> date_key, Scalar<"as_of_key", Str> as_of_key,
            Scalar<"frame_prefix", Str> frame_prefix, Scalar<"mode", ToTableMode> mode,
            Scalar<"flush_rows", Int>           flush_rows,
            Scalar<"flush_interval", TimeDelta> flush_interval, TraitsView traits,
            GlobalStateView gs, DateTime now, State<FrameRecorderState> state)
        {
            using record_replay_frame_detail::RecorderHandle;
            using namespace table_ts_detail;

            const auto config = record_replay::config(gs);
            // The LAYOUT, not the value schema: a value schema cannot describe
            // a TSD's key columns or removed flags, which is why recording a
            // partitioned series used to fail here (RFC 0019).
            const auto &layout =
                ts_table_layout(ts.base().schema(), config.date_key, config.as_of_key);
            const TableRecordingOptions options = record_replay_frame_detail::recording_options(
                as_of.value(), removes.value(), partition_names.value(), removed_names.value(),
                date_key.value(), as_of_key.value(), frame_prefix.value(), config);
            const RecordingColumns columns = recording_columns(layout, options);
            if (flush_rows.value() < 0 || flush_interval.value() < TimeDelta{0})
            {
                throw std::invalid_argument("record: flush thresholds must not be negative");
            }
            const ToTableMode mode_value = mode.value();
            if (mode_value < ToTableMode::Tick || mode_value > ToTableMode::Snap)
            {
                throw std::invalid_argument("record: mode must be Tick, Sample, or Snap");
            }

            std::vector<std::size_t> output_of_layout_column(layout.keys.size(),
                                                             RecorderHandle::dropped);
            for (std::size_t output = 0; output < columns.source.size(); ++output)
            {
                output_of_layout_column[columns.source[output]] = output;
            }

            auto handle = std::make_unique<RecorderHandle>(RecorderHandle{
                TableRecorder{columns.names, columns.metas},
                record_replay_frame_detail::frame_key(traits, recordable_id.value(), key.value()),
                std::move(output_of_layout_column),
                options.removes == TableRecordingOptions::Removes::Track, mode_value,
                options.as_of == TableRecordingOptions::AsOf::Fixed ? options.as_of_value
                                                                    : std::optional<DateTime>{}});
            handle->flush_rows = flush_rows.value();
            handle->flush_interval = flush_interval.value();
            handle->last_flush = now;

            auto selected_store = record_replay::frame_store(gs);
            if (!selected_store)
            {
                selected_store = record_replay::frame_store();
            }
            const bool native_segments = selected_store.supports_segmented_recordings();
            const bool flush_requested =
                handle->flush_rows > 0 || handle->flush_interval > TimeDelta{0};
            if (native_segments &&
                (record_replay::store_contains(gs, handle->fq_key) ||
                 record_replay::store_contains(gs, record_replay::segment_key(handle->fq_key, 0)) ||
                 (flush_requested && record_replay::store_contains(
                                         gs, record_replay::completion_key(handle->fq_key)))))
            {
                throw std::runtime_error("recording key already exists: " + handle->fq_key);
            }
            handle->segmented = flush_requested && native_segments;
            if (handle->segmented)
            {
                // Claim the logical key before publishing the first immutable
                // segment. A Python compatibility store never reaches here.
                record_replay::store_write(gs, handle->fq_key,
                                           record_replay::segmented_recording_marker());
            }
            state.set(FrameRecorderState{handle.release()});  // owned by node State until stop
        }

        static void eval(
            In<"ts", TsVar<"S">> ts, Scalar<"key", Str> key,
            Scalar<"recordable_id", Str> recordable_id, Scalar<"as_of", RecordAsOf> as_of,
            Scalar<"removes", RecordRemoves>                                  removes,
            Scalar<"partition_names", ScalarVar<"PN", HomogeneousTuple<Str>>> partition_names,
            Scalar<"removed_names", ScalarVar<"RN", HomogeneousTuple<Str>>>   removed_names,
            Scalar<"date_key", Str> date_key, Scalar<"as_of_key", Str> as_of_key,
            Scalar<"frame_prefix", Str> frame_prefix, Scalar<"mode", ToTableMode> mode,
            Scalar<"flush_rows", Int>           flush_rows,
            Scalar<"flush_interval", TimeDelta> flush_interval, State<FrameRecorderState> state,
            GlobalStateView gs, DateTime now)
        {
            static_cast<void>(key);
            static_cast<void>(recordable_id);
            // Wiring-time shape, mode, and flush choices were resolved at
            // start; the row walk reads them from the handle.
            static_cast<void>(as_of);
            static_cast<void>(removes);
            static_cast<void>(partition_names);
            static_cast<void>(removed_names);
            static_cast<void>(date_key);
            static_cast<void>(as_of_key);
            static_cast<void>(frame_prefix);
            static_cast<void>(mode);
            static_cast<void>(flush_rows);
            static_cast<void>(flush_interval);
            const auto as_of_cell = state.get().handle->fixed_as_of.value_or(now);
            using namespace table_ts_detail;
            auto       *handle = state.get().handle;
            const auto &layout =
                ts_table_layout(ts.base().schema(), record_replay::config(gs).date_key,
                                record_replay::config(gs).as_of_key);
            record_replay_frame_detail::RecordingSink recording{.handle = handle};
            try
            {
                emit_rows_to(layout, ts.base(), static_cast<Int>(handle->mode), now, as_of_cell,
                             handle->emit_removals, recording.sink(),
                             /*reject_empty_frames=*/true);
            }
            catch (...)
            {
                // A failed tick must not leave a partially appended frame that
                // stop() could publish while unwinding the graph.
                handle->discard = true;
                throw;
            }
            const bool row_limit =
                handle->flush_rows > 0 && handle->recorder.rows() >= handle->flush_rows;
            const bool time_limit = handle->flush_interval > TimeDelta{0} &&
                                    now - handle->last_flush >= handle->flush_interval;
            if (handle->segmented && (row_limit || time_limit))
            {
                record_replay_frame_detail::flush_segment(*handle, gs);
                handle->last_flush = now;
            }
        }

        static void stop(State<FrameRecorderState> state, GlobalStateView gs)
        {
            // Take ownership first so a throwing store write cannot leak.
            std::unique_ptr<record_replay_frame_detail::RecorderHandle> handle{state.get().handle};
            state.set(FrameRecorderState{});
            if (handle == nullptr || handle->discard)
            {
                return;
            }
            if (handle->segmented)
            {
                record_replay_frame_detail::flush_segment(*handle, gs);
                record_replay::store_write(gs, record_replay::completion_key(handle->fq_key),
                                           record_replay::segmented_recording_manifest(
                                               handle->next_segment, handle->total_rows));
            }
            else
            {
                record_replay::store_write(gs, handle->fq_key, handle->recorder.finish());
            }
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
                    {"date_key", Value{Str{}}},
                    {"as_of_key", Value{Str{}}},
                    {"frame_prefix", Value{Str{}}}};
        }

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return record_replay::model_is(context.global_state, record_replay::DATA_FRAME);
        }

        static void start(
            Scalar<"key", Str> key, Scalar<"recordable_id", Str> recordable_id,
            Scalar<"partition_names", ScalarVar<"PN", HomogeneousTuple<Str>>> partition_names,
            Scalar<"removed_names", ScalarVar<"RN", HomogeneousTuple<Str>>>   removed_names,
            Scalar<"date_key", Str> date_key, Scalar<"as_of_key", Str> as_of_key,
            Scalar<"frame_prefix", Str> frame_prefix, TraitsView traits, GlobalStateView gs,
            DateTime now, State<FrameReplayState> state, SingleShotScheduler sched,
            Out<TsVar<"O">> out)
        {
            using record_replay_frame_detail::ReplayHandle;
            const auto fq_key =
                record_replay_frame_detail::frame_key(traits, recordable_id.value(), key.value());
            Frame stored = record_replay::store_read(gs, fq_key);
            if (!stored.has_value())
            {
                throw std::runtime_error("replay: no recorded frame under '" + fq_key + "'");
            }
            const bool  segmented = record_replay::is_segmented_recording(stored);
            const auto &erased = static_cast<const TSOutputView &>(out);
            const auto  config = record_replay::config(gs);
            const auto &layout = table_ts_detail::ts_table_layout(erased.schema(), config.date_key,
                                                                  config.as_of_key);
            table_ts_detail::TableRecordingOptions replay_options;
            replay_options.removes = table_ts_detail::TableRecordingOptions::Removes::Track;
            replay_options.partition_names =
                record_replay_frame_detail::column_names(partition_names.value());
            replay_options.removed_names =
                record_replay_frame_detail::column_names(removed_names.value());
            replay_options.date_key = date_key.value();
            replay_options.as_of_key = as_of_key.value();
            replay_options.frame_prefix = frame_prefix.value();
            const auto recorded_columns =
                table_ts_detail::recording_columns(layout, replay_options);
            std::vector<std::string> stored_names(layout.keys.size());
            for (std::size_t i = 0; i < recorded_columns.source.size(); ++i)
            {
                stored_names[recorded_columns.source[i]] = recorded_columns.names[i];
            }
            auto handle = std::make_unique<ReplayHandle>();
            handle->recording_layout = &layout;
            handle->source_names = std::move(stored_names);
            handle->fq_key = fq_key;
            handle->as_of = config.as_of.value_or(MAX_DT);
            handle->start_time = now;
            handle->segmented = segmented;
            if (segmented)
            {
                // The marker claims the logical key. Segments are complete,
                // standalone frames and an absent next key is the natural end
                // of an interrupted or still-running recording.
                handle->next_segment = 0;
                static_cast<void>(
                    record_replay_frame_detail::load_next_replay_segment(*handle, gs));
            }
            else
            {
                handle->frame =
                    record_replay_frame_detail::prepare_replay_segment(*handle, std::move(stored));
            }
            if (frame_rows(handle->frame) > 0)
            {
                sched.schedule(
                    record_replay_frame_detail::recorded_value_time(layout, handle->frame, 0));
            }
            state.set(FrameReplayState{handle.release()});  // owned by node State until stop
        }

        static void eval(
            Scalar<"key", Str> key, Scalar<"recordable_id", Str> recordable_id,
            Scalar<"partition_names", ScalarVar<"PN", HomogeneousTuple<Str>>> partition_names,
            Scalar<"removed_names", ScalarVar<"RN", HomogeneousTuple<Str>>>   removed_names,
            Scalar<"date_key", Str> date_key, Scalar<"as_of_key", Str> as_of_key,
            Scalar<"frame_prefix", Str> frame_prefix, State<FrameReplayState> state,
            NodeScheduler sched, GlobalStateView gs, DateTime now, Out<TsVar<"O">> out)
        {
            static_cast<void>(key);
            static_cast<void>(recordable_id);
            static_cast<void>(partition_names);
            static_cast<void>(removed_names);
            static_cast<void>(date_key);
            static_cast<void>(as_of_key);
            static_cast<void>(frame_prefix);
            auto              *handle = state.get().handle;
            const auto         rows = frame_rows(handle->frame);
            const std::int64_t first = handle->row;
            while (handle->row < rows &&
                   record_replay_frame_detail::recorded_value_time(
                       *handle->recording_layout, handle->frame, handle->row) == now)
            {
                ++handle->row;
            }
            if (handle->row > first)
            {
                const record_replay_frame_detail::RecordedFrameRows recorded{
                    .layout = handle->recording_layout,
                    .frame = &handle->frame,
                    .first = first,
                    .count = handle->row - first};
                const TableRowSource source = recorded.source();
                handle->recording_layout->ops->apply(*handle->recording_layout, source,
                                                     static_cast<const TSOutputView &>(out));
            }
            if (handle->row >= rows && handle->segmented)
            {
                static_cast<void>(
                    record_replay_frame_detail::load_next_replay_segment(*handle, gs));
            }
            if (handle->frame.has_value() && handle->row < frame_rows(handle->frame))
            {
                sched.schedule(record_replay_frame_detail::recorded_value_time(
                    *handle->recording_layout, handle->frame, handle->row));
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
            return !record_replay::model_is(context.global_state, record_replay::IN_MEMORY);
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
                scalar_descriptor<DateTime>::value_meta(),
                scalar_descriptor<DateTime>::value_meta(), scalar_descriptor<Bool>::value_meta()};
            auto handle = std::make_unique<RecorderHandle>(RecorderHandle{
                TableRecorder{names, metas},
                record_replay::fq_recordable_id(traits, recordable_id.value()) + ".__compare__",
                {},
                false,
                ToTableMode::Tick});
            state.set(FrameRecorderState{handle.release()});  // owned by node State until stop
        }

        static void eval(In<"lhs", TsVar<"S">, InputValidity::Unchecked> lhs,
                         In<"rhs", TsVar<"S">, InputValidity::Unchecked> rhs,
                         Scalar<"recordable_id", Str>                    recordable_id,
                         State<FrameRecorderState> state, GlobalStateView gs, DateTime now)
        {
            static_cast<void>(recordable_id);
            // Activation means at least one side ticked: a one-sided value IS
            // a mismatch (one series produced where the other did not).
            const bool  equal = lhs.valid() && rhs.valid() && lhs.value().equals(rhs.value());
            const auto  as_of = record_replay::config(gs).as_of.value_or(now);
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
            if (handle == nullptr)
            {
                return;
            }
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
        static constexpr auto name = "replay_const";
        static constexpr bool schedule_on_start = true;

        static std::vector<std::pair<std::string_view, Value>> defaults()
        {
            return {{"recordable_id", Value{Str{}}}, {"tm", Value{MAX_DT}}};
        }

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return record_replay::model_is(context.global_state, record_replay::DATA_FRAME);
        }

        static Value const_eval(const TSValueTypeMetaData *resolved_output,
                                OperatorCallContext        context)
        {
            const auto *key = context.scalar_as<Str>("key");
            const auto *recordable_id = context.scalar_as<Str>("recordable_id");
            const auto *tm = context.scalar_as<DateTime>("tm");
            if (recordable_id == nullptr || recordable_id->empty())
            {
                throw std::invalid_argument("replay_const: an explicit recordable_id is "
                                            "required for the eager (const) call");
            }
            const auto config = record_replay::config(context.global_state);
            return record_replay::replay_const_value(
                context.global_state, *recordable_id + "." + (key != nullptr ? *key : Str{}),
                resolved_output->value_schema, tm != nullptr ? *tm : MAX_DT,
                config.as_of.value_or(MAX_DT));
        }

        static void eval(Scalar<"key", Str> key, Scalar<"recordable_id", Str> recordable_id,
                         Scalar<"tm", DateTime> tm, TraitsView traits, GlobalStateView gs,
                         DateTime now, Out<TsVar<"O">> out)
        {
            const auto &erased = static_cast<const TSOutputView &>(out);
            const auto  cutoff = tm.value() == MAX_DT ? now : tm.value();
            Value       value = record_replay::replay_const_value(
                gs,
                record_replay::fq_recordable_id(traits, recordable_id.value()) + "." + key.value(),
                erased.schema()->value_schema, cutoff,
                record_replay::config(gs).as_of.value_or(MAX_DT));
            if (value.has_value())
            {
                out.apply(value.view());
            }
        }
    };

    /** Register the data-frame record/replay backend overloads. */
    void register_record_replay_frame_operators();
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_RECORD_REPLAY_FRAME_IMPL_H
