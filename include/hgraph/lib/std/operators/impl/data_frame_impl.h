#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_DATA_FRAME_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_DATA_FRAME_IMPL_H

#include <hgraph/lib/std/operators/data_frame.h>
#include <hgraph/lib/std/operators/table_rows.h>
#include <hgraph/runtime/node_scheduler.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/operator_type_resolution.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/types/value/table_codec.h>

#include <string>
#include <string_view>
#include <tuple>
#include <vector>

namespace hgraph::stdlib
{
    using namespace hgraph::operator_type_resolution;

    /**
     * The data-frame convenience operators (design record: *Record/replay,
     * tables and const_fn*, step 6): ``from_data_frame`` replays a frame
     * VALUE by its date column, ``to_data_frame`` snapshots a time-series
     * into one-tick frames (a plain ``date`` column, no as-of), ``group_by``
     * partitions a Frame-valued TS into a TSD by key column(s). All ride
     * the tuple-row/frame codecs — no third serialisation path.
     */
    namespace data_frame_detail
    {
        struct FieldRead
        {
            std::string              column{};
            const ValueTypeMetaData *leaf{nullptr};
            std::size_t              field_index{0};
        };

        struct FromFramePlan;
        using FromFrameRowApplication = void (*)(const FromFramePlan &plan,
                                                  std::int64_t row,
                                                  const TSOutputView &out);
        void missing_from_frame_row_application(const FromFramePlan &plan,
                                                std::int64_t row,
                                                const TSOutputView &out);

        /** from_data_frame reading plan (heap handle via node State). */
        struct FromFramePlan
        {
            Frame                     frame{};
            std::string               dt_col{};
            TimeDelta                 offset{};
            std::int64_t              row{0};
            FromFrameRowApplication   apply_row{&missing_from_frame_row_application};
            const ValueTypeMetaData  *key_meta{nullptr};      // TSD key
            std::string               key_col{};
            const ValueTypeMetaData  *bundle_meta{nullptr};   // TSB(-child) value bundle
            std::vector<FieldRead>    fields{};               // value cell reads
        };

        struct ReplayFrameTick
        {
            DateTime when{};
            Value    rows{};
        };

        /** Raw bitemporal replay plan, selected once during source start. */
        struct ReplayDataFramePlan
        {
            const table_ts_detail::TsTableLayout *layout{nullptr};
            std::vector<ReplayFrameTick>           ticks{};
            std::size_t                            tick{0};
        };

        /** to_data_frame writing plan. */
        struct ToFramePlan
        {
            enum class Source : unsigned char { Date, Key, Field, ValueField, Whole };

            struct Column
            {
                Source      source{Source::Whole};
                std::size_t ts_field{0};   // TSB child index for Source::Field
            };

            const TableConverter     *converter{nullptr};   // over the OUT frame column schema
            const ValueTypeMetaData  *row_meta{nullptr};    // that column bundle
            std::vector<Column>       columns{};
            bool                      dict{false};          // input is a TSD
        };

        /** group_by partitioning plan. */
        struct GroupByPlan
        {
            const TableConverter                *converter{nullptr};   // over the frame's column schema
            std::vector<FieldRead>               key_cols{};
            const ValueTypeMetaData             *key_meta{nullptr};
            bool                                 tuple_key{false};
        };

        [[nodiscard]] const TSValueTypeMetaData *resolve_group_by_output(const TSValueTypeMetaData *ts,
                                                                         const ValueView          &by);

        /** The default snapshot-frame schema for ``ts``: an UN-NAMED column
            bundle [dt_col, (key_col,) *value columns] (upstream's
            compound_scalar resolvers). */
        [[nodiscard]] const TSValueTypeMetaData *resolve_to_frame_output(const TSValueTypeMetaData *ts,
                                                                         std::string_view dt_col,
                                                                         std::string_view key_col,
                                                                         std::string_view value_col);

        void start_from_frame(const Frame &frame, std::string_view dt_col, std::string_view key_col,
                              std::string_view value_col, TimeDelta offset,
                              DateTime start_time, const TSOutputView &out,
                              SingleShotScheduler &sched, FromFramePlan *&plan_out);
        void load_from_frame(const Frame &frame, std::string_view dt_col,
                             std::string_view key_col, std::string_view value_col,
                             TimeDelta offset, DateTime start_time,
                             const TSOutputView &out, FromFramePlan *&plan_out);
        void eval_from_frame(FromFramePlan &plan, DateTime now, NodeScheduler &sched,
                             const TSOutputView &out);

        void start_replay_data_frame(const Frame &frame, DateTime as_of_time,
                                     DateTime start_time, GlobalStateView gs,
                                     const TSOutputView &out,
                                     SingleShotScheduler &sched,
                                     ReplayDataFramePlan *&plan_out);
        void eval_replay_data_frame(ReplayDataFramePlan &plan, DateTime now,
                                    NodeScheduler &sched, const TSOutputView &out);

        void start_to_frame(const TSInputView &ts, std::string_view dt_col, std::string_view key_col,
                            std::string_view value_col, const TSOutputView &out, ToFramePlan *&plan_out);
        void eval_to_frame(const ToFramePlan &plan, const TSInputView &ts, DateTime now,
                           const TSOutputView &out);

        void start_group_by(const TSInputView &ts, const ValueView &by, const TSOutputView &out,
                            GroupByPlan *&plan_out);
        void eval_group_by(const GroupByPlan &plan, const TSInputView &ts, const TSOutputView &out);

        [[nodiscard]] Frame sort_frame(const Frame &frame, std::string_view by, bool descending);
        [[nodiscard]] Frame concat_frames(const Frame &lhs, const Frame &rhs);
        [[nodiscard]] const ValueTypeMetaData *resolve_join_row(
            const TSValueTypeMetaData *lhs, const TSValueTypeMetaData *rhs,
            const ValueView &on, std::string_view how, std::string_view suffix);
        [[nodiscard]] Frame join_frames(const Frame &lhs, const Frame &rhs,
                                        const ValueView &on, std::string_view how,
                                        std::string_view suffix);
        [[nodiscard]] Frame filter_frame_by_bundle(const Frame &frame,
                                                   TSInputView &predicate,
                                                   const ValueTypeMetaData *row);
        [[nodiscard]] Frame filter_frame_by_value(const Frame &frame,
                                                  const ValueView &predicate,
                                                  const ValueTypeMetaData *row);
        [[nodiscard]] const ValueTypeMetaData *resolve_ungroup_row(
            const TSValueTypeMetaData *ts, const ValueView *key_col);
        [[nodiscard]] Frame ungroup_frames(TSInputView &ts, const ValueView *key_col,
                                           const ValueTypeMetaData *output_row);
        [[nodiscard]] Frame ungroup_items(TSInputView &ts,
                                          const ValueTypeMetaData *output_row);
        [[nodiscard]] Frame replace_frame_columns(const Frame &frame, TSInputView &columns,
                                                  const ValueTypeMetaData *output_row);

        // Frame targets for convert/combine (design record step 6).
        [[nodiscard]] bool value_is_frame(const ValueTypeMetaData *meta);
        [[nodiscard]] bool ts_value_is_frame(const TSValueTypeMetaData *ts);
        [[nodiscard]] std::string mapping_entry(const ValueView &mapping, std::string_view key);

        void start_convert_tsd_frame(const TSInputView &ts, const ValueView &mapping,
                                     const TSOutputView &out, ToFramePlan *&plan_out);
        void start_convert_value_frame(const TSInputView &ts, const TSOutputView &out,
                                       ToFramePlan *&plan_out);
        void eval_convert_value_frame(const ToFramePlan &plan, const TSInputView &ts,
                                      const TSOutputView &out);
        void eval_convert_frame_frame(const ValueView &mapping, const TSInputView &ts,
                                      const TSOutputView &out);
        void start_combine_frame(const TSInputView &ts, const TSOutputView &out, ToFramePlan *&plan_out);
        void eval_combine_frame(const ToFramePlan &plan, const TSInputView &ts, const TSOutputView &out);
    }  // namespace data_frame_detail

    struct FromDataFrameState
    {
        data_frame_detail::FromFramePlan *handle{nullptr};
    };

    struct ToDataFrameState
    {
        data_frame_detail::ToFramePlan *handle{nullptr};
    };

    struct FromDataFrameBatchesState
    {
        data_frame_detail::FromFramePlan *handle{nullptr};
    };

    struct ReplayDataFrameState
    {
        data_frame_detail::ReplayDataFramePlan *handle{nullptr};
    };

    struct GroupByState
    {
        data_frame_detail::GroupByPlan *handle{nullptr};
    };
}  // namespace hgraph::stdlib

namespace hgraph::static_schema_detail
{
    template <>
    struct scalar_name<stdlib::FromDataFrameState>
    {
        static constexpr std::string_view value{"FromDataFrameState"};
    };

    template <>
    struct scalar_name<stdlib::ToDataFrameState>
    {
        static constexpr std::string_view value{"ToDataFrameState"};
    };


    template <>
    struct scalar_name<stdlib::ReplayDataFrameState>
    {
        static constexpr std::string_view value{"ReplayDataFrameState"};
    };

    template <>
    struct scalar_name<stdlib::FromDataFrameBatchesState>
    {
        static constexpr std::string_view value{"FromDataFrameBatchesState"};
    };

    template <>
    struct scalar_name<stdlib::GroupByState>
    {
        static constexpr std::string_view value{"GroupByState"};
    };
}  // namespace hgraph::static_schema_detail

namespace hgraph::stdlib
{
    /** ``from_data_frame[OUT](df, ...)`` — a pull source replaying the frame
        by its date column (absolute scheduling, the replay precedent). */
    struct from_data_frame_impl
    {
        static constexpr auto name = "from_data_frame";

        static auto defaults()
        {
            return std::tuple{arg<"dt_col">(Str{"date"}),
                              arg<"key_col">(Str{"key"}),
                              arg<"value_col">(Str{"value"}),
                              arg<"offset">(TimeDelta{0})};
        }

        static void start(Scalar<"df", Frame> df, Scalar<"dt_col", Str> dt_col,
                          Scalar<"key_col", Str> key_col, Scalar<"value_col", Str> value_col,
                          Scalar<"offset", TimeDelta> offset, DateTime now,
                          SingleShotScheduler sched,
                          State<FromDataFrameState> state, Out<TsVar<"O">> out)
        {
            data_frame_detail::FromFramePlan *plan = nullptr;
            data_frame_detail::start_from_frame(df.value(), dt_col.value(), key_col.value(),
                                                value_col.value(), offset.value(),
                                                now,
                                                static_cast<const TSOutputView &>(out), sched, plan);
            state.set(FromDataFrameState{plan});   // owned by node State until stop
        }

        static void eval(Scalar<"df", Frame> df, Scalar<"dt_col", Str> dt_col,
                         Scalar<"key_col", Str> key_col, Scalar<"value_col", Str> value_col,
                         Scalar<"offset", TimeDelta> offset, State<FromDataFrameState> state,
                         NodeScheduler sched, DateTime now, Out<TsVar<"O">> out)
        {
            static_cast<void>(df);
            static_cast<void>(dt_col);
            static_cast<void>(key_col);
            static_cast<void>(value_col);
            static_cast<void>(offset);
            data_frame_detail::eval_from_frame(*state.get().handle, now, sched,
                                               static_cast<const TSOutputView &>(out));
        }

        static void stop(State<FromDataFrameState> state)
        {
            std::unique_ptr<data_frame_detail::FromFramePlan> handle{state.get().handle};
            state.set(FromDataFrameState{});
        }
    };

    struct from_data_frame_batches_impl
    {
        static constexpr auto name = "from_data_frame_batches";

        static auto defaults()
        {
            return std::tuple{arg<"dt_col">(Str{"date"}),
                              arg<"key_col">(Str{"key"}),
                              arg<"value_col">(Str{"value"}),
                              arg<"offset">(TimeDelta{0})};
        }

        static void eval(In<"frames", TS<Frame>, InputValidity::Unchecked> frames,
                         Scalar<"dt_col", Str> dt_col,
                         Scalar<"key_col", Str> key_col,
                         Scalar<"value_col", Str> value_col,
                         Scalar<"offset", TimeDelta> offset,
                         State<FromDataFrameBatchesState> state,
                         NodeScheduler sched, DateTime now,
                         Out<TsVar<"O">> out)
        {
            auto *active = state.get().handle;
            if (active != nullptr)
            {
                data_frame_detail::eval_from_frame(
                    *active, now, sched, static_cast<const TSOutputView &>(out));
            }

            if (!frames.modified()) { return; }
            if (active != nullptr)
            {
                const auto rows = active->frame.has_value()
                                      ? frame_rows(active->frame)
                                      : std::int64_t{0};
                if (active->row < rows)
                {
                    throw std::invalid_argument(
                        "from_data_frame_batches: a new batch arrived before "
                        "the previous batch was consumed");
                }
                delete active;
                active = nullptr;
            }

            const Frame &frame = frames.value();
            data_frame_detail::load_from_frame(
                frame, dt_col.value(), key_col.value(), value_col.value(),
                offset.value(), now, static_cast<const TSOutputView &>(out), active);
            state.set(FromDataFrameBatchesState{active});
            data_frame_detail::eval_from_frame(
                *active, now, sched, static_cast<const TSOutputView &>(out));
        }

        static void stop(State<FromDataFrameBatchesState> state)
        {
            std::unique_ptr<data_frame_detail::FromFramePlan> handle{
                state.get().handle};
            state.set(FromDataFrameBatchesState{});
        }
    };

    struct replay_data_frame_impl
    {
        static constexpr auto name = "replay_data_frame";

        static auto defaults()
        {
            return std::tuple{arg<"as_of_time">(MAX_DT)};
        }

        static void start(Scalar<"data_frame", Frame> data_frame,
                          Scalar<"as_of_time", DateTime> as_of_time,
                          GlobalStateView gs, DateTime now,
                          SingleShotScheduler sched,
                          State<ReplayDataFrameState> state,
                          Out<TsVar<"O">> out)
        {
            data_frame_detail::ReplayDataFramePlan *plan = nullptr;
            data_frame_detail::start_replay_data_frame(
                data_frame.value(), as_of_time.value(), now, gs,
                static_cast<const TSOutputView &>(out), sched, plan);
            state.set(ReplayDataFrameState{plan});
        }

        static void eval(Scalar<"data_frame", Frame> data_frame,
                         Scalar<"as_of_time", DateTime> as_of_time,
                         State<ReplayDataFrameState> state,
                         NodeScheduler sched, DateTime now,
                         Out<TsVar<"O">> out)
        {
            static_cast<void>(data_frame);
            static_cast<void>(as_of_time);
            data_frame_detail::eval_replay_data_frame(
                *state.get().handle, now, sched,
                static_cast<const TSOutputView &>(out));
        }

        static void stop(State<ReplayDataFrameState> state)
        {
            std::unique_ptr<data_frame_detail::ReplayDataFramePlan> handle{
                state.get().handle};
            state.set(ReplayDataFrameState{});
        }
    };

    /** ``to_data_frame(ts, ...)`` — a per-tick snapshot frame; the output
        ``Frame[Schema]`` names the columns (dt_col first, TSD adds key_col). */
    template <typename TInputSchema>
    struct to_data_frame_impl_base
    {
        static constexpr auto name = "to_data_frame";

        static auto defaults()
        {
            return std::tuple{arg<"dt_col">(Str{"date"}),
                              arg<"key_col">(Str{"key"}),
                              arg<"value_col">(Str{"value"})};
        }

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (output_bound(resolution)) { return; }
            const auto *schema  = time_series_schema_at(context, 0);
            const auto *dt_col  = context.scalar_as<Str>("dt_col");
            const auto *key_col = context.scalar_as<Str>("key_col");
            const auto *value_col = context.scalar_as<Str>("value_col");
            if (schema == nullptr || dt_col == nullptr || key_col == nullptr || value_col == nullptr)
            {
                return;
            }
            const auto *out = data_frame_detail::resolve_to_frame_output(
                schema, std::string_view{*dt_col}, std::string_view{*key_col},
                std::string_view{*value_col});
            if (out != nullptr) { bind_output(resolution, out); }
        }

        static void start(In<"ts", TInputSchema, InputValidity::Unchecked> ts, Scalar<"dt_col", Str> dt_col,
                          Scalar<"key_col", Str> key_col, Scalar<"value_col", Str> value_col,
                          State<ToDataFrameState> state, Out<TsVar<"__out__">> out)
        {
            data_frame_detail::ToFramePlan *plan = nullptr;
            data_frame_detail::start_to_frame(ts.base(), dt_col.value(), key_col.value(),
                                              value_col.value(),
                                              static_cast<const TSOutputView &>(out), plan);
            state.set(ToDataFrameState{plan});
        }

        static void eval(In<"ts", TInputSchema> ts, Scalar<"dt_col", Str> dt_col,
                         Scalar<"key_col", Str> key_col, Scalar<"value_col", Str> value_col,
                         State<ToDataFrameState> state, DateTime now, Out<TsVar<"__out__">> out)
        {
            static_cast<void>(dt_col);
            static_cast<void>(key_col);
            static_cast<void>(value_col);
            data_frame_detail::eval_to_frame(*state.get().handle, ts.base(), now,
                                             static_cast<const TSOutputView &>(out));
        }

        static void stop(State<ToDataFrameState> state)
        {
            std::unique_ptr<data_frame_detail::ToFramePlan> handle{state.get().handle};
            state.set(ToDataFrameState{});
        }
    };

    // Bind TSD elements independently so the normal input adaptation path
    // dereferences reference-valued leaves before the frame is assembled.
    using to_data_frame_tsd_impl =
        to_data_frame_impl_base<TSD<ScalarVar<"K">, TsVar<"V">>>;
    using to_data_frame_impl = to_data_frame_impl_base<TsVar<"S">>;

    /** ``group_by(ts, by)`` — partition a Frame TS into TSD[key, TS[Frame]]
        (removed keys emit REMOVE; ``by`` is a column name or tuple of them). */
    struct group_by_impl
    {
        static constexpr auto name = "group_by";

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (output_bound(resolution)) { return; }
            const auto *schema = time_series_schema_at(context, 0);
            const auto *by     = context.scalar("by");
            if (schema == nullptr || by == nullptr) { return; }
            const auto *out = data_frame_detail::resolve_group_by_output(schema, by->scalar_value.view());
            if (out != nullptr) { bind_output(resolution, out); }
        }

        static void start(In<"ts", TS<ScalarVar<"F">>, InputValidity::Unchecked> ts,
                          Scalar<"by", ScalarVar<"B">> by, State<GroupByState> state,
                          Out<TsVar<"__out__">> out)
        {
            data_frame_detail::GroupByPlan *plan = nullptr;
            data_frame_detail::start_group_by(ts.base(), by.value(),
                                              static_cast<const TSOutputView &>(out), plan);
            state.set(GroupByState{plan});
        }

        static void eval(In<"ts", TS<ScalarVar<"F">>> ts, Scalar<"by", ScalarVar<"B">> by,
                         State<GroupByState> state, Out<TsVar<"__out__">> out)
        {
            static_cast<void>(by);
            data_frame_detail::eval_group_by(*state.get().handle, ts.base(),
                                             static_cast<const TSOutputView &>(out));
        }

        static void stop(State<GroupByState> state)
        {
            std::unique_ptr<data_frame_detail::GroupByPlan> handle{state.get().handle};
            state.set(GroupByState{});
        }
    };

    /** Row count for any Frame scalar, including metadata-qualified frames. */
    struct len_frame_impl
    {
        static constexpr auto name = "len_frame";

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext)
        {
            const auto *meta = resolution.find_scalar("F");
            return meta != nullptr && TypeRegistry::instance().is_frame(meta);
        }

        static void eval(In<"ts", TS<ScalarVar<"F">>> ts, Out<TS<Int>> out)
        {
            out.set(static_cast<Int>(frame_rows(ts.base().value().checked_as<Frame>())));
        }
    };

    /** Arrow-native frame ordering. The row schema is preserved exactly. */
    struct sorted_frame_impl
    {
        static constexpr auto name = "sorted_frame";

        static auto defaults() { return std::tuple{arg<"descending">(Bool{false})}; }

        static void eval(In<"ts", TS<FrameOf<ScalarVar<"R">>>> ts, Scalar<"by", Str> by,
                         Scalar<"descending", Bool> descending,
                         Out<TS<FrameOf<ScalarVar<"R">>>> out)
        {
            out.set(data_frame_detail::sort_frame(ts.value(), by.value(), descending.value()));
        }
    };

    /** Arrow-native zero-copy row concatenation for schema-identical frames. */
    struct concat_frame_impl
    {
        static constexpr auto name = "concat_frame";

        static void eval(In<"ts1", TS<FrameOf<ScalarVar<"R">>>> ts1,
                         In<"ts2", TS<FrameOf<ScalarVar<"R">>>> ts2,
                         Out<TS<FrameOf<ScalarVar<"R">>>> out)
        {
            out.set(data_frame_detail::concat_frames(ts1.value(), ts2.value()));
        }
    };

    /** Arrow Acero hash join. Output row resolution is native so erased and
        Python wiring do not need a second schema implementation. */
    struct join_frame_impl
    {
        static constexpr auto name = "join_frame";

        static auto defaults()
        {
            return std::tuple{arg<"how">(Str{"inner"}), arg<"suffix">(Str{"_right"})};
        }

        static void resolve_default_types(ResolutionMap &resolution,
                                          OperatorCallContext context)
        {
            if (resolution.find_scalar("O") != nullptr) { return; }
            const auto *lhs = time_series_schema_at(context, 0);
            const auto *rhs = time_series_schema_at(context, 1);
            const auto *on = context.scalar("on");
            const auto *how = context.scalar_as<Str>("how");
            const auto *suffix = context.scalar_as<Str>("suffix");
            if (lhs == nullptr || rhs == nullptr || on == nullptr || how == nullptr ||
                suffix == nullptr)
            {
                return;
            }
            const auto *row = data_frame_detail::resolve_join_row(
                lhs, rhs, on->scalar_value.view(), *how, *suffix);
            if (row != nullptr) { resolution.bind_scalar("O", row); }
        }

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return data_frame_detail::ts_value_is_frame(time_series_schema_at(context, 0)) &&
                   data_frame_detail::ts_value_is_frame(time_series_schema_at(context, 1));
        }

        static void eval(In<"lhs", TS<FrameOf<ScalarVar<"L">>>> lhs,
                         In<"rhs", TS<FrameOf<ScalarVar<"R">>>> rhs,
                         Scalar<"on", ScalarVar<"K">> on, Scalar<"how", Str> how,
                         Scalar<"suffix", Str> suffix,
                         Out<TS<FrameOf<ScalarVar<"O">>>> out)
        {
            out.set(data_frame_detail::join_frames(lhs.value(), rhs.value(), on.value(),
                                                   how.value(), suffix.value()));
        }
    };

    struct filter_frame_impl
    {
        static constexpr auto name = "filter_frame";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            const auto *predicate = time_series_schema_at(context, 1);
            return data_frame_detail::ts_value_is_frame(time_series_schema_at(context, 0)) &&
                   predicate != nullptr && predicate->kind == TSTypeKind::TSB;
        }

        static void eval(In<"ts", TS<FrameOf<ScalarVar<"R">>>> ts,
                         In<"predicate", TsVar<"P">> predicate,
                         Out<TS<FrameOf<ScalarVar<"R">>>> out)
        {
            auto &erased = const_cast<TSInputView &>(predicate.base());
            out.set(data_frame_detail::filter_frame_by_bundle(
                ts.value(), erased, ts.base().schema()->value_schema->element_type));
        }
    };

    struct filter_cs_impl
    {
        static constexpr auto name = "filter_cs";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            const auto *predicate = ts_value_schema_at(context, 1);
            return data_frame_detail::ts_value_is_frame(time_series_schema_at(context, 0)) &&
                   predicate != nullptr && predicate->value_kind() == ValueTypeKind::Bundle;
        }

        static void eval(In<"ts", TS<FrameOf<ScalarVar<"R">>>> ts,
                         In<"predicate", TS<ScalarVar<"P">>> predicate,
                         Out<TS<FrameOf<ScalarVar<"R">>>> out)
        {
            out.set(data_frame_detail::filter_frame_by_value(
                ts.value(), predicate.base().value(),
                ts.base().schema()->value_schema->element_type));
        }
    };

    struct ungroup_frame_impl
    {
        static constexpr auto name = "ungroup_frame";

        static void resolve_default_types(ResolutionMap &resolution,
                                          OperatorCallContext context)
        {
            if (resolution.find_scalar("O") != nullptr) { return; }
            const auto *row = data_frame_detail::resolve_ungroup_row(
                time_series_schema_at(context, 0), nullptr);
            if (row != nullptr) { resolution.bind_scalar("O", row); }
        }

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            const auto *ts = time_series_schema_at_as<AnyTSD>(context, 0);
            return ts != nullptr &&
                   data_frame_detail::ts_value_is_frame(ts->element_ts());
        }

        static void eval(
            In<"ts", TSD<ScalarVar<"K">,
                         TS<FrameOf<ScalarVar<"R">>>>> ts,
                         Out<TS<FrameOf<ScalarVar<"O">>>> out)
        {
            auto &erased = const_cast<TSInputView &>(ts.base());
            out.set(data_frame_detail::ungroup_frames(
                erased, nullptr,
                static_cast<const TSOutputView &>(out).schema()->value_schema->element_type));
        }
    };

    struct ungroup_frame_with_keys_impl
    {
        static constexpr auto name = "ungroup_frame_with_keys";

        static void resolve_default_types(ResolutionMap &resolution,
                                          OperatorCallContext context)
        {
            if (resolution.find_scalar("O") != nullptr) { return; }
            const auto *key_col = context.scalar("key_col");
            if (key_col == nullptr) { return; }
            const ValueView key_view = key_col->scalar_value.view();
            const auto *row = data_frame_detail::resolve_ungroup_row(
                time_series_schema_at(context, 0), &key_view);
            if (row != nullptr) { resolution.bind_scalar("O", row); }
        }

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            const auto *ts = time_series_schema_at_as<AnyTSD>(context, 0);
            return ts != nullptr &&
                   data_frame_detail::ts_value_is_frame(ts->element_ts());
        }

        static void eval(
            In<"ts", TSD<ScalarVar<"K">,
                         TS<FrameOf<ScalarVar<"R">>>>> ts,
                         Scalar<"key_col", ScalarVar<"C">> key_col,
                         Out<TS<FrameOf<ScalarVar<"O">>>> out)
        {
            auto &erased = const_cast<TSInputView &>(ts.base());
            const ValueView key_columns = key_col.value();
            out.set(data_frame_detail::ungroup_frames(
                erased, &key_columns,
                static_cast<const TSOutputView &>(out).schema()->value_schema->element_type));
        }
    };

    struct ungroup_items_impl
    {
        static constexpr auto name = "ungroup_items";

        static void resolve_default_types(ResolutionMap &resolution,
                                          OperatorCallContext context)
        {
            if (resolution.find_scalar("O") != nullptr) { return; }
            const auto *row = data_frame_detail::resolve_ungroup_row(
                time_series_schema_at(context, 0), nullptr);
            if (row != nullptr) { resolution.bind_scalar("O", row); }
        }

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            const auto *ts = time_series_schema_at_as<AnyTSD>(context, 0);
            const auto *value = ts != nullptr ? ts_value_schema(ts->element_ts()) : nullptr;
            return value != nullptr && value->value_kind() == ValueTypeKind::Bundle;
        }

        static void eval(In<"ts", TsVar<"S">> ts,
                         Out<TS<FrameOf<ScalarVar<"O">>>> out)
        {
            auto &erased = const_cast<TSInputView &>(ts.base());
            out.set(data_frame_detail::ungroup_items(
                erased,
                static_cast<const TSOutputView &>(out).schema()->value_schema->element_type));
        }
    };

    struct with_columns_impl
    {
        static constexpr auto name = "with_columns";

        static void resolve_default_types(ResolutionMap &resolution,
                                          OperatorCallContext context)
        {
            if (resolution.find_scalar("O") != nullptr) { return; }
            const auto *frame = ts_value_schema_at(context, 0);
            if (frame != nullptr && TypeRegistry::instance().is_frame(frame) &&
                frame->element_type != nullptr)
            {
                resolution.bind_scalar("O", frame->element_type);
            }
        }

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            const auto *columns = time_series_schema_at(context, 1);
            return data_frame_detail::ts_value_is_frame(time_series_schema_at(context, 0)) &&
                   columns != nullptr && columns->kind == TSTypeKind::TSB;
        }

        static void eval(In<"ts", TS<FrameOf<ScalarVar<"R">>>> ts,
                         In<"columns", TsVar<"C">> columns,
                         Out<TS<FrameOf<ScalarVar<"O">>>> out)
        {
            auto &erased = const_cast<TSInputView &>(columns.base());
            out.set(data_frame_detail::replace_frame_columns(
                ts.value(), erased,
                static_cast<const TSOutputView &>(out).schema()->value_schema->element_type));
        }
    };

    /** Metadata-bearing overloads preserve the Arrow schema metadata. */
    struct sorted_metadata_frame_impl
    {
        static constexpr auto name = "sorted_metadata_frame";
        static auto defaults() { return std::tuple{arg<"descending">(Bool{false})}; }
        static void eval(In<"ts", TS<FrameOf<ScalarVar<"R">, ScalarVar<"M">>>> ts,
                         Scalar<"by", Str> by, Scalar<"descending", Bool> descending,
                         Out<TS<FrameOf<ScalarVar<"R">, ScalarVar<"M">>>> out)
        {
            out.set(data_frame_detail::sort_frame(ts.value(), by.value(), descending.value()));
        }
    };

    struct concat_metadata_frame_impl
    {
        static constexpr auto name = "concat_metadata_frame";
        static void eval(In<"ts1", TS<FrameOf<ScalarVar<"R">, ScalarVar<"M">>>> ts1,
                         In<"ts2", TS<FrameOf<ScalarVar<"R">, ScalarVar<"M">>>> ts2,
                         Out<TS<FrameOf<ScalarVar<"R">, ScalarVar<"M">>>> out)
        {
            out.set(data_frame_detail::concat_frames(ts1.value(), ts2.value()));
        }
    };

    struct filter_metadata_frame_impl
    {
        static constexpr auto name = "filter_metadata_frame";
        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            const auto *predicate = time_series_schema_at(context, 1);
            const auto *frame = ts_value_schema_at(context, 0);
            return frame != nullptr && TypeRegistry::instance().is_frame(frame) &&
                   frame->key_type != nullptr && predicate != nullptr &&
                   predicate->kind == TSTypeKind::TSB;
        }
        static void eval(In<"ts", TS<FrameOf<ScalarVar<"R">, ScalarVar<"M">>>> ts,
                         In<"predicate", TsVar<"P">> predicate,
                         Out<TS<FrameOf<ScalarVar<"R">, ScalarVar<"M">>>> out)
        {
            auto &erased = const_cast<TSInputView &>(predicate.base());
            out.set(data_frame_detail::filter_frame_by_bundle(
                ts.value(), erased, ts.base().schema()->value_schema->element_type));
        }
    };

    struct filter_cs_metadata_frame_impl
    {
        static constexpr auto name = "filter_cs_metadata_frame";
        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            const auto *predicate = ts_value_schema_at(context, 1);
            const auto *frame = ts_value_schema_at(context, 0);
            return frame != nullptr && TypeRegistry::instance().is_frame(frame) &&
                   frame->key_type != nullptr && predicate != nullptr &&
                   predicate->value_kind() == ValueTypeKind::Bundle;
        }
        static void eval(In<"ts", TS<FrameOf<ScalarVar<"R">, ScalarVar<"M">>>> ts,
                         In<"predicate", TS<ScalarVar<"P">>> predicate,
                         Out<TS<FrameOf<ScalarVar<"R">, ScalarVar<"M">>>> out)
        {
            out.set(data_frame_detail::filter_frame_by_value(
                ts.value(), predicate.base().value(),
                ts.base().schema()->value_schema->element_type));
        }
    };

    struct with_columns_metadata_frame_impl
    {
        static constexpr auto name = "with_columns_metadata_frame";
        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (resolution.find_scalar("O") != nullptr) { return; }
            const auto *frame = ts_value_schema_at(context, 0);
            if (frame != nullptr && TypeRegistry::instance().is_frame(frame) &&
                frame->element_type != nullptr)
            {
                resolution.bind_scalar("O", frame->element_type);
            }
        }
        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            const auto *columns = time_series_schema_at(context, 1);
            const auto *frame = ts_value_schema_at(context, 0);
            return frame != nullptr && TypeRegistry::instance().is_frame(frame) &&
                   frame->key_type != nullptr && columns != nullptr &&
                   columns->kind == TSTypeKind::TSB;
        }
        static void eval(In<"ts", TS<FrameOf<ScalarVar<"R">, ScalarVar<"M">>>> ts,
                         In<"columns", TsVar<"C">> columns,
                         Out<TS<FrameOf<ScalarVar<"O">, ScalarVar<"M">>>> out)
        {
            auto &erased = const_cast<TSInputView &>(columns.base());
            out.set(data_frame_detail::replace_frame_columns(
                ts.value(), erased,
                static_cast<const TSOutputView &>(out).schema()->value_schema->element_type));
        }
    };

    namespace data_frame_impl_detail
    {
        [[nodiscard]] inline bool output_is_frame(const ResolutionMap &resolution)
        {
            return data_frame_detail::value_is_frame(output_ts_value_schema(resolution));
        }

        /** The no-mapping default: an empty STRING sentinel (mapping_entry
            treats any non-map as empty; a default-constructed Map value has
            no bindings and cannot hash for call-identity interning). */
        [[nodiscard]] inline Value empty_str_map() { return Value{Str{}}; }
    }  // namespace data_frame_impl_detail

    /** ``convert[TS[Frame*]](tsd)`` — snapshot a compound-valued TSD into a
        frame (mapping["key_col"] adds the key column). */
    struct convert_tsd_to_frame_impl
    {
        static constexpr auto name = "convert_tsd_to_frame";

        static auto defaults()
        {
            return std::tuple{arg<"mapping">(data_frame_impl_detail::empty_str_map())};
        }

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *in = time_series_schema_at(context, 0);
            return data_frame_impl_detail::output_is_frame(resolution) && in != nullptr &&
                   in->kind == TSTypeKind::TSD && in->element_ts()->kind == TSTypeKind::TS &&
                   in->element_ts()->value_schema->value_kind() == ValueTypeKind::Bundle;
        }

        static void start(In<"ts", TsVar<"S">, InputValidity::Unchecked> ts,
                          Scalar<"mapping", ScalarVar<"M">> mapping, State<ToDataFrameState> state,
                          Out<TsVar<"__out__">> out)
        {
            data_frame_detail::ToFramePlan *plan = nullptr;
            data_frame_detail::start_convert_tsd_frame(ts.base(), mapping.value(),
                                                       static_cast<const TSOutputView &>(out), plan);
            state.set(ToDataFrameState{plan});
        }

        static void eval(In<"ts", TsVar<"S">> ts, Scalar<"mapping", ScalarVar<"M">> mapping,
                         State<ToDataFrameState> state, DateTime now, Out<TsVar<"__out__">> out)
        {
            static_cast<void>(mapping);
            data_frame_detail::eval_to_frame(*state.get().handle, ts.base(), now,
                                             static_cast<const TSOutputView &>(out));
        }

        static void stop(State<ToDataFrameState> state)
        {
            std::unique_ptr<data_frame_detail::ToFramePlan> handle{state.get().handle};
            state.set(ToDataFrameState{});
        }
    };

    /** ``convert[TS[Frame[X]]](frame_ts[, mapping])`` — pass-through /
        column-rename (input schemas are a minimum; output is exact). */
    struct convert_frame_to_frame_impl
    {
        static constexpr auto name = "convert_frame_to_frame";

        static auto defaults()
        {
            return std::tuple{arg<"mapping">(data_frame_impl_detail::empty_str_map())};
        }

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            return data_frame_impl_detail::output_is_frame(resolution) &&
                   data_frame_detail::ts_value_is_frame(time_series_schema_at(context, 0));
        }

        static void eval(In<"ts", TsVar<"S">> ts, Scalar<"mapping", ScalarVar<"M">> mapping,
                         Out<TsVar<"__out__">> out)
        {
            data_frame_detail::eval_convert_frame_frame(mapping.value(), ts.base(),
                                                        static_cast<const TSOutputView &>(out));
        }
    };

    /** ``convert[TS[Frame[X]]](ts)`` over a compound (one row per tick) or a
        homogeneous tuple of compounds (one row per element). */
    struct convert_value_to_frame_impl
    {
        static constexpr auto name = "convert_value_to_frame";

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *in = time_series_schema_at(context, 0);
            if (!data_frame_impl_detail::output_is_frame(resolution) || in == nullptr ||
                in->kind != TSTypeKind::TS)
            {
                return false;
            }
            const auto *value = in->value_schema;
            if (value->value_kind() == ValueTypeKind::Bundle) { return true; }
            const auto *element = tuple_element_schema(value);
            return element != nullptr && element->value_kind() == ValueTypeKind::Bundle;
        }

        static void start(In<"ts", TsVar<"S">, InputValidity::Unchecked> ts,
                          State<ToDataFrameState> state, Out<TsVar<"__out__">> out)
        {
            data_frame_detail::ToFramePlan *plan = nullptr;
            data_frame_detail::start_convert_value_frame(ts.base(),
                                                         static_cast<const TSOutputView &>(out), plan);
            state.set(ToDataFrameState{plan});
        }

        static void eval(In<"ts", TsVar<"S">> ts, State<ToDataFrameState> state,
                         Out<TsVar<"__out__">> out)
        {
            data_frame_detail::eval_convert_value_frame(*state.get().handle, ts.base(),
                                                        static_cast<const TSOutputView &>(out));
        }

        static void stop(State<ToDataFrameState> state)
        {
            std::unique_ptr<data_frame_detail::ToFramePlan> handle{state.get().handle};
            state.set(ToDataFrameState{});
        }
    };

    /** ``combine[TS[Frame[X]]](a=col_ts, b=col_ts)`` — zip tuple-valued
        column time-series into a frame (fields matched by name). */
    struct combine_frame_impl
    {
        static constexpr auto name = "combine_frame";

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *in = time_series_schema_at(context, 0);
            return data_frame_impl_detail::output_is_frame(resolution) && in != nullptr &&
                   in->kind == TSTypeKind::TSB;
        }

        static void start(In<"ts", TsVar<"S">, InputValidity::Unchecked> ts,
                          State<ToDataFrameState> state, Out<TsVar<"__out__">> out)
        {
            data_frame_detail::ToFramePlan *plan = nullptr;
            data_frame_detail::start_combine_frame(ts.base(),
                                                   static_cast<const TSOutputView &>(out), plan);
            state.set(ToDataFrameState{plan});
        }

        static void eval(In<"ts", TsVar<"S">> ts, State<ToDataFrameState> state,
                         Out<TsVar<"__out__">> out)
        {
            data_frame_detail::eval_combine_frame(*state.get().handle, ts.base(),
                                                  static_cast<const TSOutputView &>(out));
        }

        static void stop(State<ToDataFrameState> state)
        {
            std::unique_ptr<data_frame_detail::ToFramePlan> handle{state.get().handle};
            state.set(ToDataFrameState{});
        }
    };

    /** Register the data-frame operator overloads. */
    void register_data_frame_operators();
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_DATA_FRAME_IMPL_H
