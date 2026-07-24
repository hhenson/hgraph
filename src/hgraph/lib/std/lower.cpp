#include <hgraph/lib/std/lower.h>

#include <hgraph/lib/std/operators/data_frame.h>
#include <hgraph/runtime/global_state.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/value/table_codec.h>

#include <arrow/api.h>
#include <arrow/compute/api.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace hgraph::stdlib::lower_detail
{
    inline constexpr std::string_view RESULT_KEY{"__hgraph.lower.result__"};

    struct Collector
    {
        const ValueTypeMetaData *row_schema{nullptr};
        std::vector<Frame> frames{};
    };

    struct CollectorState
    {
        Collector *handle{nullptr};
    };
} // namespace hgraph::stdlib::lower_detail

namespace hgraph::static_schema_detail
{
    template <> struct scalar_name<stdlib::lower_detail::CollectorState>
    {
        static constexpr std::string_view value{"LowerFrameCollectorState"};
    };
} // namespace hgraph::static_schema_detail

namespace hgraph::stdlib
{
    namespace lower_detail
    {
        void require_ok(const arrow::Status &status, std::string_view operation)
        {
            if (!status.ok())
            {
                throw std::runtime_error(std::string{"lower: Arrow "} + std::string{operation} +
                                         " failed: " + status.ToString());
            }
        }

        template <typename T> [[nodiscard]] T require_result(arrow::Result<T> result, std::string_view operation)
        {
            require_ok(result.status(), operation);
            return std::move(result).ValueUnsafe();
        }

        [[nodiscard]] Frame empty_frame(const ValueTypeMetaData *row_schema)
        {
            if (row_schema == nullptr)
            {
                throw std::logic_error("lower: resolved output frame has no row schema");
            }
            const auto &converter = table_converter(row_schema, "__lower_date__", "__lower_as_of__");
            return frame_from_values(converter, {});
        }

        [[nodiscard]] Frame concatenate(std::vector<Frame> frames, const ValueTypeMetaData *row_schema)
        {
            if (frames.empty())
            {
                return empty_frame(row_schema);
            }
            if (frames.size() == 1)
            {
                return std::move(frames.front());
            }

            std::vector<std::shared_ptr<arrow::Table>> tables;
            tables.reserve(frames.size());
            for (Frame &frame : frames)
            {
                if (frame.has_value())
                {
                    tables.push_back(std::move(frame.table));
                }
            }
            if (tables.empty())
            {
                return empty_frame(row_schema);
            }
            return Frame{require_result(arrow::ConcatenateTables(tables), "table concatenation")};
        }

        [[nodiscard]] Frame add_as_of_column(Frame frame, std::string_view name, DateTime as_of)
        {
            if (!frame.has_value())
            {
                return frame;
            }
            if (frame.table->GetColumnByName(std::string{name}) != nullptr)
            {
                throw std::invalid_argument("lower: the as-of column conflicts with an output column");
            }

            std::unique_ptr<arrow::ArrayBuilder> builder;
            require_ok(
                arrow::MakeBuilder(
                    arrow::default_memory_pool(),
                    arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"),
                    &builder),
                "as-of builder creation");
            auto &timestamps = static_cast<arrow::TimestampBuilder &>(*builder);
            for (std::int64_t row = 0; row < frame_rows(frame); ++row)
            {
                require_ok(timestamps.Append(as_of.time_since_epoch().count()), "as-of append");
            }
            std::shared_ptr<arrow::Array> array;
            require_ok(builder->Finish(&array), "as-of finish");
            auto column = std::make_shared<arrow::ChunkedArray>(std::move(array));
            frame.table = require_result(
                frame.table->AddColumn(
                    1,
                    arrow::field(
                        std::string{name},
                        arrow::timestamp(arrow::TimeUnit::MICRO, "UTC")),
                                       std::move(column)),
                "as-of column insertion");
            return frame;
        }

        struct TemporalGroup
        {
            DateTime date{};
            Value key{};

            [[nodiscard]] bool operator==(const TemporalGroup &other) const noexcept
            {
                return date == other.date && key == other.key;
            }
        };

        struct TemporalGroupHash
        {
            [[nodiscard]] std::size_t operator()(const TemporalGroup &group) const
            {
                std::size_t seed = std::hash<std::int64_t>{}(group.date.time_since_epoch().count());
                if (group.key.has_value())
                {
                    seed ^= group.key.view().hash() + 0x9e3779b97f4a7c15ULL + (seed << 6U) + (seed >> 2U);
                }
                return seed;
            }
        };

        struct SelectedRow
        {
            std::int64_t row{0};
            DateTime date{};
            DateTime as_of{};
        };

        [[nodiscard]] DateTime datetime_cell(const Frame &frame, std::string_view column, std::int64_t row)
        {
            const Value value = frame_cell(frame, column, scalar_descriptor<DateTime>::value_meta(), row);
            if (!value.has_value())
            {
                throw std::invalid_argument("lower: date and as-of columns must not contain nulls");
            }
            return value.view().checked_as<DateTime>();
        }

        [[nodiscard]] Frame take_rows(Frame frame, std::span<const SelectedRow> selected)
        {
            arrow::Int64Builder builder;
            for (const SelectedRow &row : selected)
            {
                require_ok(builder.Append(row.row), "row-index append");
            }
            std::shared_ptr<arrow::Array> indices;
            require_ok(builder.Finish(&indices), "row-index finish");
            arrow::Datum result = require_result(
                arrow::compute::Take(arrow::Datum{frame.table}, arrow::Datum{std::move(indices)}), "row selection");
            return Frame{result.table()};
        }

        [[nodiscard]] Frame prepare_input_frame(Frame frame, const TSValueTypeMetaData *input_schema,
                                                const LowerOptions &options, DateTime invocation_as_of)
        {
            frame.table = require_result(frame.table->CombineChunks(), "input chunk combination");
            std::vector<SelectedRow> selected;
            selected.reserve(static_cast<std::size_t>(frame_rows(frame)));

            const bool keyed = input_schema->kind == TSTypeKind::TSD;
            const ValueTypeMetaData *key_schema = keyed ? input_schema->key_type() : nullptr;
            std::unordered_map<TemporalGroup, SelectedRow, TemporalGroupHash> latest;
            latest.reserve(static_cast<std::size_t>(frame_rows(frame)));

            for (std::int64_t row = 0; row < frame_rows(frame); ++row)
            {
                const DateTime date = datetime_cell(frame, options.date_column, row);
                if (date < options.start_time || date > options.end_time)
                {
                    continue;
                }
                const DateTime as_of =
                    options.no_as_of_support ? MIN_DT : datetime_cell(frame, options.as_of_column, row);
                if (!options.no_as_of_support && as_of > invocation_as_of)
                {
                    continue;
                }
                TemporalGroup group{.date = date};
                if (keyed)
                {
                    group.key = frame_cell(frame, options.key_column, key_schema, row);
                    if (!group.key.has_value())
                    {
                        throw std::invalid_argument("lower: keyed input frames must not contain null keys");
                    }
                }
                const SelectedRow candidate{row, date, as_of};
                const auto [position, inserted] = latest.try_emplace(std::move(group), candidate);
                if (!options.no_as_of_support && !inserted && position->second.as_of < as_of)
                {
                    position->second = candidate;
                }
            }

            selected.reserve(latest.size());
            for (const auto &[group, row] : latest)
            {
                static_cast<void>(group);
                selected.push_back(row);
            }
            std::ranges::sort(selected,
                              [](const SelectedRow &lhs, const SelectedRow &rhs)
                              {
                                  if (lhs.date != rhs.date)
                                  {
                                      return lhs.date < rhs.date;
                                  }
                                  return lhs.row < rhs.row;
                              });
            frame = take_rows(std::move(frame), selected);

            const int as_of_index = frame.table->schema()->GetFieldIndex(options.as_of_column);
            if (as_of_index >= 0)
            {
                frame.table = require_result(frame.table->RemoveColumn(as_of_index), "input as-of column removal");
            }
            return frame;
        }

        struct collect_frames_impl
        {
            static constexpr auto name = "lower_collect_frames";

            static void start(In<"ts", TS<ScalarVar<"F">>, InputValidity::Unchecked> ts, State<CollectorState> state)
            {
                const auto *frame_schema = ts.base().schema()->value_schema;
                if (frame_schema == nullptr || frame_schema->element_type == nullptr)
                {
                    throw std::logic_error("lower: to_data_frame produced an untyped Frame");
                }
                auto collector = std::make_unique<Collector>();
                collector->row_schema = frame_schema->element_type;
                state.set(CollectorState{collector.get()});
                collector.release();
            }

            static void eval(In<"ts", TS<ScalarVar<"F">>> ts, Scalar<"include_as_of", Bool> include_as_of,
                             Scalar<"as_of_column", Str> as_of_column, Scalar<"as_of", DateTime> as_of,
                             State<CollectorState> state)
            {
                static_cast<void>(include_as_of);
                static_cast<void>(as_of_column);
                static_cast<void>(as_of);
                state.get().handle->frames.push_back(ts.base().value().checked_as<Frame>());
            }

            static void stop(Scalar<"include_as_of", Bool> include_as_of, Scalar<"as_of_column", Str> as_of_column,
                             Scalar<"as_of", DateTime> as_of, State<CollectorState> state, GlobalStateView global_state)
            {
                std::unique_ptr<Collector> collector{state.get().handle};
                state.set(CollectorState{});
                if (collector == nullptr)
                {
                    return;
                }
                Frame result = concatenate(std::move(collector->frames), collector->row_schema);
                if (include_as_of.value())
                {
                    result = add_as_of_column(std::move(result), as_of_column.value(), as_of.value());
                }
                global_state.set(RESULT_KEY, Value{std::move(result)});
            }
        };

        [[nodiscard]] WiringArg scalar_arg(Value value, const ValueTypeMetaData *schema, std::string name = {})
        {
            WiringArg arg;
            arg.kind = WiringArg::Kind::Scalar;
            arg.scalar_meta = schema;
            arg.scalar_value = std::move(value);
            arg.name = std::move(name);
            return arg;
        }

        [[nodiscard]] WiringArg ts_arg(WiringPortRef port)
        {
            WiringArg arg;
            arg.kind = WiringArg::Kind::TimeSeries;
            arg.port = std::move(port);
            return arg;
        }

        [[nodiscard]] WiringPortRef from_frame(Wiring &wiring, Frame frame, const TSValueTypeMetaData *output_schema,
                                               const LowerOptions &options)
        {
            std::array args{
                scalar_arg(Value{std::move(frame)}, scalar_descriptor<Frame>::value_meta()),
                scalar_arg(Value{Str{options.date_column}}, scalar_descriptor<Str>::value_meta()),
                scalar_arg(Value{Str{options.key_column}}, scalar_descriptor<Str>::value_meta()),
                scalar_arg(Value{Str{options.value_column}}, scalar_descriptor<Str>::value_meta()),
                scalar_arg(Value{TimeDelta{0}}, scalar_descriptor<TimeDelta>::value_meta()),
            };
            return wire_operator(wiring, from_data_frame::name, args, true, output_schema).output.erased();
        }

        [[nodiscard]] WiringPortRef to_frame(Wiring &wiring, WiringPortRef source, const LowerOptions &options)
        {
            std::array args{
                ts_arg(std::move(source)),
                scalar_arg(Value{Str{options.date_column}}, scalar_descriptor<Str>::value_meta()),
                scalar_arg(Value{Str{options.key_column}}, scalar_descriptor<Str>::value_meta()),
                scalar_arg(Value{Str{options.value_column}}, scalar_descriptor<Str>::value_meta()),
            };
            return wire_operator(wiring, to_data_frame::name, args, true).output.erased();
        }

        [[nodiscard]] DateTime current_time() noexcept
        {
            return std::chrono::time_point_cast<std::chrono::microseconds>(engine_clock::now());
        }

        void validate(const WiredFn &function, std::span<const Frame> inputs, const LowerOptions &options)
        {
            if (!function.valid())
            {
                throw std::invalid_argument("lower: function is empty");
            }
            if (function.variadic)
            {
                throw std::invalid_argument("lower: variadic graph functions are not supported");
            }
            if (inputs.size() != function.arity)
            {
                throw std::invalid_argument("lower: input frame count does not match the function arity");
            }
            if (options.date_column.empty())
            {
                throw std::invalid_argument("lower: date_column must not be empty");
            }
            if (!options.no_as_of_support && options.as_of_column.empty())
            {
                throw std::invalid_argument("lower: as_of_column must not be empty when as-of support is enabled");
            }
            if (!options.no_as_of_support && options.date_column == options.as_of_column)
            {
                throw std::invalid_argument("lower: date and as-of columns must have different names");
            }
            if (options.end_time < options.start_time)
            {
                throw std::invalid_argument("lower: end_time must not precede start_time");
            }
            for (std::size_t index = 0; index < inputs.size(); ++index)
            {
                if (!inputs[index].has_value())
                {
                    throw std::invalid_argument("lower: every input must contain an Arrow frame");
                }
                if (function.input_schema(index) == nullptr)
                {
                    throw std::invalid_argument("lower: every function input must have a "
                                                "concrete time-series schema");
                }
                const std::vector<std::string> columns = frame_column_names(inputs[index]);
                const auto has_column = [&columns](const std::string &name)
                { return std::ranges::find(columns, name) != columns.end(); };
                if (!has_column(options.date_column))
                {
                    throw std::invalid_argument("lower: every input frame must contain the date column");
                }
                if (!options.no_as_of_support && !has_column(options.as_of_column))
                {
                    throw std::invalid_argument("lower: every input frame must contain the as-of column when "
                                                "as-of support is enabled");
                }
            }
        }
    } // namespace lower_detail

    LowerExecution::LowerExecution() noexcept = default;
    LowerExecution::~LowerExecution() = default;
    LowerExecution::LowerExecution(LowerExecution &&) noexcept = default;
    LowerExecution &LowerExecution::operator=(LowerExecution &&) noexcept = default;

    LowerExecution::LowerExecution(GraphExecutorValue executor, bool has_output)
        : executor_(std::move(executor)), has_output_(has_output)
    {
    }

    void LowerExecution::run()
    {
        if (ran_)
        {
            throw std::logic_error("lower: execution has already run");
        }
        if (!executor_.has_value())
        {
            throw std::logic_error("lower: execution is not prepared");
        }
        auto view = executor_.view();
        view.run();
        const GlobalStateView graph_state = view.graph().global_state();
        if (has_output_)
        {
            const ValueView stored = graph_state.get(lower_detail::RESULT_KEY);
            if (!stored.valid())
            {
                throw std::logic_error("lower: output collector produced no result");
            }
            result_ = stored.checked_as<Frame>();
            graph_state.erase(lower_detail::RESULT_KEY);
        }
        if (GlobalState *state = GlobalContext::active_state())
        {
            state->view().copy_from(graph_state);
        }
        ran_ = true;
    }

    bool LowerExecution::ran() const noexcept { return ran_; }
    bool LowerExecution::has_output() const noexcept { return has_output_; }

    GlobalStateView LowerExecution::global_state() const
    {
        if (!executor_.has_value())
        {
            throw std::logic_error("lower: execution is not prepared");
        }
        return executor_.view().graph().global_state();
    }

    const std::optional<Frame> &LowerExecution::result() const
    {
        if (!ran_)
        {
            throw std::logic_error("lower: execution has not run");
        }
        return result_;
    }

    LowerExecution prepare_lower(const WiredFn &function, std::span<const Frame> inputs, LowerOptions options)
    {
        lower_detail::validate(function, inputs, options);

        Wiring wiring;
        const DateTime invocation_as_of = options.as_of.value_or(lower_detail::current_time());
        std::vector<WiringPortRef> ports;
        ports.reserve(inputs.size());
        for (std::size_t index = 0; index < inputs.size(); ++index)
        {
            ports.push_back(
                lower_detail::from_frame(wiring,
                                         lower_detail::prepare_input_frame(inputs[index], function.input_schema(index),
                                                                           options, invocation_as_of),
                                         function.input_schema(index), options));
        }

        const WiringPortRef output = function.wire(wiring, ports);
        const bool has_output = function.has_output && output.schema != nullptr;
        if (has_output)
        {
            const WiringPortRef frame = lower_detail::to_frame(wiring, output, options);
            wire<lower_detail::collect_frames_impl>(wiring, Port<void>{wiring, frame}, Bool{!options.no_as_of_support},
                                                    Str{options.as_of_column}, invocation_as_of);
        }

        GraphBuilder graph = std::move(wiring).finish();
        GraphExecutorBuilder builder;
        builder.graph_builder(std::move(graph)).start_time(options.start_time).end_time(options.end_time);
        if (options.observer != nullptr)
        {
            builder.add_lifecycle_observer(options.observer);
        }
        return LowerExecution{builder.make_executor(), has_output};
    }

    std::optional<Frame> lower(const WiredFn &function, std::span<const Frame> inputs, LowerOptions options)
    {
        LowerExecution execution = prepare_lower(function, inputs, std::move(options));
        execution.run();
        return execution.result();
    }
} // namespace hgraph::stdlib
