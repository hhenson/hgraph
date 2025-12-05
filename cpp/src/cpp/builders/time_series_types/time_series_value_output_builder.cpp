#include <hgraph/builders/time_series_types/time_series_value_output_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ts.h>
#include <hgraph/util/arena_enable_shared_from_this.h>

namespace hgraph {
    TimeSeriesValueOutputBuilder::TimeSeriesValueOutputBuilder(const std::type_info& value_type)
        : _value_type(value_type) {}

    time_series_output_s_ptr TimeSeriesValueOutputBuilder::make_instance(node_ptr owning_node) const {
        return arena_make_shared_as<TimeSeriesValueOutput, TimeSeriesOutput>(owning_node, _value_type);
    }

    time_series_output_s_ptr TimeSeriesValueOutputBuilder::make_instance(time_series_output_ptr owning_output) const {
        return arena_make_shared_as<TimeSeriesValueOutput, TimeSeriesOutput>(owning_output, _value_type);
    }

    void TimeSeriesValueOutputBuilder::release_instance(time_series_output_ptr item) const {
        OutputBuilder::release_instance(item);
        auto ts = dynamic_cast<TimeSeriesValueOutput *>(item);
        if (ts == nullptr) {
            throw std::runtime_error("TimeSeriesValueOutputBuilder::release_instance: expected TimeSeriesValueOutput but got different type");
        }
        // No need to reset value - it's handled by AnyValue destructor
    }

    size_t TimeSeriesValueOutputBuilder::memory_size() const {
        return add_canary_size(sizeof(TimeSeriesValueOutput));
    }

    // Helper classes for backward compatibility with Python bindings
    struct OutputBuilder_TS_Bool : TimeSeriesValueOutputBuilder {
        OutputBuilder_TS_Bool() : TimeSeriesValueOutputBuilder(typeid(bool)) {}
    };
    struct OutputBuilder_TS_Int : TimeSeriesValueOutputBuilder {
        OutputBuilder_TS_Int() : TimeSeriesValueOutputBuilder(typeid(int64_t)) {}
    };
    struct OutputBuilder_TS_Float : TimeSeriesValueOutputBuilder {
        OutputBuilder_TS_Float() : TimeSeriesValueOutputBuilder(typeid(double)) {}
    };
    struct OutputBuilder_TS_Date : TimeSeriesValueOutputBuilder {
        OutputBuilder_TS_Date() : TimeSeriesValueOutputBuilder(typeid(engine_date_t)) {}
    };
    struct OutputBuilder_TS_DateTime : TimeSeriesValueOutputBuilder {
        OutputBuilder_TS_DateTime() : TimeSeriesValueOutputBuilder(typeid(engine_time_t)) {}
    };
    struct OutputBuilder_TS_TimeDelta : TimeSeriesValueOutputBuilder {
        OutputBuilder_TS_TimeDelta() : TimeSeriesValueOutputBuilder(typeid(engine_time_delta_t)) {}
    };
    struct OutputBuilder_TS_Object : TimeSeriesValueOutputBuilder {
        OutputBuilder_TS_Object() : TimeSeriesValueOutputBuilder(typeid(nb::object)) {}
    };

    void time_series_value_output_builder_register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesValueOutputBuilder, OutputBuilder>(m, "TimeSeriesValueOutputBuilder");
        nb::class_<OutputBuilder_TS_Bool, TimeSeriesValueOutputBuilder>(m, "OutputBuilder_TS_Bool").def(nb::init<>());
        nb::class_<OutputBuilder_TS_Int, TimeSeriesValueOutputBuilder>(m, "OutputBuilder_TS_Int").def(nb::init<>());
        nb::class_<OutputBuilder_TS_Float, TimeSeriesValueOutputBuilder>(m, "OutputBuilder_TS_Float").def(nb::init<>());
        nb::class_<OutputBuilder_TS_Date, TimeSeriesValueOutputBuilder>(m, "OutputBuilder_TS_Date").def(nb::init<>());
        nb::class_<OutputBuilder_TS_DateTime, TimeSeriesValueOutputBuilder>(m, "OutputBuilder_TS_DateTime").def(nb::init<>());
        nb::class_<OutputBuilder_TS_TimeDelta, TimeSeriesValueOutputBuilder>(m, "OutputBuilder_TS_TimeDelta").def(nb::init<>());
        nb::class_<OutputBuilder_TS_Object, TimeSeriesValueOutputBuilder>(m, "OutputBuilder_TS_Object").def(nb::init<>());
    }
} // namespace hgraph
