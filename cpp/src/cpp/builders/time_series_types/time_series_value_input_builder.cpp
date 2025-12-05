#include <hgraph/builders/time_series_types/time_series_value_input_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ts.h>
#include <hgraph/util/arena_enable_shared_from_this.h>

namespace hgraph {
    TimeSeriesValueInputBuilder::TimeSeriesValueInputBuilder(const std::type_info& value_type)
        : _value_type(value_type) {}

    time_series_input_s_ptr TimeSeriesValueInputBuilder::make_instance(node_ptr owning_node) const {
        return arena_make_shared_as<TimeSeriesValueInput, TimeSeriesInput>(owning_node, _value_type);
    }

    time_series_input_s_ptr TimeSeriesValueInputBuilder::make_instance(time_series_input_ptr owning_input) const {
        return arena_make_shared_as<TimeSeriesValueInput, TimeSeriesInput>(owning_input, _value_type);
    }

    size_t TimeSeriesValueInputBuilder::memory_size() const {
        return add_canary_size(sizeof(TimeSeriesValueInput));
    }

    // Helper classes for backward compatibility with Python bindings
    // These are thin wrappers around TimeSeriesValueInputBuilder with specific types
    struct InputBuilder_TS_Bool : TimeSeriesValueInputBuilder {
        InputBuilder_TS_Bool() : TimeSeriesValueInputBuilder(typeid(bool)) {}
    };
    struct InputBuilder_TS_Int : TimeSeriesValueInputBuilder {
        InputBuilder_TS_Int() : TimeSeriesValueInputBuilder(typeid(int64_t)) {}
    };
    struct InputBuilder_TS_Float : TimeSeriesValueInputBuilder {
        InputBuilder_TS_Float() : TimeSeriesValueInputBuilder(typeid(double)) {}
    };
    struct InputBuilder_TS_Date : TimeSeriesValueInputBuilder {
        InputBuilder_TS_Date() : TimeSeriesValueInputBuilder(typeid(engine_date_t)) {}
    };
    struct InputBuilder_TS_DateTime : TimeSeriesValueInputBuilder {
        InputBuilder_TS_DateTime() : TimeSeriesValueInputBuilder(typeid(engine_time_t)) {}
    };
    struct InputBuilder_TS_TimeDelta : TimeSeriesValueInputBuilder {
        InputBuilder_TS_TimeDelta() : TimeSeriesValueInputBuilder(typeid(engine_time_delta_t)) {}
    };
    struct InputBuilder_TS_Object : TimeSeriesValueInputBuilder {
        InputBuilder_TS_Object() : TimeSeriesValueInputBuilder(typeid(nb::object)) {}
    };

    void time_series_value_input_builder_register_with_nanobind(nb::module_ &m) {
        // Register the base builder class
        nb::class_<TimeSeriesValueInputBuilder, InputBuilder>(m, "TimeSeriesValueInputBuilder");

        // Register backward-compatible builder classes with default constructors
        // These match the old templated class names expected by Python code
        nb::class_<InputBuilder_TS_Bool, TimeSeriesValueInputBuilder>(m, "InputBuilder_TS_Bool").def(nb::init<>());
        nb::class_<InputBuilder_TS_Int, TimeSeriesValueInputBuilder>(m, "InputBuilder_TS_Int").def(nb::init<>());
        nb::class_<InputBuilder_TS_Float, TimeSeriesValueInputBuilder>(m, "InputBuilder_TS_Float").def(nb::init<>());
        nb::class_<InputBuilder_TS_Date, TimeSeriesValueInputBuilder>(m, "InputBuilder_TS_Date").def(nb::init<>());
        nb::class_<InputBuilder_TS_DateTime, TimeSeriesValueInputBuilder>(m, "InputBuilder_TS_DateTime").def(nb::init<>());
        nb::class_<InputBuilder_TS_TimeDelta, TimeSeriesValueInputBuilder>(m, "InputBuilder_TS_TimeDelta").def(nb::init<>());
        nb::class_<InputBuilder_TS_Object, TimeSeriesValueInputBuilder>(m, "InputBuilder_TS_Object").def(nb::init<>());
    }
} // namespace hgraph
