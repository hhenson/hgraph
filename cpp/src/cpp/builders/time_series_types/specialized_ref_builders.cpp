#include <hgraph/builders/time_series_types/specialized_ref_builders.h>
#include <hgraph/builders/builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/types/tsb.h>  // For TimeSeriesSchema definition
#include <hgraph/util/arena_enable_shared_from_this.h>

namespace hgraph
{
    // ============================================================
    // Specialized Reference Input Builders
    // ============================================================

    // TimeSeriesValueRefInputBuilder - REF[TS[...]]
    time_series_input_s_ptr TimeSeriesValueRefInputBuilder::make_instance(node_ptr owning_node) const {
        return arena_make_shared_as<TimeSeriesValueReferenceInput, TimeSeriesInput>(owning_node);
    }

    time_series_input_s_ptr TimeSeriesValueRefInputBuilder::make_instance(time_series_input_ptr owning_input) const {
        return arena_make_shared_as<TimeSeriesValueReferenceInput, TimeSeriesInput>(owning_input);
    }

    size_t TimeSeriesValueRefInputBuilder::memory_size() const {
        return add_canary_size(sizeof(TimeSeriesValueReferenceInput));
    }

    void TimeSeriesValueRefInputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesValueRefInputBuilder, InputBuilder>(m, "InputBuilder_TS_Value_Ref").def(nb::init<>());
    }

    // TimeSeriesListRefInputBuilder - REF[TSL[...]]
    TimeSeriesListRefInputBuilder::TimeSeriesListRefInputBuilder(InputBuilder::ptr value_builder, size_t size)
        : value_builder(std::move(value_builder)), size(size) {}

    time_series_input_s_ptr TimeSeriesListRefInputBuilder::make_instance(node_ptr owning_node) const {
        return arena_make_shared_as<TimeSeriesListReferenceInput, TimeSeriesInput>(owning_node, value_builder, size);
    }

    time_series_input_s_ptr TimeSeriesListRefInputBuilder::make_instance(time_series_input_ptr owning_input) const {
        return arena_make_shared_as<TimeSeriesListReferenceInput, TimeSeriesInput>(owning_input, value_builder, size);
    }

    bool TimeSeriesListRefInputBuilder::is_same_type(const Builder &other) const {
        if (typeid(*this) != typeid(other)) return false;
        auto &other_builder = dynamic_cast<const TimeSeriesListRefInputBuilder &>(other);
        return size == other_builder.size && value_builder->is_same_type(*other_builder.value_builder);
    }

    size_t TimeSeriesListRefInputBuilder::memory_size() const {
        // Add canary size to the base list reference object
        size_t total = add_canary_size(sizeof(TimeSeriesListReferenceInput));
        // For each element, align and add its size
        for (size_t i = 0; i < size; ++i) {
            total = align_size(total, alignof(TimeSeriesType));
            total += value_builder->memory_size();
        }
        return total;
    }

    void TimeSeriesListRefInputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesListRefInputBuilder, InputBuilder>(m, "InputBuilder_TSL_Ref")
            .def(nb::init<InputBuilder::ptr, size_t>(), "value_builder"_a, "size"_a);
    }

    // TimeSeriesBundleRefInputBuilder - REF[TSB[...]]
    TimeSeriesBundleRefInputBuilder::TimeSeriesBundleRefInputBuilder(time_series_schema_ptr         schema,
                                                                     std::vector<InputBuilder::ptr> field_builders)
        : schema(std::move(schema)), field_builders(std::move(field_builders)) {}

    time_series_input_s_ptr TimeSeriesBundleRefInputBuilder::make_instance(node_ptr owning_node) const {
        return arena_make_shared_as<TimeSeriesBundleReferenceInput, TimeSeriesInput>(owning_node, field_builders, field_builders.size());
    }

    time_series_input_s_ptr TimeSeriesBundleRefInputBuilder::make_instance(time_series_input_ptr owning_input) const {
        return arena_make_shared_as<TimeSeriesBundleReferenceInput, TimeSeriesInput>(owning_input, field_builders,
                                                  field_builders.size());
    }

    bool TimeSeriesBundleRefInputBuilder::is_same_type(const Builder &other) const {
        if (typeid(*this) != typeid(other)) return false;
        auto &other_builder = dynamic_cast<const TimeSeriesBundleRefInputBuilder &>(other);
        if (field_builders.size() != other_builder.field_builders.size()) return false;
        for (size_t i = 0; i < field_builders.size(); ++i) {
            if (!field_builders[i]->is_same_type(*other_builder.field_builders[i])) return false;
        }
        return true;
    }

    size_t TimeSeriesBundleRefInputBuilder::memory_size() const {
        // Add canary size to the base bundle reference object
        size_t total = add_canary_size(sizeof(TimeSeriesBundleReferenceInput));
        // Align before each nested time-series input
        for (const auto &builder : field_builders) {
            total = align_size(total, alignof(TimeSeriesType));
            total += builder->memory_size();
        }
        return total;
    }

    void TimeSeriesBundleRefInputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesBundleRefInputBuilder, InputBuilder>(m, "InputBuilder_TSB_Ref")
            .def(nb::init<TimeSeriesSchema::ptr, std::vector<InputBuilder::ptr>>(), "schema"_a, "field_builders"_a);
    }

    // TimeSeriesDictRefInputBuilder - REF[TSD[...]]
    time_series_input_s_ptr TimeSeriesDictRefInputBuilder::make_instance(node_ptr owning_node) const {
        return arena_make_shared_as<TimeSeriesDictReferenceInput, TimeSeriesInput>(owning_node);
    }

    time_series_input_s_ptr TimeSeriesDictRefInputBuilder::make_instance(time_series_input_ptr owning_input) const {
        return arena_make_shared_as<TimeSeriesDictReferenceInput, TimeSeriesInput>(owning_input);
    }

    size_t TimeSeriesDictRefInputBuilder::memory_size() const {
        return add_canary_size(sizeof(TimeSeriesDictReferenceInput));
    }

    void TimeSeriesDictRefInputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesDictRefInputBuilder, InputBuilder>(m, "InputBuilder_TSD_Ref").def(nb::init<>());
    }

    // TimeSeriesSetRefInputBuilder - REF[TSS[...]]
    time_series_input_s_ptr TimeSeriesSetRefInputBuilder::make_instance(node_ptr owning_node) const {
        return arena_make_shared_as<TimeSeriesSetReferenceInput, TimeSeriesInput>(owning_node);
    }

    time_series_input_s_ptr TimeSeriesSetRefInputBuilder::make_instance(time_series_input_ptr owning_input) const {
        return arena_make_shared_as<TimeSeriesSetReferenceInput, TimeSeriesInput>(owning_input);
    }

    size_t TimeSeriesSetRefInputBuilder::memory_size() const {
        return add_canary_size(sizeof(TimeSeriesSetReferenceInput));
    }

    void TimeSeriesSetRefInputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesSetRefInputBuilder, InputBuilder>(m, "InputBuilder_TSS_Ref").def(nb::init<>());
    }

    // TimeSeriesWindowRefInputBuilder - REF[TSW[...]]
    time_series_input_s_ptr TimeSeriesWindowRefInputBuilder::make_instance(node_ptr owning_node) const {
        return arena_make_shared_as<TimeSeriesWindowReferenceInput, TimeSeriesInput>(owning_node);
    }

    time_series_input_s_ptr TimeSeriesWindowRefInputBuilder::make_instance(time_series_input_ptr owning_input) const {
        return arena_make_shared_as<TimeSeriesWindowReferenceInput, TimeSeriesInput>(owning_input);
    }

    size_t TimeSeriesWindowRefInputBuilder::memory_size() const {
        return add_canary_size(sizeof(TimeSeriesWindowReferenceInput));
    }

    void TimeSeriesWindowRefInputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesWindowRefInputBuilder, InputBuilder>(m, "InputBuilder_TSW_Ref").def(nb::init<>());
    }

    // ============================================================
    // Specialized Reference Output Builders
    // ============================================================

    // TimeSeriesValueRefOutputBuilder - REF[TS[...]]
    time_series_output_s_ptr TimeSeriesValueRefOutputBuilder::make_instance(node_ptr owning_node) const {
        return arena_make_shared_as<TimeSeriesValueReferenceOutput, TimeSeriesOutput>(owning_node);
    }

    time_series_output_s_ptr TimeSeriesValueRefOutputBuilder::make_instance(time_series_output_ptr owning_output) const {
        return arena_make_shared_as<TimeSeriesValueReferenceOutput, TimeSeriesOutput>(owning_output);
    }

    size_t TimeSeriesValueRefOutputBuilder::memory_size() const {
        return add_canary_size(sizeof(TimeSeriesValueReferenceOutput));
    }

    void TimeSeriesValueRefOutputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesValueRefOutputBuilder, OutputBuilder>(m, "OutputBuilder_TS_Value_Ref").def(nb::init<>());
    }

    // TimeSeriesListRefOutputBuilder - REF[TSL[...]]
    TimeSeriesListRefOutputBuilder::TimeSeriesListRefOutputBuilder(OutputBuilder::ptr value_builder, size_t size)
        : value_builder(std::move(value_builder)), size(size) {}

    time_series_output_s_ptr TimeSeriesListRefOutputBuilder::make_instance(node_ptr owning_node) const {
        return arena_make_shared_as<TimeSeriesListReferenceOutput, TimeSeriesOutput>(owning_node, value_builder, size);
    }

    time_series_output_s_ptr TimeSeriesListRefOutputBuilder::make_instance(time_series_output_ptr owning_output) const {
        return arena_make_shared_as<TimeSeriesListReferenceOutput, TimeSeriesOutput>(owning_output, value_builder, size);
    }

    bool TimeSeriesListRefOutputBuilder::is_same_type(const Builder &other) const {
        if (typeid(*this) != typeid(other)) return false;
        auto &other_builder = dynamic_cast<const TimeSeriesListRefOutputBuilder &>(other);
        return size == other_builder.size && value_builder->is_same_type(*other_builder.value_builder);
    }

    size_t TimeSeriesListRefOutputBuilder::memory_size() const {
        // Add canary size to the base list reference object
        size_t total = add_canary_size(sizeof(TimeSeriesListReferenceOutput));
        // For each element, align and add its size
        for (size_t i = 0; i < size; ++i) {
            total = align_size(total, alignof(TimeSeriesType));
            total += value_builder->memory_size();
        }
        return total;
    }

    void TimeSeriesListRefOutputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesListRefOutputBuilder, OutputBuilder>(m, "OutputBuilder_TSL_Ref")
            .def(nb::init<OutputBuilder::ptr, size_t>(), "value_builder"_a, "size"_a);
    }

    // TimeSeriesBundleRefOutputBuilder - REF[TSB[...]]
    TimeSeriesBundleRefOutputBuilder::TimeSeriesBundleRefOutputBuilder(time_series_schema_ptr          schema,
                                                                       std::vector<OutputBuilder::ptr> field_builders)
        : schema(std::move(schema)), field_builders(std::move(field_builders)) {}

    time_series_output_s_ptr TimeSeriesBundleRefOutputBuilder::make_instance(node_ptr owning_node) const {
        return arena_make_shared_as<TimeSeriesBundleReferenceOutput, TimeSeriesOutput>(owning_node, field_builders, field_builders.size());
    }

    time_series_output_s_ptr TimeSeriesBundleRefOutputBuilder::make_instance(time_series_output_ptr owning_output) const {
        return arena_make_shared_as<TimeSeriesBundleReferenceOutput, TimeSeriesOutput>(owning_output, field_builders,
                                                   field_builders.size());
    }

    bool TimeSeriesBundleRefOutputBuilder::is_same_type(const Builder &other) const {
        if (typeid(*this) != typeid(other)) return false;
        auto &other_builder = dynamic_cast<const TimeSeriesBundleRefOutputBuilder &>(other);
        if (field_builders.size() != other_builder.field_builders.size()) return false;
        for (size_t i = 0; i < field_builders.size(); ++i) {
            if (!field_builders[i]->is_same_type(*other_builder.field_builders[i])) return false;
        }
        return true;
    }

    size_t TimeSeriesBundleRefOutputBuilder::memory_size() const {
        // Add canary size to the base bundle reference object
        size_t total = add_canary_size(sizeof(TimeSeriesBundleReferenceOutput));
        // Align before each nested time-series output
        for (const auto &builder : field_builders) {
            total = align_size(total, alignof(TimeSeriesType));
            total += builder->memory_size();
        }
        return total;
    }

    void TimeSeriesBundleRefOutputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesBundleRefOutputBuilder, OutputBuilder>(m, "OutputBuilder_TSB_Ref")
            .def(nb::init<TimeSeriesSchema::ptr, std::vector<OutputBuilder::ptr>>(), "schema"_a, "field_builders"_a);
    }

    // TimeSeriesDictRefOutputBuilder - REF[TSD[...]]
    time_series_output_s_ptr TimeSeriesDictRefOutputBuilder::make_instance(node_ptr owning_node) const {
        return arena_make_shared_as<TimeSeriesDictReferenceOutput, TimeSeriesOutput>(owning_node);
    }

    time_series_output_s_ptr TimeSeriesDictRefOutputBuilder::make_instance(time_series_output_ptr owning_output) const {
        return arena_make_shared_as<TimeSeriesDictReferenceOutput, TimeSeriesOutput>(owning_output);
    }

    size_t TimeSeriesDictRefOutputBuilder::memory_size() const {
        return add_canary_size(sizeof(TimeSeriesDictReferenceOutput));
    }

    void TimeSeriesDictRefOutputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesDictRefOutputBuilder, OutputBuilder>(m, "OutputBuilder_TSD_Ref").def(nb::init<>());
    }

    // TimeSeriesSetRefOutputBuilder - REF[TSS[...]]
    time_series_output_s_ptr TimeSeriesSetRefOutputBuilder::make_instance(node_ptr owning_node) const {
        return arena_make_shared_as<TimeSeriesSetReferenceOutput, TimeSeriesOutput>(owning_node);
    }

    time_series_output_s_ptr TimeSeriesSetRefOutputBuilder::make_instance(time_series_output_ptr owning_output) const {
        return arena_make_shared_as<TimeSeriesSetReferenceOutput, TimeSeriesOutput>(owning_output);
    }

    size_t TimeSeriesSetRefOutputBuilder::memory_size() const {
        return add_canary_size(sizeof(TimeSeriesSetReferenceOutput));
    }

    void TimeSeriesSetRefOutputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesSetRefOutputBuilder, OutputBuilder>(m, "OutputBuilder_TSS_Ref").def(nb::init<>());
    }

    // TimeSeriesWindowRefOutputBuilder - REF[TSW[...]]
    time_series_output_s_ptr TimeSeriesWindowRefOutputBuilder::make_instance(node_ptr owning_node) const {
        return arena_make_shared_as<TimeSeriesWindowReferenceOutput, TimeSeriesOutput>(owning_node);
    }

    time_series_output_s_ptr TimeSeriesWindowRefOutputBuilder::make_instance(time_series_output_ptr owning_output) const {
        return arena_make_shared_as<TimeSeriesWindowReferenceOutput, TimeSeriesOutput>(owning_output);
    }

    size_t TimeSeriesWindowRefOutputBuilder::memory_size() const {
        return add_canary_size(sizeof(TimeSeriesWindowReferenceOutput));
    }

    void TimeSeriesWindowRefOutputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesWindowRefOutputBuilder, OutputBuilder>(m, "OutputBuilder_TSW_Ref").def(nb::init<>());
    }

}  // namespace hgraph
