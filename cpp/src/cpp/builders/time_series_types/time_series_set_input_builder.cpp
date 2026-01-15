#include <hgraph/builders/time_series_types/time_series_set_input_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/tss.h>
#include <hgraph/util/arena_enable_shared_from_this.h>

namespace hgraph {

    TimeSeriesSetInputBuilder::TimeSeriesSetInputBuilder(const value::TypeMeta* element_type)
        : InputBuilder(), _element_type(element_type) {}

    size_t TimeSeriesSetInputBuilder::memory_size() const {
        return add_canary_size(sizeof(TimeSeriesSetInput));
    }

    size_t TimeSeriesSetInputBuilder::type_alignment() const {
        return alignof(TimeSeriesSetInput);
    }

    void time_series_set_input_builder_register_with_nanobind(nb::module_& m) {
        nb::class_<TimeSeriesSetInputBuilder, InputBuilder>(m, "InputBuilder_TSS")
            .def(nb::init<const value::TypeMeta*>(), "element_type"_a = nullptr);
    }

} // namespace hgraph
