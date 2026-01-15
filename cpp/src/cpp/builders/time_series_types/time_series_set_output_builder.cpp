#include <hgraph/builders/time_series_types/time_series_set_output_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/tss.h>
#include <hgraph/util/arena_enable_shared_from_this.h>

namespace hgraph {

    TimeSeriesSetOutputBuilder::TimeSeriesSetOutputBuilder(const value::TypeMeta* element_type)
        : OutputBuilder(), _element_type(element_type) {}

    size_t TimeSeriesSetOutputBuilder::memory_size() const {
        return add_canary_size(sizeof(TimeSeriesSetOutput));
    }

    size_t TimeSeriesSetOutputBuilder::type_alignment() const {
        return alignof(TimeSeriesSetOutput);
    }

    void time_series_set_output_builder_register_with_nanobind(nb::module_& m) {
        nb::class_<TimeSeriesSetOutputBuilder, OutputBuilder>(m, "OutputBuilder_TSS")
            .def(nb::init<const value::TypeMeta*>(), "element_type"_a);
    }

} // namespace hgraph
