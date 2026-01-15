#include <hgraph/builders/time_series_types/time_series_signal_input_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ts_signal.h>
#include <hgraph/util/arena_enable_shared_from_this.h>

namespace hgraph {
    size_t TimeSeriesSignalInputBuilder::memory_size() const {
        return add_canary_size(sizeof(TimeSeriesSignalInput));
    }

    size_t TimeSeriesSignalInputBuilder::type_alignment() const {
        return alignof(TimeSeriesSignalInput);
    }

    void TimeSeriesSignalInputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesSignalInputBuilder, InputBuilder>(m, "InputBuilder_TS_Signal").def(nb::init<>());
    }
} // namespace hgraph
