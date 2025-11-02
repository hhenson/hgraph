#include <hgraph/builders/time_series_types/time_series_dict_output_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/tsd.h>

#include <utility>

namespace hgraph {
    TimeSeriesDictOutputBuilder::TimeSeriesDictOutputBuilder(output_builder_ptr ts_builder,
                                                             output_builder_ptr ts_ref_builder)
        : OutputBuilder(), ts_builder{std::move(ts_builder)}, ts_ref_builder{std::move(ts_ref_builder)} {
    }

    template<typename T>
    time_series_output_ptr TimeSeriesDictOutputBuilder_T<T>::make_instance(node_ptr owning_node) const {
        auto v{new TimeSeriesDictOutput_T<T>(owning_node, ts_builder, ts_ref_builder)};
        return v;
    }

    template<typename T>
    time_series_output_ptr TimeSeriesDictOutputBuilder_T<T>::make_instance(time_series_output_ptr owning_output) const {
        auto parent_ts = dynamic_cast_ref<TimeSeriesType>(owning_output);
        auto v{new TimeSeriesDictOutput_T<T>{parent_ts, ts_builder, ts_ref_builder}};
        return v;
    }

    template<typename T>
    bool TimeSeriesDictOutputBuilder_T<T>::is_same_type(const Builder &other) const {
        if (auto other_b = dynamic_cast<const TimeSeriesDictOutputBuilder_T<T> *>(&other)) {
            return ts_builder->is_same_type(*other_b->ts_builder);
        }
        return false;
    }

    template<typename T>
    void TimeSeriesDictOutputBuilder_T<T>::release_instance(time_series_output_ptr item) const {
        if (auto dict = dynamic_cast<TimeSeriesDictOutput_T<T> *>(item.get())) { dict->_dispose(); }
        OutputBuilder::release_instance(item);
    }

    void time_series_dict_output_builder_register_with_nanobind(nb::module_ &m) {
        nb::class_ < TimeSeriesDictOutputBuilder, OutputBuilder > (m, "OutputBuilder_TSD");

        nb::class_<TimeSeriesDictOutputBuilder_T<bool>, TimeSeriesDictOutputBuilder>(m, "OutputBuilder_TSD_Bool")
                .def(nb::init<output_builder_ptr, output_builder_ptr>(), "ts_builder"_a, "ts_ref_builder"_a);
        nb::class_<TimeSeriesDictOutputBuilder_T<int64_t>, TimeSeriesDictOutputBuilder>(m, "OutputBuilder_TSD_Int")
                .def(nb::init<output_builder_ptr, output_builder_ptr>(), "ts_builder"_a, "ts_ref_builder"_a);
        nb::class_<TimeSeriesDictOutputBuilder_T<double>, TimeSeriesDictOutputBuilder>(m, "OutputBuilder_TSD_Float")
                .def(nb::init<output_builder_ptr, output_builder_ptr>(), "ts_builder"_a, "ts_ref_builder"_a);
        nb::class_<TimeSeriesDictOutputBuilder_T<engine_date_t>, TimeSeriesDictOutputBuilder>(
                    m, "OutputBuilder_TSD_Date")
                .def(nb::init<output_builder_ptr, output_builder_ptr>(), "ts_builder"_a, "ts_ref_builder"_a);
        nb::class_<TimeSeriesDictOutputBuilder_T<engine_time_t>, TimeSeriesDictOutputBuilder>(
                    m, "OutputBuilder_TSD_DateTime")
                .def(nb::init<output_builder_ptr, output_builder_ptr>(), "ts_builder"_a, "ts_ref_builder"_a);
        nb::class_<TimeSeriesDictOutputBuilder_T<engine_time_delta_t>, TimeSeriesDictOutputBuilder>(m,
                    "OutputBuilder_TSD_TimeDelta")
                .def(nb::init<output_builder_ptr, output_builder_ptr>(), "ts_builder"_a, "ts_ref_builder"_a);
        nb::class_<TimeSeriesDictOutputBuilder_T<nb::object>, TimeSeriesDictOutputBuilder>(
                    m, "OutputBuilder_TSD_Object")
                .def(nb::init<output_builder_ptr, output_builder_ptr>(), "ts_builder"_a, "ts_ref_builder"_a);
    }

    // Template instantiations
    template struct TimeSeriesDictOutputBuilder_T<bool>;
    template struct TimeSeriesDictOutputBuilder_T<int64_t>;
    template struct TimeSeriesDictOutputBuilder_T<double>;
    template struct TimeSeriesDictOutputBuilder_T<engine_date_t>;
    template struct TimeSeriesDictOutputBuilder_T<engine_time_t>;
    template struct TimeSeriesDictOutputBuilder_T<engine_time_delta_t>;
    template struct TimeSeriesDictOutputBuilder_T<nb::object>;
} // namespace hgraph