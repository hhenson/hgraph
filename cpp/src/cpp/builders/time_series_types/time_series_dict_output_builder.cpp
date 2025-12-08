#include <hgraph/builders/time_series_types/time_series_dict_output_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/tsd.h>
#include <hgraph/util/arena_enable_shared_from_this.h>

#include <utility>

namespace hgraph {
    TimeSeriesDictOutputBuilder::TimeSeriesDictOutputBuilder(output_builder_s_ptr ts_builder,
                                                             output_builder_s_ptr ts_ref_builder)
        : OutputBuilder(), ts_builder{std::move(ts_builder)}, ts_ref_builder{std::move(ts_ref_builder)} {
    }

    template<typename T>
    time_series_output_s_ptr TimeSeriesDictOutputBuilder_T<T>::make_instance(node_ptr owning_node) const {
        return arena_make_shared_as<TimeSeriesDictOutput_T<T>, TimeSeriesOutput>(owning_node, ts_builder, ts_ref_builder);
    }

    template<typename T>
    time_series_output_s_ptr TimeSeriesDictOutputBuilder_T<T>::make_instance(time_series_output_ptr owning_output) const {
        return arena_make_shared_as<TimeSeriesDictOutput_T<T>, TimeSeriesOutput>(owning_output, ts_builder, ts_ref_builder);
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
        auto dict = dynamic_cast<TimeSeriesDictOutput_T<T> *>(item);
        if (dict == nullptr) {
            throw std::runtime_error("TimeSeriesDictOutputBuilder_T::release_instance: expected TimeSeriesDictOutput_T but got different type");
        }
        dict->_dispose();
        OutputBuilder::release_instance(item);
    }

    template<typename T>
    size_t TimeSeriesDictOutputBuilder_T<T>::memory_size() const {
        return add_canary_size(sizeof(TimeSeriesDictOutput_T<T>));
    }

    void time_series_dict_output_builder_register_with_nanobind(nb::module_ &m) {
        nb::class_ < TimeSeriesDictOutputBuilder, OutputBuilder > (m, "OutputBuilder_TSD");

        nb::class_<TimeSeriesDictOutputBuilder_T<bool>, TimeSeriesDictOutputBuilder>(m, "OutputBuilder_TSD_Bool")
                .def(nb::init<output_builder_s_ptr, output_builder_s_ptr>(), "ts_builder"_a, "ts_ref_builder"_a);
        nb::class_<TimeSeriesDictOutputBuilder_T<int64_t>, TimeSeriesDictOutputBuilder>(m, "OutputBuilder_TSD_Int")
                .def(nb::init<output_builder_s_ptr, output_builder_s_ptr>(), "ts_builder"_a, "ts_ref_builder"_a);
        nb::class_<TimeSeriesDictOutputBuilder_T<double>, TimeSeriesDictOutputBuilder>(m, "OutputBuilder_TSD_Float")
                .def(nb::init<output_builder_s_ptr, output_builder_s_ptr>(), "ts_builder"_a, "ts_ref_builder"_a);
        nb::class_<TimeSeriesDictOutputBuilder_T<engine_date_t>, TimeSeriesDictOutputBuilder>(
                    m, "OutputBuilder_TSD_Date")
                .def(nb::init<output_builder_s_ptr, output_builder_s_ptr>(), "ts_builder"_a, "ts_ref_builder"_a);
        nb::class_<TimeSeriesDictOutputBuilder_T<engine_time_t>, TimeSeriesDictOutputBuilder>(
                    m, "OutputBuilder_TSD_DateTime")
                .def(nb::init<output_builder_s_ptr, output_builder_s_ptr>(), "ts_builder"_a, "ts_ref_builder"_a);
        nb::class_<TimeSeriesDictOutputBuilder_T<engine_time_delta_t>, TimeSeriesDictOutputBuilder>(m,
                    "OutputBuilder_TSD_TimeDelta")
                .def(nb::init<output_builder_s_ptr, output_builder_s_ptr>(), "ts_builder"_a, "ts_ref_builder"_a);
        nb::class_<TimeSeriesDictOutputBuilder_T<nb::object>, TimeSeriesDictOutputBuilder>(
                    m, "OutputBuilder_TSD_Object")
                .def(nb::init<output_builder_s_ptr, output_builder_s_ptr>(), "ts_builder"_a, "ts_ref_builder"_a);
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