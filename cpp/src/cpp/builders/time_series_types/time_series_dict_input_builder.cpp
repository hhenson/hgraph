#include <hgraph/builders/time_series_types/time_series_dict_input_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/tsd.h>

#include <utility>

namespace hgraph {
    TimeSeriesDictInputBuilder::TimeSeriesDictInputBuilder(input_builder_ptr ts_builder)
        : InputBuilder(), ts_builder{std::move(ts_builder)} {
    }

    template<typename T>
    time_series_input_ptr TimeSeriesDictInputBuilder_T<T>::make_instance(node_ptr owning_node) const {
        auto v{new TimeSeriesDictInput_T<T>(owning_node, ts_builder)};
        return v;
    }

    template<typename T>
    time_series_input_ptr TimeSeriesDictInputBuilder_T<T>::make_instance(time_series_input_ptr owning_input) const {
        auto v{new TimeSeriesDictInput_T<T>{dynamic_cast_ref<TimeSeriesType>(owning_input), ts_builder}};
        return v;
    }

    template<typename T>
    bool TimeSeriesDictInputBuilder_T<T>::is_same_type(const Builder &other) const {
        if (auto other_b = dynamic_cast<const TimeSeriesDictInputBuilder_T<T> *>(&other)) {
            return ts_builder->is_same_type(*other_b->ts_builder);
        }
        return false;
    }

    template<typename T>
    void TimeSeriesDictInputBuilder_T<T>::release_instance(time_series_input_ptr item) const {
        InputBuilder::release_instance(item);
        auto dict = dynamic_cast<TimeSeriesDictInput_T<T> *>(item.get());
        if (dict == nullptr) { return; }
        for (auto &value: dict->_ts_values) { ts_builder->release_instance(value.second); }
    }

    void time_series_dict_input_builder_register_with_nanobind(nb::module_ &m) {
        nb::class_ < TimeSeriesDictInputBuilder, InputBuilder > (m, "InputBuilder_TSD")
                .def_ro("ts_builder", &TimeSeriesDictInputBuilder::ts_builder);

        nb::class_<TimeSeriesDictInputBuilder_T<bool>, TimeSeriesDictInputBuilder>(m, "InputBuilder_TSD_Bool")
                .def(nb::init<input_builder_ptr>(), "ts_builder"_a);
        nb::class_<TimeSeriesDictInputBuilder_T<int64_t>, TimeSeriesDictInputBuilder>(m, "InputBuilder_TSD_Int")
                .def(nb::init<input_builder_ptr>(), "ts_builder"_a);
        nb::class_<TimeSeriesDictInputBuilder_T<double>, TimeSeriesDictInputBuilder>(m, "InputBuilder_TSD_Float")
                .def(nb::init<input_builder_ptr>(), "ts_builder"_a);
        nb::class_<TimeSeriesDictInputBuilder_T<engine_date_t>, TimeSeriesDictInputBuilder>(m, "InputBuilder_TSD_Date")
                .def(nb::init<input_builder_ptr>(), "ts_builder"_a);
        nb::class_<TimeSeriesDictInputBuilder_T<engine_time_t>, TimeSeriesDictInputBuilder>(
                    m, "InputBuilder_TSD_DateTime")
                .def(nb::init<input_builder_ptr>(), "ts_builder"_a);
        nb::class_<TimeSeriesDictInputBuilder_T<engine_time_delta_t>, TimeSeriesDictInputBuilder>(
                    m, "InputBuilder_TSD_TimeDelta")
                .def(nb::init<input_builder_ptr>(), "ts_builder"_a);
        nb::class_<TimeSeriesDictInputBuilder_T<nb::object>, TimeSeriesDictInputBuilder>(m, "InputBuilder_TSD_Object")
                .def(nb::init<input_builder_ptr>(), "ts_builder"_a);
    }

    // Template instantiations
    template struct TimeSeriesDictInputBuilder_T<bool>;
    template struct TimeSeriesDictInputBuilder_T<int64_t>;
    template struct TimeSeriesDictInputBuilder_T<double>;
    template struct TimeSeriesDictInputBuilder_T<engine_date_t>;
    template struct TimeSeriesDictInputBuilder_T<engine_time_t>;
    template struct TimeSeriesDictInputBuilder_T<engine_time_delta_t>;
    template struct TimeSeriesDictInputBuilder_T<nb::object>;
} // namespace hgraph