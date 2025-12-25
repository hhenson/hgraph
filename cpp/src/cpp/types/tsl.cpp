#include <hgraph/types/node.h>
#include <hgraph/types/tsl.h>

namespace hgraph {
    template<typename T_TS>
        requires IndexedTimeSeriesT<T_TS>
    nb::object TimeSeriesList<T_TS>::py_value() const {
        nb::list result;
        for (const auto &ts: this->ts_values()) {
            if (ts->valid()) {
                result.append(ts->py_value());
            } else {
                result.append(nb::none());
            }
        }
        return nb::tuple(result);
    }

    template<typename T_TS>
        requires IndexedTimeSeriesT<T_TS>
    nb::object TimeSeriesList<T_TS>::py_delta_value() const {
        nb::dict result;
        for (auto &[ndx, ts]: modified_items()) { result[nb::cast(ndx)] = ts->py_delta_value(); }
        return result;
    }

    template<typename T_TS>
        requires IndexedTimeSeriesT<T_TS>
    typename TimeSeriesList<T_TS>::index_collection_type TimeSeriesList<T_TS>::keys() const {
        index_collection_type result;
        result.reserve(size());
        for (size_t i = 0; i < size(); ++i) { result.push_back(i); }
        return result;
    }

    template<typename T_TS>
        requires IndexedTimeSeriesT<T_TS>
    typename TimeSeriesList<T_TS>::index_collection_type TimeSeriesList<T_TS>::valid_keys() const {
        return index_with_constraint([](const ts_type &ts) { return ts.valid(); });
    }

    template<typename T_TS>
        requires IndexedTimeSeriesT<T_TS>
    typename TimeSeriesList<T_TS>::index_collection_type TimeSeriesList<T_TS>::modified_keys() const {
        return index_with_constraint([](const ts_type &ts) { return ts.modified(); });
    }

    template<typename T_TS>
        requires IndexedTimeSeriesT<T_TS>
    typename TimeSeriesList<T_TS>::enumerated_collection_type TimeSeriesList<T_TS>::items() {
        enumerated_collection_type result;
        result.reserve(size());
        for (size_t i = 0; i < size(); ++i) { result.push_back({i, ts_values()[i]}); }
        return result;
    }

    template<typename T_TS>
        requires IndexedTimeSeriesT<T_TS>
    typename TimeSeriesList<T_TS>::enumerated_collection_type TimeSeriesList<T_TS>::items() const {
        return const_cast<list_type *>(this)->items();
    }

    template<typename T_TS>
        requires IndexedTimeSeriesT<T_TS>
    typename TimeSeriesList<T_TS>::enumerated_collection_type TimeSeriesList<T_TS>::valid_items() {
        return this->items_with_constraint([](const ts_type &ts) { return ts.valid(); });
    }

    template<typename T_TS>
        requires IndexedTimeSeriesT<T_TS>
    typename TimeSeriesList<T_TS>::enumerated_collection_type TimeSeriesList<T_TS>::valid_items() const {
        return const_cast<list_type *>(this)->valid_items();
    }

    template<typename T_TS>
        requires IndexedTimeSeriesT<T_TS>
    typename TimeSeriesList<T_TS>::enumerated_collection_type TimeSeriesList<T_TS>::modified_items() {
        return this->items_with_constraint([](const ts_type &ts) { return ts.modified(); });
    }

    template<typename T_TS>
        requires IndexedTimeSeriesT<T_TS>
    typename TimeSeriesList<T_TS>::enumerated_collection_type TimeSeriesList<T_TS>::modified_items() const {
        return const_cast<list_type *>(this)->modified_items();
    }

    template<typename T_TS>
        requires IndexedTimeSeriesT<T_TS>
    bool TimeSeriesList<T_TS>::has_reference() const {
        if (size() == 0) { return false; } else { return ts_values()[0]->has_reference(); }
    }

    void TimeSeriesListOutput::apply_result(const nb::object& value) {
        if (value.is_none()) { return; }
        py_set_value(value);
    }

    bool TimeSeriesListOutput::is_same_type(const TimeSeriesType *other) const {
        auto other_list = dynamic_cast<const TimeSeriesListOutput *>(other);
        if (!other_list) { return false; }
        const auto this_size = this->size();
        const auto other_size = other_list->size();
        // Be permissive during wiring: if either list has no elements yet, treat as same type
        if (this_size == 0 || other_size == 0) { return true; }
        // Otherwise, compare the element type recursively without enforcing equal sizes
        return (*this)[0]->is_same_type((*other_list)[0].get());
    }

    void TimeSeriesListOutput::py_set_value(const nb::object& value) {
        if (value.is_none()) {
            mark_invalid();
            return;
        }
        if (nb::isinstance<nb::tuple>(value) || nb::isinstance<nb::list>(value)) {
            for (size_t i = 0, l = nb::len(value); i < l; ++i) {
                const auto &v{value[i]};
                if (v.is_valid() && !v.is_none()) { (*this)[i]->py_set_value(v); }
            }
        } else if (nb::isinstance<nb::dict>(value)) {
            for (auto [key, val]: nb::cast<nb::dict>(value)) {
                if (val.is_valid() && !val.is_none()) { (*this)[nb::cast<size_t>(key)]->py_set_value(nb::borrow(val)); }
            }
        } else {
            throw std::runtime_error("Invalid value type for TimeSeriesListOutput");
        }
    }

    bool TimeSeriesListInput::is_same_type(const TimeSeriesType *other) const {
        auto other_list = dynamic_cast<const TimeSeriesListInput *>(other);
        if (!other_list) { return false; }
        const auto this_size = this->size();
        const auto other_size = other_list->size();
        // Be permissive during wiring: if either list has no elements yet, consider types compatible
        if (this_size == 0 || other_size == 0) { return true; }
        // Otherwise compare element type recursively without enforcing size equality
        return (*this)[0]->is_same_type((*other_list)[0].get());
    }

    template struct TimeSeriesList<IndexedTimeSeriesInput>;
    template struct TimeSeriesList<IndexedTimeSeriesOutput>;
} // namespace hgraph