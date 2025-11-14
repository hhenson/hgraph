//
// Specialized time series type implementations
//

#include <hgraph/api/python/py_ts_types.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/ts.h>
#include <hgraph/types/ts_signal.h>
#include <hgraph/types/ts_indexed.h>
#include <hgraph/types/tsl.h>
#include <hgraph/types/tsb.h>
#include <hgraph/types/tsd.h>
#include <hgraph/types/tss.h>
#include <hgraph/types/tsw.h>
#include <hgraph/types/ref.h>
#include <fmt/format.h>

namespace nb = nanobind;
using namespace nb::literals;

namespace hgraph::api {

    // ============================================================================
    // Helper template functions for TSD type-specific operations
    // ============================================================================
    
    template<typename T_Key>
    nb::list build_tsd_items_list(const std::unordered_map<T_Key, time_series_input_ptr>& items, control_block_ptr cb) {
        nb::list result;
        for (const auto& [key, value] : items) {
            nb::tuple item = nb::make_tuple(
                nb::cast(key),
                wrap_input(value.get(), cb)
            );
            result.append(item);
        }
        return result;
    }
    
    // Explicit instantiation for common key types matching those in tsd.cpp
    template nb::list build_tsd_items_list<bool>(const std::unordered_map<bool, time_series_input_ptr>&, control_block_ptr);
    template nb::list build_tsd_items_list<int64_t>(const std::unordered_map<int64_t, time_series_input_ptr>&, control_block_ptr);
    template nb::list build_tsd_items_list<double>(const std::unordered_map<double, time_series_input_ptr>&, control_block_ptr);
    template nb::list build_tsd_items_list<nb::object>(const std::unordered_map<nb::object, time_series_input_ptr>&, control_block_ptr);

    template<typename T_Key>
    nb::list build_tsd_input_values_list(const std::unordered_map<T_Key, time_series_input_ptr>& items, control_block_ptr cb) {
        nb::list result;
        for (const auto& [_, value] : items) {
            (void)_;
            result.append(wrap_input(value.get(), cb));
        }
        return result;
    }

    template nb::list build_tsd_input_values_list<bool>(const std::unordered_map<bool, time_series_input_ptr>&, control_block_ptr);
    template nb::list build_tsd_input_values_list<int64_t>(const std::unordered_map<int64_t, time_series_input_ptr>&, control_block_ptr);
    template nb::list build_tsd_input_values_list<double>(const std::unordered_map<double, time_series_input_ptr>&, control_block_ptr);
    template nb::list build_tsd_input_values_list<nb::object>(const std::unordered_map<nb::object, time_series_input_ptr>&, control_block_ptr);

    template<typename T_Key>
    nb::list build_tsd_output_items_list(const std::unordered_map<T_Key, time_series_output_ptr>& items, control_block_ptr cb) {
        nb::list result;
        for (const auto& [key, value] : items) {
            nb::tuple item = nb::make_tuple(
                nb::cast(key),
                wrap_output(value.get(), cb)
            );
            result.append(item);
        }
        return result;
    }

    template<typename T_Key>
    nb::list build_tsd_output_values_list(const std::unordered_map<T_Key, time_series_output_ptr>& items, control_block_ptr cb) {
        nb::list result;
        for (const auto& [_, value] : items) {
            (void)_;
            result.append(wrap_output(value.get(), cb));
        }
        return result;
    }

    template nb::list build_tsd_output_items_list<bool>(const std::unordered_map<bool, time_series_output_ptr>&, control_block_ptr);
    template nb::list build_tsd_output_items_list<int64_t>(const std::unordered_map<int64_t, time_series_output_ptr>&, control_block_ptr);
    template nb::list build_tsd_output_items_list<double>(const std::unordered_map<double, time_series_output_ptr>&, control_block_ptr);
    template nb::list build_tsd_output_items_list<nb::object>(const std::unordered_map<nb::object, time_series_output_ptr>&, control_block_ptr);

    template nb::list build_tsd_output_values_list<bool>(const std::unordered_map<bool, time_series_output_ptr>&, control_block_ptr);
    template nb::list build_tsd_output_values_list<int64_t>(const std::unordered_map<int64_t, time_series_output_ptr>&, control_block_ptr);
    template nb::list build_tsd_output_values_list<double>(const std::unordered_map<double, time_series_output_ptr>&, control_block_ptr);
    template nb::list build_tsd_output_values_list<nb::object>(const std::unordered_map<nb::object, time_series_output_ptr>&, control_block_ptr);

    template <typename PtrT, typename WrapFn>
    nb::list build_tsd_values_from_iterable(const nb::object& iterable, WrapFn&& wrap) {
        nb::list result;
        for (auto value_obj : nb::iter(iterable)) {
            try {
                PtrT raw = nb::cast<PtrT>(value_obj);
                if (raw != nullptr) {
                    result.append(wrap(raw));
                }
            } catch (const nb::cast_error&) {
                // Ignore values that cannot be cast
            }
        }
        return result;
    }

    inline nb::list build_tsd_output_items_from_iterable(const nb::object& iterable, control_block_ptr cb) {
        nb::list result;
        for (auto item_obj : nb::iter(iterable)) {
            if (!nb::isinstance<nb::tuple>(item_obj)) {
                result.append(item_obj);
                continue;
            }
            nb::tuple tuple = nb::cast<nb::tuple>(item_obj);
            if (tuple.size() != 2) {
                result.append(item_obj);
                continue;
            }
            nb::object key = tuple[0];
            try {
                auto* raw = nb::cast<TimeSeriesOutput*>(tuple[1]);
                result.append(nb::make_tuple(key, wrap_output(raw, cb)));
            } catch (const nb::cast_error&) {
                result.append(item_obj);
            }
        }
        return result;
    }
    
    // ============================================================================
    // Constructor Definitions
    // ============================================================================
    
    PyTimeSeriesValueInput::PyTimeSeriesValueInput(TimeSeriesInput* impl, control_block_ptr control_block)
        : PyTimeSeriesInput(impl, std::move(control_block)) {}
    
    PyTimeSeriesValueOutput::PyTimeSeriesValueOutput(TimeSeriesOutput* impl, control_block_ptr control_block)
        : PyTimeSeriesOutput(impl, std::move(control_block)) {}
    
    PyTimeSeriesSignalInput::PyTimeSeriesSignalInput(TimeSeriesInput* impl, control_block_ptr control_block)
        : PyTimeSeriesInput(impl, std::move(control_block)) {}
    
    PyTimeSeriesListInput::PyTimeSeriesListInput(TimeSeriesListInput* impl, control_block_ptr control_block)
        : PyTimeSeriesInput(impl, std::move(control_block)) {}
    
    PyTimeSeriesListOutput::PyTimeSeriesListOutput(TimeSeriesListOutput* impl, control_block_ptr control_block)
        : PyTimeSeriesOutput(impl, std::move(control_block)) {}
    
    PyTimeSeriesBundleInput::PyTimeSeriesBundleInput(TimeSeriesBundleInput* impl, control_block_ptr control_block)
        : PyTimeSeriesInput(impl, std::move(control_block)) {}
    
    PyTimeSeriesBundleOutput::PyTimeSeriesBundleOutput(TimeSeriesBundleOutput* impl, control_block_ptr control_block)
        : PyTimeSeriesOutput(impl, std::move(control_block)) {}
    
    PyTimeSeriesDictInput::PyTimeSeriesDictInput(TimeSeriesDictInput* impl, control_block_ptr control_block)
        : PyTimeSeriesInput(impl, std::move(control_block)) {}
    
    PyTimeSeriesDictOutput::PyTimeSeriesDictOutput(TimeSeriesDictOutput* impl, control_block_ptr control_block)
        : PyTimeSeriesOutput(impl, std::move(control_block)) {}
    
    PyTimeSeriesSetInput::PyTimeSeriesSetInput(TimeSeriesSetInput* impl, control_block_ptr control_block)
        : PyTimeSeriesInput(impl, std::move(control_block)) {}
    
    PyTimeSeriesSetOutput::PyTimeSeriesSetOutput(TimeSeriesSetOutput* impl, control_block_ptr control_block)
        : PyTimeSeriesOutput(impl, std::move(control_block)) {}
    
    PyTimeSeriesWindowInput::PyTimeSeriesWindowInput(TimeSeriesInput* impl, control_block_ptr control_block)
        : PyTimeSeriesInput(impl, std::move(control_block)) {}
    
    namespace {
        // Helper to call has_removed_value on TimeSeriesWindowInput template instances
        template<typename T>
        bool try_has_removed_value(TimeSeriesInput* input) {
            if (auto* window_input = dynamic_cast<TimeSeriesWindowInput<T>*>(input)) {
                return window_input->has_removed_value();
            }
            return false;
        }
        
        template<typename T>
        nb::object try_removed_value(TimeSeriesInput* input) {
            if (auto* window_input = dynamic_cast<TimeSeriesWindowInput<T>*>(input)) {
                return window_input->removed_value();
            }
            return nb::none();
        }
    }
    
    bool PyTimeSeriesWindowInput::has_removed_value() const {
        auto* impl = _impl.get();
        // Try all common template types - return the first successful result
        // The helper returns false if cast fails, so we need to check each one
        bool result = try_has_removed_value<bool>(impl);
        if (result || dynamic_cast<TimeSeriesWindowInput<bool>*>(impl)) return result;
        result = try_has_removed_value<int64_t>(impl);
        if (result || dynamic_cast<TimeSeriesWindowInput<int64_t>*>(impl)) return result;
        result = try_has_removed_value<double>(impl);
        if (result || dynamic_cast<TimeSeriesWindowInput<double>*>(impl)) return result;
        result = try_has_removed_value<engine_date_t>(impl);
        if (result || dynamic_cast<TimeSeriesWindowInput<engine_date_t>*>(impl)) return result;
        result = try_has_removed_value<engine_time_t>(impl);
        if (result || dynamic_cast<TimeSeriesWindowInput<engine_time_t>*>(impl)) return result;
        result = try_has_removed_value<engine_time_delta_t>(impl);
        if (result || dynamic_cast<TimeSeriesWindowInput<engine_time_delta_t>*>(impl)) return result;
        result = try_has_removed_value<nb::object>(impl);
        return result;
    }
    
    nb::object PyTimeSeriesWindowInput::removed_value() const {
        auto* impl = _impl.get();
        // Try all common template types
        auto result = try_removed_value<bool>(impl);
        if (!result.is_none()) return result;
        result = try_removed_value<int64_t>(impl);
        if (!result.is_none()) return result;
        result = try_removed_value<double>(impl);
        if (!result.is_none()) return result;
        result = try_removed_value<engine_date_t>(impl);
        if (!result.is_none()) return result;
        result = try_removed_value<engine_time_t>(impl);
        if (!result.is_none()) return result;
        result = try_removed_value<engine_time_delta_t>(impl);
        if (!result.is_none()) return result;
        result = try_removed_value<nb::object>(impl);
        return result;
    }
    
    PyTimeSeriesWindowOutput::PyTimeSeriesWindowOutput(TimeSeriesOutput* impl, control_block_ptr control_block)
        : PyTimeSeriesOutput(impl, std::move(control_block)) {}
    
    PyTimeSeriesReferenceInput::PyTimeSeriesReferenceInput(TimeSeriesInput* impl, control_block_ptr control_block)
        : PyTimeSeriesInput(impl, std::move(control_block)) {}
    
    PyTimeSeriesReferenceOutput::PyTimeSeriesReferenceOutput(TimeSeriesOutput* impl, control_block_ptr control_block)
        : PyTimeSeriesOutput(impl, std::move(control_block)) {}
    
    // ============================================================================
    // TS (Value) Types
    // ============================================================================
    
    nb::object PyTimeSeriesValueInput::value() const {
        // Use py_value from base TimeSeriesInput
        return _impl->py_value();
    }
    
    nb::object PyTimeSeriesValueInput::delta_value() const {
        // Use py_delta_value from base TimeSeriesInput
        return _impl->py_delta_value();
    }
    
    void PyTimeSeriesValueInput::register_with_nanobind(nb::module_& m) {
        nb::class_<PyTimeSeriesValueInput, PyTimeSeriesInput>(m, "TimeSeriesValueInput")
            .def_prop_ro("value", &PyTimeSeriesValueInput::value)
            .def_prop_ro("delta_value", &PyTimeSeriesValueInput::delta_value);
    }
    
    void PyTimeSeriesValueOutput::register_with_nanobind(nb::module_& m) {
        nb::class_<PyTimeSeriesValueOutput, PyTimeSeriesOutput>(m, "TimeSeriesValueOutput")
            .def_prop_rw(
                "value",
                [](const PyTimeSeriesValueOutput& self) { return self.value(); },
                [](PyTimeSeriesValueOutput& self, nb::object value) {
                    self.set_value(std::move(value));
                },
                nb::arg("value").none());
    }
    
    // ============================================================================
    // Signal Types (INPUT-ONLY)
    // ============================================================================
    
    void PyTimeSeriesSignalInput::register_with_nanobind(nb::module_& m) {
        nb::class_<PyTimeSeriesSignalInput, PyTimeSeriesInput>(m, "TimeSeriesSignalInput");
    }
    
    // ============================================================================
    // TSL (List) Types
    // ============================================================================
    
    nb::object PyTimeSeriesListInput::get_item(int64_t index) const {
        auto* impl = static_cast<TimeSeriesListInput*>(_impl.get());
        auto item = (*impl)[static_cast<size_t>(index)];
        return wrap_input(item.get(), _impl.control_block());
    }
    
    int64_t PyTimeSeriesListInput::len() const {
        auto* impl = static_cast<TimeSeriesListInput*>(_impl.get());
        return impl->size();
    }
    
    nb::object PyTimeSeriesListInput::iter() const {
        auto* impl = static_cast<TimeSeriesListInput*>(_impl.get());
        // Return list of wrapped inputs for Python to iterate
        nb::list items;
        for (size_t i = 0; i < impl->size(); ++i) {
            items.append(wrap_input((*impl)[i], _impl.control_block()));
        }
        return items.attr("__iter__")();
    }
    
    nb::list PyTimeSeriesListInput::keys() const {
        auto* impl = static_cast<TimeSeriesListInput*>(_impl.get());
        nb::list result;
        for (auto key : impl->keys()) {
            result.append(key);
        }
        return result;
    }
    
    nb::object PyTimeSeriesListInput::values() const {
        auto* impl = static_cast<TimeSeriesListInput*>(_impl.get());
        nb::list result;
        for (auto& value : *impl) {
            result.append(wrap_input(value.get(), _impl.control_block()));
        }
        return result.attr("__iter__")();
    }
    
    nb::list PyTimeSeriesListInput::items() const {
        auto* impl = static_cast<TimeSeriesListInput*>(_impl.get());
        nb::list result;
        for (const auto& [idx, value] : impl->items()) {
            result.append(nb::make_tuple(nb::int_(idx),
                                         wrap_input(value.get(), _impl.control_block())));
        }
        return result;
    }
    
    nb::list PyTimeSeriesListInput::valid_keys() const {
        auto* impl = static_cast<TimeSeriesListInput*>(_impl.get());
        nb::list result;
        for (auto key : impl->valid_keys()) {
            result.append(key);
        }
        return result;
    }
    
    nb::object PyTimeSeriesListInput::valid_values() const {
        auto* impl = static_cast<TimeSeriesListInput*>(_impl.get());
        nb::list result;
        for (const auto& [idx, value] : impl->valid_items()) {
            result.append(wrap_input(value.get(), _impl.control_block()));
        }
        return result.attr("__iter__")();
    }
    
    nb::list PyTimeSeriesListInput::valid_items() const{
        auto* impl = static_cast<TimeSeriesListInput*>(_impl.get());
        nb::list result;
        for (const auto& [idx, value] : impl->valid_items()) {
            result.append(nb::make_tuple(nb::int_(idx),
                                         wrap_input(value.get(), _impl.control_block())));
        }
        return result;
    }
    
    nb::list PyTimeSeriesListInput::modified_keys() const {
        auto* impl = static_cast<TimeSeriesListInput*>(_impl.get());
        nb::list result;
        for (auto key : impl->modified_keys()) {
            result.append(key);
        }
        return result;
    }
    
    nb::object PyTimeSeriesListInput::modified_values() const {
        auto* impl = static_cast<TimeSeriesListInput*>(_impl.get());
        nb::list result;
        for (const auto& [idx, value] : impl->modified_items()) {
            result.append(wrap_input(value.get(), _impl.control_block()));
        }
        return result.attr("__iter__")();
    }
    
    nb::list PyTimeSeriesListInput::modified_items() const {
        auto* impl = static_cast<TimeSeriesListInput*>(_impl.get());
        nb::list result;
        for (const auto& [idx, value] : impl->modified_items()) {
            result.append(nb::make_tuple(nb::int_(idx),
                                         wrap_input(value.get(), _impl.control_block())));
        }
        return result;
    }
    
    void PyTimeSeriesListInput::register_with_nanobind(nb::module_& m) {
        nb::class_<PyTimeSeriesListInput, PyTimeSeriesInput>(m, "TimeSeriesListInput")
            .def("__getitem__", &PyTimeSeriesListInput::get_item)
            .def("__len__", &PyTimeSeriesListInput::len)
            .def("__iter__", &PyTimeSeriesListInput::iter)
            .def("keys", &PyTimeSeriesListInput::keys)
            .def("values", &PyTimeSeriesListInput::values)
            .def("items", &PyTimeSeriesListInput::items)
            .def("valid_keys", &PyTimeSeriesListInput::valid_keys)
            .def("valid_values", &PyTimeSeriesListInput::valid_values)
            .def("valid_items", &PyTimeSeriesListInput::valid_items)
            .def("modified_keys", &PyTimeSeriesListInput::modified_keys)
            .def("modified_values", &PyTimeSeriesListInput::modified_values)
            .def("modified_items", &PyTimeSeriesListInput::modified_items);
    }
    
    nb::object PyTimeSeriesListOutput::get_item(int64_t index) const {
        auto* impl = static_cast<TimeSeriesListOutput*>(_impl.get());
        auto item = (*impl)[static_cast<size_t>(index)];
        return wrap_output(item.get(), _impl.control_block());
    }
    
    int64_t PyTimeSeriesListOutput::len() const {
        auto* impl = static_cast<TimeSeriesListOutput*>(_impl.get());
        return impl->size();
    }
    
    nb::object PyTimeSeriesListOutput::iter() const {
        auto* impl = static_cast<TimeSeriesListOutput*>(_impl.get());
        // Return list of wrapped outputs for Python to iterate
        nb::list items;
        for (size_t i = 0; i < impl->size(); ++i) {
            items.append(wrap_output((*impl)[i], _impl.control_block()));
        }
        return items.attr("__iter__")();
    }
    
    nb::list PyTimeSeriesListOutput::keys() const {
        auto* impl = static_cast<TimeSeriesListOutput*>(_impl.get());
        nb::list result;
        for (auto key : impl->keys()) {
            result.append(key);
        }
        return result;
    }
    
    nb::object PyTimeSeriesListOutput::values() const {
        auto* impl = static_cast<TimeSeriesListOutput*>(_impl.get());
        nb::list result;
        for (const auto& [idx, value] : impl->items()) {
            (void)idx;
            result.append(wrap_output(value.get(), _impl.control_block()));
        }
        return result.attr("__iter__")();
    }
    
    nb::list PyTimeSeriesListOutput::items() const {
        auto* impl = static_cast<TimeSeriesListOutput*>(_impl.get());
        nb::list result;
        for (const auto& [idx, value] : impl->items()) {
            result.append(nb::make_tuple(nb::int_(idx),
                                         wrap_output(value.get(), _impl.control_block())));
        }
        return result;
    }
    
    nb::list PyTimeSeriesListOutput::valid_keys() const {
        auto* impl = static_cast<TimeSeriesListOutput*>(_impl.get());
        nb::list result;
        for (auto key : impl->valid_keys()) {
            result.append(key);
        }
        return result;
    }
    
    nb::object PyTimeSeriesListOutput::valid_values() const {
        auto* impl = static_cast<TimeSeriesListOutput*>(_impl.get());
        nb::list result;
        for (const auto& [idx, value] : impl->valid_items()) {
            (void)idx;
            result.append(wrap_output(value.get(), _impl.control_block()));
        }
        return result.attr("__iter__")();
    }
    
    nb::list PyTimeSeriesListOutput::valid_items() const {
        auto* impl = static_cast<TimeSeriesListOutput*>(_impl.get());
        nb::list result;
        for (const auto& [idx, value] : impl->valid_items()) {
            result.append(nb::make_tuple(nb::int_(idx),
                                         wrap_output(value.get(), _impl.control_block())));
        }
        return result;
    }
    
    nb::list PyTimeSeriesListOutput::modified_keys() const {
        auto* impl = static_cast<TimeSeriesListOutput*>(_impl.get());
        nb::list result;
        for (auto key : impl->modified_keys()) {
            result.append(key);
        }
        return result;
    }
    
    nb::object PyTimeSeriesListOutput::modified_values() const {
        auto* impl = static_cast<TimeSeriesListOutput*>(_impl.get());
        nb::list result;
        for (const auto& [idx, value] : impl->modified_items()) {
            (void)idx;
            result.append(wrap_output(value.get(), _impl.control_block()));
        }
        return result.attr("__iter__")();
    }
    
    nb::list PyTimeSeriesListOutput::modified_items() const {
        auto* impl = static_cast<TimeSeriesListOutput*>(_impl.get());
        nb::list result;
        for (const auto& [idx, value] : impl->modified_items()) {
            result.append(nb::make_tuple(nb::int_(idx),
                                         wrap_output(value.get(), _impl.control_block())));
        }
        return result;
    }
    
    bool PyTimeSeriesListOutput::can_apply_result(nb::object value) const {
        auto* impl = static_cast<TimeSeriesListOutput*>(_impl.get());
        return impl->can_apply_result(value);
    }
    
    void PyTimeSeriesListOutput::apply_result(nb::object value) {
        auto* impl = static_cast<TimeSeriesListOutput*>(_impl.get());
        impl->apply_result(std::move(value));
    }
    
    void PyTimeSeriesListOutput::register_with_nanobind(nb::module_& m) {
        nb::class_<PyTimeSeriesListOutput, PyTimeSeriesOutput>(m, "TimeSeriesListOutput")
            .def("__getitem__", &PyTimeSeriesListOutput::get_item)
            .def("__len__", &PyTimeSeriesListOutput::len)
            .def("__iter__", &PyTimeSeriesListOutput::iter)
            .def("keys", &PyTimeSeriesListOutput::keys)
            .def("values", &PyTimeSeriesListOutput::values)
            .def("items", &PyTimeSeriesListOutput::items)
            .def("valid_keys", &PyTimeSeriesListOutput::valid_keys)
            .def("valid_values", &PyTimeSeriesListOutput::valid_values)
            .def("valid_items", &PyTimeSeriesListOutput::valid_items)
            .def("modified_keys", &PyTimeSeriesListOutput::modified_keys)
            .def("modified_values", &PyTimeSeriesListOutput::modified_values)
            .def("modified_items", &PyTimeSeriesListOutput::modified_items)
            .def("can_apply_result", &PyTimeSeriesListOutput::can_apply_result)
            .def("apply_result", &PyTimeSeriesListOutput::apply_result);
    }
    
    // ============================================================================
    // TSB (Bundle) Types
    // ============================================================================
    
    nb::object PyTimeSeriesBundleInput::get_item(nb::object key) const {
        auto* impl = static_cast<TimeSeriesBundleInput*>(_impl.get());
        
        // Handle both string and int keys
        if (nb::isinstance<nb::int_>(key)) {
            auto item = (*impl)[nb::cast<size_t>(key)];
            return wrap_input(item.get(), _impl.control_block());
        } else if (nb::isinstance<nb::str>(key)) {
            auto item = (*impl)[nb::cast<std::string>(key)];
            return wrap_input(item.get(), _impl.control_block());
        }
        
        throw std::runtime_error(fmt::format("Invalid key type for TimeSeriesBundleInput: expected str or int"));
    }
    
    int64_t PyTimeSeriesBundleInput::len() const {
        auto* impl = static_cast<TimeSeriesBundleInput*>(_impl.get());
        return impl->size();
    }
    
    nb::object PyTimeSeriesBundleInput::iter() const {
        auto* impl = static_cast<TimeSeriesBundleInput*>(_impl.get());
        // Return iterator over values (TimeSeriesInput objects), not keys
        // This matches Python's behavior for varargs iteration
        nb::list values;
        for (size_t i = 0; i < impl->size(); ++i) {
            values.append(wrap_input(impl->get_input(i), _impl.control_block()));
        }
        return values.attr("__iter__")();
    }
    
    bool PyTimeSeriesBundleInput::contains(const std::string& key) const {
        auto* impl = static_cast<TimeSeriesBundleInput*>(_impl.get());
        return impl->contains(key);
    }
    
    nb::list PyTimeSeriesBundleInput::keys() const {
        auto* impl = static_cast<TimeSeriesBundleInput*>(_impl.get());
        nb::list result;
        for (const auto& key : impl->keys()) {
            result.append(nb::str(key.get().c_str()));
        }
        return result;
    }
    
    nb::object PyTimeSeriesBundleInput::values() const {
        auto* impl = static_cast<TimeSeriesBundleInput*>(_impl.get());
        nb::list result;
        for (size_t i = 0; i < impl->size(); ++i) {
            result.append(wrap_input(impl->get_input(i), _impl.control_block()));
        }
        return result.attr("__iter__")();
    }
    
    nb::list PyTimeSeriesBundleInput::items() const {
        auto* impl = static_cast<TimeSeriesBundleInput*>(_impl.get());
        nb::list result;
        for (const auto& [key, value] : impl->items()) {
            nb::tuple item = nb::make_tuple(
                nb::str(key.get().c_str()),
                wrap_input(value.get(), _impl.control_block())
            );
            result.append(item);
        }
        return result;
    }
    
    nb::list PyTimeSeriesBundleInput::modified_keys() const {
        auto* impl = static_cast<TimeSeriesBundleInput*>(_impl.get());
        nb::list result;
        for (const auto& key : impl->modified_keys()) {
            result.append(nb::str(key.get().c_str()));
        }
        return result;
    }
    
    nb::object PyTimeSeriesBundleInput::modified_values() const {
        auto* impl = static_cast<TimeSeriesBundleInput*>(_impl.get());
        nb::list result;
        for (const auto& [key, value] : impl->modified_items()) {
            result.append(wrap_input(value.get(), _impl.control_block()));
        }
        return result.attr("__iter__")();
    }
    
    nb::list PyTimeSeriesBundleInput::modified_items() const {
        auto* impl = static_cast<TimeSeriesBundleInput*>(_impl.get());
        nb::list result;
        for (const auto& [key, value] : impl->modified_items()) {
            nb::tuple item = nb::make_tuple(
                nb::str(key.get().c_str()),
                wrap_input(value.get(), _impl.control_block())
            );
            result.append(item);
        }
        return result;
    }
    
    nb::list PyTimeSeriesBundleInput::valid_keys() const {
        auto* impl = static_cast<TimeSeriesBundleInput*>(_impl.get());
        nb::list result;
        for (const auto& key : impl->valid_keys()) {
            result.append(nb::str(key.get().c_str()));
        }
        return result;
    }
    
    nb::object PyTimeSeriesBundleInput::valid_values() const {
        auto* impl = static_cast<TimeSeriesBundleInput*>(_impl.get());
        nb::list result;
        for (const auto& [key, value] : impl->valid_items()) {
            result.append(wrap_input(value.get(), _impl.control_block()));
        }
        return result.attr("__iter__")();
    }
    
    nb::list PyTimeSeriesBundleInput::valid_items() const {
        auto* impl = static_cast<TimeSeriesBundleInput*>(_impl.get());
        nb::list result;
        for (const auto& [key, value] : impl->valid_items()) {
            nb::tuple item = nb::make_tuple(
                nb::str(key.get().c_str()),
                wrap_input(value.get(), _impl.control_block())
            );
            result.append(item);
        }
        return result;
    }
    
    nb::object PyTimeSeriesBundleInput::schema() const {
        auto* impl = static_cast<TimeSeriesBundleInput*>(_impl.get());
        return nb::cast(&impl->schema());
    }
    
    nb::object PyTimeSeriesBundleInput::as_schema() const {
        return nb::cast<const PyTimeSeriesBundleInput&>(*this, nb::rv_policy::reference);
    }

    nb::object PyTimeSeriesBundleInput::getattr(nb::handle key) const {
        auto* impl = static_cast<TimeSeriesBundleInput*>(_impl.get());
        std::string key_str = nb::cast<std::string>(nb::str(key));
        if (!impl->contains(key_str)) {
            auto message = fmt::format("TimeSeriesBundleInput has no attribute '{}'", key_str);
            throw nb::attribute_error(message.c_str());
        }
        return get_item(nb::str(key_str.c_str()));
    }
    
    void PyTimeSeriesBundleInput::register_with_nanobind(nb::module_& m) {
        nb::class_<PyTimeSeriesBundleInput, PyTimeSeriesInput>(m, "TimeSeriesBundleInput")
            .def("__getitem__", &PyTimeSeriesBundleInput::get_item)
            .def("__len__", &PyTimeSeriesBundleInput::len)
            .def("__iter__", &PyTimeSeriesBundleInput::iter)
            .def("__contains__", &PyTimeSeriesBundleInput::contains)
            .def("__getattr__", &PyTimeSeriesBundleInput::getattr)
            .def("keys", &PyTimeSeriesBundleInput::keys)
            .def("values", &PyTimeSeriesBundleInput::values)
            .def("items", &PyTimeSeriesBundleInput::items)
            .def("modified_keys", &PyTimeSeriesBundleInput::modified_keys)
            .def("modified_values", &PyTimeSeriesBundleInput::modified_values)
            .def("modified_items", &PyTimeSeriesBundleInput::modified_items)
            .def("valid_keys", &PyTimeSeriesBundleInput::valid_keys)
            .def("valid_values", &PyTimeSeriesBundleInput::valid_values)
            .def("valid_items", &PyTimeSeriesBundleInput::valid_items)
            .def_prop_ro("__schema__", &PyTimeSeriesBundleInput::schema)
            .def_prop_ro("as_schema", &PyTimeSeriesBundleInput::as_schema);
    }
    
    nb::object PyTimeSeriesBundleOutput::get_item(nb::object key) const {
        auto* impl = static_cast<TimeSeriesBundleOutput*>(_impl.get());
        
        // Handle both string and int keys, wrap the output
        if (nb::isinstance<nb::int_>(key)) {
            auto item = (*impl)[nb::cast<size_t>(key)];
            return wrap_output(item.get(), _impl.control_block());
        } else if (nb::isinstance<nb::str>(key)) {
            auto item = (*impl)[nb::cast<std::string>(key)];
            return wrap_output(item.get(), _impl.control_block());
        }
        
        throw std::runtime_error(fmt::format("Invalid key type for TimeSeriesBundleOutput: expected str or int"));
    }
    
    int64_t PyTimeSeriesBundleOutput::len() const {
        auto* impl = static_cast<TimeSeriesBundleOutput*>(_impl.get());
        return impl->size();
    }
    
    nb::object PyTimeSeriesBundleOutput::iter() const {
        auto* impl = static_cast<TimeSeriesBundleOutput*>(_impl.get());
        // Return iterator over keys (matching Python dict behavior)
        nb::list keys;
        for (const auto& key : impl->keys()) {
            keys.append(nb::str(key.get().c_str()));
        }
        return keys.attr("__iter__")();
    }
    
    bool PyTimeSeriesBundleOutput::contains(const std::string& key) const {
        auto* impl = static_cast<TimeSeriesBundleOutput*>(_impl.get());
        return impl->contains(key);
    }
    
    nb::list PyTimeSeriesBundleOutput::keys() const {
        auto* impl = static_cast<TimeSeriesBundleOutput*>(_impl.get());
        nb::list result;
        for (const auto& key : impl->keys()) {
            result.append(nb::str(key.get().c_str()));
        }
        return result;
    }
    
    nb::list PyTimeSeriesBundleOutput::items() const {
        auto* impl = static_cast<TimeSeriesBundleOutput*>(_impl.get());
        nb::list result;
        for (const auto& [key, value] : impl->items()) {
            result.append(
                nb::make_tuple(nb::str(key.get().c_str()),
                               wrap_output(value.get(), _impl.control_block()))
            );
        }
        return result;
    }
    
    nb::object PyTimeSeriesBundleOutput::values() const {
        auto* impl = static_cast<TimeSeriesBundleOutput*>(_impl.get());
        nb::list result;
        for (const auto& [key, value] : impl->items()) {
            (void)key;
            result.append(wrap_output(value.get(), _impl.control_block()));
        }
        return result.attr("__iter__")();
    }
    
    nb::list PyTimeSeriesBundleOutput::modified_keys() const {
        auto* impl = static_cast<TimeSeriesBundleOutput*>(_impl.get());
        nb::list result;
        for (const auto& key : impl->modified_keys()) {
            result.append(nb::str(key.get().c_str()));
        }
        return result;
    }
    
    nb::object PyTimeSeriesBundleOutput::modified_values() const {
        auto* impl = static_cast<TimeSeriesBundleOutput*>(_impl.get());
        nb::list result;
        for (const auto& [key, value] : impl->modified_items()) {
            (void)key;
            result.append(wrap_output(value.get(), _impl.control_block()));
        }
        return result.attr("__iter__")();
    }
    
    nb::list PyTimeSeriesBundleOutput::modified_items() const {
        auto* impl = static_cast<TimeSeriesBundleOutput*>(_impl.get());
        nb::list result;
        for (const auto& [key, value] : impl->modified_items()) {
            result.append(
                nb::make_tuple(nb::str(key.get().c_str()),
                               wrap_output(value.get(), _impl.control_block()))
            );
        }
        return result;
    }
    
    bool PyTimeSeriesBundleOutput::can_apply_result(nb::object value) const {
        auto* impl = static_cast<TimeSeriesBundleOutput*>(_impl.get());
        return impl->can_apply_result(value);
    }
    
    void PyTimeSeriesBundleOutput::apply_result(nb::object value) {
        auto* impl = static_cast<TimeSeriesBundleOutput*>(_impl.get());
        impl->apply_result(std::move(value));
    }
    
    nb::list PyTimeSeriesBundleOutput::valid_keys() const {
        auto* impl = static_cast<TimeSeriesBundleOutput*>(_impl.get());
        nb::list result;
        for (const auto& key : impl->valid_keys()) {
            result.append(nb::str(key.get().c_str()));
        }
        return result;
    }
    
    nb::object PyTimeSeriesBundleOutput::valid_values() const {
        auto* impl = static_cast<TimeSeriesBundleOutput*>(_impl.get());
        nb::list result;
        for (const auto& [key, value] : impl->valid_items()) {
            (void)key;
            result.append(wrap_output(value.get(), _impl.control_block()));
        }
        return result.attr("__iter__")();
    }
    
    nb::list PyTimeSeriesBundleOutput::valid_items() const {
        auto* impl = static_cast<TimeSeriesBundleOutput*>(_impl.get());
        nb::list result;
        for (const auto& [key, value] : impl->valid_items()) {
            result.append(
                nb::make_tuple(nb::str(key.get().c_str()),
                               wrap_output(value.get(), _impl.control_block()))
            );
        }
        return result;
    }
    
    nb::object PyTimeSeriesBundleOutput::schema() const {
        auto* impl = static_cast<TimeSeriesBundleOutput*>(_impl.get());
        return nb::cast(&impl->schema());
    }
    
    nb::object PyTimeSeriesBundleOutput::as_schema() const {
        return nb::cast<const PyTimeSeriesBundleOutput&>(*this, nb::rv_policy::reference);
    }

    nb::object PyTimeSeriesBundleOutput::getattr(nb::handle key) const {
        auto* impl = static_cast<TimeSeriesBundleOutput*>(_impl.get());
        std::string key_str = nb::cast<std::string>(nb::str(key));
        if (!impl->contains(key_str)) {
            auto message = fmt::format("TimeSeriesBundleOutput has no attribute '{}'", key_str);
            throw nb::attribute_error(message.c_str());
        }
        return get_item(nb::str(key_str.c_str()));
    }
    
    void PyTimeSeriesBundleOutput::register_with_nanobind(nb::module_& m) {
        nb::class_<PyTimeSeriesBundleOutput, PyTimeSeriesOutput>(m, "TimeSeriesBundleOutput")
            .def("__getitem__", &PyTimeSeriesBundleOutput::get_item)
            .def("__len__", &PyTimeSeriesBundleOutput::len)
            .def("__iter__", &PyTimeSeriesBundleOutput::iter)
            .def("__contains__", &PyTimeSeriesBundleOutput::contains)
            .def("__getattr__", &PyTimeSeriesBundleOutput::getattr)
            .def("keys", &PyTimeSeriesBundleOutput::keys)
            .def("values", &PyTimeSeriesBundleOutput::values)
            .def("items", &PyTimeSeriesBundleOutput::items)
            .def("modified_keys", &PyTimeSeriesBundleOutput::modified_keys)
            .def("modified_values", &PyTimeSeriesBundleOutput::modified_values)
            .def("modified_items", &PyTimeSeriesBundleOutput::modified_items)
            .def("valid_keys", &PyTimeSeriesBundleOutput::valid_keys)
            .def("valid_values", &PyTimeSeriesBundleOutput::valid_values)
            .def("valid_items", &PyTimeSeriesBundleOutput::valid_items)
            .def("can_apply_result", &PyTimeSeriesBundleOutput::can_apply_result)
            .def("apply_result", &PyTimeSeriesBundleOutput::apply_result)
            .def_prop_ro("__schema__", &PyTimeSeriesBundleOutput::schema)
            .def_prop_ro("as_schema", &PyTimeSeriesBundleOutput::as_schema);
    }
    
    // ============================================================================
    // TSD (Dict) Types
    // ============================================================================
    
    nb::object PyTimeSeriesDictInput::get_item(nb::object key) const {
        auto* impl = static_cast<TimeSeriesDictInput*>(_impl.get());
        // Get the raw C++ time series input, then wrap it using our factory
        // This ensures nested TSDs are wrapped as TimeSeriesDictInput instead of base _TimeSeriesInput
        auto raw_result = impl->py_get_item(key);
        
        // Try to cast to a time_series_input_ptr and wrap it
        try {
            auto ts_input = nb::cast<time_series_input_ptr>(raw_result);
            return wrap_input(ts_input.get(), _impl.control_block());
        } catch (const nb::cast_error&) {
            // Fall through for non-time-series results (e.g., key_set)
        }
        
        // Otherwise return as-is (e.g., key_set)
        return raw_result;
    }
    
    nb::object PyTimeSeriesDictInput::get(nb::object key, nb::object default_value) const {
        auto* impl = static_cast<TimeSeriesDictInput*>(_impl.get());
        return impl->py_get(key, default_value);
    }
    
    bool PyTimeSeriesDictInput::contains(nb::object key) const {
        auto* impl = static_cast<TimeSeriesDictInput*>(_impl.get());
        return impl->py_contains(key);
    }
    
    int64_t PyTimeSeriesDictInput::len() const {
        auto* impl = static_cast<TimeSeriesDictInput*>(_impl.get());
        return impl->size();
    }
    
    nb::object PyTimeSeriesDictInput::keys() const {
        auto* impl = static_cast<TimeSeriesDictInput*>(_impl.get());
        return impl->py_keys();
    }
    
    nb::object PyTimeSeriesDictInput::values() const {
        auto* impl = static_cast<TimeSeriesDictInput*>(_impl.get());
        
        // Try to cast to specific template instantiations and use direct access
        nb::list result;
        if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<nb::object>*>(impl)) {
            for (const auto& [key, value] : tsd->value()) {
                result.append(wrap_input(value.get(), _impl.control_block()));
            }
        } else if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<int64_t>*>(impl)) {
            for (const auto& [key, value] : tsd->value()) {
                result.append(wrap_input(value.get(), _impl.control_block()));
            }
        } else if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<double>*>(impl)) {
            for (const auto& [key, value] : tsd->value()) {
                result.append(wrap_input(value.get(), _impl.control_block()));
            }
        } else if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<bool>*>(impl)) {
            for (const auto& [key, value] : tsd->value()) {
                result.append(wrap_input(value.get(), _impl.control_block()));
            }
        } else {
            // Fallback to old iterator-based approach for other key types
            return impl->py_values();
        }
        return result;
    }
    
    nb::object PyTimeSeriesDictInput::items() const {
        auto* impl = static_cast<TimeSeriesDictInput*>(_impl.get());
        
        // Try to cast to specific template instantiations and use direct access
        if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<nb::object>*>(impl)) {
            return build_tsd_items_list(tsd->value(), _impl.control_block());
        } else if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<int64_t>*>(impl)) {
            return build_tsd_items_list(tsd->value(), _impl.control_block());
        } else if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<double>*>(impl)) {
            return build_tsd_items_list(tsd->value(), _impl.control_block());
        } else if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<bool>*>(impl)) {
            return build_tsd_items_list(tsd->value(), _impl.control_block());
        } else {
            // Fallback to old iterator-based approach for other key types
            return impl->py_items();
        }
    }
    
    nb::object PyTimeSeriesDictInput::valid_keys() const {
        auto* impl = static_cast<TimeSeriesDictInput*>(_impl.get());
        return impl->py_valid_keys();
    }
    
    nb::object PyTimeSeriesDictInput::valid_values() const {
        auto* impl = static_cast<TimeSeriesDictInput*>(_impl.get());
        auto cb = _impl.control_block();
        
        // Try to cast to specific template instantiations and use direct access
        if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<nb::object>*>(impl)) {
            return build_tsd_input_values_list(tsd->valid_items(), cb);
        } else if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<int64_t>*>(impl)) {
            return build_tsd_input_values_list(tsd->valid_items(), cb);
        } else if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<double>*>(impl)) {
            return build_tsd_input_values_list(tsd->valid_items(), cb);
        } else if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<bool>*>(impl)) {
            return build_tsd_input_values_list(tsd->valid_items(), cb);
        }
        
        // Fallback to iterator-based approach for other key types
        return build_tsd_values_from_iterable<TimeSeriesInput*>(impl->py_valid_values(),
            [&](TimeSeriesInput* ptr) { return wrap_input(ptr, cb); });
    }
    
    nb::object PyTimeSeriesDictInput::valid_items() const {
        auto* impl = static_cast<TimeSeriesDictInput*>(_impl.get());
        
        // Try to cast to specific template instantiations and use direct access
        if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<nb::object>*>(impl)) {
            return build_tsd_items_list(tsd->valid_items(), _impl.control_block());
        } else if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<int64_t>*>(impl)) {
            return build_tsd_items_list(tsd->valid_items(), _impl.control_block());
        } else if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<double>*>(impl)) {
            return build_tsd_items_list(tsd->valid_items(), _impl.control_block());
        } else if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<bool>*>(impl)) {
            return build_tsd_items_list(tsd->valid_items(), _impl.control_block());
        } else {
            // Fallback to old iterator-based approach for other key types
            return impl->py_valid_items();
        }
    }
    
    nb::object PyTimeSeriesDictInput::added_keys() const {
        auto* impl = static_cast<TimeSeriesDictInput*>(_impl.get());
        return impl->py_added_keys();
    }
    
    nb::object PyTimeSeriesDictInput::added_items() const {
        auto* impl = static_cast<TimeSeriesDictInput*>(_impl.get());
        
        // Try to cast to specific template instantiations and use direct access
        if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<nb::object>*>(impl)) {
            return build_tsd_items_list(tsd->added_items(), _impl.control_block());
        } else if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<int64_t>*>(impl)) {
            return build_tsd_items_list(tsd->added_items(), _impl.control_block());
        } else if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<double>*>(impl)) {
            return build_tsd_items_list(tsd->added_items(), _impl.control_block());
        } else if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<bool>*>(impl)) {
            return build_tsd_items_list(tsd->added_items(), _impl.control_block());
        } else {
            // Fallback to old iterator-based approach for other key types
            return impl->py_added_items();
        }
    }
    
    nb::object PyTimeSeriesDictInput::added_values() const {
        auto* impl = static_cast<TimeSeriesDictInput*>(_impl.get());
        auto cb = _impl.control_block();

        if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<nb::object>*>(impl)) {
            return build_tsd_input_values_list(tsd->added_items(), cb);
        } else if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<int64_t>*>(impl)) {
            return build_tsd_input_values_list(tsd->added_items(), cb);
        } else if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<double>*>(impl)) {
            return build_tsd_input_values_list(tsd->added_items(), cb);
        } else if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<bool>*>(impl)) {
            return build_tsd_input_values_list(tsd->added_items(), cb);
        }

        return build_tsd_values_from_iterable<TimeSeriesInput*>(impl->py_added_values(),
            [&](TimeSeriesInput* ptr) { return wrap_input(ptr, cb); });
    }
    
    nb::object PyTimeSeriesDictInput::modified_keys() const {
        auto* impl = static_cast<TimeSeriesDictInput*>(_impl.get());
        return impl->py_modified_keys();
    }
    
    nb::object PyTimeSeriesDictInput::modified_values() const {
        auto* impl = static_cast<TimeSeriesDictInput*>(_impl.get());
        auto cb = _impl.control_block();

        if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<nb::object>*>(impl)) {
            return build_tsd_input_values_list(tsd->modified_items(), cb);
        } else if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<int64_t>*>(impl)) {
            return build_tsd_input_values_list(tsd->modified_items(), cb);
        } else if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<double>*>(impl)) {
            return build_tsd_input_values_list(tsd->modified_items(), cb);
        } else if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<bool>*>(impl)) {
            return build_tsd_input_values_list(tsd->modified_items(), cb);
        }

        return build_tsd_values_from_iterable<TimeSeriesInput*>(impl->py_modified_values(),
            [&](TimeSeriesInput* ptr) { return wrap_input(ptr, cb); });
    }
    
    nb::object PyTimeSeriesDictInput::modified_items() const {
        auto* impl = static_cast<TimeSeriesDictInput*>(_impl.get());
        
        // Try to cast to specific template instantiations and use direct access
        if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<nb::object>*>(impl)) {
            return build_tsd_items_list(tsd->modified_items(), _impl.control_block());
        } else if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<int64_t>*>(impl)) {
            return build_tsd_items_list(tsd->modified_items(), _impl.control_block());
        } else if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<double>*>(impl)) {
            return build_tsd_items_list(tsd->modified_items(), _impl.control_block());
        } else if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<bool>*>(impl)) {
            return build_tsd_items_list(tsd->modified_items(), _impl.control_block());
        } else {
            // Fallback to old iterator-based approach for other key types
            return impl->py_modified_items();
        }
    }
    
    nb::object PyTimeSeriesDictInput::removed_keys() const {
        auto* impl = static_cast<TimeSeriesDictInput*>(_impl.get());
        return impl->py_removed_keys();
    }
    
    nb::object PyTimeSeriesDictInput::removed_values() const {
        auto* impl = static_cast<TimeSeriesDictInput*>(_impl.get());
        auto cb = _impl.control_block();

        if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<nb::object>*>(impl)) {
            return build_tsd_input_values_list(tsd->removed_items(), cb);
        } else if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<int64_t>*>(impl)) {
            return build_tsd_input_values_list(tsd->removed_items(), cb);
        } else if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<double>*>(impl)) {
            return build_tsd_input_values_list(tsd->removed_items(), cb);
        } else if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<bool>*>(impl)) {
            return build_tsd_input_values_list(tsd->removed_items(), cb);
        }

        return build_tsd_values_from_iterable<TimeSeriesInput*>(impl->py_removed_values(),
            [&](TimeSeriesInput* ptr) { return wrap_input(ptr, cb); });
    }
    
    nb::object PyTimeSeriesDictInput::removed_items() const {
        auto* impl = static_cast<TimeSeriesDictInput*>(_impl.get());
        
        // Try to cast to specific template instantiations and use direct access
        if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<nb::object>*>(impl)) {
            return build_tsd_items_list(tsd->removed_items(), _impl.control_block());
        } else if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<int64_t>*>(impl)) {
            return build_tsd_items_list(tsd->removed_items(), _impl.control_block());
        } else if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<double>*>(impl)) {
            return build_tsd_items_list(tsd->removed_items(), _impl.control_block());
        } else if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<bool>*>(impl)) {
            return build_tsd_items_list(tsd->removed_items(), _impl.control_block());
        } else {
            // Fallback to old iterator-based approach for other key types
            return impl->py_removed_items();
        }
    }
    
    nb::object PyTimeSeriesDictInput::key_set() const {
        auto* impl = static_cast<TimeSeriesDictInput*>(_impl.get());
        return wrap_input(&impl->key_set(), _impl.control_block());
    }
    
    void PyTimeSeriesDictInput::_create(nb::object key) {
        auto* impl = static_cast<TimeSeriesDictInput*>(_impl.get());
        impl->py_create(key);
    }
    
    void PyTimeSeriesDictInput::on_key_added(nb::object key) {
        auto* impl = static_cast<TimeSeriesDictInput*>(_impl.get());
        
        // Cast to template type to access on_key_added
        if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<nb::object>*>(impl)) {
            tsd->on_key_added(key);
        } else if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<std::string>*>(impl)) {
            tsd->on_key_added(nb::cast<std::string>(key));
        } else if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<int64_t>*>(impl)) {
            tsd->on_key_added(nb::cast<int64_t>(key));
        } else if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<double>*>(impl)) {
            tsd->on_key_added(nb::cast<double>(key));
        } else if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<bool>*>(impl)) {
            tsd->on_key_added(nb::cast<bool>(key));
        } else {
            throw std::runtime_error("on_key_added: Unsupported TSD key type");
        }
    }
    
    void PyTimeSeriesDictInput::on_key_removed(nb::object key) {
        auto* impl = static_cast<TimeSeriesDictInput*>(_impl.get());
        
        // Cast to template type to access on_key_removed
        if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<nb::object>*>(impl)) {
            tsd->on_key_removed(key);
        } else if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<std::string>*>(impl)) {
            tsd->on_key_removed(nb::cast<std::string>(key));
        } else if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<int64_t>*>(impl)) {
            tsd->on_key_removed(nb::cast<int64_t>(key));
        } else if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<double>*>(impl)) {
            tsd->on_key_removed(nb::cast<double>(key));
        } else if (auto* tsd = dynamic_cast<TimeSeriesDictInput_T<bool>*>(impl)) {
            tsd->on_key_removed(nb::cast<bool>(key));
        } else {
            throw std::runtime_error("on_key_removed: Unsupported TSD key type");
        }
    }
    
    void PyTimeSeriesDictInput::register_with_nanobind(nb::module_& m) {
        nb::class_<PyTimeSeriesDictInput, PyTimeSeriesInput>(m, "TimeSeriesDictInput")
            .def("__getitem__", &PyTimeSeriesDictInput::get_item)
            .def("get", &PyTimeSeriesDictInput::get, "key"_a, "default"_a = nb::none())
            .def("__contains__", &PyTimeSeriesDictInput::contains)
            .def("__len__", &PyTimeSeriesDictInput::len)
            .def("keys", &PyTimeSeriesDictInput::keys)
            .def("values", &PyTimeSeriesDictInput::values)
            .def("items", &PyTimeSeriesDictInput::items)
            .def("valid_keys", &PyTimeSeriesDictInput::valid_keys)
            .def("valid_values", &PyTimeSeriesDictInput::valid_values)
            .def("valid_items", &PyTimeSeriesDictInput::valid_items)
            .def("added_keys", &PyTimeSeriesDictInput::added_keys)
            .def("added_values", &PyTimeSeriesDictInput::added_values)
            .def("added_items", &PyTimeSeriesDictInput::added_items)
            .def("modified_keys", &PyTimeSeriesDictInput::modified_keys)
            .def("modified_values", &PyTimeSeriesDictInput::modified_values)
            .def("modified_items", &PyTimeSeriesDictInput::modified_items)
            .def("removed_keys", &PyTimeSeriesDictInput::removed_keys)
            .def("removed_values", &PyTimeSeriesDictInput::removed_values)
            .def("removed_items", &PyTimeSeriesDictInput::removed_items)
            .def_prop_ro("key_set", &PyTimeSeriesDictInput::key_set)
            .def("_create", &PyTimeSeriesDictInput::_create, "key"_a)
            .def("on_key_added", &PyTimeSeriesDictInput::on_key_added, "key"_a)
            .def("on_key_removed", &PyTimeSeriesDictInput::on_key_removed, "key"_a);
    }
    
    nb::object PyTimeSeriesDictOutput::get_item(nb::object key) const {
        auto* impl = static_cast<TimeSeriesDictOutput*>(_impl.get());
        return impl->py_get_item(key);
    }
    
    nb::object PyTimeSeriesDictOutput::get(nb::object key, nb::object default_value) const {
        auto* impl = static_cast<TimeSeriesDictOutput*>(_impl.get());
        
        // Check if key exists, if so return get_item, otherwise return default
        if (impl->py_contains(key)) {
            return get_item(key);
        }
        return default_value;
    }
    
    bool PyTimeSeriesDictOutput::contains(nb::object key) const {
        auto* impl = static_cast<TimeSeriesDictOutput*>(_impl.get());
        return impl->py_contains(key);
    }
    
    int64_t PyTimeSeriesDictOutput::len() const {
        auto* impl = static_cast<TimeSeriesDictOutput*>(_impl.get());
        return impl->size();
    }
    
    nb::object PyTimeSeriesDictOutput::iter() const {
        auto* impl = static_cast<TimeSeriesDictOutput*>(_impl.get());
        return impl->py_iter();
    }
    
    nb::object PyTimeSeriesDictOutput::keys() const {
        auto* impl = static_cast<TimeSeriesDictOutput*>(_impl.get());
        return impl->py_keys();
    }
    
    nb::object PyTimeSeriesDictOutput::values() const {
        auto* impl = static_cast<TimeSeriesDictOutput*>(_impl.get());
        return impl->py_values();
    }
    
    nb::object PyTimeSeriesDictOutput::items() const {
        auto* impl = static_cast<TimeSeriesDictOutput*>(_impl.get());
        return impl->py_items();
    }
    
    nb::object PyTimeSeriesDictOutput::valid_keys() const {
        auto* impl = static_cast<TimeSeriesDictOutput*>(_impl.get());
        return impl->py_valid_keys();
    }

    nb::object PyTimeSeriesDictOutput::valid_values() const {
        auto* impl = static_cast<TimeSeriesDictOutput*>(_impl.get());
        auto cb = _impl.control_block();
        return build_tsd_values_from_iterable<TimeSeriesOutput*>(impl->py_valid_values(),
            [&](TimeSeriesOutput* ptr) { return wrap_output(ptr, cb); });
    }

    nb::object PyTimeSeriesDictOutput::valid_items() const {
        auto* impl = static_cast<TimeSeriesDictOutput*>(_impl.get());
        auto cb = _impl.control_block();
        return build_tsd_output_items_from_iterable(impl->py_valid_items(), cb);
    }

    nb::object PyTimeSeriesDictOutput::added_keys() const {
        auto* impl = static_cast<TimeSeriesDictOutput*>(_impl.get());
        return impl->py_added_keys();
    }

    nb::object PyTimeSeriesDictOutput::added_values() const {
        auto* impl = static_cast<TimeSeriesDictOutput*>(_impl.get());
        auto cb = _impl.control_block();
        return build_tsd_values_from_iterable<TimeSeriesOutput*>(impl->py_added_values(),
            [&](TimeSeriesOutput* ptr) { return wrap_output(ptr, cb); });
    }

    nb::object PyTimeSeriesDictOutput::added_items() const {
        auto* impl = static_cast<TimeSeriesDictOutput*>(_impl.get());
        auto cb = _impl.control_block();
        return build_tsd_output_items_from_iterable(impl->py_added_items(), cb);
    }

    nb::object PyTimeSeriesDictOutput::getattr(nb::handle key) const {
        auto key_str = nb::cast<std::string>(nb::str(key));
        if (!contains(nb::str(key_str.c_str()))) {
            auto message = fmt::format("TimeSeriesDictOutput has no attribute '{}'", key_str);
            throw nb::attribute_error(message.c_str());
        }
        return get_item(nb::str(key_str.c_str()));
    }
    
    void PyTimeSeriesDictOutput::set_item(nb::object key, nb::object value) {
        auto* impl = static_cast<TimeSeriesDictOutput*>(_impl.get());
        impl->py_set_item(key, value);
    }
    
    void PyTimeSeriesDictOutput::del_item(nb::object key) {
        auto* impl = static_cast<TimeSeriesDictOutput*>(_impl.get());
        impl->py_del_item(key);
    }
    
    nb::object PyTimeSeriesDictOutput::pop(nb::object key, nb::object default_value) const {
        auto* impl = static_cast<TimeSeriesDictOutput*>(_impl.get());
        return impl->py_pop(key, default_value);
    }
    
    void PyTimeSeriesDictOutput::clear() {
        auto* impl = static_cast<TimeSeriesDictOutput*>(_impl.get());
        impl->clear();
    }
    
    nb::object PyTimeSeriesDictOutput::modified_keys() const {
        auto* impl = static_cast<TimeSeriesDictOutput*>(_impl.get());
        return impl->py_modified_keys();
    }
    
    nb::object PyTimeSeriesDictOutput::modified_values() const {
        auto* impl = static_cast<TimeSeriesDictOutput*>(_impl.get());
        auto cb = _impl.control_block();
        return build_tsd_values_from_iterable<TimeSeriesOutput*>(impl->py_modified_values(),
            [&](TimeSeriesOutput* ptr) { return wrap_output(ptr, cb); });
    }
    
    nb::object PyTimeSeriesDictOutput::modified_items() const {
        auto* impl = static_cast<TimeSeriesDictOutput*>(_impl.get());
        auto cb = _impl.control_block();
        return build_tsd_output_items_from_iterable(impl->py_modified_items(), cb);
    }

    nb::object PyTimeSeriesDictOutput::removed_keys() const {
        auto* impl = static_cast<TimeSeriesDictOutput*>(_impl.get());
        return impl->py_removed_keys();
    }

    nb::object PyTimeSeriesDictOutput::removed_values() const {
        auto* impl = static_cast<TimeSeriesDictOutput*>(_impl.get());
        auto cb = _impl.control_block();
        return build_tsd_values_from_iterable<TimeSeriesOutput*>(impl->py_removed_values(),
            [&](TimeSeriesOutput* ptr) { return wrap_output(ptr, cb); });
    }

    nb::object PyTimeSeriesDictOutput::removed_items() const {
        auto* impl = static_cast<TimeSeriesDictOutput*>(_impl.get());
        auto cb = _impl.control_block();
        return build_tsd_output_items_from_iterable(impl->py_removed_items(), cb);
    }

void PyTimeSeriesDictOutput::apply_result(nb::object value) {
    auto* impl = static_cast<TimeSeriesDictOutput*>(_impl.get());

    if (auto* tsd = dynamic_cast<TimeSeriesDictOutput_T<nb::object>*>(impl)) {
        tsd->apply_result(value);
        return;
    } else if (auto* tsd = dynamic_cast<TimeSeriesDictOutput_T<int64_t>*>(impl)) {
        tsd->apply_result(value);
        return;
    } else if (auto* tsd = dynamic_cast<TimeSeriesDictOutput_T<double>*>(impl)) {
        tsd->apply_result(value);
        return;
    } else if (auto* tsd = dynamic_cast<TimeSeriesDictOutput_T<bool>*>(impl)) {
        tsd->apply_result(value);
        return;
    }

    impl->py_set_value(value);
}
    
    nb::object PyTimeSeriesDictOutput::get_ref(nb::object key, nb::object requester) const {
        auto* impl = static_cast<TimeSeriesDictOutput*>(_impl.get());
        auto result = impl->py_get_ref(key, requester);
        // Wrap the result as a time series output
        if (nb::isinstance<nb::ref<TimeSeriesOutput>>(result)) {
            auto ts_output = nb::cast<nb::ref<TimeSeriesOutput>>(result);
            return wrap_output(ts_output.get(), _impl.control_block());
        }
        return result;
    }
    
    void PyTimeSeriesDictOutput::release_ref(nb::object key, nb::object requester) {
        auto* impl = static_cast<TimeSeriesDictOutput*>(_impl.get());
        impl->py_release_ref(key, requester);
    }
    
    nb::object PyTimeSeriesDictOutput::key_set() const {
        auto* impl = static_cast<TimeSeriesDictOutput*>(_impl.get());
        return wrap_output(&impl->key_set(), _impl.control_block());
    }
    
    void PyTimeSeriesDictOutput::_create(nb::object key) {
        auto* impl = static_cast<TimeSeriesDictOutput*>(_impl.get());
        impl->py_create(key);
    }
    
    nb::object PyTimeSeriesDictOutput::get_or_create(nb::object key) {
        auto* impl = static_cast<TimeSeriesDictOutput*>(_impl.get());
        
        // py_get_or_create returns the old Python binding with cached wrapper
        // We need to get the C++ pointer and create a fresh new wrapper
        auto binding = impl->py_get_or_create(key);
        if (binding.is_none()) {
            return binding;
        }

        if (!nb::inst_ptr<PyTimeSeriesOutput>(binding)) {
            throw std::runtime_error("PyTimeSeriesDictOutput.get_or_create: expected PyTimeSeriesOutput wrapper");
        }

        return binding;
    }
    
    void PyTimeSeriesDictOutput::register_with_nanobind(nb::module_& m) {
        nb::class_<PyTimeSeriesDictOutput, PyTimeSeriesOutput>(m, "TimeSeriesDictOutput")
            .def("__getitem__", &PyTimeSeriesDictOutput::get_item)
            .def("__setitem__", &PyTimeSeriesDictOutput::set_item)
            .def("__delitem__", &PyTimeSeriesDictOutput::del_item)
            .def("__getattr__", &PyTimeSeriesDictOutput::getattr)
            .def("get", &PyTimeSeriesDictOutput::get, "key"_a, "default"_a = nb::none())
            .def("__contains__", &PyTimeSeriesDictOutput::contains)
            .def("__len__", &PyTimeSeriesDictOutput::len)
            .def("__iter__", &PyTimeSeriesDictOutput::iter)
            .def("keys", &PyTimeSeriesDictOutput::keys)
            .def("values", &PyTimeSeriesDictOutput::values)
            .def("items", &PyTimeSeriesDictOutput::items)
            .def("valid_keys", &PyTimeSeriesDictOutput::valid_keys)
            .def("valid_values", &PyTimeSeriesDictOutput::valid_values)
            .def("valid_items", &PyTimeSeriesDictOutput::valid_items)
            .def("added_keys", &PyTimeSeriesDictOutput::added_keys)
            .def("added_values", &PyTimeSeriesDictOutput::added_values)
            .def("added_items", &PyTimeSeriesDictOutput::added_items)
            .def("pop", &PyTimeSeriesDictOutput::pop, "key"_a, "default"_a = nb::none())
            .def("clear", &PyTimeSeriesDictOutput::clear)
            .def("apply_result", &PyTimeSeriesDictOutput::apply_result)
            .def("modified_keys", &PyTimeSeriesDictOutput::modified_keys)
            .def("modified_values", &PyTimeSeriesDictOutput::modified_values)
            .def("modified_items", &PyTimeSeriesDictOutput::modified_items)
            .def("removed_keys", &PyTimeSeriesDictOutput::removed_keys)
            .def("removed_values", &PyTimeSeriesDictOutput::removed_values)
            .def("removed_items", &PyTimeSeriesDictOutput::removed_items)
            .def("get_ref", &PyTimeSeriesDictOutput::get_ref, "key"_a, "requester"_a)
            .def("release_ref", &PyTimeSeriesDictOutput::release_ref, "key"_a, "requester"_a)
            .def_prop_ro("key_set", &PyTimeSeriesDictOutput::key_set)
            .def("_create", &PyTimeSeriesDictOutput::_create, "key"_a)
            .def("get_or_create", &PyTimeSeriesDictOutput::get_or_create, "key"_a);
    }
    
    // ============================================================================
    // TSS (Set) Types
    // ============================================================================
    
    bool PyTimeSeriesSetInput::contains(nb::object item) const {
        auto* impl = static_cast<TimeSeriesSetInput*>(_impl.get());
        return impl->py_contains(item);
    }
    
    int64_t PyTimeSeriesSetInput::len() const {
        auto* impl = static_cast<TimeSeriesSetInput*>(_impl.get());
        return impl->size();
    }
    
    bool PyTimeSeriesSetInput::empty() const {
        auto* impl = static_cast<TimeSeriesSetInput*>(_impl.get());
        return impl->empty();
    }
    
    nb::object PyTimeSeriesSetInput::values() const {
        auto* impl = static_cast<TimeSeriesSetInput*>(_impl.get());
        return impl->py_values();
    }
    
    nb::object PyTimeSeriesSetInput::added() const {
        auto* impl = static_cast<TimeSeriesSetInput*>(_impl.get());
        return impl->py_added();
    }
    
    nb::object PyTimeSeriesSetInput::removed() const {
        auto* impl = static_cast<TimeSeriesSetInput*>(_impl.get());
        return impl->py_removed();
    }
    
    bool PyTimeSeriesSetInput::was_added(nb::object item) const {
        auto* impl = static_cast<TimeSeriesSetInput*>(_impl.get());
        return impl->py_was_added(item);
    }
    
    bool PyTimeSeriesSetInput::was_removed(nb::object item) const {
        auto* impl = static_cast<TimeSeriesSetInput*>(_impl.get());
        return impl->py_was_removed(item);
    }
    
    void PyTimeSeriesSetInput::register_with_nanobind(nb::module_& m) {
        nb::class_<PyTimeSeriesSetInput, PyTimeSeriesInput>(m, "TimeSeriesSetInput")
            .def("__contains__", &PyTimeSeriesSetInput::contains)
            .def("__len__", &PyTimeSeriesSetInput::len)
            .def("empty", &PyTimeSeriesSetInput::empty)
            .def("values", &PyTimeSeriesSetInput::values)
            .def("added", &PyTimeSeriesSetInput::added)
            .def("removed", &PyTimeSeriesSetInput::removed)
            .def("was_added", &PyTimeSeriesSetInput::was_added)
            .def("was_removed", &PyTimeSeriesSetInput::was_removed);
    }
    
    bool PyTimeSeriesSetOutput::contains(nb::object item) const {
        auto* impl = static_cast<TimeSeriesSetOutput*>(_impl.get());
        return impl->py_contains(item);
    }
    
    int64_t PyTimeSeriesSetOutput::len() const {
        auto* impl = static_cast<TimeSeriesSetOutput*>(_impl.get());
        return impl->size();
    }
    
    bool PyTimeSeriesSetOutput::empty() const {
        auto* impl = static_cast<TimeSeriesSetOutput*>(_impl.get());
        return impl->empty();
    }
    
    nb::object PyTimeSeriesSetOutput::values() const {
        auto* impl = static_cast<TimeSeriesSetOutput*>(_impl.get());
        return impl->py_values();
    }
    
    nb::object PyTimeSeriesSetOutput::added() const {
        auto* impl = static_cast<TimeSeriesSetOutput*>(_impl.get());
        return impl->py_added();
    }
    
    nb::object PyTimeSeriesSetOutput::removed() const {
        auto* impl = static_cast<TimeSeriesSetOutput*>(_impl.get());
        return impl->py_removed();
    }
    
    bool PyTimeSeriesSetOutput::was_added(nb::object item) const {
        auto* impl = static_cast<TimeSeriesSetOutput*>(_impl.get());
        return impl->py_was_added(item);
    }
    
    bool PyTimeSeriesSetOutput::was_removed(nb::object item) const {
        auto* impl = static_cast<TimeSeriesSetOutput*>(_impl.get());
        return impl->py_was_removed(item);
    }
    
    void PyTimeSeriesSetOutput::add(nb::object item) {
        auto* impl = static_cast<TimeSeriesSetOutput*>(_impl.get());
        impl->py_add(item);
    }
    
    void PyTimeSeriesSetOutput::remove(nb::object item) {
        auto* impl = static_cast<TimeSeriesSetOutput*>(_impl.get());
        impl->py_remove(item);
    }
    
    nb::object PyTimeSeriesSetOutput::is_empty_output() {
        auto* impl = static_cast<TimeSeriesSetOutput*>(_impl.get());
        auto& output_ref = impl->is_empty_output();
        return wrap_output(output_ref.get(), _impl.control_block());
    }
    
    nb::object PyTimeSeriesSetOutput::get_contains_output(nb::object item, nb::object requester) {
        auto* impl = static_cast<TimeSeriesSetOutput*>(_impl.get());
        auto result = impl->get_contains_output(item, requester);
        return wrap_output(result.get(), _impl.control_block());
    }
    
    void PyTimeSeriesSetOutput::release_contains_output(nb::object item, nb::object requester) {
        auto* impl = static_cast<TimeSeriesSetOutput*>(_impl.get());
        impl->release_contains_output(item, requester);
    }
    
    void PyTimeSeriesSetOutput::register_with_nanobind(nb::module_& m) {
        nb::class_<PyTimeSeriesSetOutput, PyTimeSeriesOutput>(m, "TimeSeriesSetOutput")
            .def("__contains__", &PyTimeSeriesSetOutput::contains)
            .def("__len__", &PyTimeSeriesSetOutput::len)
            .def("empty", &PyTimeSeriesSetOutput::empty)
            .def("values", &PyTimeSeriesSetOutput::values)
            .def("added", &PyTimeSeriesSetOutput::added)
            .def("removed", &PyTimeSeriesSetOutput::removed)
            .def("was_added", &PyTimeSeriesSetOutput::was_added, "item"_a)
            .def("was_removed", &PyTimeSeriesSetOutput::was_removed, "item"_a)
            .def("add", &PyTimeSeriesSetOutput::add)
            .def("remove", &PyTimeSeriesSetOutput::remove)
            .def("is_empty_output", &PyTimeSeriesSetOutput::is_empty_output)
            .def("get_contains_output", &PyTimeSeriesSetOutput::get_contains_output, "item"_a, "requester"_a)
            .def("release_contains_output", &PyTimeSeriesSetOutput::release_contains_output, "item"_a, "requester"_a);
    }
    
    // ============================================================================
    // TSW (Window) Types
    // NOTE: TSW types are templates, so full implementation requires template dispatch
    // For now, providing basic interface - proper implementation via dynamic dispatch TODO
    // ============================================================================
    
    namespace {
        template<typename T>
        int64_t try_len(TimeSeriesInput* input) {
            if (auto* window_input = dynamic_cast<TimeSeriesWindowInput<T>*>(input)) {
                if (auto* fixed = window_input->as_fixed_output()) {
                    return static_cast<int64_t>(fixed->len());
                }
                if (auto* time = window_input->as_time_output()) {
                    return static_cast<int64_t>(time->len());
                }
            }
            return -1; // Not a window input of this type
        }
    }
    
    int64_t PyTimeSeriesWindowInput::len() const {
        auto* impl = _impl.get();
        int64_t result = try_len<bool>(impl);
        if (result >= 0) return result;
        result = try_len<int64_t>(impl);
        if (result >= 0) return result;
        result = try_len<double>(impl);
        if (result >= 0) return result;
        result = try_len<engine_date_t>(impl);
        if (result >= 0) return result;
        result = try_len<engine_time_t>(impl);
        if (result >= 0) return result;
        result = try_len<engine_time_delta_t>(impl);
        if (result >= 0) return result;
        result = try_len<nb::object>(impl);
        if (result >= 0) return result;
        throw std::runtime_error("TimeSeriesWindowInput: unable to determine length - output is not a window output");
    }
    
    nb::object PyTimeSeriesWindowInput::values() const {
        // Uses base py_value() which should work for all TS types
        return _impl->py_value();
    }
    
    nb::object PyTimeSeriesWindowInput::times() const {
        // TODO: Need to cast to proper template type to access times
        return nb::list();
    }
    
    void PyTimeSeriesWindowInput::register_with_nanobind(nb::module_& m) {
        nb::class_<PyTimeSeriesWindowInput, PyTimeSeriesInput>(m, "TimeSeriesWindowInput")
            .def("__len__", &PyTimeSeriesWindowInput::len)
            .def("values", &PyTimeSeriesWindowInput::values)
            .def("times", &PyTimeSeriesWindowInput::times)
            .def_prop_ro("has_removed_value", &PyTimeSeriesWindowInput::has_removed_value)
            .def_prop_ro("removed_value", &PyTimeSeriesWindowInput::removed_value);
    }
    
    int64_t PyTimeSeriesWindowOutput::len() const {
        // TODO: TSW are template types - need proper template dispatch
        return 0;
    }
    
    int64_t PyTimeSeriesWindowOutput::size() const {
        // TODO: TSW are template types - need proper template dispatch
        return 0;
    }
    
    int64_t PyTimeSeriesWindowOutput::min_size() const {
        // TODO: TSW are template types - need proper template dispatch
        return 0;
    }
    
    nb::object PyTimeSeriesWindowOutput::values() const {
        // Uses base py_value() which should work for all TS types
        return _impl->py_value();
    }
    
    nb::object PyTimeSeriesWindowOutput::times() const {
        // TODO: Need to cast to proper template type to access times
        return nb::list();
    }
    
    void PyTimeSeriesWindowOutput::register_with_nanobind(nb::module_& m) {
        nb::class_<PyTimeSeriesWindowOutput, PyTimeSeriesOutput>(m, "TimeSeriesWindowOutput")
            .def("__len__", &PyTimeSeriesWindowOutput::len)
            .def_prop_ro("size", &PyTimeSeriesWindowOutput::size)
            .def_prop_ro("min_size", &PyTimeSeriesWindowOutput::min_size)
            .def("values", &PyTimeSeriesWindowOutput::values)
            .def("times", &PyTimeSeriesWindowOutput::times);
    }
    
    // ============================================================================
    // REF (Reference) Types
    // ============================================================================
    
    nb::object PyTimeSeriesReferenceInput::get_item(int64_t index) const {
        auto* impl = static_cast<TimeSeriesReferenceInput*>(_impl.get());
        auto* item = impl->get_input(static_cast<size_t>(index));
        return wrap_input(item, _impl.control_block());
    }
    
    void PyTimeSeriesReferenceInput::register_with_nanobind(nb::module_& m) {
        nb::class_<PyTimeSeriesReferenceInput, PyTimeSeriesInput>(m, "TimeSeriesReferenceInput")
            .def("__getitem__", &PyTimeSeriesReferenceInput::get_item);
    }
    
    void PyTimeSeriesReferenceOutput::observe_reference(nb::object input) {
        auto* impl = static_cast<TimeSeriesReferenceOutput*>(_impl.get());
        // Accept raw TimeSeriesInput::ptr from wiring code
        auto ts_input = nb::cast<nb::ref<TimeSeriesInput>>(input);
        impl->observe_reference(ts_input);
    }
    
    void PyTimeSeriesReferenceOutput::stop_observing_reference(nb::object input) {
        auto* impl = static_cast<TimeSeriesReferenceOutput*>(_impl.get());
        auto ts_input = nb::cast<nb::ref<TimeSeriesInput>>(input);
        impl->stop_observing_reference(ts_input);
    }
    
    void PyTimeSeriesReferenceOutput::clear() {
        auto* impl = static_cast<TimeSeriesReferenceOutput*>(_impl.get());
        impl->clear();
    }
    
    void PyTimeSeriesReferenceOutput::register_with_nanobind(nb::module_& m) {
        nb::class_<PyTimeSeriesReferenceOutput, PyTimeSeriesOutput>(m, "TimeSeriesReferenceOutput")
            .def("observe_reference", &PyTimeSeriesReferenceOutput::observe_reference, "input"_a)
            .def("stop_observing_reference", &PyTimeSeriesReferenceOutput::stop_observing_reference, "input"_a)
            .def("clear", &PyTimeSeriesReferenceOutput::clear);
    }
    
} // namespace hgraph::api

