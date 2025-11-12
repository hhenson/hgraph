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

namespace nb = nanobind;
using namespace nb::literals;

namespace hgraph::api {
    
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
        nb::class_<PyTimeSeriesValueOutput, PyTimeSeriesOutput>(m, "TimeSeriesValueOutput");
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
    
    nb::dict PyTimeSeriesListInput::items() const {
        auto* impl = static_cast<TimeSeriesListInput*>(_impl.get());
        nb::dict result;
        for (const auto& [idx, value] : impl->items()) {
            result[nb::int_(idx)] = wrap_input(value.get(), _impl.control_block());
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
    
    nb::dict PyTimeSeriesListInput::valid_items() const {
        auto* impl = static_cast<TimeSeriesListInput*>(_impl.get());
        nb::dict result;
        for (const auto& [idx, value] : impl->valid_items()) {
            result[nb::int_(idx)] = wrap_input(value.get(), _impl.control_block());
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
    
    nb::dict PyTimeSeriesListInput::modified_items() const {
        auto* impl = static_cast<TimeSeriesListInput*>(_impl.get());
        nb::dict result;
        for (const auto& [idx, value] : impl->modified_items()) {
            result[nb::int_(idx)] = wrap_input(value.get(), _impl.control_block());
        }
        return result;
    }
    
    void PyTimeSeriesListInput::register_with_nanobind(nb::module_& m) {
        nb::class_<PyTimeSeriesListInput, PyTimeSeriesInput>(m, "TimeSeriesListInput")
            .def("__getitem__", &PyTimeSeriesListInput::get_item)
            .def("__len__", &PyTimeSeriesListInput::len)
            .def("__iter__", &PyTimeSeriesListInput::iter)
            .def("keys", &PyTimeSeriesListInput::keys)
            .def("items", &PyTimeSeriesListInput::items)
            .def("valid_keys", &PyTimeSeriesListInput::valid_keys)
            .def("valid_items", &PyTimeSeriesListInput::valid_items)
            .def("modified_keys", &PyTimeSeriesListInput::modified_keys)
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
    
    nb::dict PyTimeSeriesListOutput::items() const {
        auto* impl = static_cast<TimeSeriesListOutput*>(_impl.get());
        nb::dict result;
        for (const auto& [idx, value] : impl->items()) {
            result[nb::int_(idx)] = wrap_output(value.get(), _impl.control_block());
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
    
    nb::dict PyTimeSeriesListOutput::valid_items() const {
        auto* impl = static_cast<TimeSeriesListOutput*>(_impl.get());
        nb::dict result;
        for (const auto& [idx, value] : impl->valid_items()) {
            result[nb::int_(idx)] = wrap_output(value.get(), _impl.control_block());
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
    
    nb::dict PyTimeSeriesListOutput::modified_items() const {
        auto* impl = static_cast<TimeSeriesListOutput*>(_impl.get());
        nb::dict result;
        for (const auto& [idx, value] : impl->modified_items()) {
            result[nb::int_(idx)] = wrap_output(value.get(), _impl.control_block());
        }
        return result;
    }
    
    void PyTimeSeriesListOutput::register_with_nanobind(nb::module_& m) {
        nb::class_<PyTimeSeriesListOutput, PyTimeSeriesOutput>(m, "TimeSeriesListOutput")
            .def("__getitem__", &PyTimeSeriesListOutput::get_item)
            .def("__len__", &PyTimeSeriesListOutput::len)
            .def("__iter__", &PyTimeSeriesListOutput::iter)
            .def("keys", &PyTimeSeriesListOutput::keys)
            .def("items", &PyTimeSeriesListOutput::items)
            .def("valid_keys", &PyTimeSeriesListOutput::valid_keys)
            .def("valid_items", &PyTimeSeriesListOutput::valid_items)
            .def("modified_keys", &PyTimeSeriesListOutput::modified_keys)
            .def("modified_items", &PyTimeSeriesListOutput::modified_items);
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
        // Return iterator over keys (matching Python dict behavior)
        nb::list keys;
        for (const auto& key : impl->keys()) {
            keys.append(nb::str(key.get().c_str()));
        }
        return keys.attr("__iter__")();
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
    
    void PyTimeSeriesBundleInput::register_with_nanobind(nb::module_& m) {
        nb::class_<PyTimeSeriesBundleInput, PyTimeSeriesInput>(m, "TimeSeriesBundleInput")
            .def("__getitem__", &PyTimeSeriesBundleInput::get_item)
            .def("__len__", &PyTimeSeriesBundleInput::len)
            .def("__iter__", &PyTimeSeriesBundleInput::iter)
            .def("__contains__", &PyTimeSeriesBundleInput::contains)
            .def("keys", &PyTimeSeriesBundleInput::keys)
            .def("items", &PyTimeSeriesBundleInput::items)
            .def("modified_keys", &PyTimeSeriesBundleInput::modified_keys)
            .def("modified_items", &PyTimeSeriesBundleInput::modified_items)
            .def("valid_keys", &PyTimeSeriesBundleInput::valid_keys)
            .def("valid_items", &PyTimeSeriesBundleInput::valid_items)
            .def_prop_ro("__schema__", &PyTimeSeriesBundleInput::schema);
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
    
    nb::dict PyTimeSeriesBundleOutput::items() const {
        auto* impl = static_cast<TimeSeriesBundleOutput*>(_impl.get());
        nb::dict result;
        for (const auto& [key, value] : impl->items()) {
            result[nb::str(key.get().c_str())] = wrap_output(value.get(), _impl.control_block());
        }
        return result;
    }
    
    nb::list PyTimeSeriesBundleOutput::modified_keys() const {
        auto* impl = static_cast<TimeSeriesBundleOutput*>(_impl.get());
        nb::list result;
        for (const auto& key : impl->modified_keys()) {
            result.append(nb::str(key.get().c_str()));
        }
        return result;
    }
    
    nb::dict PyTimeSeriesBundleOutput::modified_items() const {
        auto* impl = static_cast<TimeSeriesBundleOutput*>(_impl.get());
        nb::dict result;
        for (const auto& [key, value] : impl->modified_items()) {
            result[nb::str(key.get().c_str())] = wrap_output(value.get(), _impl.control_block());
        }
        return result;
    }
    
    nb::list PyTimeSeriesBundleOutput::valid_keys() const {
        auto* impl = static_cast<TimeSeriesBundleOutput*>(_impl.get());
        nb::list result;
        for (const auto& key : impl->valid_keys()) {
            result.append(nb::str(key.get().c_str()));
        }
        return result;
    }
    
    nb::dict PyTimeSeriesBundleOutput::valid_items() const {
        auto* impl = static_cast<TimeSeriesBundleOutput*>(_impl.get());
        nb::dict result;
        for (const auto& [key, value] : impl->valid_items()) {
            result[nb::str(key.get().c_str())] = wrap_output(value.get(), _impl.control_block());
        }
        return result;
    }
    
    nb::object PyTimeSeriesBundleOutput::schema() const {
        auto* impl = static_cast<TimeSeriesBundleOutput*>(_impl.get());
        return nb::cast(&impl->schema());
    }
    
    void PyTimeSeriesBundleOutput::register_with_nanobind(nb::module_& m) {
        nb::class_<PyTimeSeriesBundleOutput, PyTimeSeriesOutput>(m, "TimeSeriesBundleOutput")
            .def("__getitem__", &PyTimeSeriesBundleOutput::get_item)
            .def("__len__", &PyTimeSeriesBundleOutput::len)
            .def("__iter__", &PyTimeSeriesBundleOutput::iter)
            .def("__contains__", &PyTimeSeriesBundleOutput::contains)
            .def("keys", &PyTimeSeriesBundleOutput::keys)
            .def("items", &PyTimeSeriesBundleOutput::items)
            .def("modified_keys", &PyTimeSeriesBundleOutput::modified_keys)
            .def("modified_items", &PyTimeSeriesBundleOutput::modified_items)
            .def("valid_keys", &PyTimeSeriesBundleOutput::valid_keys)
            .def("valid_items", &PyTimeSeriesBundleOutput::valid_items)
            .def_prop_ro("__schema__", &PyTimeSeriesBundleOutput::schema);
    }
    
    // ============================================================================
    // TSD (Dict) Types
    // ============================================================================
    
    nb::object PyTimeSeriesDictInput::get_item(nb::object key) const {
        auto* impl = static_cast<TimeSeriesDictInput*>(_impl.get());
        return impl->py_get_item(key);
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
        return impl->py_values();
    }
    
    nb::object PyTimeSeriesDictInput::items() const {
        auto* impl = static_cast<TimeSeriesDictInput*>(_impl.get());
        return impl->py_items();
    }
    
    nb::object PyTimeSeriesDictInput::valid_keys() const {
        auto* impl = static_cast<TimeSeriesDictInput*>(_impl.get());
        return impl->py_valid_keys();
    }
    
    nb::object PyTimeSeriesDictInput::added_keys() const {
        auto* impl = static_cast<TimeSeriesDictInput*>(_impl.get());
        return impl->py_added_keys();
    }
    
    nb::object PyTimeSeriesDictInput::modified_keys() const {
        auto* impl = static_cast<TimeSeriesDictInput*>(_impl.get());
        return impl->py_modified_keys();
    }
    
    nb::object PyTimeSeriesDictInput::removed_keys() const {
        auto* impl = static_cast<TimeSeriesDictInput*>(_impl.get());
        return impl->py_removed_keys();
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
            .def("added_keys", &PyTimeSeriesDictInput::added_keys)
            .def("modified_keys", &PyTimeSeriesDictInput::modified_keys)
            .def("removed_keys", &PyTimeSeriesDictInput::removed_keys);
    }
    
    nb::object PyTimeSeriesDictOutput::get_item(nb::object key) const {
        auto* impl = static_cast<TimeSeriesDictOutput*>(_impl.get());
        return impl->py_get_item(key);
    }
    
    bool PyTimeSeriesDictOutput::contains(nb::object key) const {
        auto* impl = static_cast<TimeSeriesDictOutput*>(_impl.get());
        return impl->py_contains(key);
    }
    
    int64_t PyTimeSeriesDictOutput::len() const {
        auto* impl = static_cast<TimeSeriesDictOutput*>(_impl.get());
        return impl->size();
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
    
    void PyTimeSeriesDictOutput::register_with_nanobind(nb::module_& m) {
        nb::class_<PyTimeSeriesDictOutput, PyTimeSeriesOutput>(m, "TimeSeriesDictOutput")
            .def("__getitem__", &PyTimeSeriesDictOutput::get_item)
            .def("__contains__", &PyTimeSeriesDictOutput::contains)
            .def("__len__", &PyTimeSeriesDictOutput::len)
            .def("keys", &PyTimeSeriesDictOutput::keys)
            .def("values", &PyTimeSeriesDictOutput::values)
            .def("items", &PyTimeSeriesDictOutput::items);
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
    
    void PyTimeSeriesSetOutput::add(nb::object item) {
        auto* impl = static_cast<TimeSeriesSetOutput*>(_impl.get());
        impl->py_add(item);
    }
    
    void PyTimeSeriesSetOutput::remove(nb::object item) {
        auto* impl = static_cast<TimeSeriesSetOutput*>(_impl.get());
        impl->py_remove(item);
    }
    
    void PyTimeSeriesSetOutput::register_with_nanobind(nb::module_& m) {
        nb::class_<PyTimeSeriesSetOutput, PyTimeSeriesOutput>(m, "TimeSeriesSetOutput")
            .def("__contains__", &PyTimeSeriesSetOutput::contains)
            .def("__len__", &PyTimeSeriesSetOutput::len)
            .def("empty", &PyTimeSeriesSetOutput::empty)
            .def("values", &PyTimeSeriesSetOutput::values)
            .def("add", &PyTimeSeriesSetOutput::add)
            .def("remove", &PyTimeSeriesSetOutput::remove);
    }
    
    // ============================================================================
    // TSW (Window) Types
    // NOTE: TSW types are templates, so full implementation requires template dispatch
    // For now, providing basic interface - proper implementation via dynamic dispatch TODO
    // ============================================================================
    
    int64_t PyTimeSeriesWindowInput::len() const {
        // TODO: TSW are template types - need proper template dispatch
        return 0;
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
            .def("times", &PyTimeSeriesWindowInput::times);
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

