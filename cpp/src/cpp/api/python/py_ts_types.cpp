//
// Specialized time series type implementations
//

#include <hgraph/api/python/py_ts_types.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/ts.h>
#include <hgraph/types/ts_signal.h>
#include <hgraph/types/ts_indexed.h>
#include <hgraph/types/tsb.h>
#include <hgraph/types/tsd.h>
#include <hgraph/types/tss.h>
#include <hgraph/types/tsw.h>
#include <hgraph/types/ref.h>

namespace nb = nanobind;
using namespace nb::literals;

namespace hgraph::api {
    
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
    // Signal Types
    // ============================================================================
    
    void PyTimeSeriesSignalInput::register_with_nanobind(nb::module_& m) {
        nb::class_<PyTimeSeriesSignalInput, PyTimeSeriesInput>(m, "TimeSeriesSignalInput");
    }
    
    void PyTimeSeriesSignalOutput::set_value() {
        // Signal just calls invalidate to trigger
        _impl->invalidate();
    }
    
    void PyTimeSeriesSignalOutput::register_with_nanobind(nb::module_& m) {
        nb::class_<PyTimeSeriesSignalOutput, PyTimeSeriesOutput>(m, "TimeSeriesSignalOutput")
            .def("set_value", &PyTimeSeriesSignalOutput::set_value);
    }
    
    // ============================================================================
    // TSL (List) Types
    // ============================================================================
    
    nb::object PyTimeSeriesListInput::get_item(int64_t index) const {
        // Use get_input from base TimeSeriesInput
        auto* item = _impl->get_input(static_cast<size_t>(index));
        return wrap_input(item, _impl.control_block());
    }
    
    int64_t PyTimeSeriesListInput::len() const {
        // TODO: Need a size() method on base TimeSeriesInput
        return 0;  // Placeholder
    }
    
    nb::iterator PyTimeSeriesListInput::iter() const {
        // TODO: Implement proper iterator
        throw std::runtime_error("Iterator not yet implemented for TimeSeriesListInput");
    }
    
    void PyTimeSeriesListInput::register_with_nanobind(nb::module_& m) {
        nb::class_<PyTimeSeriesListInput, PyTimeSeriesInput>(m, "TimeSeriesListInput")
            .def("__getitem__", &PyTimeSeriesListInput::get_item)
            .def("__len__", &PyTimeSeriesListInput::len)
            .def("__iter__", &PyTimeSeriesListInput::iter);
    }
    
    nb::object PyTimeSeriesListOutput::get_item(int64_t index) const {
        // TODO: Need operator[] or get_output on base TimeSeriesOutput
        return nb::none();  // Placeholder
    }
    
    int64_t PyTimeSeriesListOutput::len() const {
        // TODO: Need a size() method on base TimeSeriesOutput
        return 0;  // Placeholder
    }
    
    nb::iterator PyTimeSeriesListOutput::iter() const {
        // TODO: Implement proper iterator
        throw std::runtime_error("Iterator not yet implemented for TimeSeriesListOutput");
    }
    
    void PyTimeSeriesListOutput::register_with_nanobind(nb::module_& m) {
        nb::class_<PyTimeSeriesListOutput, PyTimeSeriesOutput>(m, "TimeSeriesListOutput")
            .def("__getitem__", &PyTimeSeriesListOutput::get_item)
            .def("__len__", &PyTimeSeriesListOutput::len)
            .def("__iter__", &PyTimeSeriesListOutput::iter);
    }
    
    // ============================================================================
    // TSB (Bundle) Types
    // ============================================================================
    
    nb::object PyTimeSeriesBundleInput::get_item(nb::object key) const {
        // Handle both string and int keys using base get_input method
        if (nb::isinstance<nb::int_>(key)) {
            auto* item = _impl->get_input(nb::cast<size_t>(key));
            return wrap_input(item, _impl.control_block());
        }
        // TODO: Handle string keys - need a method on base class
        return nb::none();
    }
    
    int64_t PyTimeSeriesBundleInput::len() const {
        // TODO: Need a size() method on base TimeSeriesInput
        return 0;  // Placeholder
    }
    
    nb::iterator PyTimeSeriesBundleInput::iter() const {
        // TODO: Implement proper iterator
        throw std::runtime_error("Iterator not yet implemented for TimeSeriesBundleInput");
    }
    
    nb::object PyTimeSeriesBundleInput::schema() const {
        auto* impl = static_cast<TimeSeriesBundleInput*>(_impl.get());
        // TODO: Wrap schema
        return nb::cast(&impl->schema());
    }
    
    void PyTimeSeriesBundleInput::register_with_nanobind(nb::module_& m) {
        nb::class_<PyTimeSeriesBundleInput, PyTimeSeriesInput>(m, "TimeSeriesBundleInput")
            .def("__getitem__", &PyTimeSeriesBundleInput::get_item)
            .def("__len__", &PyTimeSeriesBundleInput::len)
            .def("__iter__", &PyTimeSeriesBundleInput::iter)
            .def_prop_ro("schema", &PyTimeSeriesBundleInput::schema);
    }
    
    nb::object PyTimeSeriesBundleOutput::get_item(nb::object key) const {
        auto* impl = static_cast<TimeSeriesBundleOutput*>(_impl.get());
        // TODO: Wrap in appropriate PyTimeSeriesOutput type
        if (nb::isinstance<nb::str>(key)) {
            return nb::cast((*impl)[nb::cast<std::string>(key)]);
        } else {
            return nb::cast((*impl)[nb::cast<int64_t>(key)]);
        }
    }
    
    int64_t PyTimeSeriesBundleOutput::len() const {
        auto* impl = static_cast<TimeSeriesBundleOutput*>(_impl.get());
        return impl->size();
    }
    
    nb::iterator PyTimeSeriesBundleOutput::iter() const {
        // TODO: Implement proper iterator
        throw std::runtime_error("Iterator not yet implemented for TimeSeriesBundleOutput");
    }
    
    nb::object PyTimeSeriesBundleOutput::schema() const {
        auto* impl = static_cast<TimeSeriesBundleOutput*>(_impl.get());
        // TODO: Wrap schema
        return nb::cast(&impl->schema());
    }
    
    void PyTimeSeriesBundleOutput::register_with_nanobind(nb::module_& m) {
        nb::class_<PyTimeSeriesBundleOutput, PyTimeSeriesOutput>(m, "TimeSeriesBundleOutput")
            .def("__getitem__", &PyTimeSeriesBundleOutput::get_item)
            .def("__len__", &PyTimeSeriesBundleOutput::len)
            .def("__iter__", &PyTimeSeriesBundleOutput::iter)
            .def_prop_ro("schema", &PyTimeSeriesBundleOutput::schema);
    }
    
    // ============================================================================
    // TSD (Dict) Types - Stubs
    // ============================================================================
    
    nb::object PyTimeSeriesDictInput::get_item(nb::object key) const {
        // TODO: Implement
        return nb::none();
    }
    
    nb::object PyTimeSeriesDictInput::get(nb::object key, nb::object default_value) const {
        // TODO: Implement
        return default_value;
    }
    
    bool PyTimeSeriesDictInput::contains(nb::object key) const {
        // TODO: Implement
        return false;
    }
    
    int64_t PyTimeSeriesDictInput::len() const {
        // TODO: Implement
        return 0;
    }
    
    nb::object PyTimeSeriesDictInput::keys() const {
        // TODO: Implement
        return nb::list();
    }
    
    nb::object PyTimeSeriesDictInput::values() const {
        // TODO: Implement
        return nb::list();
    }
    
    nb::object PyTimeSeriesDictInput::items() const {
        // TODO: Implement
        return nb::list();
    }
    
    nb::object PyTimeSeriesDictInput::valid_keys() const {
        // TODO: Implement
        return nb::list();
    }
    
    nb::object PyTimeSeriesDictInput::added_keys() const {
        // TODO: Implement
        return nb::list();
    }
    
    nb::object PyTimeSeriesDictInput::modified_keys() const {
        // TODO: Implement
        return nb::list();
    }
    
    nb::object PyTimeSeriesDictInput::removed_keys() const {
        // TODO: Implement
        return nb::list();
    }
    
    void PyTimeSeriesDictInput::register_with_nanobind(nb::module_& m) {
        nb::class_<PyTimeSeriesDictInput, PyTimeSeriesInput>(m, "TimeSeriesDictInput")
            .def("__getitem__", &PyTimeSeriesDictInput::get_item)
            .def("get", &PyTimeSeriesDictInput::get)
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
        // TODO: Implement
        return nb::none();
    }
    
    bool PyTimeSeriesDictOutput::contains(nb::object key) const {
        // TODO: Implement
        return false;
    }
    
    int64_t PyTimeSeriesDictOutput::len() const {
        // TODO: Implement
        return 0;
    }
    
    nb::object PyTimeSeriesDictOutput::keys() const {
        // TODO: Implement
        return nb::list();
    }
    
    nb::object PyTimeSeriesDictOutput::values() const {
        // TODO: Implement
        return nb::list();
    }
    
    nb::object PyTimeSeriesDictOutput::items() const {
        // TODO: Implement
        return nb::list();
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
    // TSS, TSW, REF Types - Minimal Stubs
    // ============================================================================
    
    bool PyTimeSeriesSetInput::contains(nb::object item) const { return false; }
    int64_t PyTimeSeriesSetInput::len() const { return 0; }
    nb::iterator PyTimeSeriesSetInput::iter() const {
        throw std::runtime_error("Iterator not yet implemented for TimeSeriesSetInput");
    }
    nb::object PyTimeSeriesSetInput::added() const { return nb::set(); }
    nb::object PyTimeSeriesSetInput::removed() const { return nb::set(); }
    
    void PyTimeSeriesSetInput::register_with_nanobind(nb::module_& m) {
        nb::class_<PyTimeSeriesSetInput, PyTimeSeriesInput>(m, "TimeSeriesSetInput");
    }
    
    bool PyTimeSeriesSetOutput::contains(nb::object item) const { return false; }
    int64_t PyTimeSeriesSetOutput::len() const { return 0; }
    nb::iterator PyTimeSeriesSetOutput::iter() const {
        throw std::runtime_error("Iterator not yet implemented for TimeSeriesSetOutput");
    }
    void PyTimeSeriesSetOutput::add(nb::object item) {}
    void PyTimeSeriesSetOutput::remove(nb::object item) {}
    
    void PyTimeSeriesSetOutput::register_with_nanobind(nb::module_& m) {
        nb::class_<PyTimeSeriesSetOutput, PyTimeSeriesOutput>(m, "TimeSeriesSetOutput");
    }
    
    nb::object PyTimeSeriesWindowInput::get_item(int64_t index) const { return nb::none(); }
    int64_t PyTimeSeriesWindowInput::len() const { return 0; }
    nb::iterator PyTimeSeriesWindowInput::iter() const {
        throw std::runtime_error("Iterator not yet implemented for TimeSeriesWindowInput");
    }
    nb::object PyTimeSeriesWindowInput::times() const { return nb::list(); }
    nb::object PyTimeSeriesWindowInput::values() const { return nb::list(); }
    
    void PyTimeSeriesWindowInput::register_with_nanobind(nb::module_& m) {
        nb::class_<PyTimeSeriesWindowInput, PyTimeSeriesInput>(m, "TimeSeriesWindowInput");
    }
    
    nb::object PyTimeSeriesWindowOutput::get_item(int64_t index) const { return nb::none(); }
    int64_t PyTimeSeriesWindowOutput::len() const { return 0; }
    nb::iterator PyTimeSeriesWindowOutput::iter() const {
        throw std::runtime_error("Iterator not yet implemented for TimeSeriesWindowOutput");
    }
    void PyTimeSeriesWindowOutput::append(nb::object value) {}
    
    void PyTimeSeriesWindowOutput::register_with_nanobind(nb::module_& m) {
        nb::class_<PyTimeSeriesWindowOutput, PyTimeSeriesOutput>(m, "TimeSeriesWindowOutput");
    }
    
    nb::object PyTimeSeriesReferenceInput::value_ref() const { return nb::none(); }
    
    void PyTimeSeriesReferenceInput::register_with_nanobind(nb::module_& m) {
        nb::class_<PyTimeSeriesReferenceInput, PyTimeSeriesInput>(m, "TimeSeriesReferenceInput");
    }
    
    void PyTimeSeriesReferenceOutput::set_value_ref(nb::object ref) {}
    
    void PyTimeSeriesReferenceOutput::register_with_nanobind(nb::module_& m) {
        nb::class_<PyTimeSeriesReferenceOutput, PyTimeSeriesOutput>(m, "TimeSeriesReferenceOutput");
    }
    
} // namespace hgraph::api

