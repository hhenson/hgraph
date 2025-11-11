//
// Specialized time series type implementations
//

#include <hgraph/api/python/py_ts_types.h>
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
    
    PyTimeSeriesValueInput::PyTimeSeriesValueInput(TimeSeriesValueInput* impl, control_block_ptr control_block)
        : PyTimeSeriesInput(impl, std::move(control_block)) {}
    
    nb::object PyTimeSeriesValueInput::value() const {
        auto* impl = static_cast<TimeSeriesValueInput*>(_impl.get());
        return impl->value();
    }
    
    nb::object PyTimeSeriesValueInput::delta_value() const {
        auto* impl = static_cast<TimeSeriesValueInput*>(_impl.get());
        return impl->delta_value();
    }
    
    void PyTimeSeriesValueInput::register_with_nanobind(nb::module_& m) {
        nb::class_<PyTimeSeriesValueInput, PyTimeSeriesInput>(m, "TimeSeriesValueInput")
            .def_prop_ro("value", &PyTimeSeriesValueInput::value)
            .def_prop_ro("delta_value", &PyTimeSeriesValueInput::delta_value);
    }
    
    PyTimeSeriesValueOutput::PyTimeSeriesValueOutput(TimeSeriesValueOutput* impl, control_block_ptr control_block)
        : PyTimeSeriesOutput(impl, std::move(control_block)) {}
    
    void PyTimeSeriesValueOutput::register_with_nanobind(nb::module_& m) {
        nb::class_<PyTimeSeriesValueOutput, PyTimeSeriesOutput>(m, "TimeSeriesValueOutput");
    }
    
    // ============================================================================
    // Signal Types
    // ============================================================================
    
    PyTimeSeriesSignalInput::PyTimeSeriesSignalInput(TimeSeriesSignalInput* impl, control_block_ptr control_block)
        : PyTimeSeriesInput(impl, std::move(control_block)) {}
    
    void PyTimeSeriesSignalInput::register_with_nanobind(nb::module_& m) {
        nb::class_<PyTimeSeriesSignalInput, PyTimeSeriesInput>(m, "TimeSeriesSignalInput");
    }
    
    PyTimeSeriesSignalOutput::PyTimeSeriesSignalOutput(TimeSeriesSignalOutput* impl, control_block_ptr control_block)
        : PyTimeSeriesOutput(impl, std::move(control_block)) {}
    
    void PyTimeSeriesSignalOutput::set_value() {
        auto* impl = static_cast<TimeSeriesSignalOutput*>(_impl.get());
        impl->set_value();
    }
    
    void PyTimeSeriesSignalOutput::register_with_nanobind(nb::module_& m) {
        nb::class_<PyTimeSeriesSignalOutput, PyTimeSeriesOutput>(m, "TimeSeriesSignalOutput")
            .def("set_value", &PyTimeSeriesSignalOutput::set_value);
    }
    
    // ============================================================================
    // TSL (List) Types
    // ============================================================================
    
    PyTimeSeriesListInput::PyTimeSeriesListInput(TimeSeriesListInput* impl, control_block_ptr control_block)
        : PyTimeSeriesInput(impl, std::move(control_block)) {}
    
    nb::object PyTimeSeriesListInput::get_item(int64_t index) const {
        auto* impl = static_cast<TimeSeriesListInput*>(_impl.get());
        // TODO: Wrap in appropriate PyTimeSeriesInput type
        return nb::cast((*impl)[index]);
    }
    
    int64_t PyTimeSeriesListInput::len() const {
        auto* impl = static_cast<TimeSeriesListInput*>(_impl.get());
        return impl->size();
    }
    
    nb::iterator PyTimeSeriesListInput::iter() const {
        // TODO: Implement iterator
        return nb::make_iterator(nb::type<PyTimeSeriesListInput>(), "iterator", nullptr, nullptr);
    }
    
    void PyTimeSeriesListInput::register_with_nanobind(nb::module_& m) {
        nb::class_<PyTimeSeriesListInput, PyTimeSeriesInput>(m, "TimeSeriesListInput")
            .def("__getitem__", &PyTimeSeriesListInput::get_item)
            .def("__len__", &PyTimeSeriesListInput::len)
            .def("__iter__", &PyTimeSeriesListInput::iter);
    }
    
    PyTimeSeriesListOutput::PyTimeSeriesListOutput(TimeSeriesListOutput* impl, control_block_ptr control_block)
        : PyTimeSeriesOutput(impl, std::move(control_block)) {}
    
    nb::object PyTimeSeriesListOutput::get_item(int64_t index) const {
        auto* impl = static_cast<TimeSeriesListOutput*>(_impl.get());
        // TODO: Wrap in appropriate PyTimeSeriesOutput type
        return nb::cast((*impl)[index]);
    }
    
    int64_t PyTimeSeriesListOutput::len() const {
        auto* impl = static_cast<TimeSeriesListOutput*>(_impl.get());
        return impl->size();
    }
    
    nb::iterator PyTimeSeriesListOutput::iter() const {
        // TODO: Implement iterator
        return nb::make_iterator(nb::type<PyTimeSeriesListOutput>(), "iterator", nullptr, nullptr);
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
    
    PyTimeSeriesBundleInput::PyTimeSeriesBundleInput(TimeSeriesBundleInput* impl, control_block_ptr control_block)
        : PyTimeSeriesInput(impl, std::move(control_block)) {}
    
    nb::object PyTimeSeriesBundleInput::get_item(nb::object key) const {
        auto* impl = static_cast<TimeSeriesBundleInput*>(_impl.get());
        // TODO: Wrap in appropriate PyTimeSeriesInput type
        // Handle both string and int keys
        if (nb::isinstance<nb::str>(key)) {
            return nb::cast((*impl)[nb::cast<std::string>(key)]);
        } else {
            return nb::cast((*impl)[nb::cast<int64_t>(key)]);
        }
    }
    
    int64_t PyTimeSeriesBundleInput::len() const {
        auto* impl = static_cast<TimeSeriesBundleInput*>(_impl.get());
        return impl->size();
    }
    
    nb::iterator PyTimeSeriesBundleInput::iter() const {
        // TODO: Implement iterator
        return nb::make_iterator(nb::type<PyTimeSeriesBundleInput>(), "iterator", nullptr, nullptr);
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
    
    PyTimeSeriesBundleOutput::PyTimeSeriesBundleOutput(TimeSeriesBundleOutput* impl, control_block_ptr control_block)
        : PyTimeSeriesOutput(impl, std::move(control_block)) {}
    
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
        // TODO: Implement iterator
        return nb::make_iterator(nb::type<PyTimeSeriesBundleOutput>(), "iterator", nullptr, nullptr);
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
    
    PyTimeSeriesDictInput::PyTimeSeriesDictInput(TimeSeriesDictInput* impl, control_block_ptr control_block)
        : PyTimeSeriesInput(impl, std::move(control_block)) {}
    
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
    
    PyTimeSeriesDictOutput::PyTimeSeriesDictOutput(TimeSeriesDictOutput* impl, control_block_ptr control_block)
        : PyTimeSeriesOutput(impl, std::move(control_block)) {}
    
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
    
    PyTimeSeriesSetInput::PyTimeSeriesSetInput(TimeSeriesSetInput* impl, control_block_ptr control_block)
        : PyTimeSeriesInput(impl, std::move(control_block)) {}
    
    bool PyTimeSeriesSetInput::contains(nb::object item) const { return false; }
    int64_t PyTimeSeriesSetInput::len() const { return 0; }
    nb::iterator PyTimeSeriesSetInput::iter() const {
        return nb::make_iterator(nb::type<PyTimeSeriesSetInput>(), "iterator", nullptr, nullptr);
    }
    nb::object PyTimeSeriesSetInput::added() const { return nb::set(); }
    nb::object PyTimeSeriesSetInput::removed() const { return nb::set(); }
    
    void PyTimeSeriesSetInput::register_with_nanobind(nb::module_& m) {
        nb::class_<PyTimeSeriesSetInput, PyTimeSeriesInput>(m, "TimeSeriesSetInput");
    }
    
    PyTimeSeriesSetOutput::PyTimeSeriesSetOutput(TimeSeriesSetOutput* impl, control_block_ptr control_block)
        : PyTimeSeriesOutput(impl, std::move(control_block)) {}
    
    bool PyTimeSeriesSetOutput::contains(nb::object item) const { return false; }
    int64_t PyTimeSeriesSetOutput::len() const { return 0; }
    nb::iterator PyTimeSeriesSetOutput::iter() const {
        return nb::make_iterator(nb::type<PyTimeSeriesSetOutput>(), "iterator", nullptr, nullptr);
    }
    void PyTimeSeriesSetOutput::add(nb::object item) {}
    void PyTimeSeriesSetOutput::remove(nb::object item) {}
    
    void PyTimeSeriesSetOutput::register_with_nanobind(nb::module_& m) {
        nb::class_<PyTimeSeriesSetOutput, PyTimeSeriesOutput>(m, "TimeSeriesSetOutput");
    }
    
    PyTimeSeriesWindowInput::PyTimeSeriesWindowInput(TimeSeriesWindowInput* impl, control_block_ptr control_block)
        : PyTimeSeriesInput(impl, std::move(control_block)) {}
    
    nb::object PyTimeSeriesWindowInput::get_item(int64_t index) const { return nb::none(); }
    int64_t PyTimeSeriesWindowInput::len() const { return 0; }
    nb::iterator PyTimeSeriesWindowInput::iter() const {
        return nb::make_iterator(nb::type<PyTimeSeriesWindowInput>(), "iterator", nullptr, nullptr);
    }
    nb::object PyTimeSeriesWindowInput::times() const { return nb::list(); }
    nb::object PyTimeSeriesWindowInput::values() const { return nb::list(); }
    
    void PyTimeSeriesWindowInput::register_with_nanobind(nb::module_& m) {
        nb::class_<PyTimeSeriesWindowInput, PyTimeSeriesInput>(m, "TimeSeriesWindowInput");
    }
    
    PyTimeSeriesWindowOutput::PyTimeSeriesWindowOutput(TimeSeriesWindowOutput* impl, control_block_ptr control_block)
        : PyTimeSeriesOutput(impl, std::move(control_block)) {}
    
    nb::object PyTimeSeriesWindowOutput::get_item(int64_t index) const { return nb::none(); }
    int64_t PyTimeSeriesWindowOutput::len() const { return 0; }
    nb::iterator PyTimeSeriesWindowOutput::iter() const {
        return nb::make_iterator(nb::type<PyTimeSeriesWindowOutput>(), "iterator", nullptr, nullptr);
    }
    void PyTimeSeriesWindowOutput::append(nb::object value) {}
    
    void PyTimeSeriesWindowOutput::register_with_nanobind(nb::module_& m) {
        nb::class_<PyTimeSeriesWindowOutput, PyTimeSeriesOutput>(m, "TimeSeriesWindowOutput");
    }
    
    PyTimeSeriesReferenceInput::PyTimeSeriesReferenceInput(TimeSeriesReferenceInput* impl, control_block_ptr control_block)
        : PyTimeSeriesInput(impl, std::move(control_block)) {}
    
    nb::object PyTimeSeriesReferenceInput::value_ref() const { return nb::none(); }
    
    void PyTimeSeriesReferenceInput::register_with_nanobind(nb::module_& m) {
        nb::class_<PyTimeSeriesReferenceInput, PyTimeSeriesInput>(m, "TimeSeriesReferenceInput");
    }
    
    PyTimeSeriesReferenceOutput::PyTimeSeriesReferenceOutput(TimeSeriesReferenceOutput* impl, control_block_ptr control_block)
        : PyTimeSeriesOutput(impl, std::move(control_block)) {}
    
    void PyTimeSeriesReferenceOutput::set_value_ref(nb::object ref) {}
    
    void PyTimeSeriesReferenceOutput::register_with_nanobind(nb::module_& m) {
        nb::class_<PyTimeSeriesReferenceOutput, PyTimeSeriesOutput>(m, "TimeSeriesReferenceOutput");
    }
    
} // namespace hgraph::api

