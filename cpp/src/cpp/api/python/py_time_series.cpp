//
// PyTimeSeriesInput/Output implementation - Base time series wrappers
//

#include <hgraph/api/python/py_time_series.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/types/node.h>

namespace nb = nanobind;
using namespace nb::literals;

namespace hgraph::api {
    
    // ============================================================================
    // PyTimeSeriesInput
    // ============================================================================
    
    PyTimeSeriesInput::PyTimeSeriesInput(TimeSeriesInput* impl, control_block_ptr control_block)
        : _impl(impl, std::move(control_block)) {}
    
    nb::object PyTimeSeriesInput::owning_node() const {
        return wrap_node(_impl->owning_node(), _impl.control_block());
    }
    
    nb::object PyTimeSeriesInput::parent_input() const {
        auto parent = _impl->parent_input();
        return wrap_input(parent.get(), _impl.control_block());
    }
    
    bool PyTimeSeriesInput::has_parent_input() const {
        return _impl->has_parent_input();
    }
    
engine_time_t PyTimeSeriesInput::last_modified_time() const {
    return _impl->last_modified_time();
}

bool PyTimeSeriesInput::valid() const {
    return is_valid() && _impl->valid();
}

bool PyTimeSeriesInput::modified() const {
    return _impl->modified();
}
    
    bool PyTimeSeriesInput::all_valid() const {
        return _impl->all_valid();
    }
    
    nb::object PyTimeSeriesInput::value() const {
        return _impl->py_value();
    }
    
    nb::object PyTimeSeriesInput::delta_value() const {
        return _impl->py_delta_value();
    }
    
    bool PyTimeSeriesInput::active() const {
        return _impl->active();
    }
    
    void PyTimeSeriesInput::make_active() {
        _impl->make_active();
    }
    
    void PyTimeSeriesInput::make_passive() {
        _impl->make_passive();
    }
    
    bool PyTimeSeriesInput::bound() const {
        return _impl->bound();
    }
    
    bool PyTimeSeriesInput::has_peer() const {
        return _impl->has_peer();
    }
    
    nb::object PyTimeSeriesInput::output() const {
        auto out = _impl->output();
        return wrap_output(out.get(), _impl.control_block());
    }
    
    bool PyTimeSeriesInput::bind_output(nb::object output) {
        // Check if this is a PyTimeSeriesOutput wrapper
        if (nb::isinstance<PyTimeSeriesOutput>(output)) {
            auto& py_output = nb::cast<PyTimeSeriesOutput&>(output);
            auto* raw_ptr = py_output._impl.get();
            
            // The C++ object is managed by intrusive_base, so we can safely
            // create a ref that will keep it alive
            // Important: we must inc_ref to ensure the object stays alive
            // while bound, otherwise it could be deleted if Python wrapper is GC'd
            time_series_output_ptr output_ref(raw_ptr);
            return _impl->bind_output(output_ref);
        }
        
        // Fallback to direct cast for old bindings (already a nb::ref<TimeSeriesOutput>)
        return _impl->bind_output(nb::cast<time_series_output_ptr>(output));
    }
    
    void PyTimeSeriesInput::un_bind_output(bool unbind_refs) {
        _impl->un_bind_output(unbind_refs);
    }
    
    bool PyTimeSeriesInput::is_reference() const {
        return _impl->is_reference();
    }
    
    std::string PyTimeSeriesInput::str() const {
        return fmt::format("TimeSeriesInput@{:p}", static_cast<const void*>(_impl.get()));
    }
    
    std::string PyTimeSeriesInput::repr() const {
        return str();
    }
    
    void PyTimeSeriesInput::register_with_nanobind(nb::module_& m) {
        nb::class_<PyTimeSeriesInput>(m, "TimeSeriesInput")
            .def_prop_ro("owning_node", &PyTimeSeriesInput::owning_node)
            .def_prop_ro("parent_input", &PyTimeSeriesInput::parent_input)
            .def_prop_ro("has_parent_input", &PyTimeSeriesInput::has_parent_input)
            .def_prop_ro("last_modified_time", &PyTimeSeriesInput::last_modified_time)
            .def_prop_ro("valid", &PyTimeSeriesInput::valid)
            .def_prop_ro("modified", &PyTimeSeriesInput::modified)
            .def_prop_ro("all_valid", &PyTimeSeriesInput::all_valid)
            .def_prop_ro("value", &PyTimeSeriesInput::value)
            .def_prop_ro("delta_value", &PyTimeSeriesInput::delta_value)
            .def_prop_ro("active", &PyTimeSeriesInput::active)
            .def("make_active", &PyTimeSeriesInput::make_active)
            .def("make_passive", &PyTimeSeriesInput::make_passive)
            .def_prop_ro("bound", &PyTimeSeriesInput::bound)
            .def_prop_ro("has_peer", &PyTimeSeriesInput::has_peer)
            .def_prop_ro("output", &PyTimeSeriesInput::output)
            .def("bind_output", &PyTimeSeriesInput::bind_output, "output"_a)
            .def("un_bind_output", &PyTimeSeriesInput::un_bind_output, "unbind_refs"_a = false)
            .def("is_reference", &PyTimeSeriesInput::is_reference)
            .def("__str__", &PyTimeSeriesInput::str)
            .def("__repr__", &PyTimeSeriesInput::repr);
    }
    
    // ============================================================================
    // PyTimeSeriesOutput
    // ============================================================================
    
    PyTimeSeriesOutput::PyTimeSeriesOutput(TimeSeriesOutput* impl, control_block_ptr control_block)
        : _impl(impl, std::move(control_block)) {}
    
    nb::object PyTimeSeriesOutput::owning_node() const {
        return wrap_node(_impl->owning_node(), _impl.control_block());
    }
    
    nb::object PyTimeSeriesOutput::parent_output() const {
        auto parent = _impl->parent_output();
        return wrap_output(parent.get(), _impl.control_block());
    }
    
    bool PyTimeSeriesOutput::has_parent_output() const {
        return _impl->has_parent_output();
    }
    
engine_time_t PyTimeSeriesOutput::last_modified_time() const {
    return _impl->last_modified_time();
}

bool PyTimeSeriesOutput::valid() const {
    return _impl->valid();
}

bool PyTimeSeriesOutput::modified() const {
    return _impl->modified();
}

bool PyTimeSeriesOutput::all_valid() const {
    return _impl->all_valid();
}
    
    nb::object PyTimeSeriesOutput::value() const {
        return _impl->py_value();
    }
    
    void PyTimeSeriesOutput::set_value(nb::object value) {
        _impl->py_set_value(std::move(value));
    }
    
    nb::object PyTimeSeriesOutput::delta_value() const {
        return _impl->py_delta_value();
    }
    
    void PyTimeSeriesOutput::invalidate() {
        _impl->invalidate();
    }
    
    void PyTimeSeriesOutput::copy_from_output(const PyTimeSeriesOutput& output) {
        _impl->copy_from_output(*output._impl);
    }
    
    void PyTimeSeriesOutput::copy_from_input(const PyTimeSeriesInput& input) {
        _impl->copy_from_input(*input._impl);
    }
    
    bool PyTimeSeriesOutput::is_reference() const {
        return _impl->is_reference();
    }
    
    std::string PyTimeSeriesOutput::str() const {
        return fmt::format("TimeSeriesOutput@{:p}", static_cast<const void*>(_impl.get()));
    }
    
    std::string PyTimeSeriesOutput::repr() const {
        return str();
    }
    
    void PyTimeSeriesOutput::register_with_nanobind(nb::module_& m) {
        nb::class_<PyTimeSeriesOutput>(m, "TimeSeriesOutput")
            .def_prop_ro("owning_node", &PyTimeSeriesOutput::owning_node)
            .def_prop_ro("parent_output", &PyTimeSeriesOutput::parent_output)
            .def_prop_ro("has_parent_output", &PyTimeSeriesOutput::has_parent_output)
            .def_prop_ro("last_modified_time", &PyTimeSeriesOutput::last_modified_time)
            .def_prop_ro("valid", &PyTimeSeriesOutput::valid)
            .def_prop_ro("modified", &PyTimeSeriesOutput::modified)
            .def_prop_ro("all_valid", &PyTimeSeriesOutput::all_valid)
            .def_prop_rw("value", &PyTimeSeriesOutput::value, &PyTimeSeriesOutput::set_value)
            .def_prop_ro("delta_value", &PyTimeSeriesOutput::delta_value)
            .def("invalidate", &PyTimeSeriesOutput::invalidate)
            .def("copy_from_output", &PyTimeSeriesOutput::copy_from_output, "output"_a)
            .def("copy_from_input", &PyTimeSeriesOutput::copy_from_input, "input"_a)
            .def("is_reference", &PyTimeSeriesOutput::is_reference)
            .def("__str__", &PyTimeSeriesOutput::str)
            .def("__repr__", &PyTimeSeriesOutput::repr);
    }
    
} // namespace hgraph::api

