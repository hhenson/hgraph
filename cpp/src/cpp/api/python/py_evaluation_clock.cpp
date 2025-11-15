//
// PyEvaluationClock Implementation
//

#include <hgraph/api/python/py_evaluation_clock.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/nodes/mesh_node.h>
#include <hgraph/runtime/evaluation_engine.h>
#include <hgraph/types/constants.h>
#include <fmt/format.h>

namespace hgraph::api {
    
    PyEvaluationClock::PyEvaluationClock(EvaluationClock* impl, control_block_ptr control_block)
        : _impl(impl, std::move(control_block)) {
    }
    
    nb::object PyEvaluationClock::evaluation_time() const {
        return nb::cast(_impl->evaluation_time());
    }
    
    nb::object PyEvaluationClock::now() const {
        return nb::cast(_impl->now());
    }
    
    nb::object PyEvaluationClock::next_cycle_evaluation_time() const {
        return nb::cast(_impl->next_cycle_evaluation_time());
    }
    
    nb::object PyEvaluationClock::cycle_time() const {
        return nb::cast(_impl->cycle_time());
    }
    
    nb::object PyEvaluationClock::node() const {
        auto* mutable_impl = const_cast<hgraph::EvaluationClock*>(_impl.get());
        if (auto* nested_clock = dynamic_cast<hgraph::NestedEngineEvaluationClock*>(mutable_impl)) {
            auto nested_node = nested_clock->node();
            if (nested_node.get() != nullptr) {
                return wrap_node(nested_node.get(), _impl.control_block());
            }
        }
        return nb::none();
    }
    
    nb::object PyEvaluationClock::key() const {
        auto* mutable_impl = const_cast<hgraph::EvaluationClock*>(_impl.get());
        if (auto* nested_clock = dynamic_cast<hgraph::NestedEngineEvaluationClock*>(mutable_impl)) {
            // Try all mesh clock types
            if (auto* mesh_bool = dynamic_cast<hgraph::MeshNestedEngineEvaluationClock<bool>*>(nested_clock)) {
                return nb::cast(mesh_bool->key());
            }
            if (auto* mesh_int = dynamic_cast<hgraph::MeshNestedEngineEvaluationClock<int64_t>*>(nested_clock)) {
                return nb::cast(mesh_int->key());
            }
            if (auto* mesh_double = dynamic_cast<hgraph::MeshNestedEngineEvaluationClock<double>*>(nested_clock)) {
                return nb::cast(mesh_double->key());
            }
            if (auto* mesh_date = dynamic_cast<hgraph::MeshNestedEngineEvaluationClock<engine_date_t>*>(nested_clock)) {
                return nb::cast(mesh_date->key());
            }
            if (auto* mesh_time = dynamic_cast<hgraph::MeshNestedEngineEvaluationClock<engine_time_t>*>(nested_clock)) {
                return nb::cast(mesh_time->key());
            }
            if (auto* mesh_delta = dynamic_cast<hgraph::MeshNestedEngineEvaluationClock<engine_time_delta_t>*>(nested_clock)) {
                return nb::cast(mesh_delta->key());
            }
            if (auto* mesh_obj = dynamic_cast<hgraph::MeshNestedEngineEvaluationClock<nb::object>*>(nested_clock)) {
                return nb::cast(mesh_obj->key());
            }
        }
        return nb::none();
    }
    
    std::string PyEvaluationClock::str() const {
        return fmt::format("EvaluationClock@{:p}[eval_time={}]",
                         static_cast<const void*>(_impl.get()),
                         _impl->evaluation_time().time_since_epoch().count());
    }
    
    std::string PyEvaluationClock::repr() const {
        return str();
    }
    
    void PyEvaluationClock::register_with_nanobind(nb::module_& m) {
        nb::class_<PyEvaluationClock>(m, "EvaluationClock",
            "Python wrapper for EvaluationClock - provides access to evaluation clock functionality")
            .def_prop_ro("evaluation_time", &PyEvaluationClock::evaluation_time)
            .def_prop_ro("now", &PyEvaluationClock::now)
            .def_prop_ro("next_cycle_evaluation_time", &PyEvaluationClock::next_cycle_evaluation_time)
            .def_prop_ro("cycle_time", &PyEvaluationClock::cycle_time)
            .def_prop_ro("node", &PyEvaluationClock::node,
                "The nested node for nested clocks (None for base clocks)")
            .def_prop_ro("key", &PyEvaluationClock::key,
                "The mesh key for mesh nested clocks (None for non-mesh clocks)")
            .def("is_valid", &PyEvaluationClock::is_valid,
                "Check if this wrapper is valid (graph is alive and wrapper has value)")
            .def("__str__", &PyEvaluationClock::str)
            .def("__repr__", &PyEvaluationClock::repr);
    }
    
} // namespace hgraph::api

