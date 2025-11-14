//
// PyNode implementation - Python API wrapper for Node
//

#include <hgraph/api/python/py_node.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/node.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/types/tsb.h>  // For TimeSeriesBundleInput
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>
#include <hgraph/nodes/last_value_pull_node.h>
#include <hgraph/nodes/mesh_node.h>
#include <fmt/format.h>
#include <stdexcept>

namespace nb = nanobind;
using namespace nb::literals;

namespace hgraph::api {
    
    PyNode::PyNode(Node* impl, control_block_ptr control_block)
        : _impl(impl, std::move(control_block)) {}
    
    int64_t PyNode::node_ndx() const {
        return _impl->node_ndx();
    }
    
    nb::tuple PyNode::owning_graph_id() const {
        const auto& vec = _impl->owning_graph_id();
        nb::list py_list;
        for (const auto& id : vec) {
            py_list.append(id);
        }
        return nb::tuple(py_list);
    }
    
    nb::tuple PyNode::node_id() const {
        const auto vec = _impl->node_id();
        nb::list py_list;
        for (const auto& id : vec) {
            py_list.append(id);
        }
        return nb::tuple(py_list);
    }
    
    nb::object PyNode::signature() const {
        // TODO: Wrap NodeSignature
        return nb::cast(&_impl->signature());
    }
    
    const nb::dict& PyNode::scalars() const {
        return _impl->scalars();
    }
    
    nb::object PyNode::graph() const {
        return wrap_graph(_impl->graph(), _impl.control_block());
    }
    
    nb::object PyNode::input() const {
        auto tsb_ref = _impl->input();
        return wrap_input(tsb_ref.get(), _impl.control_block());
    }
    
    nb::dict PyNode::inputs() const {
        nb::dict d;
        auto inp = *_impl->input();
        for (const auto& key : inp.schema().keys()) {
            // Wrap each input - factory returns cached wrapper if available
            d[key.c_str()] = wrap_input(inp[key], _impl.control_block());
        }
        return d;
    }
    
    nb::object PyNode::output() const {
        // Factory returns cached wrapper if available
        return wrap_output(_impl->output(), _impl.control_block());
    }
    
    nb::object PyNode::recordable_state() const {
        if (!_impl->has_recordable_state()) {
            return nb::none();
        }
        auto state = _impl->recordable_state();
        if (!state) {
            return nb::none();
        }
        return wrap_output(state.get(), _impl.control_block());
    }
    
    nb::object PyNode::scheduler() const {
        return wrap_node_scheduler(_impl->scheduler(), _impl.control_block());
    }
    
    void PyNode::notify(engine_time_t modified_time) {
        _impl->notify(modified_time);
    }
    
    std::string PyNode::str() const {
        return _impl->str();
    }
    
    std::string PyNode::repr() const {
        return _impl->repr();
    }
    
    namespace {
        // Helper functions to call MeshNode methods via function pointers
        template<typename K>
        bool call_add_dependency(MeshNode<K>* mesh, const K& key, const K& depends_on) {
            return mesh->_add_graph_dependency(key, depends_on);
        }
        
        template<typename K>
        void call_remove_dependency(MeshNode<K>* mesh, const K& key, const K& depends_on) {
            mesh->_remove_graph_dependency(key, depends_on);
        }
    }
    
    nb::object PyNode::getattr(nb::handle name) const {
        std::string attr_name = nb::cast<std::string>(nb::str(name));
        
        // Try to access the attribute on the underlying C++ Node
        // This allows access to methods registered directly on specialized node types (e.g., MeshNode)
        auto* mutable_impl = const_cast<Node*>(_impl.get());
        
        // Try MeshNode methods - use nb::cpp_function to properly bind the methods
        if (auto* mesh_bool = dynamic_cast<MeshNode<bool>*>(mutable_impl)) {
            if (attr_name == "_add_graph_dependency") {
                return nb::cpp_function([mesh_bool](bool key, bool depends_on) {
                    return mesh_bool->_add_graph_dependency(key, depends_on);
                }, nb::name("_add_graph_dependency"), nb::is_method());
            }
            if (attr_name == "_remove_graph_dependency") {
                return nb::cpp_function([mesh_bool](bool key, bool depends_on) {
                    mesh_bool->_remove_graph_dependency(key, depends_on);
                }, nb::name("_remove_graph_dependency"), nb::is_method());
            }
        }
        // Try other MeshNode template types
        if (auto* mesh_int = dynamic_cast<MeshNode<int64_t>*>(mutable_impl)) {
            if (attr_name == "_add_graph_dependency") {
                return nb::cpp_function([mesh_int](int64_t key, int64_t depends_on) {
                    return mesh_int->_add_graph_dependency(key, depends_on);
                }, nb::name("_add_graph_dependency"), nb::is_method());
            }
            if (attr_name == "_remove_graph_dependency") {
                return nb::cpp_function([mesh_int](int64_t key, int64_t depends_on) {
                    mesh_int->_remove_graph_dependency(key, depends_on);
                }, nb::name("_remove_graph_dependency"), nb::is_method());
            }
        }
        if (auto* mesh_double = dynamic_cast<MeshNode<double>*>(mutable_impl)) {
            if (attr_name == "_add_graph_dependency") {
                return nb::cpp_function([mesh_double](double key, double depends_on) {
                    return mesh_double->_add_graph_dependency(key, depends_on);
                }, nb::name("_add_graph_dependency"), nb::is_method());
            }
            if (attr_name == "_remove_graph_dependency") {
                return nb::cpp_function([mesh_double](double key, double depends_on) {
                    mesh_double->_remove_graph_dependency(key, depends_on);
                }, nb::name("_remove_graph_dependency"), nb::is_method());
            }
        }
        if (auto* mesh_date = dynamic_cast<MeshNode<engine_date_t>*>(mutable_impl)) {
            if (attr_name == "_add_graph_dependency") {
                return nb::cpp_function([mesh_date](engine_date_t key, engine_date_t depends_on) {
                    return mesh_date->_add_graph_dependency(key, depends_on);
                }, nb::name("_add_graph_dependency"), nb::is_method());
            }
            if (attr_name == "_remove_graph_dependency") {
                return nb::cpp_function([mesh_date](engine_date_t key, engine_date_t depends_on) {
                    mesh_date->_remove_graph_dependency(key, depends_on);
                }, nb::name("_remove_graph_dependency"), nb::is_method());
            }
        }
        if (auto* mesh_time = dynamic_cast<MeshNode<engine_time_t>*>(mutable_impl)) {
            if (attr_name == "_add_graph_dependency") {
                return nb::cpp_function([mesh_time](engine_time_t key, engine_time_t depends_on) {
                    return mesh_time->_add_graph_dependency(key, depends_on);
                }, nb::name("_add_graph_dependency"), nb::is_method());
            }
            if (attr_name == "_remove_graph_dependency") {
                return nb::cpp_function([mesh_time](engine_time_t key, engine_time_t depends_on) {
                    mesh_time->_remove_graph_dependency(key, depends_on);
                }, nb::name("_remove_graph_dependency"), nb::is_method());
            }
        }
        if (auto* mesh_delta = dynamic_cast<MeshNode<engine_time_delta_t>*>(mutable_impl)) {
            if (attr_name == "_add_graph_dependency") {
                return nb::cpp_function([mesh_delta](engine_time_delta_t key, engine_time_delta_t depends_on) {
                    return mesh_delta->_add_graph_dependency(key, depends_on);
                }, nb::name("_add_graph_dependency"), nb::is_method());
            }
            if (attr_name == "_remove_graph_dependency") {
                return nb::cpp_function([mesh_delta](engine_time_delta_t key, engine_time_delta_t depends_on) {
                    mesh_delta->_remove_graph_dependency(key, depends_on);
                }, nb::name("_remove_graph_dependency"), nb::is_method());
            }
        }
        if (auto* mesh_obj = dynamic_cast<MeshNode<nb::object>*>(mutable_impl)) {
            if (attr_name == "_add_graph_dependency") {
                return nb::cpp_function([mesh_obj](nb::object key, nb::object depends_on) {
                    return mesh_obj->_add_graph_dependency(key, depends_on);
                }, nb::name("_add_graph_dependency"), nb::is_method());
            }
            if (attr_name == "_remove_graph_dependency") {
                return nb::cpp_function([mesh_obj](nb::object key, nb::object depends_on) {
                    mesh_obj->_remove_graph_dependency(key, depends_on);
                }, nb::name("_remove_graph_dependency"), nb::is_method());
            }
        }
        
        // Attribute not found
        throw nb::attribute_error(
            fmt::format("'{}' object has no attribute '{}'", "Node", attr_name).c_str());
    }
    
    void PyNode::register_with_nanobind(nb::module_& m) {
        nb::class_<PyNode>(m, "Node")
            .def_prop_ro("node_ndx", &PyNode::node_ndx)
            .def_prop_ro("owning_graph_id", &PyNode::owning_graph_id)
            .def_prop_ro("node_id", &PyNode::node_id)
            .def_prop_ro("signature", &PyNode::signature)
            .def_prop_ro("scalars", &PyNode::scalars)
            .def_prop_ro("graph", &PyNode::graph)
            .def_prop_ro("input", &PyNode::input)
            .def_prop_ro("inputs", &PyNode::inputs)
            .def_prop_ro("output", &PyNode::output)
            .def_prop_ro("recordable_state", &PyNode::recordable_state)
            .def_prop_ro("scheduler", &PyNode::scheduler)
            .def("notify", &PyNode::notify, "modified_time"_a)
            .def("__str__", &PyNode::str)
            .def("__repr__", &PyNode::repr)
            .def("__getattr__", &PyNode::getattr);
    }
    
    PyLastValuePullNode::PyLastValuePullNode(LastValuePullNode* impl, control_block_ptr control_block)
        : PyNode(impl, std::move(control_block))
        , _impl_last_value(static_cast<LastValuePullNode*>(_impl.get()), _impl.control_block()) {}
    
    void PyLastValuePullNode::apply_value(const nb::object& new_value) {
        _impl_last_value->apply_value(new_value);
    }

    void PyLastValuePullNode::copy_from_input(nb::object input) {
        auto* impl_input = unwrap_input(input);
        if (impl_input == nullptr) {
            throw std::runtime_error("LastValuePullNode.copy_from_input: expected TimeSeriesInput");
        }
        _impl_last_value->copy_from_input(*impl_input);
    }

    void PyLastValuePullNode::copy_from_output(nb::object output) {
        auto* impl_output = unwrap_output(output);
        if (impl_output == nullptr) {
            throw std::runtime_error("LastValuePullNode.copy_from_output: expected TimeSeriesOutput");
        }
        _impl_last_value->copy_from_output(*impl_output);
    }
    
    void PyLastValuePullNode::register_with_nanobind(nb::module_& m) {
        nb::class_<PyLastValuePullNode, PyNode>(m, "LastValuePullNode")
            .def("apply_value", &PyLastValuePullNode::apply_value)
            .def("copy_from_input", &PyLastValuePullNode::copy_from_input, "input"_a)
            .def("copy_from_output", &PyLastValuePullNode::copy_from_output, "output"_a);
    }
    
} // namespace hgraph::api

