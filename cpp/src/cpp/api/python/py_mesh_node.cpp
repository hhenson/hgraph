#include <hgraph/nodes/mesh_node.h>
#include <hgraph/python/format.h>


namespace hgraph {

    void register_mesh_node_with_nanobind(nb::module_ &m) {
        tp::for_each(ts_payload_types_v, [&m]<typename T>(tp::unit<T> tp) {
            // Register MeshNode specializations
            nb::class_<MeshNode<T>, TsdMapNode<T>>(m, format_py_typename("MeshNode", tp).c_str())
                .def("_add_graph_dependency", &MeshNode<T>::_add_graph_dependency, "key"_a, "depends_on"_a)
                .def("_remove_graph_dependency", &MeshNode<T>::_remove_graph_dependency, "key"_a, "depends_on"_a);

            // Register MeshNestedEngineEvaluationClock specializations with 'key' property so Python can discover mesh keys
            nb::class_<MeshNestedEngineEvaluationClock<T>, NestedEngineEvaluationClock>(
                m, format_py_typename("MeshNestedEngineEvaluationClock", tp).c_str())
                .def_prop_ro("key", &MeshNestedEngineEvaluationClock<T>::key);
        });
    }

} // namespace hgraph
