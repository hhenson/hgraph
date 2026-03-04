#include <hgraph/nodes/nest_graph_node.h>
#include <hgraph/nodes/nested_node.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/nodes/tsd_map_node.h>
#include <hgraph/nodes/reduce_node.h>
#include <hgraph/nodes/component_node.h>
#include <hgraph/nodes/switch_node.h>
#include <hgraph/nodes/try_except_node.h>
#include <hgraph/nodes/non_associative_reduce_node.h>
#include <hgraph/nodes/mesh_node.h>
#include <hgraph/nodes/last_value_pull_node.h>
#include <hgraph/nodes/context_node.h>
#include <hgraph/nodes/python_generator_node.h>
#include <hgraph/nodes/push_queue_node.h>
#include <hgraph/python/cpp_node_builder_binding.h>
#include <hgraph/types/graph.h>

namespace {
    struct CppNoopSpec {
        static constexpr const char* py_factory_name = "_cpp_noop_builder";
        static void eval(hgraph::Node&) {}
    };

    struct CppConstDefaultSpec {
        static constexpr const char* py_factory_name = "op_const_default";

        struct state {
            bool emitted{false};
            hgraph::engine_time_t emit_time{hgraph::MIN_DT};
            nb::object value{};
        };

        static state make_state(hgraph::Node& node) {
            state out{};
            const nb::dict& scalars = node.scalars();
            if (!scalars.contains("value")) {
                throw std::runtime_error("const_default requires scalar 'value'");
            }
            out.value = nb::cast<nb::object>(scalars["value"]);

            hgraph::engine_time_delta_t delay = hgraph::MIN_TD;
            if (scalars.contains("delay")) {
                nb::object delay_obj = nb::cast<nb::object>(scalars["delay"]);
                if (!delay_obj.is_none()) {
                    delay = nb::cast<hgraph::engine_time_delta_t>(delay_obj);
                }
            }

            out.emit_time = node.graph()->evaluation_engine_api()->start_time() + delay;
            return out;
        }

        static void start(hgraph::Node& node, state& state) {
            if (state.emitted) {
                return;
            }
            const auto now = node.graph()->evaluation_time();
            if (state.emit_time <= now) {
                node.notify(now);
                return;
            }
            node.scheduler()->schedule(state.emit_time, std::string{"const_default"});
        }

        static void stop(hgraph::Node& node, state&) {
            if (node.has_scheduler()) {
                node.scheduler()->un_schedule("const_default");
            }
        }

        static void eval(hgraph::Node& node, state& state) {
            if (state.emitted) {
                return;
            }

            if (node.graph()->evaluation_time() < state.emit_time) {
                node.scheduler()->schedule(state.emit_time, std::string{"const_default"});
                return;
            }

            auto output_view = node.output();
            if (!output_view) {
                throw std::runtime_error("const_default requires TS output");
            }

            output_view.from_python(state.value);
            state.emitted = true;
            if (node.has_scheduler()) {
                node.scheduler()->un_schedule("const_default");
            }
        }
    };
}

void export_nodes(nb::module_ &m) {
    using namespace hgraph;

    bind_cpp_node_builder_factories(m, CppNodeSpecList<CppNoopSpec, CppConstDefaultSpec>{});
}
