#pragma once

#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>

#include <stdexcept>
#include <string>

namespace hgraph {
    namespace ops {
        struct ConstDefaultSpec {
            static constexpr const char* py_factory_name = "op_const_default";

            struct state {
                bool emitted{false};
                engine_time_t emit_time{MIN_DT};
                nb::object value{};
            };

            static state make_state(Node& node) {
                state out{};
                const nb::dict& scalars = node.scalars();
                if (!scalars.contains("value")) {
                    throw std::runtime_error("const_default requires scalar 'value'");
                }
                out.value = nb::cast<nb::object>(scalars["value"]);

                engine_time_delta_t delay = MIN_TD;
                if (scalars.contains("delay")) {
                    nb::object delay_obj = nb::cast<nb::object>(scalars["delay"]);
                    if (!delay_obj.is_none()) {
                        delay = nb::cast<engine_time_delta_t>(delay_obj);
                    }
                }

                out.emit_time = node.graph()->evaluation_engine_api()->start_time() + delay;
                return out;
            }

            static void start(Node& node, state& state) {
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

            static void stop(Node& node, state&) {
                if (node.has_scheduler()) {
                    node.scheduler()->un_schedule("const_default");
                }
            }

            static void eval(Node& node, state& state) {
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
    }  // namespace ops
}  // namespace hgraph

