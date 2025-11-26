#include <hgraph/builders/builder.h>
#include <hgraph/builders/input_builder.h>
#include <hgraph/builders/output_builder.h>
#include <hgraph/builders/node_builder.h>
#include <hgraph/builders/graph_builder.h>

void export_builders(nb::module_ &m) {
    using namespace hgraph;
    Builder::register_with_nanobind(m);
    OutputBuilder::register_with_nanobind(m);
    InputBuilder::register_with_nanobind(m);
    NodeBuilder::register_with_nanobind(m);
    GraphBuilder::register_with_nanobind(m);

    // Expose arena debug mode flag
    m.def("set_arena_debug_mode", [](bool enabled) { arena_debug_mode = enabled; },
          "Enable or disable arena allocation debug mode (adds canary values to detect buffer overruns)");
    m.def("get_arena_debug_mode", []() { return arena_debug_mode; },
          "Get the current arena allocation debug mode setting");
    m.attr("ARENA_CANARY_PATTERN") = ARENA_CANARY_PATTERN;
}