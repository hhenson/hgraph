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
}