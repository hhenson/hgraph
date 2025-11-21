#pragma once

#include <hgraph/nodes/last_value_pull_node.h>
#include <hgraph/api/python/py_node.h>

namespace hgraph
{

 struct HGRAPH_EXPORT PyLastValuePullNode : PyNode
 {
     using PyNode::PyNode;

     void copy_from_input(const nb::handle &input);

     void copy_from_output(const nb::handle &output);

 private:
     [[nodiscard]] LastValuePullNode *impl();
 };

    void register_special_nodes_with_nanobind(nb::module_ &m);
}
