#include <hgraph/nodes/python_node.h>

namespace hgraph {
    const nb::callable &PythonNode::eval_fn() { return _eval_fn; }
} // namespace hgraph