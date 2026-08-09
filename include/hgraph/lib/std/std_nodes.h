#ifndef HGRAPH_LIB_STD_STD_NODES_H
#define HGRAPH_LIB_STD_STD_NODES_H

#include <hgraph/types/static_node.h>
#include <hgraph/types/time_series/ts_delta.h>

namespace hgraph::stdlib
{
    /**
     * A small standard library of reusable C++ nodes. These are ordinary static nodes
     * (see *Authoring Nodes in C++*).
     *
     * .. note::
     *
     *    ``const_`` / ``debug_print`` / ``null_sink`` were previously concrete nodes here;
     *    they are now **operators** (their definitions are in ``operators/`` and their
     *    implementations in ``operators/impl/``) to match the Python target API. Wire them
     *    through ``stdlib::const_`` / ``debug_print`` / ``null_sink`` after calling
     *    ``register_standard_operators()``.
     */

    /**
     * Pass-through node: emits exactly the input delta. This mirrors Python's
     * ``pass_through_node`` and is useful both for tests and for forcing a real
     * runtime node into a graph without changing the data.
     */
    struct pass_through_node
    {
        static constexpr auto name = "pass_through_node";

        static void eval(In<"ts", TsVar<"S">> ts, Out<TsVar<"S">> out)
        {
            const Value delta = capture_delta(ts.base());
            apply_delta(out, delta.view());
        }
    };
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_STD_NODES_H
