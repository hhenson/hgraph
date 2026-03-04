#pragma once

#include <hgraph/types/node.h>

namespace hgraph {
    namespace ops {
        struct NullSinkSpec {
            static constexpr const char* py_factory_name = "op_null_sink_impl";
            static void eval(Node&) {}
        };
    }  // namespace ops
}  // namespace hgraph
