#pragma once

#include <hgraph/types/node.h>

namespace hgraph {
    namespace ops {
        struct NothingSpec {
            static constexpr const char* py_factory_name = "op_nothing_impl";
            static void eval(Node&) {}
        };
    }  // namespace ops
}  // namespace hgraph
