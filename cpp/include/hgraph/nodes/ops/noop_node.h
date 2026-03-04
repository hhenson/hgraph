#pragma once

#include <hgraph/types/node.h>

namespace hgraph {
    namespace ops {
        struct NoopSpec {
            static constexpr const char* py_factory_name = "_cpp_noop_builder";
            static void eval(Node&) {}
        };
    }  // namespace ops
}  // namespace hgraph

