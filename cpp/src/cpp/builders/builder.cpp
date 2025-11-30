#include <hgraph/builders/builder.h>

namespace hgraph {
    // Global debug flag for arena allocation debugging
    bool arena_debug_mode = false;

    void Builder::register_with_nanobind(nb::module_ &m) {
        nb::class_<Builder, nb::intrusive_base>(m, "Builder");
    }
} // namespace hgraph