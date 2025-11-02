#include <hgraph/builders/builder.h>

namespace hgraph {
    void Builder::register_with_nanobind(nb::module_ &m) {
        nb::class_<Builder, nb::intrusive_base>(m, "Builder");
    }
} // namespace hgraph