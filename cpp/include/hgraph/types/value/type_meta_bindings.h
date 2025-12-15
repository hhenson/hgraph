//
// Created by Claude on 15/12/2025.
//

#ifndef HGRAPH_TYPE_META_BINDINGS_H
#define HGRAPH_TYPE_META_BINDINGS_H

#include <nanobind/nanobind.h>

namespace hgraph::value {
    void register_type_meta_with_nanobind(nanobind::module_ &m);
}

#endif // HGRAPH_TYPE_META_BINDINGS_H
