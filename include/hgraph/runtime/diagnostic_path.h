#ifndef HGRAPH_RUNTIME_DIAGNOSTIC_PATH_H
#define HGRAPH_RUNTIME_DIAGNOSTIC_PATH_H

#include <hgraph/hgraph_export.h>

#include <string>

namespace hgraph {
class GraphView;
class NodeView;

namespace diagnostic {
/** Stable display label for a graph schema. */
[[nodiscard]] HGRAPH_EXPORT std::string graph_label(const GraphView &graph);
/** Stable display label for a runtime node and its wiring label. */
[[nodiscard]] HGRAPH_EXPORT std::string node_label(const NodeView &node);
/** Structural path for a root or nested runtime graph. */
[[nodiscard]] HGRAPH_EXPORT std::string graph_path(const GraphView &graph);
/** Structural path for one runtime node. */
[[nodiscard]] HGRAPH_EXPORT std::string node_path(const NodeView &node);
} // namespace diagnostic
} // namespace hgraph

#endif // HGRAPH_RUNTIME_DIAGNOSTIC_PATH_H
