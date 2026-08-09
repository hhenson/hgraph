#include <hgraph/runtime/diagnostic_path.h>

#include <hgraph/runtime/graph.h>
#include <hgraph/runtime/node.h>

#include <algorithm>
#include <string_view>
#include <vector>

namespace hgraph::diagnostic {
std::string graph_label(const GraphView &graph) {
  const auto *schema = graph.schema();
  if (schema == nullptr || schema->name().empty()) {
    return "graph";
  }
  return std::string{schema->name()};
}

std::string node_label(const NodeView &node) {
  const auto *schema = node.schema();
  std::string result = schema != nullptr && !schema->name().empty()
                           ? std::string{schema->name()}
                           : std::string{"node"};
  const std::string_view label = node.label();
  if (!label.empty() && label != result) {
    result += ':';
    result += label;
  }
  return result;
}

std::string graph_path(const GraphView &graph) {
  std::vector<std::string> components;
  GraphView current{graph.pointer()};
  while (current.valid() && current.is_nested()) {
    NodeView parent = current.as_nested().parent_node();
    components.push_back(node_label(parent) + '<' +
                         std::to_string(parent.node_index()) + '>');
    current = parent.graph();
  }
  std::reverse(components.begin(), components.end());

  std::string result{"["};
  for (std::size_t index = 0; index < components.size(); ++index) {
    if (index != 0) {
      result += "::";
    }
    result += components[index];
  }
  result += ']';
  return result;
}

std::string node_path(const NodeView &node) {
  return graph_path(node.graph()) + '.' + node_label(node) + '<' +
         std::to_string(node.node_index()) + '>';
}
} // namespace hgraph::diagnostic
