#ifndef HGRAPH_RUNTIME_REDUCE_OUTPUT_BINDING_H
#define HGRAPH_RUNTIME_REDUCE_OUTPUT_BINDING_H

#include <hgraph/runtime/nested_bindings.h>

#include <stdexcept>
#include <utility>
#include <vector>

namespace hgraph::runtime_detail {
[[nodiscard]] inline TSEndpointSchema
reduce_output_endpoint_schema(const TSValueTypeMetaData *schema) {
  return forwarding_output_endpoint_schema(schema);
}

inline void clear_reduce_output(TSOutputView target) {
  static_cast<void>(clear_forwarding_output_tree(std::move(target), true));
}

inline void bind_reduce_output(TSOutputView target, const TSOutputView &source,
                               DateTime evaluation_time) {
  static_cast<void>(evaluation_time);
  static_cast<void>(
      bind_forwarding_output_tree_to_source(std::move(target), source, true));
}
} // namespace hgraph::runtime_detail

#endif // HGRAPH_RUNTIME_REDUCE_OUTPUT_BINDING_H
