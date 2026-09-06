#ifndef HGRAPH_TYPES_VALUE_IMPL_POOLED_POLYMORPHIC_VALUE_TYPE_H
#define HGRAPH_TYPES_VALUE_IMPL_POOLED_POLYMORPHIC_VALUE_TYPE_H

#include <hgraph/config.h>
#include <hgraph/types/metadata/value_type_meta_data.h>
#include <hgraph/types/python_object.h>
#include <hgraph/types/value/polymorphic_value_type.h>

#include <vector>

namespace hgraph::detail {
/** Which realized alternative a Python source selects (RFC 0035: an opaque
    reference in, so the type layer names no Python; the resolver installed
    is the provider's forwarder over the entry's alternatives view). */
struct PolymorphicPythonSourceResolver {
  const void *context{nullptr};
  ValueTypeRef (*resolve)(const void *context, PyRef source){nullptr};
};

[[nodiscard]] PolymorphicValueType
make_pooled_polymorphic_value_type(const ValueTypeMetaData *schema,
                                   std::vector<ValueTypeRef> alternatives,
                                   PolymorphicPythonSourceResolver python_source);
} // namespace hgraph::detail

#endif // HGRAPH_TYPES_VALUE_IMPL_POOLED_POLYMORPHIC_VALUE_TYPE_H
