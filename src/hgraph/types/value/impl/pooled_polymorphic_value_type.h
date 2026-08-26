#ifndef HGRAPH_TYPES_VALUE_IMPL_POOLED_POLYMORPHIC_VALUE_TYPE_H
#define HGRAPH_TYPES_VALUE_IMPL_POOLED_POLYMORPHIC_VALUE_TYPE_H

#include <hgraph/config.h>
#include <hgraph/types/metadata/value_type_meta_data.h>
#include <hgraph/types/value/compound_scalar_storage.h>
#include <hgraph/types/value/polymorphic_value_type.h>

#include <vector>

#if HGRAPH_ENABLE_PYTHON_USER_NODES
#include <nanobind/nanobind.h>

namespace nb = nanobind;
#endif

namespace hgraph::detail {
#if HGRAPH_ENABLE_PYTHON_USER_NODES
struct PolymorphicPythonSourceResolver {
  const void *context{nullptr};
  ValueTypeRef (*resolve)(const void *context, nb::handle source){nullptr};
};
#endif

[[nodiscard]] PolymorphicValueType
make_pooled_polymorphic_value_type(const ValueTypeMetaData *schema,
                                   const CompoundScalarStorageBinding *pool_binding,
                                   std::vector<ValueTypeRef> alternatives
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                                   ,
                                   PolymorphicPythonSourceResolver python_source
#endif
);
} // namespace hgraph::detail

#endif // HGRAPH_TYPES_VALUE_IMPL_POOLED_POLYMORPHIC_VALUE_TYPE_H
