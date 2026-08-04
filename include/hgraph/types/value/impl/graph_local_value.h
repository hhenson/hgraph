#ifndef HGRAPH_TYPES_VALUE_IMPL_GRAPH_LOCAL_VALUE_H
#define HGRAPH_TYPES_VALUE_IMPL_GRAPH_LOCAL_VALUE_H

#include <hgraph/types/value/value.h>

#include <stdexcept>

namespace hgraph::value_impl
{
    /** Runtime-only owner that deliberately preserves graph-local bindings. */
    class GraphLocalValueAccess
    {
      public:
        [[nodiscard]] static Value copy(const ValueView &source)
        {
            const auto binding = source.binding();
            if (!binding)
            {
                throw std::invalid_argument(
                    "graph-local value copy requires a bound source");
            }
            Value result;
            result.storage_ = source.has_value()
                                  ? Value::storage_type::owning_copy(
                                        *binding.record(), source.data())
                                  : Value::storage_type::empty(*binding.record());
            return result;
        }
    };

    [[nodiscard]] inline Value graph_local_value(const ValueView &source)
    {
        return GraphLocalValueAccess::copy(source);
    }
}

#endif // HGRAPH_TYPES_VALUE_IMPL_GRAPH_LOCAL_VALUE_H
