#include <hgraph/lib/std/operators/impl/string_impl.h>

namespace hgraph::stdlib
{
    void register_string_operators()
    {
        register_overload<match_, match_impl>();
        register_overload<replace, replace_impl>();
        register_overload<substr, substr_impl>();
        register_overload<split, split_tsl_impl>();
        register_overload<split, split_tuple_impl>();
        register_overload<join, join_tsl_impl>();
        register_graph_overload<join, join_multi_impl>();
        register_overload<join, join_tuple_impl>();
        register_graph_overload<format_, format_graph_impl>();
    }
}  // namespace hgraph::stdlib
