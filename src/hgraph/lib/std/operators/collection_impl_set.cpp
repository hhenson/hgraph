#include <hgraph/lib/std/operators/impl/collection_impl.h>

namespace hgraph::stdlib
{
    // TSS / TSD set algebra: the logical and bitwise spellings, the named
    // union_ / intersection_ / difference_ / symmetric_difference_ folds and
    // the binary TSD forms.
    void register_collection_set_overloads()
    {
        register_overload<not_, collection_impl_detail::not_tss>();
        register_overload<not_, collection_impl_detail::not_tsd>();
        register_overload<and_, collection_impl_detail::and_tss>();
        register_overload<or_, collection_impl_detail::or_tss>();
        register_overload<eq_, collection_impl_detail::eq_tss>();
        register_overload<eq_, collection_impl_detail::eq_tsd>();
        register_overload<eq_, collection_impl_detail::eq_tsd_epsilon>();

        register_graph_overload<union_, collection_impl_detail::union_tss_fold>();
        register_graph_overload<intersection_, collection_impl_detail::intersection_tss_fold>();
        register_graph_overload<difference_, collection_impl_detail::difference_tss_fold>();
        register_graph_overload<symmetric_difference_, collection_impl_detail::symmetric_difference_tss_fold>();

        register_graph_overload<bit_or, collection_impl_detail::union_tss_fold>();
        register_graph_overload<bit_and, collection_impl_detail::intersection_tss_fold>();
        register_graph_overload<sub_, collection_impl_detail::difference_tss_fold>();
        register_graph_overload<bit_xor, collection_impl_detail::symmetric_difference_tss_fold>();

        register_overload<bit_or, collection_impl_detail::union_tsd_binary>();
        register_overload<bit_and, collection_impl_detail::intersection_tsd_binary>();
        register_overload<sub_, collection_impl_detail::difference_tsd_binary>();
        register_overload<bit_xor, collection_impl_detail::symmetric_difference_tsd_binary>();
    }
}  // namespace hgraph::stdlib
