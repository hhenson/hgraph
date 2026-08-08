#ifndef HGRAPH_CPP_TIME_SERIES_ENDPOINT_SCHEMA_H
#define HGRAPH_CPP_TIME_SERIES_ENDPOINT_SCHEMA_H

#include <hgraph/types/metadata/ts_value_type_meta_data.h>

#include <cstddef>
#include <cstdint>
#include <vector>

namespace hgraph
{
    /**
     * Runtime endpoint role overlaid on a time-series schema.
     *
     * The role is deliberately not input-specific. Inputs, REF links, and
     * other endpoint-facing structures all need the same distinction: a
     * structural non-peered prefix, or the peered terminal reached from that
     * prefix.
     */
    enum class TSEndpointRole : std::uint8_t
    {
        Peered,
        NonPeered,
        Owned,
    };

    /**
     * Annotated time-series schema used by endpoint plan factories.
     *
     * ``schema`` remains the canonical ``TSValueTypeMetaData`` shape. The
     * endpoint annotation records how runtime endpoint state is constructed for
     * each level of that schema: a non-peered collection prefix or a peered
     * terminal. Once traversal reaches a peered node, that entire subtree is
     * associated with one output peering. An owned terminal uses ordinary local
     * TSData storage for the whole remaining subtree.
     */
    class HGRAPH_EXPORT TSEndpointSchema
    {
      public:
        TSEndpointSchema() noexcept;

        /** Peered terminal for ``schema``; no child annotation is needed. */
        [[nodiscard]] static TSEndpointSchema peered(const TSValueTypeMetaData *schema);

        /** Owned terminal for ``schema``; ordinary TSData storage is used. */
        [[nodiscard]] static TSEndpointSchema owned(const TSValueTypeMetaData *schema);

        /**
         * Non-peered TSB, fixed-size TSL, or dynamic TSD prefix with explicit child
         * endpoint annotations.
         *
         * TSB children are ordered by field index. Fixed TSL children are
         * ordered by list index. A fixed TSL has one homogeneous element
         * schema, but each index may still have a different endpoint
         * topology. For example, one TSL index may be peered as a whole,
         * while another index with the same element schema may remain
         * non-peered and expose deeper peered descendants.
         *
         * A dynamic TSL or TSD has one child annotation: the endpoint topology
         * used for every element/value slot. Use ``non_peered_list`` when
         * every TSL element has the same annotation and ``non_peered_dict``
         * for a dynamic TSD.
         */
        [[nodiscard]] static TSEndpointSchema non_peered(
            const TSValueTypeMetaData       *schema,
            std::vector<TSEndpointSchema>    children);

        /**
         * Non-peered TSL prefix where each list index uses the same element
         * annotation. Fixed lists expand it once per index; dynamic lists
         * retain one annotation for every runtime-created element.
         */
        [[nodiscard]] static TSEndpointSchema non_peered_list(
            const TSValueTypeMetaData *schema,
            const TSEndpointSchema    &element);

        /** Non-peered dynamic TSD prefix with one endpoint annotation for every value slot. */
        [[nodiscard]] static TSEndpointSchema non_peered_dict(
            const TSValueTypeMetaData *schema,
            const TSEndpointSchema    &element);

        [[nodiscard]] bool empty() const noexcept;
        [[nodiscard]] TSEndpointRole role() const noexcept;
        [[nodiscard]] const TSValueTypeMetaData *schema() const noexcept;
        [[nodiscard]] bool is_peered() const noexcept;
        [[nodiscard]] bool is_non_peered() const noexcept;
        [[nodiscard]] bool is_owned() const noexcept;

        [[nodiscard]] std::size_t child_count() const noexcept;
        [[nodiscard]] const TSEndpointSchema &child(std::size_t index) const;
        [[nodiscard]] const std::vector<TSEndpointSchema> &children() const noexcept;

      private:
        TSEndpointSchema(TSEndpointRole             role,
                         const TSValueTypeMetaData *schema,
                         std::vector<TSEndpointSchema> children);

        TSEndpointRole                role_{TSEndpointRole::Peered};
        const TSValueTypeMetaData    *schema_{nullptr};
        std::vector<TSEndpointSchema> children_{};
    };

    /** Structural equality for canonical or equivalent time-series schemas. */
    [[nodiscard]] HGRAPH_EXPORT bool time_series_schema_equivalent(
        const TSValueTypeMetaData *lhs, const TSValueTypeMetaData *rhs) noexcept;
}  // namespace hgraph

#endif  // HGRAPH_CPP_TIME_SERIES_ENDPOINT_SCHEMA_H
