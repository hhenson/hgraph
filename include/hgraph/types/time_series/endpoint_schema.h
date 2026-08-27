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
        Local,
    };

    /**
     * Annotated time-series schema used by endpoint plan factories.
     *
     * ``schema`` remains the canonical ``TSValueTypeMetaData`` shape. The
     * endpoint annotation records how runtime endpoint state is constructed for
     * each level of that schema: a non-peered collection prefix or a peered
     * terminal. Once traversal reaches a peered node, that entire subtree is
     * associated with one output peering. A ``Local`` terminal instead holds
     * its own TSData storage for the whole remaining subtree.
     *
     * ``Local`` answers "who owns the time-series STATE" — this endpoint, or
     * the output it is peered with. It is unrelated to ``ValueTypeFlags::Owned``,
     * which answers "how are a scalar's BYTES laid out"; the two are chosen by
     * different factories from different inputs, and a peered endpoint over a
     * carried value is an ordinary combination.
     */
    class HGRAPH_EXPORT TSEndpointSchema
    {
      public:
        TSEndpointSchema() noexcept;

        /** Peered terminal for ``schema``; no child annotation is needed. */
        [[nodiscard]] static TSEndpointSchema peered(const TSValueTypeMetaData *schema);

        /** Local terminal for ``schema``; it holds its own TSData storage. */
        [[nodiscard]] static TSEndpointSchema local(const TSValueTypeMetaData *schema);

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
        [[nodiscard]] bool is_local() const noexcept;

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

    /**
     * A value schema with its STORAGE CATEGORY removed, for type comparison.
     *
     * ``Owned<>`` and ``Shared<>`` are hints to the layout factory and to
     * memory management: the first says a field is held behind one owner
     * pointer (its declared type's closed union contains the leaf that embeds
     * it, so a flat layout would recurse), the second that an allocation is
     * reference counted. Neither says anything about type identity, so **type
     * resolution ignores them everywhere** — the same transparency REF already
     * has when schemas are compared.
     *
     * They differ from REF in one way that matters: a storage category is not
     * an *alternative*. There is nothing to bind, present, or choose between,
     * because the two schemas are the same type. Comparison normalises through
     * the category and the question never arises.
     *
     * Stripping loops, so a category stacked over another reduces to the type
     * underneath. Returns ``schema`` unchanged when it carries no category.
     */
    [[nodiscard]] HGRAPH_EXPORT const ValueTypeMetaData *value_schema_without_storage(
        const ValueTypeMetaData *schema) noexcept;
}  // namespace hgraph

#endif  // HGRAPH_CPP_TIME_SERIES_ENDPOINT_SCHEMA_H
