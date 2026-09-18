#ifndef HGRAPH_RUNTIME_DISTRIBUTED_BOUNDARY_H
#define HGRAPH_RUNTIME_DISTRIBUTED_BOUNDARY_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/value/binary_codec.h>
#include <cstddef>
#include <memory>
#include <optional>
#include <string_view>

namespace hgraph
{
    class TSInputView;
    class TSOutputView;
    struct TSValueTypeMetaData;
}

namespace hgraph::distributed
{
    /** Schema-compiled transport for a materialized time-series tree.
     * Ordinary cycles visit only modified children; a newly observed/rebound
     * subtree carries its complete state once. The payload owns bytes, never
     * references, and preserves invalid collection members and window history.
     * Build at wiring time and retain the plan: capture/apply perform no schema
     * registration or converter-cache lookup.
     */
    class HGRAPH_CLASS_EXPORT BoundaryTransfer
    {
      public:
        explicit BoundaryTransfer(const TSValueTypeMetaData *schema, BinaryDecodeLimits limits = {});
        /** ``needed_by`` names what is being wired -- "dmap_ input 2" -- so that
         * a scalar with no wire form is refused here, at wiring, by a message
         * that says what needed one (RFC 0040). */
        BoundaryTransfer(const TSValueTypeMetaData *schema, std::string_view needed_by, BinaryDecodeLimits limits = {});
        [[nodiscard]] const TSValueTypeMetaData *schema() const noexcept;
        /** The transport envelope is an opaque byte string on every boundary. */
        [[nodiscard]] static const ValueTypeMetaData *payload_schema();
        /** Stable process-independent partition hash for a TSS member/TSD key. */
        [[nodiscard]] std::size_t key_hash(const ValueView &key) const;
        /** A nonzero groups filters root dictionary keys by hash and root list
         * indices by ordinal. Children are never filtered recursively. */
        [[nodiscard]] Value capture(const TSInputView &input, bool full = false,
                                    std::size_t group = 0, std::size_t groups = 0) const;
        /** Decode and validate the entire payload before changing any endpoint.
         * Decode limits also bound sparse list extents, which can be much
         * larger than their encoded payload. Allocation failure during commit
         * is fatal to the run; this is not a rollback of runtime mutations.
         * merge retains entries belonging to other workers at the root.
         * Root dynamic-list length is returned separately for the caller to
         * reconcile once every worker has replied. */
        void apply(const TSOutputView &output, const ValueView &payload, bool merge = false) const;
        [[nodiscard]] std::optional<std::size_t> root_list_size(const ValueView &payload) const;

      private:
        struct Plan;
        std::shared_ptr<const Plan> plan_;
        BinaryDecodeLimits limits_;
    };
    using BoundaryTransferPtr = std::shared_ptr<const BoundaryTransfer>;
}

#endif
