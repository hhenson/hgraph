#ifndef HGRAPH_FABRIC_IMPL_METADATA_BINDING_H
#define HGRAPH_FABRIC_IMPL_METADATA_BINDING_H

#include <hgraph/fabric/config.h>
#include <hgraph/fabric/metadata_codec.h>
#include <hgraph/fabric/publication.h>
#include <hgraph/fabric/resolution.h>

#include <hgraph/persistence/value_store.h>

#include "metadata_value_binding.h"

#include <optional>
#include <span>
#include <stdexcept>
#include <string_view>
#include <utility>

namespace hgraph::fabric::detail
{
    /** One Fabric metadata schema bound to its backend and codec.

        The generic BoundValueStore deliberately has no domain-specific size
        policy. Fabric's historical 16 MiB limit is therefore enforced here on
        every durable read, write and compare/exchange winner before decoding.
        Keeping this wrapper private prevents graph algorithms from bypassing
        that semantic boundary by reaching directly for ObjectStore bytes. */
    class BoundMetadataStore final
    {
      public:
        BoundMetadataStore() noexcept = default;

        BoundMetadataStore(persistence::store::ObjectStore objects,
                           persistence::store::BoundValueCodec codec)
            : objects_{std::move(objects)}, codec_{std::move(codec)}
        {
            if (!objects_ || !codec_)
            {
                throw std::invalid_argument(
                    "fabric metadata store requires a backend and bound codec");
            }
        }

        [[nodiscard]] persistence::store::ImmutableWriteResult
        write(std::string_view key, const ValueView &value) const
        {
            return objects_.put_immutable(key, encode(value));
        }

        [[nodiscard]] std::optional<Value>
        try_read(std::string_view key) const
        {
            auto stored = try_read_versioned(key);
            if (!stored.has_value()) { return std::nullopt; }
            return std::move(stored->value);
        }

        [[nodiscard]] std::optional<persistence::store::StoredValue>
        try_read_versioned(std::string_view key) const
        {
            auto stored = objects_.get(key);
            if (!stored.has_value()) { return std::nullopt; }
            return persistence::store::StoredValue{
                decode(stored->data), std::move(stored->version_token)};
        }

        [[nodiscard]] persistence::store::ValueCompareExchangeResult
        compare_exchange(
            std::string_view key, const ValueView &value,
            std::optional<std::string_view> expected_version) const
        {
            auto result = objects_.compare_exchange_ref(
                key, expected_version, encode(value));
            persistence::store::ValueCompareExchangeResult typed{
                .exchanged = result.exchanged};
            if (result.current.has_value())
            {
                typed.current = persistence::store::StoredValue{
                    decode(result.current->data),
                    std::move(result.current->version_token)};
            }
            return typed;
        }

        [[nodiscard]] persistence::store::ObjectBytes
        encode(const ValueView &value) const
        {
            auto encoded = codec_.encode(value);
            require_metadata_within_limit(encoded.size());
            return encoded;
        }

        [[nodiscard]] Value
        decode(std::span<const std::byte> encoded) const
        {
            require_metadata_within_limit(encoded.size());
            return codec_.decode(encoded);
        }

      private:
        persistence::store::ObjectStore objects_{};
        persistence::store::BoundValueCodec codec_{};
    };

    /** Schema and codec state owned by one running Fabric algorithm.

        FabricConfig deliberately contains only reusable resources. This object
        is constructed from that configuration when a node/service algorithm
        starts and is destroyed with it, before any test registry reset can
        invalidate its interned value plans and converter handles. */
    class FabricMetadataBinding final
    {
      public:
        explicit FabricMetadataBinding(const FabricConfig &config)
            : store_{persistence::store::ValueStoreConfig{
                  .objects = config.objects,
                  .codec = config.metadata_codec}},
              revisions_{config.objects,
                         store_.bind(values_.data_revision_schema())},
              references_{config.objects,
                          store_.bind(values_.revision_reference_schema())},
              transport_{notification_codec().bind(
                  values_.data_revision_schema())}
        {
        }

        [[nodiscard]] const FabricMetadataValueBinding &values() const noexcept
        {
            return values_;
        }

        [[nodiscard]] const BoundMetadataStore &
        revisions() const noexcept
        {
            return revisions_;
        }

        [[nodiscard]] const BoundMetadataStore &
        references() const noexcept
        {
            return references_;
        }

        [[nodiscard]] persistence::store::ObjectBytes
        encode_transport(const ValueView &revision) const
        {
            auto encoded = transport_.encode(revision);
            require_metadata_within_limit(encoded.size());
            return encoded;
        }

      private:
        FabricMetadataValueBinding             values_{};
        persistence::store::ValueStore         store_{};
        BoundMetadataStore                    revisions_{};
        BoundMetadataStore                    references_{};
        persistence::store::BoundValueCodec   transport_{};
    };

    /** Internal construction path for coordinators created after node start.
        It copies a node's already-bound metadata state and performs no
        registry or converter lookup. */
    struct BoundConsistencyFactory
    {
        [[nodiscard]] static ConsistencyCoordinator coordinator(
            FabricConfig config, std::vector<Str> roots,
            FabricMetadataBinding metadata_binding);
    };

    /** Internal construction path for keyed publishers created after start. */
    struct BoundPublisherFactory
    {
        [[nodiscard]] static std::unique_ptr<PublisherStateMachine> publisher(
            FabricConfig config, Str data_id,
            FabricMetadataBinding metadata_binding);
    };
}  // namespace hgraph::fabric::detail

#endif  // HGRAPH_FABRIC_IMPL_METADATA_BINDING_H
