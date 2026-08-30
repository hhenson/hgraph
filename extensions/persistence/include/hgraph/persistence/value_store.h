#ifndef HGRAPH_PERSISTENCE_VALUE_STORE_H
#define HGRAPH_PERSISTENCE_VALUE_STORE_H

#include <hgraph/persistence/export.h>
#include <hgraph/persistence/object_store.h>
#include <hgraph/persistence/value_codec.h>

#include <hgraph/types/metadata/value_type_meta_data.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_view.h>

#include <optional>
#include <span>
#include <string>
#include <string_view>

namespace hgraph::persistence::store
{
    /** The typed twin of FrameStore: declared value schemas over the byte
        store, with a named codec rather than a fixed format (RFC 0030).

        Frames go through FrameStore and carry Arrow; structs go through here
        and carry whichever codec the store or the call names. Extensions hold
        no serialization code of their own. */
    struct ValueStoreConfig
    {
        ObjectStore objects{};

        /** Codec used by writes that name none. Empty selects "json". */
        std::string codec{};
    };

    /** Envelope written ahead of every payload so a read resolves its decoder
        from the object rather than from out-of-band agreement -- without it a
        per-call codec override would be unreadable. It records exactly one
        fact, the codec name, and is independent of every schema. */
    inline constexpr std::string_view VALUE_ENVELOPE_MAGIC{"HGV1"};

    /** The codec named by an encoded object, without decoding the payload.
        Diagnostics, migration probes, and tests. Returns nothing when the
        bytes do not carry this envelope, which is how a legacy format is
        recognised rather than mis-parsed. */
    [[nodiscard]] HGRAPH_PERSISTENCE_EXPORT std::optional<std::string>
    value_envelope_codec(std::span<const std::byte> encoded);

    class HGRAPH_PERSISTENCE_CLASS_EXPORT ValueStore final
    {
      public:
        ValueStore() noexcept = default;
        explicit ValueStore(ValueStoreConfig config);

        [[nodiscard]] explicit operator bool() const noexcept { return bool(objects_); }

        /** The codec used when a call names none. */
        [[nodiscard]] const std::string &default_codec() const noexcept { return default_codec_; }

        void write(std::string_view key, const ValueView &value,
                   std::optional<std::string_view> codec = {}) const;

        /** Throws when the key is absent; use try_read for optional reads. */
        [[nodiscard]] Value read(std::string_view key, const ValueTypeMetaData *schema) const;

        [[nodiscard]] std::optional<Value> try_read(std::string_view          key,
                                                    const ValueTypeMetaData *schema) const;

        /** Forwards the object store's version token unchanged; this store adds
            no concurrency control of its own. */
        [[nodiscard]] CompareExchangeResult
        compare_exchange(std::string_view key, const ValueView &value,
                         std::optional<std::string_view> expected_version,
                         std::optional<std::string_view> codec = {}) const;

        /** The encoded form a write would store, without storing it. Exposed so
            a caller can size or checksum an object, and so the envelope is
            testable without a backend. */
        [[nodiscard]] ObjectBytes encode(const ValueView                &value,
                                         std::optional<std::string_view> codec = {}) const;

        /** Decode bytes produced by encode(). */
        [[nodiscard]] Value decode(const ValueTypeMetaData   *schema,
                                   std::span<const std::byte> encoded) const;

      private:
        [[nodiscard]] ValueCodec resolve(std::optional<std::string_view> codec) const;

        ObjectStore objects_{};
        std::string default_codec_{JSON_VALUE_CODEC};
    };

    [[nodiscard]] HGRAPH_PERSISTENCE_EXPORT ValueStore make_value_store(ValueStoreConfig config);

}  // namespace hgraph::persistence::store

#endif  // HGRAPH_PERSISTENCE_VALUE_STORE_H
