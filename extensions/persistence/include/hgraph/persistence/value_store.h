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
        and carry whichever codec the store or the key names. Extensions hold
        no serialization code of their own.

        **Stored bytes are exactly the codec's output.** A json object is a
        json document and nothing else: it opens in a text editor, `jq` reads
        it, and a tool that knows nothing about hgraph can consume it. The
        store adds no header, no framing and no trailer, which is why the codec
        is named by the object key rather than by anything inside the file. */
    struct ValueStoreConfig
    {
        ObjectStore objects{};

        /** Codec for writes whose key does not already name one. Empty selects
            "json". */
        std::string codec{};
    };

    /** The codec named by an object key, or nothing when the key carries no
        registered extension. `records/a.json` names "json". */
    [[nodiscard]] HGRAPH_PERSISTENCE_EXPORT std::optional<std::string>
    codec_for_key(std::string_view key);

    class HGRAPH_PERSISTENCE_CLASS_EXPORT ValueStore final
    {
      public:
        ValueStore() noexcept = default;
        explicit ValueStore(ValueStoreConfig config);

        [[nodiscard]] explicit operator bool() const noexcept { return bool(objects_); }

        /** The codec used when neither the key nor the call names one. */
        [[nodiscard]] const std::string &default_codec() const noexcept { return default_codec_; }

        /** The key this store would write for `key`, with the codec's extension
            appended when the key does not already carry one. Callers that need
            to hand the object to an external reader want this. */
        [[nodiscard]] std::string resolve_key(std::string_view                key,
                                              std::optional<std::string_view> codec = {}) const;

        /** Selection order, most explicit first: an extension already on the
            key, then `codec`, then the store default. */
        void write(std::string_view key, const ValueView &value,
                   std::optional<std::string_view> codec = {}) const;

        /** Reads the object this store would have written. When the key carries
            no extension the default codec is tried first, then a single prefix
            listing finds an object written under another codec, so a per-call
            override stays readable without the caller knowing about it. */
        [[nodiscard]] Value read(std::string_view key, const ValueTypeMetaData *schema) const;

        [[nodiscard]] std::optional<Value> try_read(std::string_view          key,
                                                    const ValueTypeMetaData *schema) const;

        /** Forwards the object store's version token unchanged; this store adds
            no concurrency control of its own.

            The encoded key is part of the object's identity, so moving a
            CAS-managed object to another codec is a migration rather than an
            update -- the new key has no history to compare against. */
        [[nodiscard]] CompareExchangeResult
        compare_exchange(std::string_view key, const ValueView &value,
                         std::optional<std::string_view> expected_version,
                         std::optional<std::string_view> codec = {}) const;

        /** The bytes a write would store: the codec's output verbatim. */
        [[nodiscard]] ObjectBytes encode(const ValueView                &value,
                                         std::optional<std::string_view> codec = {}) const;

        [[nodiscard]] Value decode(const ValueTypeMetaData *schema,
                                   std::span<const std::byte> encoded,
                                   std::optional<std::string_view> codec = {}) const;

      private:
        [[nodiscard]] ValueCodec resolve(std::optional<std::string_view> codec) const;

        ObjectStore objects_{};
        std::string default_codec_{JSON_VALUE_CODEC};
    };

    [[nodiscard]] HGRAPH_PERSISTENCE_EXPORT ValueStore make_value_store(ValueStoreConfig config);

}  // namespace hgraph::persistence::store

#endif  // HGRAPH_PERSISTENCE_VALUE_STORE_H
