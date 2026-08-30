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
        and carry whichever codec the configuration names. Extensions hold no
        serialization code of their own.

        **Stored bytes are exactly the codec's output.** A json object is a json
        document and nothing else: it opens in a text editor, `jq` reads it, and
        a tool that knows nothing about hgraph can consume it. The store adds no
        header, no framing and no trailer.

        **Keys belong to the caller.** The codec is configuration -- a store
        default, or an argument on one call -- never inferred from the key. A
        caller that wants `records/alpha.json` writes that key; one whose keys
        are structured, parsed, or range-scanned keeps them exactly as they are.
        Reading with a codec the object was not written with is a configuration
        error, and reports itself as a decode failure. */
    struct ValueStoreConfig
    {
        ObjectStore objects{};

        /** Codec for calls that name none. Empty selects "json". */
        std::string codec{};
    };

    /** A decoded object with the version token that read it, so a
        read-modify-write can be completed without a second fetch. */
    struct StoredValue
    {
        Value       value{};
        std::string version_token{};
    };

    /** Typed compare/exchange result. The winner is decoded through the same
        schema-bound codec, so callers of the typed store never have to reach
        through it to the byte layer. */
    struct ValueCompareExchangeResult
    {
        bool exchanged{false};
        std::optional<StoredValue> current{};
    };

    /** One ValueStore bound to one schema for a graph run.

        This is the evaluation-path contract: the object backend and codec
        context remain owned, schema resolution has already happened, and all
        reads (including compare/exchange conflicts) return typed values. */
    class HGRAPH_PERSISTENCE_CLASS_EXPORT BoundValueStore final
    {
      public:
        BoundValueStore() noexcept = default;
        BoundValueStore(ObjectStore objects, BoundValueCodec codec);

        [[nodiscard]] explicit operator bool() const noexcept;
        [[nodiscard]] const ValueTypeMetaData *schema() const noexcept;

        [[nodiscard]] ImmutableWriteResult
        write(std::string_view key, const ValueView &value) const;
        [[nodiscard]] Value read(std::string_view key) const;
        [[nodiscard]] std::optional<Value> try_read(std::string_view key) const;
        [[nodiscard]] std::optional<StoredValue>
        try_read_versioned(std::string_view key) const;
        [[nodiscard]] ValueCompareExchangeResult
        compare_exchange(std::string_view key, const ValueView &value,
                         std::optional<std::string_view> expected_version) const;

        [[nodiscard]] ObjectBytes encode(const ValueView &value) const;
        [[nodiscard]] Value decode(std::span<const std::byte> encoded) const;

      private:
        ObjectStore      objects_{};
        BoundValueCodec codec_{};
    };

    class HGRAPH_PERSISTENCE_CLASS_EXPORT ValueStore final
    {
      public:
        ValueStore() noexcept = default;
        explicit ValueStore(ValueStoreConfig config);

        [[nodiscard]] explicit operator bool() const noexcept { return bool(objects_); }

        /** The codec used when a call names none. */
        [[nodiscard]] const std::string &default_codec() const noexcept { return default_codec_; }

        /** Immutable write at exactly `key`. The result distinguishes a created
            object from a conflicting one, which callers use to detect a double
            publish. */
        [[nodiscard]] ImmutableWriteResult
        write(std::string_view key, const ValueView &value,
              std::optional<std::string_view> codec = {}) const;

        /** Throws when the key is absent; use try_read for optional reads. */
        [[nodiscard]] Value read(std::string_view key, const ValueTypeMetaData *schema,
                                 std::optional<std::string_view> codec = {}) const;

        [[nodiscard]] std::optional<Value>
        try_read(std::string_view key, const ValueTypeMetaData *schema,
                 std::optional<std::string_view> codec = {}) const;

        /** The value and its version token. A compare-and-swap loop needs both
            and would otherwise have to reach past this store to the bytes. */
        [[nodiscard]] std::optional<StoredValue>
        try_read_versioned(std::string_view key, const ValueTypeMetaData *schema,
                           std::optional<std::string_view> codec = {}) const;

        /** Forwards the object store's version token unchanged and decodes the
            winning value through the candidate's schema. This store adds no
            concurrency control of its own. */
        [[nodiscard]] ValueCompareExchangeResult
        compare_exchange(std::string_view key, const ValueView &value,
                         std::optional<std::string_view> expected_version,
                         std::optional<std::string_view> codec = {}) const;

        /** Bind this store's codec to one schema, once. Callers on the
            evaluation path -- anything encoding or decoding during a graph
            run -- must do this while constructing run-local node/service state
            and keep the result: the bound handle performs no schema-plan or
            type-registry lookup, which the unbound calls below cannot
            promise. */
        [[nodiscard]] BoundValueCodec bind(const ValueTypeMetaData        *schema,
                                           std::optional<std::string_view> codec = {}) const;

        /** Bind both the backend and codec to one schema. Graph/runtime code
            should keep this run-local handle rather than pairing a bound codec
            with direct ObjectStore byte operations. */
        [[nodiscard]] BoundValueStore
        bind_schema(const ValueTypeMetaData        *schema,
                    std::optional<std::string_view> codec = {}) const;

        /** The bytes a write would store: the codec's output verbatim. */
        [[nodiscard]] ObjectBytes encode(const ValueView                &value,
                                         std::optional<std::string_view> codec = {}) const;

        [[nodiscard]] Value decode(const ValueTypeMetaData        *schema,
                                   std::span<const std::byte>      encoded,
                                   std::optional<std::string_view> codec = {}) const;

      private:
        /** True when a call takes the store's default rather than an override. */
        [[nodiscard]] bool uses_default(std::optional<std::string_view> codec) const noexcept;

        ObjectStore objects_{};
        std::string default_codec_{JSON_VALUE_CODEC};

        /** Resolved once at construction. Evaluation-path users of the default
            must not reach the registry per value: that would take a
            TypeSystemMutex on the single-threaded graph path. An explicit
            per-call override still resolves by name and is therefore an
            ad-hoc, non-evaluation operation. */
        ValueCodec default_resolved_{};
    };

    [[nodiscard]] HGRAPH_PERSISTENCE_EXPORT ValueStore make_value_store(ValueStoreConfig config);

}  // namespace hgraph::persistence::store

#endif  // HGRAPH_PERSISTENCE_VALUE_STORE_H
