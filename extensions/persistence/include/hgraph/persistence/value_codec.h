#ifndef HGRAPH_PERSISTENCE_VALUE_CODEC_H
#define HGRAPH_PERSISTENCE_VALUE_CODEC_H

#include <hgraph/persistence/export.h>
#include <hgraph/persistence/object_store.h>

#include <hgraph/types/metadata/value_type_meta_data.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_view.h>

#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace hgraph::persistence::store
{
    /** The codec every conforming build provides, and the default when a store
        names none. RFC 0030. */
    inline constexpr std::string_view JSON_VALUE_CODEC{"json"};

    /** Encode and decode one declared value schema.

        An ops table in the runtime's convention: function pointers whose first
        parameter is the implementation context.

        **Everything schema-dependent is resolved by `bind`.** A codec that
        needs per-schema machinery -- json's interned converter, a protobuf
        descriptor, an avro schema -- synthesizes it once in `bind` and returns
        an opaque handle; `encode` and `decode` then take that handle and must
        do no lookup, take no lock, and touch no registry. This is the same
        "compose once" contract the runtime already applies to nodes, which
        resolve their converter in `start` and carry it in node State.

        A caller on the evaluation path binds at wiring time and carries the
        BoundValueCodec. The unbound convenience calls on ValueCodec bind per
        call and are for build-time and ad-hoc use only. */
    struct ValueCodecOps
    {
        /** Resolve the schema-dependent state once. The returned handle is
            owned by the codec and must outlive every use; codecs are never
            unregistered, so an interned or context-owned pointer is correct. */
        const void *(*bind)(void *context, const ValueTypeMetaData *schema);

        /** Encode with a handle from `bind`. Must not lock or allocate
            schema state -- this is the per-tick path. */
        void (*encode)(void *context, const void *bound, const ValueView &value,
                       ObjectBytes &out);

        /** Decode with a handle from `bind`. Same constraints as `encode`. */
        Value (*decode)(void *context, const void *bound,
                        std::span<const std::byte> encoded);
    };

    /** A codec bound to one schema: the per-tick handle.

        Bind at wiring time and carry this. It holds no shared_ptr and performs
        no lookup, so encoding during evaluation acquires no TypeSystemMutex --
        the single-threaded evaluation ruling. The bound handle and the codec
        context are owned by the registry, which never removes a registration,
        so the raw pointers stay valid; a registry reset that clears interned
        state invalidates bindings exactly as it invalidates a node's captured
        converter, which is why binding belongs with graph construction. */
    class HGRAPH_PERSISTENCE_CLASS_EXPORT BoundValueCodec final
    {
      public:
        BoundValueCodec() noexcept = default;
        BoundValueCodec(ValueCodecOps ops, void *context, const void *bound) noexcept
            : ops_{ops}, context_{context}, bound_{bound}
        {
        }

        [[nodiscard]] explicit operator bool() const noexcept
        {
            return ops_.encode != nullptr;
        }

        void encode(const ValueView &value, ObjectBytes &out) const;
        [[nodiscard]] ObjectBytes encode(const ValueView &value) const;
        [[nodiscard]] Value decode(std::span<const std::byte> encoded) const;

      private:
        ValueCodecOps ops_{};
        void         *context_{nullptr};
        const void   *bound_{nullptr};
    };

    /** Register a codec under a stable name. Idempotent for an identical
        registration; a conflicting re-registration of the same name throws, so
        two extensions cannot silently disagree about what a name means.

        Registration is build-time machinery. It is not reachable from the
        per-tick path and its registry lock is a counted TypeSystemMutex. */
    HGRAPH_PERSISTENCE_EXPORT void register_value_codec(std::string_view name,
                                                        std::shared_ptr<void> context,
                                                        const ValueCodecOps &ops);

    /** A resolved codec. Holds its context alive; copyable and cheap. */
    class HGRAPH_PERSISTENCE_CLASS_EXPORT ValueCodec final
    {
      public:
        ValueCodec() noexcept = default;
        ValueCodec(std::string name, std::shared_ptr<void> context,
                   ValueCodecOps ops) noexcept;

        [[nodiscard]] bool valid() const noexcept { return ops_.encode != nullptr; }
        [[nodiscard]] const std::string &name() const noexcept { return name_; }

        /** Resolve this codec's schema-dependent state once. Do this at wiring
            time and keep the result for anything on the evaluation path. */
        [[nodiscard]] BoundValueCodec bind(const ValueTypeMetaData *schema) const;

        /** Convenience: binds per call. Build-time and ad-hoc use only -- on
            the per-tick path use bind() and keep the BoundValueCodec. */
        void encode(const ValueView &value, ObjectBytes &out) const;
        [[nodiscard]] Value decode(const ValueTypeMetaData *schema,
                                   std::span<const std::byte> encoded) const;

      private:
        std::string             name_{};
        std::shared_ptr<void>   context_{};
        ValueCodecOps           ops_{};
    };

    /** Look a codec up by name. Throws when the name is unknown -- a read that
        meets an unrecognised codec fails closed rather than guessing. */
    [[nodiscard]] HGRAPH_PERSISTENCE_EXPORT ValueCodec value_codec(std::string_view name);

    /** Whether a name is registered, without throwing. */
    [[nodiscard]] HGRAPH_PERSISTENCE_EXPORT bool value_codec_registered(
        std::string_view name) noexcept;

    /** Registered codec names, sorted. Diagnostics and tests. */
    [[nodiscard]] HGRAPH_PERSISTENCE_EXPORT std::vector<std::string> value_codec_names();

    /** Install the codecs this library owns. Idempotent; called by the
        extension's registration entry point and safe to call directly in a
        test that does not go through it. */
    HGRAPH_PERSISTENCE_EXPORT void register_builtin_value_codecs();

}  // namespace hgraph::persistence::store

#endif  // HGRAPH_PERSISTENCE_VALUE_CODEC_H
