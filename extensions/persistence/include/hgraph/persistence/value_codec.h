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

    /** Explicit erased ownership for schema-dependent codec state. */
    struct ValueCodecBinding
    {
        std::shared_ptr<const void> owner{};
        const void                 *handle{nullptr};
    };

    /** Encode and decode one declared value schema.

        An ops table in the runtime's convention: function pointers whose first
        parameter is the implementation context.

        **Everything schema-dependent is resolved by `bind`.** A codec that
        needs per-schema machinery -- json's interned converter, a protobuf
        descriptor, an avro schema -- synthesizes it once in `bind` and returns
        an opaque handle; `encode` and `decode` then take that handle and must
        do no schema-plan lookup and touch no type or realisation registry.
        This is the same
        "compose once" contract the runtime already applies to nodes, which
        resolve their converter in `start` and carry it in node State.

        A caller on the evaluation path binds while constructing run-local
        node/service state and carries the BoundValueCodec. The unbound
        convenience calls on ValueCodec bind per call and are for build-time
        and ad-hoc use only. */
    struct ValueCodecOps
    {
        /** Resolve the schema-dependent state once. The returned erased owner
            explicitly retains any run-local plan; a context-owned or interned
            handle may leave owner empty because BoundValueCodec also retains
            the registered implementation context. */
        ValueCodecBinding (*bind)(void *context,
                                  const ValueTypeMetaData *schema);

        /** Encode with a handle from `bind`. Must not look up or allocate
            schema state -- this is the per-tick path. */
        void (*encode)(void *context, const void *bound, const ValueView &value,
                       ObjectBytes &out);

        /** Decode with a handle from `bind`. Same constraints as `encode`. */
        Value (*decode)(void *context, const void *bound,
                        std::span<const std::byte> encoded);
    };

    /** A codec bound to one schema: the run-local per-tick handle.

        Bind while constructing node/service state and carry this only for that
        run. It retains the codec implementation context, but the schema-bound
        handle may refer to interned type/converter state. Test-only registry
        reset invalidates that state just as it invalidates bindings captured by
        ordinary nodes, so a bound codec must never be stored in reusable graph
        configuration or a process-lifetime static. */
    class HGRAPH_PERSISTENCE_CLASS_EXPORT BoundValueCodec final
    {
      public:
        BoundValueCodec() noexcept;
        BoundValueCodec(ValueCodecOps ops, std::shared_ptr<void> context,
                        const ValueTypeMetaData *schema,
                        ValueCodecBinding binding);

        BoundValueCodec(const BoundValueCodec &) = default;
        BoundValueCodec &operator=(const BoundValueCodec &) = default;
        BoundValueCodec(BoundValueCodec &&other) noexcept;
        BoundValueCodec &operator=(BoundValueCodec &&other) noexcept;
        ~BoundValueCodec() = default;

        [[nodiscard]] explicit operator bool() const noexcept
        {
            return schema_ != nullptr;
        }

        [[nodiscard]] const ValueTypeMetaData *schema() const noexcept { return schema_; }

        void encode(const ValueView &value, ObjectBytes &out) const;
        [[nodiscard]] ObjectBytes encode(const ValueView &value) const;
        [[nodiscard]] Value decode(std::span<const std::byte> encoded) const;

      private:
        [[nodiscard]] static const ValueCodecOps &empty_ops() noexcept;

        ValueCodecOps            ops_{};
        std::shared_ptr<void>     context_{};
        std::shared_ptr<const void> binding_owner_{};
        const ValueTypeMetaData  *schema_{nullptr};
        const void               *bound_{nullptr};
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
        ValueCodec() noexcept;
        ValueCodec(std::string name, std::shared_ptr<void> context,
                   ValueCodecOps ops) noexcept;

        ValueCodec(const ValueCodec &) = default;
        ValueCodec &operator=(const ValueCodec &) = default;
        ValueCodec(ValueCodec &&other) noexcept;
        ValueCodec &operator=(ValueCodec &&other) noexcept;
        ~ValueCodec() = default;

        [[nodiscard]] bool valid() const noexcept
        {
            return !name_.empty() && ops_.bind != nullptr &&
                   ops_.encode != nullptr && ops_.decode != nullptr;
        }
        [[nodiscard]] const std::string &name() const noexcept { return name_; }

        /** Resolve this codec's schema-dependent state once. Do this while
            constructing run-local node/service state and keep the result for
            anything on the evaluation path. */
        [[nodiscard]] BoundValueCodec bind(const ValueTypeMetaData *schema) const;

        /** Convenience: binds per call. Build-time and ad-hoc use only -- on
            the per-tick path use bind() and keep the BoundValueCodec. */
        void encode(const ValueView &value, ObjectBytes &out) const;
        [[nodiscard]] Value decode(const ValueTypeMetaData *schema,
                                   std::span<const std::byte> encoded) const;

      private:
        [[nodiscard]] static const ValueCodecOps &empty_ops() noexcept;

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
