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
        parameter is the implementation context. A codec is resolved once, when
        a store is built or a call names one, never per value. */
    struct ValueCodecOps
    {
        void (*encode)(void *context, const ValueView &value, ObjectBytes &out);
        Value (*decode)(void *context, const ValueTypeMetaData *schema,
                        std::span<const std::byte> encoded);
    };

    /** Register a codec under a stable name and the file extension its
        objects carry. Idempotent for an identical registration; a conflicting
        re-registration of the same name throws, so two extensions cannot
        silently disagree about what a name means.

        The extension is how a stored object names its codec, so it must be the
        conventional one for the format -- "json", "arrow", "parquet". A reader
        outside this codebase recognises the file by that extension and by its
        content, both of which are exactly what the format specifies.

        Registration is build-time machinery. It is not reachable from the
        per-tick path and its registry lock is a counted TypeSystemMutex. */
    HGRAPH_PERSISTENCE_EXPORT void register_value_codec(std::string_view name,
                                                        std::string_view extension,
                                                        std::shared_ptr<void> context,
                                                        const ValueCodecOps &ops);

    /** A resolved codec. Holds its context alive; copyable and cheap. */
    class HGRAPH_PERSISTENCE_CLASS_EXPORT ValueCodec final
    {
      public:
        ValueCodec() noexcept = default;
        ValueCodec(std::string name, std::string extension, std::shared_ptr<void> context,
                   ValueCodecOps ops) noexcept;

        [[nodiscard]] bool valid() const noexcept { return ops_.encode != nullptr; }
        [[nodiscard]] const std::string &name() const noexcept { return name_; }

        /** The file extension objects of this codec carry, without the dot. */
        [[nodiscard]] const std::string &extension() const noexcept { return extension_; }

        void encode(const ValueView &value, ObjectBytes &out) const;
        [[nodiscard]] Value decode(const ValueTypeMetaData *schema,
                                   std::span<const std::byte> encoded) const;

      private:
        std::string             name_{};
        std::string             extension_{};
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

    /** The codec registered for a file extension, or nothing. */
    [[nodiscard]] HGRAPH_PERSISTENCE_EXPORT std::optional<std::string>
    value_codec_for_extension(std::string_view extension);

    /** Install the codecs this library owns. Idempotent; called by the
        extension's registration entry point and safe to call directly in a
        test that does not go through it. */
    HGRAPH_PERSISTENCE_EXPORT void register_builtin_value_codecs();

}  // namespace hgraph::persistence::store

#endif  // HGRAPH_PERSISTENCE_VALUE_CODEC_H
