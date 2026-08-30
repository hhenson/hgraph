#include <hgraph/persistence/value_store.h>

#include <cstddef>
#include <stdexcept>
#include <utility>

namespace hgraph::persistence::store
{
    namespace
    {
        /** The extension of `key`, without the dot, or empty. Only the final
            segment is considered, so a dot in a directory name is not mistaken
            for a format. */
        [[nodiscard]] std::string_view key_extension(std::string_view key) noexcept
        {
            const auto slash = key.find_last_of('/');
            const auto start = (slash == std::string_view::npos) ? 0U : slash + 1U;
            const auto dot   = key.find_last_of('.');
            if (dot == std::string_view::npos || dot < start || dot + 1U >= key.size())
            {
                return {};
            }
            return key.substr(dot + 1U);
        }
    }  // namespace

    std::optional<std::string> codec_for_key(std::string_view key)
    {
        const auto extension = key_extension(key);
        if (extension.empty())
        {
            return std::nullopt;
        }
        return value_codec_for_extension(extension);
    }

    ValueStore::ValueStore(ValueStoreConfig config)
        : objects_{std::move(config.objects)},
          default_codec_{config.codec.empty() ? std::string{JSON_VALUE_CODEC}
                                              : std::move(config.codec)}
    {
        // Resolve once, at construction, so a misspelled default fails where it
        // was configured rather than on a later write.
        static_cast<void>(value_codec(default_codec_));
    }

    ValueCodec ValueStore::resolve(std::optional<std::string_view> codec) const
    {
        return value_codec(codec.has_value() ? *codec : std::string_view{default_codec_});
    }

    std::string ValueStore::resolve_key(std::string_view                key,
                                        std::optional<std::string_view> codec) const
    {
        // Most explicit wins: a key that already names a codec keeps its name.
        if (const auto named = codec_for_key(key); named.has_value())
        {
            return std::string{key};
        }
        std::string resolved{key};
        resolved.push_back('.');
        resolved += resolve(codec).extension();
        return resolved;
    }

    ObjectBytes ValueStore::encode(const ValueView                &value,
                                   std::optional<std::string_view> codec) const
    {
        ObjectBytes encoded;
        resolve(codec).encode(value, encoded);
        return encoded;
    }

    Value ValueStore::decode(const ValueTypeMetaData        *schema,
                             std::span<const std::byte>      encoded,
                             std::optional<std::string_view> codec) const
    {
        return resolve(codec).decode(schema, encoded);
    }

    void ValueStore::write(std::string_view key, const ValueView &value,
                           std::optional<std::string_view> codec) const
    {
        const std::string stored_key = resolve_key(key, codec);
        const auto        named      = codec_for_key(stored_key);
        // The stored bytes are the codec's output and nothing else: a .json
        // object is a json document an external reader can open directly.
        const ObjectBytes encoded = encode(value, named.has_value()
                                                      ? std::optional<std::string_view>{*named}
                                                      : codec);
        static_cast<void>(objects_.put_immutable(stored_key, encoded));
    }

    std::optional<Value> ValueStore::try_read(std::string_view          key,
                                              const ValueTypeMetaData *schema) const
    {
        // A key that names its codec is read directly.
        if (const auto named = codec_for_key(key); named.has_value())
        {
            const auto stored = objects_.get(key);
            if (!stored.has_value())
            {
                return std::nullopt;
            }
            return decode(schema, stored->data, *named);
        }

        // Otherwise try the default, which is the common path and costs one get.
        const std::string preferred = resolve_key(key);
        if (const auto stored = objects_.get(preferred); stored.has_value())
        {
            return decode(schema, stored->data, default_codec_);
        }

        // Then one listing finds an object written under another codec, so a
        // per-call override stays readable by a caller that does not know one
        // was used.
        std::string prefix{key};
        prefix.push_back('.');
        const auto page = objects_.list(prefix, std::nullopt, 16U);
        for (const auto &info : page.objects)
        {
            const auto named = codec_for_key(info.key);
            if (!named.has_value())
            {
                continue;
            }
            const auto stored = objects_.get(info.key);
            if (!stored.has_value())
            {
                continue;
            }
            return decode(schema, stored->data, *named);
        }
        return std::nullopt;
    }

    Value ValueStore::read(std::string_view key, const ValueTypeMetaData *schema) const
    {
        auto value = try_read(key, schema);
        if (!value.has_value())
        {
            throw ObjectStoreError{"value store key not found: " + std::string{key}};
        }
        return std::move(*value);
    }

    CompareExchangeResult
    ValueStore::compare_exchange(std::string_view key, const ValueView &value,
                                 std::optional<std::string_view> expected_version,
                                 std::optional<std::string_view> codec) const
    {
        const std::string stored_key = resolve_key(key, codec);
        const auto        named      = codec_for_key(stored_key);
        const ObjectBytes encoded = encode(value, named.has_value()
                                                      ? std::optional<std::string_view>{*named}
                                                      : codec);
        return objects_.compare_exchange_ref(stored_key, expected_version, encoded);
    }

    ValueStore make_value_store(ValueStoreConfig config)
    {
        return ValueStore{std::move(config)};
    }

}  // namespace hgraph::persistence::store
