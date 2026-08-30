#include <hgraph/persistence/value_store.h>

#include <stdexcept>
#include <utility>

namespace hgraph::persistence::store
{
    ValueStore::ValueStore(ValueStoreConfig config)
        : objects_{std::move(config.objects)},
          default_codec_{config.codec.empty() ? std::string{JSON_VALUE_CODEC}
                                              : std::move(config.codec)},
          // Resolve once, at construction, so a misspelled default fails where
          // it was configured rather than on a later write -- and so the
          // default path never reaches the registry again.
          default_resolved_{value_codec(default_codec_)}
    {
    }

    bool ValueStore::uses_default(std::optional<std::string_view> codec) const noexcept
    {
        return !codec.has_value() || *codec == default_codec_;
    }

    ObjectBytes ValueStore::encode(const ValueView                &value,
                                   std::optional<std::string_view> codec) const
    {
        ObjectBytes encoded;
        if (uses_default(codec)) { default_resolved_.encode(value, encoded); }
        else { value_codec(*codec).encode(value, encoded); }
        return encoded;
    }

    Value ValueStore::decode(const ValueTypeMetaData        *schema,
                             std::span<const std::byte>      encoded,
                             std::optional<std::string_view> codec) const
    {
        if (uses_default(codec)) { return default_resolved_.decode(schema, encoded); }
        return value_codec(*codec).decode(schema, encoded);
    }

    ImmutableWriteResult ValueStore::write(std::string_view key, const ValueView &value,
                                           std::optional<std::string_view> codec) const
    {
        // Exactly `key`, and exactly the codec's bytes: the caller owns its
        // namespace, and an external reader gets the format it expects.
        return objects_.put_immutable(key, encode(value, codec));
    }

    std::optional<StoredValue>
    ValueStore::try_read_versioned(std::string_view key, const ValueTypeMetaData *schema,
                                   std::optional<std::string_view> codec) const
    {
        const auto stored = objects_.get(key);
        if (!stored.has_value())
        {
            return std::nullopt;
        }
        return StoredValue{decode(schema, stored->data, codec), stored->version_token};
    }

    std::optional<Value> ValueStore::try_read(std::string_view                key,
                                              const ValueTypeMetaData        *schema,
                                              std::optional<std::string_view> codec) const
    {
        auto stored = try_read_versioned(key, schema, codec);
        if (!stored.has_value())
        {
            return std::nullopt;
        }
        return std::move(stored->value);
    }

    Value ValueStore::read(std::string_view key, const ValueTypeMetaData *schema,
                           std::optional<std::string_view> codec) const
    {
        auto value = try_read(key, schema, codec);
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
        return objects_.compare_exchange_ref(key, expected_version, encode(value, codec));
    }

    ValueStore make_value_store(ValueStoreConfig config)
    {
        return ValueStore{std::move(config)};
    }

}  // namespace hgraph::persistence::store
