#include <hgraph/persistence/value_store.h>

#include <stdexcept>
#include <utility>

namespace hgraph::persistence::store
{
    BoundValueStore::BoundValueStore(ObjectStore objects, BoundValueCodec codec)
        : objects_{std::move(objects)}, codec_{std::move(codec)}
    {
        if (!objects_ || !codec_)
        {
            throw std::invalid_argument(
                "bound value store requires an object store and bound codec");
        }
    }

    BoundValueStore::operator bool() const noexcept
    {
        return bool(objects_) && bool(codec_);
    }

    const ValueTypeMetaData *BoundValueStore::schema() const noexcept
    {
        return codec_.schema();
    }

    ObjectBytes BoundValueStore::encode(const ValueView &value) const
    {
        return codec_.encode(value);
    }

    Value BoundValueStore::decode(std::span<const std::byte> encoded) const
    {
        return codec_.decode(encoded);
    }

    ImmutableWriteResult BoundValueStore::write(std::string_view key,
                                                const ValueView &value) const
    {
        return objects_.put_immutable(key, encode(value));
    }

    std::optional<StoredValue>
    BoundValueStore::try_read_versioned(std::string_view key) const
    {
        const auto stored = objects_.get(key);
        if (!stored.has_value()) { return std::nullopt; }
        return StoredValue{decode(stored->data), stored->version_token};
    }

    std::optional<Value> BoundValueStore::try_read(std::string_view key) const
    {
        auto stored = try_read_versioned(key);
        if (!stored.has_value()) { return std::nullopt; }
        return std::move(stored->value);
    }

    Value BoundValueStore::read(std::string_view key) const
    {
        auto value = try_read(key);
        if (!value.has_value())
        {
            throw ObjectStoreError{"value store key not found: " + std::string{key}};
        }
        return std::move(*value);
    }

    ValueCompareExchangeResult BoundValueStore::compare_exchange(
        std::string_view key, const ValueView &value,
        std::optional<std::string_view> expected_version) const
    {
        auto result = objects_.compare_exchange_ref(key, expected_version,
                                                    encode(value));
        ValueCompareExchangeResult typed{.exchanged = result.exchanged};
        if (result.current.has_value())
        {
            typed.current = StoredValue{decode(result.current->data),
                                        std::move(result.current->version_token)};
        }
        return typed;
    }

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

    BoundValueCodec ValueStore::bind(const ValueTypeMetaData        *schema,
                                     std::optional<std::string_view> codec) const
    {
        if (uses_default(codec)) { return default_resolved_.bind(schema); }
        return value_codec(*codec).bind(schema);
    }

    BoundValueStore ValueStore::bind_schema(
        const ValueTypeMetaData *schema,
        std::optional<std::string_view> codec) const
    {
        return BoundValueStore{objects_, bind(schema, codec)};
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

    ValueCompareExchangeResult
    ValueStore::compare_exchange(std::string_view key, const ValueView &value,
                                 std::optional<std::string_view> expected_version,
                                 std::optional<std::string_view> codec) const
    {
        return bind_schema(value.schema(), codec)
            .compare_exchange(key, value, expected_version);
    }

    ValueStore make_value_store(ValueStoreConfig config)
    {
        return ValueStore{std::move(config)};
    }

}  // namespace hgraph::persistence::store
