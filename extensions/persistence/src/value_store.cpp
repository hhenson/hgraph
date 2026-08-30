#include <hgraph/persistence/value_store.h>

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <utility>

namespace hgraph::persistence::store
{
    namespace
    {
        /** "HGV1" | u8 name length | name bytes | payload. */
        constexpr std::size_t MAGIC_SIZE  = VALUE_ENVELOPE_MAGIC.size();
        constexpr std::size_t HEADER_SIZE = MAGIC_SIZE + 1U;
        constexpr std::size_t MAX_CODEC_NAME = 255U;

        [[nodiscard]] bool has_magic(std::span<const std::byte> encoded) noexcept
        {
            if (encoded.size() < HEADER_SIZE)
            {
                return false;
            }
            for (std::size_t index = 0; index != MAGIC_SIZE; ++index)
            {
                if (std::to_integer<char>(encoded[index]) != VALUE_ENVELOPE_MAGIC[index])
                {
                    return false;
                }
            }
            return true;
        }

        void write_envelope(std::string_view codec_name, ObjectBytes &out)
        {
            if (codec_name.size() > MAX_CODEC_NAME)
            {
                throw std::invalid_argument("value codec name is too long to encode");
            }
            for (const char character : VALUE_ENVELOPE_MAGIC)
            {
                out.push_back(static_cast<std::byte>(character));
            }
            out.push_back(static_cast<std::byte>(codec_name.size()));
            for (const char character : codec_name)
            {
                out.push_back(static_cast<std::byte>(character));
            }
        }

        struct Envelope
        {
            std::string                name{};
            std::span<const std::byte> payload{};
        };

        [[nodiscard]] Envelope read_envelope(std::span<const std::byte> encoded)
        {
            if (!has_magic(encoded))
            {
                throw std::invalid_argument(
                    "stored value does not carry an hgraph value envelope");
            }
            const auto name_size = std::to_integer<std::size_t>(encoded[MAGIC_SIZE]);
            if (encoded.size() < HEADER_SIZE + name_size)
            {
                throw std::invalid_argument("stored value envelope is truncated");
            }
            Envelope envelope;
            envelope.name.reserve(name_size);
            for (std::size_t index = 0; index != name_size; ++index)
            {
                envelope.name.push_back(
                    std::to_integer<char>(encoded[HEADER_SIZE + index]));
            }
            envelope.payload = encoded.subspan(HEADER_SIZE + name_size);
            return envelope;
        }
    }  // namespace

    std::optional<std::string> value_envelope_codec(std::span<const std::byte> encoded)
    {
        if (!has_magic(encoded))
        {
            return std::nullopt;
        }
        const auto name_size = std::to_integer<std::size_t>(encoded[MAGIC_SIZE]);
        if (encoded.size() < HEADER_SIZE + name_size)
        {
            return std::nullopt;
        }
        std::string name;
        name.reserve(name_size);
        for (std::size_t index = 0; index != name_size; ++index)
        {
            name.push_back(std::to_integer<char>(encoded[HEADER_SIZE + index]));
        }
        return name;
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

    ObjectBytes ValueStore::encode(const ValueView                &value,
                                   std::optional<std::string_view> codec) const
    {
        const ValueCodec selected = resolve(codec);
        ObjectBytes      encoded;
        write_envelope(selected.name(), encoded);
        selected.encode(value, encoded);
        return encoded;
    }

    Value ValueStore::decode(const ValueTypeMetaData   *schema,
                             std::span<const std::byte> encoded) const
    {
        const Envelope envelope = read_envelope(encoded);
        return value_codec(envelope.name).decode(schema, envelope.payload);
    }

    void ValueStore::write(std::string_view key, const ValueView &value,
                           std::optional<std::string_view> codec) const
    {
        const ObjectBytes encoded = encode(value, codec);
        static_cast<void>(objects_.put_immutable(key, encoded));
    }

    std::optional<Value> ValueStore::try_read(std::string_view          key,
                                              const ValueTypeMetaData *schema) const
    {
        const auto stored = objects_.get(key);
        if (!stored.has_value())
        {
            return std::nullopt;
        }
        return decode(schema, stored->data);
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
        const ObjectBytes encoded = encode(value, codec);
        return objects_.compare_exchange_ref(key, expected_version, encoded);
    }

    ValueStore make_value_store(ValueStoreConfig config)
    {
        return ValueStore{std::move(config)};
    }

}  // namespace hgraph::persistence::store
