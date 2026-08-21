#include <hgraph/fabric/keys.h>

#include <hgraph/persistence/store_location.h>

#include <charconv>
#include <iterator>
#include <limits>
#include <stdexcept>

namespace hgraph::fabric
{
    namespace
    {
        inline constexpr char BASE64_URL[]{
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"};

        void require_valid_fabric_key(std::string_view key)
        {
            persistence::store::require_valid_key(key);
            if (key.size() > MAX_FABRIC_KEY_BYTES)
            {
                throw std::invalid_argument(
                    "fabric durable key exceeds the portable 1024-byte limit");
            }
        }

        [[nodiscard]] unsigned char decode_base64(char value)
        {
            if (value >= 'A' && value <= 'Z')
            {
                return static_cast<unsigned char>(value - 'A');
            }
            if (value >= 'a' && value <= 'z')
            {
                return static_cast<unsigned char>(value - 'a' + 26);
            }
            if (value >= '0' && value <= '9')
            {
                return static_cast<unsigned char>(value - '0' + 52);
            }
            if (value == '-') { return 62; }
            if (value == '_') { return 63; }
            throw std::invalid_argument("fabric encoded data id is not base64url");
        }

        [[nodiscard]] std::string checked_root(std::string_view fabric_prefix,
                                               std::string_view data_id)
        {
            persistence::store::require_valid_key(fabric_prefix);
            std::string key =
                std::string{fabric_prefix} + "/" + encode_data_id_segment(data_id);
            require_valid_fabric_key(key);
            return key;
        }

        [[nodiscard]] std::string ordinal_key(std::string_view fabric_prefix,
                                              std::string_view data_id,
                                              std::string_view category, Int ordinal)
        {
            std::string key = checked_root(fabric_prefix, data_id);
            key += '/';
            key += category;
            key += '/';
            key += encode_fabric_ordinal(ordinal);
            require_valid_fabric_key(key);
            return key;
        }

        [[nodiscard]] std::string category_prefix(
            std::string_view fabric_prefix, std::string_view data_id,
            std::string_view category)
        {
            std::string key = checked_root(fabric_prefix, data_id);
            key += '/';
            key += category;
            require_valid_fabric_key(key);
            return key;
        }
    }  // namespace

    std::string encode_data_id_segment(std::string_view data_id)
    {
        require_data_id(data_id);
        std::string encoded;
        encoded.reserve(1U + (data_id.size() * 4U + 2U) / 3U);
        encoded.push_back('b');
        std::size_t offset{};
        while (offset + 3U <= data_id.size())
        {
            const auto first = static_cast<unsigned char>(data_id[offset]);
            const auto second = static_cast<unsigned char>(data_id[offset + 1U]);
            const auto third = static_cast<unsigned char>(data_id[offset + 2U]);
            encoded.push_back(BASE64_URL[first >> 2U]);
            encoded.push_back(BASE64_URL[((first & 0x03U) << 4U) | (second >> 4U)]);
            encoded.push_back(BASE64_URL[((second & 0x0fU) << 2U) | (third >> 6U)]);
            encoded.push_back(BASE64_URL[third & 0x3fU]);
            offset += 3U;
        }
        const std::size_t remaining = data_id.size() - offset;
        if (remaining != 0U)
        {
            const auto first = static_cast<unsigned char>(data_id[offset]);
            encoded.push_back(BASE64_URL[first >> 2U]);
            if (remaining == 1U)
            {
                encoded.push_back(BASE64_URL[(first & 0x03U) << 4U]);
            }
            else
            {
                const auto second =
                    static_cast<unsigned char>(data_id[offset + 1U]);
                encoded.push_back(
                    BASE64_URL[((first & 0x03U) << 4U) | (second >> 4U)]);
                encoded.push_back(BASE64_URL[(second & 0x0fU) << 2U]);
            }
        }
        return encoded;
    }

    Str decode_data_id_segment(std::string_view encoded)
    {
        if (encoded.size() < 3U || encoded.front() != 'b' ||
            ((encoded.size() - 1U) % 4U) == 1U)
        {
            throw std::invalid_argument("invalid fabric encoded data-id segment");
        }
        Str decoded;
        decoded.reserve(((encoded.size() - 1U) * 3U) / 4U);
        std::size_t offset{1U};
        while (offset + 4U <= encoded.size())
        {
            const auto first = decode_base64(encoded[offset]);
            const auto second = decode_base64(encoded[offset + 1U]);
            const auto third = decode_base64(encoded[offset + 2U]);
            const auto fourth = decode_base64(encoded[offset + 3U]);
            decoded.push_back(static_cast<char>((first << 2U) | (second >> 4U)));
            decoded.push_back(static_cast<char>((second << 4U) | (third >> 2U)));
            decoded.push_back(static_cast<char>((third << 6U) | fourth));
            offset += 4U;
        }
        const std::size_t remaining = encoded.size() - offset;
        if (remaining != 0U)
        {
            const auto first = decode_base64(encoded[offset]);
            const auto second = decode_base64(encoded[offset + 1U]);
            decoded.push_back(static_cast<char>((first << 2U) | (second >> 4U)));
            if (remaining == 3U)
            {
                const auto third = decode_base64(encoded[offset + 2U]);
                decoded.push_back(static_cast<char>((second << 4U) | (third >> 2U)));
            }
        }
        require_data_id(decoded);
        if (encode_data_id_segment(decoded) != encoded)
        {
            throw std::invalid_argument("non-canonical fabric encoded data-id segment");
        }
        return decoded;
    }

    std::string encode_fabric_ordinal(Int value)
    {
        if (value <= 0)
        {
            throw std::invalid_argument("fabric durable ordinal must be positive");
        }
        char digits[FABRIC_ORDINAL_WIDTH];
        const auto [end, error] = std::to_chars(std::begin(digits), std::end(digits), value);
        if (error != std::errc{})
        {
            throw std::invalid_argument("fabric durable ordinal is out of range");
        }
        const auto count = static_cast<std::size_t>(end - std::begin(digits));
        return std::string(FABRIC_ORDINAL_WIDTH - count, '0') +
               std::string{digits, end};
    }

    Int decode_fabric_ordinal(std::string_view encoded)
    {
        if (encoded.size() != FABRIC_ORDINAL_WIDTH)
        {
            throw std::invalid_argument("fabric durable ordinal has invalid width");
        }
        Int value{};
        const auto [end, error] =
            std::from_chars(encoded.data(), encoded.data() + encoded.size(), value);
        if (error != std::errc{} || end != encoded.data() + encoded.size() || value <= 0 ||
            encode_fabric_ordinal(value) != encoded)
        {
            throw std::invalid_argument("fabric durable ordinal is not canonical");
        }
        return value;
    }

    std::string data_id_key_prefix(std::string_view fabric_prefix,
                                   std::string_view data_id)
    {
        return checked_root(fabric_prefix, data_id);
    }

    std::string data_version_key(std::string_view fabric_prefix,
                                 std::string_view data_id, DataVersion version)
    {
        return ordinal_key(fabric_prefix, data_id, "data", version);
    }

    std::string revision_key_prefix(std::string_view fabric_prefix,
                                    std::string_view data_id)
    {
        return category_prefix(fabric_prefix, data_id, "revision");
    }

    std::string revision_key(std::string_view fabric_prefix,
                             std::string_view data_id, RevisionId revision)
    {
        return ordinal_key(fabric_prefix, data_id, "revision", revision);
    }

    std::string as_of_key_prefix(std::string_view fabric_prefix,
                                 std::string_view data_id)
    {
        return category_prefix(fabric_prefix, data_id, "as_of");
    }

    std::string as_of_key(std::string_view fabric_prefix, std::string_view data_id,
                          DateTime as_of)
    {
        return ordinal_key(fabric_prefix, data_id, "as_of",
                           as_of.time_since_epoch().count());
    }

    std::string latest_key(std::string_view fabric_prefix,
                           std::string_view data_id)
    {
        return category_prefix(fabric_prefix, data_id, "latest");
    }
}  // namespace hgraph::fabric
