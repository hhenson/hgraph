#include <hgraph/persistence/store_location.h>

#include <charconv>
#include <stdexcept>
#include <string>

#if defined(HGRAPH_PERSISTENCE_WITH_S3)
#include <arrow/filesystem/s3fs.h>
#endif

#include <stdexcept>

namespace hgraph::persistence::store
{
    namespace
    {
        [[nodiscard]] bool is_control(unsigned char c) noexcept { return c < 0x20 || c == 0x7f; }
    }  // namespace

    std::optional<std::string> validate_key(std::string_view key)
    {
        if (key.empty())
        {
            return "key must not be empty";
        }
        if (key.front() == '/')
        {
            return "key must not start with '/'";
        }
        if (key.back() == '/')
        {
            return "key must not end with '/'";
        }

        std::size_t start = 0;
        while (start <= key.size())
        {
            const auto             stop = key.find('/', start);
            const auto             end = stop == std::string_view::npos ? key.size() : stop;
            const std::string_view segment = key.substr(start, end - start);
            if (segment.empty())
            {
                return "key must not contain an empty path segment";
            }
            if (segment == "." || segment == "..")
            {
                return "key must not contain a '.' or '..' segment";
            }
            for (const char c : segment)
            {
                if (is_control(static_cast<unsigned char>(c)))
                {
                    return "key must not contain control characters";
                }
                if (c == '\\')
                {
                    return "key must not contain a backslash";
                }
            }
            if (stop == std::string_view::npos)
            {
                break;
            }
            start = stop + 1;
        }
        return std::nullopt;
    }

    void require_valid_key(std::string_view key)
    {
        if (const auto reason = validate_key(key))
        {
            throw std::invalid_argument("invalid store key '" + std::string{key} + "': " + *reason);
        }
    }

    void finalize_s3() noexcept
    {
#if defined(HGRAPH_PERSISTENCE_WITH_S3)
        (void)arrow::fs::EnsureS3Finalized();
#endif
    }
}  // namespace hgraph::persistence::store

namespace hgraph::persistence::store
{
    namespace
    {
        inline constexpr char BASE64_URL[]{
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"};

        [[nodiscard]] unsigned char base64_url_value(char value)
        {
            if (value >= 'A' && value <= 'Z') { return static_cast<unsigned char>(value - 'A'); }
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
            throw std::invalid_argument("key segment is not base64url");
        }
    }  // namespace

    std::string encode_key_segment(std::string_view value)
    {
        std::string encoded;
        encoded.reserve(((value.size() + 2U) / 3U) * 4U);
        std::size_t offset = 0;
        for (; offset + 3U <= value.size(); offset += 3U)
        {
            const auto first  = static_cast<unsigned char>(value[offset]);
            const auto second = static_cast<unsigned char>(value[offset + 1U]);
            const auto third  = static_cast<unsigned char>(value[offset + 2U]);
            encoded.push_back(BASE64_URL[first >> 2U]);
            encoded.push_back(BASE64_URL[((first & 0x03U) << 4U) | (second >> 4U)]);
            encoded.push_back(BASE64_URL[((second & 0x0fU) << 2U) | (third >> 6U)]);
            encoded.push_back(BASE64_URL[third & 0x3fU]);
        }
        const auto remaining = value.size() - offset;
        if (remaining == 1U)
        {
            const auto first = static_cast<unsigned char>(value[offset]);
            encoded.push_back(BASE64_URL[first >> 2U]);
            encoded.push_back(BASE64_URL[(first & 0x03U) << 4U]);
        }
        else if (remaining == 2U)
        {
            const auto first  = static_cast<unsigned char>(value[offset]);
            const auto second = static_cast<unsigned char>(value[offset + 1U]);
            encoded.push_back(BASE64_URL[first >> 2U]);
            encoded.push_back(BASE64_URL[((first & 0x03U) << 4U) | (second >> 4U)]);
            encoded.push_back(BASE64_URL[(second & 0x0fU) << 2U]);
        }
        return encoded;
    }

    std::string decode_key_segment(std::string_view encoded)
    {
        std::string decoded;
        decoded.reserve((encoded.size() / 4U) * 3U);
        std::size_t offset = 0;
        for (; offset + 4U <= encoded.size(); offset += 4U)
        {
            const auto first  = base64_url_value(encoded[offset]);
            const auto second = base64_url_value(encoded[offset + 1U]);
            const auto third  = base64_url_value(encoded[offset + 2U]);
            const auto fourth = base64_url_value(encoded[offset + 3U]);
            decoded.push_back(static_cast<char>((first << 2U) | (second >> 4U)));
            decoded.push_back(static_cast<char>(((second & 0x0fU) << 4U) | (third >> 2U)));
            decoded.push_back(static_cast<char>(((third & 0x03U) << 6U) | fourth));
        }
        const auto remaining = encoded.size() - offset;
        if (remaining == 1U)
        {
            throw std::invalid_argument("key segment is not base64url");
        }
        if (remaining == 2U)
        {
            const auto first  = base64_url_value(encoded[offset]);
            const auto second = base64_url_value(encoded[offset + 1U]);
            decoded.push_back(static_cast<char>((first << 2U) | (second >> 4U)));
        }
        else if (remaining == 3U)
        {
            const auto first  = base64_url_value(encoded[offset]);
            const auto second = base64_url_value(encoded[offset + 1U]);
            const auto third  = base64_url_value(encoded[offset + 2U]);
            decoded.push_back(static_cast<char>((first << 2U) | (second >> 4U)));
            decoded.push_back(static_cast<char>(((second & 0x0fU) << 4U) | (third >> 2U)));
        }
        return decoded;
    }

    std::string encode_ordered_ordinal(std::int64_t value, std::size_t width)
    {
        if (value <= 0)
        {
            throw std::invalid_argument("ordered ordinal must be positive");
        }
        std::string digits = std::to_string(value);
        if (digits.size() > width)
        {
            throw std::invalid_argument("ordered ordinal is out of range for its width");
        }
        return std::string(width - digits.size(), '0') + digits;
    }

    std::int64_t decode_ordered_ordinal(std::string_view encoded, std::size_t width)
    {
        if (encoded.size() != width)
        {
            throw std::invalid_argument("ordered ordinal has the wrong width");
        }
        std::int64_t value = 0;
        const auto [end, error] =
            std::from_chars(encoded.data(), encoded.data() + encoded.size(), value);
        if (error != std::errc{} || end != encoded.data() + encoded.size() || value <= 0)
        {
            throw std::invalid_argument("ordered ordinal is malformed");
        }
        return value;
    }

}  // namespace hgraph::persistence::store
