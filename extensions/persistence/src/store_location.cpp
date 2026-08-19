#include <hgraph/persistence/store_location.h>

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
