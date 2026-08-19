#include "s3_options.h"

#include <hgraph/persistence/object_store.h>

#if defined(HGRAPH_PERSISTENCE_WITH_S3)
#include <arrow/status.h>
#endif

#include <stdexcept>
#include <type_traits>

namespace hgraph::persistence::store::impl
{
    std::string normalize_s3_prefix(std::string_view prefix)
    {
        while (!prefix.empty() && prefix.front() == '/')
        {
            prefix.remove_prefix(1);
        }
        while (!prefix.empty() && prefix.back() == '/')
        {
            prefix.remove_suffix(1);
        }
        return std::string{prefix};
    }

#if defined(HGRAPH_PERSISTENCE_WITH_S3)
    arrow::fs::S3Options make_s3_options(const S3Location &location)
    {
        if (location.bucket.empty())
        {
            throw std::invalid_argument("an S3 store requires a bucket");
        }
        if (!location.prefix.empty() && normalize_s3_prefix(location.prefix).empty())
        {
            throw std::invalid_argument("an S3 store prefix must not contain only slashes");
        }

        const auto initialized = arrow::fs::EnsureS3Initialized();
        if (!initialized.ok())
        {
            throw ObjectStoreError("initialize S3: " + initialized.ToString());
        }

        arrow::fs::S3Options options;
        std::visit(
            [&](const auto &source) {
                using T = std::decay_t<decltype(source)>;
                if constexpr (std::is_same_v<T, Credentials::Ambient>)
                {
                    options = arrow::fs::S3Options::Defaults();
                }
                else if constexpr (std::is_same_v<T, Credentials::Explicit>)
                {
                    options = arrow::fs::S3Options::FromAccessKey(
                        source.access_key_id, source.secret_access_key,
                        source.session_token.value_or(std::string{}));
                }
                else if constexpr (std::is_same_v<T, Credentials::Profile>)
                {
                    throw std::runtime_error(
                        "named AWS profile '" + source.name +
                        "' is not supported by the Arrow S3 backend; set AWS_PROFILE in "
                        "the environment and use Credentials::Ambient, or supply "
                        "Credentials::Explicit");
                }
                else
                {
                    options = arrow::fs::S3Options::FromAssumeRole(
                        source.role_arn, source.session_name.value_or("hgraph"));
                }
            },
            location.credentials.source);
        if (location.region)
        {
            options.region = *location.region;
        }
        if (location.endpoint_override)
        {
            options.endpoint_override = *location.endpoint_override;
            options.scheme = location.endpoint_override->starts_with("https://") ? "https" : "http";
        }
        options.background_writes = false;
        options.allow_delayed_open = true;
        return options;
    }
#endif
}  // namespace hgraph::persistence::store::impl
