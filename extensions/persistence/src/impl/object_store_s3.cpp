#include "object_store_impl.h"

#include "object_store_common.h"
#include "s3_options.h"

#if defined(HGRAPH_PERSISTENCE_WITH_S3) && defined(HGRAPH_PERSISTENCE_WITH_CURL)
#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/s3fs.h>
#include <arrow/status.h>
#include <curl/curl.h>
#endif

#include <algorithm>
#include <cctype>
#include <exception>
#include <mutex>
#include <stdexcept>

namespace hgraph::persistence::store::impl
{
#if defined(HGRAPH_PERSISTENCE_WITH_S3) && defined(HGRAPH_PERSISTENCE_WITH_CURL)
    namespace
    {
        void check(const arrow::Status &status, std::string_view operation)
        {
            if (!status.ok())
            {
                throw ObjectStoreError(std::string{operation} + ": " + status.ToString());
            }
        }

        template <typename T>
        [[nodiscard]] T unwrap(arrow::Result<T> result, std::string_view operation)
        {
            check(result.status(), operation);
            return std::move(result).ValueOrDie();
        }

        [[nodiscard]] std::string url_encode_path(std::string_view path)
        {
            constexpr char digits[] = "0123456789ABCDEF";
            std::string    encoded;
            encoded.reserve(path.size());
            for (const unsigned char c : path)
            {
                const bool ascii_alphanumeric =
                    (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9');
                if (ascii_alphanumeric || c == '-' || c == '_' || c == '.' || c == '~' || c == '/')
                {
                    encoded.push_back(static_cast<char>(c));
                }
                else
                {
                    encoded.push_back('%');
                    encoded.push_back(digits[c >> 4]);
                    encoded.push_back(digits[c & 0x0f]);
                }
            }
            return encoded;
        }

        struct CurlResponse
        {
            long               status{};
            std::string        etag{};
            std::string        body{};
            ObjectBytes        data{};
            std::exception_ptr callback_failure{};
            bool               capture_object{false};
        };

        [[nodiscard]] std::string_view trim_header_value(std::string_view value)
        {
            while (!value.empty() && (value.front() == ' ' || value.front() == '\t'))
            {
                value.remove_prefix(1);
            }
            while (!value.empty() && (value.back() == '\r' || value.back() == '\n' ||
                                      value.back() == ' ' || value.back() == '\t'))
            {
                value.remove_suffix(1);
            }
            return value;
        }

        std::size_t capture_headers(char *data, std::size_t size, std::size_t count, void *context)
        {
            const auto length = size * count;
            auto      &response = *static_cast<CurlResponse *>(context);
            try
            {
                const std::string_view line{data, length};
                const auto             colon = line.find(':');
                if (colon != std::string_view::npos)
                {
                    auto name = line.substr(0, colon);
                    if (name.size() == 4 &&
                        std::tolower(static_cast<unsigned char>(name[0])) == 'e' &&
                        std::tolower(static_cast<unsigned char>(name[1])) == 't' &&
                        std::tolower(static_cast<unsigned char>(name[2])) == 'a' &&
                        std::tolower(static_cast<unsigned char>(name[3])) == 'g')
                    {
                        response.etag = trim_header_value(line.substr(colon + 1));
                    }
                }
            }
            catch (...)
            {
                response.callback_failure = std::current_exception();
                return 0;
            }
            return length;
        }

        std::size_t capture_body(char *data, std::size_t size, std::size_t count, void *context)
        {
            const auto length = size * count;
            auto      &response = *static_cast<CurlResponse *>(context);
            try
            {
                if (response.capture_object)
                {
                    const auto incoming =
                        std::as_bytes(std::span{data, static_cast<std::size_t>(length)});
                    response.data.insert(response.data.end(), incoming.begin(), incoming.end());
                }
                else
                {
                    constexpr std::size_t max_error_body = 16 * 1024;
                    const auto            available = max_error_body > response.body.size()
                                                          ? max_error_body - response.body.size()
                                                          : 0;
                    response.body.append(data, std::min(length, available));
                }
            }
            catch (...)
            {
                response.callback_failure = std::current_exception();
                return 0;
            }
            return length;
        }

        void curl_check(CURLcode code, std::string_view operation)
        {
            if (code != CURLE_OK)
            {
                throw ObjectStoreError(std::string{operation} + ": " + curl_easy_strerror(code));
            }
        }

        class CurlHandle final
        {
          public:
            CurlHandle()
            {
                static std::once_flag initialized;
                std::call_once(initialized, [] {
                    curl_check(curl_global_init(CURL_GLOBAL_DEFAULT), "initialize libcurl");
                });
                handle_ = curl_easy_init();
                if (handle_ == nullptr)
                {
                    throw ObjectStoreError("create libcurl easy handle");
                }
            }
            CurlHandle(const CurlHandle &) = delete;
            CurlHandle &operator=(const CurlHandle &) = delete;
            ~CurlHandle() { curl_easy_cleanup(handle_); }
            [[nodiscard]] CURL *get() const noexcept { return handle_; }

          private:
            CURL *handle_{};
        };

        class CurlHeaders final
        {
          public:
            void append(const std::string &header)
            {
                auto *next = curl_slist_append(headers_, header.c_str());
                if (next == nullptr)
                {
                    throw std::bad_alloc{};
                }
                headers_ = next;
            }
            CurlHeaders(const CurlHeaders &) = delete;
            CurlHeaders &operator=(const CurlHeaders &) = delete;
            CurlHeaders() = default;
            ~CurlHeaders() { curl_slist_free_all(headers_); }
            [[nodiscard]] curl_slist *get() const noexcept { return headers_; }

          private:
            curl_slist *headers_{};
        };

        class S3ObjectStore
        {
          public:
            explicit S3ObjectStore(const S3Location &location)
                : location_(location), options_(make_s3_options(location))
            {
                if (location.bucket.empty())
                {
                    throw std::invalid_argument("an S3 object store requires a bucket");
                }
                fs_ = unwrap(arrow::fs::S3FileSystem::Make(options_),
                             "create S3 object-store filesystem");
                prefix_ = normalize_s3_prefix(location.prefix);
                root_ = prefix_.empty() ? location.bucket : location.bucket + "/" + prefix_;
                region_ = fs_->region();
                if (region_.empty())
                {
                    region_ = location.region.value_or("us-east-1");
                }
            }

            [[nodiscard]] ImmutableWriteResult put_immutable(std::string_view           key,
                                                             std::span<const std::byte> data)
            {
                const auto response = conditional_put(key, std::nullopt, data);
                if (response.status >= 200 && response.status < 300)
                {
                    if (response.etag.empty())
                    {
                        throw ObjectStoreError("S3 conditional create succeeded without an ETag: " +
                                               std::string{key});
                    }
                    return {ImmutableWriteStatus::Created, response.etag};
                }
                if (response.status != 409 && response.status != 412)
                {
                    throw_http_failure("conditionally create S3 object", key, response);
                }
                const auto current = get(key);
                if (!current)
                {
                    throw ObjectStoreError("S3 precondition failed but no winning object is "
                                           "visible for key '" +
                                           std::string{key} + "'");
                }
                return {current->data.size() == data.size() &&
                                std::ranges::equal(current->data, data)
                            ? ImmutableWriteStatus::Unchanged
                            : ImmutableWriteStatus::Conflict,
                        current->version_token};
            }

            [[nodiscard]] std::optional<StoredObject> get(std::string_view key) const
            {
                CurlHeaders headers;
                auto        response = signed_request(key, "GET", headers, {}, true);
                if (response.status == 404)
                {
                    return std::nullopt;
                }
                if (response.status < 200 || response.status >= 300)
                {
                    throw_http_failure("read S3 object", key, response);
                }
                if (response.etag.empty())
                {
                    throw ObjectStoreError("S3 object has no ETag: " + std::string{key});
                }
                return StoredObject{std::move(response.data), std::move(response.etag)};
            }

            [[nodiscard]] ObjectListPage list(std::string_view                prefix,
                                              std::optional<std::string_view> start_after,
                                              std::size_t                     limit) const
            {
                std::string base = prefix_;
                if (!prefix.empty())
                {
                    const auto slash = prefix.find_last_of('/');
                    if (slash != std::string_view::npos)
                    {
                        base += (base.empty() ? "" : "/") + std::string{prefix.substr(0, slash)};
                    }
                }
                arrow::fs::FileSelector selector;
                selector.base_dir = base.empty() ? location_.bucket : location_.bucket + "/" + base;
                selector.recursive = true;
                selector.allow_not_found = true;
                const auto infos = unwrap(fs_->GetFileInfo(selector), "list S3 objects");

                std::vector<ObjectInfo> objects;
                objects.reserve(infos.size());
                const std::string root_with_slash = root_ + "/";
                for (const auto &info : infos)
                {
                    if (info.type() != arrow::fs::FileType::File ||
                        !info.path().starts_with(root_with_slash))
                    {
                        continue;
                    }
                    objects.push_back({info.path().substr(root_with_slash.size()),
                                       static_cast<std::uint64_t>(info.size())});
                }
                return page_objects(std::move(objects), prefix, start_after, limit);
            }

            [[nodiscard]] CompareExchangeResult compare_exchange_ref(
                std::string_view key, std::optional<std::string_view> expected_version,
                std::span<const std::byte> desired)
            {
                if (expected_version &&
                    (expected_version->contains('\r') || expected_version->contains('\n')))
                {
                    throw std::invalid_argument("an expected S3 version contains a newline");
                }
                const auto response = conditional_put(key, expected_version, desired);
                if (response.status >= 200 && response.status < 300)
                {
                    if (response.etag.empty())
                    {
                        throw ObjectStoreError("S3 reference exchange succeeded without an ETag: " +
                                               std::string{key});
                    }
                    StoredObject current = stored_object(desired);
                    current.version_token = response.etag;
                    return {true, std::move(current)};
                }
                if (response.status != 409 && response.status != 412)
                {
                    throw_http_failure("compare/exchange S3 reference", key, response);
                }
                return {false, get(key)};
            }

            void clear()
            {
                const auto info = unwrap(fs_->GetFileInfo(root_), "stat S3 object-store root");
                if (info.type() != arrow::fs::FileType::NotFound)
                {
                    check(fs_->DeleteDirContents(root_, true), "clear S3 object store");
                }
            }

          private:
            [[nodiscard]] std::string object_key(std::string_view key) const
            {
                return prefix_.empty() ? std::string{key} : prefix_ + "/" + std::string{key};
            }

            [[nodiscard]] std::string request_url(std::string_view key) const
            {
                const auto encoded_key = url_encode_path(object_key(key));
                if (location_.endpoint_override)
                {
                    std::string endpoint = *location_.endpoint_override;
                    while (!endpoint.empty() && endpoint.back() == '/')
                    {
                        endpoint.pop_back();
                    }
                    if (!endpoint.starts_with("http://") && !endpoint.starts_with("https://"))
                    {
                        endpoint = options_.scheme + "://" + endpoint;
                    }
                    return endpoint + "/" + location_.bucket + "/" + encoded_key;
                }
                const std::string suffix =
                    region_.starts_with("cn-") ? "amazonaws.com.cn" : "amazonaws.com";
                return "https://" + location_.bucket + ".s3." + region_ + "." + suffix + "/" +
                       encoded_key;
            }

            [[nodiscard]] CurlResponse conditional_put(
                std::string_view key, std::optional<std::string_view> expected_version,
                std::span<const std::byte> data) const
            {
                CurlHeaders headers;
                headers.append("Content-Type: application/octet-stream");
                headers.append(expected_version ? "If-Match: " + std::string{*expected_version}
                                                : "If-None-Match: *");
                return signed_request(key, "PUT", headers, data, false);
            }

            [[nodiscard]] CurlResponse signed_request(std::string_view key, const char *method,
                                                      CurlHeaders               &headers,
                                                      std::span<const std::byte> data,
                                                      bool capture_object) const
            {
                CurlHandle   handle;
                CurlResponse response;
                response.capture_object = capture_object;

                const auto session_token = options_.GetSessionToken();
                if (!session_token.empty())
                {
                    headers.append("x-amz-security-token: " + session_token);
                }
                const auto access_key = options_.GetAccessKey();
                const auto secret_key = options_.GetSecretKey();
                if (access_key.empty() || secret_key.empty())
                {
                    throw ObjectStoreError(
                        "S3 conditional writes require resolved AWS credentials");
                }

                const auto url = request_url(key);
                const auto sigv4 = "aws:amz:" + region_ + ":s3";
                curl_check(curl_easy_setopt(handle.get(), CURLOPT_URL, url.c_str()),
                           "set S3 request URL");
                curl_check(curl_easy_setopt(handle.get(), CURLOPT_CUSTOMREQUEST, method),
                           "set S3 request method");
                if (std::string_view{method} == "PUT")
                {
                    const void *body = data.empty() ? static_cast<const void *>("")
                                                    : static_cast<const void *>(data.data());
                    curl_check(curl_easy_setopt(handle.get(), CURLOPT_POSTFIELDS, body),
                               "set S3 request body");
                    curl_check(curl_easy_setopt(handle.get(), CURLOPT_POSTFIELDSIZE_LARGE,
                                                static_cast<curl_off_t>(data.size())),
                               "set S3 request size");
                }
                curl_check(curl_easy_setopt(handle.get(), CURLOPT_HTTPHEADER, headers.get()),
                           "set S3 request headers");
                curl_check(curl_easy_setopt(handle.get(), CURLOPT_USERNAME, access_key.c_str()),
                           "set S3 access key");
                curl_check(curl_easy_setopt(handle.get(), CURLOPT_PASSWORD, secret_key.c_str()),
                           "set S3 secret key");
                curl_check(curl_easy_setopt(handle.get(), CURLOPT_AWS_SIGV4, sigv4.c_str()),
                           "enable S3 Signature V4");
                curl_check(curl_easy_setopt(handle.get(), CURLOPT_HEADERFUNCTION, capture_headers),
                           "set S3 header callback");
                curl_check(curl_easy_setopt(handle.get(), CURLOPT_HEADERDATA, &response),
                           "set S3 header context");
                curl_check(curl_easy_setopt(handle.get(), CURLOPT_WRITEFUNCTION, capture_body),
                           "set S3 body callback");
                curl_check(curl_easy_setopt(handle.get(), CURLOPT_WRITEDATA, &response),
                           "set S3 body context");
                curl_check(curl_easy_setopt(handle.get(), CURLOPT_NOSIGNAL, 1L),
                           "disable S3 request signals");
                const auto performed = curl_easy_perform(handle.get());
                if (response.callback_failure)
                {
                    std::rethrow_exception(response.callback_failure);
                }
                curl_check(performed, "perform signed S3 request");
                curl_check(
                    curl_easy_getinfo(handle.get(), CURLINFO_RESPONSE_CODE, &response.status),
                    "read S3 response status");
                return response;
            }

            [[noreturn]] static void throw_http_failure(std::string_view    operation,
                                                        std::string_view    key,
                                                        const CurlResponse &response)
            {
                throw ObjectStoreError(std::string{operation} + " for key '" + std::string{key} +
                                       "' returned HTTP " + std::to_string(response.status) +
                                       (response.body.empty() ? "" : ": " + response.body));
            }

            S3Location                               location_{};
            arrow::fs::S3Options                     options_{};
            std::shared_ptr<arrow::fs::S3FileSystem> fs_{};
            std::string                              prefix_{};
            std::string                              root_{};
            std::string                              region_{};
        };

        [[nodiscard]] const ObjectStoreOps &s3_object_store_ops() noexcept
        {
            static const ObjectStoreOps ops{
                [](void *context, std::string_view key, std::span<const std::byte> data) {
                    return static_cast<S3ObjectStore *>(context)->put_immutable(key, data);
                },
                [](void *context, std::string_view key) {
                    return static_cast<S3ObjectStore *>(context)->get(key);
                },
                [](void *context, std::string_view prefix,
                   std::optional<std::string_view> start_after, std::size_t limit) {
                    return static_cast<S3ObjectStore *>(context)->list(prefix, start_after, limit);
                },
                [](void *context, std::string_view key,
                   std::optional<std::string_view> expected_version,
                   std::span<const std::byte>      desired) {
                    return static_cast<S3ObjectStore *>(context)->compare_exchange_ref(
                        key, expected_version, desired);
                },
                [](void *context) { static_cast<S3ObjectStore *>(context)->clear(); },
            };
            return ops;
        }
    }  // namespace
#endif

    ObjectStore make_s3_object_store(const S3Location &location)
    {
#if defined(HGRAPH_PERSISTENCE_WITH_S3) && defined(HGRAPH_PERSISTENCE_WITH_CURL)
        return ObjectStore{std::make_shared<S3ObjectStore>(location), s3_object_store_ops()};
#else
        (void)location;
        throw std::runtime_error("this build has no conditional S3 object-store "
                                 "support; build hgraph-persistence "
                                 "with Arrow S3 and libcurl >= 7.75");
#endif
    }
}  // namespace hgraph::persistence::store::impl
