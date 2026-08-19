#include "object_store_impl.h"

#include "object_store_common.h"

#include <atomic>
#include <cerrno>
#include <chrono>
#include <filesystem>
#include <limits>
#include <stdexcept>
#include <system_error>

#if defined(_WIN32)
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#else
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace hgraph::persistence::store::impl
{
    namespace
    {
        namespace fs = std::filesystem;

        [[nodiscard]] ObjectStoreError system_failure(std::string_view       operation,
                                                      const fs::path        &path,
                                                      const std::error_code &error)
        {
            return ObjectStoreError(std::string{operation} + " '" + path.string() +
                                    "': " + error.message());
        }

        [[nodiscard]] std::string key_token(std::string_view key)
        {
            return content_token(std::as_bytes(std::span{key.data(), key.size()}));
        }

        [[nodiscard]] std::uint64_t process_id() noexcept
        {
#if defined(_WIN32)
            return static_cast<std::uint64_t>(GetCurrentProcessId());
#else
            return static_cast<std::uint64_t>(::getpid());
#endif
        }

        class FileLock final
        {
          public:
            explicit FileLock(const fs::path &path)
            {
                std::error_code error;
                fs::create_directories(path.parent_path(), error);
                if (error)
                {
                    throw system_failure("create lock directory", path.parent_path(), error);
                }
#if defined(_WIN32)
                handle_ = CreateFileW(path.c_str(), GENERIC_READ | GENERIC_WRITE,
                                      FILE_SHARE_READ | FILE_SHARE_WRITE, nullptr, OPEN_ALWAYS,
                                      FILE_ATTRIBUTE_NORMAL, nullptr);
                if (handle_ == INVALID_HANDLE_VALUE)
                {
                    throw system_failure(
                        "open lock file", path,
                        std::error_code{static_cast<int>(GetLastError()), std::system_category()});
                }
                if (!LockFileEx(handle_, LOCKFILE_EXCLUSIVE_LOCK, 0, MAXDWORD, MAXDWORD,
                                &overlapped_))
                {
                    const auto failure =
                        std::error_code{static_cast<int>(GetLastError()), std::system_category()};
                    CloseHandle(handle_);
                    handle_ = INVALID_HANDLE_VALUE;
                    throw system_failure("lock file", path, failure);
                }
#else
                descriptor_ = ::open(path.c_str(), O_CREAT | O_RDWR, 0600);
                if (descriptor_ < 0)
                {
                    throw system_failure("open lock file", path,
                                         std::error_code{errno, std::generic_category()});
                }
                if (::flock(descriptor_, LOCK_EX) != 0)
                {
                    const auto failure = std::error_code{errno, std::generic_category()};
                    ::close(descriptor_);
                    descriptor_ = -1;
                    throw system_failure("lock file", path, failure);
                }
#endif
            }

            FileLock(const FileLock &) = delete;
            FileLock &operator=(const FileLock &) = delete;

            ~FileLock()
            {
#if defined(_WIN32)
                if (handle_ != INVALID_HANDLE_VALUE)
                {
                    (void)UnlockFileEx(handle_, 0, MAXDWORD, MAXDWORD, &overlapped_);
                    (void)CloseHandle(handle_);
                }
#else
                if (descriptor_ >= 0)
                {
                    (void)::flock(descriptor_, LOCK_UN);
                    (void)::close(descriptor_);
                }
#endif
            }

          private:
#if defined(_WIN32)
            HANDLE     handle_{INVALID_HANDLE_VALUE};
            OVERLAPPED overlapped_{};
#else
            int descriptor_{-1};
#endif
        };

        void write_complete_file(const fs::path &path, std::span<const std::byte> data)
        {
#if defined(_WIN32)
            const HANDLE handle = CreateFileW(path.c_str(), GENERIC_WRITE, 0, nullptr, CREATE_NEW,
                                              FILE_ATTRIBUTE_NORMAL, nullptr);
            if (handle == INVALID_HANDLE_VALUE)
            {
                throw system_failure(
                    "create temporary object", path,
                    std::error_code{static_cast<int>(GetLastError()), std::system_category()});
            }
            std::size_t offset = 0;
            while (offset < data.size())
            {
                const auto remaining = std::min<std::size_t>(data.size() - offset, MAXDWORD);
                DWORD      written = 0;
                if (!WriteFile(handle, data.data() + offset, static_cast<DWORD>(remaining),
                               &written, nullptr))
                {
                    const auto failure =
                        std::error_code{static_cast<int>(GetLastError()), std::system_category()};
                    (void)CloseHandle(handle);
                    throw system_failure("write temporary object", path, failure);
                }
                if (written == 0)
                {
                    (void)CloseHandle(handle);
                    throw ObjectStoreError("write temporary object '" + path.string() +
                                           "': no progress");
                }
                offset += written;
            }
            if (!FlushFileBuffers(handle))
            {
                const auto failure =
                    std::error_code{static_cast<int>(GetLastError()), std::system_category()};
                (void)CloseHandle(handle);
                throw system_failure("flush temporary object", path, failure);
            }
            (void)CloseHandle(handle);
#else
            const int descriptor = ::open(path.c_str(), O_CREAT | O_EXCL | O_WRONLY, 0600);
            if (descriptor < 0)
            {
                throw system_failure("create temporary object", path,
                                     std::error_code{errno, std::generic_category()});
            }
            std::size_t offset = 0;
            while (offset < data.size())
            {
                const auto written =
                    ::write(descriptor, data.data() + offset, data.size() - offset);
                if (written < 0)
                {
                    if (errno == EINTR)
                    {
                        continue;
                    }
                    const auto failure = std::error_code{errno, std::generic_category()};
                    (void)::close(descriptor);
                    throw system_failure("write temporary object", path, failure);
                }
                if (written == 0)
                {
                    (void)::close(descriptor);
                    throw ObjectStoreError("write temporary object '" + path.string() +
                                           "': no progress");
                }
                offset += static_cast<std::size_t>(written);
            }
            int sync_result = 0;
            do
            {
                sync_result = ::fsync(descriptor);
            } while (sync_result != 0 && errno == EINTR);
            if (sync_result != 0)
            {
                const auto failure = std::error_code{errno, std::generic_category()};
                (void)::close(descriptor);
                throw system_failure("flush temporary object", path, failure);
            }
            (void)::close(descriptor);
#endif
        }

        void sync_directory(const fs::path &path)
        {
#if !defined(_WIN32)
            const int descriptor = ::open(path.c_str(), O_RDONLY | O_DIRECTORY);
            if (descriptor >= 0)
            {
                (void)::fsync(descriptor);
                (void)::close(descriptor);
            }
#else
            (void)path;
#endif
        }

        [[nodiscard]] bool publish_create(const fs::path &temporary, const fs::path &destination)
        {
#if defined(_WIN32)
            if (MoveFileExW(temporary.c_str(), destination.c_str(), MOVEFILE_WRITE_THROUGH))
            {
                return true;
            }
            const auto error = GetLastError();
            if (error == ERROR_ALREADY_EXISTS || error == ERROR_FILE_EXISTS)
            {
                return false;
            }
            throw system_failure("publish immutable object", destination,
                                 std::error_code{static_cast<int>(error), std::system_category()});
#else
            if (::link(temporary.c_str(), destination.c_str()) == 0)
            {
                std::error_code ignored;
                fs::remove(temporary, ignored);
                sync_directory(destination.parent_path());
                return true;
            }
            if (errno == EEXIST)
            {
                return false;
            }
            throw system_failure("publish immutable object", destination,
                                 std::error_code{errno, std::generic_category()});
#endif
        }

        void publish_replace(const fs::path &temporary, const fs::path &destination)
        {
#if defined(_WIN32)
            if (!MoveFileExW(temporary.c_str(), destination.c_str(),
                             MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH))
            {
                throw system_failure(
                    "replace reference", destination,
                    std::error_code{static_cast<int>(GetLastError()), std::system_category()});
            }
#else
            if (::rename(temporary.c_str(), destination.c_str()) != 0)
            {
                throw system_failure("replace reference", destination,
                                     std::error_code{errno, std::generic_category()});
            }
            sync_directory(destination.parent_path());
#endif
        }

        [[nodiscard]] std::optional<StoredObject> read_object(const fs::path &path)
        {
#if defined(_WIN32)
            const HANDLE handle = CreateFileW(
                path.c_str(), GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
            if (handle == INVALID_HANDLE_VALUE)
            {
                const auto failure = GetLastError();
                if (failure == ERROR_FILE_NOT_FOUND || failure == ERROR_PATH_NOT_FOUND)
                {
                    return std::nullopt;
                }
                throw system_failure(
                    "open object", path,
                    std::error_code{static_cast<int>(failure), std::system_category()});
            }
            LARGE_INTEGER size{};
            if (!GetFileSizeEx(handle, &size))
            {
                const auto failure = GetLastError();
                (void)CloseHandle(handle);
                throw system_failure(
                    "size object", path,
                    std::error_code{static_cast<int>(failure), std::system_category()});
            }
            if (size.QuadPart < 0 || static_cast<unsigned long long>(size.QuadPart) >
                                         std::numeric_limits<std::size_t>::max())
            {
                (void)CloseHandle(handle);
                throw ObjectStoreError("object is too large to read: '" + path.string() + "'");
            }
            ObjectBytes data(static_cast<std::size_t>(size.QuadPart));
            std::size_t offset = 0;
            while (offset < data.size())
            {
                const auto remaining = std::min<std::size_t>(data.size() - offset, MAXDWORD);
                DWORD      read = 0;
                if (!ReadFile(handle, data.data() + offset, static_cast<DWORD>(remaining), &read,
                              nullptr))
                {
                    const auto failure = GetLastError();
                    (void)CloseHandle(handle);
                    throw system_failure(
                        "read object", path,
                        std::error_code{static_cast<int>(failure), std::system_category()});
                }
                if (read == 0)
                {
                    (void)CloseHandle(handle);
                    throw ObjectStoreError("read object '" + path.string() +
                                           "': unexpected end of file");
                }
                offset += read;
            }
            (void)CloseHandle(handle);
#else
            const int descriptor = ::open(path.c_str(), O_RDONLY);
            if (descriptor < 0)
            {
                if (errno == ENOENT || errno == ENOTDIR)
                {
                    return std::nullopt;
                }
                throw system_failure("open object", path,
                                     std::error_code{errno, std::generic_category()});
            }
            struct stat info{};
            if (::fstat(descriptor, &info) != 0)
            {
                const auto failure = std::error_code{errno, std::generic_category()};
                (void)::close(descriptor);
                throw system_failure("size object", path, failure);
            }
            if (info.st_size < 0 ||
                static_cast<std::uintmax_t>(info.st_size) > std::numeric_limits<std::size_t>::max())
            {
                (void)::close(descriptor);
                throw ObjectStoreError("object is too large to read: '" + path.string() + "'");
            }
            ObjectBytes data(static_cast<std::size_t>(info.st_size));
            std::size_t offset = 0;
            while (offset < data.size())
            {
                const auto read = ::read(descriptor, data.data() + offset, data.size() - offset);
                if (read < 0)
                {
                    if (errno == EINTR)
                    {
                        continue;
                    }
                    const auto failure = std::error_code{errno, std::generic_category()};
                    (void)::close(descriptor);
                    throw system_failure("read object", path, failure);
                }
                if (read == 0)
                {
                    (void)::close(descriptor);
                    throw ObjectStoreError("read object '" + path.string() +
                                           "': unexpected end of file");
                }
                offset += static_cast<std::size_t>(read);
            }
            (void)::close(descriptor);
#endif
            const auto token = content_token(data);
            return StoredObject{std::move(data), token};
        }

        class LocalObjectStore
        {
          public:
            explicit LocalObjectStore(const LocalLocation &location)
            {
                if (location.root.empty())
                {
                    throw std::invalid_argument("a local object store requires a root directory");
                }
                std::error_code error;
                fs::create_directories(location.root, error);
                if (error)
                {
                    throw system_failure("create object-store root", location.root, error);
                }
                root_ = fs::canonical(location.root, error);
                if (error)
                {
                    throw system_failure("resolve object-store root", location.root, error);
                }
                if (root_ == root_.root_path())
                {
                    throw std::invalid_argument(
                        "a local object store root must not be the filesystem root");
                }
                const auto name = root_.filename().string();
                lock_root_ = root_.parent_path() / ("." + name + ".hgraph-locks");
                staging_root_ = root_.parent_path() / ("." + name + ".hgraph-staging");
                fs::create_directories(lock_root_, error);
                if (error)
                {
                    throw system_failure("create object-store lock root", lock_root_, error);
                }
                fs::create_directories(staging_root_, error);
                if (error)
                {
                    throw system_failure("create object-store staging root", staging_root_, error);
                }
            }

            [[nodiscard]] ImmutableWriteResult put_immutable(std::string_view           key,
                                                             std::span<const std::byte> data)
            {
                const auto destination = resolve(key);
                prepare_parent(destination);
                const auto temporary = write_temporary(key, data);
                try
                {
                    if (publish_create(temporary, destination))
                    {
                        return {ImmutableWriteStatus::Created, content_token(data)};
                    }
                }
                catch (...)
                {
                    remove_temporary(temporary);
                    throw;
                }
                remove_temporary(temporary);

                const auto current = read_object(destination);
                if (!current)
                {
                    throw ObjectStoreError("immutable object disappeared during publication: " +
                                           std::string{key});
                }
                return {current->data.size() == data.size() &&
                                std::ranges::equal(current->data, data)
                            ? ImmutableWriteStatus::Unchanged
                            : ImmutableWriteStatus::Conflict,
                        current->version_token};
            }

            [[nodiscard]] std::optional<StoredObject> get(std::string_view key) const
            {
                return read_object(resolve(key));
            }

            [[nodiscard]] ObjectListPage list(std::string_view                prefix,
                                              std::optional<std::string_view> start_after,
                                              std::size_t                     limit) const
            {
                std::vector<ObjectInfo> objects;
                std::error_code         error;
                if (!fs::exists(root_, error))
                {
                    if (error)
                    {
                        throw system_failure("stat object-store root", root_, error);
                    }
                    return {};
                }
                for (fs::recursive_directory_iterator it{root_, error}, end; it != end;
                     it.increment(error))
                {
                    if (error)
                    {
                        throw system_failure("list object store", root_, error);
                    }
                    if (!it->is_regular_file(error))
                    {
                        if (error)
                        {
                            throw system_failure("stat listed object", it->path(), error);
                        }
                        continue;
                    }
                    const auto relative = fs::relative(it->path(), root_, error);
                    if (error)
                    {
                        throw system_failure("relativize object key", it->path(), error);
                    }
                    objects.push_back(
                        {relative.generic_string(), static_cast<std::uint64_t>(it->file_size())});
                }
                return page_objects(std::move(objects), prefix, start_after, limit);
            }

            [[nodiscard]] CompareExchangeResult compare_exchange_ref(
                std::string_view key, std::optional<std::string_view> expected_version,
                std::span<const std::byte> desired)
            {
                FileLock   lock{lock_root_ / key_token(key)};
                const auto destination = resolve(key);
                auto       current = read_object(destination);
                const bool matches = expected_version
                                         ? current && current->version_token == *expected_version
                                         : !current;
                if (!matches)
                {
                    return {false, std::move(current)};
                }

                prepare_parent(destination);
                const auto temporary = write_temporary(key, desired);
                try
                {
                    if (current)
                    {
                        publish_replace(temporary, destination);
                    }
                    else if (!publish_create(temporary, destination))
                    {
                        remove_temporary(temporary);
                        return {false, read_object(destination)};
                    }
                }
                catch (...)
                {
                    remove_temporary(temporary);
                    throw;
                }
                remove_temporary(temporary);
                return {true, stored_object(desired)};
            }

            void clear()
            {
                std::error_code error;
                fs::remove_all(root_, error);
                if (error)
                {
                    throw system_failure("clear object store", root_, error);
                }
                fs::remove_all(staging_root_, error);
                if (error)
                {
                    throw system_failure("clear object-store staging", staging_root_, error);
                }
                fs::create_directories(root_, error);
                if (error)
                {
                    throw system_failure("recreate object-store root", root_, error);
                }
                fs::create_directories(staging_root_, error);
                if (error)
                {
                    throw system_failure("recreate object-store staging", staging_root_, error);
                }
            }

          private:
            [[nodiscard]] fs::path resolve(std::string_view key) const
            {
                return root_ / fs::path{key};
            }

            static void prepare_parent(const fs::path &path)
            {
                std::error_code error;
                fs::create_directories(path.parent_path(), error);
                if (error)
                {
                    throw system_failure("create object parent", path.parent_path(), error);
                }
            }

            [[nodiscard]] fs::path write_temporary(std::string_view           key,
                                                   std::span<const std::byte> data) const
            {
                static std::atomic<std::uint64_t> sequence{0};
                const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
                const auto path =
                    staging_root_ /
                    (key_token(key) + "-" + std::to_string(process_id()) + "-" +
                     std::to_string(stamp) + "-" +
                     std::to_string(sequence.fetch_add(1, std::memory_order_relaxed)));
                write_complete_file(path, data);
                return path;
            }

            static void remove_temporary(const fs::path &path) noexcept
            {
                std::error_code ignored;
                fs::remove(path, ignored);
            }

            fs::path root_{};
            fs::path lock_root_{};
            fs::path staging_root_{};
        };

        [[nodiscard]] const ObjectStoreOps &local_object_store_ops() noexcept
        {
            static const ObjectStoreOps ops{
                [](void *context, std::string_view key, std::span<const std::byte> data) {
                    return static_cast<LocalObjectStore *>(context)->put_immutable(key, data);
                },
                [](void *context, std::string_view key) {
                    return static_cast<LocalObjectStore *>(context)->get(key);
                },
                [](void *context, std::string_view prefix,
                   std::optional<std::string_view> start_after, std::size_t limit) {
                    return static_cast<LocalObjectStore *>(context)->list(prefix, start_after,
                                                                          limit);
                },
                [](void *context, std::string_view key,
                   std::optional<std::string_view> expected_version,
                   std::span<const std::byte>      desired) {
                    return static_cast<LocalObjectStore *>(context)->compare_exchange_ref(
                        key, expected_version, desired);
                },
                [](void *context) { static_cast<LocalObjectStore *>(context)->clear(); },
            };
            return ops;
        }
    }  // namespace

    ObjectStore make_local_object_store(const LocalLocation &location)
    {
        return ObjectStore{std::make_shared<LocalObjectStore>(location), local_object_store_ops()};
    }
}  // namespace hgraph::persistence::store::impl
