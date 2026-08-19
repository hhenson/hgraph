#include <hgraph/persistence/frame_store.h>
#include <hgraph/persistence/object_store.h>
#include <hgraph/util/scope.h>

#include "impl/s3_options.h"

#include <arrow/buffer.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/io/interfaces.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <arrow/status.h>
#include <arrow/table.h>

#if defined(HGRAPH_PERSISTENCE_WITH_S3)
#include <arrow/filesystem/s3fs.h>
#endif

#if defined(HGRAPH_PERSISTENCE_WITH_PARQUET)
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>
#endif

#include <atomic>
#include <chrono>
#include <filesystem>
#include <mutex>
#include <stdexcept>
#include <unordered_map>
#include <utility>

namespace hgraph::persistence::store
{
    namespace
    {
        /** Turn an Arrow failure into an exception rather than a silent empty. */
        void check(const arrow::Status &status, std::string_view what)
        {
            if (!status.ok())
            {
                throw std::runtime_error(std::string{what} + ": " + status.ToString());
            }
        }

        template <typename T>
        T unwrap(arrow::Result<T> result, std::string_view what)
        {
            check(result.status(), what);
            return std::move(result).ValueOrDie();
        }

        [[nodiscard]] const FrameStoreOps &memory_store_ops() noexcept;
        [[nodiscard]] const FrameStoreOps &filesystem_store_ops() noexcept;
    }  // namespace

    bool parquet_available() noexcept
    {
#if defined(HGRAPH_PERSISTENCE_WITH_PARQUET)
        return true;
#else
        return false;
#endif
    }

    const FrameStoreOps &FrameStore::empty_ops() noexcept
    {
        static const FrameStoreOps ops{
            [](void *, std::string_view, Frame, std::optional<Compression>) {
                throw std::logic_error("no frame store is bound");
            },
            [](void *, std::string_view) { return Frame{}; },
            [](void *, std::string_view) { return false; },
            [](void *) {},
        };
        return ops;
    }

    FrameStore::FrameStore() noexcept = default;

    FrameStore::FrameStore(std::shared_ptr<void> context, const FrameStoreOps &ops)
        : context_(std::move(context)), ops_(&ops)
    {
        if (!context_)
        {
            throw std::invalid_argument("frame store context must not be null");
        }
        if (ops.write == nullptr || ops.read == nullptr || ops.contains == nullptr ||
            ops.clear == nullptr)
        {
            throw std::invalid_argument("frame store operations must be complete");
        }
    }

    FrameStore::FrameStore(FrameStore &&other) noexcept
        : context_(std::move(other.context_)), ops_(std::exchange(other.ops_, &empty_ops()))
    {
    }

    FrameStore &FrameStore::operator=(FrameStore &&other) noexcept
    {
        if (this == &other)
        {
            return *this;
        }
        context_ = std::move(other.context_);
        ops_ = std::exchange(other.ops_, &empty_ops());
        return *this;
    }

    void FrameStore::write(std::string_view key, Frame frame,
                           std::optional<Compression> compression) const
    {
        ops_->write(context_.get(), key, std::move(frame), compression);
    }

    Frame FrameStore::read(std::string_view key) const { return ops_->read(context_.get(), key); }

    bool FrameStore::contains(std::string_view key) const
    {
        return ops_->contains(context_.get(), key);
    }

    bool FrameStore::supports_segmented_recordings() const noexcept
    {
        // Segmentation is deliberately a capability of core's immutable
        // stores, not an operation added to the public extension table. That
        // keeps the FrameStoreOps ABI stable and ensures Python/custom stores
        // remain the narrow whole-frame compatibility seam.
        return context_ != nullptr &&
               (ops_ == &memory_store_ops() || ops_ == &filesystem_store_ops());
    }

    void FrameStore::clear() const { ops_->clear(context_.get()); }

    FrameStore::operator bool() const noexcept { return context_ != nullptr; }

    void FrameStore::reset() noexcept
    {
        context_.reset();
        ops_ = &empty_ops();
    }

    namespace
    {
        // ---- memory ----

        class MemoryStore
        {
          public:
            explicit MemoryStore(FrameStoreConfig config) : config_(std::move(config)) {}

            void write(std::string_view key, Frame frame, std::optional<Compression>)
            {
                require_valid_key(key);
                std::scoped_lock  lock{mutex_};
                const std::string k{key};
                if (config_.immutable && frames_.contains(k))
                {
                    throw std::runtime_error("frame-store key already exists: " + k);
                }
                frames_[k] = std::move(frame);
            }

            [[nodiscard]] Frame read(std::string_view key)
            {
                require_valid_key(key);
                std::scoped_lock lock{mutex_};
                const auto       it = frames_.find(std::string{key});
                return it == frames_.end() ? Frame{} : it->second;
            }

            [[nodiscard]] bool contains(std::string_view key)
            {
                require_valid_key(key);
                std::scoped_lock lock{mutex_};
                return frames_.contains(std::string{key});
            }

            void clear()
            {
                std::scoped_lock lock{mutex_};
                frames_.clear();
            }

          private:
            FrameStoreConfig                       config_;
            std::mutex                             mutex_{};
            std::unordered_map<std::string, Frame> frames_{};
        };

        [[nodiscard]] const FrameStoreOps &memory_store_ops() noexcept
        {
            static const FrameStoreOps ops{
                [](void *context, std::string_view key, Frame frame,
                   std::optional<Compression> compression) {
                    static_cast<MemoryStore *>(context)->write(key, std::move(frame), compression);
                },
                [](void *context, std::string_view key) {
                    return static_cast<MemoryStore *>(context)->read(key);
                },
                [](void *context, std::string_view key) {
                    return static_cast<MemoryStore *>(context)->contains(key);
                },
                [](void *context) { static_cast<MemoryStore *>(context)->clear(); },
            };
            return ops;
        }

        // ---- filesystem-backed (local and S3 share everything but the fs) ----

        class FileSystemStore
        {
          public:
            FileSystemStore(FrameStoreConfig config, ObjectStore object_store,
                            std::shared_ptr<arrow::fs::FileSystem> fs, std::string root,
                            bool atomic_local_publication = false)
                : config_(std::move(config)), fs_(std::move(fs)), root_(std::move(root)),
                  object_store_(std::move(object_store)),
                  atomic_local_publication_(atomic_local_publication)
            {
            }

            void write(std::string_view key, Frame frame, std::optional<Compression> compression)
            {
                require_valid_key(key);
                if (!frame.has_value())
                {
                    throw std::invalid_argument("cannot write an empty frame");
                }
                if (config_.immutable)
                {
                    auto output =
                        unwrap(arrow::io::BufferOutputStream::Create(), "create frame buffer");
                    write_table(*frame.table, output, compression.value_or(config_.compression));
                    auto       buffer = unwrap(output->Finish(), "finish frame buffer");
                    const auto bytes =
                        std::span{reinterpret_cast<const std::byte *>(buffer->data()),
                                  static_cast<std::size_t>(buffer->size())};
                    const auto result = object_store_.put_immutable(key, bytes);
                    if (result.status != ImmutableWriteStatus::Created)
                    {
                        throw std::runtime_error("frame-store key already exists: " +
                                                 std::string{key});
                    }
                    return;
                }
                const auto path = resolve(key);
                // The parent directory is implicit on S3 and required locally.
                const auto slash = path.find_last_of('/');
                if (slash != std::string::npos)
                {
                    check(fs_->CreateDir(path.substr(0, slash), true), "create directory");
                }

                const auto output_path = atomic_local_publication_ ? temporary_sibling(path) : path;
                auto       remove_incomplete = make_scope_exit<true>([&] {
                    if (atomic_local_publication_)
                    {
                        (void)fs_->DeleteFile(output_path);
                    }
                });
                auto       out = unwrap(fs_->OpenOutputStream(output_path), "open output stream");
                write_table(*frame.table, out, compression.value_or(config_.compression));
                check(out->Close(), "close output stream");

                if (atomic_local_publication_)
                {
                    // A reader must see either no segment or a complete segment.  The
                    // sibling is on the same filesystem, so LocalFileSystem::Move is
                    // an atomic rename rather than a copy-and-delete publication.
                    std::scoped_lock lock(local_publication_mutex());
                    check(fs_->Move(output_path, path), "publish output file");
                }
                remove_incomplete.release();
            }

            [[nodiscard]] Frame read(std::string_view key)
            {
                require_valid_key(key);
                const auto path = resolve(key);
                if (!exists(path))
                {
                    return Frame{};
                }
                auto in = unwrap(fs_->OpenInputFile(path), "open input file");
                return Frame{read_table(in)};
            }

            [[nodiscard]] bool contains(std::string_view key)
            {
                require_valid_key(key);
                return exists(resolve(key));
            }

            void clear() { object_store_.clear(); }

          private:
            [[nodiscard]] static std::string temporary_sibling(const std::string &path)
            {
                static std::atomic<std::uint64_t> sequence{0};
                const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
                return path + ".hgraph-tmp-" + std::to_string(stamp) + "-" +
                       std::to_string(sequence.fetch_add(1, std::memory_order_relaxed));
            }

            [[nodiscard]] static std::mutex &local_publication_mutex()
            {
                static std::mutex mutex;
                return mutex;
            }

            [[nodiscard]] std::string resolve(std::string_view key) const
            {
                return root_.empty() ? std::string{key} : root_ + "/" + std::string{key};
            }

            [[nodiscard]] bool exists(const std::string &path) const
            {
                const auto info = unwrap(fs_->GetFileInfo(path), "stat");
                return info.type() != arrow::fs::FileType::NotFound;
            }

            void write_table(const arrow::Table                             &table,
                             const std::shared_ptr<arrow::io::OutputStream> &out,
                             Compression                                     compression) const
            {
                switch (config_.format)
                {
                case Format::ArrowIpc: {
                    auto options = arrow::ipc::IpcWriteOptions::Defaults();
                    options.codec = ipc_codec(compression);
                    auto writer = unwrap(arrow::ipc::MakeFileWriter(out, table.schema(), options),
                                         "make IPC writer");
                    check(writer->WriteTable(table), "write IPC table");
                    check(writer->Close(), "close IPC writer");
                    break;
                }
                case Format::Parquet:
#if defined(HGRAPH_PERSISTENCE_WITH_PARQUET)
                {
                    auto props = parquet::WriterProperties::Builder()
                                     .compression(parquet_codec(compression))
                                     ->build();
                    // store_schema keeps the Arrow schema - and with it the RFC
                    // 0001 frame metadata - in the Parquet key-value metadata.
                    auto arrow_props =
                        parquet::ArrowWriterProperties::Builder().store_schema()->build();
                    check(parquet::arrow::WriteTable(
                              table, arrow::default_memory_pool(), out,
                              /*chunk_size=*/table.num_rows() > 0 ? table.num_rows() : 1, props,
                              arrow_props),
                          "write Parquet table");
                    break;
                }
#else
                    throw std::runtime_error("this build has no Parquet support; configure "
                                             "with HGRAPH_PERSISTENCE_WITH_PARQUET");
#endif
                }
            }

            [[nodiscard]] std::shared_ptr<arrow::Table> read_table(
                const std::shared_ptr<arrow::io::RandomAccessFile> &in) const
            {
                switch (config_.format)
                {
                case Format::ArrowIpc: {
                    auto reader =
                        unwrap(arrow::ipc::RecordBatchFileReader::Open(in), "open IPC reader");
                    return unwrap(reader->ToTable(), "read IPC table");
                }
                case Format::Parquet:
#if defined(HGRAPH_PERSISTENCE_WITH_PARQUET)
                {
                    auto reader = unwrap(parquet::arrow::OpenFile(in, arrow::default_memory_pool()),
                                         "open Parquet reader");
                    return unwrap(reader->ReadTable(), "read Parquet table");
                }
#else
                    throw std::runtime_error("this build has no Parquet support; configure "
                                             "with HGRAPH_PERSISTENCE_WITH_PARQUET");
#endif
                }
                return nullptr;
            }

            [[nodiscard]] static std::shared_ptr<arrow::util::Codec> ipc_codec(
                Compression compression)
            {
                switch (compression)
                {
                case Compression::None:
                case Compression::Default:
                    return nullptr;
                case Compression::Snappy:
                    // Arrow IPC permits only LZ4_FRAME and ZSTD; snappy maps to zstd.
                case Compression::Zstd:
                    return unwrap(arrow::util::Codec::Create(arrow::Compression::ZSTD),
                                  "create zstd codec");
                }
                return nullptr;
            }

#if defined(HGRAPH_PERSISTENCE_WITH_PARQUET)
            [[nodiscard]] static arrow::Compression::type parquet_codec(Compression compression)
            {
                switch (compression)
                {
                case Compression::None:
                    return arrow::Compression::UNCOMPRESSED;
                case Compression::Snappy:
                case Compression::Default:
                    return arrow::Compression::SNAPPY;
                case Compression::Zstd:
                    return arrow::Compression::ZSTD;
                }
                return arrow::Compression::SNAPPY;
            }
#endif

            FrameStoreConfig                       config_;
            std::shared_ptr<arrow::fs::FileSystem> fs_;
            std::string                            root_;
            ObjectStore                            object_store_{};
            bool                                   atomic_local_publication_{false};
        };

        [[nodiscard]] const FrameStoreOps &filesystem_store_ops() noexcept
        {
            static const FrameStoreOps ops{
                [](void *context, std::string_view key, Frame frame,
                   std::optional<Compression> compression) {
                    static_cast<FileSystemStore *>(context)->write(key, std::move(frame),
                                                                   compression);
                },
                [](void *context, std::string_view key) {
                    return static_cast<FileSystemStore *>(context)->read(key);
                },
                [](void *context, std::string_view key) {
                    return static_cast<FileSystemStore *>(context)->contains(key);
                },
                [](void *context) { static_cast<FileSystemStore *>(context)->clear(); },
            };
            return ops;
        }

    }  // namespace

    FrameStore make_frame_store(FrameStoreConfig config)
    {
        if (config.format == Format::Parquet && !parquet_available())
        {
            throw std::runtime_error(
                "Parquet format requested but this build has no Parquet support");
        }

        if (std::holds_alternative<MemoryLocation>(config.location))
        {
            return FrameStore{std::make_shared<MemoryStore>(std::move(config)), memory_store_ops()};
        }

        if (const auto *local = std::get_if<LocalLocation>(&config.location))
        {
            if (local->root.empty())
            {
                throw std::invalid_argument("a local frame store requires a root directory");
            }
            const auto      configured_root = local->root;
            std::error_code error;
            std::filesystem::create_directories(configured_root, error);
            if (error)
            {
                throw std::runtime_error("create local frame-store root '" + configured_root +
                                         "': " + error.message());
            }
            const auto resolved_root = std::filesystem::canonical(configured_root, error);
            if (error)
            {
                throw std::runtime_error("resolve local frame-store root '" + configured_root +
                                         "': " + error.message());
            }
            auto root = resolved_root.string();
            std::get<LocalLocation>(config.location).root = root;
            auto fs = std::make_shared<arrow::fs::LocalFileSystem>();
            check(fs->CreateDir(root, true), "create store root");
            auto object_store = make_object_store(ObjectStoreConfig{config.location});
            return FrameStore{
                std::make_shared<FileSystemStore>(std::move(config), std::move(object_store),
                                                  std::move(fs), std::move(root), true),
                filesystem_store_ops()};
        }

        const auto &s3 = std::get<S3Location>(config.location);
#if defined(HGRAPH_PERSISTENCE_WITH_S3)
        if (s3.bucket.empty())
        {
            throw std::invalid_argument("an S3 frame store requires a bucket");
        }
        auto       options = impl::make_s3_options(s3);
        auto       fs = unwrap(arrow::fs::S3FileSystem::Make(options), "create S3 filesystem");
        const auto prefix = impl::normalize_s3_prefix(s3.prefix);
        auto       root = prefix.empty() ? s3.bucket : s3.bucket + "/" + prefix;
        auto       object_store = make_object_store(ObjectStoreConfig{config.location});
        return FrameStore{std::make_shared<FileSystemStore>(std::move(config),
                                                            std::move(object_store), std::move(fs),
                                                            std::move(root)),
                          filesystem_store_ops()};
#else
        (void)s3;
        throw std::runtime_error("this build has no S3 support; configure with "
                                 "HGRAPH_PERSISTENCE_WITH_S3");
#endif
    }

}  // namespace hgraph::persistence::store
