#include <hgraph/types/frame_store.h>

#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/io/interfaces.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <arrow/status.h>
#include <arrow/table.h>

#if defined(HGRAPH_WITH_S3)
#include <arrow/filesystem/s3fs.h>
#endif

#if defined(HGRAPH_WITH_PARQUET)
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>
#endif

#include <mutex>
#include <stdexcept>
#include <unordered_map>
#include <utility>

namespace hgraph::store
{
    namespace
    {
        /** Turn an Arrow failure into an exception rather than a silent empty. */
        void check(const arrow::Status &status, std::string_view what)
        {
            if (!status.ok()) { throw std::runtime_error(std::string{what} + ": " + status.ToString()); }
        }

        template <typename T> T unwrap(arrow::Result<T> result, std::string_view what)
        {
            check(result.status(), what);
            return std::move(result).ValueOrDie();
        }

        [[nodiscard]] bool is_control(unsigned char c) noexcept { return c < 0x20 || c == 0x7f; }
    }  // namespace

    std::optional<std::string> validate_key(std::string_view key)
    {
        if (key.empty()) { return "key must not be empty"; }
        if (key.front() == '/') { return "key must not start with '/'"; }
        if (key.back() == '/') { return "key must not end with '/'"; }

        std::size_t start = 0;
        while (start <= key.size())
        {
            const auto        stop    = key.find('/', start);
            const auto        end     = stop == std::string_view::npos ? key.size() : stop;
            const std::string_view seg = key.substr(start, end - start);
            if (seg.empty()) { return "key must not contain an empty path segment"; }
            if (seg == "." || seg == "..") { return "key must not contain a '.' or '..' segment"; }
            for (const char c : seg)
            {
                if (is_control(static_cast<unsigned char>(c))) { return "key must not contain control characters"; }
                if (c == '\\') { return "key must not contain a backslash"; }
            }
            if (stop == std::string_view::npos) { break; }
            start = stop + 1;
        }
        return std::nullopt;
    }

    void require_valid_key(std::string_view key)
    {
        if (const auto reason = validate_key(key))
        {
            throw std::invalid_argument("invalid frame-store key '" + std::string{key} + "': " + *reason);
        }
    }

    void finalize_s3() noexcept
    {
#if defined(HGRAPH_WITH_S3)
        (void)arrow::fs::EnsureS3Finalized();
#endif
    }

    bool parquet_available() noexcept
    {
#if defined(HGRAPH_WITH_PARQUET)
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
        if (!context_) { throw std::invalid_argument("frame store context must not be null"); }
        if (ops.write == nullptr || ops.read == nullptr || ops.contains == nullptr || ops.clear == nullptr)
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
        if (this == &other) { return *this; }
        context_ = std::move(other.context_);
        ops_     = std::exchange(other.ops_, &empty_ops());
        return *this;
    }

    void FrameStore::write(std::string_view key, Frame frame, std::optional<Compression> compression) const
    {
        ops_->write(context_.get(), key, std::move(frame), compression);
    }

    Frame FrameStore::read(std::string_view key) const { return ops_->read(context_.get(), key); }

    bool FrameStore::contains(std::string_view key) const { return ops_->contains(context_.get(), key); }

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
                const auto it = frames_.find(std::string{key});
                return it == frames_.end() ? Frame{} : it->second;
            }

            [[nodiscard]] bool contains(std::string_view key)
            {
                require_valid_key(key);
                return frames_.contains(std::string{key});
            }

            void clear() { frames_.clear(); }

          private:
            FrameStoreConfig                       config_;
            std::unordered_map<std::string, Frame> frames_{};
        };

        [[nodiscard]] const FrameStoreOps &memory_store_ops() noexcept
        {
            static const FrameStoreOps ops{
                [](void *context, std::string_view key, Frame frame, std::optional<Compression> compression) {
                    static_cast<MemoryStore *>(context)->write(key, std::move(frame), compression);
                },
                [](void *context, std::string_view key) { return static_cast<MemoryStore *>(context)->read(key); },
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
            FileSystemStore(FrameStoreConfig config, std::shared_ptr<arrow::fs::FileSystem> fs, std::string root)
                : config_(std::move(config)), fs_(std::move(fs)), root_(std::move(root))
            {
            }

            void write(std::string_view key, Frame frame, std::optional<Compression> compression)
            {
                require_valid_key(key);
                if (!frame.has_value()) { throw std::invalid_argument("cannot write an empty frame"); }
                const auto path = resolve(key);
                if (config_.immutable && exists(path))
                {
                    throw std::runtime_error("frame-store key already exists: " + std::string{key});
                }
                // The parent directory is implicit on S3 and required locally.
                const auto slash = path.find_last_of('/');
                if (slash != std::string::npos) { check(fs_->CreateDir(path.substr(0, slash), true), "create directory"); }

                auto out = unwrap(fs_->OpenOutputStream(path), "open output stream");
                write_table(*frame.table, out, compression.value_or(config_.compression));
                check(out->Close(), "close output stream");
            }

            [[nodiscard]] Frame read(std::string_view key)
            {
                require_valid_key(key);
                const auto path = resolve(key);
                if (!exists(path)) { return Frame{}; }
                auto in = unwrap(fs_->OpenInputFile(path), "open input file");
                return Frame{read_table(in)};
            }

            [[nodiscard]] bool contains(std::string_view key)
            {
                require_valid_key(key);
                return exists(resolve(key));
            }

            void clear()
            {
                const auto info = unwrap(fs_->GetFileInfo(root_), "stat root");
                if (info.type() == arrow::fs::FileType::NotFound) { return; }
                check(fs_->DeleteDirContents(root_, /*missing_dir_ok=*/true), "clear store");
            }

          private:
            [[nodiscard]] std::string resolve(std::string_view key) const
            {
                return root_.empty() ? std::string{key} : root_ + "/" + std::string{key};
            }

            [[nodiscard]] bool exists(const std::string &path) const
            {
                const auto info = unwrap(fs_->GetFileInfo(path), "stat");
                return info.type() != arrow::fs::FileType::NotFound;
            }

            void write_table(const arrow::Table &table, const std::shared_ptr<arrow::io::OutputStream> &out,
                             Compression compression) const
            {
                switch (config_.format)
                {
                case Format::ArrowIpc:
                {
                    auto options = arrow::ipc::IpcWriteOptions::Defaults();
                    options.codec = ipc_codec(compression);
                    auto writer = unwrap(arrow::ipc::MakeFileWriter(out, table.schema(), options), "make IPC writer");
                    check(writer->WriteTable(table), "write IPC table");
                    check(writer->Close(), "close IPC writer");
                    break;
                }
                case Format::Parquet:
#if defined(HGRAPH_WITH_PARQUET)
                {
                    auto props = parquet::WriterProperties::Builder()
                                     .compression(parquet_codec(compression))
                                     ->build();
                    // store_schema keeps the Arrow schema - and with it the RFC
                    // 0001 frame metadata - in the Parquet key-value metadata.
                    auto arrow_props = parquet::ArrowWriterProperties::Builder().store_schema()->build();
                    check(parquet::arrow::WriteTable(table, arrow::default_memory_pool(), out,
                                                     /*chunk_size=*/table.num_rows() > 0 ? table.num_rows() : 1,
                                                     props, arrow_props),
                          "write Parquet table");
                    break;
                }
#else
                    throw std::runtime_error("this build has no Parquet support; configure with HGRAPH_WITH_PARQUET");
#endif
                }
            }

            [[nodiscard]] std::shared_ptr<arrow::Table> read_table(
                const std::shared_ptr<arrow::io::RandomAccessFile> &in) const
            {
                switch (config_.format)
                {
                case Format::ArrowIpc:
                {
                    auto reader = unwrap(arrow::ipc::RecordBatchFileReader::Open(in), "open IPC reader");
                    return unwrap(reader->ToTable(), "read IPC table");
                }
                case Format::Parquet:
#if defined(HGRAPH_WITH_PARQUET)
                {
                    auto reader = unwrap(parquet::arrow::OpenFile(in, arrow::default_memory_pool()),
                                         "open Parquet reader");
                    return unwrap(reader->ReadTable(), "read Parquet table");
                }
#else
                    throw std::runtime_error("this build has no Parquet support; configure with HGRAPH_WITH_PARQUET");
#endif
                }
                return nullptr;
            }

            [[nodiscard]] static std::shared_ptr<arrow::util::Codec> ipc_codec(Compression compression)
            {
                switch (compression)
                {
                case Compression::None:
                case Compression::Default: return nullptr;
                case Compression::Snappy:
                    // Arrow IPC permits only LZ4_FRAME and ZSTD; snappy maps to zstd.
                case Compression::Zstd:
                    return unwrap(arrow::util::Codec::Create(arrow::Compression::ZSTD), "create zstd codec");
                }
                return nullptr;
            }

#if defined(HGRAPH_WITH_PARQUET)
            [[nodiscard]] static arrow::Compression::type parquet_codec(Compression compression)
            {
                switch (compression)
                {
                case Compression::None: return arrow::Compression::UNCOMPRESSED;
                case Compression::Snappy:
                case Compression::Default: return arrow::Compression::SNAPPY;
                case Compression::Zstd: return arrow::Compression::ZSTD;
                }
                return arrow::Compression::SNAPPY;
            }
#endif

            FrameStoreConfig                       config_;
            std::shared_ptr<arrow::fs::FileSystem> fs_;
            std::string                            root_;
        };

        [[nodiscard]] const FrameStoreOps &filesystem_store_ops() noexcept
        {
            static const FrameStoreOps ops{
                [](void *context, std::string_view key, Frame frame, std::optional<Compression> compression) {
                    static_cast<FileSystemStore *>(context)->write(key, std::move(frame), compression);
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

#if defined(HGRAPH_WITH_S3)
        /**
         * Arrow requires a process-global S3 init before first use and a
         * matching finalize: "you MUST call FinalizeS3 before the end of the
         * application in order to avoid a segmentation fault at shutdown".
         *
         * The finalize cannot be automated. Both obvious placements fail,
         * measured against a live endpoint:
         *
         *   - std::atexit, and a function-local static destructor, both run
         *     after Arrow's own statics are gone. FinalizeS3 then throws
         *     "mutex lock failed: Invalid argument" and terminates the process
         *     AFTER the tests have passed - a green run with a crashing exit.
         *   - omitting it entirely leaves Arrow warning "FinalizeS3 was not
         *     called even though S3 was initialized. This could lead to a
         *     segmentation fault at exit".
         *
         * So shutdown is the application's, which is what Arrow's own
         * documentation asks for: call finalize_s3() before exit. Init stays
         * lazy, so a process that never configures S3 never initialises it and
         * never needs to finalize.
         */
        void ensure_s3_initialized() { check(arrow::fs::EnsureS3Initialized(), "initialize S3"); }
#endif
    }  // namespace

    FrameStore make_frame_store(FrameStoreConfig config)
    {
        if (config.format == Format::Parquet && !parquet_available())
        {
            throw std::runtime_error("Parquet format requested but this build has no Parquet support");
        }

        if (std::holds_alternative<MemoryLocation>(config.location))
        {
            return FrameStore{std::make_shared<MemoryStore>(std::move(config)), memory_store_ops()};
        }

        if (const auto *local = std::get_if<LocalLocation>(&config.location))
        {
            if (local->root.empty()) { throw std::invalid_argument("a local frame store requires a root directory"); }
            auto root = local->root;
            auto fs   = std::make_shared<arrow::fs::LocalFileSystem>();
            check(fs->CreateDir(root, true), "create store root");
            return FrameStore{std::make_shared<FileSystemStore>(std::move(config), std::move(fs), std::move(root)),
                              filesystem_store_ops()};
        }

        const auto &s3 = std::get<S3Location>(config.location);
#if defined(HGRAPH_WITH_S3)
        if (s3.bucket.empty()) { throw std::invalid_argument("an S3 frame store requires a bucket"); }
        ensure_s3_initialized();

        arrow::fs::S3Options options;
        std::visit(
            [&](const auto &source) {
                using T = std::decay_t<decltype(source)>;
                if constexpr (std::is_same_v<T, Credentials::Ambient>) { options = arrow::fs::S3Options::Defaults(); }
                else if constexpr (std::is_same_v<T, Credentials::Explicit>)
                {
                    options = arrow::fs::S3Options::FromAccessKey(source.access_key_id, source.secret_access_key,
                                                                  source.session_token.value_or(std::string{}));
                }
                else if constexpr (std::is_same_v<T, Credentials::Profile>)
                {
                    // Arrow's S3Options has no named-profile factory, and
                    // building one needs Aws::Auth::ProfileConfigFileAWSCredentialsProvider
                    // from the AWS SDK, whose headers Arrow does not re-export.
                    // Fail loudly rather than silently resolving something else.
                    throw std::runtime_error(
                        "named AWS profile '" + source.name +
                        "' is not supported by the Arrow S3 backend; set AWS_PROFILE in the environment "
                        "and use Credentials::Ambient, or supply Credentials::Explicit");
                }
                else { options = arrow::fs::S3Options::FromAssumeRole(source.role_arn, source.session_name.value_or("hgraph")); }
            },
            s3.credentials.source);

        if (s3.region) { options.region = *s3.region; }
        if (s3.endpoint_override)
        {
            options.endpoint_override = *s3.endpoint_override;
            options.scheme            = s3.endpoint_override->starts_with("https://") ? "https" : "http";
        }

        auto fs   = unwrap(arrow::fs::S3FileSystem::Make(options), "create S3 filesystem");
        auto root = s3.prefix.empty() ? s3.bucket : s3.bucket + "/" + s3.prefix;
        return FrameStore{std::make_shared<FileSystemStore>(std::move(config), std::move(fs), std::move(root)),
                          filesystem_store_ops()};
#else
        (void)s3;
        throw std::runtime_error("this build has no S3 support; configure with HGRAPH_WITH_S3");
#endif
    }

}  // namespace hgraph::store
