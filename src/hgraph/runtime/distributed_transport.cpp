#include <hgraph/runtime/distributed_transport.h>

#include <hgraph/runtime/distributed_protocol.h>

#include <fmt/format.h>

#include <stdexcept>
#include <utility>

#ifdef _WIN32
#include <windows.h>
#else
#include <cerrno>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace hgraph::distributed
{
    namespace
    {
        [[noreturn]] void fail(const char *what)
        {
#ifdef _WIN32
            throw std::runtime_error(
                fmt::format("distributed transport: {} failed ({})", what, ::GetLastError()));
#else
            throw std::runtime_error(
                fmt::format("distributed transport: {} failed ({})", what, errno));
#endif
        }
    }  // namespace

    PipeEndpoint::PipeEndpoint(PipeEndpoint &&other) noexcept
        : buffer_(std::move(other.buffer_))
    {
#ifdef _WIN32
        read_handle_        = std::exchange(other.read_handle_, nullptr);
        write_handle_       = std::exchange(other.write_handle_, nullptr);
#else
        read_fd_  = std::exchange(other.read_fd_, -1);
        write_fd_ = std::exchange(other.write_fd_, -1);
#endif
    }

    PipeEndpoint &PipeEndpoint::operator=(PipeEndpoint &&other) noexcept
    {
        if (this != &other)
        {
            close();
            buffer_ = std::move(other.buffer_);
#ifdef _WIN32
            read_handle_  = std::exchange(other.read_handle_, nullptr);
            write_handle_ = std::exchange(other.write_handle_, nullptr);
#else
            read_fd_  = std::exchange(other.read_fd_, -1);
            write_fd_ = std::exchange(other.write_fd_, -1);
#endif
        }
        return *this;
    }

    PipeEndpoint::~PipeEndpoint() { close(); }

#ifdef _WIN32

    bool PipeEndpoint::open() const noexcept
    {
        return read_handle_ != nullptr && write_handle_ != nullptr;
    }

    void PipeEndpoint::close() noexcept
    {
        if (read_handle_ != nullptr) { ::CloseHandle(static_cast<HANDLE>(read_handle_)); }
        if (write_handle_ != nullptr) { ::CloseHandle(static_cast<HANDLE>(write_handle_)); }
        read_handle_  = nullptr;
        write_handle_ = nullptr;
    }

    void PipeEndpoint::send(std::string_view payload)
    {
        const std::string framed = write_frame(payload);
        std::size_t       sent   = 0;
        while (sent < framed.size())
        {
            DWORD written = 0;
            if (::WriteFile(static_cast<HANDLE>(write_handle_), framed.data() + sent,
                            static_cast<DWORD>(framed.size() - sent), &written, nullptr) == 0)
            {
                fail("write");
            }
            sent += written;
        }
    }

    bool PipeEndpoint::receive(std::string &payload)
    {
        while (true)
        {
            std::string_view frame;
            std::size_t      consumed = 0;
            if (read_frame(buffer_, frame, consumed))
            {
                payload = std::string{frame};
                buffer_.erase(0, consumed);
                return true;
            }

            char  chunk[4096];
            DWORD read = 0;
            if (::ReadFile(static_cast<HANDLE>(read_handle_), chunk, sizeof(chunk), &read,
                           nullptr) == 0)
            {
                const auto error = ::GetLastError();
                // A closed far end reads as a broken pipe rather than EOF.
                if (error != ERROR_BROKEN_PIPE && error != ERROR_PIPE_NOT_CONNECTED)
                {
                    fail("read");
                }
                read = 0;
            }
            if (read == 0)
            {
                if (!buffer_.empty())
                {
                    throw std::runtime_error(
                        "distributed transport: the far end closed mid-message");
                }
                return false;
            }
            buffer_.append(chunk, read);
        }
    }

#else

    bool PipeEndpoint::open() const noexcept { return read_fd_ >= 0 && write_fd_ >= 0; }

    void PipeEndpoint::close() noexcept
    {
        // socketpair gives one bidirectional fd per end, so the two members
        // alias and must not be closed twice.
        if (read_fd_ >= 0) { ::close(read_fd_); }
        if (write_fd_ >= 0 && write_fd_ != read_fd_) { ::close(write_fd_); }
        read_fd_  = -1;
        write_fd_ = -1;
    }

    void PipeEndpoint::send(std::string_view payload)
    {
        const std::string framed = write_frame(payload);
        std::size_t       sent   = 0;
        while (sent < framed.size())
        {
            const auto written = ::write(write_fd_, framed.data() + sent, framed.size() - sent);
            if (written < 0)
            {
                if (errno == EINTR) { continue; }   // a signal, not a failure
                fail("write");
            }
            sent += static_cast<std::size_t>(written);
        }
    }

    bool PipeEndpoint::receive(std::string &payload)
    {
        while (true)
        {
            // Try the buffer first: a read may have delivered several frames,
            // or part of one.
            std::string_view frame;
            std::size_t      consumed = 0;
            if (read_frame(buffer_, frame, consumed))
            {
                payload = std::string{frame};
                buffer_.erase(0, consumed);
                return true;
            }

            char       chunk[4096];
            const auto got = ::read(read_fd_, chunk, sizeof(chunk));
            if (got < 0)
            {
                if (errno == EINTR) { continue; }
                fail("read");
            }
            if (got == 0)
            {
                // Clean end of stream. Anything buffered means the far side
                // died part-way through a message, which is a truncated cycle
                // rather than an orderly finish.
                if (!buffer_.empty())
                {
                    throw std::runtime_error(
                        "distributed transport: the far end closed mid-message");
                }
                return false;
            }
            buffer_.append(chunk, static_cast<std::size_t>(got));
        }
    }

#endif

    void connected_pipe_pair(PipeEndpoint &first, PipeEndpoint &second)
    {
        first.close();
        second.close();
#ifdef _WIN32
        // Two one-way pipes, crossed: each end writes into the other's reader.
        // CreatePipe's handles are synchronous, which is what a blocking
        // request/reply wants -- overlapped I/O would buy nothing here.
        HANDLE              a_read{}, b_write{}, b_read{}, a_write{};
        SECURITY_ATTRIBUTES inherit{sizeof(SECURITY_ATTRIBUTES), nullptr, TRUE};
        if (::CreatePipe(&a_read, &b_write, &inherit, 0) == 0) { fail("CreatePipe"); }
        if (::CreatePipe(&b_read, &a_write, &inherit, 0) == 0)
        {
            ::CloseHandle(a_read);
            ::CloseHandle(b_write);
            fail("CreatePipe");
        }
        first.read_handle_   = a_read;
        first.write_handle_  = a_write;
        second.read_handle_  = b_read;
        second.write_handle_ = b_write;
#else
        int fds[2]{-1, -1};
        if (::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) { fail("socketpair"); }
        // Bidirectional, so each end uses one fd for both directions; close()
        // knows they alias.
        first.read_fd_   = fds[0];
        first.write_fd_  = fds[0];
        second.read_fd_  = fds[1];
        second.write_fd_ = fds[1];
#endif
    }
}  // namespace hgraph::distributed
