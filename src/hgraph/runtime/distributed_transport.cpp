#include <hgraph/runtime/distributed_transport.h>

#include <hgraph/runtime/distributed_protocol.h>

#include <fmt/format.h>

#include <cstdint>
#include <stdexcept>
#include <utility>

#ifdef _WIN32
#include <windows.h>
#else
#include <cerrno>
#include <fcntl.h>
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

    PipeEndpoint PipeEndpoint::adopt(std::int64_t read_handle, std::int64_t write_handle) noexcept
    {
        PipeEndpoint adopted;
        adopted.read_handle_  = reinterpret_cast<void *>(static_cast<std::intptr_t>(read_handle));
        adopted.write_handle_ = reinterpret_cast<void *>(static_cast<std::intptr_t>(write_handle));
        return adopted;
    }

    std::int64_t PipeEndpoint::native_read_handle() const noexcept
    {
        return static_cast<std::int64_t>(reinterpret_cast<std::intptr_t>(read_handle_));
    }

    std::int64_t PipeEndpoint::native_write_handle() const noexcept
    {
        return static_cast<std::int64_t>(reinterpret_cast<std::intptr_t>(write_handle_));
    }

    void PipeEndpoint::set_inheritable(bool inheritable) const
    {
        const DWORD flags = inheritable ? HANDLE_FLAG_INHERIT : 0;
        if (read_handle_ != nullptr &&
            ::SetHandleInformation(static_cast<HANDLE>(read_handle_), HANDLE_FLAG_INHERIT, flags) ==
                0)
        {
            fail("SetHandleInformation");
        }
        if (write_handle_ != nullptr &&
            ::SetHandleInformation(static_cast<HANDLE>(write_handle_), HANDLE_FLAG_INHERIT,
                                   flags) == 0)
        {
            fail("SetHandleInformation");
        }
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

    PipeEndpoint PipeEndpoint::adopt(std::int64_t read_handle, std::int64_t write_handle) noexcept
    {
        PipeEndpoint adopted;
        adopted.read_fd_  = static_cast<int>(read_handle);
        adopted.write_fd_ = static_cast<int>(write_handle);
        return adopted;
    }

    std::int64_t PipeEndpoint::native_read_handle() const noexcept { return read_fd_; }

    std::int64_t PipeEndpoint::native_write_handle() const noexcept { return write_fd_; }

    void PipeEndpoint::set_inheritable(bool inheritable) const
    {
        const auto mark = [inheritable](int fd) {
            if (fd < 0) { return; }
            const int flags = ::fcntl(fd, F_GETFD);
            if (flags < 0) { fail("fcntl"); }
            const int wanted = inheritable ? (flags & ~FD_CLOEXEC) : (flags | FD_CLOEXEC);
            if (wanted != flags && ::fcntl(fd, F_SETFD, wanted) < 0) { fail("fcntl"); }
        };
        mark(read_fd_);
        if (write_fd_ != read_fd_) { mark(write_fd_); }
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
        // Created NON-inheritable: a spawner marks the one end the child is
        // meant to have (``set_inheritable``), so nothing else leaks into it.
        SECURITY_ATTRIBUTES no_inherit{sizeof(SECURITY_ATTRIBUTES), nullptr, FALSE};
        if (::CreatePipe(&a_read, &b_write, &no_inherit, 0) == 0) { fail("CreatePipe"); }
        if (::CreatePipe(&b_read, &a_write, &no_inherit, 0) == 0)
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
