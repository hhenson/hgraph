#include <hgraph/runtime/distributed_transport.h>

#include <hgraph/runtime/distributed_protocol.h>

#include <fmt/format.h>

#include <cstdint>
#include <algorithm>
#include <atomic>
#include <limits>
#include <hgraph/util/scope.h>
#include <stdexcept>
#include <utility>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#else
#include <cerrno>
#include <fcntl.h>
#include <poll.h>
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
        [[noreturn]] void timed_out()
        { throw std::runtime_error("distributed transport: worker deadline exceeded"); }

        int remaining_ms(PipeEndpoint::Deadline deadline)
        {
            if (deadline == PipeEndpoint::Deadline::max()) return -1;
            const auto left = deadline - std::chrono::steady_clock::now();
            if (left <= decltype(left)::zero()) timed_out();
            const auto ms = std::chrono::ceil<std::chrono::milliseconds>(left).count();
            return static_cast<int>(std::min(ms, static_cast<std::chrono::milliseconds::rep>(std::numeric_limits<int>::max())));
        }
#ifdef _WIN32
        DWORD transfer(HANDLE handle, void *data, DWORD size, bool writing,
                       PipeEndpoint::Deadline deadline)
        {
            OVERLAPPED operation{};
            operation.hEvent = ::CreateEventW(nullptr, TRUE, FALSE, nullptr);
            if (!operation.hEvent) fail("CreateEvent");
            auto close_event = make_scope_exit([&] { ::CloseHandle(operation.hEvent); });
            (void)remaining_ms(deadline);
            DWORD transferred = 0;
            const BOOL immediate = writing ? ::WriteFile(handle, data, size, &transferred, &operation)
                                           : ::ReadFile(handle, data, size, &transferred, &operation);
            if (immediate) return transferred;
            const DWORD error = ::GetLastError();
            if (!writing && (error == ERROR_BROKEN_PIPE || error == ERROR_PIPE_NOT_CONNECTED)) return 0;
            if (error != ERROR_IO_PENDING) fail(writing ? "write" : "read");
            // Keep the stack OVERLAPPED and its buffer alive until cancellation
            // has completed, even when checking the deadline itself throws.
            auto cancel = make_scope_exit([&] {
                (void)::CancelIoEx(handle, &operation);
                (void)::GetOverlappedResult(handle, &operation, &transferred, TRUE);
            });
            const int left = remaining_ms(deadline);
            const DWORD status = ::WaitForSingleObject(operation.hEvent, left < 0 ? INFINITE : static_cast<DWORD>(left));
            if (status == WAIT_TIMEOUT) timed_out();
            if (status != WAIT_OBJECT_0) fail("WaitForSingleObject");
            const BOOL done = ::GetOverlappedResult(handle, &operation, &transferred, FALSE);
            cancel.release();
            if (!done)
            {
                const auto completion_error = ::GetLastError();
                if (!writing && (completion_error == ERROR_BROKEN_PIPE || completion_error == ERROR_PIPE_NOT_CONNECTED)) return 0;
                fail(writing ? "write" : "read");
            }
            return transferred;
        }
#else
        void configure_socket(int fd)
        {
            // MSG_DONTWAIT alone does not bound large AF_UNIX sends on every
            // supported kernel. Descriptor-level nonblocking mode makes poll
            // the only waiting operation, including inherited/adopted sockets.
            const int flags = ::fcntl(fd, F_GETFL);
            if (flags < 0 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) fail("fcntl(O_NONBLOCK)");
#ifdef SO_NOSIGPIPE
            int enabled = 0;
            socklen_t length = sizeof(enabled);
            if (::getsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &enabled, &length) < 0)
                fail("getsockopt(SO_NOSIGPIPE)");
            // An inherited socket is already configured. Re-setting this
            // option after its peer closes can fail on macOS during adoption.
            if (!enabled)
            {
                enabled = 1;
                if (::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &enabled, sizeof(enabled)) < 0)
                    fail("setsockopt(SO_NOSIGPIPE)");
            }
#endif
        }

        void ready(int fd, short events, PipeEndpoint::Deadline deadline)
        {
            pollfd item{fd, events, 0};
            while (true)
            {
                const int status = ::poll(&item, 1, remaining_ms(deadline));
                if (status > 0) return; // read/send diagnoses EOF and errors
                if (status == 0) timed_out();
                if (errno != EINTR) fail("poll");
            }
        }
#endif
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
        if (write_handle_ != nullptr && write_handle_ != read_handle_) { ::CloseHandle(static_cast<HANDLE>(write_handle_)); }
        read_handle_  = nullptr;
        write_handle_ = nullptr;
    }

    PipeEndpoint PipeEndpoint::adopt(std::int64_t read_handle, std::int64_t write_handle)
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

    void PipeEndpoint::send(std::string_view payload, Deadline deadline)
    {
        const std::string framed = write_frame(payload);
        std::size_t sent = 0;
        while (sent < framed.size())
        {
            const auto size = static_cast<DWORD>(std::min<std::size_t>(framed.size() - sent, MAXDWORD));
            const auto written = transfer(static_cast<HANDLE>(write_handle_),
                const_cast<char *>(framed.data() + sent), size, true, deadline);
            if (written == 0) throw std::runtime_error("distributed transport: zero-byte write");
            sent += written;
        }
    }

    bool PipeEndpoint::receive(std::string &payload, Deadline deadline)
    {
        while (true)
        {
            std::string_view frame;
            std::size_t consumed = 0;
            if (read_frame(buffer_, frame, consumed))
            {
                payload = std::string{frame};
                buffer_.erase(0, consumed);
                return true;
            }
            char chunk[4096];
            const auto got = transfer(static_cast<HANDLE>(read_handle_), chunk, sizeof(chunk), false, deadline);
            if (got == 0)
            {
                if (!buffer_.empty()) throw std::runtime_error("distributed transport: the far end closed mid-message");
                return false;
            }
            buffer_.append(chunk, got);
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

    PipeEndpoint PipeEndpoint::adopt(std::int64_t read_handle, std::int64_t write_handle)
    {
        PipeEndpoint adopted;
        adopted.read_fd_  = static_cast<int>(read_handle);
        adopted.write_fd_ = static_cast<int>(write_handle);
        configure_socket(adopted.read_fd_);
        if (adopted.write_fd_ != adopted.read_fd_) configure_socket(adopted.write_fd_);
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

    void PipeEndpoint::send(std::string_view payload, Deadline deadline)
    {
        const std::string framed = write_frame(payload);
        std::size_t       sent   = 0;
        while (sent < framed.size())
        {
            ready(write_fd_, POLLOUT, deadline);
#ifdef MSG_NOSIGNAL
            constexpr int flags = MSG_DONTWAIT | MSG_NOSIGNAL;
#else
            constexpr int flags = MSG_DONTWAIT;
#endif
            const auto written = ::send(write_fd_, framed.data() + sent, framed.size() - sent, flags);
            if (written < 0)
            {
                if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) { continue; }
                fail("write");
            }
            if (written == 0) throw std::runtime_error("distributed transport: zero-byte write");
            sent += static_cast<std::size_t>(written);
        }
    }

    bool PipeEndpoint::receive(std::string &payload, Deadline deadline)
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
            ready(read_fd_, POLLIN, deadline);
            const auto got = ::recv(read_fd_, chunk, sizeof(chunk), MSG_DONTWAIT);
            if (got < 0)
            {
                if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) { continue; }
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
        // Overlapped duplex handles permit cancellation of blocked reads AND
        // writes. Anonymous pipes cannot provide bounded dispatch on Windows.
        static std::atomic<unsigned long long> sequence{0};
        const auto name = std::wstring{L"\\\\.\\pipe\\hgraph-"} + std::to_wstring(::GetCurrentProcessId()) +
                          L"-" + std::to_wstring(sequence.fetch_add(1));
        HANDLE server = ::CreateNamedPipeW(name.c_str(),
            PIPE_ACCESS_DUPLEX | FILE_FLAG_OVERLAPPED | FILE_FLAG_FIRST_PIPE_INSTANCE,
            PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT | PIPE_REJECT_REMOTE_CLIENTS,
            1, 65536, 65536, 0, nullptr);
        if (server == INVALID_HANDLE_VALUE) fail("CreateNamedPipe");
        auto close_server = make_scope_exit([&] { ::CloseHandle(server); });
        HANDLE client = ::CreateFileW(name.c_str(), GENERIC_READ | GENERIC_WRITE, 0, nullptr,
                                      OPEN_EXISTING, FILE_FLAG_OVERLAPPED, nullptr);
        if (client == INVALID_HANDLE_VALUE) fail("CreateFile");
        auto close_client = make_scope_exit([&] { ::CloseHandle(client); });
        // The client connected before this call; Windows reports that as the
        // successful ERROR_PIPE_CONNECTED case, without an outstanding I/O.
        OVERLAPPED connection{};
        if (!::ConnectNamedPipe(server, &connection) && ::GetLastError() != ERROR_PIPE_CONNECTED)
            fail("ConnectNamedPipe");
        first.read_handle_ = first.write_handle_ = server;
        second.read_handle_ = second.write_handle_ = client;
        close_server.release();
        close_client.release();
#else
        int fds[2]{-1, -1};
        if (::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) { fail("socketpair"); }
        // Bidirectional, so each end uses one fd for both directions; close()
        // knows they alias.
        first.read_fd_   = fds[0];
        first.write_fd_  = fds[0];
        second.read_fd_  = fds[1];
        second.write_fd_ = fds[1];
        configure_socket(fds[0]);
        configure_socket(fds[1]);
#endif
    }
}  // namespace hgraph::distributed
