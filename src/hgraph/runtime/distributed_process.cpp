#include <hgraph/runtime/distributed_process.h>

#include <hgraph/runtime/distributed_worker.h>
#include <hgraph/util/scope.h>

#include <fmt/format.h>

#include <chrono>
#include <cstdint>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

#ifdef _WIN32
#include <windows.h>
#else
#include <cerrno>
#include <spawn.h>
#include <csignal>
#include <sys/wait.h>
#include <unistd.h>
extern char **environ;
#ifdef __APPLE__
#include <mach-o/dyld.h>
#endif
#endif

namespace hgraph::distributed
{
    namespace
    {
        /**
         * How long a worker is given to notice the closed channel.
         *
         * Long enough that a worker finishing an in-flight cycle is waited
         * for, short enough that a wedged one cannot wedge the caller. The
         * distinction matters: a killed worker is a failure, and a caller that
         * blocks forever cannot report one.
         */
        constexpr auto shutdown_grace = std::chrono::seconds{5};

#ifdef _WIN32
        /**
         * Append one argument to a Windows command line, quoted.
         *
         * Windows hands a process ONE string and lets it split its own
         * arguments, so anything with a space in it must be quoted here or the
         * child sees several arguments where one was sent. That is not
         * hypothetical: MSVC renders ``typeid(...).name()`` as
         * ``struct ns::Name<...>``, so every derived recipe key contains
         * spaces. The rules are ``CommandLineToArgvW``'s, which the CRT
         * startup code uses.
         */
        void append_quoted(std::string &out, std::string_view argument)
        {
            if (!argument.empty() &&
                argument.find_first_of(" \t\n\v\"") == std::string_view::npos)
            {
                out.append(argument);
                return;
            }
            out.push_back('"');
            for (auto it = argument.begin();; ++it)
            {
                std::size_t backslashes = 0;
                while (it != argument.end() && *it == '\\')
                {
                    ++it;
                    ++backslashes;
                }
                if (it == argument.end())
                {
                    // Doubled so the closing quote is not escaped by them.
                    out.append(backslashes * 2, '\\');
                    break;
                }
                if (*it == '"')
                {
                    out.append(backslashes * 2 + 1, '\\');
                    out.push_back('"');
                }
                else
                {
                    out.append(backslashes, '\\');
                    out.push_back(*it);
                }
            }
            out.push_back('"');
        }
#endif

        [[noreturn]] void fail(const char *what)
        {
#ifdef _WIN32
            throw std::runtime_error(
                fmt::format("distributed worker process: {} failed ({})", what, ::GetLastError()));
#else
            throw std::runtime_error(
                fmt::format("distributed worker process: {} failed ({})", what, errno));
#endif
        }
    }  // namespace

    WorkerProcess::WorkerProcess(WorkerProcess &&other) noexcept
        : channel_(std::move(other.channel_)), pid_(std::exchange(other.pid_, 0))
    {
#ifdef _WIN32
        process_handle_ = std::exchange(other.process_handle_, nullptr);
#endif
    }

    WorkerProcess &WorkerProcess::operator=(WorkerProcess &&other) noexcept
    {
        if (this != &other)
        {
            // A failed reap reports -1 rather than propagating: this is
            // noexcept, and the process being replaced is already leaving.
            (void)fallback_on_exception(-1, [this] { return wait_for_exit(); });
            channel_ = std::move(other.channel_);
            pid_     = std::exchange(other.pid_, 0);
#ifdef _WIN32
            process_handle_ = std::exchange(other.process_handle_, nullptr);
#endif
        }
        return *this;
    }

    WorkerProcess::~WorkerProcess()
    {
        // A destructor cannot report, and a failed reap is not worth
        // terminating over: the process is already leaving.
        (void)fallback_on_exception(-1, [this] { return wait_for_exit(); });
    }

#ifdef _WIN32

    std::string current_executable_path()
    {
        std::wstring wide(MAX_PATH, L'\0');
        while (true)
        {
            const DWORD written =
                ::GetModuleFileNameW(nullptr, wide.data(), static_cast<DWORD>(wide.size()));
            if (written == 0) { fail("GetModuleFileName"); }
            if (written < wide.size())
            {
                wide.resize(written);
                break;
            }
            wide.resize(wide.size() * 2);
        }
        const int bytes = ::WideCharToMultiByte(CP_UTF8, 0, wide.data(),
                                                static_cast<int>(wide.size()), nullptr, 0, nullptr,
                                                nullptr);
        if (bytes <= 0) { fail("WideCharToMultiByte"); }
        std::string path(static_cast<std::size_t>(bytes), '\0');
        (void)::WideCharToMultiByte(CP_UTF8, 0, wide.data(), static_cast<int>(wide.size()),
                                    path.data(), bytes, nullptr, nullptr);
        return path;
    }

    int WorkerProcess::wait_for_exit()
    {
        if (pid_ == 0) { return 0; }
        channel_.close();   // the worker's serve loop ends on a clean close

        auto *handle = static_cast<HANDLE>(process_handle_);
        if (::WaitForSingleObject(handle, static_cast<DWORD>(shutdown_grace.count() * 1000)) !=
            WAIT_OBJECT_0)
        {
            (void)::TerminateProcess(handle, 1);
            (void)::WaitForSingleObject(handle, INFINITE);
        }
        DWORD code = 0;
        (void)::GetExitCodeProcess(handle, &code);
        (void)::CloseHandle(handle);
        process_handle_ = nullptr;
        pid_            = 0;
        return static_cast<int>(code);
    }

    WorkerProcess spawn_worker(std::string_view program, std::string_view recipe_key,
                               DateTime start_time, DateTime end_time)
    {
        const std::string executable =
            program.empty() ? current_executable_path() : std::string{program};

        PipeEndpoint mine;
        PipeEndpoint theirs;
        connected_pipe_pair(mine, theirs);
        // Only the child's end may cross, and only the two handles named
        // below: an inheritable handle list is what keeps CreateProcess from
        // handing the child everything else this process happens to hold --
        // including OUR end, which would stop the worker ever seeing a close.
        theirs.set_inheritable(true);

        std::string command;
        append_quoted(command, executable);
        for (const std::string &argument :
             {fmt::format("{}{}", worker_recipe_flag, recipe_key),
              fmt::format("{}{}", worker_read_flag, theirs.native_read_handle()),
              fmt::format("{}{}", worker_write_flag, theirs.native_write_handle()),
              fmt::format("{}{}", worker_start_flag, start_time.time_since_epoch().count()),
              fmt::format("{}{}", worker_end_flag, end_time.time_since_epoch().count())})
        {
            command.push_back(' ');
            append_quoted(command, argument);
        }

        HANDLE inherited[2]{
            reinterpret_cast<HANDLE>(static_cast<std::intptr_t>(theirs.native_read_handle())),
            reinterpret_cast<HANDLE>(static_cast<std::intptr_t>(theirs.native_write_handle()))};

        SIZE_T attribute_size = 0;
        (void)::InitializeProcThreadAttributeList(nullptr, 1, 0, &attribute_size);
        std::vector<char> attribute_storage(attribute_size);
        auto *attributes = reinterpret_cast<LPPROC_THREAD_ATTRIBUTE_LIST>(attribute_storage.data());
        if (::InitializeProcThreadAttributeList(attributes, 1, 0, &attribute_size) == 0)
        {
            fail("InitializeProcThreadAttributeList");
        }
        if (::UpdateProcThreadAttribute(attributes, 0, PROC_THREAD_ATTRIBUTE_HANDLE_LIST, inherited,
                                        sizeof(inherited), nullptr, nullptr) == 0)
        {
            ::DeleteProcThreadAttributeList(attributes);
            fail("UpdateProcThreadAttribute");
        }

        STARTUPINFOEXA startup{};
        startup.StartupInfo.cb = sizeof(STARTUPINFOEXA);
        startup.lpAttributeList = attributes;
        PROCESS_INFORMATION created{};

        const BOOL started =
            ::CreateProcessA(nullptr, command.data(), nullptr, nullptr, TRUE,
                             EXTENDED_STARTUPINFO_PRESENT, nullptr, nullptr, &startup.StartupInfo,
                             &created);
        ::DeleteProcThreadAttributeList(attributes);
        if (started == 0) { fail("CreateProcess"); }
        (void)::CloseHandle(created.hThread);

        WorkerProcess worker;
        worker.channel_        = std::move(mine);
        worker.pid_            = static_cast<long long>(created.dwProcessId);
        worker.process_handle_ = created.hProcess;
        // Our copy of the child's end goes now: while it is open, the channel
        // has a writer here and the worker's exit would never look like EOF.
        theirs.close();
        return worker;
    }

#else

    std::string current_executable_path()
    {
#ifdef __APPLE__
        std::uint32_t size = 0;
        (void)_NSGetExecutablePath(nullptr, &size);
        std::string path(size, '\0');
        if (_NSGetExecutablePath(path.data(), &size) != 0) { fail("_NSGetExecutablePath"); }
        path.resize(std::char_traits<char>::length(path.c_str()));
        return path;
#else
        std::string path(4096, '\0');
        const auto  written = ::readlink("/proc/self/exe", path.data(), path.size());
        if (written <= 0) { fail("readlink"); }
        path.resize(static_cast<std::size_t>(written));
        return path;
#endif
    }

    int WorkerProcess::wait_for_exit()
    {
        if (pid_ == 0) { return 0; }
        channel_.close();   // the worker's serve loop ends on a clean close

        const auto  deadline = std::chrono::steady_clock::now() + shutdown_grace;
        const pid_t pid      = static_cast<pid_t>(pid_);
        int         status   = 0;
        while (true)
        {
            const pid_t reaped = ::waitpid(pid, &status, WNOHANG);
            if (reaped == pid) { break; }
            if (reaped < 0 && errno != EINTR)
            {
                pid_ = 0;   // already reaped, or never ours: nothing to wait for
                return -1;
            }
            if (std::chrono::steady_clock::now() >= deadline)
            {
                (void)::kill(pid, SIGKILL);
                while (::waitpid(pid, &status, 0) < 0 && errno == EINTR) {}
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds{1});
        }
        pid_ = 0;
        return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
    }

    WorkerProcess spawn_worker(std::string_view program, std::string_view recipe_key,
                               DateTime start_time, DateTime end_time)
    {
        const std::string executable =
            program.empty() ? current_executable_path() : std::string{program};

        PipeEndpoint mine;
        PipeEndpoint theirs;
        connected_pipe_pair(mine, theirs);
        // OUR end must not cross: a child holding it keeps the channel's peer
        // alive from the inside, and the worker would never see the close that
        // ends its serve loop.
        mine.set_inheritable(false);

        // The child's descriptor is placed at a fixed number, so what the
        // worker adopts does not depend on which descriptors happened to be
        // free here.
        constexpr int channel_fd = 3;

        posix_spawn_file_actions_t actions;
        if (::posix_spawn_file_actions_init(&actions) != 0) { fail("posix_spawn_file_actions_init"); }
        if (::posix_spawn_file_actions_adddup2(&actions,
                                               static_cast<int>(theirs.native_read_handle()),
                                               channel_fd) != 0)
        {
            (void)::posix_spawn_file_actions_destroy(&actions);
            fail("posix_spawn_file_actions_adddup2");
        }

        const std::string recipe_argument = fmt::format("{}{}", worker_recipe_flag, recipe_key);
        const std::string read_argument    = fmt::format("{}{}", worker_read_flag, channel_fd);
        const std::string write_argument   = fmt::format("{}{}", worker_write_flag, channel_fd);
        const std::string start_argument =
            fmt::format("{}{}", worker_start_flag, start_time.time_since_epoch().count());
        const std::string end_argument =
            fmt::format("{}{}", worker_end_flag, end_time.time_since_epoch().count());

        char *arguments[]{const_cast<char *>(executable.c_str()),
                          const_cast<char *>(recipe_argument.c_str()),
                          const_cast<char *>(read_argument.c_str()),
                          const_cast<char *>(write_argument.c_str()),
                          const_cast<char *>(start_argument.c_str()),
                          const_cast<char *>(end_argument.c_str()),
                          nullptr};

        pid_t      child = 0;
        const int  code  = ::posix_spawn(&child, executable.c_str(), &actions, nullptr, arguments,
                                         environ);
        (void)::posix_spawn_file_actions_destroy(&actions);
        if (code != 0)
        {
            errno = code;
            fail("posix_spawn");
        }

        WorkerProcess worker;
        worker.channel_ = std::move(mine);
        worker.pid_     = static_cast<long long>(child);
        // Our copy of the child's end goes now: while it is open, the channel
        // has a peer here and the worker's exit would never look like EOF.
        theirs.close();
        return worker;
    }

#endif
}  // namespace hgraph::distributed
