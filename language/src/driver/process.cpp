#include "driver/process.h"

#include <cerrno>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

#if defined(_WIN32)
#include <windows.h>
#else
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace hgl::driver
{
#if defined(_WIN32)
    namespace
    {
        std::string windows_argument(std::string_view argument)
        {
            if (argument.find_first_of(" \t\n\v\"") == std::string_view::npos)
            {
                return std::string{argument};
            }

            std::string quoted{"\""};
            std::size_t backslashes = 0;
            for (const char c : argument)
            {
                if (c == '\\')
                {
                    ++backslashes;
                    continue;
                }
                if (c == '"')
                {
                    quoted.append(backslashes * 2 + 1, '\\');
                    quoted += c;
                    backslashes = 0;
                    continue;
                }
                quoted.append(backslashes, '\\');
                backslashes = 0;
                quoted += c;
            }
            quoted.append(backslashes * 2, '\\');
            quoted += '"';
            return quoted;
        }

        std::string windows_error(std::string_view action)
        {
            return std::string{action} + " (Windows error " + std::to_string(GetLastError()) + ")";
        }
    }  // namespace

    ProcessResult run_process(std::span<const std::string> arguments)
    {
        if (arguments.empty())
        {
            return {-1, "cannot start an empty process command"};
        }

        std::string command_line;
        for (const std::string &argument : arguments)
        {
            if (!command_line.empty())
            {
                command_line += ' ';
            }
            command_line += windows_argument(argument);
        }

        SECURITY_ATTRIBUTES security{sizeof(SECURITY_ATTRIBUTES), nullptr, TRUE};
        HANDLE output_read = nullptr;
        HANDLE output_write = nullptr;
        if (!CreatePipe(&output_read, &output_write, &security, 0))
        {
            return {-1, windows_error("cannot create process output pipe")};
        }
        if (!SetHandleInformation(output_read, HANDLE_FLAG_INHERIT, 0))
        {
            const std::string error = windows_error("cannot configure process output pipe");
            CloseHandle(output_read);
            CloseHandle(output_write);
            return {-1, error};
        }

        STARTUPINFOA startup{};
        startup.cb = sizeof(startup);
        startup.dwFlags = STARTF_USESTDHANDLES;
        startup.hStdOutput = output_write;
        startup.hStdError = output_write;
        startup.hStdInput = GetStdHandle(STD_INPUT_HANDLE);
        PROCESS_INFORMATION process{};
        const BOOL started = CreateProcessA(nullptr, command_line.data(), nullptr, nullptr, TRUE, CREATE_NO_WINDOW,
                                            nullptr, nullptr, &startup, &process);
        CloseHandle(output_write);
        if (!started)
        {
            const std::string error = windows_error("cannot start process");
            CloseHandle(output_read);
            return {-1, error};
        }

        CloseHandle(process.hThread);
        std::string output;
        char buffer[4096];
        DWORD count = 0;
        while (ReadFile(output_read, buffer, sizeof(buffer), &count, nullptr) && count != 0)
        {
            output.append(buffer, static_cast<std::size_t>(count));
        }
        CloseHandle(output_read);

        if (WaitForSingleObject(process.hProcess, INFINITE) != WAIT_OBJECT_0)
        {
            const std::string error = windows_error("cannot wait for process");
            CloseHandle(process.hProcess);
            return {-1, output + error};
        }
        DWORD status = 0;
        if (!GetExitCodeProcess(process.hProcess, &status))
        {
            const std::string error = windows_error("cannot read process status");
            CloseHandle(process.hProcess);
            return {-1, output + error};
        }
        CloseHandle(process.hProcess);
        return {static_cast<int>(status), std::move(output)};
    }
#else
    ProcessResult run_process(std::span<const std::string> arguments)
    {
        if (arguments.empty())
        {
            return {-1, "cannot start an empty process command"};
        }

        int output_pipe[2];
        if (::pipe(output_pipe) != 0)
        {
            return {-1, "cannot create process output pipe: " + std::string{std::strerror(errno)}};
        }
        const pid_t child = ::fork();
        if (child < 0)
        {
            const std::string message = "cannot start process: " + std::string{std::strerror(errno)};
            ::close(output_pipe[0]);
            ::close(output_pipe[1]);
            return {-1, message};
        }
        if (child == 0)
        {
            ::close(output_pipe[0]);
            (void)::dup2(output_pipe[1], STDOUT_FILENO);
            (void)::dup2(output_pipe[1], STDERR_FILENO);
            ::close(output_pipe[1]);
            std::vector<char *> argv;
            argv.reserve(arguments.size() + 1);
            for (const std::string &argument : arguments)
            {
                argv.push_back(const_cast<char *>(argument.c_str()));
            }
            argv.push_back(nullptr);
            ::execvp(argv.front(), argv.data());
            _exit(127);
        }

        ::close(output_pipe[1]);
        std::string output;
        char buffer[4096];
        while (true)
        {
            const ssize_t count = ::read(output_pipe[0], buffer, sizeof(buffer));
            if (count > 0)
            {
                output.append(buffer, static_cast<std::size_t>(count));
            }
            else if (count == 0)
            {
                break;
            }
            else if (errno != EINTR)
            {
                output += "cannot read process output: " + std::string{std::strerror(errno)};
                break;
            }
        }
        ::close(output_pipe[0]);

        int status = 0;
        while (::waitpid(child, &status, 0) < 0)
        {
            if (errno != EINTR)
            {
                return {-1, output + "cannot wait for process"};
            }
        }
        if (WIFEXITED(status))
        {
            return {WEXITSTATUS(status), std::move(output)};
        }
        if (WIFSIGNALED(status))
        {
            return {128 + WTERMSIG(status),
                    output + "process terminated by signal " + std::to_string(WTERMSIG(status))};
        }
        return {-1, std::move(output)};
    }
#endif
}  // namespace hgl::driver
