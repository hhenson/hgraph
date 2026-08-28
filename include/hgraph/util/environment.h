#ifndef HGRAPH_UTIL_ENVIRONMENT_H
#define HGRAPH_UTIL_ENVIRONMENT_H

#include <cerrno>
#include <cstdlib>
#include <memory>
#include <new>
#include <optional>
#include <string>

namespace hgraph
{
    /**
     * Return an owned snapshot of a process environment variable.
     *
     * MSVC deprecates ``getenv`` because its borrowed pointer can be invalidated
     * by later environment mutation. ``_dupenv_s`` provides the same owned
     * semantics used on every platform here.
     */
    [[nodiscard]] inline std::optional<std::string> environment_variable(const char *name)
    {
#if defined(_MSC_VER)
        char       *buffer = nullptr;
        std::size_t size = 0;
        const errno_t error = ::_dupenv_s(&buffer, &size, name);
        if (error == ENOMEM) { throw std::bad_alloc{}; }
        if (error != 0 || buffer == nullptr) { return std::nullopt; }

        const std::unique_ptr<char, decltype(&std::free)> owned{buffer, &std::free};
        return std::string{owned.get()};
#else
        const char *value = std::getenv(name);
        return value == nullptr ? std::nullopt : std::optional<std::string>{std::in_place, value};
#endif
    }
}

#endif  // HGRAPH_UTIL_ENVIRONMENT_H
