#ifndef HGRAPH_UTIL_ERRORS
#define HGRAPH_UTIL_ERRORS

#include <ddv/serial_visitor.h>

#include <fmt/format.h>

#include <concepts>
#include <stdexcept>
#include <source_location>
#include <stacktrace>
#include <string_view>

namespace hgraph {

    inline constexpr unsigned MAX_STACKTRACE_DEPTH = 20;

    template<typename Error = std::runtime_error, typename... Ts>
        requires (!std::constructible_from<Error, std::string>)
    [[noreturn]] constexpr auto throw_error(Ts&&... args) {
        throw Error{std::forward<Ts>(args)...};
    }

    // Overload (I) - takes error msg and appends source location & stacktrace info
    template<typename Error = std::runtime_error>
        requires std::constructible_from<Error, std::string>
    [[noreturn]] constexpr auto throw_error(
        std::string_view msg,
        std::source_location loc = std::source_location::current(),
        std::stacktrace trace = std::stacktrace::current(0, MAX_STACKTRACE_DEPTH)
    ) {
        // append location and stacktrace to error msg
        throw Error{fmt::format(
            "{}\nFile: {}({}:{}): {}\nStacktrace:\n{}", msg,
            loc.file_name(), loc.line(), loc.column(), loc.function_name(), to_string(trace)
        )};
    }

    // Overload (II) - direct formatting of error msg from args, only stacktrace is appended to the message
    template<typename Error = std::runtime_error, typename... Ts>
        requires (std::constructible_from<Error, std::string> && sizeof...(Ts) > 0)
    [[noreturn]] constexpr auto throw_error(fmt::format_string<Ts...> fmt_str, Ts&&... xs) {
        const std::stacktrace trace = std::stacktrace::current(1, MAX_STACKTRACE_DEPTH);
        throw Error{fmt::format(
            "{}\nStacktrace:\n{}", fmt::format(fmt_str, std::forward<Ts>(xs)...), to_string(trace)
        )};
    }

    template<typename Error = std::runtime_error, typename... Ts>
    constexpr auto make_throw_error(Ts... xs)
    requires requires { throw_error<Error>(xs...); } {
        if constexpr (requires { throw_error<Error>(xs..., std::declval<std::source_location>()); })
            return [...xs = std::move(xs)](std::source_location loc = std::source_location::current()) {
                throw_error<Error>(xs..., std::move(loc));
            };
        else
            return [...xs = std::move(xs)] { throw_error<Error>(xs...); };
    }

    template<typename ExpectedT>
    struct bad_expected_type : std::runtime_error {
        bad_expected_type(std::string_view rt_type_name)
            : std::runtime_error{fmt::format("Expected type '{}', got: {}", typeid(ExpectedT).name(), rt_type_name)}
        {}
    };

    template<typename ExpectedT>
    inline constexpr auto throw_if_not_expected = []<typename T>(T, std::source_location loc = std::source_location::current()) {
        // -> Overload (I)
        throw_error<bad_expected_type<ExpectedT>>(typeid(T).name(), std::move(loc));
    };

    template<typename ExpectedT>
    constexpr auto make_throw_if_not_expected(std::source_location loc = std::source_location::current()) noexcept {
        return [loc = std::move(loc)]<typename T>(T) {
            // -> Overload (I)
            throw_error<bad_expected_type<ExpectedT>>(typeid(T).name(), loc);
        };
    }

    template<typename T>
    inline constexpr auto cast_to_expected = ddv::serial{ ddv::identity<T>, throw_if_not_expected<T> };

    template<typename T, typename F>
    constexpr auto with_expected(F&& f, std::source_location loc = std::source_location::current()) {
        return ddv::serial{
            [f = std::forward<F>(f)](T x) { return f(x); },
            make_throw_if_not_expected<T>(std::move(loc))
        };
    }

} // namespace hgraph

#endif // HGRAPH_UTIL_ERRORS
