#ifndef HGRAPH_LIB_TESTING_CHECK_OUTPUT_H
#define HGRAPH_LIB_TESTING_CHECK_OUTPUT_H

#include <hgraph/types/value/value.h>

#include <catch2/catch_test_macros.hpp>

#include <fmt/format.h>

#include <algorithm>
#include <cstddef>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

// Catch2-integrated assertions for comparing an ``eval_node`` style result — a
// ``std::vector<std::optional<T>>`` of per-cycle values — against an expected
// sequence, with a readable delta message on mismatch (Catch2's default ``==``
// stringifies ``std::optional`` as the unhelpful ``{?}``).
//
//   CHECK_OUTPUT(out, {1, 2, none, 3});      // non-fatal (CHECK semantics)
//   REQUIRE_OUTPUT(out, {1, 2, none, 3});    // fatal     (REQUIRE semantics)
//
// The expected sequence is a braced list whose element type is inferred from
// ``out`` (so plain values and ``none`` mix freely), or any existing
// ``std::vector<std::optional<T>>``. This is a test-only header (it depends on
// Catch2); it is not part of the hgraph_core library.

namespace hgraph::testing
{
    /** Shorthand for "no tick this cycle" in expected-output lists. */
    inline constexpr std::nullopt_t none = std::nullopt;

    namespace detail
    {
        // A harness element is either a scalar ``T`` (compared with ``==``) or a
        // canonical delta ``Value`` (compared with ``Value::equals``, order-independent
        // for sets/maps). Both render through the value-layer ``to_string`` so display
        // is consistent and fixable in one place (e.g. 1-byte integers show numerically).
        template <typename T>
        [[nodiscard]] std::string format_opt(const std::optional<T> &v)
        {
            if (!v.has_value()) { return std::string{"none"}; }
            if constexpr (std::is_same_v<T, Value>) { return v->view().to_string(); }
            else { return Value{*v}.to_string(); }
        }

        template <typename T>
        [[nodiscard]] bool elem_equal(const std::optional<T> &a, const std::optional<T> &b)
        {
            if (a.has_value() != b.has_value()) { return false; }
            if (!a.has_value()) { return true; }
            if constexpr (std::is_same_v<T, Value>) { return a->equals(*b); }
            else { return *a == *b; }
        }

        template <typename T>
        std::string format_seq(const std::vector<std::optional<T>> &v)
        {
            std::string s = "[";
            for (std::size_t i = 0; i < v.size(); ++i)
            {
                if (i != 0) { s += ", "; }
                s += format_opt(v[i]);
            }
            s += "]";
            return s;
        }

        template <typename T>
        std::string output_delta_message(const std::vector<std::optional<T>> &actual,
                                         const std::vector<std::optional<T>> &expected)
        {
            const std::size_t na = actual.size();
            const std::size_t ne = expected.size();

            std::string msg = (na == ne) ? fmt::format("output mismatch ({} elements):", na)
                                         : fmt::format("output mismatch (sizes differ: actual {}, expected {}):", na, ne);
            msg += fmt::format("\n  actual:   {}", format_seq(actual));
            msg += fmt::format("\n  expected: {}", format_seq(expected));

            const std::size_t n = std::max(na, ne);
            for (std::size_t i = 0; i < n; ++i)
            {
                const bool in_a = i < na;
                const bool in_e = i < ne;
                if (in_a && in_e)
                {
                    if (!elem_equal(actual[i], expected[i]))
                    {
                        msg += fmt::format("\n  > index {}: actual = {}, expected = {}", i, format_opt(actual[i]),
                                           format_opt(expected[i]));
                    }
                }
                else if (in_a)
                {
                    msg += fmt::format("\n  > index {}: actual = {}, expected = (missing)", i, format_opt(actual[i]));
                }
                else
                {
                    msg += fmt::format("\n  > index {}: actual = (missing), expected = {}", i, format_opt(expected[i]));
                }
            }
            return msg;
        }

        // Returns true when equal; otherwise fills ``msg_out`` with the delta. The
        // expected list's element type is deduced from ``actual`` (the braced-list
        // argument is a non-deduced context), so a mixed value/``none`` list works.
        template <typename T>
        bool compare_output(const std::vector<std::optional<T>> &actual, const std::vector<std::optional<T>> &expected,
                            std::string &msg_out)
        {
            bool equal = actual.size() == expected.size();
            for (std::size_t i = 0; equal && i < actual.size(); ++i) { equal = elem_equal(actual[i], expected[i]); }
            if (equal) { return true; }
            msg_out = output_delta_message(actual, expected);
            return false;
        }

        // Compare a *type-erased* actual (per-cycle canonical ``Value`` deltas — e.g. from the
        // operator ``eval_node`` harness) against a **typed** expected sequence: each expected
        // scalar is boxed into a ``Value`` and compared with ``Value::equals``. This lets a
        // test write the expected with the same ``values<T>(...)`` helper it uses for inputs,
        // instead of spelling out ``value<T>(...)`` per element.
        template <typename U>
            requires(!std::is_same_v<U, Value>)
        bool compare_output(const std::vector<std::optional<Value>> &actual,
                            const std::vector<std::optional<U>> &expected, std::string &msg_out)
        {
            std::vector<std::optional<Value>> boxed;
            boxed.reserve(expected.size());
            for (const std::optional<U> &element : expected)
            {
                if (element.has_value()) { boxed.emplace_back(Value{*element}); }
                else { boxed.emplace_back(std::nullopt); }
            }
            return compare_output<Value>(actual, boxed, msg_out);
        }

        template <typename T, typename U>
        [[nodiscard]] std::optional<T> make_optional_value(U &&value)
        {
            return T{std::forward<U>(value)};
        }

        template <typename T>
        [[nodiscard]] std::optional<T> make_optional_value(std::nullopt_t)
        {
            return std::nullopt;
        }
    }  // namespace detail

    template <typename T, typename... Args>
    [[nodiscard]] std::vector<std::optional<T>> values(Args &&...args)
    {
        std::vector<std::optional<T>> out;
        out.reserve(sizeof...(Args));
        (out.push_back(detail::make_optional_value<T>(std::forward<Args>(args))), ...);
        return out;
    }
}  // namespace hgraph::testing

/** Non-fatal: compare ``actual`` to a per-cycle expected sequence; on mismatch
 *  report a delta and continue. */
#define CHECK_OUTPUT(actual, ...)                                                                       \
    do                                                                                                  \
    {                                                                                                   \
        std::string _hg_check_output_msg;                                                               \
        if (::hgraph::testing::detail::compare_output((actual), __VA_ARGS__, _hg_check_output_msg))     \
        {                                                                                               \
            SUCCEED("output matches");                                                                  \
        }                                                                                               \
        else                                                                                            \
        {                                                                                               \
            FAIL_CHECK(_hg_check_output_msg);                                                           \
        }                                                                                               \
    } while (false)

/** Fatal: like ``CHECK_OUTPUT`` but aborts the test on mismatch. */
#define REQUIRE_OUTPUT(actual, ...)                                                                     \
    do                                                                                                  \
    {                                                                                                   \
        std::string _hg_check_output_msg;                                                               \
        if (::hgraph::testing::detail::compare_output((actual), __VA_ARGS__, _hg_check_output_msg))     \
        {                                                                                               \
            SUCCEED("output matches");                                                                  \
        }                                                                                               \
        else                                                                                            \
        {                                                                                               \
            FAIL(_hg_check_output_msg);                                                                 \
        }                                                                                               \
    } while (false)

#endif  // HGRAPH_LIB_TESTING_CHECK_OUTPUT_H
