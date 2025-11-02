//
// Created by Howard Henson on 17/05/2025.
//

#ifndef FORMAT_H
#define FORMAT_H

#include <format>
#include <nanobind/nanobind.h>

namespace std {
    template<typename Py_T, typename CharT>
        requires std::is_same_v<Py_T, nanobind::handle> || std::is_same_v<Py_T, nanobind::object>
    struct formatter<Py_T, CharT> {
        bool use_repr = false;

        // Parse formatting options if needed
        constexpr auto parse(std::format_parse_context &ctx) {
            auto it = ctx.begin();
            auto end = ctx.end();

            // If we reached the end of the format string, return
            if (it == end || *it == '}') { return it; }

            // Check for format specifiers
            if (*it == ':') {
                ++it;
                if (it != end && *it == 'r') {
                    use_repr = true;
                    ++it;
                }
            }

            // Check that we've reached the end of the format spec
            if (it != end && *it != '}') { throw format_error("Invalid format specifier for nanobind::handle"); }
            return it;
        }

        // Format the value and output it to the context
        auto format(const Py_T &value, std::format_context &ctx) const {
            if (use_repr) { return format_to(ctx.out(), "{}", nanobind::repr(value).c_str()); }
            return format_to(ctx.out(), "{}", nanobind::str(value).c_str());
        }
    };

    using handle_formatter = formatter<nanobind::handle>;
    using object_formatter = formatter<nanobind::object>;
} // namespace std

#endif  // FORMAT_H