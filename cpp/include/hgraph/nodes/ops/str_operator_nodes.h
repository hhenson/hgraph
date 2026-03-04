#pragma once

#include <hgraph/types/node.h>
#include <hgraph/types/value/type_registry.h>

#include <cstdint>
#include <optional>
#include <string_view>
#include <vector>

namespace hgraph {
    namespace ops {
        namespace str_ops_detail {
            inline TSBInputView require_input_bundle(Node& node) {
                return *node.input().try_as_bundle();
            }

            inline nb::str require_string_field(const TSBInputView& bundle, std::string_view field_name) {
                const nb::object obj = bundle.field(field_name).value().template as<nb::object>();
                return nb::borrow<nb::str>(obj);
            }

            inline nb::object python_field(const TSBInputView& bundle, std::string_view field_name) {
                return bundle.field(field_name).to_python();
            }

            inline int64_t require_int_field(const TSBInputView& bundle, std::string_view field_name) {
                return bundle.field(field_name).value().template as<int64_t>();
            }

            inline std::optional<int64_t> optional_int_field(const TSBInputView& bundle, std::string_view field_name) {
                auto field = bundle.field(field_name);
                if (!field || !field.valid()) {
                    return std::nullopt;
                }
                return field.value().template as<int64_t>();
            }

            inline void emit_string(Node& node, const nb::object& output_value) {
                node.output().from_python(output_value);
            }

            inline void emit_bool(Node& node, bool output_value) {
                node.output().set_value(value::View(&output_value, value::scalar_type_meta<bool>()));
            }

            inline bool string_contains(const nb::str& value, const nb::str& needle) {
                const int contains = PySequence_Contains(value.ptr(), needle.ptr());
                if (contains < 0) {
                    nb::raise_python_error();
                }
                return contains != 0;
            }

            inline nb::str string_substring(const nb::str& value, int64_t start, std::optional<int64_t> end) {
                const nb::object start_obj = nb::int_(start);
                const nb::object end_obj = end.has_value() ? nb::object(nb::int_(*end)) : nb::none();

                nb::object slice = nb::steal<nb::object>(PySlice_New(start_obj.ptr(), end_obj.ptr(), nullptr));
                if (!slice.is_valid()) {
                    nb::raise_python_error();
                }
                nb::object sliced = nb::steal<nb::object>(PyObject_GetItem(value.ptr(), slice.ptr()));
                if (!sliced.is_valid()) {
                    nb::raise_python_error();
                }
                return nb::str(sliced);
            }
        }  // namespace str_ops_detail

        struct AddStrSpec {
            static constexpr const char* py_factory_name = "op_add_str";

            static void eval(Node& node) {
                auto bundle = str_ops_detail::require_input_bundle(node);
                const nb::str lhs = str_ops_detail::require_string_field(bundle, "lhs");
                const nb::str rhs = str_ops_detail::require_string_field(bundle, "rhs");
                const nb::str output(lhs + rhs);
                str_ops_detail::emit_string(node, output);
            }
        };

        struct MulStrsSpec {
            static constexpr const char* py_factory_name = "op_mul_strs";

            static void eval(Node& node) {
                auto bundle = str_ops_detail::require_input_bundle(node);
                const nb::str lhs = str_ops_detail::require_string_field(bundle, "lhs");
                const int64_t rhs = str_ops_detail::require_int_field(bundle, "rhs");
                const nb::str output(lhs * nb::int_(rhs));
                str_ops_detail::emit_string(node, output);
            }
        };

        struct ContainsStrSpec {
            static constexpr const char* py_factory_name = "op_contains_str";

            static void eval(Node& node) {
                auto bundle = str_ops_detail::require_input_bundle(node);
                const nb::str ts = str_ops_detail::require_string_field(bundle, "ts");
                const nb::str key = str_ops_detail::require_string_field(bundle, "key");
                str_ops_detail::emit_bool(node, str_ops_detail::string_contains(ts, key));
            }
        };

        struct SubstrDefaultSpec {
            static constexpr const char* py_factory_name = "op_substr_default";

            static void eval(Node& node) {
                auto bundle = str_ops_detail::require_input_bundle(node);
                const nb::str s = str_ops_detail::require_string_field(bundle, "s");
                const int64_t start = str_ops_detail::require_int_field(bundle, "start");
                const std::optional<int64_t> end = str_ops_detail::optional_int_field(bundle, "end");
                str_ops_detail::emit_string(node, str_ops_detail::string_substring(s, start, end));
            }
        };

        struct StrDefaultSpec {
            static constexpr const char* py_factory_name = "op_str_default";

            static void eval(Node& node) {
                auto bundle = str_ops_detail::require_input_bundle(node);
                const nb::object ts = str_ops_detail::python_field(bundle, "ts");
                nb::object out = nb::steal<nb::object>(PyObject_Str(ts.ptr()));
                if (!out.is_valid()) {
                    nb::raise_python_error();
                }
                str_ops_detail::emit_string(node, out);
            }
        };

        struct StrBytesSpec {
            static constexpr const char* py_factory_name = "op_str_bytes";

            static void eval(Node& node) {
                auto bundle = str_ops_detail::require_input_bundle(node);
                const nb::object ts = str_ops_detail::python_field(bundle, "ts");
                char* bytes_ptr = nullptr;
                Py_ssize_t bytes_size = 0;
                if (PyBytes_AsStringAndSize(ts.ptr(), &bytes_ptr, &bytes_size) < 0) {
                    nb::raise_python_error();
                }
                nb::object decoded = nb::steal<nb::object>(PyUnicode_DecodeUTF8(bytes_ptr, bytes_size, nullptr));
                if (!decoded.is_valid()) {
                    nb::raise_python_error();
                }
                str_ops_detail::emit_string(node, decoded);
            }
        };

        struct MatchDefaultSpec {
            static constexpr const char* py_factory_name = "op_match_default";

            struct state {
                nb::object search;
                nb::str is_match_key;
                nb::str groups_key;
            };

            static state make_state(Node&) {
                const nb::object re_module = nb::cast<nb::object>(nb::module_::import_("re"));
                return {
                    nb::cast<nb::object>(re_module.attr("search")),
                    nb::str("is_match"),
                    nb::str("groups"),
                };
            }

            static void eval(Node& node, state& state) {
                auto bundle = str_ops_detail::require_input_bundle(node);
                const nb::str pattern = str_ops_detail::require_string_field(bundle, "pattern");
                const nb::str s = str_ops_detail::require_string_field(bundle, "s");
                const nb::object match = nb::cast<nb::object>(state.search(pattern, s));
                nb::dict out;
                if (match.is_none()) {
                    out[state.is_match_key] = nb::bool_(false);
                } else {
                    out[state.is_match_key] = nb::bool_(true);
                    out[state.groups_key] = nb::cast<nb::object>(match.attr("groups")());
                }
                node.output().from_python(out);
            }
        };

        struct ReplaceDefaultSpec {
            static constexpr const char* py_factory_name = "op_replace_default";

            struct state {
                nb::object sub;
            };

            static state make_state(Node&) {
                const nb::object re_module = nb::cast<nb::object>(nb::module_::import_("re"));
                return {nb::cast<nb::object>(re_module.attr("sub"))};
            }

            static void eval(Node& node, state& state) {
                auto bundle = str_ops_detail::require_input_bundle(node);
                const nb::str pattern = str_ops_detail::require_string_field(bundle, "pattern");
                const nb::str repl = str_ops_detail::require_string_field(bundle, "repl");
                const nb::str s = str_ops_detail::require_string_field(bundle, "s");
                str_ops_detail::emit_string(node, nb::cast<nb::object>(state.sub(pattern, repl, s)));
            }
        };

        struct SplitDefaultSpec {
            static constexpr const char* py_factory_name = "op__split_default";

            struct state {
                nb::str separator;
                int64_t maxsplit;
            };

            static state make_state(Node& node) {
                const nb::dict& scalars = node.scalars();
                return {
                    nb::str(nb::cast<nb::object>(scalars["separator"])),
                    nb::cast<int64_t>(scalars["maxsplit"]),
                };
            }

            static void eval(Node& node, state& state) {
                auto bundle = str_ops_detail::require_input_bundle(node);
                const nb::str s = str_ops_detail::require_string_field(bundle, "s");
                nb::object parts = nb::steal<nb::object>(
                    PyUnicode_Split(s.ptr(), state.separator.ptr(), static_cast<Py_ssize_t>(state.maxsplit)));
                if (!parts.is_valid()) {
                    nb::raise_python_error();
                }
                nb::object out = nb::steal<nb::object>(PySequence_Tuple(parts.ptr()));
                if (!out.is_valid()) {
                    nb::raise_python_error();
                }
                node.output().from_python(out);
            }
        };

        struct JoinStrTupleSpec {
            static constexpr const char* py_factory_name = "op_join_str_tuple";

            struct state {
                nb::str separator;
            };

            static state make_state(Node& node) {
                const nb::dict& scalars = node.scalars();
                return {
                    nb::str(nb::cast<nb::object>(scalars["separator"])),
                };
            }

            static void eval(Node& node, state& state) {
                auto bundle = str_ops_detail::require_input_bundle(node);
                const nb::object strings = str_ops_detail::python_field(bundle, "strings");
                nb::object out = nb::steal<nb::object>(PyUnicode_Join(state.separator.ptr(), strings.ptr()));
                if (!out.is_valid()) {
                    nb::raise_python_error();
                }
                str_ops_detail::emit_string(node, out);
            }
        };

        struct FormatSpec {
            static constexpr const char* py_factory_name = "op_format_";

            struct state {
                int64_t sample{-1};
                int64_t count{0};
            };

            static state make_state(Node& node) {
                const nb::dict& scalars = node.scalars();
                if (!scalars.contains("__sample__")) {
                    return {};
                }
                return {nb::cast<int64_t>(scalars["__sample__"]), 0};
            }

            static void eval(Node& node, state& state) {
                if (state.sample > 1) {
                    ++state.count;
                    if (state.count % state.sample != 0) {
                        return;
                    }
                }

                auto bundle = str_ops_detail::require_input_bundle(node);
                const nb::str fmt = str_ops_detail::require_string_field(bundle, "fmt");

                std::vector<nb::object> pos_values;
                if (bundle.contains("__pos_args__")) {
                    auto pos_field = bundle.field("__pos_args__");
                    if (auto pos_bundle = pos_field.try_as_bundle()) {
                        pos_values.reserve(pos_bundle->count());
                        for (size_t i : pos_bundle->indices()) {
                            pos_values.emplace_back(pos_bundle->at(i).to_python());
                        }
                    }
                }

                nb::tuple pos_args = nb::steal<nb::tuple>(PyTuple_New(static_cast<Py_ssize_t>(pos_values.size())));
                if (!pos_args.is_valid()) {
                    nb::raise_python_error();
                }
                for (size_t i = 0; i < pos_values.size(); ++i) {
                    PyTuple_SET_ITEM(pos_args.ptr(), static_cast<Py_ssize_t>(i), pos_values[i].release().ptr());
                }

                nb::dict kw_args;
                if (bundle.contains("__kw_args__")) {
                    auto kw_field = bundle.field("__kw_args__");
                    if (auto kw_bundle = kw_field.try_as_bundle()) {
                        for (size_t i : kw_bundle->indices()) {
                            const std::string_view name = kw_bundle->name_at(i);
                            if (name.empty()) {
                                continue;
                            }
                            kw_args[nb::str(name.data(), name.size())] = kw_bundle->at(i).to_python();
                        }
                    }
                }

                const nb::object format_fn = nb::cast<nb::object>(fmt.attr("format"));
                nb::object out = nb::steal<nb::object>(PyObject_Call(format_fn.ptr(), pos_args.ptr(), kw_args.ptr()));
                if (!out.is_valid()) {
                    nb::raise_python_error();
                }
                str_ops_detail::emit_string(node, out);
            }
        };
    }  // namespace ops
}  // namespace hgraph
