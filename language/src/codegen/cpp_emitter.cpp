#include "codegen/cpp_emitter.h"

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <limits>
#include <map>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

// The C++ backend of the first pass. Its composition subset mirrors the
// direct-wiring backend construct by construct so that programs both accept
// build the same graph. It additionally lowers the first scalar runtime
// subset to public static-node selectors; the native compiler and hgraph
// registry check the emitted implementation when its package is built.
namespace hgl::codegen
{
    namespace
    {
        using semantics::BindingKind;
        using syntax::Category;
        using syntax::SourceRange;

        struct Abort
        {
        };

        // ------------------------------------------------------------ types

        /// An HGL type as the emitter reasons about it (the AST shape after
        /// constant size expressions are printed).
        struct HType
        {
            enum class Kind : std::uint8_t
            {
                Unknown,  ///< a port whose schema the registry decides
                Scalar,
                Tuple,
                List,
                Set,
                Map,
                Rolling,
                Atomic,
            };

            Kind               kind{Kind::Unknown};
            ast::ScalarType    scalar{ast::ScalarType::Bool};
            std::vector<HType> children{};
            std::string        size{};      ///< list fixed size / rolling max, as C++ text
            std::string        min_size{};  ///< rolling minimum, as C++ text
            bool               duration_window{false};

            [[nodiscard]] bool is(ast::ScalarType s) const noexcept { return kind == Kind::Scalar && scalar == s; }
            [[nodiscard]] bool numeric() const noexcept { return is(ast::ScalarType::I64) || is(ast::ScalarType::F64); }
        };

        HType scalar_type(ast::ScalarType scalar)
        {
            HType type;
            type.kind   = HType::Kind::Scalar;
            type.scalar = scalar;
            return type;
        }

        bool same_type(const HType &a, const HType &b)
        {
            if (a.kind != b.kind || a.children.size() != b.children.size()) { return false; }
            if (a.kind == HType::Kind::Scalar && a.scalar != b.scalar) { return false; }
            if (a.size != b.size || a.min_size != b.min_size || a.duration_window != b.duration_window) { return false; }
            for (std::size_t i = 0; i < a.children.size(); ++i)
            {
                if (!same_type(a.children[i], b.children[i])) { return false; }
            }
            return true;
        }

        // ----------------------------------------------------------- values

        /// An emit-time value: a C++ expression plus what the direct backend
        /// would know about it (developer guide, "First pass": constant,
        /// port, function).
        struct Value
        {
            enum class Kind : std::uint8_t
            {
                Void,
                Const,
                Port,
                Runtime,  ///< an evaluation-time scalar, optionally backed by a selector
                Function,
                Operator,       ///< an imported kernel operator: `name` is the C++ marker
                LocalOperator,  ///< a module `operator`: `name` is the C++ marker
                Intrinsic,      ///< `name` is the intrinsic
            };

            Kind        kind{Kind::Void};
            std::string code{};
            /// Runtime values backed by an endpoint keep its selector spelling
            /// so metadata intrinsics and assignments do not read the payload.
            std::string selector{};
            /// Const: the value type. Port: the temporal type, Unknown when the
            /// registry decides it (an operator result).
            HType       type{};
            ast::DeclId decl{ast::no_node};
            std::string name{};
            SourceRange range{};
            /// Known numeric value of a constant expression. Const parameters
            /// deliberately leave this empty: they are values at composition
            /// time, not compile-time literals. The emitter uses this only for
            /// diagnostics that native C++ would otherwise defer or lose
            /// (rolling sizes and zero divisors).
            std::variant<std::monostate, std::int64_t, double> number{};

            [[nodiscard]] bool is_const() const noexcept { return kind == Kind::Const; }
            [[nodiscard]] bool is_port() const noexcept { return kind == Kind::Port; }
            [[nodiscard]] bool is_runtime() const noexcept { return kind == Kind::Runtime; }
        };

        struct Frame
        {
            ast::DeclId                           fn{ast::no_node};
            std::vector<Value>                    params{};
            std::unordered_map<ast::StmtId, Value> locals{};
            std::unordered_map<std::string, Value> injects{};
            bool                                   runtime{false};
            bool                                   runtime_inputs_available{true};
            bool                                   output_available{false};
        };

        struct RuntimeState
        {
            ast::StmtId id{ast::no_node};
            std::string name{};
            HType       type{};
            ast::ExprId init{ast::no_node};
            SourceRange range{};
        };

        struct RuntimeInfo
        {
            std::vector<RuntimeState>       states{};
            std::vector<ast::StmtId>        start_blocks{};
            std::vector<ast::StmtId>        stop_blocks{};
            std::unordered_set<std::size_t> active_parameters{};
            bool                            inject_out{false};
            bool                            has_when{false};
        };

        std::optional<double>       numeric_value(const Value &value);
        std::optional<std::int64_t> integer_value(const Value &value);

        // --------------------------------------------------------- printing

        constexpr std::string_view cpp_keywords[] = {
            "alignas", "alignof", "and", "and_eq", "asm", "auto", "bitand", "bitor", "bool", "break", "case",
            "catch", "char", "char8_t", "char16_t", "char32_t", "class", "compl", "concept", "const", "consteval",
            "constexpr", "constinit", "const_cast", "continue", "co_await", "co_return", "co_yield", "decltype",
            "default", "delete", "do", "double", "dynamic_cast", "else", "enum", "explicit", "export", "extern",
            "false", "float", "for", "friend", "goto", "if", "inline", "int", "long", "mutable", "namespace", "new",
            "noexcept", "not", "not_eq", "nullptr", "operator", "or", "or_eq", "private", "protected", "public",
            "register", "reinterpret_cast", "requires", "return", "short", "signed", "sizeof", "static",
            "static_assert", "static_cast", "struct", "switch", "template", "this", "thread_local", "throw", "true",
            "try", "typedef", "typeid", "typename", "union", "unsigned", "using", "virtual", "void", "volatile",
            "wchar_t", "while", "xor", "xor_eq",
            // names the generated code uses itself
            "w", "hgraph", "std", "ops", "register_operators", "compose", "name", "defaults", "recordable_state",
            "hgl_state", "hgl_output",
        };

        constexpr std::string_view python_keywords[] = {
            "False", "None", "True", "and", "as", "assert", "async", "await", "break", "class", "continue",
            "def", "del", "elif", "else", "except", "finally", "for", "from", "global", "if", "import", "in",
            "is", "lambda", "nonlocal", "not", "or", "pass", "raise", "return", "try", "while", "with", "yield",
        };

        /// An HGL identifier as a C++ identifier: a keyword or a name the
        /// generated code reserves gets a trailing underscore.
        std::string cpp_name(std::string_view name)
        {
            for (const std::string_view keyword : cpp_keywords)
            {
                if (keyword == name) { return std::string{name} + "_"; }
            }
            return std::string{name};
        }

        bool is_python_keyword(std::string_view name)
        {
            return std::find(std::begin(python_keywords), std::end(python_keywords), name) != std::end(python_keywords);
        }

        bool is_python_identifier(std::string_view name)
        {
            if (name.empty() || !((name.front() >= 'a' && name.front() <= 'z') ||
                                  (name.front() >= 'A' && name.front() <= 'Z') || name.front() == '_'))
            {
                return false;
            }
            return std::all_of(name.begin() + 1, name.end(), [](char c) {
                return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_';
            });
        }

        /// The normal Python spelling of an HGL export. Keywords and the
        /// wrapper's public metadata name get a trailing underscore; a
        /// collision after this mapping is diagnosed when the module emits.
        std::string python_name(std::string_view name)
        {
            return is_python_keyword(name) || name == "__all__" ? std::string{name} + "_" : std::string{name};
        }

        std::string quote(std::string_view text)
        {
            std::string out = "\"";
            for (const char c : text)
            {
                switch (c)
                {
                    case '\\': out += "\\\\"; break;
                    case '"': out += "\\\""; break;
                    case '\n': out += "\\n"; break;
                    case '\r': out += "\\r"; break;
                    case '\t': out += "\\t"; break;
                    default:
                        if (static_cast<unsigned char>(c) < 0x20)
                        {
                            char buffer[8];
                            std::snprintf(buffer, sizeof buffer, "\\x%02x", static_cast<unsigned>(static_cast<unsigned char>(c)));
                            out += buffer;
                        }
                        else { out += c; }
                }
            }
            return out + "\"";
        }

        std::string float_literal(double value)
        {
            char buffer[64];
            std::snprintf(buffer, sizeof buffer, "%.17g", value);
            std::string text{buffer};
            if (text.find_first_of(".eEn") == std::string::npos) { text += ".0"; }
            return text;
        }

        std::string join(const std::vector<std::string> &parts, std::string_view separator)
        {
            std::string out;
            for (std::size_t i = 0; i < parts.size(); ++i)
            {
                if (i != 0) { out += separator; }
                out += parts[i];
            }
            return out;
        }

        /// Indented line-oriented output.
        class Writer
        {
          public:
            void line(std::string_view text = {})
            {
                if (!text.empty()) { out_.append(static_cast<std::size_t>(indent_) * 4, ' '); }
                out_ += text;
                out_ += '\n';
            }
            void open(std::string_view text)
            {
                if (!text.empty()) { line(text); }
                line("{");
                ++indent_;
            }
            void close(std::string_view suffix = {})
            {
                --indent_;
                line("}" + std::string{suffix});
            }
            void indent() { ++indent_; }
            void dedent() { --indent_; }
            [[nodiscard]] std::string str() const { return out_; }

          private:
            std::string out_;
            int         indent_{0};
        };

        // ---------------------------------------------------------- emitter

        class Emitter
        {
          public:
            Emitter(const syntax::SourceFile &file, const ast::Module &module, const semantics::ResolvedModule &resolved,
                    const EmitOptions &options, syntax::DiagnosticSink &diagnostics)
                : file_{file}, module_{module}, resolved_{resolved}, options_{options}, diagnostics_{diagnostics}
            {
            }

            [[nodiscard]] EmittedModule emit();

          private:
            // -- diagnostics
            [[noreturn]] void fail(Category category, SourceRange range, std::string message)
            {
                diagnostics_.report(category, range, std::move(message));
                throw Abort{};
            }
            [[noreturn]] void backend(SourceRange range, std::string message)
            {
                fail(Category::Backend, range, std::move(message));
            }
            [[noreturn]] void unsupported(SourceRange range, std::string what)
            {
                backend(range, std::move(what) + " is not supported by emit-cpp yet");
            }

            [[nodiscard]] const ast::FunctionDecl &function(ast::DeclId decl) const
            {
                return std::get<ast::FunctionDecl>(module_.decl(decl).node);
            }
            [[nodiscard]] std::string slice(SourceRange range) const { return std::string{file_.slice(range)}; }
            [[nodiscard]] std::string where(SourceRange range) const
            {
                const syntax::Location at = file_.location(range.begin);
                return basename_ + ":" + std::to_string(at.line);
            }

            // -- types
            [[nodiscard]] HType       type_of(ast::TypeId id, Frame &frame);
            [[nodiscard]] std::string value_type(const HType &type, SourceRange range);
            [[nodiscard]] std::string schema(const HType &type, SourceRange range);
            [[nodiscard]] std::string size_text(ast::ExprId id, Frame &frame, std::string_view what);

            // -- expressions
            [[nodiscard]] Value eval_expr(ast::ExprId id, Frame &frame);
            [[nodiscard]] Value eval_name(ast::ExprId id, Frame &frame);
            [[nodiscard]] Value eval_call(const ast::Call &call, SourceRange range, Frame &frame);
            [[nodiscard]] Value eval_intrinsic(const Value &callee, const ast::Call &call, SourceRange range, Frame &frame);
            [[nodiscard]] Value call_function(ast::DeclId decl, const std::vector<ast::Argument> &arguments, SourceRange range,
                                              Frame &frame);
            [[nodiscard]] Value fold_unary(ast::UnaryOp op, const Value &operand, SourceRange range);
            [[nodiscard]] Value fold_binary(ast::BinaryOp op, const Value &lhs, const Value &rhs, SourceRange range);
            [[nodiscard]] Value wire_binary(ast::BinaryOp op, const Value &lhs, const Value &rhs, SourceRange range);
            [[nodiscard]] Value wire(std::string marker, const std::vector<std::string> &args, SourceRange range,
                                     const HType &result = HType{});
            [[nodiscard]] std::string argument_code(const Value &value);
            [[nodiscard]] std::string as_port(const Value &value, const HType &temporal, SourceRange range);
            [[nodiscard]] std::string as_const(const Value &value, const HType &target, SourceRange range, const std::string &what);
            [[nodiscard]] std::vector<ast::ExprId> bind_arguments(const ast::FunctionDecl &fn,
                                                                  const std::vector<ast::Argument> &arguments, SourceRange range);

            // -- statements
            void emit_block(ast::BlockId id, Frame &frame, Writer &out, bool function_body);
            void emit_stmt(ast::StmtId id, Frame &frame, Writer &out);
            void emit_return(const Value &value, Frame &frame, Writer &out, SourceRange range);
            void emit_runtime_stmt(ast::StmtId id, Frame &frame, Writer &out);
            void emit_runtime_block(ast::BlockId id, Frame &frame, Writer &out);
            void emit_runtime_if(const ast::If &branch, Frame &frame, Writer &out);
            [[nodiscard]] std::string as_runtime(const Value &value, const HType &target, SourceRange range, const std::string &what);

            // -- declarations
            void check_supported(ast::DeclId decl);
            [[nodiscard]] std::string signature(ast::DeclId decl, Frame &frame, bool with_names);
            [[nodiscard]] std::string result_type(ast::DeclId decl, Frame &frame);
            [[nodiscard]] std::string marker(ast::DeclId decl, std::string_view registry_name, Frame &frame);
            /// How a function is written: its struct declaration (header), the
            /// whole struct inline (module-internal helpers and operator
            /// implementations), or the out-of-line `compose` definition of a
            /// struct the header declared (exports).
            enum class Form : std::uint8_t
            {
                Declaration,
                InlineStruct,
                OutOfLine,
            };
            void emit_function(ast::DeclId decl, Writer &out, Form form);
            void emit_runtime_function(ast::DeclId decl, Writer &out);
            void emit_if(const ast::If &branch, Frame &frame, Writer &out);
            [[nodiscard]] RuntimeInfo runtime_info(ast::DeclId decl);
            void collect_runtime_activation(ast::ExprId id, ast::DeclId decl, RuntimeInfo &info);
            using RuntimeValidSet = std::unordered_set<std::size_t>;
            void check_runtime_expr(ast::ExprId id, ast::DeclId decl, const RuntimeValidSet &valid);
            [[nodiscard]] RuntimeValidSet runtime_true_valid(ast::ExprId id, ast::DeclId decl,
                                                             const RuntimeValidSet &valid);
            void check_runtime_block(ast::BlockId id, ast::DeclId decl, const RuntimeValidSet &valid,
                                     bool allow_when = false);
            void check_runtime_stmt(ast::StmtId id, ast::DeclId decl, const RuntimeValidSet &valid, bool allow_when);
            [[nodiscard]] std::string runtime_signature(ast::DeclId decl, const RuntimeInfo &info, Frame &frame, bool with_names,
                                                        bool include_inputs, bool include_output);
            void prepare_runtime_frame(ast::DeclId decl, const RuntimeInfo &info, Frame &frame, Writer &out, bool include_inputs,
                                       bool include_output);
            void emit_runtime_defaults(ast::DeclId decl, Writer &out);
            [[nodiscard]] std::vector<ast::DeclId> ordered_internal_functions();
            void collect_calls(ast::ExprId id, std::set<ast::DeclId> &calls);
            void collect_calls_block(ast::BlockId id, std::set<ast::DeclId> &calls);

            const syntax::SourceFile        &file_;
            const ast::Module               &module_;
            const semantics::ResolvedModule &resolved_;
            const EmitOptions               &options_;
            syntax::DiagnosticSink          &diagnostics_;
            std::string                      basename_{};
            std::string                      namespace_{};
            std::string                      module_name_{};
            bool                             uses_analytics_{false};
            /// Locals declared in the current function, for unique C++ names.
            std::unordered_map<std::string, int> local_counts_{};
            std::unordered_set<std::string>      local_names_{};
        };

        // ------------------------------------------------------------ types

        std::string Emitter::size_text(ast::ExprId id, Frame &frame, std::string_view what)
        {
            const Value value = eval_expr(id, frame);
            const auto  size  = integer_value(value);
            if (!value.is_const() || !value.type.is(ast::ScalarType::I64) || !size || *size < 0)
            {
                fail(Category::Type, module_.expr(id).range, std::string{what} + " must be a non-negative i64 constant");
            }
            return "static_cast<std::size_t>(" + value.code + ")";
        }

        HType Emitter::type_of(ast::TypeId id, Frame &frame)
        {
            const ast::Type &type = module_.type(id);
            switch (type.kind)
            {
                case ast::TypeKind::Scalar: return scalar_type(type.scalar);
                case ast::TypeKind::Named: {
                    const semantics::Binding &binding = resolved_.type_binding(id);
                    if (binding.kind == BindingKind::Generic) { unsupported(type.range, "a generic type"); }
                    if (binding.kind == BindingKind::Struct) { unsupported(type.range, "a struct type"); }
                    backend(type.range, "unresolved named type '" + std::string{type.name.text} + "'");
                }
                case ast::TypeKind::Tuple: {
                    HType result;
                    result.kind = HType::Kind::Tuple;
                    for (const ast::TypeId child : type.children) { result.children.push_back(type_of(child, frame)); }
                    return result;
                }
                case ast::TypeKind::List: {
                    HType result;
                    result.kind = HType::Kind::List;
                    result.children.push_back(type_of(type.children[0], frame));
                    if (type.size != ast::no_node) { result.size = size_text(type.size, frame, "a list size"); }
                    return result;
                }
                case ast::TypeKind::Set: {
                    HType result;
                    result.kind = HType::Kind::Set;
                    result.children.push_back(type_of(type.children[0], frame));
                    return result;
                }
                case ast::TypeKind::Map: {
                    HType result;
                    result.kind = HType::Kind::Map;
                    result.children.push_back(type_of(type.children[0], frame));
                    result.children.push_back(type_of(type.children[1], frame));
                    return result;
                }
                case ast::TypeKind::Rolling: {
                    HType result;
                    result.kind = HType::Kind::Rolling;
                    result.children.push_back(type_of(type.children[0], frame));
                    const Value size = eval_expr(type.size, frame);
                    if (size.is_const() && size.type.is(ast::ScalarType::Duration))
                    {
                        // The registry has duration windows; hgraph has no
                        // duration-valued compile-time TSW marker yet (parity
                        // matrix, TSW), so generated code cannot spell one.
                        result.duration_window = true;
                        unsupported(type.range, "a duration rolling window (hgraph has no compile-time duration TSW marker)");
                    }
                    if (!size.is_const() || !size.type.is(ast::ScalarType::I64))
                    {
                        fail(Category::Type, module_.expr(type.size).range,
                             "a rolling size is a positive i64 constant or a duration");
                    }
                    const auto max_size = integer_value(size);
                    if (!max_size || *max_size <= 0)
                    {
                        fail(Category::Type, module_.expr(type.size).range,
                             "a rolling size is a positive i64 constant or a duration");
                    }
                    result.size     = "static_cast<std::size_t>(" + size.code + ")";
                    result.min_size = type.min_size == ast::no_node ? result.size
                                                                     : size_text(type.min_size, frame, "a rolling minimum");
                    return result;
                }
                case ast::TypeKind::Atomic: {
                    HType result;
                    result.kind = HType::Kind::Atomic;
                    result.children.push_back(type_of(type.children[0], frame));
                    return result;
                }
            }
            backend(type.range, "unsupported type");
        }

        std::string Emitter::value_type(const HType &type, SourceRange range)
        {
            switch (type.kind)
            {
                case HType::Kind::Scalar:
                    switch (type.scalar)
                    {
                        case ast::ScalarType::Bool: return "hgraph::Bool";
                        case ast::ScalarType::I64: return "hgraph::Int";
                        case ast::ScalarType::F64: return "hgraph::Float";
                        case ast::ScalarType::Str: return "hgraph::Str";
                        case ast::ScalarType::Date: return "hgraph::Date";
                        case ast::ScalarType::Time: return "hgraph::Time";
                        case ast::ScalarType::DateTime: return "hgraph::DateTime";
                        case ast::ScalarType::Duration: return "hgraph::TimeDelta";
                        case ast::ScalarType::CivilDateTime:
                        case ast::ScalarType::ZonedDateTime:
                        case ast::ScalarType::ZonedTime:
                        case ast::ScalarType::TimeZone: break;
                    }
                    backend(range, std::string{"'"} + std::string{ast::scalar_type_name(type.scalar)} +
                                       "' is not supported by the first pass (datetime and duration are)");
                case HType::Kind::Tuple: {
                    std::vector<std::string> elements;
                    for (const HType &child : type.children) { elements.push_back(value_type(child, range)); }
                    return "hgraph::Tuple<" + join(elements, ", ") + ">";
                }
                case HType::Kind::Set: return "hgraph::Set<" + value_type(type.children[0], range) + ">";
                case HType::Kind::Map:
                    return "hgraph::Map<" + value_type(type.children[0], range) + ", " + value_type(type.children[1], range) + ">";
                case HType::Kind::List: unsupported(range, "a list value type");
                case HType::Kind::Rolling: backend(range, "'rolling' has no value type; it is a time-series window");
                case HType::Kind::Atomic: return value_type(type.children[0], range);
                case HType::Kind::Unknown: break;
            }
            backend(range, "this value has no C++ type");
        }

        std::string Emitter::schema(const HType &type, SourceRange range)
        {
            switch (type.kind)
            {
                case HType::Kind::Scalar: return "hgraph::TS<" + value_type(type, range) + ">";
                case HType::Kind::Atomic: return "hgraph::TS<" + value_type(type.children[0], range) + ">";
                case HType::Kind::Tuple:
                    backend(range, "a structural tuple has no first-pass schema; write atomic<tuple<...>> for one value");
                case HType::Kind::List:
                    return "hgraph::TSL<" + schema(type.children[0], range) + (type.size.empty() ? "" : ", " + type.size) + ">";
                case HType::Kind::Set: return "hgraph::TSS<" + value_type(type.children[0], range) + ">";
                case HType::Kind::Map:
                    return "hgraph::TSD<" + value_type(type.children[0], range) + ", " + schema(type.children[1], range) + ">";
                case HType::Kind::Rolling:
                    return "hgraph::TSW<" + value_type(type.children[0], range) + ", " + type.size + ", " + type.min_size + ">";
                case HType::Kind::Unknown: break;
            }
            backend(range, "this value has no time-series schema");
        }

        // ------------------------------------------------------------ values

        Value make_const(std::string code, HType type, SourceRange range,
                         std::variant<std::monostate, std::int64_t, double> number = {})
        {
            Value value;
            value.kind  = Value::Kind::Const;
            value.code  = std::move(code);
            value.type  = std::move(type);
            value.range = range;
            value.number = std::move(number);
            return value;
        }

        std::optional<double> numeric_value(const Value &value)
        {
            if (const auto *integer = std::get_if<std::int64_t>(&value.number)) { return static_cast<double>(*integer); }
            if (const auto *floating = std::get_if<double>(&value.number)) { return *floating; }
            return std::nullopt;
        }

        std::optional<std::int64_t> integer_value(const Value &value)
        {
            if (const auto *integer = std::get_if<std::int64_t>(&value.number)) { return *integer; }
            return std::nullopt;
        }

        std::optional<std::int64_t> checked_add(std::int64_t lhs, std::int64_t rhs)
        {
            constexpr auto min = std::numeric_limits<std::int64_t>::min();
            constexpr auto max = std::numeric_limits<std::int64_t>::max();
            if ((rhs > 0 && lhs > max - rhs) || (rhs < 0 && lhs < min - rhs)) { return std::nullopt; }
            return lhs + rhs;
        }

        std::optional<std::int64_t> checked_sub(std::int64_t lhs, std::int64_t rhs)
        {
            constexpr auto min = std::numeric_limits<std::int64_t>::min();
            constexpr auto max = std::numeric_limits<std::int64_t>::max();
            if ((rhs > 0 && lhs < min + rhs) || (rhs < 0 && lhs > max + rhs)) { return std::nullopt; }
            return lhs - rhs;
        }

        std::optional<std::int64_t> checked_mul(std::int64_t lhs, std::int64_t rhs)
        {
            constexpr auto min = std::numeric_limits<std::int64_t>::min();
            constexpr auto max = std::numeric_limits<std::int64_t>::max();
            if (lhs == 0 || rhs == 0) { return 0; }
            if ((lhs == -1 && rhs == min) || (rhs == -1 && lhs == min)) { return std::nullopt; }
            if (lhs > 0)
            {
                if ((rhs > 0 && lhs > max / rhs) || (rhs < 0 && rhs < min / lhs)) { return std::nullopt; }
            }
            else if ((rhs > 0 && lhs < min / rhs) || (rhs < 0 && lhs < max / rhs)) { return std::nullopt; }
            return lhs * rhs;
        }

        std::variant<std::monostate, std::int64_t, double> folded_number(ast::BinaryOp op, const Value &lhs,
                                                                        const Value &rhs)
        {
            const auto left_int  = integer_value(lhs);
            const auto right_int = integer_value(rhs);
            if (left_int && right_int)
            {
                std::optional<std::int64_t> result;
                switch (op)
                {
                    case ast::BinaryOp::Add: result = checked_add(*left_int, *right_int); break;
                    case ast::BinaryOp::Sub: result = checked_sub(*left_int, *right_int); break;
                    case ast::BinaryOp::Mul: result = checked_mul(*left_int, *right_int); break;
                    case ast::BinaryOp::Rem:
                        if (*right_int != 0 &&
                            !(*left_int == std::numeric_limits<std::int64_t>::min() && *right_int == -1))
                        {
                            result = *left_int % *right_int;
                        }
                        break;
                    case ast::BinaryOp::Div:
                        if (*right_int != 0) { return static_cast<double>(*left_int) / static_cast<double>(*right_int); }
                        return {};
                    default: return {};
                }
                return result ? std::variant<std::monostate, std::int64_t, double>{*result}
                              : std::variant<std::monostate, std::int64_t, double>{};
            }
            const auto left = numeric_value(lhs);
            const auto right = numeric_value(rhs);
            if (!left || !right) { return {}; }
            switch (op)
            {
                case ast::BinaryOp::Add: return *left + *right;
                case ast::BinaryOp::Sub: return *left - *right;
                case ast::BinaryOp::Mul: return *left * *right;
                case ast::BinaryOp::Div:
                    if (*right != 0.0) { return *left / *right; }
                    return {};
                default: return {};
            }
        }

        Value make_port(std::string code, HType type, SourceRange range)
        {
            Value value;
            value.kind  = Value::Kind::Port;
            value.code  = std::move(code);
            value.type  = std::move(type);
            value.range = range;
            return value;
        }

        Value make_runtime(std::string code, HType type, SourceRange range, std::string selector = {})
        {
            Value value;
            value.kind     = Value::Kind::Runtime;
            value.code     = std::move(code);
            value.selector = std::move(selector);
            value.type     = std::move(type);
            value.range    = range;
            return value;
        }

        std::string Emitter::argument_code(const Value &value)
        {
            switch (value.kind)
            {
                case Value::Kind::Const:
                case Value::Kind::Port:
                    return value.code;
                case Value::Kind::Runtime:
                    backend(value.range, "an evaluation-time value cannot be passed while wiring");
                case Value::Kind::Function:
                case Value::Kind::Operator:
                case Value::Kind::LocalOperator:
                case Value::Kind::Intrinsic:
                    backend(value.range, "passing a function to an operator is not supported by the first pass");
                case Value::Kind::Void: break;
            }
            backend(value.range, "this expression produces no value");
        }

        std::string Emitter::as_runtime(const Value &value, const HType &target, SourceRange range, const std::string &what)
        {
            if (!value.is_const() && !value.is_runtime())
            {
                fail(Category::Type, range, what + " needs an evaluation-time scalar value");
            }
            if (same_type(value.type, target))
            {
                return value.code;
            }
            if (value.type.is(ast::ScalarType::I64) && target.is(ast::ScalarType::F64))
            {
                return "static_cast<hgraph::Float>(" + value.code + ")";
            }
            fail(Category::Type, range, what + " expects " + value_type(target, range) + ", got " + value_type(value.type, range));
        }

        /// The value as a constant of `target` (a `const` parameter): the
        /// same conversions the direct backend's `convert` allows.
        std::string Emitter::as_const(const Value &value, const HType &target, SourceRange range, const std::string &what)
        {
            if (!value.is_const()) { fail(Category::Type, range, what + " is const; a constant is required"); }
            if (same_type(value.type, target)) { return value.code; }
            if (value.type.is(ast::ScalarType::I64) && target.is(ast::ScalarType::F64))
            {
                return "static_cast<hgraph::Float>(" + value.code + ")";
            }
            fail(Category::Type, range, what + " expects " + value_type(target, range) + ", got " + value_type(value.type, range));
        }

        /// The value as a port of `temporal`: a constant is wired through
        /// `const` at that schema (exactly `wire_constant`); a port whose
        /// schema the registry decides is narrowed with `.as<>()`, which the
        /// wiring checks.
        std::string Emitter::as_port(const Value &value, const HType &temporal, SourceRange range)
        {
            const std::string s = schema(temporal, range);
            if (value.is_const())
            {
                const HType inner = temporal.kind == HType::Kind::Atomic ? temporal.children[0] : temporal;
                std::string converted = value.code;
                if (inner.kind == HType::Kind::Scalar && !same_type(value.type, inner))
                {
                    converted = as_const(value, inner, range, "this value");
                }
                return "hgraph::wire<hgraph::stdlib::const_, " + s + ">(w, " + converted + ")";
            }
            if (!value.is_port()) { fail(Category::Type, range, "a time-series value is required"); }
            if (value.type.kind != HType::Kind::Unknown && same_type(value.type, temporal)) { return value.code; }
            return value.code + ".as<" + s + ">()";
        }

        Value Emitter::wire(std::string marker, const std::vector<std::string> &args, SourceRange range, const HType &result)
        {
            return make_port("hgraph::wire<" + marker + ">(w" + (args.empty() ? "" : ", " + join(args, ", ")) + ")", result, range);
        }

        // --------------------------------------------------------- constants

        Value Emitter::fold_unary(ast::UnaryOp op, const Value &operand, SourceRange range)
        {
            const bool runtime = operand.is_runtime();
            const auto result  = [&](Value value) {
                if (runtime)
                {
                    value.kind   = Value::Kind::Runtime;
                    value.number = {};
                }
                return value;
            };
            switch (op)
            {
                case ast::UnaryOp::Negate:
                    if (operand.type.numeric() || operand.type.is(ast::ScalarType::Duration))
                    {
                        std::variant<std::monostate, std::int64_t, double> number;
                        if (const auto integer = integer_value(operand);
                            integer && *integer != std::numeric_limits<std::int64_t>::min())
                        {
                            number = -*integer;
                        }
                        else if (const auto *floating = std::get_if<double>(&operand.number)) { number = -*floating; }
                        return result(make_const("(-" + operand.code + ")", operand.type, range, std::move(number)));
                    }
                    fail(Category::Type, range, "unary '-' needs a number, got " + value_type(operand.type, range));
                case ast::UnaryOp::Not:
                    if (operand.type.is(ast::ScalarType::Bool))
                    {
                        return result(make_const("(!" + operand.code + ")", operand.type, range));
                    }
                    fail(Category::Type, range, "'!' needs a bool, got " + value_type(operand.type, range));
            }
            backend(range, "unsupported unary operator");
        }

        Value Emitter::fold_binary(ast::BinaryOp op, const Value &lhs, const Value &rhs, SourceRange range)
        {
            using ast::BinaryOp;
            using ast::ScalarType;
            const bool numeric = lhs.type.numeric() && rhs.type.numeric();
            const bool ints    = lhs.type.is(ScalarType::I64) && rhs.type.is(ScalarType::I64);
            const bool runtime = lhs.is_runtime() || rhs.is_runtime();
            const auto type_error = [&]() -> Value {
                fail(Category::Type, range,
                     std::string{"'"} + std::string{ast::binary_op_spelling(op)} + "' is not defined for " +
                         value_type(lhs.type, range) + " and " + value_type(rhs.type, range));
            };
            const auto binary = [&](std::string_view spelling, HType type) {
                Value value = make_const("(" + lhs.code + " " + std::string{spelling} + " " + rhs.code + ")", std::move(type), range,
                                         runtime ? std::variant<std::monostate, std::int64_t, double>{} : folded_number(op, lhs, rhs));
                if (runtime)
                {
                    value.kind = Value::Kind::Runtime;
                }
                return value;
            };
            const HType float_t = scalar_type(ScalarType::F64);
            const HType bool_t  = scalar_type(ScalarType::Bool);
            switch (op)
            {
                case BinaryOp::Add:
                    if (ints) { return binary("+", lhs.type); }
                    if (numeric) { return binary("+", float_t); }
                    if (lhs.type.is(ScalarType::Str) && rhs.type.is(ScalarType::Str)) { return binary("+", lhs.type); }
                    if (lhs.type.is(ScalarType::Duration) && rhs.type.is(ScalarType::Duration)) { return binary("+", lhs.type); }
                    if (lhs.type.is(ScalarType::DateTime) && rhs.type.is(ScalarType::Duration)) { return binary("+", lhs.type); }
                    return type_error();
                case BinaryOp::Sub:
                    if (ints) { return binary("-", lhs.type); }
                    if (numeric) { return binary("-", float_t); }
                    if (lhs.type.is(ScalarType::Duration) && rhs.type.is(ScalarType::Duration)) { return binary("-", lhs.type); }
                    if (lhs.type.is(ScalarType::DateTime) && rhs.type.is(ScalarType::Duration)) { return binary("-", lhs.type); }
                    if (lhs.type.is(ScalarType::DateTime) && rhs.type.is(ScalarType::DateTime))
                    {
                        return binary("-", scalar_type(ScalarType::Duration));
                    }
                    return type_error();
                case BinaryOp::Mul:
                    if (ints) { return binary("*", lhs.type); }
                    if (numeric) { return binary("*", float_t); }
                    if (lhs.type.is(ScalarType::Duration) && rhs.type.is(ScalarType::I64)) { return binary("*", lhs.type); }
                    return type_error();
                case BinaryOp::Div:
                    // Like hgraph's `div_`: integer division is a float.
                    if (numeric)
                    {
                        if (const auto divisor = numeric_value(rhs); divisor && *divisor == 0.0)
                        {
                            fail(Category::Type, range, "division by zero");
                        }
                        Value value = make_const("(static_cast<hgraph::Float>(" + lhs.code + ") / static_cast<hgraph::Float>(" +
                                                     rhs.code + "))",
                                                 float_t, range,
                                                 runtime ? std::variant<std::monostate, std::int64_t, double>{}
                                                         : folded_number(op, lhs, rhs));
                        if (runtime)
                        {
                            value.kind = Value::Kind::Runtime;
                        }
                        return value;
                    }
                    return type_error();
                case BinaryOp::Rem:
                    if (ints)
                    {
                        if (const auto divisor = integer_value(rhs); divisor && *divisor == 0)
                        {
                            fail(Category::Type, range, "division by zero");
                        }
                        return binary("%", lhs.type);
                    }
                    return type_error();
                case BinaryOp::Equal:
                case BinaryOp::NotEqual:
                    if (numeric || same_type(lhs.type, rhs.type))
                    {
                        return binary(op == BinaryOp::Equal ? "==" : "!=", bool_t);
                    }
                    return type_error();
                case BinaryOp::Less:
                case BinaryOp::LessEqual:
                case BinaryOp::Greater:
                case BinaryOp::GreaterEqual:
                    if (numeric || same_type(lhs.type, rhs.type))
                    {
                        const std::string_view spelling = op == BinaryOp::Less        ? "<"
                                                          : op == BinaryOp::LessEqual ? "<="
                                                          : op == BinaryOp::Greater   ? ">"
                                                                                      : ">=";
                        return binary(spelling, bool_t);
                    }
                    return type_error();
                case BinaryOp::And:
                case BinaryOp::Or:
                    if (lhs.type.is(ScalarType::Bool) && rhs.type.is(ScalarType::Bool))
                    {
                        return binary(op == BinaryOp::And ? "&&" : "||", bool_t);
                    }
                    return type_error();
            }
            backend(range, "unsupported binary operator");
        }

        Value Emitter::wire_binary(ast::BinaryOp op, const Value &lhs, const Value &rhs, SourceRange range)
        {
            using ast::BinaryOp;
            const char *name = nullptr;
            switch (op)
            {
                case BinaryOp::Add: name = "add_"; break;
                case BinaryOp::Sub: name = "sub_"; break;
                case BinaryOp::Mul: name = "mul_"; break;
                case BinaryOp::Div: name = "div_"; break;
                case BinaryOp::Rem: name = "mod_"; break;
                case BinaryOp::Equal: name = "eq_"; break;
                case BinaryOp::NotEqual: name = "ne_"; break;
                case BinaryOp::Less: name = "lt_"; break;
                case BinaryOp::LessEqual: name = "le_"; break;
                case BinaryOp::Greater: name = "gt_"; break;
                case BinaryOp::GreaterEqual: name = "ge_"; break;
                case BinaryOp::And: name = "and_"; break;
                case BinaryOp::Or: name = "or_"; break;
            }
            return wire(std::string{"hgraph::stdlib::"} + name, {argument_code(lhs), argument_code(rhs)}, range);
        }

        // -------------------------------------------------------- expressions

        Value Emitter::eval_name(ast::ExprId id, Frame &frame)
        {
            const semantics::Binding &binding = resolved_.binding(id);
            const SourceRange         range   = module_.expr(id).range;
            switch (binding.kind)
            {
                case BindingKind::Local: {
                    const auto found = frame.locals.find(binding.stmt);
                    if (found == frame.locals.end())
                    {
                        const auto injected = frame.injects.find(slice(range));
                        if (injected == frame.injects.end())
                        {
                            backend(range, "'" + slice(range) + "' is not bound in this function");
                        }
                        Value value = injected->second;
                        value.range = range;
                        return value;
                    }
                    Value value = found->second;
                    value.range = range;
                    return value;
                }
                case BindingKind::Parameter: {
                    if (binding.decl != frame.fn || binding.index >= frame.params.size())
                    {
                        backend(range, "'" + slice(range) + "' is not a parameter of this function");
                    }
                    if (frame.runtime && !frame.runtime_inputs_available &&
                        !function(frame.fn).signature.parameters[binding.index].is_const)
                    {
                        fail(Category::Phase, range,
                             "temporal parameters are not available in runtime lifecycle blocks");
                    }
                    Value value = frame.params[binding.index];
                    value.range = range;
                    return value;
                }
                case BindingKind::Generic: unsupported(range, "a generic parameter");
                case BindingKind::Struct: unsupported(range, "a struct");
                case BindingKind::Function: {
                    Value value;
                    value.kind  = Value::Kind::Function;
                    value.decl  = binding.decl;
                    value.range = range;
                    return value;
                }
                case BindingKind::Operator: {
                    // The kernel table: `hgraph.std::x` is the marker
                    // `hgraph::stdlib::<registry name>`; `hgraph.analytics::x` is
                    // `hgraph::analytics::x` (registry `hgraph.analytics.x`).
                    Value value;
                    value.kind  = Value::Kind::Operator;
                    value.range = range;
                    const std::string &registry = binding.registry_name;
                    if (registry.starts_with("hgraph.analytics."))
                    {
                        uses_analytics_ = true;
                        value.name      = "hgraph::analytics::" + registry.substr(std::string_view{"hgraph.analytics."}.size());
                    }
                    else { value.name = "hgraph::stdlib::" + registry; }
                    return value;
                }
                case BindingKind::LocalOperator: {
                    const auto &op = std::get<ast::OperatorDecl>(module_.decl(binding.decl).node);
                    Value       value;
                    value.kind  = Value::Kind::LocalOperator;
                    value.decl  = binding.decl;
                    value.name  = "ops::" + cpp_name(op.name.text);
                    value.range = range;
                    return value;
                }
                case BindingKind::Intrinsic: {
                    Value value;
                    value.kind  = Value::Kind::Intrinsic;
                    value.name  = binding.registry_name;
                    value.range = range;
                    return value;
                }
                case BindingKind::Test:
                case BindingKind::Unbound: break;
            }
            backend(range, "'" + slice(range) + "' has no value");
        }

        Value Emitter::eval_expr(ast::ExprId id, Frame &frame)
        {
            const ast::Expr &expr = module_.expr(id);
            return std::visit(
                [&](const auto &node) -> Value {
                    using T = std::decay_t<decltype(node)>;
                    if constexpr (std::is_same_v<T, ast::IntLiteral>)
                    {
                        return make_const("hgraph::Int{" + std::to_string(node.value) + "}", scalar_type(ast::ScalarType::I64),
                                          expr.range, static_cast<std::int64_t>(node.value));
                    }
                    else if constexpr (std::is_same_v<T, ast::FloatLiteral>)
                    {
                        return make_const("hgraph::Float{" + float_literal(node.value) + "}", scalar_type(ast::ScalarType::F64),
                                          expr.range, node.value);
                    }
                    else if constexpr (std::is_same_v<T, ast::StringLiteral>)
                    {
                        return make_const("hgraph::Str{" + quote(node.value) + "}", scalar_type(ast::ScalarType::Str), expr.range);
                    }
                    else if constexpr (std::is_same_v<T, ast::BoolLiteral>)
                    {
                        return make_const(node.value ? "true" : "false", scalar_type(ast::ScalarType::Bool), expr.range);
                    }
                    else if constexpr (std::is_same_v<T, ast::NullLiteral>) { unsupported(expr.range, "'null'"); }
                    else if constexpr (std::is_same_v<T, ast::TemporalLiteral>)
                    {
                        using syntax::TemporalKind;
                        const std::string micros = std::to_string(node.value.micros);
                        switch (node.value.kind)
                        {
                            case TemporalKind::Date:
                                return make_const("hgraph::Date{std::chrono::sys_days{std::chrono::days{" + micros + "}}}",
                                                  scalar_type(ast::ScalarType::Date), expr.range);
                            case TemporalKind::Time:
                                return make_const("hgraph::Time{" + micros + "}", scalar_type(ast::ScalarType::Time), expr.range);
                            case TemporalKind::DateTime:
                                return make_const("hgraph::DateTime{std::chrono::microseconds{" + micros + "}}",
                                                  scalar_type(ast::ScalarType::DateTime), expr.range);
                            case TemporalKind::Duration:
                                return make_const("hgraph::TimeDelta{" + micros + "}", scalar_type(ast::ScalarType::Duration),
                                                  expr.range);
                            case TemporalKind::CivilDateTime:
                            case TemporalKind::ZonedDateTime:
                            case TemporalKind::ZonedTime:
                            case TemporalKind::TimeZone: break;
                        }
                        backend(expr.range, "zoned and civil literals are not supported by the first pass");
                    }
                    else if constexpr (std::is_same_v<T, ast::Placeholder>)
                    {
                        fail(Category::Type, expr.range, "'_' is only valid in a harness sequence");
                    }
                    else if constexpr (std::is_same_v<T, ast::NameRef> || std::is_same_v<T, ast::QualifiedRef>)
                    {
                        return eval_name(id, frame);
                    }
                    else if constexpr (std::is_same_v<T, ast::Unary>)
                    {
                        const Value operand = eval_expr(node.operand, frame);
                        if (operand.is_const() || operand.is_runtime())
                        {
                            return fold_unary(node.op, operand, expr.range);
                        }
                        if (!operand.is_port())
                        {
                            backend(expr.range, "this operand has no value");
                        }
                        return wire(node.op == ast::UnaryOp::Negate ? "hgraph::stdlib::neg_" : "hgraph::stdlib::not_",
                                    {operand.code}, expr.range);
                    }
                    else if constexpr (std::is_same_v<T, ast::Binary>)
                    {
                        const Value lhs = eval_expr(node.lhs, frame);
                        const Value rhs = eval_expr(node.rhs, frame);
                        if ((lhs.is_const() || lhs.is_runtime()) && (rhs.is_const() || rhs.is_runtime()))
                        {
                            return fold_binary(node.op, lhs, rhs, expr.range);
                        }
                        return wire_binary(node.op, lhs, rhs, expr.range);
                    }
                    else if constexpr (std::is_same_v<T, ast::Call>) { return eval_call(node, expr.range, frame); }
                    else if constexpr (std::is_same_v<T, ast::Index>)
                    {
                        const Value target = eval_expr(node.target, frame);
                        const Value index  = eval_expr(node.index, frame);
                        if (target.is_port())
                        {
                            return wire("hgraph::stdlib::getitem_", {target.code, argument_code(index)}, expr.range);
                        }
                        unsupported(expr.range, "indexing a constant");
                    }
                    else if constexpr (std::is_same_v<T, ast::Field>)
                    {
                        const Value target = eval_expr(node.target, frame);
                        if (target.is_port())
                        {
                            return wire("hgraph::stdlib::getattr_", {target.code, "hgraph::Str{" + quote(node.field.text) + "}"},
                                        expr.range);
                        }
                        unsupported(expr.range, "field access on a constant");
                    }
                    else if constexpr (std::is_same_v<T, ast::SequenceLiteral>) { unsupported(expr.range, "a list literal"); }
                    else if constexpr (std::is_same_v<T, ast::TupleLiteral>) { unsupported(expr.range, "a tuple literal"); }
                    else if constexpr (std::is_same_v<T, ast::AnonymousFn>)
                    {
                        backend(expr.range, "anonymous functions are not supported by the first pass");
                    }
                    else if constexpr (std::is_same_v<T, ast::If>) { unsupported(expr.range, "'if' used as a value"); }
                    else if constexpr (std::is_same_v<T, ast::BlockExpr>) { unsupported(expr.range, "a block used as a value"); }
                    else if constexpr (std::is_same_v<T, ast::Eval>)
                    {
                        fail(Category::Type, expr.range, "'eval' is only valid in a test");
                    }
                    else if constexpr (std::is_same_v<T, ast::Construct>) { unsupported(expr.range, "struct construction"); }
                    else
                    {
                        static_assert(sizeof(T) == 0, "unhandled expression node");
                    }
                },
                expr.node);
        }

        // ------------------------------------------------------------- calls

        Value Emitter::eval_call(const ast::Call &call, SourceRange range, Frame &frame)
        {
            const Value callee = eval_expr(call.callee, frame);
            if (frame.runtime && callee.kind != Value::Kind::Intrinsic)
            {
                backend(range, "calls in a runtime function are not supported by emit-cpp yet");
            }
            switch (callee.kind)
            {
                case Value::Kind::Operator:
                case Value::Kind::LocalOperator: {
                    std::vector<std::string> args;
                    args.reserve(call.arguments.size());
                    for (const ast::Argument &argument : call.arguments)
                    {
                        std::string code = argument_code(eval_expr(argument.value, frame));
                        if (!argument.name.empty())
                        {
                            code = "hgraph::arg<" + quote(argument.name.text) + ">(" + code + ")";
                        }
                        args.push_back(std::move(code));
                    }
                    return wire(callee.name, args, range);
                }
                case Value::Kind::Function: return call_function(callee.decl, call.arguments, range, frame);
                case Value::Kind::Intrinsic: return eval_intrinsic(callee, call, range, frame);
                case Value::Kind::Const:
                case Value::Kind::Port:
                case Value::Kind::Runtime:
                case Value::Kind::Void:
                    break;
            }
            fail(Category::Type, module_.expr(call.callee).range,
                 "'" + slice(module_.expr(call.callee).range) + "' is not callable");
        }

        Value Emitter::eval_intrinsic(const Value &callee, const ast::Call &call, SourceRange range, Frame &frame)
        {
            const std::string &name = callee.name;
            if (name == "valid" || name == "modified" || name == "all_valid")
            {
                if (call.arguments.empty())
                {
                    fail(Category::Type, range, "'" + name + "' takes at least one time-series argument");
                }
                if (frame.runtime)
                {
                    std::vector<std::string> tests;
                    tests.reserve(call.arguments.size());
                    for (const ast::Argument &argument : call.arguments)
                    {
                        const Value value = eval_expr(argument.value, frame);
                        if (!value.is_runtime() || value.selector.empty())
                        {
                            fail(Category::Type, module_.expr(argument.value).range,
                                 "'" + name + "' takes time-series selectors in a runtime function");
                        }
                        const std::string method =
                            name == "modified" ? "modified()" : name == "all_valid" ? "all_valid()" : "valid()";
                        tests.push_back(value.selector + "." + method);
                    }
                    return make_runtime("(" + join(tests, name == "modified" ? " || " : " && ") + ")", scalar_type(ast::ScalarType::Bool),
                                        range);
                }
                const std::string op   = name == "modified" ? "hgraph::stdlib::modified" : "hgraph::stdlib::valid";
                const std::string fold = name == "modified" ? "hgraph::stdlib::or_" : "hgraph::stdlib::and_";
                std::optional<Value> result;
                for (const ast::Argument &argument : call.arguments)
                {
                    const Value value = eval_expr(argument.value, frame);
                    if (!value.is_port())
                    {
                        fail(Category::Type, module_.expr(argument.value).range, "'" + name + "' takes time-series arguments");
                    }
                    Value flag = wire(op, {value.code}, range);
                    result     = result ? wire(fold, {result->code, flag.code}, range) : flag;
                }
                return *result;
            }
            if (name == "last_modified" || name == "key_set")
            {
                if (call.arguments.size() != 1) { fail(Category::Type, range, "'" + name + "' takes one time-series argument"); }
                const Value value = eval_expr(call.arguments[0].value, frame);
                if (frame.runtime)
                {
                    if (name == "key_set")
                    {
                        backend(range, "runtime collection traversal is not supported by emit-cpp yet");
                    }
                    if (!value.is_runtime() || value.selector.empty())
                    {
                        fail(Category::Type, module_.expr(call.arguments[0].value).range,
                             "'last_modified' takes a time-series selector in a runtime function");
                    }
                    return make_runtime(value.selector + ".last_modified_time()", scalar_type(ast::ScalarType::DateTime), range);
                }
                if (!value.is_port())
                {
                    fail(Category::Type, module_.expr(call.arguments[0].value).range, "'" + name + "' takes a time-series argument");
                }
                return wire(name == "last_modified" ? "hgraph::stdlib::last_modified_time" : "hgraph::stdlib::keys_", {value.code},
                            range);
            }
            backend(range, "'" + name +
                               "' is a runtime traversal; it is not available in a composition body of the first pass");
        }

        std::vector<ast::ExprId> Emitter::bind_arguments(const ast::FunctionDecl &fn, const std::vector<ast::Argument> &arguments,
                                                         SourceRange range)
        {
            const auto              &params = fn.signature.parameters;
            std::vector<ast::ExprId> bound(params.size(), ast::no_node);
            std::size_t              next = 0;
            for (const ast::Argument &argument : arguments)
            {
                const SourceRange at = module_.expr(argument.value).range;
                if (argument.name.empty())
                {
                    if (next >= params.size())
                    {
                        fail(Category::Type, at, "'" + std::string{fn.name.text} + "' takes " + std::to_string(params.size()) + " arguments");
                    }
                    if (bound[next] != ast::no_node) { fail(Category::Type, at, "positional argument after a named one"); }
                    bound[next++] = argument.value;
                    continue;
                }
                const auto found = std::find_if(params.begin(), params.end(),
                                                [&](const ast::Parameter &param) { return param.name.text == argument.name.text; });
                if (found == params.end())
                {
                    fail(Category::Name, argument.name.range,
                         "'" + std::string{fn.name.text} + "' has no parameter named '" + std::string{argument.name.text} + "'");
                }
                const auto index = static_cast<std::size_t>(found - params.begin());
                if (bound[index] != ast::no_node)
                {
                    fail(Category::Name, argument.name.range, "'" + std::string{argument.name.text} + "' is given twice");
                }
                bound[index] = argument.value;
                next         = std::max(next, index + 1);
            }
            for (std::size_t i = 0; i < params.size(); ++i)
            {
                if (bound[i] == ast::no_node && params[i].default_value == ast::no_node)
                {
                    fail(Category::Type, range,
                         "'" + std::string{fn.name.text} + "' needs an argument for '" + std::string{params[i].name.text} + "'");
                }
            }
            return bound;
        }

        /// A call to a module function becomes `wire<name>(w, args...)`: the
        /// callee is a graph struct, so its compose parameters take the
        /// arguments in declaration order, defaults folded in here as the
        /// direct backend does.
        Value Emitter::call_function(ast::DeclId decl, const std::vector<ast::Argument> &arguments, SourceRange range, Frame &frame)
        {
            check_supported(decl);
            const ast::FunctionDecl       &fn    = function(decl);
            const std::vector<ast::ExprId> bound = bind_arguments(fn, arguments, range);
            const auto                    &params = fn.signature.parameters;
            Frame                          callee;
            callee.fn = decl;
            std::vector<std::string> args(params.size());
            for (std::size_t i = 0; i < params.size(); ++i)
            {
                const ast::Parameter &param = params[i];
                Value arg = bound[i] != ast::no_node ? eval_expr(bound[i], frame) : eval_expr(param.default_value, callee);
                const HType type = type_of(param.type, callee);
                if (param.is_const)
                {
                    args[i] = as_const(arg, type, arg.range, "parameter '" + std::string{param.name.text} + "'");
                }
                else { args[i] = as_port(arg, type, arg.range); }
            }
            HType result;
            if (fn.signature.result != ast::no_node) { result = type_of(fn.signature.result, callee); }
            Value value = wire(cpp_name(fn.name.text), args, range, result);
            if (fn.signature.result == ast::no_node) { value.kind = Value::Kind::Void; }
            return value;
        }

        // -------------------------------------------------------- statements

        void Emitter::emit_return(const Value &value, Frame &frame, Writer &out, SourceRange range)
        {
            const ast::FunctionDecl &fn = function(frame.fn);
            if (fn.signature.result == ast::no_node)
            {
                if (value.kind != Value::Kind::Void) { fail(Category::Type, range, "'" + std::string{fn.name.text} + "' has no result"); }
                if (!value.code.empty()) { out.line(value.code + ";"); }
                out.line("return;");
                return;
            }
            const HType result = type_of(fn.signature.result, frame);
            out.line("return " + as_port(value, result, range) + ";");
        }

        void Emitter::emit_stmt(ast::StmtId id, Frame &frame, Writer &out)
        {
            const ast::Stmt &stmt = module_.stmt(id);
            std::visit(
                [&](const auto &node) {
                    using T = std::decay_t<decltype(node)>;
                    if constexpr (std::is_same_v<T, ast::LocalDecl>)
                    {
                        Value value = eval_expr(node.init, frame);
                        if (node.type != ast::no_node)
                        {
                            const HType declared = type_of(node.type, frame);
                            if (value.is_const())
                            {
                                value.code = as_const(value, declared, value.range, "'" + std::string{node.name.text} + "'");
                                value.type = declared;
                            }
                            else if (value.is_port())
                            {
                                value.code = as_port(value, declared, value.range);
                                value.type = declared;
                            }
                        }
                        if (value.kind != Value::Kind::Const && value.kind != Value::Kind::Port)
                        {
                            unsupported(stmt.range, "binding a function or operator to a local");
                        }
                        // A unique C++ local per declaration: HGL lets a
                        // later `let` shadow an earlier one in a block.
                        const std::string base = cpp_name(node.name.text);
                        std::string       local = base;
                        int              &suffix = local_counts_[base];
                        while (local_names_.contains(local)) { local = base + "_" + std::to_string(++suffix); }
                        local_names_.insert(local);
                        out.line((node.mutable_ ? "auto " : "const auto ") + local + " = " + value.code + ";");
                        value.code       = local;
                        frame.locals[id] = std::move(value);
                    }
                    else if constexpr (std::is_same_v<T, ast::AssignStmt>)
                    {
                        const ast::Expr &place = module_.expr(node.place);
                        if (!std::holds_alternative<ast::NameRef>(place.node) ||
                            resolved_.binding(node.place).kind != BindingKind::Local)
                        {
                            backend(place.range, "assignment targets a local in the first pass");
                        }
                        const ast::StmtId target = resolved_.binding(node.place).stmt;
                        if (const auto *local = std::get_if<ast::LocalDecl>(&module_.stmt(target).node);
                            local == nullptr || !local->mutable_)
                        {
                            fail(Category::Type, place.range, "'" + slice(place.range) + "' is not a 'var'");
                        }
                        Value        value   = eval_expr(node.value, frame);
                        const Value &current = frame.locals.at(target);
                        if (node.op != ast::AssignOp::Assign)
                        {
                            const ast::BinaryOp op = node.op == ast::AssignOp::Add   ? ast::BinaryOp::Add
                                                     : node.op == ast::AssignOp::Sub ? ast::BinaryOp::Sub
                                                     : node.op == ast::AssignOp::Mul ? ast::BinaryOp::Mul
                                                                                     : ast::BinaryOp::Div;
                            value = current.is_const() && value.is_const() ? fold_binary(op, current, value, stmt.range)
                                                                           : wire_binary(op, current, value, stmt.range);
                        }
                        if (current.kind != value.kind)
                        {
                            fail(Category::Type, stmt.range,
                                 "assignment to '" + slice(place.range) + "' changes its inferred type");
                        }
                        if (current.is_const())
                        {
                            value.code = as_const(value, current.type, value.range,
                                                  "assignment to '" + slice(place.range) + "'");
                            value.type = current.type;
                        }
                        else if (current.is_port())
                        {
                            // `auto` fixes the C++ port type at the declaration.
                            // Retain that static HGL type after every rebind and
                            // narrow an erased operator result at this boundary.
                            if (current.type.kind != HType::Kind::Unknown)
                            {
                                value.code = as_port(value, current.type, value.range);
                            }
                            value.type = current.type;
                        }
                        out.line(current.code + " = " + value.code + ";");
                        Value updated       = value;
                        updated.code        = current.code;
                        frame.locals[target] = std::move(updated);
                    }
                    else if constexpr (std::is_same_v<T, ast::ReturnStmt>)
                    {
                        Value value;
                        if (node.value != ast::no_node) { value = eval_expr(node.value, frame); }
                        emit_return(value, frame, out, stmt.range);
                    }
                    else if constexpr (std::is_same_v<T, ast::AssertStmt>)
                    {
                        fail(Category::Type, stmt.range, "'assert' is only valid in a test");
                    }
                    else if constexpr (std::is_same_v<T, ast::ExprStmt>)
                    {
                        const ast::Expr &expr = module_.expr(node.expr);
                        if (const auto *branch = std::get_if<ast::If>(&expr.node))
                        {
                            emit_if(*branch, frame, out);
                            return;
                        }
                        const Value value = eval_expr(node.expr, frame);
                        if (value.kind == Value::Kind::Void) { out.line(value.code + ";"); }
                        else { out.line("(void)" + value.code + ";"); }
                    }
                    else
                    {
                        backend(stmt.range, "runtime statements are not evaluated by the first pass");
                    }
                },
                stmt.node);
        }

        /// A wiring-time `if`: its condition is a constant, so it is a C++
        /// `if`; an `else if` chain recurses.
        void Emitter::emit_if(const ast::If &branch, Frame &frame, Writer &out)
        {
            const Value condition = eval_expr(branch.condition, frame);
            if (condition.is_port())
            {
                backend(module_.expr(branch.condition).range,
                        "'if' over a time-series condition is not supported by the first pass; use if_then_else");
            }
            if (!condition.is_const() || !condition.type.is(ast::ScalarType::Bool))
            {
                fail(Category::Type, module_.expr(branch.condition).range, "an 'if' condition is a bool");
            }
            out.open("if (" + condition.code + ")");
            emit_block(branch.then_block, frame, out, false);
            out.close();
            if (branch.otherwise == ast::no_node) { return; }
            const ast::Expr &otherwise = module_.expr(branch.otherwise);
            out.open("else");
            if (const auto *block = std::get_if<ast::BlockExpr>(&otherwise.node)) { emit_block(block->block, frame, out, false); }
            else if (const auto *chained = std::get_if<ast::If>(&otherwise.node)) { emit_if(*chained, frame, out); }
            else { unsupported(otherwise.range, "this 'else' form"); }
            out.close();
        }

        void Emitter::emit_block(ast::BlockId id, Frame &frame, Writer &out, bool function_body)
        {
            const ast::Block &block = module_.block(id);
            for (std::size_t i = 0; i < block.statements.size(); ++i)
            {
                const bool is_tail = block.tail != ast::no_node && i + 1 == block.statements.size();
                if (is_tail && function_body)
                {
                    const Value value = eval_expr(block.tail, frame);
                    emit_return(value, frame, out, module_.expr(block.tail).range);
                    return;
                }
                emit_stmt(block.statements[i], frame, out);
            }
            if (function_body && function(frame.fn).signature.result != ast::no_node && block.tail == ast::no_node)
            {
                // Every path must return: a body that ends after a `return`
                // inside `if` still needs a terminating statement for C++.
                out.line("throw std::logic_error(\"" + std::string{function(frame.fn).name.text} +
                         ": reached the end of the body without a result\");");
            }
        }

        void Emitter::emit_runtime_if(const ast::If &branch, Frame &frame, Writer &out)
        {
            const Value condition = eval_expr(branch.condition, frame);
            if ((!condition.is_const() && !condition.is_runtime()) || !condition.type.is(ast::ScalarType::Bool))
            {
                fail(Category::Type, module_.expr(branch.condition).range, "an 'if' condition is a bool scalar");
            }
            out.open("if (" + condition.code + ")");
            emit_runtime_block(branch.then_block, frame, out);
            out.close();
            if (branch.otherwise == ast::no_node)
            {
                return;
            }
            const ast::Expr &otherwise = module_.expr(branch.otherwise);
            if (const auto *block = std::get_if<ast::BlockExpr>(&otherwise.node))
            {
                out.open("else");
                emit_runtime_block(block->block, frame, out);
                out.close();
            }
            else if (const auto *chained = std::get_if<ast::If>(&otherwise.node))
            {
                out.open("else");
                emit_runtime_if(*chained, frame, out);
                out.close();
            }
            else
            {
                unsupported(otherwise.range, "this runtime 'else' form");
            }
        }

        void Emitter::emit_runtime_stmt(ast::StmtId id, Frame &frame, Writer &out)
        {
            const ast::Stmt &stmt = module_.stmt(id);
            std::visit(
                [&](const auto &node) {
                    using T = std::decay_t<decltype(node)>;
                    if constexpr (std::is_same_v<T, ast::LocalDecl>)
                    {
                        Value value = eval_expr(node.init, frame);
                        if (!value.is_const() && !value.is_runtime())
                        {
                            fail(Category::Type, stmt.range, "a runtime local needs a scalar value");
                        }
                        if (node.type != ast::no_node)
                        {
                            const HType declared = type_of(node.type, frame);
                            value.code           = as_runtime(value, declared, value.range, "'" + std::string{node.name.text} + "'");
                            value.type           = declared;
                        }
                        const std::string base   = cpp_name(node.name.text);
                        std::string       local  = base;
                        int              &suffix = local_counts_[base];
                        while (local_names_.contains(local))
                        {
                            local = base + "_" + std::to_string(++suffix);
                        }
                        local_names_.insert(local);
                        out.line((node.mutable_ ? "auto " : "const auto ") + local + " = " + value.code + ";");
                        value.code = local;
                        value.selector.clear();
                        value.kind       = Value::Kind::Runtime;
                        value.number     = {};
                        frame.locals[id] = std::move(value);
                    }
                    else if constexpr (std::is_same_v<T, ast::AssignStmt>)
                    {
                        const ast::Expr &place = module_.expr(node.place);
                        if (!std::holds_alternative<ast::NameRef>(place.node) || resolved_.binding(node.place).kind != BindingKind::Local)
                        {
                            backend(place.range, "runtime assignment currently targets a "
                                                 "local, state value, or 'out'");
                        }
                        const semantics::Binding &binding = resolved_.binding(node.place);
                        const ast::StmtId         target  = binding.stmt;
                        const auto                local   = frame.locals.find(target);
                        Value                     current;
                        bool                      selector_assignment = false;
                        if (local != frame.locals.end())
                        {
                            current = local->second;
                            if (const auto *decl = std::get_if<ast::LocalDecl>(&module_.stmt(target).node);
                                decl != nullptr && !decl->mutable_)
                            {
                                fail(Category::Type, place.range, "'" + slice(place.range) + "' is not a 'var'");
                            }
                            selector_assignment = std::holds_alternative<ast::StateDecl>(module_.stmt(target).node);
                        }
                        else
                        {
                            const auto injected = frame.injects.find(slice(place.range));
                            if (injected == frame.injects.end())
                            {
                                backend(place.range, "'" + slice(place.range) + "' is not writable in this hook");
                            }
                            current             = injected->second;
                            selector_assignment = true;
                        }
                        Value value = eval_expr(node.value, frame);
                        if (node.op != ast::AssignOp::Assign)
                        {
                            const ast::BinaryOp op = node.op == ast::AssignOp::Add   ? ast::BinaryOp::Add
                                                     : node.op == ast::AssignOp::Sub ? ast::BinaryOp::Sub
                                                     : node.op == ast::AssignOp::Mul ? ast::BinaryOp::Mul
                                                                                     : ast::BinaryOp::Div;
                            value                  = fold_binary(op, current, value, stmt.range);
                        }
                        const std::string converted =
                            as_runtime(value, current.type, value.range, "assignment to '" + slice(place.range) + "'");
                        if (selector_assignment)
                        {
                            if (current.selector.empty())
                            {
                                backend(place.range, "this runtime value is not writable");
                            }
                            out.line(current.selector + ".set(" + converted + ");");
                        }
                        else
                        {
                            out.line(current.code + " = " + converted + ";");
                            Value updated        = current;
                            updated.kind         = Value::Kind::Runtime;
                            updated.number       = {};
                            frame.locals[target] = std::move(updated);
                        }
                    }
                    else if constexpr (std::is_same_v<T, ast::ReturnStmt>)
                    {
                        if (!frame.output_available)
                        {
                            fail(Category::Phase, stmt.range, "'return' is not available in a lifecycle block");
                        }
                        if (node.value != ast::no_node)
                        {
                            const ast::FunctionDecl &fn = function(frame.fn);
                            if (fn.signature.result == ast::no_node)
                            {
                                fail(Category::Type, stmt.range, "'" + std::string{fn.name.text} + "' has no result");
                            }
                            const HType result = type_of(fn.signature.result, frame);
                            const Value value  = eval_expr(node.value, frame);
                            out.line("hgl_output.set(" + as_runtime(value, result, value.range, "return value") + ");");
                        }
                        out.line("return;");
                    }
                    else if constexpr (std::is_same_v<T, ast::WhenStmt>)
                    {
                        const Value condition = eval_expr(node.condition, frame);
                        if ((!condition.is_const() && !condition.is_runtime()) || !condition.type.is(ast::ScalarType::Bool))
                        {
                            fail(Category::Type, module_.expr(node.condition).range, "a 'when' condition is a bool scalar");
                        }
                        out.open("if (" + condition.code + ")");
                        emit_runtime_block(node.block, frame, out);
                        out.close();
                    }
                    else if constexpr (std::is_same_v<T, ast::ExprStmt>)
                    {
                        const ast::Expr &expr = module_.expr(node.expr);
                        if (const auto *branch = std::get_if<ast::If>(&expr.node))
                        {
                            emit_runtime_if(*branch, frame, out);
                            return;
                        }
                        const Value value = eval_expr(node.expr, frame);
                        if (value.kind == Value::Kind::Void)
                        {
                            out.line(value.code + ";");
                        }
                        else
                        {
                            out.line("(void)" + value.code + ";");
                        }
                    }
                    else if constexpr (std::is_same_v<T, ast::AssertStmt>)
                    {
                        fail(Category::Type, stmt.range, "'assert' is only valid in a test");
                    }
                    else if constexpr (std::is_same_v<T, ast::ForStmt>)
                    {
                        backend(stmt.range, "runtime collection traversal is not supported by emit-cpp yet");
                    }
                    else
                    {
                        backend(stmt.range, "state, inject, start, and stop are "
                                            "function-level runtime declarations");
                    }
                },
                stmt.node);
        }

        void Emitter::emit_runtime_block(ast::BlockId id, Frame &frame, Writer &out)
        {
            for (const ast::StmtId stmt : module_.block(id).statements)
            {
                emit_runtime_stmt(stmt, frame, out);
            }
        }

        // ------------------------------------------------------ declarations

        void Emitter::collect_runtime_activation(ast::ExprId id, ast::DeclId decl, RuntimeInfo &info)
        {
            const ast::Expr &expr = module_.expr(id);
            std::visit(
                [&](const auto &node) {
                    using T = std::decay_t<decltype(node)>;
                    if constexpr (std::is_same_v<T, ast::Call>)
                    {
                        const semantics::Binding &callee = resolved_.binding(node.callee);
                        if (callee.kind == BindingKind::Intrinsic && callee.registry_name == "modified")
                        {
                            for (const ast::Argument &argument : node.arguments)
                            {
                                const semantics::Binding &binding = resolved_.binding(argument.value);
                                if (binding.kind != BindingKind::Parameter || binding.decl != decl ||
                                    binding.index >= function(decl).signature.parameters.size() ||
                                    function(decl).signature.parameters[binding.index].is_const)
                                {
                                    backend(module_.expr(argument.value).range, "the first runtime-node slice requires 'modified' "
                                                                                "arguments to be temporal parameters");
                                }
                                info.active_parameters.insert(binding.index);
                            }
                            return;
                        }
                        collect_runtime_activation(node.callee, decl, info);
                        for (const ast::Argument &argument : node.arguments)
                        {
                            collect_runtime_activation(argument.value, decl, info);
                        }
                    }
                    else if constexpr (std::is_same_v<T, ast::Unary>)
                    {
                        collect_runtime_activation(node.operand, decl, info);
                    }
                    else if constexpr (std::is_same_v<T, ast::Binary>)
                    {
                        collect_runtime_activation(node.lhs, decl, info);
                        collect_runtime_activation(node.rhs, decl, info);
                    }
                    else if constexpr (std::is_same_v<T, ast::Index>)
                    {
                        collect_runtime_activation(node.target, decl, info);
                        collect_runtime_activation(node.index, decl, info);
                    }
                    else if constexpr (std::is_same_v<T, ast::Field>)
                    {
                        collect_runtime_activation(node.target, decl, info);
                    }
                    else if constexpr (std::is_same_v<T, ast::If>)
                    {
                        collect_runtime_activation(node.condition, decl, info);
                    }
                },
                expr.node);
        }

        void Emitter::check_runtime_expr(ast::ExprId id, ast::DeclId decl, const RuntimeValidSet &valid)
        {
            const ast::Expr &expr = module_.expr(id);
            std::visit(
                [&](const auto &node) {
                    using T = std::decay_t<decltype(node)>;
                    if constexpr (std::is_same_v<T, ast::NameRef> || std::is_same_v<T, ast::QualifiedRef>)
                    {
                        const semantics::Binding &binding = resolved_.binding(id);
                        if (binding.kind == BindingKind::Parameter && binding.decl == decl &&
                            binding.index < function(decl).signature.parameters.size() &&
                            !function(decl).signature.parameters[binding.index].is_const && !valid.contains(binding.index))
                        {
                            fail(Category::Type, expr.range, "temporal input '" + slice(expr.range) +
                                                                 "' may be invalid here; guard the read with valid(" +
                                                                 slice(expr.range) + ")");
                        }
                    }
                    else if constexpr (std::is_same_v<T, ast::Unary>)
                    {
                        check_runtime_expr(node.operand, decl, valid);
                    }
                    else if constexpr (std::is_same_v<T, ast::Binary>)
                    {
                        check_runtime_expr(node.lhs, decl, valid);
                        const RuntimeValidSet rhs_valid =
                            node.op == ast::BinaryOp::And ? runtime_true_valid(node.lhs, decl, valid) : valid;
                        check_runtime_expr(node.rhs, decl, rhs_valid);
                    }
                    else if constexpr (std::is_same_v<T, ast::Call>)
                    {
                        const semantics::Binding &callee = resolved_.binding(node.callee);
                        if (callee.kind == BindingKind::Intrinsic &&
                            (callee.registry_name == "valid" || callee.registry_name == "all_valid" ||
                             callee.registry_name == "modified" || callee.registry_name == "last_modified"))
                        {
                            // Metadata intrinsics inspect endpoint selectors; they do not read payloads.
                            return;
                        }
                        check_runtime_expr(node.callee, decl, valid);
                        for (const ast::Argument &argument : node.arguments)
                        {
                            check_runtime_expr(argument.value, decl, valid);
                        }
                    }
                    else if constexpr (std::is_same_v<T, ast::Index>)
                    {
                        check_runtime_expr(node.target, decl, valid);
                        check_runtime_expr(node.index, decl, valid);
                    }
                    else if constexpr (std::is_same_v<T, ast::Field>)
                    {
                        check_runtime_expr(node.target, decl, valid);
                    }
                    else if constexpr (std::is_same_v<T, ast::SequenceLiteral>)
                    {
                        for (const ast::SequenceElement &element : node.elements)
                        {
                            if (element.key != ast::no_node) { check_runtime_expr(element.key, decl, valid); }
                            check_runtime_expr(element.value, decl, valid);
                        }
                    }
                    else if constexpr (std::is_same_v<T, ast::TupleLiteral>)
                    {
                        for (const ast::ExprId element : node.elements)
                        {
                            check_runtime_expr(element, decl, valid);
                        }
                    }
                    else if constexpr (std::is_same_v<T, ast::AnonymousFn>)
                    {
                        check_runtime_expr(node.body, decl, valid);
                    }
                    else if constexpr (std::is_same_v<T, ast::If>)
                    {
                        const RuntimeValidSet then_valid = runtime_true_valid(node.condition, decl, valid);
                        check_runtime_block(node.then_block, decl, then_valid);
                        if (node.otherwise != ast::no_node)
                        {
                            check_runtime_expr(node.otherwise, decl, valid);
                        }
                    }
                    else if constexpr (std::is_same_v<T, ast::BlockExpr>)
                    {
                        check_runtime_block(node.block, decl, valid);
                    }
                    else if constexpr (std::is_same_v<T, ast::Eval>)
                    {
                        check_runtime_expr(node.callee, decl, valid);
                        for (const ast::Argument &argument : node.arguments)
                        {
                            check_runtime_expr(argument.value, decl, valid);
                        }
                    }
                    else if constexpr (std::is_same_v<T, ast::Construct>)
                    {
                        for (const ast::Argument &argument : node.arguments)
                        {
                            check_runtime_expr(argument.value, decl, valid);
                        }
                    }
                },
                expr.node);
        }

        Emitter::RuntimeValidSet Emitter::runtime_true_valid(ast::ExprId id, ast::DeclId decl,
                                                              const RuntimeValidSet &valid)
        {
            check_runtime_expr(id, decl, valid);
            RuntimeValidSet result = valid;
            const ast::Expr &expr  = module_.expr(id);
            if (const auto *call = std::get_if<ast::Call>(&expr.node))
            {
                const semantics::Binding &callee = resolved_.binding(call->callee);
                if (callee.kind == BindingKind::Intrinsic &&
                    (callee.registry_name == "valid" || callee.registry_name == "all_valid"))
                {
                    for (const ast::Argument &argument : call->arguments)
                    {
                        const semantics::Binding &binding = resolved_.binding(argument.value);
                        if (binding.kind == BindingKind::Parameter && binding.decl == decl &&
                            binding.index < function(decl).signature.parameters.size() &&
                            !function(decl).signature.parameters[binding.index].is_const)
                        {
                            result.insert(binding.index);
                        }
                    }
                }
                return result;
            }
            const auto *binary = std::get_if<ast::Binary>(&expr.node);
            if (binary == nullptr) { return result; }
            if (binary->op == ast::BinaryOp::And)
            {
                result = runtime_true_valid(binary->lhs, decl, valid);
                return runtime_true_valid(binary->rhs, decl, result);
            }
            if (binary->op == ast::BinaryOp::Or)
            {
                const RuntimeValidSet lhs = runtime_true_valid(binary->lhs, decl, valid);
                const RuntimeValidSet rhs = runtime_true_valid(binary->rhs, decl, valid);
                RuntimeValidSet       intersection;
                for (const std::size_t index : lhs)
                {
                    if (rhs.contains(index)) { intersection.insert(index); }
                }
                return intersection;
            }
            return result;
        }

        void Emitter::check_runtime_stmt(ast::StmtId id, ast::DeclId decl, const RuntimeValidSet &valid, bool allow_when)
        {
            const ast::Stmt &stmt = module_.stmt(id);
            std::visit(
                [&](const auto &node) {
                    using T = std::decay_t<decltype(node)>;
                    if constexpr (std::is_same_v<T, ast::LocalDecl>)
                    {
                        check_runtime_expr(node.init, decl, valid);
                    }
                    else if constexpr (std::is_same_v<T, ast::WhenStmt>)
                    {
                        if (!allow_when)
                        {
                            backend(stmt.range, "a 'when' block must be at function top level");
                        }
                        const RuntimeValidSet body_valid = runtime_true_valid(node.condition, decl, valid);
                        check_runtime_block(node.block, decl, body_valid);
                    }
                    else if constexpr (std::is_same_v<T, ast::ForStmt>)
                    {
                        check_runtime_expr(node.iterable, decl, valid);
                        check_runtime_block(node.block, decl, valid);
                    }
                    else if constexpr (std::is_same_v<T, ast::AssignStmt>)
                    {
                        check_runtime_expr(node.value, decl, valid);
                    }
                    else if constexpr (std::is_same_v<T, ast::ReturnStmt>)
                    {
                        if (node.value != ast::no_node) { check_runtime_expr(node.value, decl, valid); }
                    }
                    else if constexpr (std::is_same_v<T, ast::AssertStmt>)
                    {
                        check_runtime_expr(node.condition, decl, valid);
                    }
                    else if constexpr (std::is_same_v<T, ast::ExprStmt>)
                    {
                        check_runtime_expr(node.expr, decl, valid);
                    }
                },
                stmt.node);
        }

        void Emitter::check_runtime_block(ast::BlockId id, ast::DeclId decl, const RuntimeValidSet &valid, bool allow_when)
        {
            for (const ast::StmtId stmt : module_.block(id).statements)
            {
                check_runtime_stmt(stmt, decl, valid, allow_when);
            }
        }

        RuntimeInfo Emitter::runtime_info(ast::DeclId decl)
        {
            const ast::FunctionDecl &fn = function(decl);
            RuntimeInfo              info;
            Frame                    frame;
            frame.fn      = decl;
            frame.runtime = true;

            if (fn.concise_body != ast::no_node || fn.block_body == ast::no_node)
            {
                backend(module_.decl(decl).range, "a runtime function needs a block body");
            }
            if (fn.signature.result == ast::no_node)
            {
                backend(fn.name.range, "generated runtime sinks are not supported by emit-cpp yet");
            }
            const HType result = type_of(fn.signature.result, frame);
            if (result.kind != HType::Kind::Scalar)
            {
                backend(module_.type(fn.signature.result).range, "the first runtime-node slice supports scalar time-series outputs");
            }

            std::size_t temporal_count = 0;
            for (const ast::Parameter &param : fn.signature.parameters)
            {
                const HType type = type_of(param.type, frame);
                if (type.kind != HType::Kind::Scalar)
                {
                    backend(module_.type(param.type).range, "the first runtime-node slice supports scalar parameters");
                }
                if (!param.is_const)
                {
                    ++temporal_count;
                }
            }
            if (temporal_count == 0)
            {
                backend(fn.name.range, "generated runtime sources are not supported by emit-cpp yet");
            }

            for (const ast::StmtId id : module_.block(fn.block_body).statements)
            {
                const ast::Stmt &stmt = module_.stmt(id);
                std::visit(
                    [&](const auto &node) {
                        using T = std::decay_t<decltype(node)>;
                        if constexpr (std::is_same_v<T, ast::StateDecl>)
                        {
                            if (node.type == ast::no_node)
                            {
                                backend(node.name.range, "a generated runtime state declaration "
                                                         "needs an explicit scalar type");
                            }
                            const HType type = type_of(node.type, frame);
                            if (type.kind != HType::Kind::Scalar)
                            {
                                backend(module_.type(node.type).range, "the first runtime-node slice supports scalar state fields");
                            }
                            if (node.init == ast::no_node)
                            {
                                backend(node.name.range, "a generated runtime state field needs an initializer");
                            }
                            info.states.push_back(RuntimeState{
                                .id = id, .name = std::string{node.name.text}, .type = type, .init = node.init, .range = node.name.range});
                        }
                        else if constexpr (std::is_same_v<T, ast::InjectDecl>)
                        {
                            for (const ast::Name &name : node.names)
                            {
                                if (name.text != "out")
                                {
                                    backend(name.range, "injectable '" + std::string{name.text} + "' is not supported by emit-cpp yet");
                                }
                                info.inject_out = true;
                            }
                        }
                        else if constexpr (std::is_same_v<T, ast::LifecycleBlock>)
                        {
                            (node.is_stop ? info.stop_blocks : info.start_blocks).push_back(id);
                        }
                        else if constexpr (std::is_same_v<T, ast::WhenStmt>)
                        {
                            info.has_when = true;
                            collect_runtime_activation(node.condition, decl, info);
                        }
                    },
                    stmt.node);
            }
            if (info.start_blocks.size() > 1)
            {
                backend(module_.stmt(info.start_blocks[1]).range, "a runtime function has at most one 'start' block");
            }
            if (info.stop_blocks.size() > 1)
            {
                backend(module_.stmt(info.stop_blocks[1]).range, "a runtime function has at most one 'stop' block");
            }
            if (info.has_when && info.active_parameters.empty())
            {
                backend(fn.name.range, "a generated runtime function with 'when' needs a "
                                       "temporal parameter in 'modified(...)'");
            }
            if (!info.has_when)
            {
                for (std::size_t i = 0; i < fn.signature.parameters.size(); ++i)
                {
                    if (!fn.signature.parameters[i].is_const)
                    {
                        info.active_parameters.insert(i);
                    }
                }
            }
            RuntimeValidSet valid;
            if (!info.has_when) { valid = info.active_parameters; }
            for (const ast::StmtId id : module_.block(fn.block_body).statements)
            {
                const ast::Stmt &stmt = module_.stmt(id);
                if (std::holds_alternative<ast::StateDecl>(stmt.node) ||
                    std::holds_alternative<ast::InjectDecl>(stmt.node) ||
                    std::holds_alternative<ast::LifecycleBlock>(stmt.node))
                {
                    continue;
                }
                check_runtime_stmt(id, decl, valid, true);
            }
            return info;
        }

        void Emitter::check_supported(ast::DeclId decl)
        {
            const ast::FunctionDecl &fn    = function(decl);
            const SourceRange        range = module_.decl(decl).range;
            if (!fn.generics.empty())
            {
                unsupported(range, "a generic function");
            }
            if (resolved_.kind(decl) == semantics::FunctionKind::Runtime)
            {
                static_cast<void>(runtime_info(decl));
            }
        }

        std::string Emitter::runtime_signature(ast::DeclId decl, const RuntimeInfo &info, Frame &frame, bool with_names,
                                               bool include_inputs, bool include_output)
        {
            const ast::FunctionDecl &fn = function(decl);
            std::vector<std::string> params;
            for (std::size_t i = 0; i < fn.signature.parameters.size(); ++i)
            {
                const ast::Parameter &param  = fn.signature.parameters[i];
                const HType           type   = type_of(param.type, frame);
                const std::string     name   = with_names ? " " + cpp_name(param.name.text) : "";
                const std::string     unused = with_names ? "[[maybe_unused]] " : "";
                if (param.is_const)
                {
                    params.push_back(unused + "hgraph::Scalar<" + quote(param.name.text) + ", " +
                                     value_type(type, module_.type(param.type).range) + ">" + name);
                    continue;
                }
                if (!include_inputs) { continue; }
                std::string selector =
                    unused + "hgraph::In<" + quote(param.name.text) + ", " + schema(type, module_.type(param.type).range);
                if (!info.active_parameters.contains(i))
                {
                    selector += ", hgraph::InputActivity::Passive";
                }
                if (info.has_when)
                {
                    selector += ", hgraph::InputValidity::Unchecked";
                }
                selector += ">" + name;
                params.push_back(std::move(selector));
            }
            if (!info.states.empty())
            {
                params.push_back(std::string{with_names ? "[[maybe_unused]] " : ""} + "hgraph::RecordableState<recordable_state>" +
                                 std::string{with_names ? " hgl_state" : ""});
            }
            if (include_output)
            {
                params.push_back(std::string{with_names ? "[[maybe_unused]] " : ""} + "hgraph::Out<" +
                                 schema(type_of(fn.signature.result, frame), module_.type(fn.signature.result).range) + ">" +
                                 std::string{with_names ? " hgl_output" : ""});
            }
            return join(params, ", ");
        }

        void Emitter::prepare_runtime_frame(ast::DeclId decl, const RuntimeInfo &info, Frame &frame, Writer &out, bool include_inputs,
                                            bool include_output)
        {
            const ast::FunctionDecl &fn    = function(decl);
            frame.fn                       = decl;
            frame.runtime                  = true;
            frame.runtime_inputs_available = include_inputs;
            frame.output_available         = include_output;
            frame.params.resize(fn.signature.parameters.size());
            frame.locals.clear();
            frame.injects.clear();
            local_counts_.clear();
            local_names_.clear();
            local_names_.insert("hgl_state");
            local_names_.insert("hgl_output");
            for (std::size_t i = 0; i < fn.signature.parameters.size(); ++i)
            {
                const ast::Parameter &param = fn.signature.parameters[i];
                const HType           type  = type_of(param.type, frame);
                const std::string     name  = cpp_name(param.name.text);
                local_names_.insert(name);
                frame.params[i] = param.is_const ? make_const(name + ".value()", type, param.name.range)
                                                 : make_runtime(name + ".value()", type, param.name.range, name);
            }
            for (const RuntimeState &state : info.states)
            {
                const std::string base   = cpp_name(state.name);
                std::string       local  = base;
                int              &suffix = local_counts_[base];
                while (local_names_.contains(local))
                {
                    local = base + "_" + std::to_string(++suffix);
                }
                local_names_.insert(local);
                out.line("auto " + local + " = hgl_state.field<" + quote(state.name) + ">();");
                frame.locals[state.id] = make_runtime(local + ".value().checked_as<" + value_type(state.type, state.range) + ">()",
                                                      state.type, state.range, local);
            }
            if (include_output && info.inject_out)
            {
                const HType result = type_of(fn.signature.result, frame);
                frame.injects.emplace("out", make_runtime("hgl_output.value().checked_as<" +
                                                              value_type(result, module_.type(fn.signature.result).range) + ">()",
                                                          result, fn.name.range, "hgl_output"));
            }
        }

        void Emitter::emit_runtime_defaults(ast::DeclId decl, Writer &out)
        {
            const ast::FunctionDecl &fn = function(decl);
            std::vector<std::string> defaults;
            for (const ast::Parameter &param : fn.signature.parameters)
            {
                if (!param.is_const || param.default_value == ast::no_node)
                {
                    continue;
                }
                Frame scratch;
                scratch.fn        = decl;
                const Value value = eval_expr(param.default_value, scratch);
                const HType type  = type_of(param.type, scratch);
                defaults.push_back("hgraph::arg<" + quote(param.name.text) + ">(" +
                                   as_const(value, type, value.range, "default of '" + std::string{param.name.text} + "'") + ")");
            }
            if (!defaults.empty())
            {
                out.line("static auto defaults() { return std::tuple{" + join(defaults, ", ") + "}; }");
            }
        }

        void Emitter::emit_runtime_function(ast::DeclId decl, Writer &out)
        {
            const ast::FunctionDecl &fn   = function(decl);
            const RuntimeInfo        info = runtime_info(decl);
            Frame                    frame;
            frame.fn      = decl;
            frame.runtime = true;
            out.line("// " + where(module_.decl(decl).range));
            out.open("struct " + cpp_name(fn.name.text));
            out.line("[[maybe_unused]] static constexpr auto name = " +
                     quote(module_name_ + "." + std::string{fn.name.text}) + ";");
            emit_runtime_defaults(decl, out);
            if (!info.states.empty())
            {
                std::vector<std::string> fields;
                for (const RuntimeState &state : info.states)
                {
                    fields.push_back("hgraph::Field<" + quote(state.name) + ", " + schema(state.type, state.range) + ">");
                }
                out.line("using recordable_state = hgraph::TSB<" + quote(module_name_ + "." + std::string{fn.name.text} + ".state") + ", " +
                         join(fields, ", ") + ">;");
            }

            if (!info.states.empty() || !info.start_blocks.empty())
            {
                out.line("static void start(" + runtime_signature(decl, info, frame, true, false, false) + ")");
                out.open("");
                prepare_runtime_frame(decl, info, frame, out, false, false);
                for (const RuntimeState &state : info.states)
                {
                    const Value &target = frame.locals.at(state.id);
                    const Value  init   = eval_expr(state.init, frame);
                    out.line("if (!" + target.selector + ".valid()) { " + target.selector + ".set(" +
                             as_runtime(init, state.type, init.range, "initializer of '" + state.name + "'") + "); }");
                }
                for (const ast::StmtId id : info.start_blocks)
                {
                    emit_runtime_block(std::get<ast::LifecycleBlock>(module_.stmt(id).node).block, frame, out);
                }
                out.close();
            }

            out.line("static void eval(" + runtime_signature(decl, info, frame, true, true, true) + ")");
            out.open("");
            prepare_runtime_frame(decl, info, frame, out, true, true);
            for (const ast::StmtId id : module_.block(fn.block_body).statements)
            {
                const ast::StmtNode &node = module_.stmt(id).node;
                if (std::holds_alternative<ast::StateDecl>(node) || std::holds_alternative<ast::InjectDecl>(node) ||
                    std::holds_alternative<ast::LifecycleBlock>(node))
                {
                    continue;
                }
                emit_runtime_stmt(id, frame, out);
            }
            out.close();

            if (!info.stop_blocks.empty())
            {
                out.line("static void stop(" + runtime_signature(decl, info, frame, true, false, false) + ")");
                out.open("");
                prepare_runtime_frame(decl, info, frame, out, false, false);
                emit_runtime_block(std::get<ast::LifecycleBlock>(module_.stmt(info.stop_blocks.front()).node).block, frame, out);
                out.close();
            }
            out.close(";");
            out.line();
        }

        std::string Emitter::signature(ast::DeclId decl, Frame &frame, bool with_names)
        {
            const ast::FunctionDecl &fn = function(decl);
            std::vector<std::string> params{with_names ? "hgraph::Wiring &w" : "hgraph::Wiring &"};
            for (const ast::Parameter &param : fn.signature.parameters)
            {
                const HType type = type_of(param.type, frame);
                const std::string name = with_names ? " " + cpp_name(param.name.text) : "";
                if (param.is_const)
                {
                    params.push_back("hgraph::Scalar<" + quote(param.name.text) + ", " + value_type(type, module_.type(param.type).range) +
                                     ">" + name);
                }
                else { params.push_back("hgraph::Port<" + schema(type, module_.type(param.type).range) + ">" + name); }
            }
            return join(params, ", ");
        }

        std::string Emitter::result_type(ast::DeclId decl, Frame &frame)
        {
            const ast::FunctionDecl &fn = function(decl);
            if (fn.signature.result == ast::no_node) { return "void"; }
            return "hgraph::Port<" + schema(type_of(fn.signature.result, frame), module_.type(fn.signature.result).range) + ">";
        }

        /// The operator marker for a public callable: its parameters as
        /// `In`/`Scalar` selectors and its result as `Out`.
        std::string Emitter::marker(ast::DeclId decl, std::string_view registry_name, Frame &frame)
        {
            const ast::DeclNode                 &node   = module_.decl(decl).node;
            const ast::Signature                *sig    = nullptr;
            const std::vector<ast::GenericParameter> *generics = nullptr;
            if (const auto *fn = std::get_if<ast::FunctionDecl>(&node))
            {
                sig      = &fn->signature;
                generics = &fn->generics;
            }
            else
            {
                const auto &op = std::get<ast::OperatorDecl>(node);
                sig      = &op.signature;
                generics = &op.generics;
            }
            if (!generics->empty()) { unsupported(module_.decl(decl).range, "a generic operator"); }
            std::vector<std::string> selectors{quote(registry_name)};
            for (const ast::Parameter &param : sig->parameters)
            {
                const HType       type  = type_of(param.type, frame);
                const SourceRange range = module_.type(param.type).range;
                if (param.is_const)
                {
                    selectors.push_back("hgraph::Scalar<" + quote(param.name.text) + ", " + value_type(type, range) + ">");
                }
                else { selectors.push_back("hgraph::In<" + quote(param.name.text) + ", " + schema(type, range) + ">"); }
            }
            if (sig->result != ast::no_node)
            {
                selectors.push_back("hgraph::Out<" + schema(type_of(sig->result, frame), module_.type(sig->result).range) + ">");
            }
            return "hgraph::Operator<" + join(selectors, ", ") + ">";
        }

        void Emitter::emit_function(ast::DeclId decl, Writer &out, Form form)
        {
            check_supported(decl);
            if (resolved_.kind(decl) == semantics::FunctionKind::Runtime)
            {
                if (form != Form::InlineStruct)
                {
                    backend(module_.decl(decl).range, "a generated runtime node must be "
                                                      "emitted as a complete static struct");
                }
                emit_runtime_function(decl, out);
                return;
            }
            const ast::FunctionDecl &fn = function(decl);
            Frame                    frame;
            frame.fn = decl;
            const std::string name = cpp_name(fn.name.text);

            out.line("// " + where(module_.decl(decl).range));
            if (form == Form::OutOfLine)
            {
                out.line(result_type(decl, frame) + " " + name + "::compose(" + signature(decl, frame, true) + ")");
            }
            else
            {
                out.open("struct " + name);
                out.line("[[maybe_unused]] static constexpr auto name = " +
                         quote(module_name_ + "." + std::string{fn.name.text}) + ";");
                // Defaults of const parameters travel with the graph so the
                // registry can apply them when the function is called by name.
                std::vector<std::string> defaults;
                for (const ast::Parameter &param : fn.signature.parameters)
                {
                    if (!param.is_const || param.default_value == ast::no_node) { continue; }
                    Frame scratch;
                    scratch.fn        = decl;
                    const Value value = eval_expr(param.default_value, scratch);
                    const HType type  = type_of(param.type, scratch);
                    defaults.push_back("hgraph::arg<" + quote(param.name.text) + ">(" +
                                       as_const(value, type, value.range, "default of '" + std::string{param.name.text} + "'") + ")");
                }
                if (!defaults.empty())
                {
                    out.line("static auto defaults() { return std::tuple{" + join(defaults, ", ") + "}; }");
                }
                out.line("static " + result_type(decl, frame) + " compose(" + signature(decl, frame, form != Form::Declaration) +
                         (form == Form::Declaration ? ");" : ")"));
                if (form == Form::Declaration)
                {
                    out.close(";");
                    out.line();
                    return;
                }
            }

            // The body.
            local_counts_.clear();
            local_names_.clear();
            local_names_.insert("w");
            frame.params.resize(fn.signature.parameters.size());
            for (std::size_t i = 0; i < fn.signature.parameters.size(); ++i)
            {
                const ast::Parameter &param = fn.signature.parameters[i];
                const HType           type  = type_of(param.type, frame);
                local_names_.insert(cpp_name(param.name.text));
                if (param.is_const)
                {
                    frame.params[i] = make_const(cpp_name(param.name.text) + ".value()", type, param.name.range);
                }
                else { frame.params[i] = make_port(cpp_name(param.name.text), type, param.name.range); }
            }
            out.open("");
            if (fn.concise_body != ast::no_node)
            {
                const Value value = eval_expr(fn.concise_body, frame);
                emit_return(value, frame, out, module_.expr(fn.concise_body).range);
            }
            else { emit_block(fn.block_body, frame, out, true); }
            out.close();
            if (form == Form::InlineStruct) { out.close(";"); }
            out.line();
        }

        void Emitter::collect_calls_block(ast::BlockId id, std::set<ast::DeclId> &calls)
        {
            for (const ast::StmtId stmt_id : module_.block(id).statements)
            {
                std::visit(
                    [&](const auto &node) {
                        using T = std::decay_t<decltype(node)>;
                        if constexpr (std::is_same_v<T, ast::LocalDecl> || std::is_same_v<T, ast::StateDecl>)
                        {
                            if (node.init != ast::no_node) { collect_calls(node.init, calls); }
                        }
                        else if constexpr (std::is_same_v<T, ast::AssignStmt>) { collect_calls(node.value, calls); }
                        else if constexpr (std::is_same_v<T, ast::ReturnStmt>)
                        {
                            if (node.value != ast::no_node) { collect_calls(node.value, calls); }
                        }
                        else if constexpr (std::is_same_v<T, ast::AssertStmt>) { collect_calls(node.condition, calls); }
                        else if constexpr (std::is_same_v<T, ast::ExprStmt>) { collect_calls(node.expr, calls); }
                        else if constexpr (std::is_same_v<T, ast::WhenStmt>)
                        {
                            collect_calls(node.condition, calls);
                            collect_calls_block(node.block, calls);
                        }
                        else if constexpr (std::is_same_v<T, ast::ForStmt>)
                        {
                            collect_calls(node.iterable, calls);
                            collect_calls_block(node.block, calls);
                        }
                        else if constexpr (std::is_same_v<T, ast::LifecycleBlock>) { collect_calls_block(node.block, calls); }
                    },
                    module_.stmt(stmt_id).node);
            }
        }

        void Emitter::collect_calls(ast::ExprId id, std::set<ast::DeclId> &calls)
        {
            const ast::Expr &expr = module_.expr(id);
            std::visit(
                [&](const auto &node) {
                    using T = std::decay_t<decltype(node)>;
                    if constexpr (std::is_same_v<T, ast::NameRef> || std::is_same_v<T, ast::QualifiedRef>)
                    {
                        const semantics::Binding &binding = resolved_.binding(id);
                        if (binding.kind == BindingKind::Function) { calls.insert(binding.decl); }
                    }
                    else if constexpr (std::is_same_v<T, ast::Unary>) { collect_calls(node.operand, calls); }
                    else if constexpr (std::is_same_v<T, ast::Binary>)
                    {
                        collect_calls(node.lhs, calls);
                        collect_calls(node.rhs, calls);
                    }
                    else if constexpr (std::is_same_v<T, ast::Call>)
                    {
                        collect_calls(node.callee, calls);
                        for (const ast::Argument &argument : node.arguments) { collect_calls(argument.value, calls); }
                    }
                    else if constexpr (std::is_same_v<T, ast::Index>)
                    {
                        collect_calls(node.target, calls);
                        collect_calls(node.index, calls);
                    }
                    else if constexpr (std::is_same_v<T, ast::Field>) { collect_calls(node.target, calls); }
                    else if constexpr (std::is_same_v<T, ast::SequenceLiteral>)
                    {
                        for (const ast::SequenceElement &element : node.elements) { collect_calls(element.value, calls); }
                    }
                    else if constexpr (std::is_same_v<T, ast::TupleLiteral>)
                    {
                        for (const ast::ExprId element : node.elements) { collect_calls(element, calls); }
                    }
                    else if constexpr (std::is_same_v<T, ast::AnonymousFn>) { collect_calls(node.body, calls); }
                    else if constexpr (std::is_same_v<T, ast::If>)
                    {
                        collect_calls(node.condition, calls);
                        collect_calls_block(node.then_block, calls);
                        if (node.otherwise != ast::no_node) { collect_calls(node.otherwise, calls); }
                    }
                    else if constexpr (std::is_same_v<T, ast::BlockExpr>) { collect_calls_block(node.block, calls); }
                    else if constexpr (std::is_same_v<T, ast::Eval>)
                    {
                        collect_calls(node.callee, calls);
                        for (const ast::Argument &argument : node.arguments) { collect_calls(argument.value, calls); }
                    }
                    else if constexpr (std::is_same_v<T, ast::Construct>)
                    {
                        for (const ast::Argument &argument : node.arguments) { collect_calls(argument.value, calls); }
                    }
                },
                expr.node);
        }

        /// Internal functions in dependency order: C++ needs a helper defined
        /// before the compose body that wires it.
        std::vector<ast::DeclId> Emitter::ordered_internal_functions()
        {
            std::vector<ast::DeclId> internal;
            for (const ast::DeclId id : resolved_.functions)
            {
                if (function(id).visibility == ast::FunctionVisibility::Internal) { internal.push_back(id); }
            }
            std::map<ast::DeclId, std::set<ast::DeclId>> deps;
            for (const ast::DeclId id : internal)
            {
                const ast::FunctionDecl &fn = function(id);
                std::set<ast::DeclId>    calls;
                if (fn.concise_body != ast::no_node) { collect_calls(fn.concise_body, calls); }
                if (fn.block_body != ast::no_node) { collect_calls_block(fn.block_body, calls); }
                for (const ast::Parameter &param : fn.signature.parameters)
                {
                    if (param.default_value != ast::no_node) { collect_calls(param.default_value, calls); }
                }
                std::set<ast::DeclId> filtered;
                for (const ast::DeclId call : calls)
                {
                    if (std::find(internal.begin(), internal.end(), call) != internal.end()) { filtered.insert(call); }
                }
                deps[id] = std::move(filtered);
            }
            std::vector<ast::DeclId> ordered;
            std::set<ast::DeclId>    done;
            std::set<ast::DeclId>    visiting;
            std::function<void(ast::DeclId)> visit = [&](ast::DeclId id) {
                if (done.contains(id)) { return; }
                if (visiting.contains(id))
                {
                    backend(module_.decl(id).range,
                            "'" + std::string{function(id).name.text} + "' is recursive; recursive functions are not supported");
                }
                visiting.insert(id);
                for (const ast::DeclId dep : deps[id]) { visit(dep); }
                visiting.erase(id);
                done.insert(id);
                ordered.push_back(id);
            };
            for (const ast::DeclId id : internal) { visit(id); }
            return ordered;
        }

        // ------------------------------------------------------------ module

        EmittedModule Emitter::emit()
        {
            EmittedModule result;
            result.module_name = resolved_.module_path;
            if (result.module_name.empty()) { fail(Category::Module, SourceRange{0, 0}, "emit-cpp needs a module declaration"); }
            {
                std::string ns;
                std::string part;
                for (const char c : result.module_name)
                {
                    if (c == '.')
                    {
                        ns += cpp_name(part) + "::";
                        part.clear();
                    }
                    else { part += c; }
                }
                ns += cpp_name(part);
                namespace_ = ns;
            }
            result.namespace_name = namespace_;
            module_name_          = result.module_name;
            basename_             = file_.path();
            if (const auto slash = basename_.find_last_of("/\\"); slash != std::string::npos) { basename_.erase(0, slash + 1); }

            for (const ast::DeclId id : resolved_.structs) { unsupported(module_.decl(id).range, "a struct declaration"); }

            // Every emitted function is checked up front so the whole unit
            // fails closed before a partial pair is written.
            std::vector<ast::DeclId> exports;
            std::vector<ast::DeclId> impls;
            std::map<std::string, std::string> cpp_functions;
            for (const ast::DeclId id : resolved_.functions)
            {
                check_supported(id);
                const ast::FunctionDecl &fn = function(id);
                const std::string        source_name{fn.name.text};
                const std::string        generated_name = cpp_name(source_name);
                if (const auto [found, inserted] = cpp_functions.emplace(generated_name, source_name);
                    !inserted && found->second != source_name)
                {
                    backend(fn.name.range, "C++ function '" + source_name + "' collides with '" + found->second +
                                               "' as '" + generated_name + "'");
                }
                std::map<std::string, std::string> cpp_parameters;
                for (const ast::Parameter &param : fn.signature.parameters)
                {
                    const std::string parameter_name{param.name.text};
                    const std::string generated_parameter = cpp_name(parameter_name);
                    if (const auto [found, inserted] = cpp_parameters.emplace(generated_parameter, parameter_name);
                        !inserted && found->second != parameter_name)
                    {
                        backend(param.name.range, "C++ parameter '" + parameter_name + "' collides with '" + found->second +
                                                      "' as '" + generated_parameter + "'");
                    }
                }
                if (fn.visibility == ast::FunctionVisibility::Export) { exports.push_back(id); }
                if (fn.visibility == ast::FunctionVisibility::Impl) { impls.push_back(id); }
            }
            const std::vector<ast::DeclId> internal = ordered_internal_functions();

            // Bodies first: they discover which kernels (analytics) the
            // header must include.
            Writer body;
            body.line("namespace " + namespace_);
            body.line("{");
            if (!internal.empty() || !impls.empty())
            {
                body.indent();
                body.open("namespace");
                for (const ast::DeclId id : internal)
                {
                    if (resolved_.kind(id) != semantics::FunctionKind::Runtime) { continue; }
                    Frame frame;
                    frame.fn = id;
                    body.line("struct hgl_internal_operator_" + std::to_string(id) + " : " +
                              marker(id, result.module_name + "." + std::string{function(id).name.text}, frame) + " {};");
                }
                if (std::any_of(internal.begin(), internal.end(), [&](ast::DeclId id) {
                        return resolved_.kind(id) == semantics::FunctionKind::Runtime;
                    }))
                {
                    body.line();
                }
                for (const ast::DeclId id : internal) { emit_function(id, body, Form::InlineStruct); }
                for (const ast::DeclId id : impls) { emit_function(id, body, Form::InlineStruct); }
                body.close("  // namespace");
                body.line();
                body.dedent();
            }
            body.indent();
            for (const ast::DeclId id : exports)
            {
                if (resolved_.kind(id) == semantics::FunctionKind::Composition)
                {
                    emit_function(id, body, Form::OutOfLine);
                }
            }

            // Registration: exported functions and operator implementations
            // become registry candidates under module-qualified names, and
            // the installer replays them after a registry reset.
            body.open("hgraph::OperatorProviderHandle register_operators()");
            body.line("auto &registry = hgraph::OperatorRegistry::instance();");
            body.open("auto provider = registry.register_installer(" + quote(result.module_name) + ", []");
            for (const ast::DeclId id : exports)
            {
                const std::string name = cpp_name(function(id).name.text);
                const std::string registration = resolved_.kind(id) == semantics::FunctionKind::Runtime
                                                     ? "hgraph::register_overload"
                                                     : "hgraph::register_graph_overload";
                body.line(registration + "<ops::" + name + ", " + name + ">();");
            }
            for (const ast::DeclId id : internal)
            {
                if (resolved_.kind(id) != semantics::FunctionKind::Runtime) { continue; }
                body.line("hgraph::register_overload<hgl_internal_operator_" + std::to_string(id) + ", " +
                          cpp_name(function(id).name.text) + ">();");
            }
            for (const ast::DeclId id : impls)
            {
                const ast::FunctionDecl &fn = function(id);
                bool                     bound = false;
                for (const ast::DeclId op_id : resolved_.operators)
                {
                    if (std::get<ast::OperatorDecl>(module_.decl(op_id).node).name.text == fn.name.text)
                    {
                        bound = true;
                        break;
                    }
                }
                if (!bound)
                {
                    unsupported(module_.decl(id).range, "an impl fn of an imported operator");
                }
                const std::string registration = resolved_.kind(id) == semantics::FunctionKind::Runtime
                                                     ? "hgraph::register_overload"
                                                     : "hgraph::register_graph_overload";
                body.line(registration + "<ops::" + cpp_name(fn.name.text) + ", " + cpp_name(fn.name.text) + ">();");
            }
            body.close(");");
            body.open("auto rollback = hgraph::make_scope_exit<true>([&]");
            body.line("(void)registry.remove_provider(provider);");
            body.close(");");
            body.line("registry.activate_provider(provider);");
            body.line("rollback.release();");
            body.line("return provider;");
            body.close();
            body.dedent();
            body.line("}  // namespace " + namespace_);

            // The header.
            Writer header;
            const std::string banner = "// Generated by hgl " + options_.tool_version + " from " + basename_ + "; do not edit.";
            header.line(banner);
            header.line("#pragma once");
            header.line();
            header.line("#include <hgraph/lib/std/operators/operators.h>");
            if (uses_analytics_) { header.line("#include <hgraph/analytics/operators.h>"); }
            header.line("#include <hgraph/types/graph_wiring.h>");
            header.line("#include <hgraph/types/operator_dispatch.h>");
            header.line("#include <hgraph/types/static_node.h>");
            header.line("#include <hgraph/types/static_schema.h>");
            header.line();
            header.line("#include <chrono>");
            header.line("#include <stdexcept>");
            header.line("#include <tuple>");
            header.line();
            header.line("namespace " + namespace_);
            header.line("{");
            header.indent();
            header.line("/// Operator markers: the module's public callables by registry name.");
            header.open("namespace ops");
            for (const ast::DeclId id : resolved_.operators)
            {
                const auto &op = std::get<ast::OperatorDecl>(module_.decl(id).node);
                Frame       frame;
                header.line("// " + where(module_.decl(id).range));
                header.line("struct " + cpp_name(op.name.text) + " : " +
                            marker(id, result.module_name + "." + std::string{op.name.text}, frame) + " {};");
            }
            for (const ast::DeclId id : exports)
            {
                const ast::FunctionDecl &fn = function(id);
                Frame                    frame;
                frame.fn = id;
                header.line("// " + where(module_.decl(id).range));
                header.line("struct " + cpp_name(fn.name.text) + " : " +
                            marker(id, result.module_name + "." + std::string{fn.name.text}, frame) + " {};");
            }
            header.close("  // namespace ops");
            header.line();
            for (const ast::DeclId id : exports)
            {
                emit_function(id, header,
                              resolved_.kind(id) == semantics::FunctionKind::Runtime ? Form::InlineStruct : Form::Declaration);
                result.exports.push_back(std::string{function(id).name.text});
            }
            header.line("/// Register the module's operators and implementations with the hgraph");
            header.line("/// registry and return the exact removable provider generation.");
            header.line("hgraph::OperatorProviderHandle register_operators();");
            header.dedent();
            header.line("}  // namespace " + namespace_);

            Writer source;
            source.line(banner);
            source.line("#include \"" + options_.header_name + "\"");
            source.line();
            source.line("#include <hgraph/types/operator_dispatch.h>");
            source.line("#include <hgraph/util/scope.h>");
            source.line();
            result.header = header.str();
            result.source = source.str() + body.str();

            if (!options_.python_native_module.empty())
            {
                if (!is_python_identifier(options_.python_native_module) || is_python_keyword(options_.python_native_module))
                {
                    backend(SourceRange{0, 0}, "'" + options_.python_native_module +
                                                        "' is not a valid Python native-module identifier");
                }
                std::string py;
                py += "\"\"\"Generated by hgl " + options_.tool_version + " from " + basename_ + "; do not edit.\n\n";
                py += "Python surface of HGL module ``" + result.module_name + "``: importing this module loads the\n";
                py += "native registration module and exposes each exported function as an hgraph operator.\n\"\"\"\n\n";
                py += "from hgraph import operator_function as _hgl_operator_function\n\n";
                py += "from . import " + options_.python_native_module + " as _hgl_native  # noqa: F401  (registers the operators)\n\n";
                py += "globals().update({\n";
                std::vector<std::string> names;
                std::map<std::string, std::string> python_exports;
                for (std::size_t i = 0; i < result.exports.size(); ++i)
                {
                    const std::string &name  = result.exports[i];
                    const std::string  alias = python_name(name);
                    if (const auto [found, inserted] = python_exports.emplace(alias, name); !inserted)
                    {
                        backend(module_.decl(exports[i]).range,
                                "Python export '" + name + "' collides with '" + found->second + "' as '" + alias + "'");
                    }
                    py += "    " + quote(alias) + ": _hgl_operator_function(" + quote(result.module_name + "." + name) + "),\n";
                    names.push_back(quote(alias));
                }
                py += "})\n\n__all__ = [" + join(names, ", ") + "]\n";
                result.python = std::move(py);
            }
            return result;
        }
    }  // namespace

    std::optional<EmittedModule> emit_cpp(const syntax::SourceFile &file, const ast::Module &module,
                                          const semantics::ResolvedModule &resolved, const EmitOptions &options,
                                          syntax::DiagnosticSink &diagnostics)
    {
        Emitter emitter{file, module, resolved, options, diagnostics};
        try
        {
            return emitter.emit();
        }
        catch (const Abort &)
        {
            return std::nullopt;
        }
    }
}  // namespace hgl::codegen
