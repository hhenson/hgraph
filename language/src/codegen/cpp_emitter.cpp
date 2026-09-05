#include "codegen/cpp_emitter.h"

#include <algorithm>
#include <cmath>
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
// direct-wiring backend construct by construct so that programs accepted by
// both build the same graph. It also lowers the documented runtime, structural,
// generic, window, and collection examples to public hgraph authoring APIs; the
// native compiler and hgraph registry check each emitted package.
namespace hgl::codegen
{
    namespace gir = hgraph_ir;

    namespace
    {
        using semantics::BindingKind;
        using syntax::Category;
        using syntax::SourceRange;

        struct Abort
        {
        };

        // ------------------------------------------------------------ types

        /// A normalized type used by the temporary C++ body printer. Public
        /// interface instances come from hgraph IR; local annotations still
        /// arrive through the syntax compatibility adapter.
        struct HType
        {
            enum class Kind : std::uint8_t {
                Unknown,  ///< a port whose schema the registry decides
                Scalar,
                Tuple,
                List,
                Set,
                Map,
                Rolling,
                Atomic,
                Generic,
                Struct,
            };

            Kind               kind{Kind::Unknown};
            ast::ScalarType    scalar{ast::ScalarType::Bool};
            std::vector<HType> children{};
            std::string        size{};      ///< list fixed size / rolling max, as C++ text
            std::string        min_size{};  ///< rolling minimum, as C++ text
            bool               duration_window{false};
            ast::DeclId        declaration{ast::no_node};
            std::string        nominal_identity{};
            std::string        cpp_type{};

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
            if (a.nominal_identity != b.nominal_identity || a.cpp_type != b.cpp_type) { return false; }
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
            enum class Kind : std::uint8_t {
                Void,
                Const,
                Port,
                Runtime,   ///< an evaluation-time scalar, optionally backed by a selector
                Iterator,  ///< an evaluation-local borrowed collection range
                Function,
                Struct,
                Operator,       ///< an imported kernel operator: `name` is the C++ marker
                LocalOperator,  ///< a module `operator`: `name` is the C++ marker
                Intrinsic,      ///< `name` is the intrinsic
            };

            Kind        kind{Kind::Void};
            std::string code{};
            /// A temporal struct constructor also carries its atomic
            /// aggregate spelling so the result context selects the shape.
            std::string atomic_code{};
            /// Runtime values backed by an endpoint keep its selector spelling
            /// so metadata intrinsics and assignments do not read the payload.
            std::string selector{};
            /// Const: the value type. Port: the temporal type, Unknown when the
            /// registry decides it (an operator result).
            HType       type{};
            ast::DeclId decl{ast::no_node};
            std::string name{};
            SourceRange range{};
            bool               structured_delta{false};
            std::vector<HType> iterator_types{};
            ast::ExprId        iterator_predicate{ast::no_node};
            /// Known numeric value of a constant expression. Const parameters
            /// deliberately leave this empty: they are values at composition
            /// time, not compile-time literals. The emitter uses this only for
            /// diagnostics that native C++ would otherwise defer or lose
            /// (rolling sizes and zero divisors).
            std::variant<std::monostate, std::int64_t, double> number{};

            [[nodiscard]] bool is_const() const noexcept { return kind == Kind::Const; }
            [[nodiscard]] bool is_port() const noexcept { return kind == Kind::Port; }
            [[nodiscard]] bool is_runtime() const noexcept { return kind == Kind::Runtime; }
            [[nodiscard]] bool is_iterator() const noexcept { return kind == Kind::Iterator; }
        };

        struct Frame
        {
            ast::DeclId                           fn{ast::no_node};
            std::vector<Value>                    params{};
            std::unordered_map<ast::StmtId, Value> locals{};
            std::unordered_map<ast::StmtId, Value> second_locals{};
            std::unordered_map<std::string, Value> injects{};
            std::unordered_map<std::size_t, HType> generic_types{};
            ast::ExprId                            anonymous{ast::no_node};
            std::vector<Value>                     anonymous_params{};
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
            bool                            inject_logger{false};
            bool                            has_when{false};
        };

        Value                       make_const(std::string code, HType type, SourceRange range,
                                               std::variant<std::monostate, std::int64_t, double> number = {});
        std::optional<Value>        temporal_constant(syntax::TemporalValue literal, SourceRange range);
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
            "w", "hgraph", "std", "operators", "operator_contracts", "register_operators", "compose", "name",
            "defaults", "recordable_state", "hgl_state", "hgl_output",
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

        std::string integer_literal(std::int64_t value) {
            if (value == std::numeric_limits<std::int64_t>::min()) { return "std::numeric_limits<hgraph::Int>::min()"; }
            return "hgraph::Int{" + std::to_string(value) + "}";
        }

        std::string float_literal(double value) {
            if (std::isnan(value)) { return "std::numeric_limits<hgraph::Float>::quiet_NaN()"; }
            if (std::isinf(value)) {
                return std::string{std::signbit(value) ? "-" : ""} + "std::numeric_limits<hgraph::Float>::infinity()";
            }
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
            void                      append(std::string_view text) { out_ += text; }
            [[nodiscard]] std::string str() const { return out_; }

          private:
            std::string out_;
            int         indent_{0};
        };

        // ---------------------------------------------------------- emitter

        class Emitter
        {
          public:
            Emitter(const syntax::SourceFile &file, const gir::Module &graph, const ast::Module &module,
                    const semantics::ResolvedModule &resolved, const EmitOptions &options, syntax::DiagnosticSink &diagnostics)
                : file_{file}, graph_{graph}, module_{module}, resolved_{resolved}, options_{options}, diagnostics_{diagnostics} {}

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
            [[nodiscard]] const ast::StructDecl &structure(ast::DeclId decl) const {
                return std::get<ast::StructDecl>(module_.decl(decl).node);
            }
            void                                       bind_hgraph_declarations();
            [[nodiscard]] const gir::Callable         &callable(ast::DeclId decl) const { return *callables_.at(decl); }
            [[nodiscard]] const gir::OperatorContract &operator_decl(ast::DeclId decl) const { return *operators_.at(decl); }
            [[nodiscard]] static std::string_view      local_identity(std::string_view identity) noexcept;
            [[nodiscard]] std::string_view             callable_name(ast::DeclId decl) const;
            [[nodiscard]] std::string                  callable_cpp_name(ast::DeclId decl);
            [[nodiscard]] static std::string           operator_registry_name(const gir::OperatorContract &op) {
                return op.registry_name.empty() ? op.identity : op.registry_name;
            }
            [[nodiscard]] const ast::GenericParameter &generic_parameter(ast::DeclId decl, std::size_t index) const;
            [[nodiscard]] std::string                  slice(SourceRange range) const { return std::string{file_.slice(range)}; }
            [[nodiscard]] std::string where(SourceRange range) const
            {
                const syntax::Location at = file_.location(range.begin);
                return basename_ + ":" + std::to_string(at.line);
            }

            // -- types
            [[nodiscard]] HType       type_of(ast::TypeId id, Frame &frame);
            [[nodiscard]] HType                       planned_type(gir::TypeId id, SourceRange fallback = {});
            [[nodiscard]] const gir::Type            &graph_type(gir::TypeId id, SourceRange fallback);
            [[nodiscard]] const gir::ConstExpr       &graph_constant(gir::ConstExprId id, SourceRange fallback);
            [[nodiscard]] std::optional<std::int64_t> planned_integer(gir::ConstExprId id, SourceRange fallback);
            [[nodiscard]] bool                        has_planned_result(gir::TypeId id, SourceRange fallback);
            [[nodiscard]] Value                       planned_constant(gir::ConstExprId id, SourceRange fallback = {});
            [[nodiscard]] std::string value_type(const HType &type, SourceRange range);
            [[nodiscard]] std::string schema(const HType &type, SourceRange range);
            [[nodiscard]] std::string size_text(ast::ExprId id, Frame &frame, std::string_view what);

            // -- expressions
            [[nodiscard]] Value eval_expr(ast::ExprId id, Frame &frame);
            [[nodiscard]] Value eval_name(ast::ExprId id, Frame &frame);
            [[nodiscard]] Value eval_call(const ast::Call &call, SourceRange range, Frame &frame);
            [[nodiscard]] Value eval_construct(ast::DeclId decl, ast::TypeId type, const std::vector<ast::Argument> &arguments,
                                               bool delta, SourceRange range, Frame &frame);
            [[nodiscard]] Value lower_map_call(const Value &callee, const ast::Call &call, SourceRange range, Frame &frame);
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
            [[nodiscard]] std::vector<ast::ExprId> bind_arguments(ast::DeclId decl, const std::vector<ast::Argument> &arguments,
                                                                  SourceRange range);

            // -- statements
            void emit_block(ast::BlockId id, Frame &frame, Writer &out, bool function_body);
            void emit_stmt(ast::StmtId id, Frame &frame, Writer &out);
            void emit_return(const Value &value, Frame &frame, Writer &out, SourceRange range);
            void emit_runtime_stmt(ast::StmtId id, Frame &frame, Writer &out);
            void emit_runtime_block(ast::BlockId id, Frame &frame, Writer &out);
            void emit_runtime_if(const ast::If &branch, Frame &frame, Writer &out);
            [[nodiscard]] bool expression_terminates(ast::ExprId id) const;
            [[nodiscard]] bool block_terminates(ast::BlockId id) const;
            [[nodiscard]] std::string as_runtime(const Value &value, const HType &target, SourceRange range, const std::string &what);

            // -- declarations
            void check_supported(ast::DeclId decl);
            [[nodiscard]] std::string signature(ast::DeclId decl, bool with_names);
            [[nodiscard]] std::string result_type(ast::DeclId decl);
            [[nodiscard]] std::string operator_contract(const std::vector<gir::Parameter> &parameters, gir::TypeId result,
                                                        std::string_view registry_name);
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
            void                      emit_struct(ast::DeclId decl, Writer &out);
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
            [[nodiscard]] std::string runtime_signature(ast::DeclId decl, const RuntimeInfo &info, bool with_names,
                                                        bool include_inputs, bool include_output);
            void prepare_runtime_frame(ast::DeclId decl, const RuntimeInfo &info, Frame &frame, Writer &out, bool include_inputs,
                                       bool include_output);
            void emit_defaults(const gir::Callable &callable, Writer &out);
            [[nodiscard]] std::vector<ast::DeclId> ordered_internal_functions();
            void collect_calls(ast::ExprId id, std::set<ast::DeclId> &calls);
            void collect_calls_block(ast::BlockId id, std::set<ast::DeclId> &calls);

            const syntax::SourceFile        &file_;
            const gir::Module                                             &graph_;
            const ast::Module               &module_;
            const semantics::ResolvedModule &resolved_;
            const EmitOptions               &options_;
            syntax::DiagnosticSink          &diagnostics_;
            std::string                      basename_{};
            std::string                      namespace_{};
            std::string                      module_name_{};
            std::vector<ast::DeclId>                                       callable_declarations_{};
            std::vector<ast::DeclId>                                       operator_declarations_{};
            std::unordered_map<ast::DeclId, const gir::Callable *>         callables_{};
            std::unordered_map<ast::DeclId, const gir::OperatorContract *> operators_{};
            bool                             uses_analytics_{false};
            /// Locals declared in the current function, for unique C++ names.
            std::unordered_map<std::string, int> local_counts_{};
            std::unordered_set<std::string>      local_names_{};
            Writer                               generated_helpers_{};
            std::size_t                          anonymous_function_index_{0};
        };

        std::string_view Emitter::local_identity(std::string_view identity) noexcept {
            const std::size_t separator = identity.find_last_of('.');
            return separator == std::string_view::npos ? identity : identity.substr(separator + 1);
        }

        std::string_view Emitter::callable_name(ast::DeclId decl) const {
            const gir::Callable &item = callable(decl);
            if (item.visibility == gir::CallableVisibility::Implementation && !item.operator_identity.empty()) {
                return local_identity(item.operator_identity);
            }
            return local_identity(item.identity);
        }

        std::string Emitter::callable_cpp_name(ast::DeclId decl) {
            const gir::Callable &item = callable(decl);
            std::string          name = cpp_name(callable_name(decl));
            if (item.visibility != gir::CallableVisibility::Implementation) { return name; }

            const std::size_t marker = item.identity.find_last_of('#');
            if (marker == std::string::npos || marker + 1U == item.identity.size() ||
                item.identity.find_first_not_of("0123456789", marker + 1U) != std::string::npos) {
                backend(item.range, "hgraph IR implementation '" + item.identity + "' has no canonical numeric identity");
            }
            return name + "_impl_" + item.identity.substr(marker + 1U);
        }

        void Emitter::bind_hgraph_declarations() {
            const auto function_for_range = [&](SourceRange range) {
                ast::DeclId match = ast::no_node;
                for (const ast::DeclId id : resolved_.functions) {
                    if (module_.decl(id).range != range) { continue; }
                    if (match != ast::no_node) {
                        backend(range, "hgraph IR callable range matches more than one syntax declaration");
                    }
                    match = id;
                }
                return match;
            };
            for (const gir::Callable &item : graph_.callables) {
                const ast::DeclId id = function_for_range(item.range);
                if (id == ast::no_node) {
                    backend(item.range, "hgraph IR callable '" + item.identity + "' has no syntax body adapter");
                }
                const ast::Signature &source = function(id).signature;
                if (item.parameters.size() != source.parameters.size() ||
                    has_planned_result(item.result, item.range) != (source.result != ast::no_node)) {
                    backend(item.range,
                            "hgraph IR callable '" + item.identity + "' disagrees with the syntax body adapter's signature shape");
                }
                for (std::size_t index = 0; index < item.parameters.size(); ++index) {
                    if (item.parameters[index].is_const != source.parameters[index].is_const) {
                        backend(item.range, "hgraph IR callable '" + item.identity +
                                                "' disagrees with the syntax body adapter's parameter roles");
                    }
                    if (item.parameters[index].default_value.valid() != (source.parameters[index].default_value != ast::no_node)) {
                        backend(item.range, "hgraph IR callable '" + item.identity +
                                                "' disagrees with the syntax body adapter's default shape");
                    }
                }
                if (!callables_.emplace(id, &item).second) {
                    backend(item.range, "more than one hgraph IR callable maps to the same syntax declaration");
                }
                callable_declarations_.push_back(id);
            }
            if (callable_declarations_.size() != resolved_.functions.size()) {
                backend(SourceRange{}, "the hgraph IR and syntax body adapter disagree on the module's callables");
            }

            const auto operator_for_range = [&](SourceRange range) {
                ast::DeclId match = ast::no_node;
                for (const ast::DeclId id : resolved_.operators) {
                    if (module_.decl(id).range != range) { continue; }
                    if (match != ast::no_node) {
                        backend(range, "hgraph IR operator range matches more than one syntax declaration");
                    }
                    match = id;
                }
                return match;
            };
            for (const gir::OperatorContract &item : graph_.operators) {
                if (item.imported) { continue; }
                const ast::DeclId id = operator_for_range(item.range);
                if (id == ast::no_node) {
                    backend(item.range, "hgraph IR operator '" + item.identity + "' has no syntax signature adapter");
                }
                if (!operators_.emplace(id, &item).second) {
                    backend(item.range, "more than one hgraph IR operator maps to the same syntax declaration");
                }
                operator_declarations_.push_back(id);
            }
            if (operator_declarations_.size() != resolved_.operators.size()) {
                backend(SourceRange{}, "the hgraph IR and syntax signature adapter disagree on the module's operators");
            }
        }

        // ------------------------------------------------------------ types

        const ast::GenericParameter &Emitter::generic_parameter(ast::DeclId decl, std::size_t index) const {
            const ast::DeclNode &node = module_.decl(decl).node;
            if (const auto *fn = std::get_if<ast::FunctionDecl>(&node)) { return fn->generics.at(index); }
            if (const auto *op = std::get_if<ast::OperatorDecl>(&node)) { return op->generics.at(index); }
            return std::get<ast::StructDecl>(node).generics.at(index);
        }

        const gir::Type &Emitter::graph_type(gir::TypeId id, SourceRange fallback) {
            if (!id.valid() || id.value >= graph_.types.size()) { backend(fallback, "hgraph IR contains an invalid type ID"); }
            return graph_.types[id.value];
        }

        const gir::ConstExpr &Emitter::graph_constant(gir::ConstExprId id, SourceRange fallback) {
            if (!id.valid() || id.value >= graph_.const_exprs.size()) {
                backend(fallback, "hgraph IR contains an invalid constant-expression ID");
            }
            return graph_.const_exprs[id.value];
        }

        bool Emitter::has_planned_result(gir::TypeId id, SourceRange fallback) {
            if (!id.valid()) { return false; }
            if (id.value >= graph_.types.size()) { backend(fallback, "hgraph IR contains an invalid result type ID"); }
            return graph_.types[id.value].kind != ir::hir::TypeKind::Void;
        }

        std::optional<std::int64_t> Emitter::planned_integer(gir::ConstExprId id, SourceRange fallback) {
            const gir::ConstExpr &expression = graph_constant(id, fallback);
            if (expression.kind != gir::ConstExprKind::Literal || !expression.literal) { return std::nullopt; }
            if (const auto *value = std::get_if<std::int64_t>(&*expression.literal)) { return *value; }
            return std::nullopt;
        }

        Value Emitter::planned_constant(gir::ConstExprId id, SourceRange fallback) {
            const gir::ConstExpr &expression = graph_constant(id, fallback);
            const SourceRange     range      = expression.range.end > expression.range.begin ? expression.range : fallback;
            if (expression.literal) {
                return std::visit(
                    [&](const auto &literal) -> Value {
                        using T = std::decay_t<decltype(literal)>;
                        if constexpr (std::is_same_v<T, ir::hir::NullValue>) {
                            unsupported(range, "'null' in a parameter default");
                        } else if constexpr (std::is_same_v<T, ir::hir::PlaceholderValue>) {
                            fail(Category::Type, range, "'_' is only valid in a harness sequence");
                        } else if constexpr (std::is_same_v<T, bool>) {
                            return make_const(literal ? "true" : "false", scalar_type(ast::ScalarType::Bool), range);
                        } else if constexpr (std::is_same_v<T, std::int64_t>) {
                            return make_const(integer_literal(literal), scalar_type(ast::ScalarType::I64), range, literal);
                        } else if constexpr (std::is_same_v<T, double>) {
                            return make_const("hgraph::Float{" + float_literal(literal) + "}", scalar_type(ast::ScalarType::F64),
                                              range, literal);
                        } else if constexpr (std::is_same_v<T, std::string>) {
                            return make_const("hgraph::Str{" + quote(literal) + "}", scalar_type(ast::ScalarType::Str), range);
                        } else if constexpr (std::is_same_v<T, syntax::TemporalValue>) {
                            if (std::optional<Value> value = temporal_constant(literal, range)) { return std::move(*value); }
                            backend(range, "zoned and civil literals are not supported by the first pass");
                        }
                    },
                    *expression.literal);
            }

            switch (expression.kind) {
                case gir::ConstExprKind::Unary:
                    {
                        const ast::UnaryOp op =
                            expression.unary == ir::hir::UnaryOp::Negate ? ast::UnaryOp::Negate : ast::UnaryOp::Not;
                        return fold_unary(op, planned_constant(expression.lhs, range), range);
                    }
                case gir::ConstExprKind::Binary:
                    {
                        ast::BinaryOp op;
                        switch (expression.binary) {
                            case ir::hir::BinaryOp::Mul: op = ast::BinaryOp::Mul; break;
                            case ir::hir::BinaryOp::Div: op = ast::BinaryOp::Div; break;
                            case ir::hir::BinaryOp::Rem: op = ast::BinaryOp::Rem; break;
                            case ir::hir::BinaryOp::Add: op = ast::BinaryOp::Add; break;
                            case ir::hir::BinaryOp::Sub: op = ast::BinaryOp::Sub; break;
                            case ir::hir::BinaryOp::Less: op = ast::BinaryOp::Less; break;
                            case ir::hir::BinaryOp::LessEqual: op = ast::BinaryOp::LessEqual; break;
                            case ir::hir::BinaryOp::Greater: op = ast::BinaryOp::Greater; break;
                            case ir::hir::BinaryOp::GreaterEqual: op = ast::BinaryOp::GreaterEqual; break;
                            case ir::hir::BinaryOp::Equal: op = ast::BinaryOp::Equal; break;
                            case ir::hir::BinaryOp::NotEqual: op = ast::BinaryOp::NotEqual; break;
                            case ir::hir::BinaryOp::And: op = ast::BinaryOp::And; break;
                            case ir::hir::BinaryOp::Or: op = ast::BinaryOp::Or; break;
                        }
                        return fold_binary(op, planned_constant(expression.lhs, range), planned_constant(expression.rhs, range),
                                           range);
                    }
                case gir::ConstExprKind::Parameter: unsupported(range, "a generic parameter in a parameter default");
                case gir::ConstExprKind::Index: unsupported(range, "an indexed parameter default");
                case gir::ConstExprKind::Field: unsupported(range, "a field-read parameter default");
                case gir::ConstExprKind::Sequence: unsupported(range, "a list or map parameter default");
                case gir::ConstExprKind::Tuple: unsupported(range, "a tuple parameter default");
                case gir::ConstExprKind::Construct: unsupported(range, "a struct parameter default");
                case gir::ConstExprKind::Literal: break;
            }
            backend(range, "hgraph IR contains an incomplete constant expression");
        }

        HType Emitter::planned_type(gir::TypeId id, SourceRange fallback) {
            const gir::Type  &type  = graph_type(id, fallback);
            const SourceRange range = type.range.end > type.range.begin ? type.range : fallback;
            using TypeKind          = ir::hir::TypeKind;
            switch (type.kind) {
                case TypeKind::Scalar:
                    {
                        using ScalarType = ir::hir::ScalarType;
                        switch (type.scalar) {
                            case ScalarType::Bool: return scalar_type(ast::ScalarType::Bool);
                            case ScalarType::I64: return scalar_type(ast::ScalarType::I64);
                            case ScalarType::F64: return scalar_type(ast::ScalarType::F64);
                            case ScalarType::Str: return scalar_type(ast::ScalarType::Str);
                            case ScalarType::Date: return scalar_type(ast::ScalarType::Date);
                            case ScalarType::Time: return scalar_type(ast::ScalarType::Time);
                            case ScalarType::DateTime: return scalar_type(ast::ScalarType::DateTime);
                            case ScalarType::Duration: return scalar_type(ast::ScalarType::Duration);
                            case ScalarType::CivilDateTime: return scalar_type(ast::ScalarType::CivilDateTime);
                            case ScalarType::ZonedDateTime: return scalar_type(ast::ScalarType::ZonedDateTime);
                            case ScalarType::ZonedTime: return scalar_type(ast::ScalarType::ZonedTime);
                            case ScalarType::TimeZone: return scalar_type(ast::ScalarType::TimeZone);
                        }
                        break;
                    }
                case TypeKind::Symbol:
                    {
                        if (type.binding.valid()) {
                            if (type.binding.value >= graph_.bindings.size()) {
                                backend(range, "hgraph IR type refers to an invalid generic binding");
                            }
                            const gir::Binding &binding = graph_.bindings[type.binding.value];
                            if (binding.kind != gir::BindingKind::TypeParameter) {
                                backend(range, "a non-type generic cannot be used as a value type");
                            }
                            HType result;
                            result.kind     = HType::Kind::Generic;
                            result.cpp_type = "hgraph::ScalarVar<" + quote(binding.name) + ">";
                            return result;
                        }
                        const auto contract =
                            std::find_if(graph_.structures.begin(), graph_.structures.end(),
                                         [&](const auto &candidate) { return candidate.identity == type.nominal_identity; });
                        if (contract == graph_.structures.end()) {
                            backend(range, "unknown hgraph IR nominal type '" + type.nominal_identity + "'");
                        }
                        HType result;
                        result.kind             = HType::Kind::Struct;
                        result.nominal_identity = type.nominal_identity;
                        result.cpp_type         = cpp_name(local_identity(type.nominal_identity));
                        std::vector<std::string> arguments;
                        arguments.reserve(type.arguments.size());
                        for (const gir::TypeArgument &argument : type.arguments) {
                            if (argument.type) {
                                HType child = planned_type(*argument.type, range);
                                arguments.push_back(value_type(child, range));
                                result.children.push_back(std::move(child));
                            } else if (argument.value) {
                                const std::optional<std::int64_t> value = planned_integer(*argument.value, range);
                                if (!value) { backend(range, "a generated const struct argument must be an i64 literal"); }
                                arguments.push_back(std::to_string(*value));
                            } else {
                                backend(range, "an unresolved hgraph IR struct type argument");
                            }
                        }
                        if (!arguments.empty()) { result.cpp_type += "<" + join(arguments, ", ") + ">"; }
                        return result;
                    }
                case TypeKind::Tuple:
                    {
                        HType result;
                        result.kind = HType::Kind::Tuple;
                        for (gir::TypeId child : type.children) { result.children.push_back(planned_type(child, range)); }
                        return result;
                    }
                case TypeKind::List:
                    {
                        if (type.children.size() != 1U) { backend(range, "hgraph IR list type requires one element type"); }
                        HType result;
                        result.kind = HType::Kind::List;
                        result.children.push_back(planned_type(type.children.front(), range));
                        if (type.size.valid()) {
                            const std::optional<std::int64_t> size = planned_integer(type.size, range);
                            if (!size || *size <= 0) {
                                fail(Category::Type, range, "a fixed list size must be a positive i64 literal");
                            }
                            result.size = std::to_string(*size);
                        }
                        return result;
                    }
                case TypeKind::Set:
                    {
                        if (type.children.size() != 1U) { backend(range, "hgraph IR set type requires one element type"); }
                        HType result;
                        result.kind = HType::Kind::Set;
                        result.children.push_back(planned_type(type.children.front(), range));
                        return result;
                    }
                case TypeKind::Map:
                    {
                        if (type.children.size() != 2U) { backend(range, "hgraph IR map type requires key and value types"); }
                        HType result;
                        result.kind = HType::Kind::Map;
                        result.children.push_back(planned_type(type.children[0], range));
                        result.children.push_back(planned_type(type.children[1], range));
                        return result;
                    }
                case TypeKind::Rolling:
                    {
                        if (type.children.size() != 1U) { backend(range, "hgraph IR rolling type requires one element type"); }
                        HType result;
                        result.kind = HType::Kind::Rolling;
                        result.children.push_back(planned_type(type.children.front(), range));
                        if (!type.size.valid()) { return result; }
                        const gir::ConstExpr &maximum = graph_constant(type.size, range);
                        if (maximum.kind != gir::ConstExprKind::Literal || !maximum.literal) {
                            // A symbolic size is a type relationship, not a C++
                            // non-type template parameter on the operator marker.
                            return result;
                        }
                        if (const auto *size = std::get_if<std::int64_t>(&*maximum.literal)) {
                            if (*size <= 0) {
                                fail(Category::Type, range, "a rolling size is a positive i64 constant or a duration");
                            }
                            result.size                               = std::to_string(*size);
                            const std::optional<std::int64_t> minimum = planned_integer(type.min_size, range);
                            if (!minimum || *minimum < 0 || *minimum > *size) {
                                fail(Category::Type, range,
                                     "rolling sizes require a positive maximum and a non-negative minimum no larger than it");
                            }
                            result.min_size = std::to_string(*minimum);
                            return result;
                        }
                        if (const auto *size = std::get_if<syntax::TemporalValue>(&*maximum.literal);
                            size != nullptr && size->kind == syntax::TemporalKind::Duration) {
                            const gir::ConstExpr &minimum = graph_constant(type.min_size, range);
                            const auto           *minimum_value =
                                minimum.literal ? std::get_if<syntax::TemporalValue>(&*minimum.literal) : nullptr;
                            if (minimum.kind != gir::ConstExprKind::Literal || minimum_value == nullptr ||
                                minimum_value->kind != syntax::TemporalKind::Duration) {
                                fail(Category::Type, range, "a duration rolling minimum must be a duration literal");
                            }
                            if (size->micros <= 0 || minimum_value->micros < 0 || minimum_value->micros > size->micros) {
                                fail(Category::Type, range,
                                     "rolling durations require a positive maximum and a non-negative minimum no larger than it");
                            }
                            result.duration_window = true;
                            result.size            = std::to_string(size->micros);
                            result.min_size        = std::to_string(minimum_value->micros);
                            return result;
                        }
                        fail(Category::Type, range, "a rolling size is a positive i64 constant or a duration");
                    }
                case TypeKind::Atomic:
                    {
                        if (type.children.size() != 1U) { backend(range, "hgraph IR atomic type requires one value type"); }
                        HType result;
                        result.kind = HType::Kind::Atomic;
                        result.children.push_back(planned_type(type.children.front(), range));
                        return result;
                    }
                case TypeKind::Void:
                case TypeKind::Iterator:
                case TypeKind::Callable:
                case TypeKind::Capability:
                case TypeKind::HarnessSequence:
                case TypeKind::Deferred: break;
            }
            backend(range, "unsupported hgraph IR interface type");
        }

        std::string Emitter::size_text(ast::ExprId id, Frame &frame, std::string_view what)
        {
            const Value value = eval_expr(id, frame);
            const auto  size  = integer_value(value);
            if (!value.is_const() || !value.type.is(ast::ScalarType::I64) || !size || *size < 0)
            {
                fail(Category::Type, module_.expr(id).range, std::string{what} + " must be a non-negative i64 constant");
            }
            return std::to_string(*size);
        }

        HType Emitter::type_of(ast::TypeId id, Frame &frame)
        {
            const ast::Type &type = module_.type(id);
            switch (type.kind)
            {
                case ast::TypeKind::Scalar: return scalar_type(type.scalar);
                case ast::TypeKind::Named: {
                    const semantics::Binding &binding = resolved_.type_binding(id);
                    if (binding.kind == BindingKind::Generic) {
                        if (const auto found = frame.generic_types.find(binding.index); found != frame.generic_types.end()) {
                            return found->second;
                        }
                        const ast::GenericParameter &parameter = generic_parameter(binding.decl, binding.index);
                        if (parameter.is_const) { backend(type.range, "a const generic cannot be used as a value type"); }
                        HType result;
                        result.kind     = HType::Kind::Generic;
                        result.cpp_type = "hgraph::ScalarVar<" + quote(parameter.name.text) + ">";
                        return result;
                    }
                    if (binding.kind == BindingKind::Struct) {
                        const ast::StructDecl &decl = structure(binding.decl);
                        HType                  result;
                        result.kind             = HType::Kind::Struct;
                        result.declaration      = binding.decl;
                        result.nominal_identity = resolved_.module_path + "." + std::string{decl.name.text};
                        result.cpp_type         = cpp_name(decl.name.text);

                        std::vector<std::string> arguments;
                        arguments.reserve(type.arguments.size());
                        for (const ast::GenericArgument &argument : type.arguments) {
                            if (argument.type != ast::no_node) {
                                HType child = type_of(argument.type, frame);
                                arguments.push_back(value_type(child, module_.type(argument.type).range));
                                result.children.push_back(std::move(child));
                            } else if (argument.value != ast::no_node) {
                                const Value value   = eval_expr(argument.value, frame);
                                const auto  integer = integer_value(value);
                                if (!integer) {
                                    backend(argument.range, "a generated const struct argument must be an i64 literal");
                                }
                                arguments.push_back(std::to_string(*integer));
                            } else {
                                backend(argument.range, "an unresolved struct type argument");
                            }
                        }
                        if (!arguments.empty()) { result.cpp_type += "<" + join(arguments, ", ") + ">"; }
                        return result;
                    }
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
                    const semantics::Binding &size_binding = resolved_.binding(type.size);
                    if (size_binding.kind == BindingKind::Generic) {
                        // The current hgraph window wildcard preserves the
                        // scalar type while the concrete period remains bound
                        // by the actual input schema at wiring time.
                        return result;
                    }
                    const Value size = eval_expr(type.size, frame);
                    if (size.is_const() && size.type.is(ast::ScalarType::Duration)) {
                        result.duration_window = true;
                        const auto *literal    = std::get_if<ast::TemporalLiteral>(&module_.expr(type.size).node);
                        if (literal == nullptr) {
                            fail(Category::Type, module_.expr(type.size).range,
                                 "a generated duration rolling size must be a duration literal");
                        }
                        result.size = std::to_string(literal->value.micros);
                        if (type.min_size == ast::no_node) {
                            result.min_size = result.size;
                        } else {
                            const auto *minimum = std::get_if<ast::TemporalLiteral>(&module_.expr(type.min_size).node);
                            if (minimum == nullptr || minimum->value.kind != syntax::TemporalKind::Duration) {
                                fail(Category::Type, module_.expr(type.min_size).range,
                                     "a duration rolling minimum must be a duration literal");
                            }
                            result.min_size = std::to_string(minimum->value.micros);
                        }
                        return result;
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
                    result.size     = std::to_string(*max_size);
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
                case HType::Kind::Generic: return type.cpp_type;
                case HType::Kind::Struct: return "typename " + type.cpp_type + "::value_type";
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
                    if (type.size.empty()) { return "hgraph::TSWAny<" + value_type(type.children[0], range) + ">"; }
                    if (type.duration_window) {
                        return "hgraph::TSWDuration<" + value_type(type.children[0], range) + ", " + type.size + ", " +
                               type.min_size + ">";
                    }
                    return "hgraph::TSW<" + value_type(type.children[0], range) + ", " + type.size + ", " + type.min_size + ">";
                case HType::Kind::Struct: return "typename " + type.cpp_type + "::time_series";
                case HType::Kind::Generic: return "hgraph::TS<" + value_type(type, range) + ">";
                case HType::Kind::Unknown: break;
            }
            backend(range, "this value has no time-series schema");
        }

        // ------------------------------------------------------------ values

        Value make_const(std::string code, HType type, SourceRange range,
                         std::variant<std::monostate, std::int64_t, double> number) {
            Value value;
            value.kind  = Value::Kind::Const;
            value.code  = std::move(code);
            value.type  = std::move(type);
            value.range = range;
            value.number = std::move(number);
            return value;
        }

        std::optional<Value> temporal_constant(syntax::TemporalValue literal, SourceRange range) {
            const std::string micros = literal.micros == std::numeric_limits<std::int64_t>::min()
                                           ? "std::numeric_limits<hgraph::TimeDelta::rep>::min()"
                                           : std::to_string(literal.micros);
            switch (literal.kind) {
                case syntax::TemporalKind::Date:
                    return make_const("hgraph::Date{std::chrono::sys_days{std::chrono::days{" + micros + "}}}",
                                      scalar_type(ast::ScalarType::Date), range);
                case syntax::TemporalKind::Time:
                    return make_const("hgraph::Time{" + micros + "}", scalar_type(ast::ScalarType::Time), range);
                case syntax::TemporalKind::DateTime:
                    return make_const("hgraph::DateTime{std::chrono::microseconds{" + micros + "}}",
                                      scalar_type(ast::ScalarType::DateTime), range);
                case syntax::TemporalKind::Duration:
                    return make_const("hgraph::TimeDelta{" + micros + "}", scalar_type(ast::ScalarType::Duration), range);
                case syntax::TemporalKind::CivilDateTime:
                case syntax::TemporalKind::ZonedDateTime:
                case syntax::TemporalKind::ZonedTime:
                case syntax::TemporalKind::TimeZone: return std::nullopt;
            }
            return std::nullopt;
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
                case Value::Kind::Iterator: backend(value.range, "a runtime iterator is only valid as the source of a 'for' loop");
                case Value::Kind::Function:
                case Value::Kind::Struct:
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
            if (temporal.kind == HType::Kind::Atomic && !value.atomic_code.empty() && same_type(value.type, temporal.children[0])) {
                return value.atomic_code;
            }
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
            HType result;
            switch (op) {
                case ast::BinaryOp::Equal:
                case ast::BinaryOp::NotEqual:
                case ast::BinaryOp::Less:
                case ast::BinaryOp::LessEqual:
                case ast::BinaryOp::Greater:
                case ast::BinaryOp::GreaterEqual:
                case ast::BinaryOp::And:
                case ast::BinaryOp::Or: result = scalar_type(ast::ScalarType::Bool); break;
                case ast::BinaryOp::Add:
                case ast::BinaryOp::Sub:
                case ast::BinaryOp::Mul:
                case ast::BinaryOp::Div:
                case ast::BinaryOp::Rem:
                    if (lhs.type.numeric() && rhs.type.numeric()) {
                        result = scalar_type(lhs.type.is(ast::ScalarType::F64) || rhs.type.is(ast::ScalarType::F64)
                                                 ? ast::ScalarType::F64
                                                 : ast::ScalarType::I64);
                    }
                    break;
            }
            Value value = wire(std::string{"hgraph::stdlib::"} + name, {argument_code(lhs), argument_code(rhs)}, range, result);
            if (result.kind != HType::Kind::Unknown) { value.code += ".as<" + schema(result, range) + ">()"; }
            return value;
        }

        // -------------------------------------------------------- expressions

        Value Emitter::eval_name(ast::ExprId id, Frame &frame)
        {
            const semantics::Binding &binding = resolved_.binding(id);
            const SourceRange         range   = module_.expr(id).range;
            switch (binding.kind)
            {
                case BindingKind::Local: {
                        const auto &locals = binding.second ? frame.second_locals : frame.locals;
                        const auto  found  = locals.find(binding.stmt);
                        if (found == locals.end()) {
                            const auto injected = frame.injects.find(slice(range));
                            if (injected == frame.injects.end()) {
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
                        if (binding.decl == ast::no_node) {
                            if (binding.stmt != frame.anonymous || binding.index >= frame.anonymous_params.size()) {
                                backend(range, "'" + slice(range) + "' is not bound in this anonymous function");
                            }
                            Value value = frame.anonymous_params[binding.index];
                            value.range = range;
                            return value;
                        }
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
                case BindingKind::Struct:
                    {
                        Value value;
                        value.kind  = Value::Kind::Struct;
                        value.decl  = binding.decl;
                        value.range = range;
                        return value;
                    }
                case BindingKind::Function:
                    {
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
                    const gir::OperatorContract &op = operator_decl(binding.decl);
                    Value                        value;
                    value.kind  = Value::Kind::LocalOperator;
                    value.decl  = binding.decl;
                    value.name  = "operators::" + cpp_name(local_identity(op.identity));
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
                        return make_const(integer_literal(node.value), scalar_type(ast::ScalarType::I64), expr.range,
                                          static_cast<std::int64_t>(node.value));
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
                        if (std::optional<Value> value = temporal_constant(node.value, expr.range)) { return std::move(*value); }
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
                        if (target.kind == Value::Kind::Intrinsic && target.name == "logger") {
                            Value value;
                            value.kind  = Value::Kind::Intrinsic;
                            value.name  = "logger." + std::string{node.field.text};
                            value.range = expr.range;
                            return value;
                        }
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
                    } else if constexpr (std::is_same_v<T, ast::Construct>) {
                        const semantics::Binding &binding = resolved_.type_binding(node.type);
                        return eval_construct(binding.decl, node.type, node.arguments, node.delta, expr.range, frame);
                    } else {
                        static_assert(sizeof(T) == 0, "unhandled expression node");
                    }
                },
                expr.node);
        }

        // ------------------------------------------------------------- calls

        Value Emitter::eval_call(const ast::Call &call, SourceRange range, Frame &frame)
        {
            const Value callee = eval_expr(call.callee, frame);
            if (frame.runtime && callee.kind != Value::Kind::Intrinsic && callee.kind != Value::Kind::Struct) {
                backend(range, "calls in a runtime function are not supported by emit-cpp yet");
            }
            switch (callee.kind)
            {
                case Value::Kind::Operator:
                case Value::Kind::LocalOperator: {
                        if (callee.name == "hgraph::stdlib::map_" &&
                            std::ranges::any_of(call.arguments, [&](const ast::Argument &argument) {
                                return std::holds_alternative<ast::AnonymousFn>(module_.expr(argument.value).node);
                            })) {
                            return lower_map_call(callee, call, range, frame);
                        }
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
                case Value::Kind::Struct: return eval_construct(callee.decl, ast::no_node, call.arguments, false, range, frame);
                case Value::Kind::Function: return call_function(callee.decl, call.arguments, range, frame);
                case Value::Kind::Intrinsic: return eval_intrinsic(callee, call, range, frame);
                case Value::Kind::Const:
                case Value::Kind::Port:
                case Value::Kind::Runtime:
                case Value::Kind::Iterator:
                case Value::Kind::Void:
                    break;
            }
            fail(Category::Type, module_.expr(call.callee).range,
                 "'" + slice(module_.expr(call.callee).range) + "' is not callable");
        }

        Value Emitter::lower_map_call(const Value &callee, const ast::Call &call, SourceRange range, Frame &frame) {
            ast::ExprId        anonymous_id = ast::no_node;
            std::vector<Value> inputs;
            for (const ast::Argument &argument : call.arguments) {
                if (std::holds_alternative<ast::AnonymousFn>(module_.expr(argument.value).node)) {
                    if (anonymous_id != ast::no_node) {
                        backend(module_.expr(argument.value).range, "map takes one anonymous function");
                    }
                    anonymous_id = argument.value;
                } else {
                    inputs.push_back(eval_expr(argument.value, frame));
                }
            }
            const auto &anonymous = std::get<ast::AnonymousFn>(module_.expr(anonymous_id).node);
            if (anonymous.parameters.size() != inputs.size()) {
                fail(Category::Type, module_.expr(anonymous_id).range,
                     "the map function parameter count must match its mapped inputs");
            }

            Frame lambda;
            lambda.fn = ast::no_node;
            lambda.params.resize(inputs.size());
            std::vector<std::string> parameters{"hgraph::Wiring &w"};
            for (std::size_t index = 0; index < inputs.size(); ++index) {
                const Value &input = inputs[index];
                if (!input.is_port() || input.type.kind != HType::Kind::Map) {
                    backend(input.range, "the first anonymous map slice takes temporal map inputs");
                }
                HType parameter_type = input.type.children[1];
                if (anonymous.parameters[index].type != ast::no_node) {
                    parameter_type = type_of(anonymous.parameters[index].type, lambda);
                }
                const std::string name = cpp_name(anonymous.parameters[index].name.text);
                lambda.params[index]   = make_port(name, parameter_type, anonymous.parameters[index].name.range);
                parameters.push_back("hgraph::Port<" + schema(parameter_type, anonymous.parameters[index].name.range) + "> " +
                                     name);
            }
            lambda.anonymous        = anonymous_id;
            lambda.anonymous_params = lambda.params;

            const Value lambda_result = eval_expr(anonymous.body, lambda);
            HType       result_type   = lambda_result.type;
            if (anonymous.result != ast::no_node) { result_type = type_of(anonymous.result, lambda); }
            if (result_type.kind == HType::Kind::Unknown) {
                backend(module_.expr(anonymous.body).range, "the anonymous map result type cannot be inferred");
            }

            const std::string helper = "hgl_anonymous_" + std::to_string(++anonymous_function_index_);
            generated_helpers_.line("// " + where(module_.expr(anonymous_id).range));
            generated_helpers_.open("struct " + helper);
            generated_helpers_.line("static constexpr auto name = " +
                                    quote(module_name_ + ".<anonymous:" + std::to_string(anonymous_function_index_) + ">") + ";");
            generated_helpers_.line("static hgraph::Port<" + schema(result_type, module_.expr(anonymous.body).range) +
                                    "> compose(" + join(parameters, ", ") + ")");
            generated_helpers_.open("");
            generated_helpers_.line("return " + as_port(lambda_result, result_type, module_.expr(anonymous.body).range) + ";");
            generated_helpers_.close();
            generated_helpers_.close(";");
            generated_helpers_.line();

            std::vector<std::string> args{"hgraph::fn<" + helper + ">()"};
            for (const Value &input : inputs) { args.push_back(argument_code(input)); }

            HType mapped;
            mapped.kind = HType::Kind::Map;
            mapped.children.push_back(inputs.front().type.children[0]);
            mapped.children.push_back(result_type);
            Value result = wire(callee.name, args, range, mapped);
            result.code += ".as<" + schema(mapped, range) + ">()";
            return result;
        }

        Value Emitter::eval_construct(ast::DeclId decl, ast::TypeId type_id, const std::vector<ast::Argument> &arguments,
                                      bool delta, SourceRange range, Frame &frame) {
            const ast::StructDecl       &item = structure(decl);
            const semantics::StructInfo &info = resolved_.structure(decl);
            HType                        type;
            if (type_id != ast::no_node) {
                type = type_of(type_id, frame);
            } else {
                if (!item.generics.empty()) {
                    backend(range, "generic struct '" + std::string{item.name.text} + "' needs explicit type arguments");
                }
                type.kind             = HType::Kind::Struct;
                type.declaration      = decl;
                type.nominal_identity = resolved_.module_path + "." + std::string{item.name.text};
                type.cpp_type         = cpp_name(item.name.text);
            }

            Frame       struct_frame  = frame;
            std::size_t type_argument = 0;
            for (std::size_t generic = 0; generic < item.generics.size(); ++generic) {
                if (item.generics[generic].is_const) { continue; }
                if (type_argument < type.children.size()) { struct_frame.generic_types[generic] = type.children[type_argument++]; }
            }

            const auto argument_for = [&](std::string_view field) -> ast::ExprId {
                for (const ast::Argument &argument : arguments) {
                    if (argument.name.text == field) { return argument.value; }
                }
                return ast::no_node;
            };

            std::vector<std::string>                   temporal_fields;
            std::vector<std::pair<std::size_t, Value>> delta_fields;
            for (std::size_t index = 0; index < info.fields.size(); ++index) {
                const semantics::StructField &field    = info.fields[index];
                ast::ExprId                   value_id = argument_for(field.name);
                if (value_id == ast::no_node && !delta) { value_id = field.default_value; }
                const HType field_type = type_of(field.type, struct_frame);

                if (value_id == ast::no_node) {
                    if (delta) { continue; }
                    temporal_fields.push_back("hgraph::wire<hgraph::stdlib::nothing, " +
                                              schema(field_type, module_.type(field.type).range) + ">(w)");
                    continue;
                }
                if (std::holds_alternative<ast::NullLiteral>(module_.expr(value_id).node)) {
                    if (delta) {
                        backend(module_.expr(value_id).range, "clearing an optional struct field needs a native clear-delta "
                                                              "operation");
                    }
                    temporal_fields.push_back("hgraph::wire<hgraph::stdlib::nothing, " +
                                              schema(field_type, module_.type(field.type).range) + ">(w)");
                    continue;
                }

                Value value = eval_expr(value_id, frame);
                if (delta) {
                    if (!frame.runtime || (!value.is_const() && !value.is_runtime())) {
                        backend(module_.expr(value_id).range, "a structured delta is only available in a runtime function");
                    }
                    value.code = as_runtime(value, field_type, value.range, "field '" + field.name + "'");
                    value.type = field_type;
                    delta_fields.emplace_back(index, std::move(value));
                } else {
                    temporal_fields.push_back(as_port(value, field_type, value.range));
                }
            }

            if (delta) {
                std::string code =
                    "[&]() { hgraph::BundleBuilder builder{hgraph::delta_value_binding<" + schema(type, range) + ">()}; ";
                for (const auto &[index, value] : delta_fields) {
                    code += "builder.set(" + std::to_string(index) + ", hgraph::Value{" + value.code + "}); ";
                }
                code += "return builder.build(); }()";
                Value result            = make_runtime(std::move(code), type, range);
                result.structured_delta = true;
                return result;
            }

            Value result = make_port("hgraph::stdlib::to_tsb<" + schema(type, range) + ">(w" +
                                         (temporal_fields.empty() ? std::string{} : ", " + join(temporal_fields, ", ")) + ")",
                                     type, range);
            result.atomic_code =
                "hgraph::wire<hgraph::stdlib::combine_cs, hgraph::TS<" + value_type(type, range) + ">>(w, " + result.code + ")";
            return result;
        }

        Value Emitter::eval_intrinsic(const Value &callee, const ast::Call &call, SourceRange range, Frame &frame)
        {
            const std::string &name = callee.name;
            if (name.starts_with("logger.")) {
                if (!frame.runtime) { fail(Category::Phase, range, "logger methods are only available in runtime hooks"); }
                if (name != "logger.info") { unsupported(range, "logger method '" + name.substr(7) + "'"); }
                if (call.arguments.size() != 1) {
                    fail(Category::Type, range, "'logger.info' takes one message in the first slice");
                }
                const Value message = eval_expr(call.arguments.front().value, frame);
                if ((!message.is_const() && !message.is_runtime()) || !message.type.is(ast::ScalarType::Str)) {
                    fail(Category::Type, message.range, "'logger.info' takes a str message");
                }
                Value result;
                result.kind  = Value::Kind::Void;
                result.code  = "logger.log(2, " + message.code + ")";
                result.range = range;
                return result;
            }
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
            if (name == "keys" || name == "values" || name == "items") {
                if (!frame.runtime) {
                    backend(range, "'" + name +
                                       "' is a runtime traversal; it is not available in a "
                                       "composition body");
                }
                if (call.arguments.empty() || call.arguments.size() > 2) {
                    fail(Category::Type, range, "'" + name + "' takes a collection and an optional predicate");
                }
                const Value source = eval_expr(call.arguments.front().value, frame);
                if (!source.is_runtime() || source.selector.empty()) {
                    fail(Category::Type, source.range, "'" + name + "' takes a runtime collection selector");
                }

                std::string predicate;
                ast::ExprId general_predicate = ast::no_node;
                if (call.arguments.size() == 2) {
                    const ast::ExprId predicate_id   = call.arguments[1].value;
                    const ast::Expr  &predicate_expr = module_.expr(predicate_id);
                    if (std::holds_alternative<ast::AnonymousFn>(predicate_expr.node)) {
                        general_predicate = predicate_id;
                    } else {
                        const Value value = eval_expr(predicate_id, frame);
                        if (value.kind != Value::Kind::Intrinsic || (value.name != "valid" && value.name != "modified" &&
                                                                     value.name != "added" && value.name != "removed")) {
                            fail(Category::Type, predicate_expr.range,
                                 "an iterator predicate is a metadata predicate or concise fn");
                        }
                        predicate = value.name;
                    }
                }

                std::string method = name;
                if (!predicate.empty()) {
                    if (source.type.kind == HType::Kind::Set && name == "values") {
                        method = predicate == "added" ? "added" : predicate == "removed" ? "removed" : name;
                    } else {
                        method = predicate + "_" + name;
                    }
                }

                Value result;
                result.kind               = Value::Kind::Iterator;
                result.code               = source.selector + "." + method + "()";
                result.type               = source.type;
                result.name               = name;
                result.range              = range;
                result.iterator_predicate = general_predicate;
                if (source.type.kind == HType::Kind::Map) {
                    if (name == "keys") {
                        result.iterator_types.push_back(source.type.children[0]);
                    } else if (name == "values") {
                        result.iterator_types.push_back(source.type.children[1]);
                    } else {
                        result.iterator_types = {source.type.children[0], source.type.children[1]};
                    }
                } else if (source.type.kind == HType::Kind::Set && name == "values") {
                    result.iterator_types.push_back(source.type.children[0]);
                } else if (source.type.kind == HType::Kind::List) {
                    if (name == "values") {
                        result.iterator_types.push_back(source.type.children[0]);
                    } else {
                        result.iterator_types = {scalar_type(ast::ScalarType::I64), source.type.children[0]};
                    }
                } else {
                    backend(source.range, "this collection does not support '" + name + "'");
                }
                return result;
            }
            backend(range, "'" + name +
                               "' is a runtime traversal; it is not available in a composition body of the first pass");
        }

        std::vector<ast::ExprId> Emitter::bind_arguments(ast::DeclId decl, const std::vector<ast::Argument> &arguments,
                                                         SourceRange range) {
            const gir::Callable     &fn     = callable(decl);
            const auto              &params = fn.parameters;
            std::vector<ast::ExprId> bound(params.size(), ast::no_node);
            std::size_t              next = 0;
            for (const ast::Argument &argument : arguments)
            {
                const SourceRange at = module_.expr(argument.value).range;
                if (argument.name.empty())
                {
                    if (next >= params.size())
                    {
                        fail(Category::Type, at,
                             "'" + std::string{callable_name(decl)} + "' takes " + std::to_string(params.size()) + " arguments");
                    }
                    if (bound[next] != ast::no_node) { fail(Category::Type, at, "positional argument after a named one"); }
                    bound[next++] = argument.value;
                    continue;
                }
                const auto found = std::find_if(params.begin(), params.end(),
                                                [&](const gir::Parameter &param) { return param.name == argument.name.text; });
                if (found == params.end())
                {
                    fail(Category::Name, argument.name.range,
                         "'" + std::string{callable_name(decl)} + "' has no parameter named '" + std::string{argument.name.text} +
                             "'");
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
                if (bound[i] == ast::no_node && !params[i].default_value.valid()) {
                    fail(Category::Type, range,
                         "'" + std::string{callable_name(decl)} + "' needs an argument for '" + params[i].name + "'");
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
            const gir::Callable           &planned = callable(decl);
            const std::vector<ast::ExprId> bound   = bind_arguments(decl, arguments, range);
            std::vector<std::string>       args(planned.parameters.size());
            for (std::size_t i = 0; i < planned.parameters.size(); ++i) {
                const gir::Parameter &param = planned.parameters[i];
                Value                 arg =
                    bound[i] != ast::no_node ? eval_expr(bound[i], frame) : planned_constant(param.default_value, planned.range);
                const HType type = planned_type(param.type, planned.range);
                if (param.is_const)
                {
                    args[i] = as_const(arg, type, arg.range, "parameter '" + param.name + "'");
                }
                else { args[i] = as_port(arg, type, arg.range); }
            }
            HType result;
            if (has_planned_result(planned.result, planned.range)) { result = planned_type(planned.result, planned.range); }
            Value value = wire(callable_cpp_name(decl), args, range, result);
            if (!has_planned_result(planned.result, planned.range)) { value.kind = Value::Kind::Void; }
            return value;
        }

        // -------------------------------------------------------- statements

        void Emitter::emit_return(const Value &value, Frame &frame, Writer &out, SourceRange range)
        {
            const ast::FunctionDecl &fn      = function(frame.fn);
            const gir::Callable     &planned = callable(frame.fn);
            if (!has_planned_result(planned.result, planned.range)) {
                if (value.kind != Value::Kind::Void) { fail(Category::Type, range, "'" + std::string{fn.name.text} + "' has no result"); }
                if (!value.code.empty()) { out.line(value.code + ";"); }
                out.line("return;");
                return;
            }
            const HType result = planned_type(planned.result, planned.range);
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
            if (function_body && function(frame.fn).signature.result != ast::no_node && block.tail == ast::no_node &&
                !block_terminates(id))
            {
                // Every path must return: a body that ends after a `return`
                // inside `if` still needs a terminating statement for C++.
                out.line("throw std::logic_error(\"" + std::string{function(frame.fn).name.text} +
                         ": reached the end of the body without a result\");");
            }
        }

        bool Emitter::expression_terminates(ast::ExprId id) const
        {
            const ast::Expr &expr = module_.expr(id);
            if (const auto *block = std::get_if<ast::BlockExpr>(&expr.node)) { return block_terminates(block->block); }
            const auto *branch = std::get_if<ast::If>(&expr.node);
            return branch != nullptr && block_terminates(branch->then_block) && branch->otherwise != ast::no_node &&
                   expression_terminates(branch->otherwise);
        }

        bool Emitter::block_terminates(ast::BlockId id) const
        {
            const ast::Block &block = module_.block(id);
            if (block.tail != ast::no_node) { return true; }
            if (block.statements.empty()) { return false; }
            const ast::StmtNode &last = module_.stmt(block.statements.back()).node;
            if (std::holds_alternative<ast::ReturnStmt>(last)) { return true; }
            if (const auto *expression = std::get_if<ast::ExprStmt>(&last))
            {
                return expression_terminates(expression->expr);
            }
            return false;
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
                        if (const auto *index = std::get_if<ast::Index>(&place.node);
                            index != nullptr && slice(module_.expr(index->target).range) == "out") {
                            const gir::Callable &planned = callable(frame.fn);
                            if (!frame.output_available || !has_planned_result(planned.result, planned.range)) {
                                fail(Category::Phase, place.range, "'out' is not available in this lifecycle block");
                            }
                            const HType result = planned_type(planned.result, planned.range);
                            if (result.kind != HType::Kind::Map || result.children.size() != 2) {
                                backend(place.range, "indexed output assignment currently requires a map result");
                            }
                            if (node.op != ast::AssignOp::Assign) {
                                unsupported(stmt.range, "compound assignment to an output collection child");
                            }
                            const Value key   = eval_expr(index->index, frame);
                            const Value value = eval_expr(node.value, frame);
                            out.line("hgl_output.set(" + as_runtime(key, result.children[0], key.range, "output key") + ", " +
                                     as_runtime(value, result.children[1], value.range, "output value") + ");");
                            return;
                        }
                        if (!std::holds_alternative<ast::NameRef>(place.node) ||
                            resolved_.binding(node.place).kind != BindingKind::Local) {
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
                            const ast::FunctionDecl &fn      = function(frame.fn);
                            const gir::Callable     &planned = callable(frame.fn);
                            if (!has_planned_result(planned.result, planned.range)) {
                                fail(Category::Type, stmt.range, "'" + std::string{fn.name.text} + "' has no result");
                            }
                            const HType result = planned_type(planned.result, planned.range);
                            const Value value  = eval_expr(node.value, frame);
                            if (value.structured_delta) {
                                if (result.kind != HType::Kind::Struct || !same_type(value.type, result)) {
                                    fail(Category::Type, value.range, "the structured delta does not match the result type");
                                }
                                out.line("hgraph::apply_delta(hgl_output.base(), " + value.code + ".view());");
                            } else {
                                out.line("hgl_output.set(" + as_runtime(value, result, value.range, "return value") + ");");
                            }
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
                        const Value iterator = eval_expr(node.iterable, frame);
                        if (!iterator.is_iterator()) {
                            fail(Category::Type, module_.expr(node.iterable).range,
                                 "a runtime 'for' loop needs keys(...), values(...), or "
                                 "items(...)");
                        }
                        const bool pair = !node.second.empty();
                        if (iterator.iterator_types.size() != (pair ? 2U : 1U)) {
                            fail(Category::Type, stmt.range,
                                 pair ? "this iterator yields one value" : "this iterator yields a pair");
                        }

                        const std::string first_raw  = "hgl_" + cpp_name(node.first.text) + "_item";
                        const std::string second_raw = "hgl_" + cpp_name(node.second.text) + "_item";
                        out.open(pair ? "for (const auto &[" + first_raw + ", " + second_raw + "] : " + iterator.code + ")"
                                      : "for (const auto &" + first_raw + " : " + iterator.code + ")");

                        const auto bind_value = [&](const std::string &raw, const HType &type, bool endpoint, bool list_index,
                                                    bool map_key) {
                            Value value;
                            value.kind  = Value::Kind::Runtime;
                            value.type  = type;
                            value.range = stmt.range;
                            if (endpoint) {
                                value.selector = raw;
                                value.code     = iterator.type.kind == HType::Kind::List
                                                     ? raw + ".value().checked_as<" + value_type(type, stmt.range) + ">()"
                                                     : raw + ".value()";
                            } else if (list_index) {
                                value.code = "static_cast<hgraph::Int>(" + raw + ")";
                            } else if (map_key) {
                                value.code = raw + ".checked_as<" + value_type(type, stmt.range) + ">()";
                            } else {
                                value.code = raw;
                            }
                            return value;
                        };

                        const bool map  = iterator.type.kind == HType::Kind::Map;
                        const bool list = iterator.type.kind == HType::Kind::List;
                        Value      first =
                            bind_value(first_raw, iterator.iterator_types[0], !pair && (map || list) && iterator.name == "values",
                                       pair && list, map && (pair || iterator.name == "keys"));
                        frame.locals[id] = first;
                        if (pair) {
                            frame.second_locals[id] = bind_value(second_raw, iterator.iterator_types[1], true, false, false);
                        }

                        bool predicate_scope = false;
                        if (iterator.iterator_predicate != ast::no_node) {
                            const auto &predicate = std::get<ast::AnonymousFn>(module_.expr(iterator.iterator_predicate).node);
                            Frame       predicate_frame = frame;
                            predicate_frame.anonymous   = iterator.iterator_predicate;
                            predicate_frame.anonymous_params =
                                pair ? std::vector<Value>{frame.locals.at(id), frame.second_locals.at(id)}
                                     : std::vector<Value>{frame.locals.at(id)};
                            if (predicate.parameters.size() != predicate_frame.anonymous_params.size()) {
                                fail(Category::Type, module_.expr(iterator.iterator_predicate).range,
                                     "the iterator predicate parameter count must match its "
                                     "values");
                            }
                            const Value condition = eval_expr(predicate.body, predicate_frame);
                            if ((!condition.is_const() && !condition.is_runtime()) || !condition.type.is(ast::ScalarType::Bool)) {
                                fail(Category::Type, module_.expr(predicate.body).range, "an iterator predicate returns bool");
                            }
                            out.open("if (" + condition.code + ")");
                            predicate_scope = true;
                        }
                        emit_runtime_block(node.block, frame, out);
                        if (predicate_scope) { out.close(); }
                        out.close();
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
            const gir::Callable     &planned = callable(decl);
            RuntimeInfo              info;
            Frame                    frame;
            frame.fn      = decl;
            frame.runtime = true;

            if (fn.concise_body != ast::no_node || fn.block_body == ast::no_node)
            {
                backend(module_.decl(decl).range, "a runtime function needs a block body");
            }
            if (has_planned_result(planned.result, planned.range)) {
                const HType result = planned_type(planned.result, planned.range);
                if (result.kind != HType::Kind::Scalar && result.kind != HType::Kind::Struct && result.kind != HType::Kind::Map) {
                    backend(graph_type(planned.result, planned.range).range,
                            "the runtime-node slice supports scalar, struct, and map outputs");
                }
            }

            std::size_t temporal_count = 0;
            for (const gir::Parameter &param : planned.parameters) {
                const HType type = planned_type(param.type, planned.range);
                if (type.kind != HType::Kind::Scalar && type.kind != HType::Kind::Map && type.kind != HType::Kind::Set &&
                    type.kind != HType::Kind::List) {
                    backend(graph_type(param.type, planned.range).range,
                            "the runtime-node slice supports scalar and collection parameters");
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
                                if (name.text == "out") {
                                    info.inject_out = true;
                                } else if (name.text == "logger") {
                                    info.inject_logger = true;
                                } else {
                                    backend(name.range,
                                            "injectable '" + std::string{name.text} + "' is not supported by emit-cpp yet");
                                }
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

        void Emitter::check_supported(ast::DeclId decl) {
            if (callable(decl).kind == gir::CallableKind::RuntimeNode) { static_cast<void>(runtime_info(decl)); }
        }

        std::string Emitter::runtime_signature(ast::DeclId decl, const RuntimeInfo &info, bool with_names, bool include_inputs,
                                               bool include_output) {
            const gir::Callable     &fn = callable(decl);
            std::vector<std::string> params;
            for (std::size_t i = 0; i < fn.parameters.size(); ++i) {
                const gir::Parameter &param  = fn.parameters[i];
                const HType           type   = planned_type(param.type, fn.range);
                const SourceRange     range  = graph_type(param.type, fn.range).range;
                const std::string     name   = with_names ? " " + cpp_name(param.name) : "";
                const std::string     unused = with_names ? "[[maybe_unused]] " : "";
                if (param.is_const)
                {
                    params.push_back(unused + "hgraph::Scalar<" + quote(param.name) + ", " + value_type(type, range) + ">" + name);
                    continue;
                }
                if (!include_inputs) { continue; }
                std::string selector = unused + "hgraph::In<" + quote(param.name) + ", " + schema(type, range);
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
            if (info.inject_logger) {
                params.push_back(std::string{with_names ? "[[maybe_unused]] " : ""} + "hgraph::LoggerView" +
                                 std::string{with_names ? " logger" : ""});
            }
            if (include_output && has_planned_result(fn.result, fn.range)) {
                params.push_back(std::string{with_names ? "[[maybe_unused]] " : ""} + "hgraph::Out<" +
                                 schema(planned_type(fn.result, fn.range), graph_type(fn.result, fn.range).range) + ">" +
                                 std::string{with_names ? " hgl_output" : ""});
            }
            return join(params, ", ");
        }

        void Emitter::prepare_runtime_frame(ast::DeclId decl, const RuntimeInfo &info, Frame &frame, Writer &out, bool include_inputs,
                                            bool include_output)
        {
            const ast::FunctionDecl &fn    = function(decl);
            const gir::Callable     &planned = callable(decl);
            frame.fn                       = decl;
            frame.runtime                  = true;
            frame.runtime_inputs_available = include_inputs;
            frame.output_available         = include_output;
            frame.params.resize(fn.signature.parameters.size());
            frame.locals.clear();
            frame.second_locals.clear();
            frame.injects.clear();
            local_counts_.clear();
            local_names_.clear();
            local_names_.insert("hgl_state");
            local_names_.insert("hgl_output");
            local_names_.insert("logger");
            for (std::size_t i = 0; i < fn.signature.parameters.size(); ++i)
            {
                const ast::Parameter &param = fn.signature.parameters[i];
                const HType           type  = planned_type(planned.parameters[i].type, planned.range);
                const std::string     name  = cpp_name(planned.parameters[i].name);
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
                out.line("[[maybe_unused]] auto " + local + " = hgl_state.field<" + quote(state.name) + ">();");
                frame.locals[state.id] = make_runtime(local + ".value().checked_as<" + value_type(state.type, state.range) + ">()",
                                                      state.type, state.range, local);
            }
            if (include_output && info.inject_out)
            {
                const HType result = planned_type(planned.result, planned.range);
                frame.injects.emplace("out",
                                      make_runtime("hgl_output.value().checked_as<" +
                                                       value_type(result, graph_type(planned.result, planned.range).range) + ">()",
                                                   result, fn.name.range, "hgl_output"));
            }
            if (info.inject_logger) {
                Value logger;
                logger.kind = Value::Kind::Intrinsic;
                logger.name = "logger";
                frame.injects.emplace("logger", std::move(logger));
            }
        }

        void Emitter::emit_defaults(const gir::Callable &planned, Writer &out) {
            std::vector<std::string> defaults;
            for (const gir::Parameter &param : planned.parameters) {
                if (!param.is_const || !param.default_value.valid()) { continue; }
                const Value value = planned_constant(param.default_value, planned.range);
                const HType type  = planned_type(param.type, planned.range);
                defaults.push_back("hgraph::arg<" + quote(param.name) + ">(" +
                                   as_const(value, type, value.range, "default of '" + param.name + "'") + ")");
            }
            if (!defaults.empty()) { out.line("static auto defaults() { return std::tuple{" + join(defaults, ", ") + "}; }"); }
        }

        void Emitter::emit_runtime_function(ast::DeclId decl, Writer &out)
        {
            const ast::FunctionDecl &fn   = function(decl);
            const RuntimeInfo        info = runtime_info(decl);
            Frame                    frame;
            frame.fn      = decl;
            frame.runtime = true;
            out.line("// " + where(module_.decl(decl).range));
            out.open("struct " + callable_cpp_name(decl));
            out.line("[[maybe_unused]] static constexpr auto name = " + quote(callable(decl).identity) + ";");
            emit_defaults(callable(decl), out);
            if (!info.states.empty())
            {
                std::vector<std::string> fields;
                for (const RuntimeState &state : info.states)
                {
                    fields.push_back("hgraph::Field<" + quote(state.name) + ", " + schema(state.type, state.range) + ">");
                }
                out.line("using recordable_state = hgraph::TSB<" + quote(callable(decl).identity + ".state") + ", " +
                         join(fields, ", ") + ">;");
            }

            if (!info.states.empty() || !info.start_blocks.empty())
            {
                out.line("static void start(" + runtime_signature(decl, info, true, false, false) + ")");
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

            const bool has_output = has_planned_result(callable(decl).result, callable(decl).range);
            out.line("static void eval(" + runtime_signature(decl, info, true, true, has_output) + ")");
            out.open("");
            prepare_runtime_frame(decl, info, frame, out, true, has_output);
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
                out.line("static void stop(" + runtime_signature(decl, info, true, false, false) + ")");
                out.open("");
                prepare_runtime_frame(decl, info, frame, out, false, false);
                emit_runtime_block(std::get<ast::LifecycleBlock>(module_.stmt(info.stop_blocks.front()).node).block, frame, out);
                out.close();
            }
            out.close(";");
            out.line();
        }

        std::string Emitter::signature(ast::DeclId decl, bool with_names) {
            const gir::Callable     &fn = callable(decl);
            std::vector<std::string> params{with_names ? "hgraph::Wiring &w" : "hgraph::Wiring &"};
            for (const gir::Parameter &param : fn.parameters) {
                const HType       type  = planned_type(param.type, fn.range);
                const SourceRange range = graph_type(param.type, fn.range).range;
                const std::string name  = with_names ? " " + cpp_name(param.name) : "";
                if (param.is_const)
                {
                    params.push_back("hgraph::Scalar<" + quote(param.name) + ", " + value_type(type, range) + ">" + name);
                } else {
                    params.push_back("hgraph::Port<" + schema(type, range) + ">" + name);
                }
            }
            return join(params, ", ");
        }

        std::string Emitter::result_type(ast::DeclId decl) {
            const gir::Callable &fn = callable(decl);
            if (!has_planned_result(fn.result, fn.range)) { return "void"; }
            return "hgraph::Port<" + schema(planned_type(fn.result, fn.range), graph_type(fn.result, fn.range).range) + ">";
        }

        /// The operator contract for a public callable: its parameters as
        /// `In`/`Scalar` selectors and its result as `Out`.
        std::string Emitter::operator_contract(const std::vector<gir::Parameter> &parameters, gir::TypeId result,
                                               std::string_view registry_name) {
            std::vector<std::string> selectors{quote(registry_name)};
            for (const gir::Parameter &param : parameters) {
                const HType       type  = planned_type(param.type);
                const SourceRange range = graph_type(param.type, {}).range;
                if (param.is_const)
                {
                    selectors.push_back("hgraph::Scalar<" + quote(param.name) + ", " + value_type(type, range) + ">");
                } else {
                    selectors.push_back("hgraph::In<" + quote(param.name) + ", " + schema(type, range) + ">");
                }
            }
            if (has_planned_result(result, {})) {
                selectors.push_back("hgraph::Out<" + schema(planned_type(result), graph_type(result, {}).range) + ">");
            }
            return "hgraph::Operator<" + join(selectors, ", ") + ">";
        }

        void Emitter::emit_struct(ast::DeclId decl, Writer &out) {
            const ast::StructDecl       &item = structure(decl);
            const semantics::StructInfo &info = resolved_.structure(decl);
            for (const ast::GenericParameter &parameter : item.generics)
            {
                if (parameter.is_const)
                {
                    backend(parameter.name.range,
                            "const generic struct arguments require typed constant Bundle metadata in hgraph");
                }
            }
            Frame                        frame;
            frame.fn = decl;

            std::vector<std::string> template_parameters;
            std::vector<std::string> type_arguments;
            for (std::size_t index = 0; index < item.generics.size(); ++index) {
                const ast::GenericParameter &parameter = item.generics[index];
                const std::string            name      = cpp_name(parameter.name.text);
                if (parameter.is_const) {
                    template_parameters.push_back("std::int64_t " + name);
                    continue;
                }
                template_parameters.push_back("typename " + name);
                HType type;
                type.kind                  = HType::Kind::Generic;
                type.cpp_type              = name;
                frame.generic_types[index] = type;
                type_arguments.push_back(name);
            }

            out.line("// " + where(module_.decl(decl).range));
            if (!template_parameters.empty()) { out.line("template <" + join(template_parameters, ", ") + ">"); }
            out.open("struct " + cpp_name(item.name.text));

            std::vector<std::string> parents;
            for (const ast::TypeId parent : item.parents) {
                const HType type = type_of(parent, frame);
                parents.push_back(value_type(type, module_.type(parent).range));
            }

            std::vector<std::string> value_fields;
            std::vector<std::string> temporal_fields;
            for (const semantics::StructField &field : info.fields) {
                const HType type = type_of(field.type, frame);
                value_fields.push_back("hgraph::Field<" + quote(field.name) + ", " +
                                       value_type(type, module_.type(field.type).range) + ">");
                temporal_fields.push_back("hgraph::Field<" + quote(field.name) + ", " +
                                          schema(type, module_.type(field.type).range) + ">");
            }

            std::vector<std::string> bundle_parts{quote(module_name_), quote(item.name.text), item.abstract ? "true" : "false",
                                                  "hgraph::BundleParents<" + join(parents, ", ") + ">",
                                                  "hgraph::BundleArguments<" + join(type_arguments, ", ") + ">"};
            bundle_parts.insert(bundle_parts.end(), value_fields.begin(), value_fields.end());
            out.line("using value_type = hgraph::NominalBundle<" + join(bundle_parts, ", ") + ">;");

            std::vector<std::string> tsb_parts{"value_type"};
            tsb_parts.insert(tsb_parts.end(), temporal_fields.begin(), temporal_fields.end());
            out.line("using time_series = hgraph::NominalTSB<" + join(tsb_parts, ", ") + ">;");
            out.close(";");
            out.line();
        }

        void Emitter::emit_function(ast::DeclId decl, Writer &out, Form form)
        {
            check_supported(decl);
            if (callable(decl).kind == gir::CallableKind::RuntimeNode) {
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
            const std::string name = callable_cpp_name(decl);

            out.line("// " + where(module_.decl(decl).range));
            if (form == Form::OutOfLine)
            {
                out.line(result_type(decl) + " " + name + "::compose(" + signature(decl, true) + ")");
            }
            else
            {
                out.open("struct " + name);
                out.line("[[maybe_unused]] static constexpr auto name = " + quote(callable(decl).identity) + ";");
                // Defaults of const parameters travel with the graph so the
                // registry can apply them when the function is called by name.
                emit_defaults(callable(decl), out);
                out.line("static " + result_type(decl) + " compose(" + signature(decl, form != Form::Declaration) +
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
                const HType           type  = planned_type(callable(decl).parameters[i].type, callable(decl).range);
                const std::string     name  = cpp_name(callable(decl).parameters[i].name);
                local_names_.insert(name);
                if (param.is_const)
                {
                    frame.params[i] = make_const(name + ".value()", type, param.name.range);
                } else {
                    frame.params[i] = make_port(name, type, param.name.range);
                }
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
            for (const ast::DeclId id : callable_declarations_) {
                if (callable(id).visibility == gir::CallableVisibility::Internal) { internal.push_back(id); }
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
                            "'" + std::string{callable_name(id)} + "' is recursive; recursive functions are not supported");
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
            bind_hgraph_declarations();
            EmittedModule result;
            result.module_name = graph_.path;
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

            // Every emitted function is checked up front so the whole unit
            // fails closed before a partial pair is written.
            std::vector<ast::DeclId> exports;
            std::vector<ast::DeclId> impls;
            std::map<std::string, std::string> cpp_functions;
            for (const ast::DeclId id : callable_declarations_) {
                check_supported(id);
                const gir::Callable     &fn = callable(id);
                const std::string        source_name{callable_name(id)};
                const std::string        generated_name = callable_cpp_name(id);
                if (const auto [found, inserted] = cpp_functions.emplace(generated_name, source_name);
                    !inserted && found->second != source_name)
                {
                    backend(fn.range,
                            "C++ function '" + source_name + "' collides with '" + found->second + "' as '" + generated_name + "'");
                }
                std::map<std::string, std::string> cpp_parameters;
                for (const gir::Parameter &param : fn.parameters) {
                    const std::string parameter_name{param.name};
                    const std::string generated_parameter = cpp_name(parameter_name);
                    if (const auto [found, inserted] = cpp_parameters.emplace(generated_parameter, parameter_name);
                        !inserted && found->second != parameter_name)
                    {
                        backend(graph_type(param.type, fn.range).range, "C++ parameter '" + parameter_name + "' collides with '" +
                                                                            found->second + "' as '" + generated_parameter + "'");
                    }
                }
                if (callable(id).visibility == gir::CallableVisibility::Export) { exports.push_back(id); }
                if (callable(id).visibility == gir::CallableVisibility::Implementation) { impls.push_back(id); }
            }
            const std::vector<ast::DeclId> internal = ordered_internal_functions();

            // Bodies first: they discover which kernels (analytics) the
            // header must include. Anonymous graph bodies are collected while
            // their containing functions emit, then placed before every use.
            Writer private_functions;
            for (const ast::DeclId id : internal) { emit_function(id, private_functions, Form::InlineStruct); }
            for (const ast::DeclId id : impls) { emit_function(id, private_functions, Form::InlineStruct); }
            Writer public_functions;
            for (const ast::DeclId id : exports) {
                if (callable(id).kind == gir::CallableKind::Composition) { emit_function(id, public_functions, Form::OutOfLine); }
            }

            Writer body;
            body.line("namespace " + namespace_);
            body.line("{");
            if (!internal.empty() || !impls.empty() || !generated_helpers_.str().empty()) {
                body.indent();
                body.open("namespace");
                const bool has_internal_runtime = std::any_of(internal.begin(), internal.end(), [&](ast::DeclId id) {
                    return callable(id).kind == gir::CallableKind::RuntimeNode;
                });
                if (has_internal_runtime)
                {
                    body.open("namespace operator_contracts");
                }
                for (const ast::DeclId id : internal)
                {
                    if (callable(id).kind != gir::CallableKind::RuntimeNode) { continue; }
                    const gir::Callable &item = callable(id);
                    body.line("using " + callable_cpp_name(id) + " = " +
                              operator_contract(item.parameters, item.result, item.identity) + ";");
                }
                if (has_internal_runtime)
                {
                    body.close("  // namespace operator_contracts");
                    body.line();
                }
                body.append(generated_helpers_.str());
                body.append(private_functions.str());
                body.close("  // namespace");
                body.line();
                body.dedent();
            }
            body.indent();
            body.append(public_functions.str());

            // Registration: exported functions and operator implementations
            // become registry candidates under module-qualified names, and
            // the installer replays them after a registry reset.
            body.open("hgraph::OperatorProviderHandle register_operators()");
            body.line("auto &registry = hgraph::OperatorRegistry::instance();");
            body.open("auto provider = registry.register_installer(" + quote(result.module_name) + ", []");
            for (const ast::DeclId id : exports)
            {
                const std::string name         = callable_cpp_name(id);
                const std::string registration = callable(id).kind == gir::CallableKind::RuntimeNode
                                                     ? "hgraph::register_overload"
                                                     : "hgraph::register_graph_overload";
                body.line(registration + "<operators::" + name + ", " + name + ">();");
            }
            for (const ast::DeclId id : internal)
            {
                if (callable(id).kind != gir::CallableKind::RuntimeNode) { continue; }
                const std::string name = callable_cpp_name(id);
                body.line("hgraph::register_overload<operator_contracts::" + name + ", " + name + ">();");
            }
            for (const ast::DeclId id : impls)
            {
                const gir::Callable &implementation = callable(id);
                const auto contract = std::find_if(graph_.operators.begin(), graph_.operators.end(), [&](const auto &candidate) {
                    return candidate.identity == implementation.operator_identity;
                });
                if (contract == graph_.operators.end() || contract->imported) {
                    unsupported(module_.decl(id).range, "an impl fn of an imported operator");
                }
                const std::string registration = implementation.kind == gir::CallableKind::RuntimeNode
                                                     ? "hgraph::register_overload"
                                                     : "hgraph::register_graph_overload";
                body.line(registration + "<operators::" + cpp_name(local_identity(contract->identity)) + ", " +
                          callable_cpp_name(id) + ">();");
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
            header.line("#include <cstdint>");
            header.line("#include <limits>");
            header.line("#include <stdexcept>");
            header.line("#include <tuple>");
            header.line();
            header.line("namespace " + namespace_);
            header.line("{");
            header.indent();
            for (const ast::DeclId id : resolved_.structs) { emit_struct(id, header); }
            if (!operator_declarations_.empty() || !exports.empty()) {
                header.line("/// Operator contracts for the module's public callables.");
                header.open("namespace operators");
                for (const ast::DeclId id : operator_declarations_) {
                    const gir::OperatorContract &op = operator_decl(id);
                    header.line("// " + where(module_.decl(id).range));
                    header.line("using " + cpp_name(local_identity(op.identity)) + " = " +
                                operator_contract(op.parameters, op.result, operator_registry_name(op)) + ";");
                }
                for (const ast::DeclId id : exports)
                {
                    const gir::Callable &item = callable(id);
                    header.line("// " + where(module_.decl(id).range));
                    header.line("using " + callable_cpp_name(id) + " = " +
                                operator_contract(item.parameters, item.result, item.identity) + ";");
                }
                header.close("  // namespace operators");
                header.line();
            }
            for (const ast::DeclId id : exports)
            {
                emit_function(id, header,
                              callable(id).kind == gir::CallableKind::RuntimeNode ? Form::InlineStruct : Form::Declaration);
                result.exports.push_back(std::string{callable_name(id)});
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
                    py += "    " + quote(alias) + ": _hgl_operator_function(" + quote(callable(exports[i]).identity) + "),\n";
                    names.push_back(quote(alias));
                }
                py += "})\n\n__all__ = [" + join(names, ", ") + "]\n";
                result.python = std::move(py);
            }
            return result;
        }
    }  // namespace

    std::optional<EmittedModule> emit_cpp(const syntax::SourceFile &file, const hgraph_ir::Module &graph, const ast::Module &module,
                                          const semantics::ResolvedModule &resolved, const EmitOptions &options,
                                          syntax::DiagnosticSink &diagnostics) {
        Emitter emitter{file, graph, module, resolved, options, diagnostics};
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
