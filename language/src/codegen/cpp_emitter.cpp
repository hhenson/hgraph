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
        using syntax::Category;
        using syntax::SourceRange;

        struct Abort
        {
        };

        // ------------------------------------------------------------ types

        /// A normalized native spelling derived from canonical hgraph IR.
        /// Code generation consumes this representation without re-running
        /// frontend name, type, or phase analysis.
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

        ast::UnaryOp syntax_op(ir::hir::UnaryOp op) noexcept {
            return op == ir::hir::UnaryOp::Negate ? ast::UnaryOp::Negate : ast::UnaryOp::Not;
        }

        ast::BinaryOp syntax_op(ir::hir::BinaryOp op) noexcept {
            switch (op) {
                case ir::hir::BinaryOp::Mul: return ast::BinaryOp::Mul;
                case ir::hir::BinaryOp::Div: return ast::BinaryOp::Div;
                case ir::hir::BinaryOp::Rem: return ast::BinaryOp::Rem;
                case ir::hir::BinaryOp::Add: return ast::BinaryOp::Add;
                case ir::hir::BinaryOp::Sub: return ast::BinaryOp::Sub;
                case ir::hir::BinaryOp::Less: return ast::BinaryOp::Less;
                case ir::hir::BinaryOp::LessEqual: return ast::BinaryOp::LessEqual;
                case ir::hir::BinaryOp::Greater: return ast::BinaryOp::Greater;
                case ir::hir::BinaryOp::GreaterEqual: return ast::BinaryOp::GreaterEqual;
                case ir::hir::BinaryOp::Equal: return ast::BinaryOp::Equal;
                case ir::hir::BinaryOp::NotEqual: return ast::BinaryOp::NotEqual;
                case ir::hir::BinaryOp::And: return ast::BinaryOp::And;
                case ir::hir::BinaryOp::Or: return ast::BinaryOp::Or;
            }
            std::unreachable();
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
            HType              type{};
            gir::CallableId    callable{};
            std::string name{};
            SourceRange range{};
            bool               structured_delta{false};
            std::vector<HType> iterator_types{};
            gir::ValueId       planned_iterator_predicate{};
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
            std::unordered_map<std::uint32_t, Value> planned_bindings{};
            bool                                   runtime{false};
            bool                                   runtime_inputs_available{true};
            bool                                   output_available{false};
        };

        struct RuntimeState
        {
            gir::BindingId binding{};
            std::string    name{};
            HType          type{};
            gir::ValueId   init{};
            SourceRange    range{};
        };

        struct RuntimeInfo
        {
            std::vector<RuntimeState>       states{};
            std::vector<gir::BlockId>       start_blocks{};
            std::vector<gir::BlockId>       stop_blocks{};
            std::unordered_set<std::size_t> active_parameters{};
            gir::BindingId                  out_binding{};
            gir::BindingId                  logger_binding{};
            bool                            has_when{false};
        };

        Value                       make_const(std::string code, HType type, SourceRange range,
                                               std::variant<std::monostate, std::int64_t, double> number = {});
        Value                       make_port(std::string code, HType type, SourceRange range);
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
            [[nodiscard]] const gir::Binding          &planned_binding(gir::BindingId id, SourceRange fallback);
            [[nodiscard]] const gir::Callable         &callable(ast::DeclId decl) const { return *callables_.at(decl); }
            [[nodiscard]] const gir::OperatorContract &operator_decl(ast::DeclId decl) const { return *operators_.at(decl); }
            [[nodiscard]] const gir::StructContract &struct_contract(ast::DeclId decl) const { return *structures_.at(decl); }
            [[nodiscard]] static std::string_view      local_identity(std::string_view identity) noexcept;
            [[nodiscard]] std::string_view             callable_name(ast::DeclId decl) const;
            [[nodiscard]] std::string                  callable_cpp_name(ast::DeclId decl);
            [[nodiscard]] static std::string           operator_registry_name(const gir::OperatorContract &op) {
                return op.registry_name.empty() ? op.identity : op.registry_name;
            }
            [[nodiscard]] std::string where(SourceRange range) const
            {
                const syntax::Location at = file_.location(range.begin);
                return basename_ + ":" + std::to_string(at.line);
            }

            // -- types
            using PlannedTypeBindings = std::unordered_map<std::uint32_t, HType>;
            [[nodiscard]] HType planned_type(gir::TypeId id, SourceRange fallback = {},
                                             const PlannedTypeBindings *bindings = nullptr);
            [[nodiscard]] const gir::StructContract &planned_structure(std::string_view identity, SourceRange fallback);
            [[nodiscard]] PlannedTypeBindings        planned_struct_bindings(const gir::StructContract &contract, const HType &type,
                                                                             SourceRange fallback);
            [[nodiscard]] const gir::Type            &graph_type(gir::TypeId id, SourceRange fallback);
            [[nodiscard]] const gir::ConstExpr       &graph_constant(gir::ConstExprId id, SourceRange fallback);
            [[nodiscard]] std::optional<std::int64_t> planned_integer(gir::ConstExprId id, SourceRange fallback);
            [[nodiscard]] bool                        has_planned_result(gir::TypeId id, SourceRange fallback);
            [[nodiscard]] Value                       planned_constant(gir::ConstExprId id, SourceRange fallback = {});
            [[nodiscard]] Value                       planned_literal(const ir::hir::Constant &literal, SourceRange range);
            [[nodiscard]] bool                        planned_null(gir::ConstExprId id, SourceRange fallback);
            [[nodiscard]] bool                        planned_null(gir::ValueId id, SourceRange fallback);
            [[nodiscard]] Value                       planned_field_value(gir::ConstExprId id, SourceRange fallback,
                                                                          const PlannedTypeBindings *bindings = nullptr);
            [[nodiscard]] Value                       planned_construct(const gir::ConstExpr &expression, SourceRange fallback,
                                                                        const PlannedTypeBindings *bindings);
            [[nodiscard]] std::string value_type(const HType &type, SourceRange range);
            [[nodiscard]] std::string                 schema(const HType &type, SourceRange range);

            // -- expressions
            [[nodiscard]] Value eval_planned_expr(gir::ValueId id, Frame &frame);
            [[nodiscard]] Value eval_planned_reference(const gir::Reference &reference, SourceRange range, Frame &frame);
            [[nodiscard]] Value eval_planned_call(const gir::Value &expression, const gir::Call &call, Frame &frame);
            [[nodiscard]] Value eval_planned_construct(gir::TypeId type, const std::vector<gir::Argument> &arguments, bool delta,
                                                       SourceRange range, Frame &frame);
            [[nodiscard]] Value eval_planned_intrinsic(const Value &callee, const gir::Call &call, SourceRange range, Frame &frame);
            [[nodiscard]] Value lower_planned_map_call(const Value &callee, const gir::Call &call, SourceRange range, Frame &frame);
            [[nodiscard]] Value call_planned_function(gir::CallableId id, const std::vector<gir::Argument> &arguments,
                                                      SourceRange range, Frame &frame);
            [[nodiscard]] std::vector<std::optional<gir::ValueId>>
            bind_planned_arguments(gir::CallableId id, const std::vector<gir::Argument> &arguments, SourceRange range);
            [[nodiscard]] std::string planned_operator_marker(std::string_view identity, std::string_view registry_name,
                                                              SourceRange range);
            [[nodiscard]] Value fold_unary(ast::UnaryOp op, const Value &operand, SourceRange range);
            [[nodiscard]] Value fold_binary(ast::BinaryOp op, const Value &lhs, const Value &rhs, SourceRange range);
            [[nodiscard]] Value wire_binary(ast::BinaryOp op, const Value &lhs, const Value &rhs, SourceRange range);
            [[nodiscard]] Value wire(std::string marker, const std::vector<std::string> &args, SourceRange range,
                                     const HType &result = HType{});
            [[nodiscard]] std::string argument_code(const Value &value);
            [[nodiscard]] std::string as_port(const Value &value, const HType &temporal, SourceRange range);
            [[nodiscard]] std::string as_const(const Value &value, const HType &target, SourceRange range, const std::string &what);
            // -- statements
            void emit_planned_block(gir::BlockId id, Frame &frame, Writer &out, bool function_body, SourceRange fallback);
            void emit_planned_statement(gir::StatementId id, Frame &frame, Writer &out, SourceRange fallback);
            void emit_planned_if(const gir::Conditional &branch, SourceRange range, Frame &frame, Writer &out);
            void emit_return(const Value &value, Frame &frame, Writer &out, SourceRange range);
            void emit_runtime_stmt(gir::StatementId id, Frame &frame, Writer &out, SourceRange fallback);
            void emit_runtime_block(gir::BlockId id, Frame &frame, Writer &out, SourceRange fallback);
            void emit_runtime_if(const gir::Conditional &branch, SourceRange range, Frame &frame, Writer &out);
            [[nodiscard]] bool        planned_expression_terminates(gir::ValueId id, SourceRange fallback);
            [[nodiscard]] bool        planned_block_terminates(gir::BlockId id, SourceRange fallback);
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
            void emit_struct(const gir::StructContract &item, Writer &out);
            void                      emit_runtime_function(ast::DeclId decl, Writer &out);
            [[nodiscard]] RuntimeInfo runtime_info(ast::DeclId decl);
            [[nodiscard]] std::optional<std::size_t> runtime_parameter(gir::ValueId id, ast::DeclId decl);
            void collect_runtime_activation(gir::ValueId id, ast::DeclId decl, RuntimeInfo &info);
            using RuntimeValidSet = std::unordered_set<std::size_t>;
            void                          check_runtime_expr(gir::ValueId id, ast::DeclId decl, const RuntimeValidSet &valid);
            [[nodiscard]] RuntimeValidSet runtime_true_valid(gir::ValueId id, ast::DeclId decl, const RuntimeValidSet &valid);
            void check_runtime_block(gir::BlockId id, ast::DeclId decl, const RuntimeValidSet &valid, bool allow_when = false);
            void check_runtime_stmt(gir::StatementId id, ast::DeclId decl, const RuntimeValidSet &valid, bool allow_when,
                                    SourceRange fallback);
            [[nodiscard]] std::string runtime_signature(ast::DeclId decl, const RuntimeInfo &info, bool with_names,
                                                        bool include_inputs, bool include_output);
            void prepare_runtime_frame(ast::DeclId decl, const RuntimeInfo &info, Frame &frame, Writer &out, bool include_inputs,
                                       bool include_output);
            void emit_defaults(const gir::Callable &callable, Writer &out);
            [[nodiscard]] std::vector<ast::DeclId> ordered_internal_functions();
            struct PlannedCalls
            {
                std::set<std::uint32_t> calls{};
                std::set<std::uint32_t> values{};
                std::set<std::uint32_t> blocks{};
            };
            void                                collect_calls(gir::ValueId id, PlannedCalls &calls, SourceRange fallback = {});
            void                                collect_calls(gir::BlockId id, PlannedCalls &calls, SourceRange fallback = {});
            [[nodiscard]] const gir::Value     &planned_value(gir::ValueId id, SourceRange fallback);
            [[nodiscard]] const gir::Statement &planned_statement(gir::StatementId id, SourceRange fallback);
            [[nodiscard]] const gir::Block     &planned_block(gir::BlockId id, SourceRange fallback);
            [[nodiscard]] ast::DeclId           syntax_callable(gir::CallableId id, SourceRange fallback);

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
            std::unordered_map<ast::DeclId, const gir::StructContract *>   structures_{};
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

        const gir::Binding &Emitter::planned_binding(gir::BindingId id, SourceRange fallback) {
            if (!id.valid() || id.value >= graph_.bindings.size()) {
                backend(fallback, "hgraph IR contains an invalid body binding ID");
            }
            return graph_.bindings[id.value];
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

            const auto structure_for_range = [&](SourceRange range) {
                ast::DeclId match = ast::no_node;
                for (const ast::DeclId id : resolved_.structs) {
                    if (module_.decl(id).range != range) { continue; }
                    if (match != ast::no_node) {
                        backend(range, "hgraph IR struct range matches more than one syntax declaration");
                    }
                    match = id;
                }
                return match;
            };
            for (const gir::StructContract &item : graph_.structures) {
                const ast::DeclId id = structure_for_range(item.range);
                if (id == ast::no_node) {
                    backend(item.range, "hgraph IR struct '" + item.identity + "' has no syntax construction adapter");
                }
                const ast::StructDecl       &source = structure(id);
                const semantics::StructInfo &info   = resolved_.structure(id);
                if (item.generics.size() != source.generics.size() || item.parents.size() != source.parents.size() ||
                    item.fields.size() != info.fields.size()) {
                    backend(item.range,
                            "hgraph IR struct '" + item.identity +
                                "' disagrees with the syntax construction adapter's declaration shape");
                }
                for (std::size_t index = 0; index < item.generics.size(); ++index) {
                    if (item.generics[index].is_const != source.generics[index].is_const) {
                        backend(item.range, "hgraph IR struct '" + item.identity +
                                                "' disagrees with the syntax construction adapter's generic roles");
                    }
                }
                for (std::size_t index = 0; index < item.fields.size(); ++index) {
                    if (item.fields[index].optional != info.fields[index].optional) {
                        backend(item.range, "hgraph IR struct '" + item.identity +
                                                "' disagrees with the syntax construction adapter's field optionality");
                    }
                }
                if (!structures_.emplace(id, &item).second) {
                    backend(item.range, "more than one hgraph IR struct maps to the same syntax declaration");
                }
            }
            if (structures_.size() != resolved_.structs.size()) {
                backend(SourceRange{}, "the hgraph IR and syntax construction adapter disagree on the module's structs");
            }
        }

        // ------------------------------------------------------------ types

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

        Value Emitter::planned_literal(const ir::hir::Constant &literal, SourceRange range) {
            return std::visit(
                [&](const auto &item) -> Value {
                    using T = std::decay_t<decltype(item)>;
                    if constexpr (std::is_same_v<T, ir::hir::NullValue>) {
                        unsupported(range, "'null' in a generated expression");
                    } else if constexpr (std::is_same_v<T, ir::hir::PlaceholderValue>) {
                        fail(Category::Type, range, "'_' is only valid in a harness sequence");
                    } else if constexpr (std::is_same_v<T, bool>) {
                        return make_const(item ? "true" : "false", scalar_type(ast::ScalarType::Bool), range);
                    } else if constexpr (std::is_same_v<T, std::int64_t>) {
                        return make_const(integer_literal(item), scalar_type(ast::ScalarType::I64), range, item);
                    } else if constexpr (std::is_same_v<T, double>) {
                        return make_const("hgraph::Float{" + float_literal(item) + "}", scalar_type(ast::ScalarType::F64), range,
                                          item);
                    } else if constexpr (std::is_same_v<T, std::string>) {
                        return make_const("hgraph::Str{" + quote(item) + "}", scalar_type(ast::ScalarType::Str), range);
                    } else if constexpr (std::is_same_v<T, syntax::TemporalValue>) {
                        if (std::optional<Value> value = temporal_constant(item, range)) { return std::move(*value); }
                        backend(range, "zoned and civil literals are not supported by the first pass");
                    }
                },
                literal);
        }

        Value Emitter::planned_constant(gir::ConstExprId id, SourceRange fallback) {
            const gir::ConstExpr &expression = graph_constant(id, fallback);
            const SourceRange     range      = expression.range.end > expression.range.begin ? expression.range : fallback;
            if (expression.literal) { return planned_literal(*expression.literal, range); }

            switch (expression.kind) {
                case gir::ConstExprKind::Unary:
                    {
                        return fold_unary(syntax_op(expression.unary), planned_constant(expression.lhs, range), range);
                    }
                case gir::ConstExprKind::Binary:
                    {
                        const ast::BinaryOp op = syntax_op(expression.binary);
                        return fold_binary(op, planned_constant(expression.lhs, range), planned_constant(expression.rhs, range),
                                           range);
                    }
                case gir::ConstExprKind::Parameter: unsupported(range, "a generic parameter in a generated constant expression");
                case gir::ConstExprKind::Index: unsupported(range, "an indexed generated constant expression");
                case gir::ConstExprKind::Field: unsupported(range, "a field-read generated constant expression");
                case gir::ConstExprKind::Sequence: unsupported(range, "a generated list or map constant");
                case gir::ConstExprKind::Tuple: unsupported(range, "a generated tuple constant");
                case gir::ConstExprKind::Construct: unsupported(range, "a generated struct constant");
                case gir::ConstExprKind::Literal: break;
            }
            backend(range, "hgraph IR contains an incomplete constant expression");
        }

        bool Emitter::planned_null(gir::ConstExprId id, SourceRange fallback) {
            const gir::ConstExpr &expression = graph_constant(id, fallback);
            return expression.kind == gir::ConstExprKind::Literal && expression.literal &&
                   std::holds_alternative<ir::hir::NullValue>(*expression.literal);
        }

        bool Emitter::planned_null(gir::ValueId id, SourceRange fallback) {
            const gir::Value &expression = planned_value(id, fallback);
            if (expression.constant) { return std::holds_alternative<ir::hir::NullValue>(*expression.constant); }
            const auto *literal = std::get_if<gir::Literal>(&expression.node);
            return literal != nullptr && std::holds_alternative<ir::hir::NullValue>(literal->value);
        }

        HType Emitter::planned_type(gir::TypeId id, SourceRange fallback, const PlannedTypeBindings *bindings) {
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
                            if (bindings != nullptr) {
                                if (const auto found = bindings->find(type.binding.value); found != bindings->end()) {
                                    return found->second;
                                }
                            }
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
                                HType child = planned_type(*argument.type, range, bindings);
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
                        for (gir::TypeId child : type.children) {
                            result.children.push_back(planned_type(child, range, bindings));
                        }
                        return result;
                    }
                case TypeKind::List:
                    {
                        if (type.children.size() != 1U) { backend(range, "hgraph IR list type requires one element type"); }
                        HType result;
                        result.kind = HType::Kind::List;
                        result.children.push_back(planned_type(type.children.front(), range, bindings));
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
                        result.children.push_back(planned_type(type.children.front(), range, bindings));
                        return result;
                    }
                case TypeKind::Map:
                    {
                        if (type.children.size() != 2U) { backend(range, "hgraph IR map type requires key and value types"); }
                        HType result;
                        result.kind = HType::Kind::Map;
                        result.children.push_back(planned_type(type.children[0], range, bindings));
                        result.children.push_back(planned_type(type.children[1], range, bindings));
                        return result;
                    }
                case TypeKind::Rolling:
                    {
                        if (type.children.size() != 1U) { backend(range, "hgraph IR rolling type requires one element type"); }
                        HType result;
                        result.kind = HType::Kind::Rolling;
                        result.children.push_back(planned_type(type.children.front(), range, bindings));
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
                        result.children.push_back(planned_type(type.children.front(), range, bindings));
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

        const gir::StructContract &Emitter::planned_structure(std::string_view identity, SourceRange fallback) {
            const auto found = std::find_if(graph_.structures.begin(), graph_.structures.end(),
                                            [&](const auto &candidate) { return candidate.identity == identity; });
            if (found == graph_.structures.end()) {
                backend(fallback, "unknown hgraph IR nominal type '" + std::string{identity} + "'");
            }
            return *found;
        }

        Emitter::PlannedTypeBindings Emitter::planned_struct_bindings(const gir::StructContract &contract, const HType &type,
                                                                      SourceRange fallback) {
            PlannedTypeBindings result;
            std::size_t         type_argument = 0;
            for (const gir::GenericParameter &generic : contract.generics) {
                if (generic.is_const) { continue; }
                if (!generic.binding.valid() || generic.binding.value >= graph_.bindings.size()) {
                    backend(contract.range, "hgraph IR struct '" + contract.identity + "' has an invalid generic binding");
                }
                if (graph_.bindings[generic.binding.value].kind != gir::BindingKind::TypeParameter) {
                    backend(contract.range, "hgraph IR struct '" + contract.identity + "' has a mismatched type binding");
                }
                if (type_argument >= type.children.size()) {
                    backend(fallback, "constructed type '" + contract.identity + "' is missing a generic type argument");
                }
                if (!result.emplace(generic.binding.value, type.children[type_argument++]).second) {
                    backend(contract.range, "hgraph IR struct '" + contract.identity + "' repeats a generic binding");
                }
            }
            if (type_argument != type.children.size()) {
                backend(fallback, "constructed type '" + contract.identity + "' has too many generic type arguments");
            }
            return result;
        }

        Value Emitter::planned_field_value(gir::ConstExprId id, SourceRange fallback, const PlannedTypeBindings *bindings) {
            const gir::ConstExpr &expression = graph_constant(id, fallback);
            const SourceRange     range      = expression.range.end > expression.range.begin ? expression.range : fallback;
            switch (expression.kind) {
                case gir::ConstExprKind::Unary:
                    {
                        const Value        operand = planned_field_value(expression.lhs, range, bindings);
                        const ast::UnaryOp op =
                            expression.unary == ir::hir::UnaryOp::Negate ? ast::UnaryOp::Negate : ast::UnaryOp::Not;
                        if (operand.is_const() || operand.is_runtime()) { return fold_unary(op, operand, range); }
                        if (!operand.is_port()) { backend(range, "this operand has no value"); }
                        return wire(op == ast::UnaryOp::Negate ? "hgraph::stdlib::neg_" : "hgraph::stdlib::not_", {operand.code},
                                    range);
                    }
                case gir::ConstExprKind::Binary:
                    {
                        const Value         lhs = planned_field_value(expression.lhs, range, bindings);
                        const Value         rhs = planned_field_value(expression.rhs, range, bindings);
                        const ast::BinaryOp op  = [&] {
                            switch (expression.binary) {
                                case ir::hir::BinaryOp::Mul: return ast::BinaryOp::Mul;
                                case ir::hir::BinaryOp::Div: return ast::BinaryOp::Div;
                                case ir::hir::BinaryOp::Rem: return ast::BinaryOp::Rem;
                                case ir::hir::BinaryOp::Add: return ast::BinaryOp::Add;
                                case ir::hir::BinaryOp::Sub: return ast::BinaryOp::Sub;
                                case ir::hir::BinaryOp::Less: return ast::BinaryOp::Less;
                                case ir::hir::BinaryOp::LessEqual: return ast::BinaryOp::LessEqual;
                                case ir::hir::BinaryOp::Greater: return ast::BinaryOp::Greater;
                                case ir::hir::BinaryOp::GreaterEqual: return ast::BinaryOp::GreaterEqual;
                                case ir::hir::BinaryOp::Equal: return ast::BinaryOp::Equal;
                                case ir::hir::BinaryOp::NotEqual: return ast::BinaryOp::NotEqual;
                                case ir::hir::BinaryOp::And: return ast::BinaryOp::And;
                                case ir::hir::BinaryOp::Or: return ast::BinaryOp::Or;
                            }
                            std::unreachable();
                        }();
                        if ((lhs.is_const() || lhs.is_runtime()) && (rhs.is_const() || rhs.is_runtime())) {
                            return fold_binary(op, lhs, rhs, range);
                        }
                        return wire_binary(op, lhs, rhs, range);
                    }
                case gir::ConstExprKind::Index:
                    {
                        const Value target = planned_field_value(expression.lhs, range, bindings);
                        const Value index  = planned_field_value(expression.rhs, range, bindings);
                        if (target.is_port()) {
                            return wire("hgraph::stdlib::getitem_", {target.code, argument_code(index)}, range);
                        }
                        unsupported(range, "indexing a constant");
                    }
                case gir::ConstExprKind::Field:
                    {
                        const Value target = planned_field_value(expression.lhs, range, bindings);
                        if (target.is_port()) {
                            return wire("hgraph::stdlib::getattr_", {target.code, "hgraph::Str{" + quote(expression.member) + "}"},
                                        range);
                        }
                        unsupported(range, "field access on a constant");
                    }
                case gir::ConstExprKind::Construct: return planned_construct(expression, fallback, bindings);
                case gir::ConstExprKind::Literal:
                case gir::ConstExprKind::Parameter:
                case gir::ConstExprKind::Sequence:
                case gir::ConstExprKind::Tuple: return planned_constant(id, fallback);
            }
            std::unreachable();
        }

        Value Emitter::planned_construct(const gir::ConstExpr &expression, SourceRange fallback,
                                         const PlannedTypeBindings *bindings) {
            const SourceRange range = expression.range.end > expression.range.begin ? expression.range : fallback;
            if (expression.delta) { unsupported(range, "a sparse delta in a struct field default"); }

            HType type = planned_type(expression.constructed_type, range, bindings);
            if (type.kind == HType::Kind::Atomic) {
                if (type.children.size() != 1U) { backend(range, "an atomic constructed type requires one value type"); }
                HType inner = std::move(type.children.front());
                type        = std::move(inner);
            }
            if (type.kind != HType::Kind::Struct) { backend(range, "a constructed hgraph IR default does not name a struct type"); }
            const gir::StructContract &contract = planned_structure(type.nominal_identity, range);
            if (contract.abstract) {
                fail(Category::Type, range,
                     "abstract struct '" + std::string{local_identity(contract.identity)} + "' is not constructible");
            }
            PlannedTypeBindings generics = bindings != nullptr ? *bindings : PlannedTypeBindings{};
            for (auto &&[binding, value] : planned_struct_bindings(contract, type, range)) {
                generics.insert_or_assign(binding, std::move(value));
            }

            std::unordered_map<std::string_view, gir::ConstExprId> supplied;
            supplied.reserve(expression.arguments.size());
            for (const gir::ConstArgument &argument : expression.arguments) {
                const auto field = std::find_if(contract.fields.begin(), contract.fields.end(),
                                                [&](const auto &candidate) { return candidate.name == argument.name; });
                if (field == contract.fields.end()) {
                    backend(range, "struct '" + std::string{local_identity(contract.identity)} + "' has no field named '" +
                                       argument.name + "'");
                }
                if (!supplied.emplace(argument.name, argument.value).second) {
                    backend(range, "field '" + argument.name + "' is given twice");
                }
            }

            std::vector<std::string> temporal_fields;
            temporal_fields.reserve(contract.fields.size());
            for (const gir::StructField &field : contract.fields) {
                gir::ConstExprId value;
                if (const auto found = supplied.find(field.name); found != supplied.end()) {
                    value = found->second;
                } else {
                    value = field.default_value;
                }
                const HType       field_type  = planned_type(field.type, field.range, &generics);
                const SourceRange field_range = graph_type(field.type, field.range).range;
                if (!value.valid()) {
                    if (!field.optional) {
                        backend(range,
                                "struct '" + std::string{local_identity(contract.identity)} + "' needs field '" + field.name + "'");
                    }
                    temporal_fields.push_back("hgraph::wire<hgraph::stdlib::nothing, " + schema(field_type, field_range) + ">(w)");
                    continue;
                }
                if (planned_null(value, field.range)) {
                    if (!field.optional) {
                        fail(Category::Type, graph_constant(value, field.range).range,
                             "required field '" + field.name + "' cannot be null");
                    }
                    temporal_fields.push_back("hgraph::wire<hgraph::stdlib::nothing, " + schema(field_type, field_range) + ">(w)");
                    continue;
                }
                Value item = planned_field_value(value, field.range, &generics);
                temporal_fields.push_back(as_port(item, field_type, item.range));
            }

            Value result = make_port("hgraph::stdlib::to_tsb<" + schema(type, range) + ">(w" +
                                         (temporal_fields.empty() ? std::string{} : ", " + join(temporal_fields, ", ")) + ")",
                                     type, range);
            result.atomic_code =
                "hgraph::wire<hgraph::stdlib::combine_cs, hgraph::TS<" + value_type(type, range) + ">>(w, " + result.code + ")";
            return result;
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

        std::string Emitter::planned_operator_marker(std::string_view identity, std::string_view registry_name, SourceRange range) {
            const auto local = std::find_if(graph_.operators.begin(), graph_.operators.end(), [&](const gir::OperatorContract &op) {
                return !op.imported && op.identity == identity;
            });
            if (local != graph_.operators.end()) { return "operators::" + cpp_name(local_identity(local->identity)); }

            std::string                name{registry_name.empty() ? identity : registry_name};
            constexpr std::string_view analytics = "hgraph.analytics.";
            constexpr std::string_view standard  = "hgraph.std.";
            if (name.starts_with(analytics)) {
                uses_analytics_ = true;
                return "hgraph::analytics::" + name.substr(analytics.size());
            }
            if (name.starts_with(standard)) { name.erase(0, standard.size()); }
            if (name.empty() || name.find('.') != std::string::npos) {
                backend(range, "operator '" + std::string{identity} + "' has no supported native marker");
            }
            return "hgraph::stdlib::" + name;
        }

        // -------------------------------------------------------- expressions

        Value Emitter::eval_planned_reference(const gir::Reference &reference, SourceRange range, Frame &frame) {
            switch (reference.kind) {
                case gir::ReferenceKind::Binding:
                    {
                        const gir::Binding &binding = planned_binding(reference.binding, range);
                        if (frame.runtime && !frame.runtime_inputs_available && binding.kind == gir::BindingKind::SignalParameter) {
                            fail(Category::Phase, range, "temporal parameters are not available in runtime lifecycle blocks");
                        }
                        const auto          found   = frame.planned_bindings.find(reference.binding.value);
                        if (found == frame.planned_bindings.end()) {
                            backend(range, "'" + binding.name + "' is not bound in this function");
                        }
                        Value result = found->second;
                        result.range = range;
                        return result;
                    }
                case gir::ReferenceKind::Callable:
                    {
                        (void)syntax_callable(reference.callable, range);
                        Value result;
                        result.kind     = Value::Kind::Function;
                        result.callable = reference.callable;
                        result.range    = range;
                        return result;
                    }
                case gir::ReferenceKind::Operator:
                    {
                        Value result;
                        result.kind  = std::ranges::any_of(graph_.operators,
                                                           [&](const gir::OperatorContract &op) {
                                                              return !op.imported && op.identity == reference.identity;
                                                           })
                                           ? Value::Kind::LocalOperator
                                           : Value::Kind::Operator;
                        result.name  = planned_operator_marker(reference.identity, reference.registry_name, range);
                        result.range = range;
                        return result;
                    }
                case gir::ReferenceKind::Struct:
                    {
                        static_cast<void>(planned_structure(reference.identity, range));
                        Value result;
                        result.kind  = Value::Kind::Struct;
                        result.name  = reference.identity;
                        result.range = range;
                        return result;
                    }
                case gir::ReferenceKind::Intrinsic:
                    {
                        Value result;
                        result.kind  = Value::Kind::Intrinsic;
                        result.name  = reference.registry_name.empty() ? std::string{local_identity(reference.identity)}
                                                                       : reference.registry_name;
                        result.range = range;
                        return result;
                    }
            }
            backend(range, "hgraph IR contains an unsupported reference");
        }

        Value Emitter::eval_planned_expr(gir::ValueId id, Frame &frame) {
            const gir::Value &expression = planned_value(id, {});
            if (expression.constant) { return planned_literal(*expression.constant, expression.range); }
            return std::visit(
                [&](const auto &node) -> Value {
                    using T = std::decay_t<decltype(node)>;
                    if constexpr (std::is_same_v<T, gir::Literal>) {
                        return planned_literal(node.value, expression.range);
                    } else if constexpr (std::is_same_v<T, gir::Reference>) {
                        return eval_planned_reference(node, expression.range, frame);
                    } else if constexpr (std::is_same_v<T, gir::Unary>) {
                        const Value operand = eval_planned_expr(node.operand, frame);
                        if (operand.is_const() || operand.is_runtime()) {
                            return fold_unary(syntax_op(node.op), operand, expression.range);
                        }
                        if (!operand.is_port()) { backend(expression.range, "this operand has no value"); }
                        const std::string fallback = node.op == ir::hir::UnaryOp::Negate ? "neg_" : "not_";
                        return wire(planned_operator_marker(expression.operation.identity,
                                                            expression.operation.registry_name.empty()
                                                                ? std::string_view{fallback}
                                                                : std::string_view{expression.operation.registry_name},
                                                            expression.range),
                                    {operand.code}, expression.range);
                    } else if constexpr (std::is_same_v<T, gir::Binary>) {
                        const Value lhs = eval_planned_expr(node.lhs, frame);
                        const Value rhs = eval_planned_expr(node.rhs, frame);
                        if ((lhs.is_const() || lhs.is_runtime()) && (rhs.is_const() || rhs.is_runtime())) {
                            return fold_binary(syntax_op(node.op), lhs, rhs, expression.range);
                        }
                        if (expression.operation.registry_name.empty()) {
                            return wire_binary(syntax_op(node.op), lhs, rhs, expression.range);
                        }
                        const HType result = planned_type(expression.type, expression.range);
                        Value       value  = wire(planned_operator_marker(expression.operation.identity,
                                                                          expression.operation.registry_name, expression.range),
                                                  {argument_code(lhs), argument_code(rhs)}, expression.range, result);
                        value.code += ".as<" + schema(result, expression.range) + ">()";
                        return value;
                    } else if constexpr (std::is_same_v<T, gir::Call>) {
                        return eval_planned_call(expression, node, frame);
                    } else if constexpr (std::is_same_v<T, gir::Index>) {
                        const Value target = eval_planned_expr(node.target, frame);
                        const Value index  = eval_planned_expr(node.index, frame);
                        if (!target.is_port()) { unsupported(expression.range, "indexing a constant"); }
                        const std::string marker = planned_operator_marker(
                            expression.operation.identity,
                            expression.operation.registry_name.empty() ? std::string_view{"getitem_"}
                                                                       : std::string_view{expression.operation.registry_name},
                            expression.range);
                        return wire(marker, {target.code, argument_code(index)}, expression.range);
                    } else if constexpr (std::is_same_v<T, gir::Field>) {
                        const Value target = eval_planned_expr(node.target, frame);
                        if (target.kind == Value::Kind::Intrinsic && target.name == "logger") {
                            Value value;
                            value.kind  = Value::Kind::Intrinsic;
                            value.name  = "logger." + node.name;
                            value.range = expression.range;
                            return value;
                        }
                        if (!target.is_port()) { unsupported(expression.range, "field access on a constant"); }
                        const std::string marker = planned_operator_marker(
                            expression.operation.identity,
                            expression.operation.registry_name.empty() ? std::string_view{"getattr_"}
                                                                       : std::string_view{expression.operation.registry_name},
                            expression.range);
                        return wire(marker, {target.code, "hgraph::Str{" + quote(node.name) + "}"}, expression.range);
                    } else if constexpr (std::is_same_v<T, gir::Sequence>) {
                        unsupported(expression.range, "a list literal");
                    } else if constexpr (std::is_same_v<T, gir::Tuple>) {
                        unsupported(expression.range, "a tuple literal");
                    } else if constexpr (std::is_same_v<T, gir::Lambda>) {
                        backend(expression.range, "anonymous functions are not supported by the first pass");
                    } else if constexpr (std::is_same_v<T, gir::Conditional>) {
                        unsupported(expression.range, "'if' used as a value");
                    } else if constexpr (std::is_same_v<T, gir::BlockValue>) {
                        unsupported(expression.range, "a block used as a value");
                    } else if constexpr (std::is_same_v<T, gir::HarnessEval>) {
                        fail(Category::Type, expression.range, "'eval' is only valid in a test");
                    } else if constexpr (std::is_same_v<T, gir::Construct>) {
                        return eval_planned_construct(node.type, node.arguments, node.delta, expression.range, frame);
                    }
                },
                expression.node);
        }

        // ------------------------------------------------------------- calls

        std::vector<std::optional<gir::ValueId>>
        Emitter::bind_planned_arguments(gir::CallableId id, const std::vector<gir::Argument> &arguments, SourceRange range) {
            if (!id.valid() || id.value >= graph_.callables.size()) {
                backend(range, "hgraph IR contains an invalid called function ID");
            }
            const gir::Callable                     &target = graph_.callables[id.value];
            std::vector<std::optional<gir::ValueId>> bound(target.parameters.size());
            std::size_t                              next = 0;
            for (const gir::Argument &argument : arguments) {
                if (argument.name.empty()) {
                    if (next >= target.parameters.size()) {
                        fail(Category::Type, argument.range,
                             "'" + std::string{local_identity(target.identity)} + "' takes " +
                                 std::to_string(target.parameters.size()) + " arguments");
                    }
                    if (bound[next]) { fail(Category::Type, argument.range, "positional argument after a named one"); }
                    bound[next++] = argument.value;
                    continue;
                }
                const auto found = std::find_if(target.parameters.begin(), target.parameters.end(),
                                                [&](const gir::Parameter &parameter) { return parameter.name == argument.name; });
                if (found == target.parameters.end()) {
                    fail(Category::Name, argument.range,
                         "'" + std::string{local_identity(target.identity)} + "' has no parameter named '" + argument.name + "'");
                }
                const std::size_t index = static_cast<std::size_t>(found - target.parameters.begin());
                if (bound[index]) { fail(Category::Name, argument.range, "'" + argument.name + "' is given twice"); }
                bound[index] = argument.value;
                next         = std::max(next, index + 1U);
            }
            for (std::size_t index = 0; index < target.parameters.size(); ++index) {
                if (!bound[index] && !target.parameters[index].default_value.valid()) {
                    fail(Category::Type, range,
                         "'" + std::string{local_identity(target.identity)} + "' needs an argument for '" +
                             target.parameters[index].name + "'");
                }
            }
            return bound;
        }

        Value Emitter::call_planned_function(gir::CallableId id, const std::vector<gir::Argument> &arguments, SourceRange range,
                                             Frame &frame) {
            const ast::DeclId decl = syntax_callable(id, range);
            check_supported(decl);
            const gir::Callable     &target = graph_.callables[id.value];
            const auto               bound  = bind_planned_arguments(id, arguments, range);
            std::vector<std::string> args(target.parameters.size());
            for (std::size_t index = 0; index < target.parameters.size(); ++index) {
                const gir::Parameter &parameter = target.parameters[index];
                Value                 argument  = bound[index] ? eval_planned_expr(*bound[index], frame)
                                                               : planned_constant(parameter.default_value, target.range);
                const HType           type      = planned_type(parameter.type, target.range);
                args[index] = parameter.is_const ? as_const(argument, type, argument.range, "parameter '" + parameter.name + "'")
                                                 : as_port(argument, type, argument.range);
            }
            HType result;
            if (has_planned_result(target.result, target.range)) { result = planned_type(target.result, target.range); }
            Value value = wire(callable_cpp_name(decl), args, range, result);
            if (!has_planned_result(target.result, target.range)) { value.kind = Value::Kind::Void; }
            return value;
        }

        Value Emitter::lower_planned_map_call(const Value &callee, const gir::Call &call, SourceRange range, Frame &frame) {
            gir::ValueId       lambda_id{};
            std::vector<Value> inputs;
            for (const gir::Argument &argument : call.arguments) {
                const gir::Value &expression = planned_value(argument.value, argument.range);
                if (std::holds_alternative<gir::Lambda>(expression.node)) {
                    if (lambda_id.valid()) { backend(expression.range, "map takes one anonymous function"); }
                    lambda_id = argument.value;
                } else {
                    inputs.push_back(eval_planned_expr(argument.value, frame));
                }
            }
            if (!lambda_id.valid()) { backend(range, "map needs an anonymous function"); }
            if (inputs.empty()) { backend(range, "map needs at least one temporal map input"); }

            const gir::Value  &lambda_expression = planned_value(lambda_id, range);
            const gir::Lambda &anonymous         = std::get<gir::Lambda>(lambda_expression.node);
            if (anonymous.parameters.size() != inputs.size()) {
                fail(Category::Type, lambda_expression.range, "the map function parameter count must match its mapped inputs");
            }

            Frame lambda;
            lambda.fn = frame.fn;
            std::vector<std::string> parameters{"hgraph::Wiring &w"};
            for (std::size_t index = 0; index < inputs.size(); ++index) {
                const Value &input = inputs[index];
                if (!input.is_port() || input.type.kind != HType::Kind::Map) {
                    backend(input.range, "the first anonymous map slice takes temporal map inputs");
                }
                const gir::Binding &binding = planned_binding(anonymous.parameters[index], lambda_expression.range);
                if (binding.kind != gir::BindingKind::LambdaParameter) {
                    backend(binding.range, "hgraph IR lambda parameter has the wrong binding kind");
                }
                const HType       parameter_type = planned_type(binding.type, binding.range);
                const std::string name           = cpp_name(binding.name);
                Value             value          = make_port(name, parameter_type, binding.range);
                if (!lambda.planned_bindings.emplace(anonymous.parameters[index].value, value).second) {
                    backend(binding.range, "hgraph IR lambda repeats a parameter binding");
                }
                parameters.push_back("hgraph::Port<" + schema(parameter_type, binding.range) + "> " + name);
            }

            const Value lambda_result = eval_planned_expr(anonymous.body, lambda);
            const HType result_type =
                anonymous.result.valid() ? planned_type(anonymous.result, lambda_expression.range) : lambda_result.type;
            if (result_type.kind == HType::Kind::Unknown) {
                backend(planned_value(anonymous.body, lambda_expression.range).range,
                        "the anonymous map result type cannot be inferred");
            }

            const std::string helper = "hgl_anonymous_" + std::to_string(++anonymous_function_index_);
            generated_helpers_.line("// " + where(lambda_expression.range));
            generated_helpers_.open("struct " + helper);
            generated_helpers_.line("static constexpr auto name = " +
                                    quote(module_name_ + ".<anonymous:" + std::to_string(anonymous_function_index_) + ">") + ";");
            generated_helpers_.line("static hgraph::Port<" + schema(result_type, lambda_expression.range) + "> compose(" +
                                    join(parameters, ", ") + ")");
            generated_helpers_.open("");
            generated_helpers_.line("return " + as_port(lambda_result, result_type, lambda_expression.range) + ";");
            generated_helpers_.close();
            generated_helpers_.close(";");
            generated_helpers_.line();

            std::vector<std::string> planned_arguments{"hgraph::fn<" + helper + ">()"};
            for (const Value &input : inputs) { planned_arguments.push_back(argument_code(input)); }

            HType mapped;
            mapped.kind = HType::Kind::Map;
            mapped.children.push_back(inputs.front().type.children[0]);
            mapped.children.push_back(result_type);
            Value result = wire(callee.name, planned_arguments, range, mapped);
            result.code += ".as<" + schema(mapped, range) + ">()";
            return result;
        }

        Value Emitter::eval_planned_construct(gir::TypeId type_id, const std::vector<gir::Argument> &arguments, bool delta,
                                              SourceRange range, Frame &frame) {
            HType type = planned_type(type_id, range);
            if (type.kind == HType::Kind::Atomic) {
                if (type.children.size() != 1U) { backend(range, "an atomic constructor needs one value type"); }
                type = type.children.front();
            }
            if (type.kind != HType::Kind::Struct) { backend(range, "a generated constructor needs a nominal struct type"); }
            const gir::StructContract &contract         = planned_structure(type.nominal_identity, range);
            const PlannedTypeBindings  planned_generics = planned_struct_bindings(contract, type, range);

            const auto argument_for = [&](std::string_view field) -> const gir::Argument * {
                const auto found = std::find_if(arguments.begin(), arguments.end(),
                                                [&](const gir::Argument &argument) { return argument.name == field; });
                return found == arguments.end() ? nullptr : &*found;
            };

            std::vector<std::string>                   temporal_fields;
            std::vector<std::pair<std::size_t, Value>> delta_fields;
            for (std::size_t index = 0; index < contract.fields.size(); ++index) {
                const gir::StructField &field       = contract.fields[index];
                const gir::Argument    *argument    = argument_for(field.name);
                const HType             field_type  = planned_type(field.type, field.range, &planned_generics);
                const SourceRange       field_range = graph_type(field.type, field.range).range;

                if (argument == nullptr) {
                    if (delta) { continue; }
                    if (field.default_value.valid()) {
                        if (planned_null(field.default_value, field.range)) {
                            if (!field.optional) {
                                fail(Category::Type, graph_constant(field.default_value, field.range).range,
                                     "required field '" + field.name + "' cannot be null");
                            }
                            temporal_fields.push_back("hgraph::wire<hgraph::stdlib::nothing, " + schema(field_type, field_range) +
                                                      ">(w)");
                            continue;
                        }
                        Value value = planned_field_value(field.default_value, field.range, &planned_generics);
                        temporal_fields.push_back(as_port(value, field_type, value.range));
                        continue;
                    }
                    if (!field.optional) {
                        backend(range,
                                "struct '" + std::string{local_identity(contract.identity)} + "' needs field '" + field.name + "'");
                    }
                    temporal_fields.push_back("hgraph::wire<hgraph::stdlib::nothing, " + schema(field_type, field_range) + ">(w)");
                    continue;
                }

                if (planned_null(argument->value, argument->range)) {
                    if (delta) {
                        backend(argument->range, "clearing an optional struct field needs a native clear-delta operation");
                    }
                    temporal_fields.push_back("hgraph::wire<hgraph::stdlib::nothing, " + schema(field_type, field_range) + ">(w)");
                    continue;
                }

                Value value = eval_planned_expr(argument->value, frame);
                if (delta) {
                    if (!frame.runtime || (!value.is_const() && !value.is_runtime())) {
                        backend(argument->range, "a structured delta is only available in a runtime function");
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

        Value Emitter::eval_planned_intrinsic(const Value &callee, const gir::Call &call, SourceRange range, Frame &frame) {
            const std::string &name = callee.name;
            if (name.starts_with("logger.")) {
                if (!frame.runtime) { fail(Category::Phase, range, "logger methods are only available in runtime hooks"); }
                if (name != "logger.info") { unsupported(range, "logger method '" + name.substr(7) + "'"); }
                if (call.arguments.size() != 1U) {
                    fail(Category::Type, range, "'logger.info' takes one message in the first slice");
                }
                const Value message = eval_planned_expr(call.arguments.front().value, frame);
                if ((!message.is_const() && !message.is_runtime()) || !message.type.is(ast::ScalarType::Str)) {
                    fail(Category::Type, message.range, "'logger.info' takes a str message");
                }
                Value result;
                result.kind  = Value::Kind::Void;
                result.code  = "logger.log(2, " + message.code + ")";
                result.range = range;
                return result;
            }
            if (name == "valid" || name == "modified" || name == "all_valid") {
                if (call.arguments.empty()) {
                    fail(Category::Type, range, "'" + name + "' takes at least one time-series argument");
                }
                if (frame.runtime) {
                    std::vector<std::string> tests;
                    tests.reserve(call.arguments.size());
                    for (const gir::Argument &argument : call.arguments) {
                        const Value value = eval_planned_expr(argument.value, frame);
                        if (!value.is_runtime() || value.selector.empty()) {
                            fail(Category::Type, argument.range,
                                 "'" + name + "' takes time-series selectors in a runtime function");
                        }
                        const std::string method = name == "modified"    ? "modified()"
                                                   : name == "all_valid" ? "all_valid()"
                                                                         : "valid()";
                        tests.push_back(value.selector + "." + method);
                    }
                    return make_runtime("(" + join(tests, name == "modified" ? " || " : " && ") + ")",
                                        scalar_type(ast::ScalarType::Bool), range);
                }
                const std::string    operation = name == "modified" ? "modified" : "valid";
                const std::string    fold      = name == "modified" ? "or_" : "and_";
                std::optional<Value> result;
                for (const gir::Argument &argument : call.arguments) {
                    const Value value = eval_planned_expr(argument.value, frame);
                    if (!value.is_port()) { fail(Category::Type, argument.range, "'" + name + "' takes time-series arguments"); }
                    Value flag = wire("hgraph::stdlib::" + operation, {value.code}, range);
                    result     = result ? wire("hgraph::stdlib::" + fold, {result->code, flag.code}, range) : std::move(flag);
                }
                return std::move(*result);
            }
            if (name == "last_modified" || name == "last_modified_time" || name == "key_set") {
                if (call.arguments.size() != 1U) { fail(Category::Type, range, "'" + name + "' takes one time-series argument"); }
                const Value value = eval_planned_expr(call.arguments.front().value, frame);
                if (frame.runtime) {
                    if (name == "key_set") { backend(range, "runtime collection traversal is not supported by emit-cpp yet"); }
                    if (!value.is_runtime() || value.selector.empty()) {
                        fail(Category::Type, call.arguments.front().range,
                             "'last_modified' takes a time-series selector in a runtime function");
                    }
                    return make_runtime(value.selector + ".last_modified_time()", scalar_type(ast::ScalarType::DateTime), range);
                }
                if (!value.is_port()) {
                    fail(Category::Type, call.arguments.front().range, "'" + name + "' takes a time-series argument");
                }
                return wire(name == "key_set" ? "hgraph::stdlib::keys_" : "hgraph::stdlib::last_modified_time", {value.code},
                            range);
            }
            if (name == "keys" || name == "values" || name == "items") {
                if (!frame.runtime) {
                    backend(range, "'" + name + "' is a runtime traversal; it is not available in a composition body");
                }
                if (call.arguments.empty() || call.arguments.size() > 2U) {
                    fail(Category::Type, range, "'" + name + "' takes a collection and an optional predicate");
                }
                const Value source = eval_planned_expr(call.arguments.front().value, frame);
                if (!source.is_runtime() || source.selector.empty()) {
                    fail(Category::Type, source.range, "'" + name + "' takes a runtime collection selector");
                }

                std::string  predicate;
                gir::ValueId general_predicate{};
                if (call.arguments.size() == 2U) {
                    const gir::Argument &argument   = call.arguments[1];
                    const gir::Value    &expression = planned_value(argument.value, argument.range);
                    if (std::holds_alternative<gir::Lambda>(expression.node)) {
                        general_predicate = argument.value;
                    } else {
                        const Value value = eval_planned_expr(argument.value, frame);
                        if (value.kind != Value::Kind::Intrinsic || (value.name != "valid" && value.name != "modified" &&
                                                                     value.name != "added" && value.name != "removed")) {
                            fail(Category::Type, expression.range, "an iterator predicate is a metadata predicate or concise fn");
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
                result.kind                       = Value::Kind::Iterator;
                result.code                       = source.selector + "." + method + "()";
                result.type                       = source.type;
                result.name                       = name;
                result.range                      = range;
                result.planned_iterator_predicate = general_predicate;
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
            backend(range, "'" + name + "' is a runtime traversal; it is not available in a composition body of the first pass");
        }

        Value Emitter::eval_planned_call(const gir::Value &expression, const gir::Call &call, Frame &frame) {
            const Value callee = eval_planned_expr(call.callee, frame);
            if (frame.runtime && callee.kind != Value::Kind::Intrinsic && callee.kind != Value::Kind::Struct) {
                backend(expression.range, "calls in a runtime function are not supported by emit-cpp yet");
            }
            switch (callee.kind) {
                case Value::Kind::Operator:
                case Value::Kind::LocalOperator:
                    {
                        if ((expression.operation.registry_name == "map_" || callee.name == "hgraph::stdlib::map_") &&
                            std::ranges::any_of(call.arguments, [&](const gir::Argument &argument) {
                                return std::holds_alternative<gir::Lambda>(planned_value(argument.value, argument.range).node);
                            })) {
                            return lower_planned_map_call(callee, call, expression.range, frame);
                        }
                        const std::string marker =
                            expression.operation.kind == gir::OperationKind::NominalOperator
                                ? planned_operator_marker(expression.operation.identity, expression.operation.registry_name,
                                                          expression.range)
                                : callee.name;
                        std::vector<std::string> planned_arguments;
                        planned_arguments.reserve(call.arguments.size());
                        for (const gir::Argument &argument : call.arguments) {
                            std::string code = argument_code(eval_planned_expr(argument.value, frame));
                            if (!argument.name.empty()) { code = "hgraph::arg<" + quote(argument.name) + ">(" + code + ")"; }
                            planned_arguments.push_back(std::move(code));
                        }
                        return wire(marker, planned_arguments, expression.range);
                    }
                case Value::Kind::Struct:
                    return eval_planned_construct(expression.type, call.arguments, false, expression.range, frame);
                case Value::Kind::Function:
                    if (expression.operation.kind != gir::OperationKind::ExactFunction || !expression.operation.callable.valid()) {
                        backend(expression.range, "hgraph IR exact function call has no resolved callable");
                    }
                    if (callee.callable != expression.operation.callable) {
                        backend(expression.range, "hgraph IR exact function call disagrees with its callee reference");
                    }
                    return call_planned_function(expression.operation.callable, call.arguments, expression.range, frame);
                case Value::Kind::Intrinsic: return eval_planned_intrinsic(callee, call, expression.range, frame);
                case Value::Kind::Const:
                case Value::Kind::Port:
                case Value::Kind::Runtime:
                case Value::Kind::Iterator:
                case Value::Kind::Void: break;
            }
            fail(Category::Type, planned_value(call.callee, expression.range).range, "this value is not callable");
        }

        // -------------------------------------------------------- statements

        void Emitter::emit_return(const Value &value, Frame &frame, Writer &out, SourceRange range)
        {
            const gir::Callable &planned = callable(frame.fn);
            if (!has_planned_result(planned.result, planned.range)) {
                if (value.kind != Value::Kind::Void) {
                    fail(Category::Type, range, "'" + std::string{local_identity(planned.identity)} + "' has no result");
                }
                if (!value.code.empty()) { out.line(value.code + ";"); }
                out.line("return;");
                return;
            }
            const HType result = planned_type(planned.result, planned.range);
            out.line("return " + as_port(value, result, range) + ";");
        }

        void Emitter::emit_planned_statement(gir::StatementId id, Frame &frame, Writer &out, SourceRange fallback) {
            const gir::Statement &statement = planned_statement(id, fallback);
            std::visit(
                [&](const auto &node) {
                    using T = std::decay_t<decltype(node)>;
                    if constexpr (std::is_same_v<T, gir::LocalBinding>) {
                        const gir::Binding &binding = planned_binding(node.binding, statement.range);
                        if (binding.kind != gir::BindingKind::LocalLet && binding.kind != gir::BindingKind::LocalVar) {
                            backend(statement.range, "hgraph IR local statement refers to a non-local binding");
                        }
                        Value value = eval_planned_expr(node.init, frame);
                        if (value.kind != Value::Kind::Const && value.kind != Value::Kind::Port) {
                            unsupported(statement.range, "binding a function or operator to a local");
                        }
                        const HType declared = planned_type(node.type, statement.range);
                        if (value.is_const()) {
                            value.code = as_const(value, declared, value.range, "'" + binding.name + "'");
                        } else {
                            value.code = as_port(value, declared, value.range);
                        }
                        value.type = declared;

                        const std::string base   = cpp_name(binding.name);
                        std::string       local  = base;
                        int              &suffix = local_counts_[base];
                        while (local_names_.contains(local)) { local = base + "_" + std::to_string(++suffix); }
                        local_names_.insert(local);
                        out.line((binding.kind == gir::BindingKind::LocalVar ? "auto " : "const auto ") + local + " = " +
                                 value.code + ";");
                        value.code = local;
                        if (!frame.planned_bindings.emplace(node.binding.value, std::move(value)).second) {
                            backend(binding.range, "hgraph IR block repeats a local binding");
                        }
                    } else if constexpr (std::is_same_v<T, gir::Assignment>) {
                        const gir::Value &place     = planned_value(node.place, statement.range);
                        const auto       *reference = std::get_if<gir::Reference>(&place.node);
                        if (reference == nullptr || reference->kind != gir::ReferenceKind::Binding) {
                            backend(place.range, "assignment targets a local in the first pass");
                        }
                        const gir::Binding &binding = planned_binding(reference->binding, place.range);
                        if (binding.kind != gir::BindingKind::LocalVar) {
                            fail(Category::Type, place.range, "'" + binding.name + "' is not a 'var'");
                        }
                        const auto current_it = frame.planned_bindings.find(reference->binding.value);
                        if (current_it == frame.planned_bindings.end()) {
                            backend(place.range, "'" + binding.name + "' is not bound in this function");
                        }
                        const Value current = current_it->second;
                        Value       value   = eval_planned_expr(node.value, frame);
                        if (node.op != gir::AssignOp::Assign) {
                            const ast::BinaryOp op = node.op == gir::AssignOp::Add   ? ast::BinaryOp::Add
                                                     : node.op == gir::AssignOp::Sub ? ast::BinaryOp::Sub
                                                     : node.op == gir::AssignOp::Mul ? ast::BinaryOp::Mul
                                                                                     : ast::BinaryOp::Div;
                            value = current.is_const() && value.is_const() ? fold_binary(op, current, value, statement.range)
                                                                           : wire_binary(op, current, value, statement.range);
                        }
                        if (current.kind != value.kind) {
                            fail(Category::Type, statement.range, "assignment to '" + binding.name + "' changes its inferred type");
                        }
                        if (current.is_const()) {
                            value.code = as_const(value, current.type, value.range, "assignment to '" + binding.name + "'");
                            value.type = current.type;
                        } else if (current.is_port()) {
                            if (current.type.kind != HType::Kind::Unknown) {
                                value.code = as_port(value, current.type, value.range);
                            }
                            value.type = current.type;
                        }
                        out.line(current.code + " = " + value.code + ";");
                        value.code                                       = current.code;
                        frame.planned_bindings[reference->binding.value] = std::move(value);
                    } else if constexpr (std::is_same_v<T, gir::Return>) {
                        Value value;
                        if (node.value.valid()) { value = eval_planned_expr(node.value, frame); }
                        emit_return(value, frame, out, statement.range);
                    } else if constexpr (std::is_same_v<T, gir::Assert>) {
                        fail(Category::Type, statement.range, "'assert' is only valid in a test");
                    } else if constexpr (std::is_same_v<T, gir::Evaluate>) {
                        const gir::Value &expression = planned_value(node.value, statement.range);
                        if (const auto *branch = std::get_if<gir::Conditional>(&expression.node)) {
                            emit_planned_if(*branch, expression.range, frame, out);
                            return;
                        }
                        const Value value = eval_planned_expr(node.value, frame);
                        if (value.kind == Value::Kind::Void) {
                            out.line(value.code + ";");
                        } else {
                            out.line("(void)" + value.code + ";");
                        }
                    } else {
                        backend(statement.range, "runtime statements are not evaluated by the first pass");
                    }
                },
                statement.node);
        }

        void Emitter::emit_planned_if(const gir::Conditional &branch, SourceRange range, Frame &frame, Writer &out) {
            const gir::Value &condition_expression = planned_value(branch.condition, range);
            const Value       condition            = eval_planned_expr(branch.condition, frame);
            if (condition.is_port()) {
                backend(condition_expression.range,
                        "'if' over a time-series condition is not supported by the first pass; use if_then_else");
            }
            if (!condition.is_const() || !condition.type.is(ast::ScalarType::Bool)) {
                fail(Category::Type, condition_expression.range, "an 'if' condition is a bool");
            }
            out.open("if (" + condition.code + ")");
            emit_planned_block(branch.then_block, frame, out, false, range);
            out.close();
            if (!branch.otherwise.valid()) { return; }

            const gir::Value &otherwise = planned_value(branch.otherwise, range);
            out.open("else");
            if (const auto *block = std::get_if<gir::BlockValue>(&otherwise.node)) {
                emit_planned_block(block->block, frame, out, false, otherwise.range);
            } else if (const auto *chained = std::get_if<gir::Conditional>(&otherwise.node)) {
                emit_planned_if(*chained, otherwise.range, frame, out);
            } else {
                unsupported(otherwise.range, "this 'else' form");
            }
            out.close();
        }

        void Emitter::emit_planned_block(gir::BlockId id, Frame &frame, Writer &out, bool function_body, SourceRange fallback) {
            const gir::Block &block = planned_block(id, fallback);
            for (gir::StatementId statement : block.statements) { emit_planned_statement(statement, frame, out, block.range); }
            if (block.tail.valid()) {
                const gir::Value &tail = planned_value(block.tail, block.range);
                if (function_body) {
                    const Value value = eval_planned_expr(block.tail, frame);
                    emit_return(value, frame, out, tail.range);
                } else if (const auto *branch = std::get_if<gir::Conditional>(&tail.node)) {
                    emit_planned_if(*branch, tail.range, frame, out);
                } else {
                    const Value value = eval_planned_expr(block.tail, frame);
                    if (value.kind == Value::Kind::Void) {
                        out.line(value.code + ";");
                    } else {
                        out.line("(void)" + value.code + ";");
                    }
                }
                return;
            }
            if (function_body && has_planned_result(callable(frame.fn).result, callable(frame.fn).range) &&
                !planned_block_terminates(id, fallback)) {
                out.line("throw std::logic_error(\"" + std::string{local_identity(callable(frame.fn).identity)} +
                         ": reached the end of the body without a result\");");
            }
        }

        bool Emitter::planned_expression_terminates(gir::ValueId id, SourceRange fallback) {
            const gir::Value &expression = planned_value(id, fallback);
            if (const auto *block = std::get_if<gir::BlockValue>(&expression.node)) {
                return planned_block_terminates(block->block, expression.range);
            }
            const auto *branch = std::get_if<gir::Conditional>(&expression.node);
            return branch != nullptr && planned_block_terminates(branch->then_block, expression.range) &&
                   branch->otherwise.valid() && planned_expression_terminates(branch->otherwise, expression.range);
        }

        bool Emitter::planned_block_terminates(gir::BlockId id, SourceRange fallback) {
            const gir::Block &block = planned_block(id, fallback);
            if (block.tail.valid()) { return true; }
            if (block.statements.empty()) { return false; }
            const gir::Statement &last = planned_statement(block.statements.back(), block.range);
            if (std::holds_alternative<gir::Return>(last.node)) { return true; }
            if (const auto *expression = std::get_if<gir::Evaluate>(&last.node)) {
                return planned_expression_terminates(expression->value, last.range);
            }
            return false;
        }

        void Emitter::emit_runtime_if(const gir::Conditional &branch, SourceRange range, Frame &frame, Writer &out) {
            const gir::Value &condition_expression = planned_value(branch.condition, range);
            const Value       condition            = eval_planned_expr(branch.condition, frame);
            if ((!condition.is_const() && !condition.is_runtime()) || !condition.type.is(ast::ScalarType::Bool)) {
                fail(Category::Type, condition_expression.range, "an 'if' condition is a bool scalar");
            }
            out.open("if (" + condition.code + ")");
            emit_runtime_block(branch.then_block, frame, out, range);
            out.close();
            if (!branch.otherwise.valid()) { return; }

            const gir::Value &otherwise = planned_value(branch.otherwise, range);
            out.open("else");
            if (const auto *block = std::get_if<gir::BlockValue>(&otherwise.node)) {
                emit_runtime_block(block->block, frame, out, otherwise.range);
            } else if (const auto *chained = std::get_if<gir::Conditional>(&otherwise.node)) {
                emit_runtime_if(*chained, otherwise.range, frame, out);
            } else {
                unsupported(otherwise.range, "this runtime 'else' form");
            }
            out.close();
        }

        void Emitter::emit_runtime_stmt(gir::StatementId id, Frame &frame, Writer &out, SourceRange fallback) {
            const gir::Statement &statement = planned_statement(id, fallback);
            std::visit(
                [&](const auto &node) {
                    using T = std::decay_t<decltype(node)>;
                    if constexpr (std::is_same_v<T, gir::LocalBinding>) {
                        const gir::Binding &binding = planned_binding(node.binding, statement.range);
                        if (binding.kind != gir::BindingKind::LocalLet && binding.kind != gir::BindingKind::LocalVar) {
                            backend(statement.range, "hgraph IR local statement refers to a non-local binding");
                        }
                        Value value = eval_planned_expr(node.init, frame);
                        if (!value.is_const() && !value.is_runtime()) {
                            fail(Category::Type, statement.range, "a runtime local needs a scalar value");
                        }
                        const HType declared = planned_type(node.type, statement.range);
                        value.code           = as_runtime(value, declared, value.range, "'" + binding.name + "'");
                        value.type           = declared;

                        const std::string base   = cpp_name(binding.name);
                        std::string       local  = base;
                        int              &suffix = local_counts_[base];
                        while (local_names_.contains(local)) { local = base + "_" + std::to_string(++suffix); }
                        local_names_.insert(local);
                        out.line((binding.kind == gir::BindingKind::LocalVar ? "auto " : "const auto ") + local + " = " +
                                 value.code + ";");
                        value.code = local;
                        value.selector.clear();
                        value.kind   = Value::Kind::Runtime;
                        value.number = {};
                        if (!frame.planned_bindings.emplace(node.binding.value, std::move(value)).second) {
                            backend(binding.range, "hgraph IR block repeats a local binding");
                        }
                    } else if constexpr (std::is_same_v<T, gir::Assignment>) {
                        const gir::Value &place = planned_value(node.place, statement.range);
                        if (const auto *index = std::get_if<gir::Index>(&place.node)) {
                            const gir::Value &target_expression = planned_value(index->target, place.range);
                            const auto       *target_reference  = std::get_if<gir::Reference>(&target_expression.node);
                            if (target_reference != nullptr && target_reference->kind == gir::ReferenceKind::Binding) {
                                const gir::Binding &target = planned_binding(target_reference->binding, target_expression.range);
                                if (target.kind == gir::BindingKind::Capability && target.name == "out") {
                                    const gir::Callable &planned = callable(frame.fn);
                                    if (!frame.output_available || !has_planned_result(planned.result, planned.range)) {
                                        fail(Category::Phase, place.range, "'out' is not available in this lifecycle block");
                                    }
                                    const HType result = planned_type(planned.result, planned.range);
                                    if (result.kind != HType::Kind::Map || result.children.size() != 2U) {
                                        backend(place.range, "indexed output assignment currently requires a map result");
                                    }
                                    if (node.op != gir::AssignOp::Assign) {
                                        unsupported(statement.range, "compound assignment to an output collection child");
                                    }
                                    const Value key   = eval_planned_expr(index->index, frame);
                                    const Value value = eval_planned_expr(node.value, frame);
                                    out.line("hgl_output.set(" + as_runtime(key, result.children[0], key.range, "output key") +
                                             ", " + as_runtime(value, result.children[1], value.range, "output value") + ");");
                                    return;
                                }
                            }
                        }

                        const auto *reference = std::get_if<gir::Reference>(&place.node);
                        if (reference == nullptr || reference->kind != gir::ReferenceKind::Binding) {
                            backend(place.range, "runtime assignment currently targets a local, state value, or 'out'");
                        }
                        const gir::Binding &binding = planned_binding(reference->binding, place.range);
                        if (binding.kind == gir::BindingKind::LocalLet) {
                            fail(Category::Type, place.range, "'" + binding.name + "' is not a 'var'");
                        }
                        if (binding.kind != gir::BindingKind::LocalVar && binding.kind != gir::BindingKind::State &&
                            binding.kind != gir::BindingKind::Capability) {
                            backend(place.range, "'" + binding.name + "' is not writable in this hook");
                        }
                        if (binding.kind == gir::BindingKind::Capability && binding.name != "out") {
                            backend(place.range, "'" + binding.name + "' is not writable in this hook");
                        }
                        if (binding.kind == gir::BindingKind::Capability && !frame.output_available) {
                            fail(Category::Phase, place.range, "'out' is not available in this lifecycle block");
                        }
                        const auto current_it = frame.planned_bindings.find(reference->binding.value);
                        if (current_it == frame.planned_bindings.end()) {
                            backend(place.range, "'" + binding.name + "' is not bound in this function");
                        }
                        const Value current = current_it->second;
                        Value       value   = eval_planned_expr(node.value, frame);
                        if (node.op != gir::AssignOp::Assign) {
                            const ast::BinaryOp op = node.op == gir::AssignOp::Add   ? ast::BinaryOp::Add
                                                     : node.op == gir::AssignOp::Sub ? ast::BinaryOp::Sub
                                                     : node.op == gir::AssignOp::Mul ? ast::BinaryOp::Mul
                                                                                     : ast::BinaryOp::Div;
                            value                  = fold_binary(op, current, value, statement.range);
                        }
                        const std::string converted =
                            as_runtime(value, current.type, value.range, "assignment to '" + binding.name + "'");
                        if (binding.kind == gir::BindingKind::State || binding.kind == gir::BindingKind::Capability) {
                            if (current.selector.empty()) { backend(place.range, "this runtime value is not writable"); }
                            out.line(current.selector + ".set(" + converted + ");");
                        } else {
                            out.line(current.code + " = " + converted + ";");
                            Value updated                                    = current;
                            updated.kind                                     = Value::Kind::Runtime;
                            updated.number                                   = {};
                            frame.planned_bindings[reference->binding.value] = std::move(updated);
                        }
                    } else if constexpr (std::is_same_v<T, gir::Return>) {
                        if (!frame.output_available) {
                            fail(Category::Phase, statement.range, "'return' is not available in a lifecycle block");
                        }
                        if (node.value.valid()) {
                            const gir::Callable &planned = callable(frame.fn);
                            if (!has_planned_result(planned.result, planned.range)) {
                                fail(Category::Type, statement.range,
                                     "'" + std::string{local_identity(planned.identity)} + "' has no result");
                            }
                            const HType result = planned_type(planned.result, planned.range);
                            const Value value  = eval_planned_expr(node.value, frame);
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
                    } else if constexpr (std::is_same_v<T, gir::Activation>) {
                        const gir::Value &condition_expression = planned_value(node.condition, statement.range);
                        const Value       condition            = eval_planned_expr(node.condition, frame);
                        if ((!condition.is_const() && !condition.is_runtime()) || !condition.type.is(ast::ScalarType::Bool)) {
                            fail(Category::Type, condition_expression.range, "a 'when' condition is a bool scalar");
                        }
                        out.open("if (" + condition.code + ")");
                        emit_runtime_block(node.block, frame, out, statement.range);
                        out.close();
                    } else if constexpr (std::is_same_v<T, gir::Evaluate>) {
                        const gir::Value &expression = planned_value(node.value, statement.range);
                        if (const auto *branch = std::get_if<gir::Conditional>(&expression.node)) {
                            emit_runtime_if(*branch, expression.range, frame, out);
                            return;
                        }
                        const Value value = eval_planned_expr(node.value, frame);
                        out.line(value.kind == Value::Kind::Void ? value.code + ";" : "(void)" + value.code + ";");
                    } else if constexpr (std::is_same_v<T, gir::Assert>) {
                        fail(Category::Type, statement.range, "'assert' is only valid in a test");
                    } else if constexpr (std::is_same_v<T, gir::Traversal>) {
                        const gir::Value &iterable = planned_value(node.iterable, statement.range);
                        const Value       iterator = eval_planned_expr(node.iterable, frame);
                        if (!iterator.is_iterator()) {
                            fail(Category::Type, iterable.range,
                                 "a runtime 'for' loop needs keys(...), values(...), or items(...)");
                        }
                        if (node.bindings.empty() || node.bindings.size() > 2U) {
                            backend(statement.range, "hgraph IR traversal needs one or two loop bindings");
                        }
                        const bool pair = node.bindings.size() == 2U;
                        if (iterator.iterator_types.size() != node.bindings.size()) {
                            fail(Category::Type, statement.range,
                                 pair ? "this iterator yields one value" : "this iterator yields a pair");
                        }

                        std::vector<const gir::Binding *> bindings;
                        bindings.reserve(node.bindings.size());
                        for (gir::BindingId id : node.bindings) {
                            const gir::Binding &binding = planned_binding(id, statement.range);
                            if (binding.kind != gir::BindingKind::LoopValue) {
                                backend(binding.range, "hgraph IR traversal value has the wrong binding kind");
                            }
                            bindings.push_back(&binding);
                        }
                        const std::string first_raw  = "hgl_" + cpp_name(bindings[0]->name) + "_item";
                        const std::string second_raw = pair ? "hgl_" + cpp_name(bindings[1]->name) + "_item" : std::string{};
                        out.open(pair ? "for (const auto &[" + first_raw + ", " + second_raw + "] : " + iterator.code + ")"
                                      : "for (const auto &" + first_raw + " : " + iterator.code + ")");

                        const auto bind_value = [&](const std::string &raw, const HType &type, bool endpoint, bool list_index,
                                                    bool map_key) {
                            Value value;
                            value.kind  = Value::Kind::Runtime;
                            value.type  = type;
                            value.range = statement.range;
                            if (endpoint) {
                                value.selector = raw;
                                value.code     = iterator.type.kind == HType::Kind::List
                                                     ? raw + ".value().checked_as<" + value_type(type, statement.range) + ">()"
                                                     : raw + ".value()";
                            } else if (list_index) {
                                value.code = "static_cast<hgraph::Int>(" + raw + ")";
                            } else if (map_key) {
                                value.code = raw + ".checked_as<" + value_type(type, statement.range) + ">()";
                            } else {
                                value.code = raw;
                            }
                            return value;
                        };

                        const bool map  = iterator.type.kind == HType::Kind::Map;
                        const bool list = iterator.type.kind == HType::Kind::List;
                        std::vector<Value> loop_values;
                        loop_values.push_back(bind_value(first_raw, iterator.iterator_types[0],
                                                         !pair && (map || list) && iterator.name == "values", pair && list,
                                                         map && (pair || iterator.name == "keys")));
                        if (pair) { loop_values.push_back(bind_value(second_raw, iterator.iterator_types[1], true, false, false)); }
                        for (std::size_t index = 0; index < node.bindings.size(); ++index) {
                            if (!frame.planned_bindings.emplace(node.bindings[index].value, loop_values[index]).second) {
                                backend(bindings[index]->range, "hgraph IR traversal repeats a loop binding");
                            }
                        }

                        bool predicate_scope = false;
                        if (iterator.planned_iterator_predicate.valid()) {
                            const gir::Value &predicate_expression =
                                planned_value(iterator.planned_iterator_predicate, statement.range);
                            const auto *predicate = std::get_if<gir::Lambda>(&predicate_expression.node);
                            if (predicate == nullptr) {
                                backend(predicate_expression.range, "hgraph IR iterator predicate is not a lambda");
                            }
                            if (predicate->parameters.size() != loop_values.size()) {
                                fail(Category::Type, predicate_expression.range,
                                     "the iterator predicate parameter count must match its values");
                            }
                            Frame predicate_frame = frame;
                            for (std::size_t index = 0; index < predicate->parameters.size(); ++index) {
                                const gir::Binding &binding =
                                    planned_binding(predicate->parameters[index], predicate_expression.range);
                                if (binding.kind != gir::BindingKind::LambdaParameter) {
                                    backend(binding.range, "hgraph IR lambda parameter has the wrong binding kind");
                                }
                                if (!predicate_frame.planned_bindings
                                         .emplace(predicate->parameters[index].value, loop_values[index])
                                         .second) {
                                    backend(binding.range, "hgraph IR lambda repeats a parameter binding");
                                }
                            }
                            const gir::Value &body      = planned_value(predicate->body, predicate_expression.range);
                            const Value       condition = eval_planned_expr(predicate->body, predicate_frame);
                            if ((!condition.is_const() && !condition.is_runtime()) || !condition.type.is(ast::ScalarType::Bool)) {
                                fail(Category::Type, body.range, "an iterator predicate returns bool");
                            }
                            out.open("if (" + condition.code + ")");
                            predicate_scope = true;
                        }
                        emit_runtime_block(node.block, frame, out, statement.range);
                        if (predicate_scope) { out.close(); }
                        out.close();
                        for (gir::BindingId binding : node.bindings) { frame.planned_bindings.erase(binding.value); }
                    } else {
                        backend(statement.range, "state, inject, start, and stop are function-level runtime declarations");
                    }
                },
                statement.node);
        }

        void Emitter::emit_runtime_block(gir::BlockId id, Frame &frame, Writer &out, SourceRange fallback) {
            const gir::Block &block = planned_block(id, fallback);
            for (gir::StatementId statement : block.statements) { emit_runtime_stmt(statement, frame, out, block.range); }
            if (!block.tail.valid()) { return; }
            const gir::Value &expression = planned_value(block.tail, block.range);
            if (const auto *branch = std::get_if<gir::Conditional>(&expression.node)) {
                emit_runtime_if(*branch, expression.range, frame, out);
                return;
            }
            const Value value = eval_planned_expr(block.tail, frame);
            out.line(value.kind == Value::Kind::Void ? value.code + ";" : "(void)" + value.code + ";");
        }

        // ------------------------------------------------------ declarations

        std::optional<std::size_t> Emitter::runtime_parameter(gir::ValueId id, ast::DeclId decl) {
            const gir::Value &expression = planned_value(id, callable(decl).range);
            const auto       *reference  = std::get_if<gir::Reference>(&expression.node);
            if (reference == nullptr || reference->kind != gir::ReferenceKind::Binding) { return std::nullopt; }
            const gir::Callable &planned = callable(decl);
            for (std::size_t index = 0; index < planned.parameters.size(); ++index) {
                if (planned.parameters[index].binding == reference->binding && !planned.parameters[index].is_const) {
                    return index;
                }
            }
            return std::nullopt;
        }

        void Emitter::collect_runtime_activation(gir::ValueId id, ast::DeclId decl, RuntimeInfo &info) {
            const gir::Value &expression = planned_value(id, callable(decl).range);
            std::visit(
                [&](const auto &node) {
                    using T = std::decay_t<decltype(node)>;
                    if constexpr (std::is_same_v<T, gir::Call>) {
                        const gir::Value     &callee    = planned_value(node.callee, expression.range);
                        const gir::Reference *reference = std::get_if<gir::Reference>(&callee.node);
                        if (reference != nullptr && reference->kind == gir::ReferenceKind::Intrinsic &&
                            reference->registry_name == "modified") {
                            for (const gir::Argument &argument : node.arguments) {
                                const std::optional<std::size_t> parameter = runtime_parameter(argument.value, decl);
                                if (!parameter) {
                                    backend(argument.range, "the first runtime-node slice requires 'modified' arguments to be "
                                                            "temporal parameters");
                                }
                                info.active_parameters.insert(*parameter);
                            }
                            return;
                        }
                        collect_runtime_activation(node.callee, decl, info);
                        for (const gir::Argument &argument : node.arguments) {
                            collect_runtime_activation(argument.value, decl, info);
                        }
                    } else if constexpr (std::is_same_v<T, gir::Unary>) {
                        collect_runtime_activation(node.operand, decl, info);
                    } else if constexpr (std::is_same_v<T, gir::Binary>) {
                        collect_runtime_activation(node.lhs, decl, info);
                        collect_runtime_activation(node.rhs, decl, info);
                    } else if constexpr (std::is_same_v<T, gir::Index>) {
                        collect_runtime_activation(node.target, decl, info);
                        collect_runtime_activation(node.index, decl, info);
                    } else if constexpr (std::is_same_v<T, gir::Field>) {
                        collect_runtime_activation(node.target, decl, info);
                    } else if constexpr (std::is_same_v<T, gir::Conditional>) {
                        collect_runtime_activation(node.condition, decl, info);
                    }
                },
                expression.node);
        }

        void Emitter::check_runtime_expr(gir::ValueId id, ast::DeclId decl, const RuntimeValidSet &valid) {
            const gir::Value &expression = planned_value(id, callable(decl).range);
            std::visit(
                [&](const auto &node) {
                    using T = std::decay_t<decltype(node)>;
                    if constexpr (std::is_same_v<T, gir::Reference>) {
                        if (node.kind != gir::ReferenceKind::Binding) { return; }
                        const std::optional<std::size_t> parameter = runtime_parameter(id, decl);
                        if (parameter && !valid.contains(*parameter)) {
                            const gir::Binding &binding = planned_binding(node.binding, expression.range);
                            fail(Category::Type, expression.range,
                                 "temporal input '" + binding.name + "' may be invalid here; guard the read with valid(" +
                                     binding.name + ")");
                        }
                    } else if constexpr (std::is_same_v<T, gir::Unary>) {
                        check_runtime_expr(node.operand, decl, valid);
                    } else if constexpr (std::is_same_v<T, gir::Binary>) {
                        check_runtime_expr(node.lhs, decl, valid);
                        const RuntimeValidSet rhs_valid =
                            node.op == ir::hir::BinaryOp::And ? runtime_true_valid(node.lhs, decl, valid) : valid;
                        check_runtime_expr(node.rhs, decl, rhs_valid);
                    } else if constexpr (std::is_same_v<T, gir::Call>) {
                        const gir::Value      &callee    = planned_value(node.callee, expression.range);
                        const gir::Reference  *reference = std::get_if<gir::Reference>(&callee.node);
                        const std::string_view name =
                            reference == nullptr || reference->kind != gir::ReferenceKind::Intrinsic ? std::string_view{}
                            : reference->registry_name.empty() ? local_identity(reference->identity)
                                                               : std::string_view{reference->registry_name};
                        if (name == "valid" || name == "all_valid" || name == "modified" || name == "last_modified" ||
                            name == "last_modified_time") {
                            // Metadata intrinsics inspect endpoint selectors; they do not read payloads.
                            return;
                        }
                        check_runtime_expr(node.callee, decl, valid);
                        for (const gir::Argument &argument : node.arguments) { check_runtime_expr(argument.value, decl, valid); }
                    } else if constexpr (std::is_same_v<T, gir::Index>) {
                        check_runtime_expr(node.target, decl, valid);
                        check_runtime_expr(node.index, decl, valid);
                    } else if constexpr (std::is_same_v<T, gir::Field>) {
                        check_runtime_expr(node.target, decl, valid);
                    } else if constexpr (std::is_same_v<T, gir::Sequence>) {
                        for (const gir::SequenceElement &element : node.elements) {
                            if (element.key.valid()) { check_runtime_expr(element.key, decl, valid); }
                            check_runtime_expr(element.value, decl, valid);
                        }
                    } else if constexpr (std::is_same_v<T, gir::Tuple>) {
                        for (gir::ValueId element : node.elements) { check_runtime_expr(element, decl, valid); }
                    } else if constexpr (std::is_same_v<T, gir::Lambda>) {
                        check_runtime_expr(node.body, decl, valid);
                    } else if constexpr (std::is_same_v<T, gir::Conditional>) {
                        const RuntimeValidSet then_valid = runtime_true_valid(node.condition, decl, valid);
                        check_runtime_block(node.then_block, decl, then_valid);
                        if (node.otherwise.valid()) { check_runtime_expr(node.otherwise, decl, valid); }
                    } else if constexpr (std::is_same_v<T, gir::BlockValue>) {
                        check_runtime_block(node.block, decl, valid);
                    } else if constexpr (std::is_same_v<T, gir::HarnessEval>) {
                        check_runtime_expr(node.callee, decl, valid);
                        for (const gir::Argument &argument : node.arguments) { check_runtime_expr(argument.value, decl, valid); }
                    } else if constexpr (std::is_same_v<T, gir::Construct>) {
                        for (const gir::Argument &argument : node.arguments) { check_runtime_expr(argument.value, decl, valid); }
                    }
                },
                expression.node);
        }

        Emitter::RuntimeValidSet Emitter::runtime_true_valid(gir::ValueId id, ast::DeclId decl, const RuntimeValidSet &valid) {
            check_runtime_expr(id, decl, valid);
            RuntimeValidSet   result     = valid;
            const gir::Value &expression = planned_value(id, callable(decl).range);
            if (const auto *call = std::get_if<gir::Call>(&expression.node)) {
                const gir::Value      &callee    = planned_value(call->callee, expression.range);
                const gir::Reference  *reference = std::get_if<gir::Reference>(&callee.node);
                const std::string_view name      = reference == nullptr || reference->kind != gir::ReferenceKind::Intrinsic
                                                       ? std::string_view{}
                                                   : reference->registry_name.empty() ? local_identity(reference->identity)
                                                                                      : std::string_view{reference->registry_name};
                if (name == "valid" || name == "all_valid") {
                    for (const gir::Argument &argument : call->arguments) {
                        if (const std::optional<std::size_t> parameter = runtime_parameter(argument.value, decl)) {
                            result.insert(*parameter);
                        }
                    }
                }
                return result;
            }
            const auto *binary = std::get_if<gir::Binary>(&expression.node);
            if (binary == nullptr) { return result; }
            if (binary->op == ir::hir::BinaryOp::And) {
                result = runtime_true_valid(binary->lhs, decl, valid);
                return runtime_true_valid(binary->rhs, decl, result);
            }
            if (binary->op == ir::hir::BinaryOp::Or) {
                const RuntimeValidSet lhs = runtime_true_valid(binary->lhs, decl, valid);
                const RuntimeValidSet rhs = runtime_true_valid(binary->rhs, decl, valid);
                RuntimeValidSet       intersection;
                for (const std::size_t index : lhs) {
                    if (rhs.contains(index)) { intersection.insert(index); }
                }
                return intersection;
            }
            return result;
        }

        void Emitter::check_runtime_stmt(gir::StatementId id, ast::DeclId decl, const RuntimeValidSet &valid, bool allow_when,
                                         SourceRange fallback) {
            const gir::Statement &statement = planned_statement(id, fallback);
            std::visit(
                [&](const auto &node) {
                    using T = std::decay_t<decltype(node)>;
                    if constexpr (std::is_same_v<T, gir::LocalBinding>) {
                        check_runtime_expr(node.init, decl, valid);
                    } else if constexpr (std::is_same_v<T, gir::Activation>) {
                        if (!allow_when) { backend(statement.range, "a 'when' block must be at function top level"); }
                        const RuntimeValidSet body_valid = runtime_true_valid(node.condition, decl, valid);
                        check_runtime_block(node.block, decl, body_valid);
                    } else if constexpr (std::is_same_v<T, gir::Traversal>) {
                        check_runtime_expr(node.iterable, decl, valid);
                        check_runtime_block(node.block, decl, valid);
                    } else if constexpr (std::is_same_v<T, gir::Assignment>) {
                        check_runtime_expr(node.value, decl, valid);
                    } else if constexpr (std::is_same_v<T, gir::Return>) {
                        if (node.value.valid()) { check_runtime_expr(node.value, decl, valid); }
                    } else if constexpr (std::is_same_v<T, gir::Assert>) {
                        check_runtime_expr(node.condition, decl, valid);
                    } else if constexpr (std::is_same_v<T, gir::Evaluate>) {
                        check_runtime_expr(node.value, decl, valid);
                    }
                },
                statement.node);
        }

        void Emitter::check_runtime_block(gir::BlockId id, ast::DeclId decl, const RuntimeValidSet &valid, bool allow_when) {
            const gir::Block &block = planned_block(id, callable(decl).range);
            for (gir::StatementId statement : block.statements) {
                check_runtime_stmt(statement, decl, valid, allow_when, block.range);
            }
            if (block.tail.valid()) { check_runtime_expr(block.tail, decl, valid); }
        }

        RuntimeInfo Emitter::runtime_info(ast::DeclId decl) {
            const gir::Callable &planned = callable(decl);
            RuntimeInfo          info;

            if (planned.concise_body.valid() || !planned.block_body.valid()) {
                backend(planned.range, "a runtime function needs a block body");
            }
            if (has_planned_result(planned.result, planned.range)) {
                const HType result = planned_type(planned.result, planned.range);
                if (result.kind != HType::Kind::Scalar && result.kind != HType::Kind::Struct && result.kind != HType::Kind::Map) {
                    backend(graph_type(planned.result, planned.range).range,
                            "the runtime-node slice supports scalar, struct, and map outputs");
                }
            }

            std::size_t temporal_count = 0;
            for (const gir::Parameter &parameter : planned.parameters) {
                const gir::Binding    &binding = planned_binding(parameter.binding, planned.range);
                const gir::BindingKind expected =
                    parameter.is_const ? gir::BindingKind::ConstParameter : gir::BindingKind::SignalParameter;
                if (binding.kind != expected) { backend(binding.range, "hgraph IR runtime parameter has the wrong binding kind"); }
                const HType type = planned_type(parameter.type, planned.range);
                if (type.kind != HType::Kind::Scalar && type.kind != HType::Kind::Map && type.kind != HType::Kind::Set &&
                    type.kind != HType::Kind::List) {
                    backend(graph_type(parameter.type, planned.range).range,
                            "the runtime-node slice supports scalar and collection parameters");
                }
                if (!parameter.is_const) { ++temporal_count; }
            }
            if (temporal_count == 0) { backend(planned.range, "generated runtime sources are not supported by emit-cpp yet"); }

            std::unordered_set<std::uint32_t> injected_bindings;
            const gir::Block                 &body = planned_block(planned.block_body, planned.range);
            for (gir::StatementId id : body.statements) {
                const gir::Statement &statement = planned_statement(id, body.range);
                std::visit(
                    [&](const auto &node) {
                        using T = std::decay_t<decltype(node)>;
                        if constexpr (std::is_same_v<T, gir::StateBinding>) {
                            const gir::Binding &binding = planned_binding(node.binding, statement.range);
                            if (binding.kind != gir::BindingKind::State) {
                                backend(statement.range, "hgraph IR state statement refers to a non-state binding");
                            }
                            const HType type = planned_type(node.type, statement.range);
                            if (type.kind != HType::Kind::Scalar) {
                                backend(graph_type(node.type, statement.range).range,
                                        "the first runtime-node slice supports scalar state fields");
                            }
                            if (!node.init.valid()) {
                                backend(binding.range, "a generated runtime state field needs an initializer");
                            }
                            info.states.push_back(RuntimeState{.binding = node.binding,
                                                               .name    = binding.name,
                                                               .type    = type,
                                                               .init    = node.init,
                                                               .range   = binding.range});
                        } else if constexpr (std::is_same_v<T, gir::Inject>) {
                            for (gir::BindingId id : node.bindings) {
                                const gir::Binding &binding = planned_binding(id, statement.range);
                                if (binding.kind != gir::BindingKind::Capability) {
                                    backend(binding.range, "hgraph IR inject statement refers to a non-capability binding");
                                }
                                if (!injected_bindings.insert(id.value).second) {
                                    backend(binding.range, "hgraph IR runtime body repeats an injected capability binding");
                                }
                                const bool declared =
                                    std::ranges::any_of(planned.capabilities, [&](const gir::Capability &capability) {
                                        return capability.binding == id;
                                    });
                                if (!declared) {
                                    backend(binding.range, "hgraph IR inject binding is absent from the callable capabilities");
                                }
                                if (binding.name == "out") {
                                    if (info.out_binding.valid()) {
                                        backend(binding.range, "runtime function injects 'out' more than once");
                                    }
                                    info.out_binding = id;
                                } else if (binding.name == "logger") {
                                    if (info.logger_binding.valid()) {
                                        backend(binding.range, "runtime function injects 'logger' more than once");
                                    }
                                    info.logger_binding = id;
                                } else {
                                    backend(binding.range, "injectable '" + binding.name + "' is not supported by emit-cpp yet");
                                }
                            }
                        } else if constexpr (std::is_same_v<T, gir::Lifecycle>) {
                            static_cast<void>(planned_block(node.block, statement.range));
                            (node.kind == gir::LifecycleKind::Stop ? info.stop_blocks : info.start_blocks).push_back(node.block);
                        } else if constexpr (std::is_same_v<T, gir::Activation>) {
                            info.has_when = true;
                            collect_runtime_activation(node.condition, decl, info);
                        }
                    },
                    statement.node);
            }
            for (const gir::Capability &capability : planned.capabilities) {
                const gir::Binding &binding = planned_binding(capability.binding, planned.range);
                if (binding.kind != gir::BindingKind::Capability || binding.name != capability.name) {
                    backend(binding.range, "hgraph IR callable has an inconsistent capability binding");
                }
                if (!injected_bindings.contains(capability.binding.value)) {
                    backend(binding.range, "hgraph IR callable capability is absent from the runtime body");
                }
            }
            if (info.start_blocks.size() > 1U) {
                backend(planned_block(info.start_blocks[1], body.range).range, "a runtime function has at most one 'start' block");
            }
            if (info.stop_blocks.size() > 1U) {
                backend(planned_block(info.stop_blocks[1], body.range).range, "a runtime function has at most one 'stop' block");
            }
            if (info.has_when && info.active_parameters.empty()) {
                backend(planned.range, "a generated runtime function with 'when' needs a temporal parameter in 'modified(...)'");
            }
            if (!info.has_when) {
                for (std::size_t index = 0; index < planned.parameters.size(); ++index) {
                    if (!planned.parameters[index].is_const) { info.active_parameters.insert(index); }
                }
            }
            RuntimeValidSet valid;
            if (!info.has_when) { valid = info.active_parameters; }
            for (gir::StatementId id : body.statements) {
                const gir::Statement &statement = planned_statement(id, body.range);
                if (std::holds_alternative<gir::StateBinding>(statement.node) ||
                    std::holds_alternative<gir::Inject>(statement.node) || std::holds_alternative<gir::Lifecycle>(statement.node)) {
                    continue;
                }
                check_runtime_stmt(id, decl, valid, true, body.range);
            }
            if (body.tail.valid()) { check_runtime_expr(body.tail, decl, valid); }
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
            if (info.logger_binding.valid()) {
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
            const gir::Callable &planned   = callable(decl);
            frame.fn                       = decl;
            frame.runtime                  = true;
            frame.runtime_inputs_available = include_inputs;
            frame.output_available         = include_output;
            frame.params.resize(planned.parameters.size());
            frame.planned_bindings.clear();
            local_counts_.clear();
            local_names_.clear();
            local_names_.insert("hgl_state");
            local_names_.insert("hgl_output");
            local_names_.insert("logger");
            for (std::size_t index = 0; index < planned.parameters.size(); ++index) {
                const gir::Parameter &parameter = planned.parameters[index];
                const gir::Binding   &binding   = planned_binding(parameter.binding, planned.range);
                const HType           type      = planned_type(parameter.type, planned.range);
                const std::string     name      = cpp_name(parameter.name);
                local_names_.insert(name);
                frame.params[index] = parameter.is_const ? make_const(name + ".value()", type, binding.range)
                                                         : make_runtime(name + ".value()", type, binding.range, name);
                if (!frame.planned_bindings.emplace(parameter.binding.value, frame.params[index]).second) {
                    backend(binding.range, "hgraph IR callable repeats a parameter binding");
                }
            }
            for (const RuntimeState &state : info.states) {
                const std::string base   = cpp_name(state.name);
                std::string       local  = base;
                int              &suffix = local_counts_[base];
                while (local_names_.contains(local)) { local = base + "_" + std::to_string(++suffix); }
                local_names_.insert(local);
                out.line("[[maybe_unused]] auto " + local + " = hgl_state.field<" + quote(state.name) + ">();");
                Value value = make_runtime(local + ".value().checked_as<" + value_type(state.type, state.range) + ">()", state.type,
                                           state.range, local);
                if (!frame.planned_bindings.emplace(state.binding.value, std::move(value)).second) {
                    backend(state.range, "hgraph IR callable repeats a state binding");
                }
            }
            if (include_output && info.out_binding.valid()) {
                const HType result = planned_type(planned.result, planned.range);
                Value       value  = make_runtime("hgl_output.value().checked_as<" +
                                                      value_type(result, graph_type(planned.result, planned.range).range) + ">()",
                                                  result, planned_binding(info.out_binding, planned.range).range, "hgl_output");
                if (!frame.planned_bindings.emplace(info.out_binding.value, std::move(value)).second) {
                    backend(planned.range, "hgraph IR callable repeats the output capability binding");
                }
            }
            if (info.logger_binding.valid()) {
                Value logger;
                logger.kind = Value::Kind::Intrinsic;
                logger.name = "logger";
                if (!frame.planned_bindings.emplace(info.logger_binding.value, std::move(logger)).second) {
                    backend(planned.range, "hgraph IR callable repeats the logger capability binding");
                }
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

        void Emitter::emit_runtime_function(ast::DeclId decl, Writer &out) {
            const gir::Callable &planned = callable(decl);
            const RuntimeInfo    info    = runtime_info(decl);
            Frame                frame;
            frame.fn      = decl;
            frame.runtime = true;
            out.line("// " + where(planned.range));
            out.open("struct " + callable_cpp_name(decl));
            out.line("[[maybe_unused]] static constexpr auto name = " + quote(planned.identity) + ";");
            emit_defaults(planned, out);
            if (!info.states.empty()) {
                std::vector<std::string> fields;
                for (const RuntimeState &state : info.states) {
                    fields.push_back("hgraph::Field<" + quote(state.name) + ", " + schema(state.type, state.range) + ">");
                }
                out.line("using recordable_state = hgraph::TSB<" + quote(planned.identity + ".state") + ", " + join(fields, ", ") +
                         ">;");
            }

            if (!info.states.empty() || !info.start_blocks.empty()) {
                out.line("static void start(" + runtime_signature(decl, info, true, false, false) + ")");
                out.open("");
                prepare_runtime_frame(decl, info, frame, out, false, false);
                for (const RuntimeState &state : info.states) {
                    const Value &target = frame.planned_bindings.at(state.binding.value);
                    const Value  init   = eval_planned_expr(state.init, frame);
                    out.line("if (!" + target.selector + ".valid()) { " + target.selector + ".set(" +
                             as_runtime(init, state.type, init.range, "initializer of '" + state.name + "'") + "); }");
                }
                for (gir::BlockId block : info.start_blocks) { emit_runtime_block(block, frame, out, planned.range); }
                out.close();
            }

            const bool has_output = has_planned_result(planned.result, planned.range);
            out.line("static void eval(" + runtime_signature(decl, info, true, true, has_output) + ")");
            out.open("");
            prepare_runtime_frame(decl, info, frame, out, true, has_output);
            const gir::Block &body = planned_block(planned.block_body, planned.range);
            for (gir::StatementId id : body.statements) {
                const gir::Statement &statement = planned_statement(id, body.range);
                if (std::holds_alternative<gir::StateBinding>(statement.node) ||
                    std::holds_alternative<gir::Inject>(statement.node) || std::holds_alternative<gir::Lifecycle>(statement.node)) {
                    continue;
                }
                emit_runtime_stmt(id, frame, out, body.range);
            }
            if (body.tail.valid()) {
                const gir::Value &expression = planned_value(body.tail, body.range);
                if (const auto *branch = std::get_if<gir::Conditional>(&expression.node)) {
                    emit_runtime_if(*branch, expression.range, frame, out);
                } else {
                    const Value value = eval_planned_expr(body.tail, frame);
                    out.line(value.kind == Value::Kind::Void ? value.code + ";" : "(void)" + value.code + ";");
                }
            }
            out.close();

            if (!info.stop_blocks.empty()) {
                out.line("static void stop(" + runtime_signature(decl, info, true, false, false) + ")");
                out.open("");
                prepare_runtime_frame(decl, info, frame, out, false, false);
                emit_runtime_block(info.stop_blocks.front(), frame, out, planned.range);
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

        void Emitter::emit_struct(const gir::StructContract &item, Writer &out) {
            std::vector<std::string> template_parameters;
            std::vector<std::string> type_arguments;
            PlannedTypeBindings      generic_types;
            for (std::size_t index = 0; index < item.generics.size(); ++index) {
                const gir::GenericParameter &parameter = item.generics[index];
                if (!parameter.binding.valid() || parameter.binding.value >= graph_.bindings.size()) {
                    backend(item.range, "hgraph IR struct '" + item.identity + "' has an invalid generic binding");
                }
                const gir::Binding &binding = graph_.bindings[parameter.binding.value];
                if (parameter.is_const) {
                    if (binding.kind != gir::BindingKind::ConstParameter) {
                        backend(binding.range, "hgraph IR struct '" + item.identity + "' has a mismatched const binding");
                    }
                    backend(binding.range, "const generic struct arguments require typed constant Bundle metadata in hgraph");
                }
                if (binding.kind != gir::BindingKind::TypeParameter) {
                    backend(binding.range, "hgraph IR struct '" + item.identity + "' has a mismatched type binding");
                }
                const std::string name = cpp_name(parameter.name);
                template_parameters.push_back("typename " + name);
                HType type;
                type.kind     = HType::Kind::Generic;
                type.cpp_type = name;
                if (!generic_types.emplace(parameter.binding.value, type).second) {
                    backend(binding.range, "hgraph IR struct '" + item.identity + "' repeats a generic binding");
                }
                type_arguments.push_back(name);
            }

            const std::size_t identity_separator = item.identity.find_last_of('.');
            if (identity_separator == std::string::npos || identity_separator == 0U ||
                identity_separator + 1U == item.identity.size()) {
                backend(item.range, "hgraph IR struct '" + item.identity + "' has no module-qualified identity");
            }
            const std::string identity_module = item.identity.substr(0, identity_separator);
            const std::string identity_name   = item.identity.substr(identity_separator + 1U);

            out.line("// " + where(item.range));
            if (!template_parameters.empty()) { out.line("template <" + join(template_parameters, ", ") + ">"); }
            out.open("struct " + cpp_name(identity_name));

            std::vector<std::string> parents;
            for (const gir::TypeId parent : item.parents) {
                const gir::Type &planned = graph_type(parent, item.range);
                const HType      type    = planned_type(parent, item.range, &generic_types);
                parents.push_back(value_type(type, planned.range));
            }

            std::vector<std::string> value_fields;
            std::vector<std::string> temporal_fields;
            for (const gir::StructField &field : item.fields) {
                const gir::Type &planned = graph_type(field.type, field.range);
                const HType      type    = planned_type(field.type, field.range, &generic_types);
                value_fields.push_back("hgraph::Field<" + quote(field.name) + ", " +
                                       value_type(type, planned.range) + ">");
                temporal_fields.push_back("hgraph::Field<" + quote(field.name) + ", " +
                                          schema(type, planned.range) + ">");
            }

            std::vector<std::string> bundle_parts{quote(identity_module), quote(identity_name), item.abstract ? "true" : "false",
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
            const gir::Callable     &planned = callable(decl);
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
            frame.params.resize(planned.parameters.size());
            for (std::size_t i = 0; i < planned.parameters.size(); ++i) {
                const gir::Parameter &parameter      = planned.parameters[i];
                const gir::Binding   &binding        = planned_binding(parameter.binding, planned.range);
                const HType           type           = planned_type(parameter.type, planned.range);
                const std::string     parameter_name = cpp_name(parameter.name);
                local_names_.insert(parameter_name);
                if (parameter.is_const) {
                    if (binding.kind != gir::BindingKind::ConstParameter) {
                        backend(binding.range, "hgraph IR const parameter has the wrong binding kind");
                    }
                    frame.params[i] = make_const(parameter_name + ".value()", type, binding.range);
                } else {
                    if (binding.kind != gir::BindingKind::SignalParameter) {
                        backend(binding.range, "hgraph IR temporal parameter has the wrong binding kind");
                    }
                    frame.params[i] = make_port(parameter_name, type, binding.range);
                }
                if (!frame.planned_bindings.emplace(parameter.binding.value, frame.params[i]).second) {
                    backend(binding.range, "hgraph IR callable repeats a parameter binding");
                }
            }
            out.open("");
            if (planned.concise_body.valid() == planned.block_body.valid()) {
                backend(planned.range, "hgraph IR callable '" + std::string{callable_name(decl)} +
                                           "' must have exactly one concise or block body");
            }
            if (planned.concise_body.valid()) {
                const gir::Value &body  = planned_value(planned.concise_body, planned.range);
                const Value       value = eval_planned_expr(planned.concise_body, frame);
                emit_return(value, frame, out, body.range);
            } else {
                emit_planned_block(planned.block_body, frame, out, true, planned.range);
            }
            out.close();
            if (form == Form::InlineStruct) { out.close(";"); }
            out.line();
        }

        const gir::Value &Emitter::planned_value(gir::ValueId id, SourceRange fallback) {
            if (!id.valid() || id.value >= graph_.values.size()) {
                backend(fallback, "hgraph IR contains an invalid body value ID");
            }
            return graph_.values[id.value];
        }

        const gir::Statement &Emitter::planned_statement(gir::StatementId id, SourceRange fallback) {
            if (!id.valid() || id.value >= graph_.statements.size()) {
                backend(fallback, "hgraph IR contains an invalid body statement ID");
            }
            return graph_.statements[id.value];
        }

        const gir::Block &Emitter::planned_block(gir::BlockId id, SourceRange fallback) {
            if (!id.valid() || id.value >= graph_.blocks.size()) {
                backend(fallback, "hgraph IR contains an invalid body block ID");
            }
            return graph_.blocks[id.value];
        }

        ast::DeclId Emitter::syntax_callable(gir::CallableId id, SourceRange fallback) {
            if (!id.valid() || id.value >= graph_.callables.size()) {
                backend(fallback, "hgraph IR contains an invalid callable dependency ID");
            }
            const gir::Callable *planned = &graph_.callables[id.value];
            const auto           found   = std::find_if(callables_.begin(), callables_.end(),
                                                        [&](const auto &candidate) { return candidate.second == planned; });
            if (found == callables_.end()) { backend(fallback, "hgraph IR callable dependency has no syntax body adapter"); }
            return found->first;
        }

        void Emitter::collect_calls(gir::ValueId id, PlannedCalls &calls, SourceRange fallback) {
            const gir::Value &value = planned_value(id, fallback);
            if (!calls.values.insert(id.value).second) { return; }
            if (value.operation.kind == gir::OperationKind::ExactFunction) {
                if (!value.operation.callable.valid()) {
                    backend(value.range, "hgraph IR contains an invalid callable dependency ID");
                }
                calls.calls.insert(value.operation.callable.value);
            }
            std::visit(
                [&](const auto &node) {
                    using T = std::decay_t<decltype(node)>;
                    if constexpr (std::is_same_v<T, gir::Unary>) {
                        collect_calls(node.operand, calls, value.range);
                    } else if constexpr (std::is_same_v<T, gir::Binary>) {
                        collect_calls(node.lhs, calls, value.range);
                        collect_calls(node.rhs, calls, value.range);
                    } else if constexpr (std::is_same_v<T, gir::Call> || std::is_same_v<T, gir::HarnessEval>) {
                        collect_calls(node.callee, calls, value.range);
                        for (const gir::Argument &argument : node.arguments) {
                            collect_calls(argument.value, calls, argument.range);
                        }
                    } else if constexpr (std::is_same_v<T, gir::Index>) {
                        collect_calls(node.target, calls, value.range);
                        collect_calls(node.index, calls, value.range);
                    } else if constexpr (std::is_same_v<T, gir::Field>) {
                        collect_calls(node.target, calls, value.range);
                    } else if constexpr (std::is_same_v<T, gir::Sequence>) {
                        for (const gir::SequenceElement &element : node.elements) {
                            if (element.key.valid()) { collect_calls(element.key, calls, value.range); }
                            collect_calls(element.value, calls, value.range);
                        }
                    } else if constexpr (std::is_same_v<T, gir::Tuple>) {
                        for (gir::ValueId element : node.elements) { collect_calls(element, calls, value.range); }
                    } else if constexpr (std::is_same_v<T, gir::Lambda>) {
                        collect_calls(node.body, calls, value.range);
                    } else if constexpr (std::is_same_v<T, gir::Conditional>) {
                        collect_calls(node.condition, calls, value.range);
                        collect_calls(node.then_block, calls, value.range);
                        if (node.otherwise.valid()) { collect_calls(node.otherwise, calls, value.range); }
                    } else if constexpr (std::is_same_v<T, gir::BlockValue>) {
                        collect_calls(node.block, calls, value.range);
                    } else if constexpr (std::is_same_v<T, gir::Construct>) {
                        for (const gir::Argument &argument : node.arguments) {
                            collect_calls(argument.value, calls, argument.range);
                        }
                    }
                },
                value.node);
        }

        void Emitter::collect_calls(gir::BlockId id, PlannedCalls &calls, SourceRange fallback) {
            const gir::Block &block = planned_block(id, fallback);
            if (!calls.blocks.insert(id.value).second) { return; }
            for (gir::StatementId statement_id : block.statements) {
                const gir::Statement &statement = planned_statement(statement_id, block.range);
                std::visit(
                    [&](const auto &node) {
                        using T = std::decay_t<decltype(node)>;
                        if constexpr (std::is_same_v<T, gir::LocalBinding> || std::is_same_v<T, gir::StateBinding>) {
                            collect_calls(node.init, calls, statement.range);
                        } else if constexpr (std::is_same_v<T, gir::Lifecycle>) {
                            collect_calls(node.block, calls, statement.range);
                        } else if constexpr (std::is_same_v<T, gir::Activation>) {
                            collect_calls(node.condition, calls, statement.range);
                            collect_calls(node.block, calls, statement.range);
                        } else if constexpr (std::is_same_v<T, gir::Traversal>) {
                            collect_calls(node.iterable, calls, statement.range);
                            collect_calls(node.block, calls, statement.range);
                        } else if constexpr (std::is_same_v<T, gir::Assignment>) {
                            collect_calls(node.place, calls, statement.range);
                            collect_calls(node.value, calls, statement.range);
                        } else if constexpr (std::is_same_v<T, gir::Return>) {
                            if (node.value.valid()) { collect_calls(node.value, calls, statement.range); }
                        } else if constexpr (std::is_same_v<T, gir::Assert>) {
                            collect_calls(node.condition, calls, statement.range);
                        } else if constexpr (std::is_same_v<T, gir::Evaluate>) {
                            collect_calls(node.value, calls, statement.range);
                        }
                    },
                    statement.node);
            }
            if (block.tail.valid()) { collect_calls(block.tail, calls, block.range); }
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
                const gir::Callable &fn = callable(id);
                PlannedCalls         calls;
                if (fn.concise_body.valid() == fn.block_body.valid()) {
                    backend(fn.range, "hgraph IR callable '" + std::string{callable_name(id)} +
                                          "' must have exactly one concise or block body");
                }
                if (fn.concise_body.valid()) {
                    collect_calls(fn.concise_body, calls, fn.range);
                } else {
                    collect_calls(fn.block_body, calls, fn.range);
                }
                std::set<ast::DeclId> filtered;
                for (const std::uint32_t call_id : calls.calls) {
                    const ast::DeclId call = syntax_callable(gir::CallableId{call_id}, fn.range);
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
                    backend(callable(id).range,
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
            for (const gir::StructContract &item : graph_.structures) { emit_struct(item, header); }
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
