#include "codegen/cpp_emitter.h"

#include "descriptor/module_descriptor.h"
#include "hgraph_ir/control_flow.h"

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
    namespace hir = ir::hir;

    namespace
    {
        using syntax::Category;
        using syntax::SourceRange;

        struct Abort
        {};

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
                Reference,
                Signal,
                Generic,
                Struct,
            };

            Kind               kind{Kind::Unknown};
            hir::ScalarType    scalar{hir::ScalarType::Bool};
            std::vector<HType> children{};
            std::string        size{};      ///< list fixed size / rolling max, as C++ text
            std::string        min_size{};  ///< rolling minimum, as C++ text
            bool               duration_window{false};
            std::string        nominal_identity{};
            std::string        cpp_type{};

            [[nodiscard]] bool is(hir::ScalarType s) const noexcept { return kind == Kind::Scalar && scalar == s; }
            [[nodiscard]] bool numeric() const noexcept { return is(hir::ScalarType::I64) || is(hir::ScalarType::F64); }
        };

        HType scalar_type(hir::ScalarType scalar) {
            HType type;
            type.kind   = HType::Kind::Scalar;
            type.scalar = scalar;
            return type;
        }

        HType reference_type(HType type) {
            if (type.kind == HType::Kind::Reference) { return type; }
            HType reference;
            reference.kind = HType::Kind::Reference;
            reference.children.push_back(std::move(type));
            return reference;
        }

        bool same_type(const HType &a, const HType &b) {
            if (a.kind != b.kind || a.children.size() != b.children.size()) { return false; }
            if (a.kind == HType::Kind::Scalar && a.scalar != b.scalar) { return false; }
            if (a.nominal_identity != b.nominal_identity || a.cpp_type != b.cpp_type) { return false; }
            if (a.size != b.size || a.min_size != b.min_size || a.duration_window != b.duration_window) { return false; }
            for (std::size_t i = 0; i < a.children.size(); ++i) {
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
                Iterator,  ///< a phase-specific collection traversal plan
                Function,
                NativeFunction,
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
            HType                 type{};
            gir::CallableId       callable{};
            gir::NativeFunctionId native_function{};
            std::string           name{};
            SourceRange           range{};
            bool                  structured_delta{false};
            std::vector<HType>    iterator_types{};
            gir::ValueId          planned_iterator_predicate{};
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
            gir::CallableId                          fn{};
            std::vector<Value>                       params{};
            std::unordered_map<std::uint32_t, Value> planned_bindings{};
            bool                                     runtime{false};
            bool                                     runtime_inputs_available{true};
            bool                                     output_available{false};
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
            "alignas",
            "alignof",
            "and",
            "and_eq",
            "asm",
            "auto",
            "bitand",
            "bitor",
            "bool",
            "break",
            "case",
            "catch",
            "char",
            "char8_t",
            "char16_t",
            "char32_t",
            "class",
            "compl",
            "concept",
            "const",
            "consteval",
            "constexpr",
            "constinit",
            "const_cast",
            "continue",
            "co_await",
            "co_return",
            "co_yield",
            "decltype",
            "default",
            "delete",
            "do",
            "double",
            "dynamic_cast",
            "else",
            "enum",
            "explicit",
            "export",
            "extern",
            "false",
            "float",
            "for",
            "friend",
            "goto",
            "if",
            "inline",
            "int",
            "long",
            "mutable",
            "namespace",
            "new",
            "noexcept",
            "not",
            "not_eq",
            "nullptr",
            "operator",
            "or",
            "or_eq",
            "private",
            "protected",
            "public",
            "register",
            "reinterpret_cast",
            "requires",
            "return",
            "short",
            "signed",
            "sizeof",
            "static",
            "static_assert",
            "static_cast",
            "struct",
            "switch",
            "template",
            "this",
            "thread_local",
            "throw",
            "true",
            "try",
            "typedef",
            "typeid",
            "typename",
            "union",
            "unsigned",
            "using",
            "virtual",
            "void",
            "volatile",
            "wchar_t",
            "while",
            "xor",
            "xor_eq",
            // names the generated code uses itself
            "w",
            "hgraph",
            "std",
            "operators",
            "operator_contracts",
            "register_operators",
            "compose",
            "name",
            "defaults",
            "recordable_state",
            "hgl_state",
            "hgl_output",
        };

        constexpr std::string_view python_keywords[] = {
            "False",  "None",     "True", "and",    "as",      "assert", "async",  "await",  "break", "class",  "continue", "def",
            "del",    "elif",     "else", "except", "finally", "for",    "from",   "global", "if",    "import", "in",       "is",
            "lambda", "nonlocal", "not",  "or",     "pass",    "raise",  "return", "try",    "while", "with",   "yield",
        };

        /// An HGL identifier as a C++ identifier: a keyword or a name the
        /// generated code reserves gets a trailing underscore.
        std::string cpp_name(std::string_view name) {
            for (const std::string_view keyword : cpp_keywords) {
                if (keyword == name) { return std::string{name} + "_"; }
            }
            return std::string{name};
        }

        bool is_python_keyword(std::string_view name) {
            return std::find(std::begin(python_keywords), std::end(python_keywords), name) != std::end(python_keywords);
        }

        bool is_python_identifier(std::string_view name) {
            if (name.empty() || !((name.front() >= 'a' && name.front() <= 'z') || (name.front() >= 'A' && name.front() <= 'Z') ||
                                  name.front() == '_')) {
                return false;
            }
            return std::all_of(name.begin() + 1, name.end(), [](char c) {
                return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_';
            });
        }

        bool is_public_header_name(std::string_view name) {
            if (name.empty() || name.front() == '/' || name.contains("..")) { return false; }
            return std::ranges::all_of(name, [](char c) {
                return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '-' ||
                       c == '.' || c == '/' || c == '+';
            });
        }

        [[nodiscard]] constexpr bool cpp_identifier_start(char value) noexcept {
            return (value >= 'a' && value <= 'z') || (value >= 'A' && value <= 'Z') || value == '_';
        }

        [[nodiscard]] constexpr bool cpp_identifier_continue(char value) noexcept {
            return cpp_identifier_start(value) || (value >= '0' && value <= '9');
        }

        /// HGraph IR can be constructed independently of a descriptor reader;
        /// recheck the executable token boundary before inserting it into C++.
        [[nodiscard]] bool exact_cpp_symbol(std::string_view value) noexcept {
            if (value.starts_with("::")) { value.remove_prefix(2U); }
            if (value.empty()) { return false; }
            while (true) {
                if (!cpp_identifier_start(value.front())) { return false; }
                std::size_t length = 1U;
                while (length < value.size() && cpp_identifier_continue(value[length])) { ++length; }
                value.remove_prefix(length);
                if (value.empty()) { return true; }
                if (!value.starts_with("::")) { return false; }
                value.remove_prefix(2U);
                if (value.empty()) { return false; }
            }
        }

        /// The normal Python spelling of an HGL export. Keywords and the
        /// wrapper's public metadata name get a trailing underscore; a
        /// collision after this mapping is diagnosed when the module emits.
        std::string python_name(std::string_view name) {
            return is_python_keyword(name) || name == "__all__" ? std::string{name} + "_" : std::string{name};
        }

        std::string quote(std::string_view text) {
            std::string out = "\"";
            for (const char c : text) {
                switch (c) {
                    case '\\': out += "\\\\"; break;
                    case '"': out += "\\\""; break;
                    case '\n': out += "\\n"; break;
                    case '\r': out += "\\r"; break;
                    case '\t': out += "\\t"; break;
                    default:
                        if (static_cast<unsigned char>(c) < 0x20) {
                            char buffer[8];
                            std::snprintf(buffer, sizeof buffer, "\\x%02x", static_cast<unsigned>(static_cast<unsigned char>(c)));
                            out += buffer;
                        } else {
                            out += c;
                        }
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

        std::string join(const std::vector<std::string> &parts, std::string_view separator) {
            std::string out;
            for (std::size_t i = 0; i < parts.size(); ++i) {
                if (i != 0) { out += separator; }
                out += parts[i];
            }
            return out;
        }

        /// Indented line-oriented output.
        class Writer
        {
          public:
            void line(std::string_view text = {}) {
                if (!text.empty()) { out_.append(static_cast<std::size_t>(indent_) * 4, ' '); }
                out_ += text;
                out_ += '\n';
            }
            void open(std::string_view text) {
                if (!text.empty()) { line(text); }
                line("{");
                ++indent_;
            }
            void close(std::string_view suffix = {}) {
                --indent_;
                line("}" + std::string{suffix});
            }
            void                      indent() { ++indent_; }
            void                      dedent() { --indent_; }
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
            Emitter(const syntax::SourceFile &file, const gir::Module &graph, const EmitOptions &options,
                    syntax::DiagnosticSink &diagnostics)
                : file_{file}, graph_{graph}, options_{options}, diagnostics_{diagnostics} {}

            [[nodiscard]] EmittedModule emit();

          private:
            // -- diagnostics
            [[noreturn]] void fail(Category category, SourceRange range, std::string message) {
                diagnostics_.report(category, range, std::move(message));
                throw Abort{};
            }
            [[noreturn]] void backend(SourceRange range, std::string message) {
                fail(Category::Backend, range, std::move(message));
            }
            [[noreturn]] void unsupported(SourceRange range, std::string what) {
                backend(range, std::move(what) + " is not supported by emit-cpp yet");
            }

            void                                       bind_hgraph_declarations();
            [[nodiscard]] const gir::Binding          &planned_binding(gir::BindingId id, SourceRange fallback);
            [[nodiscard]] const gir::Callable         &callable(gir::CallableId id, SourceRange fallback = {});
            [[nodiscard]] const gir::NativeFunction   &native_function(gir::NativeFunctionId id, SourceRange fallback = {});
            [[nodiscard]] const gir::OperatorContract &operator_decl(gir::OperatorId id, SourceRange fallback = {});
            [[nodiscard]] const gir::StructContract   &struct_contract(gir::StructId id, SourceRange fallback = {});
            [[nodiscard]] static std::string_view      local_identity(std::string_view identity) noexcept;
            [[nodiscard]] std::string_view             callable_name(gir::CallableId id);
            [[nodiscard]] std::string                  callable_cpp_name(gir::CallableId id);
            [[nodiscard]] static std::string           operator_registry_name(const gir::OperatorContract &op) {
                return op.registry_name.empty() ? op.identity : op.registry_name;
            }
            [[nodiscard]] std::string where(SourceRange range) const {
                const syntax::Location at = file_.location(range.begin);
                return basename_ + ":" + std::to_string(at.line);
            }

            // -- types
            using PlannedTypeBindings = std::unordered_map<std::uint32_t, HType>;
            [[nodiscard]] HType                      planned_type(gir::TypeId id, SourceRange fallback = {},
                                                                  const PlannedTypeBindings *bindings = nullptr);
            [[nodiscard]] const gir::StructContract &planned_structure(std::string_view identity, SourceRange fallback);
            [[nodiscard]] PlannedTypeBindings        planned_struct_bindings(const gir::StructContract &contract, const HType &type,
                                                                             SourceRange fallback);
            [[nodiscard]] const gir::Type           &graph_type(gir::TypeId id, SourceRange fallback);
            [[nodiscard]] const gir::ConstExpr      &graph_constant(gir::ConstExprId id, SourceRange fallback);
            [[nodiscard]] gir::ConstExprId           materialized_constant(gir::ConstExprId id, SourceRange fallback);
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
            [[nodiscard]] std::string                 value_type(const HType &type, SourceRange range);
            [[nodiscard]] std::string                 schema(const HType &type, SourceRange range);
            [[nodiscard]] std::string                 materialization_cpp_name(const gir::Materialization &item, std::size_t index);
            void                                      begin_materialization(const gir::Materialization &item, std::size_t index);
            void                                      end_materialization();
            [[nodiscard]] std::string_view            active_callable_identity(const gir::Callable &item) const noexcept;

            // -- expressions
            [[nodiscard]] Value eval_planned_expr(gir::ValueId id, Frame &frame);
            [[nodiscard]] Value lower_planned_conditional(
                gir::ValueId id, const gir::Conditional &branch, SourceRange range, Frame &frame, bool result_used = true,
                std::optional<gir::ConditionalContinuationPlan> continuation = std::nullopt, bool *returns_from_callable = nullptr);
            void emit_planned_conditional_branch(std::string_view name, const gir::ConditionalBranchPlan &branch,
                                                 const gir::ConditionalPlan                       &plan,
                                                 const std::vector<std::pair<std::string, HType>> &parameters, Frame &outer,
                                                 const std::vector<gir::ConditionalResultSlot> &results,
                                                 std::string_view result_schema, SourceRange range);
            void emit_planned_callable_path(const gir::ConditionalContinuationSegment      &segment,
                                            std::optional<gir::ConditionalContinuationPlan> following, Frame &frame, Writer &out,
                                            SourceRange fallback);
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
            [[nodiscard]] Value       fold_unary(hir::UnaryOp op, const Value &operand, SourceRange range);
            [[nodiscard]] Value       fold_binary(hir::BinaryOp op, const Value &lhs, const Value &rhs, SourceRange range);
            [[nodiscard]] Value       wire_binary(hir::BinaryOp op, const Value &lhs, const Value &rhs, SourceRange range);
            [[nodiscard]] Value       wire(std::string marker, const std::vector<std::string> &args, SourceRange range,
                                           const HType &result = HType{});
            [[nodiscard]] std::string argument_code(const Value &value);
            [[nodiscard]] std::string as_port(const Value &value, const HType &temporal, SourceRange range);
            [[nodiscard]] std::string as_const(const Value &value, const HType &target, SourceRange range, const std::string &what);
            [[nodiscard]] Value       call_planned_native(gir::NativeFunctionId id, const std::vector<gir::Argument> &arguments,
                                                          SourceRange range, Frame &frame);
            // -- statements
            void emit_planned_block(gir::BlockId id, Frame &frame, Writer &out, bool function_body, SourceRange fallback);
            void emit_planned_statement(gir::StatementId id, Frame &frame, Writer &out, SourceRange fallback);
            void emit_planned_traversal(const gir::Traversal &traversal, SourceRange range, Frame &frame, Writer &out);
            void emit_planned_if(const gir::Conditional &branch, SourceRange range, Frame &frame, Writer &out);
            void emit_return(const Value &value, Frame &frame, Writer &out, SourceRange range);
            void emit_runtime_stmt(gir::StatementId id, Frame &frame, Writer &out, SourceRange fallback);
            void emit_runtime_block(gir::BlockId id, Frame &frame, Writer &out, SourceRange fallback);
            void emit_runtime_if(const gir::Conditional &branch, SourceRange range, Frame &frame, Writer &out);
            [[nodiscard]] bool        planned_expression_terminates(gir::ValueId id, SourceRange fallback);
            [[nodiscard]] bool        planned_block_terminates(gir::BlockId id, SourceRange fallback);
            [[nodiscard]] std::string as_runtime(const Value &value, const HType &target, SourceRange range,
                                                 const std::string &what);

            // -- declarations
            void                      check_supported(gir::CallableId id);
            [[nodiscard]] std::string signature(gir::CallableId id, bool with_names);
            [[nodiscard]] std::string result_type(gir::CallableId id);
            [[nodiscard]] std::string operator_contract(const std::vector<gir::Parameter> &parameters, gir::TypeId result,
                                                        std::string_view registry_name);
            /// How a function is written: its struct declaration (header), the
            /// whole struct inline (module-internal helpers and operator
            /// implementations), or the out-of-line `compose` definition of a
            /// struct the header declared (exports).
            enum class Form : std::uint8_t {
                Declaration,
                InlineStruct,
                OutOfLine,
            };
            void                                      emit_function(gir::CallableId id, Writer &out, Form form);
            void                                      emit_struct(const gir::StructContract &item, Writer &out);
            void                                      emit_runtime_function(gir::CallableId id, Writer &out);
            [[nodiscard]] RuntimeInfo                 runtime_info(gir::CallableId id);
            [[nodiscard]] std::optional<std::size_t>  runtime_parameter(gir::ValueId id, gir::CallableId callable_id);
            [[nodiscard]] std::optional<std::size_t>  runtime_root_parameter(gir::ValueId id, gir::CallableId callable_id);
            [[nodiscard]] std::optional<std::string>  runtime_scalar_key(gir::ValueId id, gir::CallableId callable_id);
            [[nodiscard]] std::optional<std::int64_t> runtime_integer_constant(gir::ValueId id, gir::CallableId callable_id);
            [[nodiscard]] std::optional<std::string>  runtime_selector_key(gir::ValueId id, gir::CallableId callable_id);
            void collect_runtime_activation(gir::ValueId id, gir::CallableId callable_id, RuntimeInfo &info);
            using RuntimeValidSet = std::unordered_set<std::string>;
            void check_runtime_expr(gir::ValueId id, gir::CallableId callable_id, const RuntimeValidSet &valid);
            void check_runtime_selector(gir::ValueId id, gir::CallableId callable_id, const RuntimeValidSet &valid);
            [[nodiscard]] RuntimeValidSet runtime_true_valid(gir::ValueId id, gir::CallableId callable_id,
                                                             const RuntimeValidSet &valid);
            void check_runtime_block(gir::BlockId id, gir::CallableId callable_id, const RuntimeValidSet &valid,
                                     bool allow_when = false);
            void check_runtime_stmt(gir::StatementId id, gir::CallableId callable_id, const RuntimeValidSet &valid, bool allow_when,
                                    SourceRange fallback);
            [[nodiscard]] std::string runtime_signature(gir::CallableId id, const RuntimeInfo &info, bool with_names,
                                                        bool include_inputs, bool include_output);
            void prepare_runtime_frame(gir::CallableId id, const RuntimeInfo &info, Frame &frame, Writer &out, bool include_inputs,
                                       bool include_output);
            void emit_defaults(const gir::Callable &callable, Writer &out);
            [[nodiscard]] std::vector<gir::CallableId> ordered_internal_functions();
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

            const syntax::SourceFile    &file_;
            const gir::Module           &graph_;
            const EmitOptions           &options_;
            syntax::DiagnosticSink      &diagnostics_;
            std::string                  basename_{};
            std::string                  namespace_{};
            std::string                  module_name_{};
            std::vector<gir::StructId>   structure_declarations_{};
            std::vector<gir::OperatorId> operator_declarations_{};
            std::vector<gir::CallableId> callable_declarations_{};
            bool                         uses_analytics_{false};
            /// Locals declared in the current function, for unique C++ names.
            std::unordered_map<std::string, int>                local_counts_{};
            std::unordered_set<std::string>                     local_names_{};
            Writer                                              generated_helpers_{};
            std::size_t                                         anonymous_function_index_{0};
            Writer                                             *current_body_{nullptr};
            std::size_t                                         conditional_index_{0};
            std::size_t                                         traversal_index_{0};
            PlannedTypeBindings                                 materialized_types_{};
            std::unordered_map<std::uint32_t, gir::ConstExprId> materialized_values_{};
            std::unordered_set<std::uint32_t>                   retained_generics_{};
            std::string                                         materialized_cpp_name_{};
            std::string                                         materialized_identity_{};
            gir::CallableId                                     materialized_callable_{};
        };

        std::string_view Emitter::local_identity(std::string_view identity) noexcept {
            const std::size_t separator = identity.find_last_of('.');
            return separator == std::string_view::npos ? identity : identity.substr(separator + 1);
        }

        std::string_view Emitter::callable_name(gir::CallableId decl) {
            const gir::Callable &item = callable(decl);
            if (item.visibility == gir::CallableVisibility::Implementation && !item.operator_identity.empty()) {
                return local_identity(item.operator_identity);
            }
            return local_identity(item.identity);
        }

        std::string Emitter::callable_cpp_name(gir::CallableId decl) {
            if (materialized_callable_ == decl && !materialized_cpp_name_.empty()) { return materialized_cpp_name_; }
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

        std::string Emitter::materialization_cpp_name(const gir::Materialization &item, std::size_t index) {
            const gir::CallableId saved = materialized_callable_;
            materialized_callable_      = {};
            std::string result          = callable_cpp_name(item.implementation);
            materialized_callable_      = saved;
            for (const gir::Substitution &substitution : item.substitutions) {
                result += "__";
                if (substitution.retained) {
                    result += "any_" + cpp_name(planned_binding(substitution.parameter, item.range).name);
                } else if (substitution.type.valid()) {
                    const gir::Type &type = graph_type(substitution.type, item.range);
                    if (type.kind == hir::TypeKind::Scalar) {
                        result += hir::scalar_type_name(type.scalar);
                    } else if (!type.nominal_identity.empty()) {
                        result += cpp_name(local_identity(type.nominal_identity));
                    } else {
                        result += "type";
                    }
                } else if (substitution.constant) {
                    std::visit(
                        [&](const auto &value) {
                            using T = std::decay_t<decltype(value)>;
                            if constexpr (std::is_same_v<T, std::int64_t>) {
                                const std::string spelling = std::to_string(value);
                                result += value < 0 ? "neg_" + spelling.substr(1) : spelling;
                            } else if constexpr (std::is_same_v<T, bool>) {
                                result += value ? "true" : "false";
                            } else {
                                result += "value";
                            }
                        },
                        *substitution.constant);
                } else {
                    result += "value";
                }
            }
            result += "__m" + std::to_string(index);
            return result;
        }

        void Emitter::begin_materialization(const gir::Materialization &item, std::size_t index) {
            if (!item.implementation.valid() || item.implementation.value >= graph_.callables.size()) {
                backend(item.range, "hgraph IR materialization names an invalid implementation");
            }
            const gir::Callable &implementation = callable(item.implementation, item.range);
            if (implementation.visibility != gir::CallableVisibility::Implementation || implementation.generics.empty()) {
                backend(item.range, "hgraph IR materialization target is not a generic operator implementation");
            }
            materialized_types_.clear();
            materialized_values_.clear();
            retained_generics_.clear();
            for (const gir::Substitution &substitution : item.substitutions) {
                if (!substitution.parameter.valid()) {
                    backend(item.range, "hgraph IR materialization has an invalid generic binding");
                }
                if (substitution.retained) {
                    if (substitution.type.valid() || substitution.value.valid() || substitution.constant) {
                        backend(item.range, "hgraph IR retained generic also has a concrete binding");
                    }
                    retained_generics_.insert(substitution.parameter.value);
                } else if (substitution.type.valid()) {
                    materialized_types_.emplace(substitution.parameter.value, planned_type(substitution.type, item.range));
                } else if (substitution.value.valid()) {
                    materialized_values_.emplace(substitution.parameter.value, substitution.value);
                } else {
                    backend(item.range, "hgraph IR materialization leaves a generic argument unresolved");
                }
            }
            if (materialized_types_.size() + materialized_values_.size() + retained_generics_.size() !=
                implementation.generics.size()) {
                backend(item.range, "hgraph IR materialization does not classify every implementation generic");
            }
            materialized_cpp_name_ = materialization_cpp_name(item, index);
            if (item.identity.empty()) { backend(item.range, "hgraph IR materialization has no candidate identity"); }
            materialized_identity_ = item.identity;
            materialized_callable_ = item.implementation;
        }

        void Emitter::end_materialization() {
            materialized_types_.clear();
            materialized_values_.clear();
            retained_generics_.clear();
            materialized_cpp_name_.clear();
            materialized_identity_.clear();
            materialized_callable_ = {};
        }

        std::string_view Emitter::active_callable_identity(const gir::Callable &item) const noexcept {
            return materialized_identity_.empty() ? std::string_view{item.identity} : std::string_view{materialized_identity_};
        }

        const gir::Binding &Emitter::planned_binding(gir::BindingId id, SourceRange fallback) {
            if (!id.valid() || id.value >= graph_.bindings.size()) {
                backend(fallback, "hgraph IR contains an invalid body binding ID");
            }
            return graph_.bindings[id.value];
        }

        const gir::Callable &Emitter::callable(gir::CallableId id, SourceRange fallback) {
            if (!id.valid() || id.value >= graph_.callables.size()) {
                backend(fallback, "hgraph IR contains an invalid callable ID");
            }
            return graph_.callables[id.value];
        }

        const gir::NativeFunction &Emitter::native_function(gir::NativeFunctionId id, SourceRange fallback) {
            if (!id.valid() || id.value >= graph_.native_functions.size()) {
                backend(fallback, "hgraph IR contains an invalid native function ID");
            }
            return graph_.native_functions[id.value];
        }

        const gir::OperatorContract &Emitter::operator_decl(gir::OperatorId id, SourceRange fallback) {
            if (!id.valid() || id.value >= graph_.operators.size()) {
                backend(fallback, "hgraph IR contains an invalid operator ID");
            }
            return graph_.operators[id.value];
        }

        const gir::StructContract &Emitter::struct_contract(gir::StructId id, SourceRange fallback) {
            if (!id.valid() || id.value >= graph_.structures.size()) {
                backend(fallback, "hgraph IR contains an invalid struct ID");
            }
            return graph_.structures[id.value];
        }

        void Emitter::bind_hgraph_declarations() {
            std::vector<bool> seen_structures(graph_.structures.size());
            std::vector<bool> seen_operators(graph_.operators.size());
            std::vector<bool> seen_callables(graph_.callables.size());
            std::vector<bool> seen_tests(graph_.tests.size());

            for (const gir::DeclarationRef &declaration : graph_.source_order) {
                std::visit(
                    [&](auto id) {
                        using T = decltype(id);
                        if constexpr (std::is_same_v<T, gir::StructId>) {
                            if (!id.valid() || id.value >= graph_.structures.size()) {
                                backend({}, "hgraph IR source order contains an invalid struct ID");
                            }
                            const gir::StructContract &item = struct_contract(id);
                            if (seen_structures[id.value]) {
                                backend(item.range, "hgraph IR source order contains a duplicate struct handle");
                            }
                            seen_structures[id.value] = true;
                            structure_declarations_.push_back(id);
                        } else if constexpr (std::is_same_v<T, gir::OperatorId>) {
                            if (!id.valid() || id.value >= graph_.operators.size()) {
                                backend({}, "hgraph IR source order contains an invalid operator ID");
                            }
                            const gir::OperatorContract &item = operator_decl(id);
                            if (item.imported) {
                                backend(item.range, "hgraph IR source order contains an imported operator handle");
                            }
                            if (seen_operators[id.value]) {
                                backend(item.range, "hgraph IR source order contains a duplicate operator handle");
                            }
                            seen_operators[id.value] = true;
                            operator_declarations_.push_back(id);
                        } else if constexpr (std::is_same_v<T, gir::CallableId>) {
                            if (!id.valid() || id.value >= graph_.callables.size()) {
                                backend({}, "hgraph IR source order contains an invalid callable ID");
                            }
                            const gir::Callable &item = callable(id);
                            if (seen_callables[id.value]) {
                                backend(item.range, "hgraph IR source order contains a duplicate callable handle");
                            }
                            seen_callables[id.value] = true;
                            callable_declarations_.push_back(id);
                        } else {
                            if (!id.valid() || id.value >= graph_.tests.size()) {
                                backend({}, "hgraph IR source order contains an invalid test ID");
                            }
                            const gir::TestPlan &item = graph_.tests[id.value];
                            if (seen_tests[id.value]) {
                                backend(item.range, "hgraph IR source order contains a duplicate test handle");
                            }
                            seen_tests[id.value] = true;
                        }
                    },
                    declaration);
            }

            const std::size_t local_operator_count =
                static_cast<std::size_t>(std::count_if(graph_.operators.begin(), graph_.operators.end(),
                                                       [](const gir::OperatorContract &item) { return !item.imported; }));
            if (structure_declarations_.size() != graph_.structures.size()) {
                backend({}, "hgraph IR source order omits a struct declaration");
            }
            if (operator_declarations_.size() != local_operator_count) {
                backend({}, "hgraph IR source order omits an operator declaration");
            }
            if (callable_declarations_.size() != graph_.callables.size()) {
                backend({}, "hgraph IR source order omits a callable declaration");
            }
            if (static_cast<std::size_t>(std::count(seen_tests.begin(), seen_tests.end(), true)) != graph_.tests.size()) {
                backend({}, "hgraph IR source order omits a test declaration");
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

        gir::ConstExprId Emitter::materialized_constant(gir::ConstExprId id, SourceRange fallback) {
            std::unordered_set<std::uint32_t> seen;
            while (true) {
                const gir::ConstExpr &expression = graph_constant(id, fallback);
                if (expression.kind != gir::ConstExprKind::Parameter || !expression.parameter_binding.valid()) { return id; }
                const auto found = materialized_values_.find(expression.parameter_binding.value);
                if (found == materialized_values_.end()) { return id; }
                if (!seen.insert(id.value).second) { backend(fallback, "cyclic materialized constant substitution"); }
                id = found->second;
            }
        }

        std::optional<std::int64_t> Emitter::planned_integer(gir::ConstExprId id, SourceRange fallback) {
            const gir::ConstExpr &expression = graph_constant(materialized_constant(id, fallback), fallback);
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
                        return make_const(item ? "true" : "false", scalar_type(hir::ScalarType::Bool), range);
                    } else if constexpr (std::is_same_v<T, std::int64_t>) {
                        return make_const(integer_literal(item), scalar_type(hir::ScalarType::I64), range, item);
                    } else if constexpr (std::is_same_v<T, double>) {
                        return make_const("hgraph::Float{" + float_literal(item) + "}", scalar_type(hir::ScalarType::F64), range,
                                          item);
                    } else if constexpr (std::is_same_v<T, std::string>) {
                        return make_const("hgraph::Str{" + quote(item) + "}", scalar_type(hir::ScalarType::Str), range);
                    } else if constexpr (std::is_same_v<T, syntax::TemporalValue>) {
                        if (std::optional<Value> value = temporal_constant(item, range)) { return std::move(*value); }
                        backend(range, std::string{gir::first_pass::unsupported_temporal_literal});
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
                        return fold_unary(expression.unary, planned_constant(expression.lhs, range), range);
                    }
                case gir::ConstExprKind::Binary:
                    {
                        return fold_binary(expression.binary, planned_constant(expression.lhs, range),
                                           planned_constant(expression.rhs, range), range);
                    }
                case gir::ConstExprKind::Parameter:
                    if (expression.parameter_binding.valid()) {
                        const auto found = materialized_values_.find(expression.parameter_binding.value);
                        if (found != materialized_values_.end()) { return planned_constant(found->second, range); }
                        if (retained_generics_.contains(expression.parameter_binding.value)) {
                            unsupported(range, "a retained generic used as a value; generic reification");
                        }
                    }
                    unsupported(range, "an unmaterialized generic parameter in a generated constant expression");
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
                            case ScalarType::Bool: return scalar_type(hir::ScalarType::Bool);
                            case ScalarType::I64: return scalar_type(hir::ScalarType::I64);
                            case ScalarType::F64: return scalar_type(hir::ScalarType::F64);
                            case ScalarType::Str: return scalar_type(hir::ScalarType::Str);
                            case ScalarType::Date: return scalar_type(hir::ScalarType::Date);
                            case ScalarType::Time: return scalar_type(hir::ScalarType::Time);
                            case ScalarType::DateTime: return scalar_type(hir::ScalarType::DateTime);
                            case ScalarType::Duration: return scalar_type(hir::ScalarType::Duration);
                            case ScalarType::CivilDateTime: return scalar_type(hir::ScalarType::CivilDateTime);
                            case ScalarType::ZonedDateTime: return scalar_type(hir::ScalarType::ZonedDateTime);
                            case ScalarType::ZonedTime: return scalar_type(hir::ScalarType::ZonedTime);
                            case ScalarType::TimeZone: return scalar_type(hir::ScalarType::TimeZone);
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
                            if (const auto found = materialized_types_.find(type.binding.value);
                                found != materialized_types_.end()) {
                                return found->second;
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
                        for (gir::TypeId child : type.children) { result.children.push_back(planned_type(child, range, bindings)); }
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
                            if (size) {
                                if (*size <= 0) { backend(range, "typed HIR admitted a non-positive list size"); }
                                result.size = std::to_string(*size);
                            } else {
                                const gir::ConstExpr &expression = graph_constant(type.size, range);
                                if (expression.kind != gir::ConstExprKind::Parameter || !expression.parameter_binding.valid() ||
                                    (materialized_callable_.valid() &&
                                     !retained_generics_.contains(expression.parameter_binding.value))) {
                                    unsupported(range, "a list size given by an unmaterialized const generic");
                                }
                                const gir::Binding &binding = planned_binding(expression.parameter_binding, range);
                                if (binding.kind != gir::BindingKind::ConstParameter) {
                                    backend(range, "a retained list size does not name a const generic");
                                }
                                result.size = "hgraph::SIZE<" + quote(binding.name) + ">";
                            }
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
                        const gir::ConstExprId maximum_id  = materialized_constant(type.size, range);
                        const gir::ConstExpr  &maximum     = graph_constant(maximum_id, range);
                        const auto             maximum_i64 = planned_integer(maximum_id, range);
                        if (maximum_i64) {
                            if (*maximum_i64 <= 0) { backend(range, "typed HIR admitted a non-positive rolling size"); }
                            result.size                               = std::to_string(*maximum_i64);
                            const std::optional<std::int64_t> minimum = planned_integer(type.min_size, range);
                            if (!minimum) { unsupported(range, "a rolling minimum given by a const generic"); }
                            if (*minimum <= 0 || *minimum > *maximum_i64) {
                                backend(range, "typed HIR admitted an invalid rolling minimum size");
                            }
                            result.min_size = std::to_string(*minimum);
                            return result;
                        }
                        if (maximum.kind != gir::ConstExprKind::Literal || !maximum.literal) {
                            // A symbolic size is a type relationship, not a C++
                            // non-type template parameter on the operator marker.
                            return result;
                        }
                        if (const auto *size = std::get_if<syntax::TemporalValue>(&*maximum.literal);
                            size != nullptr && size->kind == syntax::TemporalKind::Duration) {
                            const gir::ConstExpr &minimum = graph_constant(materialized_constant(type.min_size, range), range);
                            const auto           *minimum_value =
                                minimum.literal ? std::get_if<syntax::TemporalValue>(&*minimum.literal) : nullptr;
                            if (minimum.kind != gir::ConstExprKind::Literal) {
                                unsupported(range, "a rolling minimum given by a const generic");
                            }
                            if (minimum_value == nullptr || minimum_value->kind != syntax::TemporalKind::Duration) {
                                backend(range, "typed HIR admitted a rolling duration with a non-duration minimum");
                            }
                            if (size->micros <= 0 || minimum_value->micros < 0 || minimum_value->micros > size->micros) {
                                backend(range, "typed HIR admitted an invalid rolling duration");
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
                case TypeKind::Reference:
                    {
                        if (type.children.size() != 1U) { backend(range, "hgraph IR ref type requires one target type"); }
                        HType result;
                        result.kind = HType::Kind::Reference;
                        result.children.push_back(planned_type(type.children.front(), range, bindings));
                        return result;
                    }
                case TypeKind::Signal:
                    {
                        HType result;
                        result.kind = HType::Kind::Signal;
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
                        const hir::UnaryOp op      = expression.unary;
                        if (operand.is_const() || operand.is_runtime()) { return fold_unary(op, operand, range); }
                        if (!operand.is_port()) { backend(range, "this operand has no value"); }
                        return wire(op == hir::UnaryOp::Negate ? "hgraph::stdlib::neg_" : "hgraph::stdlib::not_", {operand.code},
                                    range);
                    }
                case gir::ConstExprKind::Binary:
                    {
                        const Value         lhs = planned_field_value(expression.lhs, range, bindings);
                        const Value         rhs = planned_field_value(expression.rhs, range, bindings);
                        const hir::BinaryOp op  = expression.binary;
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

        std::string Emitter::value_type(const HType &type, SourceRange range) {
            switch (type.kind) {
                case HType::Kind::Scalar:
                    switch (type.scalar) {
                        case hir::ScalarType::Bool: return "hgraph::Bool";
                        case hir::ScalarType::I64: return "hgraph::Int";
                        case hir::ScalarType::F64: return "hgraph::Float";
                        case hir::ScalarType::Str: return "hgraph::Str";
                        case hir::ScalarType::Date: return "hgraph::Date";
                        case hir::ScalarType::Time: return "hgraph::Time";
                        case hir::ScalarType::DateTime: return "hgraph::DateTime";
                        case hir::ScalarType::Duration: return "hgraph::TimeDelta";
                        case hir::ScalarType::CivilDateTime:
                        case hir::ScalarType::ZonedDateTime:
                        case hir::ScalarType::ZonedTime:
                        case hir::ScalarType::TimeZone: break;
                    }
                    backend(range, std::string{"'"} + std::string{hir::scalar_type_name(type.scalar)} +
                                       "' is not supported by the first pass (datetime and duration are)");
                case HType::Kind::Tuple:
                    {
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
                case HType::Kind::Reference: backend(range, "'ref' has no scalar value type");
                case HType::Kind::Signal: backend(range, "'signal' has no scalar value type");
                case HType::Kind::Generic: return type.cpp_type;
                case HType::Kind::Struct: return "typename " + type.cpp_type + "::value_type";
                case HType::Kind::Unknown: break;
            }
            backend(range, "this value has no C++ type");
        }

        std::string Emitter::schema(const HType &type, SourceRange range) {
            switch (type.kind) {
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
                case HType::Kind::Reference: return "hgraph::REF<" + schema(type.children[0], range) + ">";
                case HType::Kind::Signal: return "hgraph::SIGNAL";
                case HType::Kind::Generic: return "hgraph::TS<" + value_type(type, range) + ">";
                case HType::Kind::Unknown: break;
            }
            backend(range, "this value has no time-series schema");
        }

        // ------------------------------------------------------------ values

        Value make_const(std::string code, HType type, SourceRange range,
                         std::variant<std::monostate, std::int64_t, double> number) {
            Value value;
            value.kind   = Value::Kind::Const;
            value.code   = std::move(code);
            value.type   = std::move(type);
            value.range  = range;
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
                                      scalar_type(hir::ScalarType::Date), range);
                case syntax::TemporalKind::Time:
                    return make_const("hgraph::Time{" + micros + "}", scalar_type(hir::ScalarType::Time), range);
                case syntax::TemporalKind::DateTime:
                    return make_const("hgraph::DateTime{std::chrono::microseconds{" + micros + "}}",
                                      scalar_type(hir::ScalarType::DateTime), range);
                case syntax::TemporalKind::Duration:
                    return make_const("hgraph::TimeDelta{" + micros + "}", scalar_type(hir::ScalarType::Duration), range);
                case syntax::TemporalKind::CivilDateTime:
                case syntax::TemporalKind::ZonedDateTime:
                case syntax::TemporalKind::ZonedTime:
                case syntax::TemporalKind::TimeZone: return std::nullopt;
            }
            return std::nullopt;
        }

        std::optional<double> numeric_value(const Value &value) {
            if (const auto *integer = std::get_if<std::int64_t>(&value.number)) { return static_cast<double>(*integer); }
            if (const auto *floating = std::get_if<double>(&value.number)) { return *floating; }
            return std::nullopt;
        }

        std::optional<std::int64_t> integer_value(const Value &value) {
            if (const auto *integer = std::get_if<std::int64_t>(&value.number)) { return *integer; }
            return std::nullopt;
        }

        std::optional<std::int64_t> checked_add(std::int64_t lhs, std::int64_t rhs) {
            constexpr auto min = std::numeric_limits<std::int64_t>::min();
            constexpr auto max = std::numeric_limits<std::int64_t>::max();
            if ((rhs > 0 && lhs > max - rhs) || (rhs < 0 && lhs < min - rhs)) { return std::nullopt; }
            return lhs + rhs;
        }

        std::optional<std::int64_t> checked_sub(std::int64_t lhs, std::int64_t rhs) {
            constexpr auto min = std::numeric_limits<std::int64_t>::min();
            constexpr auto max = std::numeric_limits<std::int64_t>::max();
            if ((rhs > 0 && lhs < min + rhs) || (rhs < 0 && lhs > max + rhs)) { return std::nullopt; }
            return lhs - rhs;
        }

        std::optional<std::int64_t> checked_mul(std::int64_t lhs, std::int64_t rhs) {
            constexpr auto min = std::numeric_limits<std::int64_t>::min();
            constexpr auto max = std::numeric_limits<std::int64_t>::max();
            if (lhs == 0 || rhs == 0) { return 0; }
            if ((lhs == -1 && rhs == min) || (rhs == -1 && lhs == min)) { return std::nullopt; }
            if (lhs > 0) {
                if ((rhs > 0 && lhs > max / rhs) || (rhs < 0 && rhs < min / lhs)) { return std::nullopt; }
            } else if ((rhs > 0 && lhs < min / rhs) || (rhs < 0 && lhs < max / rhs)) {
                return std::nullopt;
            }
            return lhs * rhs;
        }

        std::variant<std::monostate, std::int64_t, double> folded_number(hir::BinaryOp op, const Value &lhs, const Value &rhs) {
            const auto left_int  = integer_value(lhs);
            const auto right_int = integer_value(rhs);
            if (left_int && right_int) {
                std::optional<std::int64_t> result;
                switch (op) {
                    case hir::BinaryOp::Add: result = checked_add(*left_int, *right_int); break;
                    case hir::BinaryOp::Sub: result = checked_sub(*left_int, *right_int); break;
                    case hir::BinaryOp::Mul: result = checked_mul(*left_int, *right_int); break;
                    case hir::BinaryOp::Rem:
                        if (*right_int != 0 && !(*left_int == std::numeric_limits<std::int64_t>::min() && *right_int == -1)) {
                            result = *left_int % *right_int;
                        }
                        break;
                    case hir::BinaryOp::Div:
                        if (*right_int != 0) { return static_cast<double>(*left_int) / static_cast<double>(*right_int); }
                        return {};
                    default: return {};
                }
                return result ? std::variant<std::monostate, std::int64_t, double>{*result}
                              : std::variant<std::monostate, std::int64_t, double>{};
            }
            const auto left  = numeric_value(lhs);
            const auto right = numeric_value(rhs);
            if (!left || !right) { return {}; }
            switch (op) {
                case hir::BinaryOp::Add: return *left + *right;
                case hir::BinaryOp::Sub: return *left - *right;
                case hir::BinaryOp::Mul: return *left * *right;
                case hir::BinaryOp::Div:
                    if (*right != 0.0) { return *left / *right; }
                    return {};
                default: return {};
            }
        }

        Value make_port(std::string code, HType type, SourceRange range) {
            Value value;
            value.kind  = Value::Kind::Port;
            value.code  = std::move(code);
            value.type  = std::move(type);
            value.range = range;
            return value;
        }

        Value make_runtime(std::string code, HType type, SourceRange range, std::string selector = {}) {
            Value value;
            value.kind     = Value::Kind::Runtime;
            value.code     = std::move(code);
            value.selector = std::move(selector);
            value.type     = std::move(type);
            value.range    = range;
            return value;
        }

        std::string Emitter::argument_code(const Value &value) {
            switch (value.kind) {
                case Value::Kind::Const:
                case Value::Kind::Port: return value.code;
                case Value::Kind::Runtime: backend(value.range, "an evaluation-time value cannot be passed while wiring");
                case Value::Kind::Iterator: backend(value.range, "a runtime iterator is only valid as the source of a 'for' loop");
                case Value::Kind::Function:
                case Value::Kind::NativeFunction:
                case Value::Kind::Struct:
                case Value::Kind::Operator:
                case Value::Kind::LocalOperator:
                case Value::Kind::Intrinsic:
                    backend(value.range, "passing a function to an operator is not supported by the first pass");
                case Value::Kind::Void: break;
            }
            backend(value.range, "hgraph IR value produces no wiring value");
        }

        std::string Emitter::as_runtime(const Value &value, const HType &target, SourceRange range, const std::string &what) {
            if (!value.is_const() && !value.is_runtime()) {
                fail(Category::Type, range, what + " needs an evaluation-time scalar value");
            }
            if (same_type(value.type, target)) { return value.code; }
            if (value.type.is(hir::ScalarType::I64) && target.is(hir::ScalarType::F64)) {
                return "static_cast<hgraph::Float>(" + value.code + ")";
            }
            fail(Category::Type, range, what + " expects " + value_type(target, range) + ", got " + value_type(value.type, range));
        }

        /// The value as a constant of `target` (a `const` parameter): the
        /// same conversions the direct backend's `convert` allows.
        std::string Emitter::as_const(const Value &value, const HType &target, SourceRange range, const std::string &what) {
            if (!value.is_const()) { fail(Category::Type, range, what + " is const; a constant is required"); }
            if (same_type(value.type, target)) { return value.code; }
            if (value.type.is(hir::ScalarType::I64) && target.is(hir::ScalarType::F64)) {
                return "static_cast<hgraph::Float>(" + value.code + ")";
            }
            fail(Category::Type, range, what + " expects " + value_type(target, range) + ", got " + value_type(value.type, range));
        }

        /// The value as a port of `temporal`: a constant is wired through
        /// `const` at that schema (exactly `wire_constant`); a port whose
        /// schema the registry decides is narrowed with `.as<>()`, which the
        /// wiring checks.
        std::string Emitter::as_port(const Value &value, const HType &temporal, SourceRange range) {
            const std::string s = schema(temporal, range);
            if (temporal.kind == HType::Kind::Atomic && !value.atomic_code.empty() && same_type(value.type, temporal.children[0])) {
                return value.atomic_code;
            }
            if (value.is_const()) {
                const HType inner     = temporal.kind == HType::Kind::Atomic ? temporal.children[0] : temporal;
                std::string converted = value.code;
                if (inner.kind == HType::Kind::Scalar && !same_type(value.type, inner)) {
                    converted = as_const(value, inner, range, "this value");
                }
                return "hgraph::wire<hgraph::stdlib::const_, " + s + ">(w, " + converted + ")";
            }
            if (!value.is_port()) { fail(Category::Type, range, "a time-series value is required"); }
            if (value.type.kind != HType::Kind::Unknown && same_type(value.type, temporal)) { return value.code; }
            return value.code + ".as<" + s + ">()";
        }

        Value Emitter::wire(std::string marker, const std::vector<std::string> &args, SourceRange range, const HType &result) {
            return make_port("hgraph::wire<" + marker + ">(w" + (args.empty() ? "" : ", " + join(args, ", ")) + ")", result, range);
        }

        // --------------------------------------------------------- constants

        Value Emitter::fold_unary(hir::UnaryOp op, const Value &operand, SourceRange range) {
            const bool runtime = operand.is_runtime();
            const auto result  = [&](Value value) {
                if (runtime) {
                    value.kind   = Value::Kind::Runtime;
                    value.number = {};
                }
                return value;
            };
            switch (op) {
                case hir::UnaryOp::Negate:
                    if (operand.type.numeric() || operand.type.is(hir::ScalarType::Duration)) {
                        std::variant<std::monostate, std::int64_t, double> number;
                        if (const auto integer = integer_value(operand);
                            integer && *integer != std::numeric_limits<std::int64_t>::min()) {
                            number = -*integer;
                        } else if (const auto *floating = std::get_if<double>(&operand.number)) {
                            number = -*floating;
                        }
                        return result(make_const("(-" + operand.code + ")", operand.type, range, std::move(number)));
                    }
                    fail(Category::Type, range, "unary '-' needs a number, got " + value_type(operand.type, range));
                case hir::UnaryOp::Not:
                    if (operand.type.is(hir::ScalarType::Bool)) {
                        return result(make_const("(!" + operand.code + ")", operand.type, range));
                    }
                    fail(Category::Type, range, "'!' needs a bool, got " + value_type(operand.type, range));
            }
            backend(range, "unsupported unary operator");
        }

        Value Emitter::fold_binary(hir::BinaryOp op, const Value &lhs, const Value &rhs, SourceRange range) {
            using hir::BinaryOp;
            using hir::ScalarType;
            const bool numeric    = lhs.type.numeric() && rhs.type.numeric();
            const bool ints       = lhs.type.is(ScalarType::I64) && rhs.type.is(ScalarType::I64);
            const bool runtime    = lhs.is_runtime() || rhs.is_runtime();
            const auto type_error = [&]() -> Value {
                fail(Category::Type, range,
                     std::string{"'"} + std::string{hir::binary_op_spelling(op)} + "' is not defined for " +
                         value_type(lhs.type, range) + " and " + value_type(rhs.type, range));
            };
            const auto binary = [&](std::string_view spelling, HType type) {
                Value value =
                    make_const("(" + lhs.code + " " + std::string{spelling} + " " + rhs.code + ")", std::move(type), range,
                               runtime ? std::variant<std::monostate, std::int64_t, double>{} : folded_number(op, lhs, rhs));
                if (runtime) { value.kind = Value::Kind::Runtime; }
                return value;
            };
            const HType float_t = scalar_type(ScalarType::F64);
            const HType bool_t  = scalar_type(ScalarType::Bool);
            switch (op) {
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
                    if (lhs.type.is(ScalarType::DateTime) && rhs.type.is(ScalarType::DateTime)) {
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
                    if (numeric) {
                        if (const auto divisor = numeric_value(rhs); divisor && *divisor == 0.0) {
                            fail(Category::Type, range, "division by zero");
                        }
                        Value value = make_const(
                            "(static_cast<hgraph::Float>(" + lhs.code + ") / static_cast<hgraph::Float>(" + rhs.code + "))",
                            float_t, range,
                            runtime ? std::variant<std::monostate, std::int64_t, double>{} : folded_number(op, lhs, rhs));
                        if (runtime) { value.kind = Value::Kind::Runtime; }
                        return value;
                    }
                    return type_error();
                case BinaryOp::Rem:
                    if (ints) {
                        if (const auto divisor = integer_value(rhs); divisor && *divisor == 0) {
                            fail(Category::Type, range, "division by zero");
                        }
                        return binary("%", lhs.type);
                    }
                    return type_error();
                case BinaryOp::Equal:
                case BinaryOp::NotEqual:
                    if (numeric || same_type(lhs.type, rhs.type)) { return binary(op == BinaryOp::Equal ? "==" : "!=", bool_t); }
                    return type_error();
                case BinaryOp::Less:
                case BinaryOp::LessEqual:
                case BinaryOp::Greater:
                case BinaryOp::GreaterEqual:
                    if (numeric || same_type(lhs.type, rhs.type)) {
                        const std::string_view spelling = op == BinaryOp::Less        ? "<"
                                                          : op == BinaryOp::LessEqual ? "<="
                                                          : op == BinaryOp::Greater   ? ">"
                                                                                      : ">=";
                        return binary(spelling, bool_t);
                    }
                    return type_error();
                case BinaryOp::And:
                case BinaryOp::Or:
                    if (lhs.type.is(ScalarType::Bool) && rhs.type.is(ScalarType::Bool)) {
                        return binary(op == BinaryOp::And ? "&&" : "||", bool_t);
                    }
                    return type_error();
            }
            backend(range, "unsupported binary operator");
        }

        Value Emitter::wire_binary(hir::BinaryOp op, const Value &lhs, const Value &rhs, SourceRange range) {
            using hir::BinaryOp;
            const char *name = nullptr;
            switch (op) {
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
                case hir::BinaryOp::Equal:
                case hir::BinaryOp::NotEqual:
                case hir::BinaryOp::Less:
                case hir::BinaryOp::LessEqual:
                case hir::BinaryOp::Greater:
                case hir::BinaryOp::GreaterEqual:
                case hir::BinaryOp::And:
                case hir::BinaryOp::Or: result = scalar_type(hir::ScalarType::Bool); break;
                case hir::BinaryOp::Add:
                case hir::BinaryOp::Sub:
                case hir::BinaryOp::Mul:
                case hir::BinaryOp::Div:
                case hir::BinaryOp::Rem:
                    if (lhs.type.numeric() && rhs.type.numeric()) {
                        result = scalar_type(lhs.type.is(hir::ScalarType::F64) || rhs.type.is(hir::ScalarType::F64)
                                                 ? hir::ScalarType::F64
                                                 : hir::ScalarType::I64);
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
                        const auto found = frame.planned_bindings.find(reference.binding.value);
                        if (found == frame.planned_bindings.end()) {
                            if (binding.kind == gir::BindingKind::ConstParameter) {
                                if (const auto materialized = materialized_values_.find(reference.binding.value);
                                    materialized != materialized_values_.end()) {
                                    return planned_constant(materialized->second, range);
                                }
                                if (retained_generics_.contains(reference.binding.value)) {
                                    unsupported(range, "a retained generic used as a value; generic reification");
                                }
                            }
                            backend(range, "'" + binding.name + "' is not bound in this function");
                        }
                        Value result = found->second;
                        result.range = range;
                        return result;
                    }
                case gir::ReferenceKind::Callable:
                    {
                        (void)callable(reference.callable, range);
                        Value result;
                        result.kind     = Value::Kind::Function;
                        result.callable = reference.callable;
                        result.range    = range;
                        return result;
                    }
                case gir::ReferenceKind::NativeFunction:
                    {
                        (void)native_function(reference.native_function, range);
                        Value result;
                        result.kind            = Value::Kind::NativeFunction;
                        result.native_function = reference.native_function;
                        result.range           = range;
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

        void Emitter::emit_planned_conditional_branch(std::string_view name, const gir::ConditionalBranchPlan &branch,
                                                      const gir::ConditionalPlan                       &plan,
                                                      const std::vector<std::pair<std::string, HType>> &parameters, Frame &outer,
                                                      const std::vector<gir::ConditionalResultSlot> &results,
                                                      std::string_view result_schema, SourceRange range) {
            if (current_body_ == nullptr) { backend(range, "a generated conditional branch has no enclosing function body"); }

            const auto saved_counts = local_counts_;
            const auto saved_names  = local_names_;
            local_counts_.clear();
            local_names_.clear();
            local_names_.insert("w");

            Frame nested;
            nested.fn = outer.fn;
            std::vector<std::string> signature{"[[maybe_unused]] hgraph::Wiring &w"};
            for (std::size_t index = 0; index < plan.captures.size(); ++index) {
                const gir::ConditionalCapture &capture = plan.captures[index];
                const gir::Binding            &binding = planned_binding(capture.binding, range);
                const auto &[cpp, declared_type]       = parameters[index];
                HType type                             = declared_type;
                if (gir::temporal_branch_forwards(plan, branch, capture.binding)) { type = reference_type(std::move(type)); }
                local_names_.insert(cpp);
                signature.push_back("[[maybe_unused]] hgraph::Port<" + schema(type, binding.range) + "> " + cpp);
                if (!nested.planned_bindings.emplace(capture.binding.value, make_port(cpp, type, binding.range)).second) {
                    backend(binding.range, "a generated conditional branch repeats a capture binding");
                }
            }

            const SourceRange branch_range = branch.block.valid() ? planned_block(branch.block, range).range : range;
            current_body_->line("// " + where(branch_range));
            current_body_->open("struct " + std::string{name});
            const bool        has_output      = !results.empty();
            const std::string result_spelling = has_output ? "hgraph::Port<" + std::string{result_schema} + ">" : "void";
            current_body_->line("static " + result_spelling + " compose(" + join(signature, ", ") + ")");
            current_body_->open("");

            // An assignment to a binding declared outside the source branch
            // becomes local to this child composition. Terminal conditional
            // plans have no escaping result slots, but their continuations
            // still need these definite-assignment locals.
            for (gir::BindingId binding_id : branch.assigned_outer) {
                if (nested.planned_bindings.contains(binding_id.value)) { continue; }
                const gir::Binding &binding = planned_binding(binding_id, branch_range);
                const HType         type    = planned_type(binding.type, binding.range);
                const std::string   base    = cpp_name(binding.name);
                std::string         local   = base;
                int                &suffix  = local_counts_[base];
                while (local_names_.contains(local)) { local = base + "_" + std::to_string(++suffix); }
                local_names_.insert(local);
                current_body_->line("hgraph::Port<" + schema(type, binding.range) + "> " + local + ";");
                nested.planned_bindings.emplace(binding_id.value, make_port(local, type, binding.range));
            }

            if (plan.returns_from_callable) {
                gir::ConditionalContinuationSegment body;
                if (branch.block.valid()) {
                    const gir::Block &block = planned_block(branch.block, range);
                    body.statements         = block.statements;
                    body.tail               = block.tail;
                }
                emit_planned_callable_path(body, branch.continuation, nested, *current_body_, range);
                current_body_->close();
                current_body_->close(";");
                local_counts_ = saved_counts;
                local_names_  = saved_names;
                return;
            }

            if (!has_output) {
                if (branch.block.valid()) { emit_planned_block(branch.block, nested, *current_body_, false, range); }
                if (branch.continuation) {
                    for (const gir::ConditionalContinuationSegment &segment : branch.continuation->segments) {
                        for (gir::StatementId statement : segment.statements) {
                            emit_planned_statement(statement, nested, *current_body_, range);
                        }
                        if (segment.tail.valid()) {
                            const Value value = eval_planned_expr(segment.tail, nested);
                            current_body_->line(value.kind == Value::Kind::Void ? value.code + ";" : "(void)" + value.code + ";");
                        }
                    }
                }
                current_body_->close();
                current_body_->close(";");
                local_counts_ = saved_counts;
                local_names_  = saved_names;
                return;
            }

            const gir::Block *body       = branch.block.valid() ? &planned_block(branch.block, range) : nullptr;
            const SourceRange body_range = body != nullptr ? body->range : range;
            for (const gir::ConditionalResultSlot &slot : results) {
                if (slot.source != gir::ConditionalResultSource::Binding || nested.planned_bindings.contains(slot.binding.value)) {
                    continue;
                }
                const gir::Binding &binding = planned_binding(slot.binding, body_range);
                const HType         type    = planned_type(slot.type, binding.range);
                const std::string   base    = cpp_name(binding.name);
                std::string         local   = base;
                int                &suffix  = local_counts_[base];
                while (local_names_.contains(local)) { local = base + "_" + std::to_string(++suffix); }
                local_names_.insert(local);
                current_body_->line("hgraph::Port<" + schema(type, binding.range) + "> " + local + ";");
                nested.planned_bindings.emplace(slot.binding.value, make_port(local, type, binding.range));
            }
            if (body != nullptr) {
                for (gir::StatementId statement : body->statements) {
                    emit_planned_statement(statement, nested, *current_body_, body_range);
                }
            }
            const auto           expression_slot = std::ranges::find_if(results, [](const gir::ConditionalResultSlot &slot) {
                return slot.source == gir::ConditionalResultSource::Expression;
            });
            std::optional<Value> expression_value;
            if (expression_slot != results.end()) {
                if (body == nullptr) {
                    const HType type = planned_type(expression_slot->type, body_range);
                    expression_value =
                        make_port("hgraph::wire<hgraph::stdlib::nothing, " + schema(type, body_range) + ">(w)", type, body_range);
                } else if (!body->tail.valid()) {
                    backend(body_range, "a value-producing time-series 'if' branch must end with a value");
                } else {
                    expression_value = eval_planned_expr(body->tail, nested);
                }
            } else if (body != nullptr && body->tail.valid()) {
                const Value tail = eval_planned_expr(body->tail, nested);
                current_body_->line(tail.kind == Value::Kind::Void ? tail.code + ";" : "(void)" + tail.code + ";");
            }

            std::vector<std::string> outputs;
            outputs.reserve(results.size());
            for (const gir::ConditionalResultSlot &slot : results) {
                const HType type = planned_type(slot.type, body_range);
                if (slot.source == gir::ConditionalResultSource::Expression) {
                    const SourceRange output_range = body != nullptr ? planned_value(body->tail, body_range).range : body_range;
                    outputs.push_back(as_port(*expression_value, type, output_range));
                    continue;
                }
                const auto found = nested.planned_bindings.find(slot.binding.value);
                if (found == nested.planned_bindings.end()) {
                    backend(body_range, "hgraph IR conditional branch did not assign escaping result '" + slot.field_name + "'");
                }
                outputs.push_back(as_port(found->second, type, body_range));
            }
            if (outputs.size() == 1U) {
                current_body_->line("return " + outputs.front() + ";");
            } else {
                current_body_->line("return hgraph::stdlib::to_tsb<" + std::string{result_schema} + ">(w, " + join(outputs, ", ") +
                                    ");");
            }
            current_body_->close();
            current_body_->close(";");

            local_counts_ = saved_counts;
            local_names_  = saved_names;
        }

        void Emitter::emit_planned_callable_path(const gir::ConditionalContinuationSegment      &segment,
                                                 std::optional<gir::ConditionalContinuationPlan> following, Frame &frame,
                                                 Writer &out, SourceRange fallback) {
            const auto continuation = [&](std::size_t first_statement, gir::ValueId conditional) {
                gir::ConditionalContinuationPlan result =
                    following.value_or(gir::ConditionalContinuationPlan{.result = callable(frame.fn).result});
                return gir::prepend_temporal_continuation(segment, first_statement, std::move(result), conditional);
            };

            for (std::size_t index = 0; index < segment.statements.size(); ++index) {
                const gir::StatementId statement_id = segment.statements[index];
                const gir::Statement  &statement    = planned_statement(statement_id, fallback);
                if (const auto *evaluate = std::get_if<gir::Evaluate>(&statement.node)) {
                    const gir::Value &expression = planned_value(evaluate->value, statement.range);
                    if (const auto *branch = std::get_if<gir::Conditional>(&expression.node);
                        branch != nullptr && expression.phase == hir::Phase::Wiring) {
                        bool        terminal = false;
                        const Value selected = lower_planned_conditional(evaluate->value, *branch, expression.range, frame, false,
                                                                         continuation(index + 1U, evaluate->value), &terminal);
                        if (terminal) {
                            emit_return(selected, frame, out, expression.range);
                            return;
                        }
                        out.line(selected.code + ";");
                        continue;
                    }
                }

                emit_planned_statement(statement_id, frame, out, fallback);
                if (std::holds_alternative<gir::Return>(statement.node)) { return; }
                if (const auto *evaluate = std::get_if<gir::Evaluate>(&statement.node);
                    evaluate != nullptr && planned_expression_terminates(evaluate->value, statement.range)) {
                    return;
                }
            }

            Value result;
            if (segment.tail.valid()) {
                const gir::Value &tail = planned_value(segment.tail, fallback);
                if (const auto *branch = std::get_if<gir::Conditional>(&tail.node);
                    branch != nullptr && tail.phase == hir::Phase::Wiring) {
                    bool terminal = false;
                    result        = lower_planned_conditional(segment.tail, *branch, tail.range, frame,
                                                              !following || following->segments.empty(),
                                                              continuation(segment.statements.size(), segment.tail), &terminal);
                    if (terminal) {
                        emit_return(result, frame, out, tail.range);
                        return;
                    }
                } else {
                    result = eval_planned_expr(segment.tail, frame);
                }
            }

            if (!following) { return; }
            if (following->segments.empty()) {
                emit_return(result, frame, out, segment.tail.valid() ? planned_value(segment.tail, fallback).range : fallback);
                return;
            }

            if (segment.tail.valid()) {
                out.line(result.kind == Value::Kind::Void ? result.code + ";" : "(void)" + result.code + ";");
            }
            gir::ConditionalContinuationPlan    remaining = std::move(*following);
            gir::ConditionalContinuationSegment next      = std::move(remaining.segments.front());
            remaining.segments.erase(remaining.segments.begin());
            emit_planned_callable_path(next, std::move(remaining), frame, out, fallback);
        }

        Value Emitter::lower_planned_conditional(gir::ValueId id, const gir::Conditional &, SourceRange range, Frame &frame,
                                                 bool result_used, std::optional<gir::ConditionalContinuationPlan> continuation,
                                                 bool *returns_from_callable) {
            const gir::ConditionalPlan plan    = gir::analyze_temporal_conditional(graph_, id, std::move(continuation));
            const auto                 results = gir::plan_temporal_conditional_results(graph_, plan, result_used);
            if (returns_from_callable != nullptr) { *returns_from_callable = plan.returns_from_callable; }
            // The plan carries every first-pass rule it breaks; the wording is
            // owned by the shared analysis (control_flow.h, PlanIssue).
            for (const gir::PlanIssue &issue : plan.issues) { backend(issue.range, issue.message); }

            const gir::ConditionalBranchPlan           otherwise = plan.when_false.value_or(gir::ConditionalBranchPlan{});
            std::vector<std::pair<std::string, HType>> parameters;
            std::vector<std::string>                   arguments;
            std::unordered_set<std::string>            parameter_names{"w"};
            std::unordered_map<std::string, int>       parameter_counts;
            parameters.reserve(plan.captures.size());
            arguments.reserve(plan.captures.size() + 2U);
            for (const gir::ConditionalCapture &capture : plan.captures) {
                const gir::Binding &binding = planned_binding(capture.binding, range);
                const auto          outer   = frame.planned_bindings.find(capture.binding.value);
                if (outer == frame.planned_bindings.end() || !outer->second.is_port()) {
                    backend(binding.range, "hgraph IR conditional capture is not bound to a time-series port");
                }
                const std::string base   = cpp_name(binding.name);
                std::string       name   = base;
                int              &suffix = parameter_counts[base];
                while (parameter_names.contains(name)) { name = base + "_" + std::to_string(++suffix); }
                parameter_names.insert(name);
                HType type = planned_type(capture.type, binding.range);
                parameters.emplace_back(std::move(name), type);
                const bool forwards = gir::temporal_branch_forwards(plan, plan.when_true, capture.binding) ||
                                      gir::temporal_branch_forwards(plan, otherwise, capture.binding);
                arguments.push_back(forwards ? as_port(outer->second, reference_type(std::move(type)), binding.range)
                                             : outer->second.code);
            }

            const Value condition = eval_planned_expr(plan.condition, frame);
            if (!condition.is_port()) {
                backend(planned_value(plan.condition, range).range, "a temporal conditional needs a port");
            }

            const std::size_t index       = ++conditional_index_;
            const std::string base        = "hgl_" + callable_cpp_name(frame.fn) + "_if_" + std::to_string(index);
            const std::string then_branch = base + "_then";
            const std::string else_branch = base + "_else";
            std::string       result_schema;
            if (results.size() == 1U) {
                result_schema = schema(planned_type(results.front().type, range), range);
            } else if (results.size() > 1U) {
                std::vector<std::string> fields;
                fields.reserve(results.size());
                for (const gir::ConditionalResultSlot &slot : results) {
                    fields.push_back("hgraph::Field<" + quote(slot.field_name) + ", " +
                                     schema(planned_type(slot.type, range), range) + ">");
                }
                result_schema = "hgraph::UnNamedTSB<" + join(fields, ", ") + ">";
            }
            emit_planned_conditional_branch(then_branch, plan.when_true, plan, parameters, frame, results, result_schema, range);
            emit_planned_conditional_branch(else_branch, otherwise, plan, parameters, frame, results, result_schema, range);

            std::vector<std::string> switch_arguments{
                condition.code, "hgraph::stdlib::switch_cases({{hgraph::Value{hgraph::Bool{true}}, hgraph::fn<" + then_branch +
                                    ">()}, {hgraph::Value{hgraph::Bool{false}}, hgraph::fn<" + else_branch + ">()}})"};
            switch_arguments.insert(switch_arguments.end(), arguments.begin(), arguments.end());
            if (results.empty()) {
                Value value;
                value.kind  = Value::Kind::Void;
                value.code  = "hgraph::wire<hgraph::stdlib::switch_sink_>(w, " + join(switch_arguments, ", ") + ")";
                value.range = range;
                return value;
            }
            if (results.size() == 1U) {
                const HType result = planned_type(results.front().type, range);
                Value       value  = wire("hgraph::stdlib::switch_", switch_arguments, range, result);
                value.code += ".as<" + result_schema + ">()";
                if (results.front().source == gir::ConditionalResultSource::Expression ||
                    results.front().source == gir::ConditionalResultSource::FunctionReturn) {
                    return value;
                }

                const gir::BindingId output_binding = results.front().binding;
                const auto           outer          = frame.planned_bindings.find(output_binding.value);
                if (outer == frame.planned_bindings.end() || !outer->second.is_port()) {
                    backend(planned_binding(output_binding, range).range,
                            "an escaping conditional result has no enclosing temporal binding");
                }
                const std::string outer_code                 = outer->second.code;
                Value             remapped                   = value;
                remapped.code                                = outer_code;
                frame.planned_bindings[output_binding.value] = std::move(remapped);

                Value assignment;
                assignment.kind  = Value::Kind::Void;
                assignment.code  = outer_code + " = " + value.code;
                assignment.range = range;
                return assignment;
            }

            const std::string selected_name = base + "_results";
            std::string       code = "[&]() { auto " + selected_name + " = hgraph::wire<hgraph::stdlib::switch_, " + result_schema +
                                     ">(w, " + join(switch_arguments, ", ") + "); ";
            const gir::ConditionalResultSlot *expression_result = nullptr;
            for (const gir::ConditionalResultSlot &slot : results) {
                if (slot.source == gir::ConditionalResultSource::Expression ||
                    slot.source == gir::ConditionalResultSource::FunctionReturn) {
                    expression_result = &slot;
                    continue;
                }
                const auto outer = frame.planned_bindings.find(slot.binding.value);
                if (outer == frame.planned_bindings.end() || !outer->second.is_port()) {
                    backend(planned_binding(slot.binding, range).range,
                            "an escaping conditional result has no enclosing temporal binding");
                }
                const HType       type                     = planned_type(slot.type, range);
                const std::string outer_code               = outer->second.code;
                frame.planned_bindings[slot.binding.value] = make_port(outer_code, type, range);
                code += outer_code + " = hgraph::wire<hgraph::stdlib::getattr_>(w, " + selected_name + ", hgraph::Str{" +
                        quote(slot.field_name) + "}).as<" + schema(type, range) + ">(); ";
            }
            if (expression_result != nullptr) {
                const HType type = planned_type(expression_result->type, range);
                code += "return hgraph::wire<hgraph::stdlib::getattr_>(w, " + selected_name + ", hgraph::Str{" +
                        quote(expression_result->field_name) + "}).as<" + schema(type, range) + ">(); }()";
                if (current_body_ == nullptr) {
                    backend(range, "a mixed temporal conditional has no enclosing generated function body");
                }
                std::string value_name = base + "_value";
                int        &suffix     = local_counts_[value_name];
                while (local_names_.contains(value_name)) { value_name = base + "_value_" + std::to_string(++suffix); }
                local_names_.insert(value_name);
                current_body_->line("auto " + value_name + " = " + code + ";");
                return make_port(std::move(value_name), type, range);
            }
            code += "}()";

            Value assignment;
            assignment.kind  = Value::Kind::Void;
            assignment.code  = std::move(code);
            assignment.range = range;
            return assignment;
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
                        if (operand.is_const() || operand.is_runtime()) { return fold_unary(node.op, operand, expression.range); }
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
                            return fold_binary(node.op, lhs, rhs, expression.range);
                        }
                        if (expression.operation.registry_name.empty()) { return wire_binary(node.op, lhs, rhs, expression.range); }
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
                        if (frame.runtime) {
                            if (!target.is_runtime() || target.selector.empty() || target.type.kind != HType::Kind::List ||
                                target.type.children.size() != 1U || target.type.children.front().kind != HType::Kind::Reference) {
                                backend(expression.range,
                                        "runtime indexing currently selects reference elements from a list input");
                            }
                            if ((!index.is_const() && !index.is_runtime()) || !index.type.is(hir::ScalarType::I64)) {
                                fail(Category::Type, index.range, "a runtime list index is an i64 value");
                            }
                            const std::string selector = target.selector + "[static_cast<std::size_t>(" + index.code + ")]";
                            return make_runtime(selector + ".value()", target.type.children.front(), expression.range, selector);
                        }
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
                        backend(expression.range, "hgraph IR lambda outside a planned map call");
                    } else if constexpr (std::is_same_v<T, gir::Conditional>) {
                        if (expression.phase == hir::Phase::Wiring) {
                            return lower_planned_conditional(id, node, expression.range, frame);
                        }
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
            const gir::Callable &target = callable(id, range);
            check_supported(id);
            const auto               bound = bind_planned_arguments(id, arguments, range);
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
            Value value = wire(callable_cpp_name(id), args, range, result);
            if (!has_planned_result(target.result, target.range)) { value.kind = Value::Kind::Void; }
            return value;
        }

        Value Emitter::call_planned_native(gir::NativeFunctionId id, const std::vector<gir::Argument> &arguments, SourceRange range,
                                           Frame &frame) {
            const gir::NativeFunction &target = native_function(id, range);
            if (!exact_cpp_symbol(target.cpp_symbol)) {
                backend(range, "native function '" + target.identity + "' has an invalid exact C++ symbol");
            }
            std::vector<std::optional<gir::ValueId>> bound(target.parameters.size());
            std::size_t                              next = 0U;
            for (const gir::Argument &argument : arguments) {
                if (argument.name.empty()) {
                    while (next < bound.size() && bound[next]) { ++next; }
                    if (next >= bound.size()) {
                        fail(Category::Type, argument.range,
                             "native function '" + target.identity + "' takes " + std::to_string(target.parameters.size()) +
                                 " arguments");
                    }
                    bound[next++] = argument.value;
                    continue;
                }
                const auto found = std::ranges::find(target.parameters, argument.name, &gir::NativeParameter::name);
                if (found == target.parameters.end()) {
                    fail(Category::Name, argument.range,
                         "native function '" + target.identity + "' has no parameter named '" + argument.name + "'");
                }
                const std::size_t index = static_cast<std::size_t>(found - target.parameters.begin());
                if (bound[index]) { fail(Category::Name, argument.range, "'" + argument.name + "' is given twice"); }
                bound[index] = argument.value;
            }

            std::vector<std::string> args;
            args.reserve(target.parameters.size());
            for (std::size_t index = 0; index < target.parameters.size(); ++index) {
                const gir::NativeParameter &parameter = target.parameters[index];
                if (!bound[index]) {
                    fail(Category::Type, range,
                         "native function '" + target.identity + "' needs an argument for '" + parameter.name + "'");
                }
                const Value argument = eval_planned_expr(*bound[index], frame);
                const HType expected = planned_type(parameter.type, range);
                if (!same_type(argument.type, expected)) {
                    fail(Category::Type, argument.range, "native parameter '" + parameter.name + "' requires an exact scalar type");
                }
                if (parameter.is_const || !frame.runtime) {
                    args.push_back(as_const(argument, expected, argument.range, "native parameter '" + parameter.name + "'"));
                } else {
                    if (!argument.is_const() && !argument.is_runtime()) {
                        fail(Category::Type, argument.range,
                             "native parameter '" + parameter.name + "' requires an evaluation-time scalar value");
                    }
                    args.push_back(argument.code);
                }
            }

            const std::string code = target.cpp_symbol + "(" + join(args, ", ") + ")";
            if (graph_type(target.result, range).kind == hir::TypeKind::Void) {
                Value result;
                result.kind  = Value::Kind::Void;
                result.code  = code;
                result.range = range;
                return result;
            }
            const HType result_type = planned_type(target.result, range);
            return frame.runtime ? make_runtime(code, result_type, range) : make_const(code, result_type, range);
        }

        Value Emitter::lower_planned_map_call(const Value &callee, const gir::Call &call, SourceRange range, Frame &frame) {
            gir::ValueId       lambda_id{};
            std::vector<Value> inputs;
            for (const gir::Argument &argument : call.arguments) {
                const gir::Value &expression = planned_value(argument.value, argument.range);
                if (std::holds_alternative<gir::Lambda>(expression.node)) {
                    if (lambda_id.valid()) { backend(expression.range, "hgraph IR map call has more than one anonymous function"); }
                    lambda_id = argument.value;
                } else {
                    inputs.push_back(eval_planned_expr(argument.value, frame));
                }
            }
            if (!lambda_id.valid()) { backend(range, "hgraph IR map call has no anonymous function"); }
            if (inputs.empty()) { backend(range, "hgraph IR map call has no mapped input"); }

            const gir::Value  &lambda_expression = planned_value(lambda_id, range);
            const gir::Lambda &anonymous         = std::get<gir::Lambda>(lambda_expression.node);
            if (anonymous.parameters.size() != inputs.size()) {
                backend(lambda_expression.range, "hgraph IR anonymous map parameter count differs from its inputs");
            }

            Frame lambda;
            lambda.fn = frame.fn;
            std::vector<std::string> parameters{"hgraph::Wiring &w"};
            for (std::size_t index = 0; index < inputs.size(); ++index) {
                const Value &input = inputs[index];
                if (!input.is_port() || input.type.kind != HType::Kind::Map) {
                    backend(input.range, "hgraph IR map call input is not a temporal map");
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
                backend(planned_value(anonymous.body, lambda_expression.range).range, "hgraph IR anonymous map result has no type");
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
                    if (delta) { backend(argument->range, "hgraph IR delta construction retained an optional-field clear"); }
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
                if ((!message.is_const() && !message.is_runtime()) || !message.type.is(hir::ScalarType::Str)) {
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
                                        scalar_type(hir::ScalarType::Bool), range);
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
                    return make_runtime(value.selector + ".last_modified_time()", scalar_type(hir::ScalarType::DateTime), range);
                }
                if (!value.is_port()) {
                    fail(Category::Type, call.arguments.front().range, "'" + name + "' takes a time-series argument");
                }
                return wire(name == "key_set" ? "hgraph::stdlib::keys_" : "hgraph::stdlib::last_modified_time", {value.code},
                            range);
            }
            if (name == "keys" || name == "values" || name == "items") {
                if (!frame.runtime) {
                    // The first-pass iterator rules are reported once by the
                    // shared traversal analysis; the iterator only has to exist.
                    if (call.arguments.size() != 1U) { backend(range, "hgraph IR graph iterator call has more than one argument"); }
                    const Value source = eval_planned_expr(call.arguments.front().value, frame);
                    if (!source.is_port() || (source.type.kind != HType::Kind::List && source.type.kind != HType::Kind::Map)) {
                        backend(range, "hgraph IR graph iterator schema does not match its collection type");
                    }

                    Value result;
                    result.kind  = Value::Kind::Iterator;
                    result.code  = source.code;
                    result.type  = source.type;
                    result.name  = name;
                    result.range = range;
                    if (source.type.kind == HType::Kind::Map) {
                        if (name == "items") { result.iterator_types.push_back(source.type.children[0]); }
                        result.iterator_types.push_back(source.type.children[1]);
                    } else {
                        if (name == "items") { result.iterator_types.push_back(scalar_type(hir::ScalarType::I64)); }
                        result.iterator_types.push_back(source.type.children.front());
                    }
                    return result;
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
                        result.iterator_types = {scalar_type(hir::ScalarType::I64), source.type.children[0]};
                    }
                } else {
                    backend(source.range, "this collection does not support '" + name + "'");
                }
                return result;
            }
            backend(range, "hgraph IR intrinsic without a composition lowering: " + name);
        }

        Value Emitter::eval_planned_call(const gir::Value &expression, const gir::Call &call, Frame &frame) {
            const Value callee = eval_planned_expr(call.callee, frame);
            if (frame.runtime && callee.kind != Value::Kind::Intrinsic && callee.kind != Value::Kind::Struct &&
                callee.kind != Value::Kind::NativeFunction) {
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
                        Value value = wire(marker, planned_arguments, expression.range);
                        if (expression.value_kind == hir::ValueKind::Void) { value.kind = Value::Kind::Void; }
                        return value;
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
                case Value::Kind::NativeFunction:
                    if (expression.operation.kind != gir::OperationKind::ExactFunction ||
                        !expression.operation.native_function.valid()) {
                        backend(expression.range, "hgraph IR exact native call has no resolved native function");
                    }
                    if (callee.native_function != expression.operation.native_function) {
                        backend(expression.range, "hgraph IR exact native call disagrees with its callee reference");
                    }
                    return call_planned_native(expression.operation.native_function, call.arguments, expression.range, frame);
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

        void Emitter::emit_return(const Value &value, Frame &frame, Writer &out, SourceRange range) {
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
                        const HType       declared = planned_type(node.type, statement.range);
                        const std::string base     = cpp_name(binding.name);
                        std::string       local    = base;
                        int              &suffix   = local_counts_[base];
                        while (local_names_.contains(local)) { local = base + "_" + std::to_string(++suffix); }
                        local_names_.insert(local);
                        if (!node.init.valid()) {
                            if (binding.kind != gir::BindingKind::LocalVar) {
                                backend(binding.range, "only a 'var' may omit its initializer");
                            }
                            out.line("hgraph::Port<" + schema(declared, binding.range) + "> " + local + ";");
                            Value value = make_port(local, declared, binding.range);
                            if (!frame.planned_bindings.emplace(node.binding.value, std::move(value)).second) {
                                backend(binding.range, "hgraph IR block repeats a local binding");
                            }
                            return;
                        }
                        Value value = eval_planned_expr(node.init, frame);
                        if (value.kind != Value::Kind::Const && value.kind != Value::Kind::Port) {
                            unsupported(statement.range, "binding a function or operator to a local");
                        }
                        if (value.is_const()) {
                            value.code = as_const(value, declared, value.range, "'" + binding.name + "'");
                        } else {
                            value.code = as_port(value, declared, value.range);
                        }
                        value.type = declared;

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
                            backend(place.range, "hgraph IR assignment place is not a binding");
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
                            const hir::BinaryOp op = node.op == gir::AssignOp::Add   ? hir::BinaryOp::Add
                                                     : node.op == gir::AssignOp::Sub ? hir::BinaryOp::Sub
                                                     : node.op == gir::AssignOp::Mul ? hir::BinaryOp::Mul
                                                                                     : hir::BinaryOp::Div;
                            value = current.is_const() && value.is_const() ? fold_binary(op, current, value, statement.range)
                                                                           : wire_binary(op, current, value, statement.range);
                        }
                        const bool promotes_constant_to_port = current.is_port() && value.is_const();
                        if (current.kind != value.kind && !promotes_constant_to_port) {
                            fail(Category::Type, statement.range, "assignment to '" + binding.name + "' changes its inferred type");
                        }
                        if (current.is_const()) {
                            value.code = as_const(value, current.type, value.range, "assignment to '" + binding.name + "'");
                            value.type = current.type;
                        } else if (current.is_port()) {
                            if (current.type.kind != HType::Kind::Unknown) {
                                value.code = as_port(value, current.type, value.range);
                            }
                            value.kind = Value::Kind::Port;
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
                            if (expression.phase != hir::Phase::Wiring) {
                                emit_planned_if(*branch, expression.range, frame, out);
                            } else {
                                const Value value = lower_planned_conditional(node.value, *branch, expression.range, frame, false);
                                out.line(value.code + ";");
                            }
                            return;
                        }
                        const Value value = eval_planned_expr(node.value, frame);
                        if (value.kind == Value::Kind::Void) {
                            out.line(value.code + ";");
                        } else {
                            out.line("(void)" + value.code + ";");
                        }
                    } else if constexpr (std::is_same_v<T, gir::Traversal>) {
                        emit_planned_traversal(node, statement.range, frame, out);
                    } else {
                        backend(statement.range, "hgraph IR runtime statement in a composition body");
                    }
                },
                statement.node);
        }

        void Emitter::emit_planned_traversal(const gir::Traversal &traversal, SourceRange range, Frame &frame, Writer &out) {
            const gir::TraversalPlan plan = gir::analyze_traversal(graph_, traversal, range);
            for (const gir::PlanIssue &issue : plan.issues) { backend(issue.range, issue.message); }

            const gir::Value &iterable_expression = planned_value(traversal.iterable, range);
            const Value       iterator            = eval_planned_expr(traversal.iterable, frame);
            if (!iterator.is_iterator()) {
                fail(Category::Type, iterable_expression.range,
                     "a graph 'for' loop needs values(...) or items(...) over a temporal map or list");
            }
            if (traversal.bindings.empty() || traversal.bindings.size() > 2U ||
                traversal.bindings.size() != iterator.iterator_types.size()) {
                backend(range, "hgraph IR traversal bindings do not match its iterator");
            }

            if (iterator.type.kind == HType::Kind::List && !iterator.type.size.empty()) {
                if (iterator.type.size.starts_with("hgraph::SIZE<")) {
                    unsupported(iterable_expression.range, "graph traversal over a list whose size remains a resolver generic");
                }
                const std::int64_t count = std::stoll(iterator.type.size);
                for (std::int64_t index = 0; index < count; ++index) {
                    Frame iteration = frame;
                    out.open("");

                    Value position =
                        make_const("hgraph::Int{" + std::to_string(index) + "}", scalar_type(hir::ScalarType::I64), range, index);
                    Value selected = make_port("hgraph::tsl_element(" + iterator.code + ", " + std::to_string(index) + ")",
                                               iterator.type.children.front(), range);

                    std::vector<Value> loop_values;
                    if (iterator.name == "items") { loop_values.push_back(std::move(position)); }
                    loop_values.push_back(std::move(selected));
                    for (std::size_t binding_index = 0; binding_index < traversal.bindings.size(); ++binding_index) {
                        const gir::Binding &binding = planned_binding(traversal.bindings[binding_index], range);
                        if (binding.kind != gir::BindingKind::LoopValue ||
                            !same_type(planned_type(binding.type, binding.range), loop_values[binding_index].type)) {
                            backend(binding.range, "hgraph IR fixed-list traversal has an invalid loop binding");
                        }
                        iteration.planned_bindings[traversal.bindings[binding_index].value] = loop_values[binding_index];
                    }
                    emit_planned_block(traversal.block, iteration, out, false, range);
                    out.close();
                }
                return;
            }

            const bool map          = iterator.type.kind == HType::Kind::Map;
            const bool dynamic_list = iterator.type.kind == HType::Kind::List && iterator.type.size.empty();
            if (!map && !dynamic_list) {
                backend(iterable_expression.range, "hgraph IR dynamic traversal schema is not a temporal map or list");
            }

            std::vector<std::pair<gir::ConditionalCapture, Value>> captures;
            captures.reserve(plan.captures.size());
            for (const gir::ConditionalCapture &capture : plan.captures) {
                const gir::Binding &binding = planned_binding(capture.binding, range);
                const auto          outer   = frame.planned_bindings.find(capture.binding.value);
                if (outer == frame.planned_bindings.end() || !outer->second.is_port()) {
                    backend(binding.range, "hgraph IR dynamic traversal capture is not bound to a time-series port");
                }
                captures.emplace_back(capture, outer->second);
            }

            const auto saved_counts = local_counts_;
            const auto saved_names  = local_names_;
            local_counts_.clear();
            local_names_.clear();
            local_names_.insert("w");

            std::unordered_map<std::string, int> parameter_counts;
            const auto                           unique_parameter = [&](std::string_view raw) {
                const std::string base = cpp_name(raw);
                std::string       name = base;
                int              &next = parameter_counts[base];
                while (local_names_.contains(name)) { name = base + "_" + std::to_string(++next); }
                local_names_.insert(name);
                return name;
            };

            Frame nested;
            nested.fn = frame.fn;
            std::vector<std::string> signature{"[[maybe_unused]] hgraph::Wiring &w"};
            for (std::size_t index = 0; index < traversal.bindings.size(); ++index) {
                const gir::Binding &binding = planned_binding(traversal.bindings[index], range);
                const HType         type    = planned_type(binding.type, binding.range);
                if (binding.kind != gir::BindingKind::LoopValue || !same_type(type, iterator.iterator_types[index])) {
                    backend(binding.range, "hgraph IR dynamic traversal has an invalid loop binding");
                }
                const std::string name = unique_parameter(binding.name);
                const std::string port =
                    iterator.name == "items" && index == 0U
                        ? "hgraph::NamedPort<" + quote(map ? "key" : "ndx") + ", " + schema(type, binding.range) + ">"
                        : "hgraph::Port<" + schema(type, binding.range) + ">";
                signature.push_back("[[maybe_unused]] " + port + " " + name);
                nested.planned_bindings.emplace(traversal.bindings[index].value, make_port(name, type, binding.range));
            }
            for (const auto &[capture, outer] : captures) {
                const gir::Binding &binding = planned_binding(capture.binding, range);
                const HType         type    = planned_type(capture.type, binding.range);
                const std::string   name    = unique_parameter(binding.name);
                signature.push_back("[[maybe_unused]] hgraph::Port<" + schema(type, binding.range) + "> " + name);
                nested.planned_bindings.emplace(capture.binding.value, make_port(name, type, binding.range));
            }

            const std::string helper = "hgl_" + callable_cpp_name(frame.fn) + "_for_" + std::to_string(++traversal_index_);
            out.line("// " + where(planned_block(traversal.block, range).range));
            out.open("struct " + helper);
            out.line("static void compose(" + join(signature, ", ") + ")");
            out.open("");
            emit_planned_block(traversal.block, nested, out, false, range);
            out.close();
            out.close(";");

            local_counts_ = saved_counts;
            local_names_  = saved_names;

            std::vector<std::string> arguments{"hgraph::fn<" + helper + ">()", iterator.code};
            for (const auto &[capture, outer] : captures) {
                static_cast<void>(capture);
                arguments.push_back("hgraph::stdlib::pass_through(" + outer.code + ")");
            }
            out.line("hgraph::wire<hgraph::stdlib::map_sink_>(w, " + join(arguments, ", ") + ");");
        }

        void Emitter::emit_planned_if(const gir::Conditional &branch, SourceRange range, Frame &frame, Writer &out) {
            const gir::Value &condition_expression = planned_value(branch.condition, range);
            const Value       condition            = eval_planned_expr(branch.condition, frame);
            // A temporal condition is planned through switch_ before this
            // scalar form is reached, so a port here is an IR inconsistency.
            if (condition.is_port()) { backend(condition_expression.range, "hgraph IR scalar 'if' has a time-series condition"); }
            if (!condition.is_const() || !condition.type.is(hir::ScalarType::Bool)) {
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
            for (std::size_t index = 0; index < block.statements.size(); ++index) {
                const gir::StatementId statement_id = block.statements[index];
                if (function_body) {
                    const gir::Statement &statement = planned_statement(statement_id, block.range);
                    if (const auto *evaluate = std::get_if<gir::Evaluate>(&statement.node)) {
                        const gir::Value &expression = planned_value(evaluate->value, statement.range);
                        if (const auto *branch = std::get_if<gir::Conditional>(&expression.node);
                            branch != nullptr && expression.phase == hir::Phase::Wiring) {
                            const gir::ConditionalContinuationPlan continuation =
                                gir::plan_temporal_continuation(graph_, id, index + 1U, callable(frame.fn).result, evaluate->value);
                            bool        terminal = false;
                            const Value value = lower_planned_conditional(evaluate->value, *branch, expression.range, frame, false,
                                                                          continuation, &terminal);
                            if (terminal) {
                                emit_return(value, frame, out, expression.range);
                                return;
                            }
                            out.line(value.code + ";");
                            continue;
                        }
                    }
                }
                emit_planned_statement(statement_id, frame, out, block.range);
            }
            if (block.tail.valid()) {
                const gir::Value &tail = planned_value(block.tail, block.range);
                if (function_body) {
                    Value value;
                    if (const auto *branch = std::get_if<gir::Conditional>(&tail.node);
                        branch != nullptr && tail.phase == hir::Phase::Wiring) {
                        gir::ConditionalContinuationPlan continuation = gir::plan_temporal_continuation(
                            graph_, id, block.statements.size(), callable(frame.fn).result, block.tail);
                        value = lower_planned_conditional(block.tail, *branch, tail.range, frame, true, std::move(continuation));
                    } else {
                        value = eval_planned_expr(block.tail, frame);
                    }
                    emit_return(value, frame, out, tail.range);
                } else if (const auto *branch = std::get_if<gir::Conditional>(&tail.node)) {
                    if (tail.phase != hir::Phase::Wiring) {
                        emit_planned_if(*branch, tail.range, frame, out);
                    } else {
                        const Value value = lower_planned_conditional(block.tail, *branch, tail.range, frame, false);
                        out.line(value.code + ";");
                    }
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
            if ((!condition.is_const() && !condition.is_runtime()) || !condition.type.is(hir::ScalarType::Bool)) {
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
                        const HType       declared = planned_type(node.type, statement.range);
                        const std::string base     = cpp_name(binding.name);
                        std::string       local    = base;
                        int              &suffix   = local_counts_[base];
                        while (local_names_.contains(local)) { local = base + "_" + std::to_string(++suffix); }
                        local_names_.insert(local);
                        if (!node.init.valid()) {
                            if (binding.kind != gir::BindingKind::LocalVar) {
                                backend(binding.range, "only a 'var' may omit its initializer");
                            }
                            if (declared.kind != HType::Kind::Scalar) {
                                backend(binding.range, "an uninitialized runtime local currently requires a scalar type");
                            }
                            out.line(value_type(declared, binding.range) + " " + local + ";");
                            Value value = make_runtime(local, declared, binding.range);
                            if (!frame.planned_bindings.emplace(node.binding.value, std::move(value)).second) {
                                backend(binding.range, "hgraph IR block repeats a local binding");
                            }
                            return;
                        }
                        Value value = eval_planned_expr(node.init, frame);
                        if (!value.is_const() && !value.is_runtime()) {
                            fail(Category::Type, statement.range, "a runtime local needs a scalar value");
                        }
                        value.code = as_runtime(value, declared, value.range, "'" + binding.name + "'");
                        value.type = declared;
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
                                        backend(place.range, "typed HIR admitted 'out' in a lifecycle block");
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
                            backend(place.range, "typed HIR admitted 'out' in a lifecycle block");
                        }
                        const auto current_it = frame.planned_bindings.find(reference->binding.value);
                        if (current_it == frame.planned_bindings.end()) {
                            backend(place.range, "'" + binding.name + "' is not bound in this function");
                        }
                        const Value current = current_it->second;
                        Value       value   = eval_planned_expr(node.value, frame);
                        if (node.op != gir::AssignOp::Assign) {
                            const hir::BinaryOp op = node.op == gir::AssignOp::Add   ? hir::BinaryOp::Add
                                                     : node.op == gir::AssignOp::Sub ? hir::BinaryOp::Sub
                                                     : node.op == gir::AssignOp::Mul ? hir::BinaryOp::Mul
                                                                                     : hir::BinaryOp::Div;
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
                            backend(statement.range, "typed HIR admitted 'return' in a lifecycle block");
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
                        if ((!condition.is_const() && !condition.is_runtime()) || !condition.type.is(hir::ScalarType::Bool)) {
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

                        const bool         map  = iterator.type.kind == HType::Kind::Map;
                        const bool         list = iterator.type.kind == HType::Kind::List;
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
                            if ((!condition.is_const() && !condition.is_runtime()) || !condition.type.is(hir::ScalarType::Bool)) {
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

        std::optional<std::size_t> Emitter::runtime_parameter(gir::ValueId id, gir::CallableId decl) {
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

        std::optional<std::size_t> Emitter::runtime_root_parameter(gir::ValueId id, gir::CallableId decl) {
            if (const std::optional<std::size_t> parameter = runtime_parameter(id, decl)) { return parameter; }
            const gir::Value &expression = planned_value(id, callable(decl).range);
            if (const auto *index = std::get_if<gir::Index>(&expression.node)) {
                return runtime_root_parameter(index->target, decl);
            }
            if (const auto *field = std::get_if<gir::Field>(&expression.node)) {
                return runtime_root_parameter(field->target, decl);
            }
            return std::nullopt;
        }

        std::optional<std::string> Emitter::runtime_scalar_key(gir::ValueId id, gir::CallableId decl) {
            const gir::Value &expression = planned_value(id, callable(decl).range);
            if (const auto *reference = std::get_if<gir::Reference>(&expression.node);
                reference != nullptr && reference->kind == gir::ReferenceKind::Binding) {
                return "binding:" + std::to_string(reference->binding.value);
            }
            if (const auto *literal = std::get_if<gir::Literal>(&expression.node)) {
                if (const auto *integer = std::get_if<std::int64_t>(&literal->value)) {
                    return "integer:" + std::to_string(*integer);
                }
            }
            return std::nullopt;
        }

        std::optional<std::int64_t> Emitter::runtime_integer_constant(gir::ValueId id, gir::CallableId decl) {
            const gir::Value &expression = planned_value(id, callable(decl).range);
            if (!expression.constant) { return std::nullopt; }
            return std::visit(
                [](const auto &value) -> std::optional<std::int64_t> {
                    using T = std::decay_t<decltype(value)>;
                    if constexpr (std::is_same_v<T, std::int64_t>) { return value; }
                    return std::nullopt;
                },
                *expression.constant);
        }

        std::optional<std::string> Emitter::runtime_selector_key(gir::ValueId id, gir::CallableId decl) {
            const gir::Value &expression = planned_value(id, callable(decl).range);
            if (const std::optional<std::size_t> parameter = runtime_parameter(id, decl)) {
                return "parameter:" + std::to_string(*parameter);
            }
            if (const auto *index = std::get_if<gir::Index>(&expression.node)) {
                const std::optional<std::string> target = runtime_selector_key(index->target, decl);
                if (!target) { return std::nullopt; }
                if (const std::optional<std::string> subscript = runtime_scalar_key(index->index, decl)) {
                    return *target + "[" + *subscript + "]";
                }
                return std::nullopt;
            }
            if (const auto *field = std::get_if<gir::Field>(&expression.node)) {
                if (const std::optional<std::string> target = runtime_selector_key(field->target, decl)) {
                    return *target + "." + field->name;
                }
            }
            return std::nullopt;
        }

        void Emitter::collect_runtime_activation(gir::ValueId id, gir::CallableId decl, RuntimeInfo &info) {
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
                                const std::optional<std::size_t> parameter = runtime_root_parameter(argument.value, decl);
                                if (!parameter) {
                                    backend(argument.range,
                                            "a generated runtime node requires 'modified' arguments to select a temporal input");
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

        void Emitter::check_runtime_expr(gir::ValueId id, gir::CallableId decl, const RuntimeValidSet &valid) {
            const gir::Value &expression = planned_value(id, callable(decl).range);
            std::visit(
                [&](const auto &node) {
                    using T = std::decay_t<decltype(node)>;
                    if constexpr (std::is_same_v<T, gir::Reference>) {
                        if (node.kind != gir::ReferenceKind::Binding) { return; }
                        const std::optional<std::size_t> parameter = runtime_parameter(id, decl);
                        const std::optional<std::string> key       = runtime_selector_key(id, decl);
                        if (parameter && (!key || !valid.contains(*key))) {
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
                            for (const gir::Argument &argument : node.arguments) {
                                check_runtime_selector(argument.value, decl, valid);
                            }
                            return;
                        }
                        check_runtime_expr(node.callee, decl, valid);
                        for (const gir::Argument &argument : node.arguments) { check_runtime_expr(argument.value, decl, valid); }
                    } else if constexpr (std::is_same_v<T, gir::Index>) {
                        check_runtime_selector(id, decl, valid);
                        const std::optional<std::string> key = runtime_selector_key(id, decl);
                        if (!key || !valid.contains(*key)) {
                            fail(Category::Type, expression.range,
                                 "selected temporal input may be invalid here; guard the read with valid(...)");
                        }
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

        void Emitter::check_runtime_selector(gir::ValueId id, gir::CallableId decl, const RuntimeValidSet &valid) {
            const gir::Value &expression = planned_value(id, callable(decl).range);
            if (const auto *index = std::get_if<gir::Index>(&expression.node)) {
                check_runtime_selector(index->target, decl, valid);
                check_runtime_expr(index->index, decl, valid);
                const HType target = planned_type(planned_value(index->target, expression.range).type, expression.range);
                if (target.kind != HType::Kind::List || target.size.empty()) {
                    backend(expression.range, "safe runtime indexing currently requires a fixed-size list input");
                }
                const std::int64_t size = std::stoll(target.size);
                if (const std::optional<std::int64_t> literal = runtime_integer_constant(index->index, decl)) {
                    if (*literal < 0 || *literal >= size) {
                        fail(Category::Type, planned_value(index->index, expression.range).range,
                             "a fixed-list index is outside its valid range");
                    }
                    return;
                }
                const std::optional<std::string> subscript = runtime_scalar_key(index->index, decl);
                if (!subscript || !valid.contains("nonnegative:" + *subscript) ||
                    !valid.contains("below:" + *subscript + ":" + target.size)) {
                    fail(Category::Type, planned_value(index->index, expression.range).range,
                         "a dynamic fixed-list index must be guarded by 'index >= 0 && index < size'");
                }
            } else if (const auto *field = std::get_if<gir::Field>(&expression.node)) {
                check_runtime_selector(field->target, decl, valid);
            }
        }

        Emitter::RuntimeValidSet Emitter::runtime_true_valid(gir::ValueId id, gir::CallableId decl, const RuntimeValidSet &valid) {
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
                        if (const std::optional<std::string> key = runtime_selector_key(argument.value, decl)) {
                            result.insert(*key);
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
                for (const std::string &selector : lhs) {
                    if (rhs.contains(selector)) { intersection.insert(selector); }
                }
                return intersection;
            }
            if (binary->op == ir::hir::BinaryOp::GreaterEqual) {
                const std::optional<std::string>  index = runtime_scalar_key(binary->lhs, decl);
                const std::optional<std::int64_t> bound = runtime_integer_constant(binary->rhs, decl);
                if (index && bound == 0) { result.insert("nonnegative:" + *index); }
            } else if (binary->op == ir::hir::BinaryOp::Less) {
                const std::optional<std::string>  index = runtime_scalar_key(binary->lhs, decl);
                const std::optional<std::int64_t> bound = runtime_integer_constant(binary->rhs, decl);
                if (index && bound && *bound > 0) { result.insert("below:" + *index + ":" + std::to_string(*bound)); }
            }
            return result;
        }

        void Emitter::check_runtime_stmt(gir::StatementId id, gir::CallableId decl, const RuntimeValidSet &valid, bool allow_when,
                                         SourceRange fallback) {
            const gir::Statement &statement = planned_statement(id, fallback);
            std::visit(
                [&](const auto &node) {
                    using T = std::decay_t<decltype(node)>;
                    if constexpr (std::is_same_v<T, gir::LocalBinding>) {
                        if (node.init.valid()) { check_runtime_expr(node.init, decl, valid); }
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

        void Emitter::check_runtime_block(gir::BlockId id, gir::CallableId decl, const RuntimeValidSet &valid, bool allow_when) {
            const gir::Block &block = planned_block(id, callable(decl).range);
            for (gir::StatementId statement : block.statements) {
                check_runtime_stmt(statement, decl, valid, allow_when, block.range);
            }
            if (block.tail.valid()) { check_runtime_expr(block.tail, decl, valid); }
        }

        RuntimeInfo Emitter::runtime_info(gir::CallableId decl) {
            const gir::Callable &planned = callable(decl);
            RuntimeInfo          info;

            if (planned.concise_body.valid() || !planned.block_body.valid()) {
                backend(planned.range, "a runtime function needs a block body");
            }
            if (has_planned_result(planned.result, planned.range)) {
                const HType result = planned_type(planned.result, planned.range);
                if (result.kind != HType::Kind::Scalar && result.kind != HType::Kind::Struct && result.kind != HType::Kind::Map &&
                    result.kind != HType::Kind::Reference) {
                    backend(graph_type(planned.result, planned.range).range,
                            "the runtime-node slice supports scalar, struct, map, and ref outputs");
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
                    type.kind != HType::Kind::List && type.kind != HType::Kind::Reference && type.kind != HType::Kind::Signal) {
                    backend(graph_type(parameter.type, planned.range).range,
                            "the runtime-node slice supports scalar, collection, ref, and signal parameters");
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
                                    // The checker admits only the approved names
                                    // and rejects the agreed-but-unimplemented ones.
                                    backend(binding.range,
                                            "hgraph IR inject binding '" + binding.name + "' has no generated selector");
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
                backend(planned_block(info.start_blocks[1], body.range).range, "typed HIR admitted a second 'start' block");
            }
            if (info.stop_blocks.size() > 1U) {
                backend(planned_block(info.stop_blocks[1], body.range).range, "typed HIR admitted a second 'stop' block");
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
            if (!info.has_when) {
                for (const std::size_t parameter : info.active_parameters) {
                    valid.insert("parameter:" + std::to_string(parameter));
                }
            }
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

        void Emitter::check_supported(gir::CallableId decl) {
            if (callable(decl).kind == gir::CallableKind::RuntimeNode) { static_cast<void>(runtime_info(decl)); }
        }

        std::string Emitter::runtime_signature(gir::CallableId decl, const RuntimeInfo &info, bool with_names, bool include_inputs,
                                               bool include_output) {
            const gir::Callable     &fn = callable(decl);
            std::vector<std::string> params;
            for (std::size_t i = 0; i < fn.parameters.size(); ++i) {
                const gir::Parameter &param  = fn.parameters[i];
                const HType           type   = planned_type(param.type, fn.range);
                const SourceRange     range  = graph_type(param.type, fn.range).range;
                const std::string     name   = with_names ? " " + cpp_name(param.name) : "";
                const std::string     unused = with_names ? "[[maybe_unused]] " : "";
                if (param.is_const) {
                    params.push_back(unused + "hgraph::Scalar<" + quote(param.name) + ", " + value_type(type, range) + ">" + name);
                    continue;
                }
                if (!include_inputs) { continue; }
                std::string selector = unused + "hgraph::In<" + quote(param.name) + ", " + schema(type, range);
                if (!info.active_parameters.contains(i)) { selector += ", hgraph::InputActivity::Passive"; }
                if (info.has_when) { selector += ", hgraph::InputValidity::Unchecked"; }
                selector += ">" + name;
                params.push_back(std::move(selector));
            }
            if (!info.states.empty()) {
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

        void Emitter::prepare_runtime_frame(gir::CallableId decl, const RuntimeInfo &info, Frame &frame, Writer &out,
                                            bool include_inputs, bool include_output) {
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

        void Emitter::emit_runtime_function(gir::CallableId decl, Writer &out) {
            const gir::Callable &planned = callable(decl);
            const RuntimeInfo    info    = runtime_info(decl);
            Frame                frame;
            frame.fn      = decl;
            frame.runtime = true;
            out.line("// " + where(planned.range));
            out.open("struct " + callable_cpp_name(decl));
            out.line("[[maybe_unused]] static constexpr auto name = " + quote(active_callable_identity(planned)) + ";");
            emit_defaults(planned, out);
            if (!info.states.empty()) {
                std::vector<std::string> fields;
                for (const RuntimeState &state : info.states) {
                    fields.push_back("hgraph::Field<" + quote(state.name) + ", " + schema(state.type, state.range) + ">");
                }
                out.line("using recordable_state = hgraph::TSB<" +
                         quote(std::string{active_callable_identity(planned)} + ".state") + ", " + join(fields, ", ") + ">;");
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

        std::string Emitter::signature(gir::CallableId decl, bool with_names) {
            const gir::Callable     &fn = callable(decl);
            std::vector<std::string> params{with_names ? "[[maybe_unused]] hgraph::Wiring &w" : "hgraph::Wiring &"};
            for (const gir::Parameter &param : fn.parameters) {
                const HType       type  = planned_type(param.type, fn.range);
                const SourceRange range = graph_type(param.type, fn.range).range;
                const std::string name  = with_names ? " " + cpp_name(param.name) : "";
                if (param.is_const) {
                    params.push_back("hgraph::Scalar<" + quote(param.name) + ", " + value_type(type, range) + ">" + name);
                } else {
                    params.push_back("hgraph::Port<" + schema(type, range) + ">" + name);
                }
            }
            return join(params, ", ");
        }

        std::string Emitter::result_type(gir::CallableId decl) {
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
                if (param.is_const) {
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
                value_fields.push_back("hgraph::Field<" + quote(field.name) + ", " + value_type(type, planned.range) + ">");
                temporal_fields.push_back("hgraph::Field<" + quote(field.name) + ", " + schema(type, planned.range) + ">");
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

        void Emitter::emit_function(gir::CallableId decl, Writer &out, Form form) {
            check_supported(decl);
            if (callable(decl).kind == gir::CallableKind::RuntimeNode) {
                if (form != Form::InlineStruct) {
                    backend(callable(decl).range, "a generated runtime node must be emitted as a complete static struct");
                }
                emit_runtime_function(decl, out);
                return;
            }
            const gir::Callable &planned = callable(decl);
            Frame                frame;
            frame.fn               = decl;
            const std::string name = callable_cpp_name(decl);

            out.line("// " + where(planned.range));
            if (form == Form::OutOfLine) {
                out.line(result_type(decl) + " " + name + "::compose(" + signature(decl, true) + ")");
            } else {
                out.open("struct " + name);
                out.line("[[maybe_unused]] static constexpr auto name = " + quote(active_callable_identity(planned)) + ";");
                // Defaults of const parameters travel with the graph so the
                // registry can apply them when the function is called by name.
                emit_defaults(callable(decl), out);
                out.line("static " + result_type(decl) + " compose(" + signature(decl, form != Form::Declaration) +
                         (form == Form::Declaration ? ");" : ")"));
                if (form == Form::Declaration) {
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
            current_body_ = &out;
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
            current_body_ = nullptr;
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

        void Emitter::collect_calls(gir::ValueId id, PlannedCalls &calls, SourceRange fallback) {
            const gir::Value &value = planned_value(id, fallback);
            if (!calls.values.insert(id.value).second) { return; }
            if (value.operation.kind == gir::OperationKind::ExactFunction) {
                if (value.operation.native_function.valid()) {
                    (void)native_function(value.operation.native_function, value.range);
                } else if (!value.operation.callable.valid() || value.operation.callable.value >= graph_.callables.size()) {
                    backend(value.range, "hgraph IR contains an invalid callable dependency ID");
                } else {
                    calls.calls.insert(value.operation.callable.value);
                }
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
                            if (node.init.valid()) { collect_calls(node.init, calls, statement.range); }
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
        std::vector<gir::CallableId> Emitter::ordered_internal_functions() {
            std::vector<gir::CallableId> internal;
            for (const gir::CallableId id : callable_declarations_) {
                if (callable(id).visibility == gir::CallableVisibility::Internal) { internal.push_back(id); }
            }
            std::map<std::uint32_t, std::set<std::uint32_t>> deps;
            for (const gir::CallableId id : internal) {
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
                std::set<std::uint32_t> filtered;
                for (const std::uint32_t call_id : calls.calls) {
                    const gir::CallableId call{call_id};
                    static_cast<void>(callable(call, fn.range));
                    if (std::find(internal.begin(), internal.end(), call) != internal.end()) { filtered.insert(call_id); }
                }
                deps[id.value] = std::move(filtered);
            }
            std::vector<gir::CallableId>         ordered;
            std::set<std::uint32_t>              done;
            std::set<std::uint32_t>              visiting;
            std::function<void(gir::CallableId)> visit = [&](gir::CallableId id) {
                if (done.contains(id.value)) { return; }
                if (visiting.contains(id.value)) {
                    backend(callable(id).range,
                            "'" + std::string{callable_name(id)} + "' is recursive; recursive functions are not supported");
                }
                visiting.insert(id.value);
                for (const std::uint32_t dep : deps[id.value]) { visit(gir::CallableId{dep}); }
                visiting.erase(id.value);
                done.insert(id.value);
                ordered.push_back(id);
            };
            for (const gir::CallableId id : internal) { visit(id); }
            return ordered;
        }

        // ------------------------------------------------------------ module

        EmittedModule Emitter::emit() {
            bind_hgraph_declarations();
            EmittedModule result;
            result.module_name = graph_.path;
            if (result.module_name.empty()) { fail(Category::Module, SourceRange{0, 0}, "emit-cpp needs a module declaration"); }
            namespace_            = module_namespace(graph_);
            result.namespace_name = namespace_;
            module_name_          = result.module_name;
            basename_             = file_.path();
            if (const auto slash = basename_.find_last_of("/\\"); slash != std::string::npos) { basename_.erase(0, slash + 1); }

            // Every emitted function is checked up front so the whole unit
            // fails closed before a partial pair is written.
            std::vector<gir::CallableId>       exports;
            std::vector<gir::CallableId>       impls;
            std::map<std::string, std::string> cpp_functions;
            for (const gir::CallableId id : callable_declarations_) {
                const gir::Callable &fn = callable(id);
                const bool           generic_implementation =
                    fn.visibility == gir::CallableVisibility::Implementation && !fn.generics.empty();
                if (!generic_implementation) { check_supported(id); }
                const std::string source_name{callable_name(id)};
                const std::string generated_name = callable_cpp_name(id);
                if (const auto [found, inserted] = cpp_functions.emplace(generated_name, source_name);
                    !inserted && found->second != source_name) {
                    backend(fn.range,
                            "C++ function '" + source_name + "' collides with '" + found->second + "' as '" + generated_name + "'");
                }
                std::map<std::string, std::string> cpp_parameters;
                for (const gir::Parameter &param : fn.parameters) {
                    const std::string parameter_name{param.name};
                    const std::string generated_parameter = cpp_name(parameter_name);
                    if (const auto [found, inserted] = cpp_parameters.emplace(generated_parameter, parameter_name);
                        !inserted && found->second != parameter_name) {
                        backend(graph_type(param.type, fn.range).range, "C++ parameter '" + parameter_name + "' collides with '" +
                                                                            found->second + "' as '" + generated_parameter + "'");
                    }
                }
                if (callable(id).visibility == gir::CallableVisibility::Export) { exports.push_back(id); }
                if (callable(id).visibility == gir::CallableVisibility::Implementation) { impls.push_back(id); }
            }
            const std::vector<gir::CallableId> internal = ordered_internal_functions();
            std::set<std::string>              native_headers;
            std::set<std::string>              cmake_packages{"hgraph"};
            std::set<std::string>              imported_targets{"hgraph::core"};
            std::set<std::string>              runtime_images;
            for (const gir::NativeFunction &native : graph_.native_functions) {
                for (const std::string &header : native.public_headers) {
                    if (!is_public_header_name(header)) {
                        backend({}, "native function '" + native.identity + "' names an unsafe public header '" + header + "'");
                    }
                    native_headers.insert(header);
                }
                cmake_packages.insert(native.cmake_packages.begin(), native.cmake_packages.end());
                imported_targets.insert(native.imported_targets.begin(), native.imported_targets.end());
                runtime_images.insert(native.runtime_images.begin(), native.runtime_images.end());
            }

            // Bodies first: they discover which kernels (analytics) the
            // header must include. Anonymous graph bodies are collected while
            // their containing functions emit, then placed before every use.
            Writer private_functions;
            for (const gir::CallableId id : internal) { emit_function(id, private_functions, Form::InlineStruct); }
            for (const gir::CallableId id : impls) {
                if (callable(id).generics.empty()) { emit_function(id, private_functions, Form::InlineStruct); }
            }
            for (std::size_t index = 0; index < graph_.materializations.size(); ++index) {
                const gir::Materialization &materialization = graph_.materializations[index];
                begin_materialization(materialization, index);
                check_supported(materialization.implementation);
                emit_function(materialization.implementation, private_functions, Form::InlineStruct);
                end_materialization();
            }
            Writer public_functions;
            for (const gir::CallableId id : exports) {
                if (callable(id).kind == gir::CallableKind::Composition) { emit_function(id, public_functions, Form::OutOfLine); }
            }

            Writer body;
            body.line("namespace " + namespace_);
            body.line("{");
            if (!internal.empty() || !impls.empty() || !generated_helpers_.str().empty()) {
                body.indent();
                body.open("namespace");
                const bool has_internal_runtime = std::any_of(internal.begin(), internal.end(), [&](gir::CallableId id) {
                    return callable(id).kind == gir::CallableKind::RuntimeNode;
                });
                if (has_internal_runtime) { body.open("namespace operator_contracts"); }
                for (const gir::CallableId id : internal) {
                    if (callable(id).kind != gir::CallableKind::RuntimeNode) { continue; }
                    const gir::Callable &item = callable(id);
                    body.line("using " + callable_cpp_name(id) + " = " +
                              operator_contract(item.parameters, item.result, item.identity) + ";");
                }
                if (has_internal_runtime) {
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
            for (const gir::CallableId id : exports) {
                const std::string name         = callable_cpp_name(id);
                const std::string registration = callable(id).kind == gir::CallableKind::RuntimeNode
                                                     ? "hgraph::register_overload"
                                                     : "hgraph::register_graph_overload";
                body.line(registration + "<operators::" + name + ", " + name + ">();");
            }
            for (const gir::CallableId id : internal) {
                if (callable(id).kind != gir::CallableKind::RuntimeNode) { continue; }
                const std::string name = callable_cpp_name(id);
                body.line("hgraph::register_overload<operator_contracts::" + name + ", " + name + ">();");
            }
            for (const gir::CallableId id : impls) {
                const gir::Callable &implementation = callable(id);
                if (!implementation.generics.empty()) { continue; }
                const auto contract = std::find_if(graph_.operators.begin(), graph_.operators.end(), [&](const auto &candidate) {
                    return candidate.identity == implementation.operator_identity;
                });
                if (contract == graph_.operators.end() || contract->imported) {
                    unsupported(implementation.range, "an impl fn of an imported operator");
                }
                const std::string registration = implementation.kind == gir::CallableKind::RuntimeNode
                                                     ? "hgraph::register_overload"
                                                     : "hgraph::register_graph_overload";
                body.line(registration + "<operators::" + cpp_name(local_identity(contract->identity)) + ", " +
                          callable_cpp_name(id) + ">();");
            }
            for (std::size_t index = 0; index < graph_.materializations.size(); ++index) {
                const gir::Materialization &materialization = graph_.materializations[index];
                const gir::Callable        &implementation  = callable(materialization.implementation, materialization.range);
                const auto contract = std::find_if(graph_.operators.begin(), graph_.operators.end(), [&](const auto &candidate) {
                    return candidate.identity == implementation.operator_identity;
                });
                if (contract == graph_.operators.end() || contract->imported) {
                    unsupported(implementation.range, "an instantiated impl fn of an imported operator");
                }
                const std::string registration = implementation.kind == gir::CallableKind::RuntimeNode
                                                     ? "hgraph::register_overload"
                                                     : "hgraph::register_graph_overload";
                body.line(registration + "<operators::" + cpp_name(local_identity(contract->identity)) + ", " +
                          materialization_cpp_name(materialization, index) + ">();");
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
            Writer            header;
            const std::string banner = "// Generated by hgl " + options_.tool_version + " from " + basename_ + "; do not edit.";
            header.line(banner);
            header.line("#pragma once");
            header.line();
            for (const std::string &native_header : native_headers) { header.line("#include <" + native_header + ">"); }
            if (!native_headers.empty()) { header.line(); }
            header.line("#include <hgraph/lib/std/operators/operators.h>");
            if (uses_analytics_) { header.line("#include <hgraph/analytics/operators.h>"); }
            header.line("#include <hgraph/types/graph_wiring.h>");
            header.line("#include <hgraph/types/subgraph_wiring.h>");
            header.line("#include <hgraph/types/operator_dispatch.h>");
            header.line("#include <hgraph/types/static_node.h>");
            header.line("#include <hgraph/types/static_schema.h>");
            header.line();
            header.line("#include <chrono>");
            header.line("#include <cstddef>");
            header.line("#include <cstdint>");
            header.line("#include <limits>");
            header.line("#include <stdexcept>");
            header.line("#include <tuple>");
            header.line();
            header.line("namespace " + namespace_);
            header.line("{");
            header.indent();
            for (const gir::StructId id : structure_declarations_) { emit_struct(struct_contract(id), header); }
            if (!operator_declarations_.empty() || !exports.empty()) {
                header.line("/// Operator contracts for the module's public callables.");
                header.open("namespace operators");
                for (const gir::OperatorId id : operator_declarations_) {
                    const gir::OperatorContract &op = operator_decl(id);
                    header.line("// " + where(op.range));
                    header.line("using " + cpp_name(local_identity(op.identity)) + " = " +
                                operator_contract(op.parameters, op.result, operator_registry_name(op)) + ";");
                }
                for (const gir::CallableId id : exports) {
                    const gir::Callable &item = callable(id);
                    header.line("// " + where(item.range));
                    header.line("using " + callable_cpp_name(id) + " = " +
                                operator_contract(item.parameters, item.result, item.identity) + ";");
                }
                header.close("  // namespace operators");
                header.line();
            }
            for (const gir::CallableId id : exports) {
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

            if (!options_.python_native_module.empty()) {
                if (!is_python_identifier(options_.python_native_module) || is_python_keyword(options_.python_native_module)) {
                    backend(SourceRange{0, 0},
                            "'" + options_.python_native_module + "' is not a valid Python native-module identifier");
                }
                std::string py;
                py += "\"\"\"Generated by hgl " + options_.tool_version + " from " + basename_ + "; do not edit.\n\n";
                py += "Python surface of HGL module ``" + result.module_name + "``: importing this module loads the\n";
                py += "native registration module and exposes each exported function as an hgraph operator.\n\"\"\"\n\n";
                py += "from hgraph import operator_function as _hgl_operator_function\n\n";
                py += "from . import " + options_.python_native_module +
                      " as _hgl_native  # noqa: F401  (registers the operators)\n\n";
                py += "globals().update({\n";
                std::vector<std::string>           names;
                std::map<std::string, std::string> python_exports;
                for (std::size_t i = 0; i < result.exports.size(); ++i) {
                    const std::string &name  = result.exports[i];
                    const std::string  alias = python_name(name);
                    if (const auto [found, inserted] = python_exports.emplace(alias, name); !inserted) {
                        backend(callable(exports[i]).range,
                                "Python export '" + name + "' collides with '" + found->second + "' as '" + alias + "'");
                    }
                    py += "    " + quote(alias) + ": _hgl_operator_function(" + quote(callable(exports[i]).identity) + "),\n";
                    names.push_back(quote(alias));
                }
                py += "})\n\n__all__ = [" + join(names, ", ") + "]\n";
                result.python = std::move(py);
            }
            descriptor::DescribeOptions descriptor_options;
            descriptor_options.language_version  = options_.tool_version;
            descriptor_options.provider_identity = result.module_name;
            descriptor_options.public_headers    = {options_.header_name};
            descriptor_options.public_headers.insert(descriptor_options.public_headers.end(), native_headers.begin(),
                                                     native_headers.end());
            descriptor_options.cmake_packages.assign(cmake_packages.begin(), cmake_packages.end());
            descriptor_options.imported_targets.assign(imported_targets.begin(), imported_targets.end());
            descriptor_options.runtime_images.assign(runtime_images.begin(), runtime_images.end());
            descriptor_options.registration_symbol        = namespace_ + "::register_operators";
            const descriptor::ModuleDescriptor descriptor = descriptor::describe_module(graph_, std::move(descriptor_options));
            result.descriptor_fingerprint                 = descriptor.descriptor_fingerprint;
            result.descriptor                             = descriptor::to_json(descriptor);
            return result;
        }
    }  // namespace

    std::string module_namespace(const hgraph_ir::Module &graph) {
        std::string ns;
        std::string part;
        for (const char c : graph.path) {
            if (c == '.') {
                ns += cpp_name(part) + "::";
                part.clear();
            } else {
                part += c;
            }
        }
        ns += cpp_name(part);
        return ns;
    }

    std::optional<EmittedModule> emit_cpp(const syntax::SourceFile &file, const hgraph_ir::Module &graph,
                                          const EmitOptions &options, syntax::DiagnosticSink &diagnostics) {
        Emitter emitter{file, graph, options, diagnostics};
        try {
            return emitter.emit();
        } catch (const Abort &) { return std::nullopt; }
    }
}  // namespace hgl::codegen
