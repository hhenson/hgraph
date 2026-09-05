#include "wiring/backend.h"

#include "syntax/temporal.h"
#include "wiring/type_bridge.h"

#include <hgraph/lib/std/operators/registration.h>
#include <hgraph/lib/std/standard_types.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/runtime/evaluation_clock.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/record_replay.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/types/value/value_view.h>
#include <hgraph/util/scope.h>

#if defined(HGL_HAVE_ANALYTICS)
    #include <hgraph/analytics/operators.h>
#endif

#include <algorithm>
#include <chrono>
#include <compare>
#include <exception>
#include <iostream>
#include <optional>
#include <span>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

// The direct backend consumes only execution-facing hgraph IR. Syntax and
// semantic identities end at hgraph_ir::lower; this layer folds scalar values,
// expands composition callables, and delegates every temporal operation to
// hgraph's ordinary operator registry and graph runtime.
namespace hgl::wiring
{
    namespace
    {
        namespace hir = ir::hir;
        namespace gir = hgraph_ir;

        using syntax::Category;
        using syntax::SourceRange;

        std::ostream *sink_stream = &std::cout;

        struct print_tick : hgraph::Operator<"hgl.print_tick", hgraph::In<"ts", hgraph::TsVar<"S">>>
        {};

        struct print_tick_impl
        {
            static void eval(hgraph::In<"ts", hgraph::TsVar<"S">> ts, hgraph::EvaluationClockView clock) {
                *sink_stream << format_time(clock.evaluation_time()) << ' ' << ts.value().to_string() << '\n';
            }
        };

        const hgraph::stdlib::RegisteredStandardTypes &standard_types() {
            struct Cached
            {
                std::uint64_t                           generation{0};
                hgraph::stdlib::RegisteredStandardTypes types{};
            };
            thread_local Cached cached;
            auto               &registry = hgraph::TypeRegistry::instance();
            const std::uint64_t current  = registry.reset_generation();
            if (cached.types.bool_type == nullptr || cached.generation != current) {
                cached.types      = hgraph::stdlib::register_standard_types(registry);
                cached.generation = registry.reset_generation();
            }
            return cached.types;
        }

        struct Abort
        {};

        struct TestFailure
        { std::string message; };

        struct Slot
        {
            enum class Kind : std::uint8_t {
                Void,
                Const,
                Null,
                Placeholder,
                Delta,
                Port,
                Struct,
                Function,
                Operator,
                Intrinsic,
                Sequence,
            };

            Kind                                      kind{Kind::Void};
            hgraph::Value                             value{};
            hgraph::WiringPortRef                     port{};
            gir::CallableId                           callable{};
            gir::TypeId                               type{};
            std::string                               name{};
            gir::ValueId                              expression{};
            bool                                      resolved{false};
            std::vector<std::optional<hgraph::Value>> elements{};
            const hgraph::ValueTypeMetaData          *element_meta{nullptr};
            SourceRange                               range{};

            [[nodiscard]] bool                             is_const() const noexcept { return kind == Kind::Const; }
            [[nodiscard]] bool                             is_port() const noexcept { return kind == Kind::Port; }
            [[nodiscard]] const hgraph::ValueTypeMetaData *meta() const noexcept { return value.schema(); }
        };

        Slot make_const(hgraph::Value value, SourceRange range) {
            Slot result;
            result.kind  = Slot::Kind::Const;
            result.value = std::move(value);
            result.range = range;
            return result;
        }

        Slot make_port(hgraph::WiringPortRef port, SourceRange range) {
            Slot result;
            result.kind  = Slot::Kind::Port;
            result.port  = std::move(port);
            result.range = range;
            return result;
        }

        Slot make_marker(Slot::Kind kind, SourceRange range) {
            Slot result;
            result.kind  = kind;
            result.range = range;
            return result;
        }

        struct Frame
        {
            gir::CallableId                         callable{};
            std::unordered_map<std::uint32_t, Slot> bindings{};
            std::optional<Slot>                     returned{};
            bool                                    in_test{false};
        };

        hgraph::WiringArg time_series_arg(hgraph::WiringPortRef port, std::string name = {}) {
            hgraph::WiringArg result;
            result.kind = hgraph::WiringArg::Kind::TimeSeries;
            result.port = std::move(port);
            result.name = std::move(name);
            return result;
        }

        hgraph::WiringArg scalar_arg(const hgraph::Value &value, std::string name = {}) {
            hgraph::WiringArg result;
            result.kind         = hgraph::WiringArg::Kind::Scalar;
            result.scalar_value = value;
            result.scalar_meta  = value.schema();
            result.name         = std::move(name);
            return result;
        }

        std::string local_name(std::string_view identity) {
            const std::size_t separator = identity.rfind('.');
            return std::string{separator == std::string_view::npos ? identity : identity.substr(separator + 1U)};
        }

        std::string binary_spelling(hir::BinaryOp op) {
            switch (op) {
                case hir::BinaryOp::Mul: return "*";
                case hir::BinaryOp::Div: return "/";
                case hir::BinaryOp::Rem: return "%";
                case hir::BinaryOp::Add: return "+";
                case hir::BinaryOp::Sub: return "-";
                case hir::BinaryOp::Less: return "<";
                case hir::BinaryOp::LessEqual: return "<=";
                case hir::BinaryOp::Greater: return ">";
                case hir::BinaryOp::GreaterEqual: return ">=";
                case hir::BinaryOp::Equal: return "==";
                case hir::BinaryOp::NotEqual: return "!=";
                case hir::BinaryOp::And: return "&&";
                case hir::BinaryOp::Or: return "||";
            }
            return "?";
        }

        std::string describe_view(const hgraph::ValueView &view) {
            if (view.schema() == standard_types().float_type) {
                std::string text = view.to_string();
                if (text.find_first_of(".ean") == std::string::npos) { text += ".0"; }
                return text;
            }
            if (view.is_tuple()) {
                const auto  tuple = view.as_tuple();
                std::string out   = "(";
                for (std::size_t index = 0; index < tuple.size(); ++index) {
                    if (index != 0) { out += ", "; }
                    out += describe_view(tuple.at(index));
                }
                return out + (tuple.size() == 1 ? ",)" : ")");
            }
            if (view.is_list()) {
                const auto  list = view.as_list();
                std::string out  = "[";
                for (std::size_t index = 0; index < list.size(); ++index) {
                    if (index != 0) { out += ", "; }
                    out += describe_view(list.at(index));
                }
                return out + "]";
            }
            return view.to_string();
        }

        std::string describe_value(const hgraph::Value &value) { return describe_view(value.view()); }

        std::string describe_sequence(const std::vector<std::optional<hgraph::Value>> &elements) {
            std::string out = "[";
            for (std::size_t index = 0; index < elements.size(); ++index) {
                if (index != 0) { out += ", "; }
                out += elements[index] ? describe_value(*elements[index]) : "_";
            }
            return out + ']';
        }

        class Compiler
        {
          public:
            Compiler(const syntax::SourceFile &file, const gir::Module &module, syntax::DiagnosticSink &diagnostics)
                : file_{file}, module_{module}, diagnostics_{diagnostics}, bridge_{module, diagnostics},
                  registry_{hgraph::TypeRegistry::instance()} {}

            [[nodiscard]] std::vector<TestResult>      run_tests(const TestOptions &options);
            [[nodiscard]] bool                         run_program(const RunOptions &options, std::ostream &out);
            [[nodiscard]] std::optional<hgraph::Value> evaluate_constant(gir::ValueId value);

          private:
            [[noreturn]] void fail(Category category, SourceRange range, std::string message) {
                diagnostics_.report(category, range, std::move(message));
                throw Abort{};
            }

            [[noreturn]] void backend(SourceRange range, std::string message) {
                fail(Category::Backend, range, std::move(message));
            }

            [[nodiscard]] const gir::Value &value(gir::ValueId id) {
                if (!id.valid() || id.value >= module_.values.size()) { backend({}, "invalid hgraph IR value ID"); }
                return module_.values[id.value];
            }

            [[nodiscard]] const gir::Callable &callable(gir::CallableId id) {
                if (!id.valid() || id.value >= module_.callables.size()) { backend({}, "invalid hgraph IR callable ID"); }
                return module_.callables[id.value];
            }

            [[nodiscard]] const gir::Binding &binding(gir::BindingId id) {
                if (!id.valid() || id.value >= module_.bindings.size()) { backend({}, "invalid hgraph IR binding ID"); }
                return module_.bindings[id.value];
            }

            [[nodiscard]] const gir::StructContract &structure(gir::TypeId type) {
                if (!type.valid() || type.value >= module_.types.size()) { backend({}, "invalid hgraph IR type ID"); }
                while (module_.types[type.value].kind == hir::TypeKind::Atomic) {
                    if (module_.types[type.value].children.size() != 1U) {
                        backend(module_.types[type.value].range, "an atomic struct type needs one child");
                    }
                    type = module_.types[type.value].children.front();
                    if (!type.valid() || type.value >= module_.types.size()) { backend({}, "invalid atomic child type ID"); }
                }
                const std::string &identity = module_.types[type.value].nominal_identity;
                const auto         found = std::find_if(module_.structures.begin(), module_.structures.end(),
                                                        [&](const gir::StructContract &item) { return item.identity == identity; });
                if (found == module_.structures.end()) {
                    backend(module_.types[type.value].range, "unknown struct '" + identity + "'");
                }
                return *found;
            }

            [[nodiscard]] const hgraph::ValueTypeMetaData *value_meta(gir::TypeId type) {
                const auto *result = bridge_.value(type);
                if (result == nullptr) { throw Abort{}; }
                return result;
            }

            [[nodiscard]] const hgraph::TSValueTypeMetaData *schema(gir::TypeId type) {
                const auto *result = bridge_.schema(type);
                if (result == nullptr) { throw Abort{}; }
                return result;
            }

            [[nodiscard]] std::string slice(SourceRange range) const { return std::string{file_.slice(range)}; }

            [[nodiscard]] Slot          constant(const hir::Constant &constant, SourceRange range);
            [[nodiscard]] Slot          eval_const_expr(gir::ConstExprId id, Frame &frame);
            [[nodiscard]] hgraph::Value convert(const hgraph::Value &source, const hgraph::ValueTypeMetaData *target,
                                                SourceRange range, std::string_view role);
            [[nodiscard]] Slot          fold_unary(hir::UnaryOp op, const Slot &operand, SourceRange range);
            [[nodiscard]] Slot          fold_binary(hir::BinaryOp op, const Slot &lhs, const Slot &rhs, SourceRange range);
            [[nodiscard]] Slot compare_sequences(const Slot &lhs, const Slot &rhs, bool negate, SourceRange range, Frame &frame);
            void               resolve_sequence(Slot &slot, const hgraph::ValueTypeMetaData *element, Frame &frame);
            [[nodiscard]] Slot constant_of(const Slot &slot, const hgraph::ValueTypeMetaData *meta, Frame &frame,
                                           std::string_view role);

            [[nodiscard]] hgraph::Wiring &wiring(SourceRange range) {
                if (wiring_ == nullptr) { backend(range, "a time-series expression is only wired inside eval or a run"); }
                return *wiring_;
            }

            [[nodiscard]] Slot              wire(std::string_view name, std::vector<hgraph::WiringArg> arguments, SourceRange range,
                                                 std::optional<bool>                output_required = std::nullopt,
                                                 const hgraph::TSValueTypeMetaData *expected        = nullptr);
            [[nodiscard]] hgraph::WiringArg argument_of(const Slot &slot, std::string name);
            [[nodiscard]] Slot              wire_constant(const Slot &slot, const hgraph::TSValueTypeMetaData *target);
            [[nodiscard]] Slot wire_binary(const gir::Value &expression, hir::BinaryOp op, const Slot &lhs, const Slot &rhs);

            [[nodiscard]] Slot eval_value(gir::ValueId id, Frame &frame);
            [[nodiscard]] Slot eval_reference(const gir::Reference &reference, SourceRange range, Frame &frame);
            [[nodiscard]] Slot eval_call(const gir::Value &expression, const gir::Call &call, Frame &frame);
            [[nodiscard]] Slot eval_intrinsic(std::string_view name, const std::vector<gir::Argument> &arguments, SourceRange range,
                                              Frame &frame);
            [[nodiscard]] Slot eval_harness(const gir::HarnessEval &eval, SourceRange range, Frame &frame);
            [[nodiscard]] Slot eval_sequence(gir::ValueId id, const gir::Sequence &sequence, SourceRange range, Frame &frame);
            [[nodiscard]] Slot eval_tuple(const gir::Tuple &tuple, SourceRange range, Frame &frame);
            [[nodiscard]] Slot assemble_construct(gir::TypeId type, std::vector<std::pair<std::string, Slot>> supplied, bool delta,
                                                  SourceRange range, Frame &frame);
            [[nodiscard]] Slot eval_construct(gir::TypeId type, const std::vector<gir::Argument> &arguments, bool delta,
                                              SourceRange range, Frame &frame);
            [[nodiscard]] Slot call_function(gir::CallableId id, const std::vector<gir::Argument> &arguments, SourceRange range,
                                             Frame &frame);
            [[nodiscard]] Slot wire_function(gir::CallableId id, Frame &frame, SourceRange range);
            [[nodiscard]] Slot invoke(gir::CallableId id, Frame &frame);
            [[nodiscard]] Slot exec_block(gir::BlockId id, Frame &frame);
            void               exec_statement(gir::StatementId id, Frame &frame);
            [[nodiscard]] std::vector<std::optional<gir::ValueId>>
            bind_arguments(const gir::Callable &target, const std::vector<gir::Argument> &arguments, SourceRange range);
            [[nodiscard]] Slot        bind_parameter(const gir::Parameter &parameter, const Slot &argument, Frame &frame,
                                                     SourceRange range);
            [[nodiscard]] std::string describe(const Slot &slot);

            const syntax::SourceFile                      &file_;
            const gir::Module                             &module_;
            syntax::DiagnosticSink                        &diagnostics_;
            TypeBridge                                     bridge_;
            hgraph::TypeRegistry                          &registry_;
            const hgraph::stdlib::RegisteredStandardTypes &types_{standard_types()};
            hgraph::Wiring                                *wiring_{nullptr};
            std::string                                    comparison_detail_{};
        };

        Slot Compiler::constant(const hir::Constant &source, SourceRange range) {
            return std::visit(
                [&](const auto &item) -> Slot {
                    using T = std::decay_t<decltype(item)>;
                    if constexpr (std::is_same_v<T, bool> || std::is_same_v<T, std::int64_t> || std::is_same_v<T, double> ||
                                  std::is_same_v<T, std::string>) {
                        return make_const(hgraph::Value{item}, range);
                    } else if constexpr (std::is_same_v<T, hir::NullValue>) {
                        return make_marker(Slot::Kind::Null, range);
                    } else if constexpr (std::is_same_v<T, hir::PlaceholderValue>) {
                        return make_marker(Slot::Kind::Placeholder, range);
                    } else if constexpr (std::is_same_v<T, syntax::TemporalValue>) {
                        switch (item.kind) {
                            case syntax::TemporalKind::Date:
                                return make_const(
                                    hgraph::Value{hgraph::Date{std::chrono::sys_days{std::chrono::days{item.micros}}}}, range);
                            case syntax::TemporalKind::Time: return make_const(hgraph::Value{hgraph::Time{item.micros}}, range);
                            case syntax::TemporalKind::DateTime:
                                return make_const(hgraph::Value{hgraph::DateTime{std::chrono::microseconds{item.micros}}}, range);
                            case syntax::TemporalKind::Duration:
                                return make_const(hgraph::Value{hgraph::TimeDelta{item.micros}}, range);
                            case syntax::TemporalKind::CivilDateTime:
                            case syntax::TemporalKind::ZonedDateTime:
                            case syntax::TemporalKind::ZonedTime:
                            case syntax::TemporalKind::TimeZone:
                                backend(range, "zoned and civil literals are not supported by the first pass");
                        }
                    }
                    backend(range, "unsupported hgraph IR constant");
                },
                source);
        }

        hgraph::Value Compiler::convert(const hgraph::Value &source, const hgraph::ValueTypeMetaData *target, SourceRange range,
                                        std::string_view role) {
            const hgraph::ValueTypeMetaData *actual = source.schema();
            if (actual == target) { return source; }
            if (actual == types_.int_type && target == types_.float_type) {
                return hgraph::Value{static_cast<hgraph::Float>(source.view().checked_as<hgraph::Int>())};
            }
            if (actual->try_value_kind() == hgraph::ValueTypeKind::Tuple &&
                target->try_value_kind() == hgraph::ValueTypeKind::Tuple && actual->field_count == target->field_count) {
                hgraph::Value result{hgraph::ValuePlanFactory::instance().type_for(target)};
                auto          output = result.as_tuple().begin_mutation();
                const auto    input  = source.view().as_tuple();
                for (std::size_t index = 0; index < target->field_count; ++index) {
                    output.at(index).copy_from(
                        convert(hgraph::Value{input.at(index)}, target->fields[index].type, range, role).view());
                }
                return result;
            }
            if (actual->try_value_kind() == hgraph::ValueTypeKind::List &&
                target->try_value_kind() == hgraph::ValueTypeKind::List) {
                hgraph::ListBuilder output{hgraph::ValuePlanFactory::instance().type_for(target->element_type), *target};
                const auto          input = source.view().as_list();
                for (std::size_t index = 0; index < input.size(); ++index) {
                    output.push_back(convert(hgraph::Value{input.at(index)}, target->element_type, range, role).view());
                }
                return output.build();
            }
            fail(Category::Type, range,
                 std::string{role} + " expects " + std::string{target->name()} + ", got " + std::string{actual->name()});
        }

        Slot Compiler::fold_unary(hir::UnaryOp op, const Slot &operand, SourceRange range) {
            if (op == hir::UnaryOp::Negate) {
                if (operand.meta() == types_.int_type) {
                    return make_const(hgraph::Value{hgraph::Int{-operand.value.view().checked_as<hgraph::Int>()}}, range);
                }
                if (operand.meta() == types_.float_type) {
                    return make_const(hgraph::Value{hgraph::Float{-operand.value.view().checked_as<hgraph::Float>()}}, range);
                }
                if (operand.meta() == types_.timedelta_type) {
                    return make_const(hgraph::Value{-operand.value.view().checked_as<hgraph::TimeDelta>()}, range);
                }
                fail(Category::Type, range, "unary '-' needs a number, got " + std::string{operand.meta()->name()});
            }
            if (operand.meta() == types_.bool_type) {
                return make_const(hgraph::Value{hgraph::Bool{!operand.value.view().checked_as<hgraph::Bool>()}}, range);
            }
            fail(Category::Type, range, "'!' needs a bool, got " + std::string{operand.meta()->name()});
        }

        Slot Compiler::fold_binary(hir::BinaryOp op, const Slot &lhs, const Slot &rhs, SourceRange range) {
            const bool lhs_int   = lhs.meta() == types_.int_type;
            const bool rhs_int   = rhs.meta() == types_.int_type;
            const bool lhs_float = lhs.meta() == types_.float_type;
            const bool rhs_float = rhs.meta() == types_.float_type;
            const bool numeric   = (lhs_int || lhs_float) && (rhs_int || rhs_float);
            const auto number    = [&](const Slot &item) {
                return item.meta() == types_.int_type ? static_cast<hgraph::Float>(item.value.view().checked_as<hgraph::Int>())
                                                      : item.value.view().checked_as<hgraph::Float>();
            };
            const auto type_error = [&]() -> Slot {
                fail(Category::Type, range,
                     "'" + binary_spelling(op) + "' is not defined for " + std::string{lhs.meta()->name()} + " and " +
                         std::string{rhs.meta()->name()});
            };
            switch (op) {
                case hir::BinaryOp::Add:
                    if (lhs_int && rhs_int) {
                        return make_const(hgraph::Value{hgraph::Int{lhs.value.view().checked_as<hgraph::Int>() +
                                                                    rhs.value.view().checked_as<hgraph::Int>()}},
                                          range);
                    }
                    if (numeric) { return make_const(hgraph::Value{number(lhs) + number(rhs)}, range); }
                    if (lhs.meta() == types_.str_type && rhs.meta() == types_.str_type) {
                        return make_const(
                            hgraph::Value{lhs.value.view().checked_as<hgraph::Str>() + rhs.value.view().checked_as<hgraph::Str>()},
                            range);
                    }
                    if (lhs.meta() == types_.timedelta_type && rhs.meta() == types_.timedelta_type) {
                        return make_const(hgraph::Value{lhs.value.view().checked_as<hgraph::TimeDelta>() +
                                                        rhs.value.view().checked_as<hgraph::TimeDelta>()},
                                          range);
                    }
                    if (lhs.meta() == types_.datetime_type && rhs.meta() == types_.timedelta_type) {
                        return make_const(hgraph::Value{lhs.value.view().checked_as<hgraph::DateTime>() +
                                                        rhs.value.view().checked_as<hgraph::TimeDelta>()},
                                          range);
                    }
                    return type_error();
                case hir::BinaryOp::Sub:
                    if (lhs_int && rhs_int) {
                        return make_const(hgraph::Value{hgraph::Int{lhs.value.view().checked_as<hgraph::Int>() -
                                                                    rhs.value.view().checked_as<hgraph::Int>()}},
                                          range);
                    }
                    if (numeric) { return make_const(hgraph::Value{number(lhs) - number(rhs)}, range); }
                    if (lhs.meta() == types_.timedelta_type && rhs.meta() == types_.timedelta_type) {
                        return make_const(hgraph::Value{lhs.value.view().checked_as<hgraph::TimeDelta>() -
                                                        rhs.value.view().checked_as<hgraph::TimeDelta>()},
                                          range);
                    }
                    if (lhs.meta() == types_.datetime_type && rhs.meta() == types_.timedelta_type) {
                        return make_const(hgraph::Value{lhs.value.view().checked_as<hgraph::DateTime>() -
                                                        rhs.value.view().checked_as<hgraph::TimeDelta>()},
                                          range);
                    }
                    if (lhs.meta() == types_.datetime_type && rhs.meta() == types_.datetime_type) {
                        return make_const(hgraph::Value{lhs.value.view().checked_as<hgraph::DateTime>() -
                                                        rhs.value.view().checked_as<hgraph::DateTime>()},
                                          range);
                    }
                    return type_error();
                case hir::BinaryOp::Mul:
                    if (lhs_int && rhs_int) {
                        return make_const(hgraph::Value{hgraph::Int{lhs.value.view().checked_as<hgraph::Int>() *
                                                                    rhs.value.view().checked_as<hgraph::Int>()}},
                                          range);
                    }
                    if (numeric) { return make_const(hgraph::Value{number(lhs) * number(rhs)}, range); }
                    if (lhs.meta() == types_.timedelta_type && rhs_int) {
                        return make_const(hgraph::Value{lhs.value.view().checked_as<hgraph::TimeDelta>() *
                                                        rhs.value.view().checked_as<hgraph::Int>()},
                                          range);
                    }
                    return type_error();
                case hir::BinaryOp::Div:
                    if (numeric) {
                        if (number(rhs) == 0.0) { fail(Category::Type, range, "division by zero"); }
                        return make_const(hgraph::Value{number(lhs) / number(rhs)}, range);
                    }
                    return type_error();
                case hir::BinaryOp::Rem:
                    if (lhs_int && rhs_int) {
                        const auto divisor = rhs.value.view().checked_as<hgraph::Int>();
                        if (divisor == 0) { fail(Category::Type, range, "division by zero"); }
                        return make_const(hgraph::Value{hgraph::Int{lhs.value.view().checked_as<hgraph::Int>() % divisor}}, range);
                    }
                    return type_error();
                case hir::BinaryOp::Equal:
                case hir::BinaryOp::NotEqual:
                    {
                        bool equal = false;
                        if (numeric) {
                            equal = number(lhs) == number(rhs);
                        } else if (lhs.meta() == rhs.meta()) {
                            equal = lhs.value.view().equals(rhs.value.view());
                        } else {
                            return type_error();
                        }
                        return make_const(hgraph::Value{hgraph::Bool{op == hir::BinaryOp::Equal ? equal : !equal}}, range);
                    }
                case hir::BinaryOp::Less:
                case hir::BinaryOp::LessEqual:
                case hir::BinaryOp::Greater:
                case hir::BinaryOp::GreaterEqual:
                    {
                        std::partial_ordering order = std::partial_ordering::unordered;
                        if (numeric) {
                            order = number(lhs) <=> number(rhs);
                        } else if (lhs.meta() == rhs.meta()) {
                            order = lhs.value.view().compare(rhs.value.view());
                        } else {
                            return type_error();
                        }
                        if (order == std::partial_ordering::unordered) { return type_error(); }
                        const bool result = op == hir::BinaryOp::Less        ? order < 0
                                            : op == hir::BinaryOp::LessEqual ? order <= 0
                                            : op == hir::BinaryOp::Greater   ? order > 0
                                                                             : order >= 0;
                        return make_const(hgraph::Value{hgraph::Bool{result}}, range);
                    }
                case hir::BinaryOp::And:
                case hir::BinaryOp::Or:
                    if (lhs.meta() == types_.bool_type && rhs.meta() == types_.bool_type) {
                        const bool left  = lhs.value.view().checked_as<hgraph::Bool>();
                        const bool right = rhs.value.view().checked_as<hgraph::Bool>();
                        return make_const(hgraph::Value{hgraph::Bool{op == hir::BinaryOp::And ? left && right : left || right}},
                                          range);
                    }
                    return type_error();
            }
            return type_error();
        }

        Slot Compiler::eval_const_expr(gir::ConstExprId id, Frame &frame) {
            if (!id.valid() || id.value >= module_.const_exprs.size()) { backend({}, "invalid constant expression ID"); }
            const gir::ConstExpr &expression = module_.const_exprs[id.value];
            if (expression.literal) { return constant(*expression.literal, expression.range); }
            switch (expression.kind) {
                case gir::ConstExprKind::Parameter:
                    {
                        const auto found = frame.bindings.find(expression.parameter_binding.value);
                        if (!expression.parameter_binding.valid() || found == frame.bindings.end()) {
                            backend(expression.range, "constant parameter '" + expression.parameter + "' is not bound");
                        }
                        return found->second;
                    }
                case gir::ConstExprKind::Unary:
                    return fold_unary(expression.unary, eval_const_expr(expression.lhs, frame), expression.range);
                case gir::ConstExprKind::Binary:
                    return fold_binary(expression.binary, eval_const_expr(expression.lhs, frame),
                                       eval_const_expr(expression.rhs, frame), expression.range);
                case gir::ConstExprKind::Index:
                    {
                        const Slot target = eval_const_expr(expression.lhs, frame);
                        const Slot index  = eval_const_expr(expression.rhs, frame);
                        if (!target.is_const() || index.meta() != types_.int_type) {
                            backend(expression.range, "a compile-time index needs a tuple/list and i64");
                        }
                        const auto position = index.value.view().checked_as<hgraph::Int>();
                        if (position < 0) { fail(Category::Type, expression.range, "index out of range"); }
                        if (target.value.view().is_tuple()) {
                            const auto input = target.value.view().as_tuple();
                            if (static_cast<std::size_t>(position) >= input.size()) {
                                fail(Category::Type, expression.range, "tuple index out of range");
                            }
                            return make_const(hgraph::Value{input.at(static_cast<std::size_t>(position))}, expression.range);
                        }
                        if (target.value.view().is_list()) {
                            const auto input = target.value.view().as_list();
                            if (static_cast<std::size_t>(position) >= input.size()) {
                                fail(Category::Type, expression.range, "list index out of range");
                            }
                            return make_const(hgraph::Value{input.at(static_cast<std::size_t>(position))}, expression.range);
                        }
                        backend(expression.range, "a compile-time index needs a tuple or list");
                    }
                case gir::ConstExprKind::Field:
                    {
                        const Slot target = eval_const_expr(expression.lhs, frame);
                        if (!target.is_const() || !target.value.view().is_bundle()) {
                            backend(expression.range, "compile-time field access needs a struct value");
                        }
                        const auto field = target.value.view().as_bundle().field(expression.member);
                        return field.valid() ? make_const(hgraph::Value{field}, expression.range)
                                             : make_marker(Slot::Kind::Null, expression.range);
                    }
                case gir::ConstExprKind::Tuple:
                    {
                        std::vector<hgraph::Value>                     values;
                        std::vector<const hgraph::ValueTypeMetaData *> metadata;
                        for (gir::ConstExprId item : expression.items) {
                            Slot slot = eval_const_expr(item, frame);
                            if (!slot.is_const()) { backend(expression.range, "a tuple element is a constant"); }
                            metadata.push_back(slot.meta());
                            values.push_back(std::move(slot.value));
                        }
                        const auto   *meta = registry_.tuple(metadata);
                        hgraph::Value result{hgraph::ValuePlanFactory::instance().type_for(meta)};
                        auto          output = result.as_tuple().begin_mutation();
                        for (std::size_t index = 0; index < values.size(); ++index) {
                            output.at(index).copy_from(values[index].view());
                        }
                        return make_const(std::move(result), expression.range);
                    }
                case gir::ConstExprKind::Sequence:
                    {
                        std::vector<hgraph::Value> values;
                        for (const gir::ConstElement &element : expression.elements) {
                            if (element.key.valid()) { backend(expression.range, "a keyed compile-time sequence is unsupported"); }
                            Slot item = eval_const_expr(element.value, frame);
                            if (!item.is_const()) { backend(expression.range, "a list element is a constant"); }
                            values.push_back(std::move(item.value));
                        }
                        if (values.empty()) { fail(Category::Type, expression.range, "an empty list has no element type"); }
                        const auto         *meta = registry_.list(values.front().schema());
                        hgraph::ListBuilder output{hgraph::ValuePlanFactory::instance().type_for(meta->element_type), *meta};
                        for (const hgraph::Value &item : values) {
                            output.push_back(convert(item, meta->element_type, expression.range, "a list element").view());
                        }
                        return make_const(output.build(), expression.range);
                    }
                case gir::ConstExprKind::Construct:
                    {
                        std::vector<std::pair<std::string, Slot>> supplied;
                        supplied.reserve(expression.arguments.size());
                        for (const gir::ConstArgument &argument : expression.arguments) {
                            supplied.emplace_back(argument.name, eval_const_expr(argument.value, frame));
                        }
                        return assemble_construct(expression.constructed_type, std::move(supplied), expression.delta,
                                                  expression.range, frame);
                    }
                case gir::ConstExprKind::Literal: break;
            }
            backend(expression.range, "constant expression was not folded before hgraph IR lowering");
        }

        void Compiler::resolve_sequence(Slot &slot, const hgraph::ValueTypeMetaData *element, Frame &frame) {
            if (slot.resolved) { return; }
            const gir::Value &literal = value(slot.expression);
            const auto       *source  = std::get_if<gir::Sequence>(&literal.node);
            if (source == nullptr) { backend(literal.range, "invalid harness sequence"); }
            for (const gir::SequenceElement &entry : source->elements) {
                if (entry.key.valid()) {
                    fail(Category::Test, value(entry.key).range,
                         "timed sequences are not supported by the first pass; write one value per cycle");
                }
                Slot item = eval_value(entry.value, frame);
                if (item.kind == Slot::Kind::Placeholder) {
                    slot.elements.emplace_back(std::nullopt);
                    continue;
                }
                if (!item.is_const()) {
                    fail(Category::Type, value(entry.value).range, "a harness sequence element is a constant");
                }
                slot.elements.emplace_back(convert(item.value, element, item.range, "the sequence element"));
            }
            slot.element_meta = element;
            slot.resolved     = true;
        }

        Slot Compiler::constant_of(const Slot &slot, const hgraph::ValueTypeMetaData *meta, Frame &frame, std::string_view role) {
            if (slot.kind == Slot::Kind::Sequence && meta->try_value_kind() == hgraph::ValueTypeKind::List) {
                Slot sequence = slot;
                resolve_sequence(sequence, meta->element_type, frame);
                hgraph::ListBuilder output{hgraph::ValuePlanFactory::instance().type_for(meta->element_type), *meta};
                for (const auto &item : sequence.elements) {
                    if (!item) { fail(Category::Type, slot.range, std::string{role} + " is a list; '_' is not a list element"); }
                    output.push_back(item->view());
                }
                return make_const(output.build(), slot.range);
            }
            if (!slot.is_const()) { fail(Category::Type, slot.range, std::string{role} + " is const; a constant is required"); }
            return make_const(convert(slot.value, meta, slot.range, role), slot.range);
        }

        Slot Compiler::compare_sequences(const Slot &lhs, const Slot &rhs, bool negate, SourceRange range, Frame &frame) {
            Slot left  = lhs;
            Slot right = rhs;
            if (!left.resolved && !right.resolved) {
                backend(range, "comparing two literal sequences is not supported; one side comes from eval");
            }
            if (!left.resolved) { resolve_sequence(left, right.element_meta, frame); }
            if (!right.resolved) { resolve_sequence(right, left.element_meta, frame); }
            bool equal = left.elements.size() == right.elements.size();
            comparison_detail_.clear();
            if (!equal) {
                comparison_detail_ = "expected " + std::to_string(right.elements.size()) + " cycles, observed " +
                                     std::to_string(left.elements.size()) + ": " + describe_sequence(left.elements);
            }
            for (std::size_t index = 0; equal && index < left.elements.size(); ++index) {
                const auto &observed = left.elements[index];
                const auto &expected = right.elements[index];
                const bool  same =
                    observed.has_value() == expected.has_value() && (!observed || observed->view().equals(expected->view()));
                if (!same) {
                    equal              = false;
                    comparison_detail_ = "cycle " + std::to_string(index) + ": expected " +
                                         (expected ? describe_value(*expected) : "_") + ", observed " +
                                         (observed ? describe_value(*observed) : "_") + " in " + describe_sequence(left.elements);
                }
            }
            return make_const(hgraph::Value{hgraph::Bool{negate ? !equal : equal}}, range);
        }

        Slot Compiler::wire(std::string_view name, std::vector<hgraph::WiringArg> arguments, SourceRange range,
                            std::optional<bool> output_required, const hgraph::TSValueTypeMetaData *expected) {
            try {
                hgraph::OperatorWireResult result = hgraph::wire_operator(
                    wiring(range), name, std::span<const hgraph::WiringArg>{arguments}, output_required, expected);
                return result.has_output ? make_port(result.output.erased(), range) : Slot{};
            } catch (const hgraph::OperatorResolutionError &error) {
                fail(Category::Operator, range, std::string{name} + ": " + error.what());
            } catch (const std::exception &error) { backend(range, std::string{name} + ": " + error.what()); }
        }

        hgraph::WiringArg Compiler::argument_of(const Slot &slot, std::string name) {
            if (slot.is_const()) { return scalar_arg(slot.value, std::move(name)); }
            if (slot.is_port()) { return time_series_arg(slot.port, std::move(name)); }
            if (slot.kind == Slot::Kind::Null) { backend(slot.range, "null needs an optional field context"); }
            if (slot.kind == Slot::Kind::Delta) { backend(slot.range, "a structured delta is not an ordinary operator value"); }
            if (slot.kind == Slot::Kind::Sequence) { backend(slot.range, "a harness sequence is only valid in eval"); }
            if (slot.kind == Slot::Kind::Function || slot.kind == Slot::Kind::Operator || slot.kind == Slot::Kind::Intrinsic ||
                slot.kind == Slot::Kind::Struct) {
                backend(slot.range, "passing a callable to an operator is not supported by the first pass");
            }
            backend(slot.range, "this expression produces no value");
        }

        Slot Compiler::wire_constant(const Slot &slot, const hgraph::TSValueTypeMetaData *target) {
            return wire("const", {scalar_arg(slot.value, "value")}, slot.range, true, target);
        }

        Slot Compiler::wire_binary(const gir::Value &expression, hir::BinaryOp op, const Slot &lhs, const Slot &rhs) {
            std::string name = expression.operation.registry_name;
            if (name.empty()) {
                switch (op) {
                    case hir::BinaryOp::Add: name = "add_"; break;
                    case hir::BinaryOp::Sub: name = "sub_"; break;
                    case hir::BinaryOp::Mul: name = "mul_"; break;
                    case hir::BinaryOp::Div: name = "div_"; break;
                    case hir::BinaryOp::Rem: name = "mod_"; break;
                    case hir::BinaryOp::Equal: name = "eq_"; break;
                    case hir::BinaryOp::NotEqual: name = "ne_"; break;
                    case hir::BinaryOp::Less: name = "lt_"; break;
                    case hir::BinaryOp::LessEqual: name = "le_"; break;
                    case hir::BinaryOp::Greater: name = "gt_"; break;
                    case hir::BinaryOp::GreaterEqual: name = "ge_"; break;
                    case hir::BinaryOp::And: name = "and_"; break;
                    case hir::BinaryOp::Or: name = "or_"; break;
                }
            }
            return wire(name, {argument_of(lhs, {}), argument_of(rhs, {})}, expression.range);
        }

        Slot Compiler::eval_reference(const gir::Reference &reference, SourceRange range, Frame &frame) {
            Slot result;
            result.range = range;
            switch (reference.kind) {
                case gir::ReferenceKind::Binding:
                    {
                        const auto found = frame.bindings.find(reference.binding.value);
                        if (!reference.binding.valid() || found == frame.bindings.end()) {
                            const std::string name = reference.binding.valid() ? binding(reference.binding).name : "value";
                            backend(range, "'" + name + "' is not bound in this activation");
                        }
                        result       = found->second;
                        result.range = range;
                        return result;
                    }
                case gir::ReferenceKind::Callable:
                    result.kind     = Slot::Kind::Function;
                    result.callable = reference.callable;
                    result.name     = reference.identity;
                    return result;
                case gir::ReferenceKind::Operator:
                    result.kind = Slot::Kind::Operator;
                    result.name = reference.registry_name.empty() ? reference.identity : reference.registry_name;
                    return result;
                case gir::ReferenceKind::Struct:
                    result.kind = Slot::Kind::Struct;
                    result.name = reference.identity;
                    return result;
                case gir::ReferenceKind::Intrinsic:
                    result.kind = Slot::Kind::Intrinsic;
                    result.name = reference.registry_name.empty() ? reference.identity : reference.registry_name;
                    return result;
            }
            backend(range, "invalid hgraph IR reference");
        }

        Slot Compiler::eval_tuple(const gir::Tuple &tuple, SourceRange range, Frame &frame) {
            std::vector<hgraph::Value>                     values;
            std::vector<const hgraph::ValueTypeMetaData *> metadata;
            for (gir::ValueId element : tuple.elements) {
                Slot item = eval_value(element, frame);
                if (item.kind == Slot::Kind::Placeholder) {
                    backend(item.range, "'_' inside a tuple is not supported by the first pass (whole-value ticks only)");
                }
                if (!item.is_const()) { backend(item.range, "a tuple of time-series values is not supported by the first pass"); }
                metadata.push_back(item.meta());
                values.push_back(std::move(item.value));
            }
            const auto   *meta = registry_.tuple(metadata);
            hgraph::Value result{hgraph::ValuePlanFactory::instance().type_for(meta)};
            auto          output = result.as_tuple().begin_mutation();
            for (std::size_t index = 0; index < values.size(); ++index) { output.at(index).copy_from(values[index].view()); }
            return make_const(std::move(result), range);
        }

        Slot Compiler::eval_sequence(gir::ValueId id, const gir::Sequence &sequence, SourceRange range, Frame &frame) {
            if (frame.in_test) {
                Slot result;
                result.kind       = Slot::Kind::Sequence;
                result.expression = id;
                result.range      = range;
                return result;
            }
            std::vector<hgraph::Value> values;
            for (const gir::SequenceElement &element : sequence.elements) {
                if (element.key.valid()) {
                    fail(Category::Type, value(element.key).range, "a keyed sequence is only valid in a test");
                }
                Slot item = eval_value(element.value, frame);
                if (!item.is_const()) { backend(item.range, "a list of time-series values is not supported by the first pass"); }
                values.push_back(std::move(item.value));
            }
            if (values.empty()) { fail(Category::Type, range, "an empty list has no element type"); }
            const auto *meta = value_meta(value(id).type);
            if (meta->try_value_kind() != hgraph::ValueTypeKind::List) {
                backend(range, "a list expression has non-list metadata");
            }
            hgraph::ListBuilder output{hgraph::ValuePlanFactory::instance().type_for(meta->element_type), *meta};
            for (const hgraph::Value &item : values) {
                output.push_back(convert(item, meta->element_type, range, "a list element").view());
            }
            return make_const(output.build(), range);
        }

        Slot Compiler::assemble_construct(gir::TypeId type, std::vector<std::pair<std::string, Slot>> supplied_values, bool delta,
                                          SourceRange range, Frame &frame) {
            const gir::StructContract &contract = structure(type);
            if (contract.abstract) {
                fail(Category::Type, range, "abstract struct '" + local_name(contract.identity) + "' is not constructible");
            }
            const hgraph::ValueTypeMetaData *meta = value_meta(type);
            if (meta->try_value_kind() != hgraph::ValueTypeKind::Bundle || meta->field_count != contract.fields.size()) {
                backend(range, "struct metadata does not match '" + contract.identity + "'");
            }
            std::vector<std::optional<Slot>> supplied(contract.fields.size());
            for (auto &[name, slot] : supplied_values) {
                if (name.empty()) { fail(Category::Type, slot.range, "struct construction uses named arguments"); }
                const auto found = std::find_if(contract.fields.begin(), contract.fields.end(),
                                                [&](const gir::StructField &field) { return field.name == name; });
                if (found == contract.fields.end()) {
                    fail(Category::Name, slot.range,
                         "struct '" + local_name(contract.identity) + "' has no field named '" + name + "'");
                }
                const std::size_t index = static_cast<std::size_t>(found - contract.fields.begin());
                if (supplied[index]) { fail(Category::Name, slot.range, "field '" + name + "' is given twice"); }
                supplied[index] = std::move(slot);
            }

            std::vector<std::optional<Slot>> effective = supplied;
            for (std::size_t index = 0; index < contract.fields.size(); ++index) {
                const gir::StructField &field = contract.fields[index];
                if (!effective[index] && !delta && field.default_value.valid()) {
                    effective[index] = eval_const_expr(field.default_value, frame);
                }
                if (!effective[index]) {
                    if (delta || field.optional) { continue; }
                    fail(Category::Type, range, "struct '" + local_name(contract.identity) + "' needs field '" + field.name + "'");
                }
                Slot &item = *effective[index];
                if (item.kind == Slot::Kind::Null) {
                    if (!field.optional) { fail(Category::Type, item.range, "required field '" + field.name + "' cannot be null"); }
                    if (delta) {
                        backend(item.range,
                                "clearing an optional struct field needs the distinct public hgraph clear-delta operation");
                    }
                    continue;
                }
                if (!item.is_const() && !item.is_port() && item.kind != Slot::Kind::Delta) {
                    fail(Category::Type, item.range, "field '" + field.name + "' needs a value");
                }
                if (item.kind == Slot::Kind::Delta && !delta) {
                    fail(Category::Type, item.range, "a sparse delta cannot initialise complete field '" + field.name + "'");
                }
            }

            const bool temporal =
                std::any_of(effective.begin(), effective.end(), [](const auto &item) { return item && item->is_port(); });
            if (temporal) {
                if (delta) { backend(range, "a temporal structured delta is only available in a runtime function"); }
                const hgraph::TSValueTypeMetaData *target = schema(type);
                if (target->kind != hgraph::TSTypeKind::TSB || target->field_count() != contract.fields.size()) {
                    backend(range, "temporal struct metadata does not match '" + contract.identity + "'");
                }
                std::vector<hgraph::WiringPortRef> children;
                for (std::size_t index = 0; index < contract.fields.size(); ++index) {
                    const auto *field_schema = target->fields()[index].type;
                    if (!effective[index] || effective[index]->kind == Slot::Kind::Null) {
                        children.push_back(hgraph::WiringPortRef::null_source(field_schema));
                    } else if (effective[index]->is_const()) {
                        Slot converted = make_const(convert(effective[index]->value, meta->fields[index].type,
                                                            effective[index]->range, "field '" + contract.fields[index].name + "'"),
                                                    effective[index]->range);
                        children.push_back(wire_constant(converted, field_schema).port);
                    } else if (effective[index]->is_port() && effective[index]->port.schema == field_schema) {
                        children.push_back(effective[index]->port);
                    } else {
                        fail(Category::Type, effective[index]->range,
                             "field '" + contract.fields[index].name + "' expects " + std::string{field_schema->name()});
                    }
                }
                return make_port(hgraph::WiringPortRef::structural_source(target, std::move(children)), range);
            }

            hgraph::BundleBuilder output{hgraph::ValuePlanFactory::instance().type_for(meta)};
            for (std::size_t index = 0; index < contract.fields.size(); ++index) {
                if (!effective[index] || effective[index]->kind == Slot::Kind::Null) { continue; }
                if (!effective[index]->is_const() && !(delta && effective[index]->kind == Slot::Kind::Delta)) {
                    fail(Category::Type, effective[index]->range,
                         "field '" + contract.fields[index].name + "' needs a scalar value");
                }
                output.set(index, convert(effective[index]->value, meta->fields[index].type, effective[index]->range,
                                          "field '" + contract.fields[index].name + "'")
                                      .view());
            }
            Slot result = make_const(output.build(), range);
            if (delta) { result.kind = Slot::Kind::Delta; }
            return result;
        }

        Slot Compiler::eval_construct(gir::TypeId type, const std::vector<gir::Argument> &arguments, bool delta, SourceRange range,
                                      Frame &frame) {
            std::vector<std::pair<std::string, Slot>> supplied;
            supplied.reserve(arguments.size());
            for (const gir::Argument &argument : arguments) {
                supplied.emplace_back(argument.name, eval_value(argument.value, frame));
            }
            return assemble_construct(type, std::move(supplied), delta, range, frame);
        }

        std::vector<std::optional<gir::ValueId>>
        Compiler::bind_arguments(const gir::Callable &target, const std::vector<gir::Argument> &arguments, SourceRange range) {
            std::vector<std::optional<gir::ValueId>> result(target.parameters.size());
            std::size_t                              next = 0;
            for (const gir::Argument &argument : arguments) {
                if (argument.name.empty()) {
                    if (next >= result.size()) {
                        fail(Category::Type, argument.range,
                             "'" + local_name(target.identity) + "' takes " + std::to_string(result.size()) + " arguments");
                    }
                    if (result[next]) { fail(Category::Type, argument.range, "positional argument after a named one"); }
                    result[next++] = argument.value;
                    continue;
                }
                const auto found = std::find_if(target.parameters.begin(), target.parameters.end(),
                                                [&](const gir::Parameter &parameter) { return parameter.name == argument.name; });
                if (found == target.parameters.end()) {
                    fail(Category::Name, argument.range,
                         "'" + local_name(target.identity) + "' has no parameter named '" + argument.name + "'");
                }
                const std::size_t index = static_cast<std::size_t>(found - target.parameters.begin());
                if (result[index]) { fail(Category::Name, argument.range, "'" + argument.name + "' is given twice"); }
                result[index] = argument.value;
                next          = std::max(next, index + 1U);
            }
            for (std::size_t index = 0; index < target.parameters.size(); ++index) {
                if (!result[index] && !target.parameters[index].default_value.valid()) {
                    fail(Category::Type, range,
                         "'" + local_name(target.identity) + "' needs an argument for '" + target.parameters[index].name + "'");
                }
            }
            return result;
        }

        Slot Compiler::bind_parameter(const gir::Parameter &parameter, const Slot &argument, Frame &frame, SourceRange range) {
            if (parameter.is_const) {
                Slot result  = constant_of(argument, value_meta(parameter.type), frame, "parameter '" + parameter.name + "'");
                result.range = range;
                return result;
            }
            const auto *target = schema(parameter.type);
            if (argument.is_const()) {
                return wire_constant(
                    make_const(convert(argument.value, target->value_schema, argument.range, "parameter '" + parameter.name + "'"),
                               argument.range),
                    target);
            }
            if (!argument.is_port()) {
                fail(Category::Type, argument.range, "parameter '" + parameter.name + "' takes a time-series value");
            }
            if (argument.port.schema != target) {
                fail(Category::Type, argument.range,
                     "parameter '" + parameter.name + "' expects " + std::string{target->name()} + ", got " +
                         std::string{argument.port.schema == nullptr ? "an untyped port" : argument.port.schema->name()});
            }
            return make_port(argument.port, range);
        }

        Slot Compiler::wire_function(gir::CallableId id, Frame &frame, SourceRange range) {
            const gir::Callable           &target = callable(id);
            std::vector<hgraph::WiringArg> arguments;
            for (const gir::Parameter &parameter : target.parameters) {
                arguments.push_back(argument_of(frame.bindings.at(parameter.binding.value), parameter.name));
            }
            return wire(target.identity, std::move(arguments), range);
        }

        Slot Compiler::invoke(gir::CallableId id, Frame &frame) {
            const gir::Callable &target = callable(id);
            if (target.concise_body.valid()) { return eval_value(target.concise_body, frame); }
            Slot result = exec_block(target.block_body, frame);
            return frame.returned ? *frame.returned : result;
        }

        Slot Compiler::call_function(gir::CallableId id, const std::vector<gir::Argument> &arguments, SourceRange range,
                                     Frame &caller) {
            const gir::Callable &target = callable(id);
            if (target.visibility == gir::CallableVisibility::Implementation) {
                backend(range, "an impl fn is reached through its operator, not called directly");
            }
            if (!target.generics.empty()) { backend(range, "generic functions are not supported by the first pass"); }
            const auto bound = bind_arguments(target, arguments, range);
            Frame      callee;
            callee.callable = id;
            for (std::size_t pass = 0; pass < 2; ++pass) {
                for (std::size_t index = 0; index < target.parameters.size(); ++index) {
                    const gir::Parameter &parameter = target.parameters[index];
                    if (parameter.is_const != (pass == 0)) { continue; }
                    Slot argument =
                        bound[index] ? eval_value(*bound[index], caller) : eval_const_expr(parameter.default_value, callee);
                    callee.bindings[parameter.binding.value] = bind_parameter(parameter, argument, callee, argument.range);
                }
            }
            Slot result  = target.kind == gir::CallableKind::RuntimeNode ? wire_function(id, callee, range) : invoke(id, callee);
            result.range = range;
            return result;
        }

        Slot Compiler::eval_intrinsic(std::string_view name, const std::vector<gir::Argument> &arguments, SourceRange range,
                                      Frame &frame) {
            if (name == "valid" || name == "modified" || name == "all_valid") {
                if (arguments.empty()) {
                    fail(Category::Type, range, "'" + std::string{name} + "' takes at least one time-series argument");
                }
                const char         *operation = name == "modified" ? "modified" : "valid";
                const char         *fold      = name == "modified" ? "or_" : "and_";
                std::optional<Slot> result;
                for (const gir::Argument &argument : arguments) {
                    Slot item = eval_value(argument.value, frame);
                    if (!item.is_port()) {
                        fail(Category::Type, item.range, "'" + std::string{name} + "' takes time-series arguments");
                    }
                    Slot flag = wire(operation, {time_series_arg(item.port)}, range);
                    result    = result ? wire(fold, {time_series_arg(result->port), time_series_arg(flag.port)}, range) : flag;
                }
                return *result;
            }
            if (name == "last_modified" || name == "last_modified_time" || name == "key_set") {
                if (arguments.size() != 1U) {
                    fail(Category::Type, range, "'" + std::string{name} + "' takes one time-series argument");
                }
                Slot item = eval_value(arguments.front().value, frame);
                if (!item.is_port()) {
                    fail(Category::Type, item.range, "'" + std::string{name} + "' takes a time-series argument");
                }
                return wire(name == "key_set" ? "keys_" : "last_modified_time", {time_series_arg(item.port)}, range);
            }
            backend(range, "'" + std::string{name} +
                               "' is a runtime traversal; it is not available in a composition body of the first pass");
        }

        Slot Compiler::eval_call(const gir::Value &expression, const gir::Call &call, Frame &frame) {
            if (expression.operation.kind == gir::OperationKind::Constructor) {
                return eval_construct(expression.type, call.arguments, false, expression.range, frame);
            }
            Slot callee = eval_value(call.callee, frame);
            switch (callee.kind) {
                case Slot::Kind::Function: return call_function(callee.callable, call.arguments, expression.range, frame);
                case Slot::Kind::Operator:
                    {
                        std::string name =
                            expression.operation.registry_name.empty() ? callee.name : expression.operation.registry_name;
                        std::vector<hgraph::WiringArg> arguments;
                        for (const gir::Argument &argument : call.arguments) {
                            arguments.push_back(argument_of(eval_value(argument.value, frame), argument.name));
                        }
                        return wire(name, std::move(arguments), expression.range);
                    }
                case Slot::Kind::Intrinsic: return eval_intrinsic(callee.name, call.arguments, expression.range, frame);
                case Slot::Kind::Struct: return eval_construct(expression.type, call.arguments, false, expression.range, frame);
                default: break;
            }
            fail(Category::Type, value(call.callee).range, "'" + slice(value(call.callee).range) + "' is not callable");
        }

        Slot Compiler::eval_value(gir::ValueId id, Frame &frame) {
            const gir::Value &expression = value(id);
            if (expression.constant) { return constant(*expression.constant, expression.range); }
            return std::visit(
                [&](const auto &node) -> Slot {
                    using T = std::decay_t<decltype(node)>;
                    if constexpr (std::is_same_v<T, gir::Literal>) {
                        return constant(node.value, expression.range);
                    } else if constexpr (std::is_same_v<T, gir::Reference>) {
                        return eval_reference(node, expression.range, frame);
                    } else if constexpr (std::is_same_v<T, gir::Unary>) {
                        Slot operand = eval_value(node.operand, frame);
                        if (operand.is_const()) { return fold_unary(node.op, operand, expression.range); }
                        const std::string name = expression.operation.registry_name.empty()
                                                     ? (node.op == hir::UnaryOp::Negate ? "neg_" : "not_")
                                                     : expression.operation.registry_name;
                        return wire(name, {argument_of(operand, {})}, expression.range);
                    } else if constexpr (std::is_same_v<T, gir::Binary>) {
                        Slot lhs = eval_value(node.lhs, frame);
                        Slot rhs = eval_value(node.rhs, frame);
                        if (lhs.kind == Slot::Kind::Sequence || rhs.kind == Slot::Kind::Sequence) {
                            if ((node.op != hir::BinaryOp::Equal && node.op != hir::BinaryOp::NotEqual) || lhs.kind != rhs.kind) {
                                fail(Category::Type, expression.range, "a harness sequence only compares with '==' or '!='");
                            }
                            return compare_sequences(lhs, rhs, node.op == hir::BinaryOp::NotEqual, expression.range, frame);
                        }
                        if (lhs.is_const() && rhs.is_const()) { return fold_binary(node.op, lhs, rhs, expression.range); }
                        return wire_binary(expression, node.op, lhs, rhs);
                    } else if constexpr (std::is_same_v<T, gir::Call>) {
                        return eval_call(expression, node, frame);
                    } else if constexpr (std::is_same_v<T, gir::Index>) {
                        Slot target = eval_value(node.target, frame);
                        Slot index  = eval_value(node.index, frame);
                        if (target.is_port()) {
                            const std::string name =
                                expression.operation.registry_name.empty() ? "getitem_" : expression.operation.registry_name;
                            return wire(name, {argument_of(target, {}), argument_of(index, {})}, expression.range);
                        }
                        if (target.is_const() && index.is_const() && index.meta() == types_.int_type) {
                            const auto position = index.value.view().checked_as<hgraph::Int>();
                            if (position < 0) { fail(Category::Type, expression.range, "index out of range"); }
                            if (target.value.view().is_tuple()) {
                                const auto input = target.value.view().as_tuple();
                                if (static_cast<std::size_t>(position) >= input.size()) {
                                    fail(Category::Type, expression.range, "tuple index out of range");
                                }
                                return make_const(hgraph::Value{input.at(static_cast<std::size_t>(position))}, expression.range);
                            }
                            if (target.value.view().is_list()) {
                                const auto input = target.value.view().as_list();
                                if (static_cast<std::size_t>(position) >= input.size()) {
                                    fail(Category::Type, expression.range, "list index out of range");
                                }
                                return make_const(hgraph::Value{input.at(static_cast<std::size_t>(position))}, expression.range);
                            }
                        }
                        backend(expression.range, "indexing is supported on time-series values and constant tuples and lists");
                    } else if constexpr (std::is_same_v<T, gir::Field>) {
                        Slot target = eval_value(node.target, frame);
                        if (target.is_port()) {
                            const std::string name =
                                expression.operation.registry_name.empty() ? "getattr_" : expression.operation.registry_name;
                            return wire(name, {argument_of(target, {}), scalar_arg(hgraph::Value{node.name})}, expression.range);
                        }
                        if ((target.is_const() || target.kind == Slot::Kind::Delta) && target.value.view().is_bundle()) {
                            const auto field = target.value.view().as_bundle().field(node.name);
                            return field.valid() ? make_const(hgraph::Value{field}, expression.range)
                                                 : make_marker(Slot::Kind::Null, expression.range);
                        }
                        backend(expression.range, "field access needs a temporal or structured value");
                    } else if constexpr (std::is_same_v<T, gir::Sequence>) {
                        return eval_sequence(id, node, expression.range, frame);
                    } else if constexpr (std::is_same_v<T, gir::Tuple>) {
                        return eval_tuple(node, expression.range, frame);
                    } else if constexpr (std::is_same_v<T, gir::Lambda>) {
                        backend(expression.range, "anonymous functions are not supported by the first pass");
                    } else if constexpr (std::is_same_v<T, gir::Conditional>) {
                        Slot condition = eval_value(node.condition, frame);
                        if (condition.is_port()) {
                            backend(value(node.condition).range,
                                    "'if' over a time-series condition is not supported by the first pass; use if_then_else");
                        }
                        if (!condition.is_const() || condition.meta() != types_.bool_type) {
                            fail(Category::Type, value(node.condition).range, "an 'if' condition is a bool");
                        }
                        if (condition.value.view().checked_as<hgraph::Bool>()) { return exec_block(node.then_block, frame); }
                        return node.otherwise.valid() ? eval_value(node.otherwise, frame) : Slot{};
                    } else if constexpr (std::is_same_v<T, gir::BlockValue>) {
                        return exec_block(node.block, frame);
                    } else if constexpr (std::is_same_v<T, gir::HarnessEval>) {
                        return eval_harness(node, expression.range, frame);
                    } else if constexpr (std::is_same_v<T, gir::Construct>) {
                        return eval_construct(node.type, node.arguments, node.delta, expression.range, frame);
                    }
                },
                expression.node);
        }

        Slot Compiler::exec_block(gir::BlockId id, Frame &frame) {
            if (!id.valid() || id.value >= module_.blocks.size()) { backend({}, "invalid hgraph IR block ID"); }
            const gir::Block &block = module_.blocks[id.value];
            for (gir::StatementId statement : block.statements) {
                if (frame.returned) { break; }
                exec_statement(statement, frame);
            }
            return !frame.returned && block.tail.valid() ? eval_value(block.tail, frame) : Slot{};
        }

        void Compiler::exec_statement(gir::StatementId id, Frame &frame) {
            if (!id.valid() || id.value >= module_.statements.size()) { backend({}, "invalid hgraph IR statement ID"); }
            const gir::Statement &statement = module_.statements[id.value];
            std::visit(
                [&](const auto &node) {
                    using T = std::decay_t<decltype(node)>;
                    if constexpr (std::is_same_v<T, gir::LocalBinding>) {
                        Slot initial = eval_value(node.init, frame);
                        if (node.type.valid() && node.type.value < module_.types.size()) {
                            const hir::TypeKind kind = module_.types[node.type.value].kind;
                            if ((initial.is_const() || initial.kind == Slot::Kind::Sequence) &&
                                kind != hir::TypeKind::HarnessSequence && kind != hir::TypeKind::Deferred) {
                                initial =
                                    constant_of(initial, value_meta(node.type), frame, "'" + binding(node.binding).name + "'");
                            } else if (initial.is_port()) {
                                const auto *target = schema(node.type);
                                if (initial.port.schema != target) {
                                    fail(Category::Type, initial.range,
                                         "'" + binding(node.binding).name + "' is declared " + std::string{target->name()} +
                                             " but the initialiser is " + std::string{initial.port.schema->name()});
                                }
                            }
                        }
                        frame.bindings[node.binding.value] = std::move(initial);
                    } else if constexpr (std::is_same_v<T, gir::Assignment>) {
                        const gir::Value &place     = value(node.place);
                        const auto       *reference = std::get_if<gir::Reference>(&place.node);
                        if (reference == nullptr || reference->kind != gir::ReferenceKind::Binding) {
                            backend(place.range, "assignment targets a local in the first pass");
                        }
                        const gir::Binding &target = binding(reference->binding);
                        if (target.kind != gir::BindingKind::LocalVar) {
                            fail(Category::Type, place.range, "'" + target.name + "' is not a 'var'");
                        }
                        const Slot current = frame.bindings.at(reference->binding.value);
                        Slot       next    = eval_value(node.value, frame);
                        if (node.op != gir::AssignOp::Assign) {
                            const hir::BinaryOp op = node.op == gir::AssignOp::Add   ? hir::BinaryOp::Add
                                                     : node.op == gir::AssignOp::Sub ? hir::BinaryOp::Sub
                                                     : node.op == gir::AssignOp::Mul ? hir::BinaryOp::Mul
                                                                                     : hir::BinaryOp::Div;
                            next = current.is_const() && next.is_const() ? fold_binary(op, current, next, statement.range)
                                                                         : wire_binary(value(node.value), op, current, next);
                        }
                        if (current.kind != next.kind) {
                            fail(Category::Type, statement.range, "assignment to '" + target.name + "' changes its inferred type");
                        }
                        if (current.is_const()) {
                            next = make_const(
                                convert(next.value, current.meta(), next.range, "assignment to '" + target.name + "'"), next.range);
                        } else if (current.is_port() && current.port.schema != next.port.schema) {
                            fail(Category::Type, next.range,
                                 "assignment to '" + target.name + "' expects " + std::string{current.port.schema->name()} +
                                     ", got " + std::string{next.port.schema->name()});
                        }
                        frame.bindings[reference->binding.value] = std::move(next);
                    } else if constexpr (std::is_same_v<T, gir::Return>) {
                        frame.returned = node.value.valid() ? eval_value(node.value, frame) : Slot{};
                    } else if constexpr (std::is_same_v<T, gir::Assert>) {
                        Slot condition = eval_value(node.condition, frame);
                        if (!condition.is_const() || condition.meta() != types_.bool_type) {
                            fail(Category::Type, value(node.condition).range, "'assert' takes a bool");
                        }
                        if (!condition.value.view().checked_as<hgraph::Bool>()) {
                            std::string message = "assert failed: " + slice(value(node.condition).range);
                            if (!comparison_detail_.empty()) { message += "\n" + comparison_detail_; }
                            throw TestFailure{std::move(message)};
                        }
                    } else if constexpr (std::is_same_v<T, gir::Evaluate>) {
                        (void)eval_value(node.value, frame);
                    } else {
                        backend(statement.range, "runtime statements are not evaluated by the first pass");
                    }
                },
                statement.node);
        }

        Slot Compiler::eval_harness(const gir::HarnessEval &eval, SourceRange range, Frame &caller) {
            Slot callee_slot = eval_value(eval.callee, caller);
            if (callee_slot.kind == Slot::Kind::Operator || callee_slot.kind == Slot::Kind::Intrinsic) {
                backend(value(eval.callee).range, "eval takes a module function; wrap the operator in a fn to evaluate it");
            }
            if (callee_slot.kind != Slot::Kind::Function) {
                fail(Category::Type, value(eval.callee).range, "eval takes a function");
            }
            const gir::Callable &target = callable(callee_slot.callable);
            if (!target.generics.empty()) { backend(range, "generic functions are not supported by the first pass"); }
            const auto bound = bind_arguments(target, eval.arguments, range);

            hgraph::Wiring local_wiring;
            hgraph::record_replay::set_config(
                local_wiring.global_state(),
                hgraph::record_replay::RecordReplayConfig{.backend = std::string{hgraph::record_replay::TESTING}});
            hgraph::Wiring *previous = wiring_;
            wiring_                  = &local_wiring;
            auto restore_wiring      = hgraph::make_scope_exit([&]() noexcept { wiring_ = previous; });

            Frame callee;
            callee.callable = callee_slot.callable;
            std::vector<std::vector<std::optional<hgraph::Value>>> inputs;
            std::vector<std::string>                               keys;
            std::size_t                                            cycles = 0;
            for (std::size_t pass = 0; pass < 2; ++pass) {
                for (std::size_t index = 0; index < target.parameters.size(); ++index) {
                    const gir::Parameter &parameter = target.parameters[index];
                    if (parameter.is_const != (pass == 0)) { continue; }
                    if (parameter.is_const) {
                        Slot argument =
                            bound[index] ? eval_value(*bound[index], caller) : eval_const_expr(parameter.default_value, callee);
                        callee.bindings[parameter.binding.value] = bind_parameter(parameter, argument, callee, argument.range);
                        continue;
                    }
                    const auto *parameter_schema = schema(parameter.type);
                    if (parameter_schema->kind != hgraph::TSTypeKind::TS) {
                        backend(binding(parameter.binding).range, "eval drives ts parameters in the first pass; '" +
                                                                      parameter.name + "' is " +
                                                                      std::string{parameter_schema->name()});
                    }
                    const std::string key = "hgl::in::" + parameter.name;
                    if (bound[index]) {
                        Slot sequence = eval_value(*bound[index], caller);
                        if (sequence.kind != Slot::Kind::Sequence) {
                            fail(Category::Type, sequence.range, "eval drives '" + parameter.name + "' with a harness sequence");
                        }
                        resolve_sequence(sequence, parameter_schema->value_schema, caller);
                        inputs.push_back(std::move(sequence.elements));
                    } else {
                        Slot item = eval_const_expr(parameter.default_value, callee);
                        if (!item.is_const()) { fail(Category::Type, item.range, "a temporal parameter's default is a constant"); }
                        inputs.push_back({convert(item.value, parameter_schema->value_schema, item.range,
                                                  "parameter '" + parameter.name + "'")});
                    }
                    cycles = std::max(cycles, inputs.back().size());
                    keys.push_back(key);
                    callee.bindings[parameter.binding.value] =
                        wire("replay", {scalar_arg(hgraph::Value{key}, "key")}, range, true, parameter_schema);
                }
            }

            Slot result = target.kind == gir::CallableKind::RuntimeNode ? wire_function(callee_slot.callable, callee, range)
                                                                        : invoke(callee_slot.callable, callee);
            if (result.is_const()) { result = wire_constant(result, registry_.ts(result.meta())); }
            const hgraph::TSValueTypeMetaData *output_schema = nullptr;
            if (result.is_port()) {
                output_schema = result.port.schema;
                (void)wire("record", {time_series_arg(result.port), scalar_arg(hgraph::Value{std::string{"hgl::out"}}, "key")},
                           range, false);
            } else if (result.kind != Slot::Kind::Void) {
                backend(range, "'" + local_name(target.identity) + "' does not produce a time-series value");
            }

            std::vector<std::optional<hgraph::Value>> observed;
            try {
                hgraph::GraphBuilder graph = std::move(local_wiring).finish();
                for (std::size_t index = 0; index < keys.size(); ++index) {
                    hgraph::testing::set_replay_deltas(graph.global_state(), keys[index], inputs[index]);
                }
                hgraph::GraphExecutorBuilder builder;
                builder.graph_builder(std::move(graph)).start_time(hgraph::MIN_ST).end_time(hgraph::MAX_ET);
                hgraph::GraphExecutorValue executor = builder.make_executor();
                auto                       view     = executor.view();
                view.run();
                if (output_schema != nullptr) {
                    observed = hgraph::testing::get_recorded_deltas(view.graph().global_state(), "hgl::out");
                }
            } catch (const std::exception &error) {
                throw TestFailure{"eval of '" + local_name(target.identity) + "' failed: " + error.what()};
            }
            while (observed.size() < cycles) { observed.emplace_back(std::nullopt); }
            Slot sequence;
            sequence.kind         = Slot::Kind::Sequence;
            sequence.resolved     = true;
            sequence.elements     = std::move(observed);
            sequence.element_meta = output_schema != nullptr ? output_schema->delta_value_schema : nullptr;
            sequence.range        = range;
            return sequence;
        }

        std::string Compiler::describe(const Slot &slot) {
            switch (slot.kind) {
                case Slot::Kind::Const: return describe_value(slot.value);
                case Slot::Kind::Null: return "null";
                case Slot::Kind::Placeholder: return "_";
                case Slot::Kind::Delta: return "delta " + describe_value(slot.value);
                case Slot::Kind::Port: return std::string{slot.port.schema->name()};
                case Slot::Kind::Struct: return "struct " + local_name(slot.name);
                case Slot::Kind::Function: return "fn " + local_name(callable(slot.callable).identity);
                case Slot::Kind::Operator: return "operator " + slot.name;
                case Slot::Kind::Intrinsic: return "intrinsic " + slot.name;
                case Slot::Kind::Sequence: return slot.resolved ? describe_sequence(slot.elements) : slice(slot.range);
                case Slot::Kind::Void: return {};
            }
            return {};
        }

        std::vector<TestResult> Compiler::run_tests(const TestOptions &options) {
            std::vector<TestResult> results;
            for (const gir::TestPlan &test : module_.tests) {
                const std::string name = local_name(test.identity);
                if (!options.names.empty() && std::find(options.names.begin(), options.names.end(), name) == options.names.end()) {
                    continue;
                }
                TestResult result;
                result.name = name;
                Frame frame;
                frame.in_test = true;
                comparison_detail_.clear();
                const std::size_t before = diagnostics_.size();
                try {
                    Slot tail     = exec_block(test.body, frame);
                    result.passed = true;
                    if (options.describe_tail && tail.kind != Slot::Kind::Void) { result.tail = describe(tail); }
                } catch (const TestFailure &failure) { result.message = failure.message; } catch (const Abort &) {
                    result.message = "diagnostics reported";
                } catch (const std::exception &error) { result.message = std::string{"error: "} + error.what(); }
                if (diagnostics_.size() != before) { result.passed = false; }
                results.push_back(std::move(result));
            }
            return results;
        }

        std::optional<hgraph::Value> Compiler::evaluate_constant(gir::ValueId id) {
            try {
                Frame frame;
                Slot  result = eval_value(id, frame);
                if (!result.is_const()) { fail(Category::Type, result.range, "a --set value is a constant expression"); }
                return result.value;
            } catch (const Abort &) { return std::nullopt; }
        }

        bool Compiler::run_program(const RunOptions &options, std::ostream &out) {
            std::vector<gir::CallableId> candidates;
            for (std::uint32_t index = 0; index < module_.callables.size(); ++index) {
                const gir::Callable &target = module_.callables[index];
                if (target.visibility != gir::CallableVisibility::Export) { continue; }
                if (!options.entry.empty()) {
                    if (local_name(target.identity) == options.entry) { candidates.push_back(gir::CallableId{index}); }
                    continue;
                }
                if (std::all_of(target.parameters.begin(), target.parameters.end(),
                                [](const gir::Parameter &parameter) { return parameter.is_const; })) {
                    candidates.push_back(gir::CallableId{index});
                }
            }
            const SourceRange whole{};
            try {
                if (candidates.empty()) {
                    backend(whole, options.entry.empty() ? "no entry: an entry is an export fn with only const parameters"
                                                         : "no export fn named '" + options.entry + "'");
                }
                if (candidates.size() > 1U) { backend(whole, "several entries; choose one with --entry"); }
                const gir::CallableId entry  = candidates.front();
                const gir::Callable  &target = callable(entry);
                for (const Setting &setting : options.settings) {
                    if (std::none_of(target.parameters.begin(), target.parameters.end(),
                                     [&](const gir::Parameter &parameter) { return parameter.name == setting.name; })) {
                        fail(Category::Name, whole,
                             "--set " + setting.name + ": '" + local_name(target.identity) + "' has no parameter named '" +
                                 setting.name + "'");
                    }
                }

                hgraph::Wiring graph_wiring;
                wiring_            = &graph_wiring;
                auto  clear_wiring = hgraph::make_scope_exit([&]() noexcept { wiring_ = nullptr; });
                Frame frame;
                frame.callable = entry;
                for (const gir::Parameter &parameter : target.parameters) {
                    if (!parameter.is_const) {
                        backend(binding(parameter.binding).range,
                                "entry parameter '" + parameter.name + "' is not const; nothing drives it");
                    }
                    const auto setting = std::find_if(options.settings.begin(), options.settings.end(),
                                                      [&](const Setting &item) { return item.name == parameter.name; });
                    Slot       value_slot;
                    if (setting != options.settings.end()) {
                        value_slot = make_const(setting->value, binding(parameter.binding).range);
                    } else if (parameter.default_value.valid()) {
                        value_slot = eval_const_expr(parameter.default_value, frame);
                    } else {
                        fail(Category::Type, binding(parameter.binding).range,
                             "'" + parameter.name + "' has no default; pass --set " + parameter.name + "=<value>");
                    }
                    frame.bindings[parameter.binding.value] =
                        bind_parameter(parameter, value_slot, frame, binding(parameter.binding).range);
                }
                Slot result = target.kind == gir::CallableKind::RuntimeNode ? wire_function(entry, frame, target.range)
                                                                            : invoke(entry, frame);
                if (result.is_const()) { result = wire_constant(result, registry_.ts(result.meta())); }
                if (!result.is_port()) {
                    backend(target.range, "'" + local_name(target.identity) + "' produces no time-series value to run");
                }
                (void)wire("hgl.print_tick", {time_series_arg(result.port)}, target.range, false);

                hgraph::GraphBuilder         graph = std::move(graph_wiring).finish();
                hgraph::GraphExecutorBuilder builder;
                builder.graph_builder(std::move(graph));
                const bool real_time = options.mode == RunMode::RealTime;
                builder.mode(real_time ? hgraph::GraphExecutorMode::RealTime : hgraph::GraphExecutorMode::Simulation);
                hgraph::DateTime start = hgraph::MIN_ST;
                if (options.start) {
                    start = *options.start;
                } else if (real_time) {
                    start = std::chrono::time_point_cast<std::chrono::microseconds>(hgraph::engine_clock::now());
                }
                builder.start_time(start);
                if (options.end) {
                    builder.end_time(*options.end);
                } else if (options.end_after) {
                    builder.end_time(start + *options.end_after);
                } else {
                    builder.end_time(hgraph::MAX_ET);
                }

                std::ostream *previous                  = sink_stream;
                sink_stream                             = &out;
                auto                       restore_sink = hgraph::make_scope_exit([&]() noexcept { sink_stream = previous; });
                hgraph::GraphExecutorValue executor     = builder.make_executor();
                executor.view().run();
                return true;
            } catch (const Abort &) { return false; } catch (const std::exception &error) {
                diagnostics_.report(Category::Backend, whole, std::string{"run failed: "} + error.what());
                return false;
            }
        }
    }  // namespace

    void ensure_session() {
        static const bool installed = [] {
            (void)standard_types();
            hgraph::stdlib::register_standard_operators();
#if defined(HGL_HAVE_ANALYTICS)
            hgraph::analytics::register_analytics_operators();
#endif
            auto &registry = hgraph::OperatorRegistry::instance();
            registry.register_installer("hgl.wiring", [] { hgraph::register_overload<print_tick, print_tick_impl>(); });
            registry.run_installers();
            return true;
        }();
        (void)installed;
    }

    bool has_operator(std::string_view name) {
        ensure_session();
        return !hgraph::OperatorRegistry::instance().overload_signatures(name).empty();
    }

    std::vector<TestResult> run_tests(const syntax::SourceFile &file, const gir::Module &module, const TestOptions &options,
                                      syntax::DiagnosticSink &diagnostics) {
        ensure_session();
        return Compiler{file, module, diagnostics}.run_tests(options);
    }

    bool run_program(const syntax::SourceFile &file, const gir::Module &module, const RunOptions &options,
                     syntax::DiagnosticSink &diagnostics, std::ostream &out) {
        ensure_session();
        return Compiler{file, module, diagnostics}.run_program(options, out);
    }

    std::optional<hgraph::Value> evaluate_constant(const syntax::SourceFile &file, const gir::Module &module, gir::ValueId value,
                                                   syntax::DiagnosticSink &diagnostics) {
        ensure_session();
        return Compiler{file, module, diagnostics}.evaluate_constant(value);
    }

    std::string format_time(hgraph::DateTime when) {
        syntax::TemporalValue value;
        value.kind           = syntax::TemporalKind::DateTime;
        value.micros         = when.time_since_epoch().count();
        std::string spelling = syntax::canonical_spelling(value);
        if (!spelling.empty() && spelling.front() == '@') { spelling.erase(0, 1); }
        return spelling;
    }
}  // namespace hgl::wiring
