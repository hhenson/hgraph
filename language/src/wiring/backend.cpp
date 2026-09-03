#include "wiring/backend.h"

#include "syntax/parser.h"
#include "syntax/temporal.h"

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

#if defined(HGL_HAVE_ANALYTICS)
#include <hgraph/analytics/operators.h>
#endif

#include <algorithm>
#include <chrono>
#include <exception>
#include <functional>
#include <iostream>
#include <iterator>
#include <map>
#include <sstream>
#include <stdexcept>
#include <unordered_map>
#include <utility>
#include <variant>

// The direct-wiring backend of the first pass (developer guide,
// "Direct-wiring backend" and "First pass"): a walk over the resolved syntax
// tree that folds constants, wires every time-series operation through
// ``wire_operator``, and drives ``test``/``eval`` with the ``replay``/``record``
// harness and ``run`` with the ``hgl.print_tick`` sink. Nothing here is a
// second runtime: every time-series value is an hgraph port.
namespace hgl::wiring
{
    namespace
    {
        using syntax::Category;
        using syntax::SourceRange;
        using semantics::BindingKind;

        // ----------------------------------------------------------- the sink

        std::ostream *sink_stream = &std::cout;

        /// ``hgl.print_tick``: the ``hgl run`` sink, one ``time value`` line
        /// per tick of its input (user guide, "Running a program").
        struct print_tick : hgraph::Operator<"hgl.print_tick", hgraph::In<"ts", hgraph::TsVar<"S">>>
        {
        };

        struct print_tick_impl
        {
            static void eval(hgraph::In<"ts", hgraph::TsVar<"S">> ts, hgraph::EvaluationClockView clock)
            {
                *sink_stream << format_time(clock.evaluation_time()) << ' ' << ts.value().to_string() << '\n';
            }
        };

        // --------------------------------------------------------- the session

        const hgraph::stdlib::RegisteredStandardTypes &standard_types()
        {
            static const hgraph::stdlib::RegisteredStandardTypes types = hgraph::stdlib::register_standard_types();
            return types;
        }

        // ------------------------------------------------------------ values

        /// Unwinds one test or run after a diagnostic has been reported.
        struct Abort
        {
        };

        /// Unwinds a test body on a failed ``assert``.
        struct TestFailure
        {
            std::string message;
        };

        /// A wiring-time value (developer guide, "First pass": constant,
        /// port, function, harness sequence) plus the callable kinds.
        struct Slot
        {
            enum class Kind : std::uint8_t
            {
                Void,
                Const,
                Null,
                Delta,
                Port,
                Struct,
                Function,
                Operator,
                Intrinsic,
                Sequence,
            };

            Kind                                       kind{Kind::Void};
            hgraph::Value                              value{};
            hgraph::WiringPortRef                      port{};
            ast::DeclId                                fn{ast::no_node};
            std::string                                name{};
            /// Sequence: the literal, until ``eval`` or a comparison gives it
            /// an element schema.
            ast::ExprId                                literal{ast::no_node};
            bool                                       resolved{false};
            std::vector<std::optional<hgraph::Value>> elements{};
            const hgraph::ValueTypeMetaData           *elem_meta{nullptr};
            SourceRange                                range{};

            [[nodiscard]] bool is_const() const noexcept { return kind == Kind::Const; }
            [[nodiscard]] bool is_port() const noexcept { return kind == Kind::Port; }
            [[nodiscard]] const hgraph::ValueTypeMetaData *meta() const noexcept { return value.schema(); }
        };

        Slot make_const(hgraph::Value value, SourceRange range)
        {
            Slot slot;
            slot.kind  = Slot::Kind::Const;
            slot.value = std::move(value);
            slot.range = range;
            return slot;
        }

        Slot make_port(hgraph::WiringPortRef port, SourceRange range)
        {
            Slot slot;
            slot.kind  = Slot::Kind::Port;
            slot.port  = std::move(port);
            slot.range = range;
            return slot;
        }

        Slot make_null(SourceRange range)
        {
            Slot slot;
            slot.kind  = Slot::Kind::Null;
            slot.range = range;
            return slot;
        }

        /// One function activation (or test body): its parameters by index
        /// and its locals by declaring statement.
        struct Frame
        {
            ast::DeclId                              fn{ast::no_node};
            std::vector<Slot>                        params{};
            std::unordered_map<ast::StmtId, Slot>    locals{};
            std::optional<Slot>                      returned{};
            bool                                     in_test{false};
        };

        hgraph::WiringArg ts_arg(hgraph::WiringPortRef port, std::string name = {})
        {
            hgraph::WiringArg arg;
            arg.kind = hgraph::WiringArg::Kind::TimeSeries;
            arg.port = std::move(port);
            arg.name = std::move(name);
            return arg;
        }

        hgraph::WiringArg scalar_arg(const hgraph::Value &value, std::string name = {})
        {
            hgraph::WiringArg arg;
            arg.kind         = hgraph::WiringArg::Kind::Scalar;
            arg.scalar_value = value;
            arg.scalar_meta  = value.schema();
            arg.name         = std::move(name);
            return arg;
        }

        /// A value in source spelling where hgraph's `to_string` differs: an
        /// `f64` keeps a decimal point, tuples and lists describe their
        /// elements the same way.
        std::string describe_view(const hgraph::ValueView &view)
        {
            if (view.schema() == standard_types().float_type)
            {
                std::string text = view.to_string();
                if (text.find_first_of(".ean") == std::string::npos) { text += ".0"; }
                return text;
            }
            if (view.is_tuple())
            {
                const auto  tuple = view.as_tuple();
                std::string out   = "(";
                for (std::size_t i = 0; i < tuple.size(); ++i)
                {
                    if (i != 0) { out += ", "; }
                    out += describe_view(tuple.at(i));
                }
                return out + (tuple.size() == 1 ? ",)" : ")");
            }
            if (view.is_list())
            {
                const auto  list = view.as_list();
                std::string out  = "[";
                for (std::size_t i = 0; i < list.size(); ++i)
                {
                    if (i != 0) { out += ", "; }
                    out += describe_view(list.at(i));
                }
                return out + "]";
            }
            return view.to_string();
        }

        std::string describe_value(const hgraph::Value &value) { return describe_view(value.view()); }

        std::string describe_sequence(const std::vector<std::optional<hgraph::Value>> &elements)
        {
            std::string out = "[";
            for (std::size_t i = 0; i < elements.size(); ++i)
            {
                if (i != 0) { out += ", "; }
                out += elements[i].has_value() ? describe_value(*elements[i]) : "_";
            }
            out += ']';
            return out;
        }

        // ---------------------------------------------------------- compiler

        class Compiler
        {
          public:
            Compiler(const syntax::SourceFile &file, const ast::Module &module,
                     const semantics::ResolvedModule &resolved, syntax::DiagnosticSink &diagnostics)
                : file_{file}, module_{module}, resolved_{resolved}, diagnostics_{diagnostics},
                  types_{standard_types()}, registry_{hgraph::TypeRegistry::instance()}
            {
            }

            [[nodiscard]] std::vector<TestResult> run_tests(const TestOptions &options);
            [[nodiscard]] bool run_program(const RunOptions &options, std::ostream &out);

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

            // -- types
            [[nodiscard]] const hgraph::ValueTypeMetaData *scalar_meta(ast::ScalarType scalar, SourceRange range);
            [[nodiscard]] const hgraph::ValueTypeMetaData *value_meta_for_type(ast::TypeId id, Frame &frame);
            [[nodiscard]] const hgraph::TSValueTypeMetaData *schema_for_type(ast::TypeId id, Frame &frame);
            [[nodiscard]] std::size_t size_argument(ast::ExprId id, Frame &frame, std::string_view what);
            [[nodiscard]] hgraph::Value convert(const hgraph::Value &value, const hgraph::ValueTypeMetaData *target,
                                                SourceRange range, std::string_view what);

            struct ActiveTypeArgument
            {
                ast::DeclId                      decl{ast::no_node};
                std::uint32_t                    index{0};
                const hgraph::ValueTypeMetaData *meta{nullptr};
                ast::TypeId                      source_type{ast::no_node};
                ast::DeclId                      source_struct{ast::no_node};
            };

            struct AppliedStruct
            {
                ast::DeclId                                    decl{ast::no_node};
                std::size_t                                    mark{0};
                std::string                                    local_name{};
                std::vector<const hgraph::ValueTypeMetaData *> generic_types{};
            };

            struct RuntimeStructField
            {
                std::string                        name{};
                const hgraph::ValueTypeMetaData   *value_meta{nullptr};
                const hgraph::TSValueTypeMetaData *schema{nullptr};
                bool                               optional{false};
                bool                               has_default{false};
                Slot                               default_value{};
            };

            [[nodiscard]] AppliedStruct                    apply_struct(ast::TypeId id, Frame &frame);
            [[nodiscard]] AppliedStruct                    apply_plain_struct(ast::DeclId decl, SourceRange range);
            void                                           restore_types(std::size_t mark) { active_type_arguments_.resize(mark); }
            [[nodiscard]] const hgraph::ValueTypeMetaData *type_argument(const ast::GenericArgument &argument, Frame &frame);
            [[nodiscard]] const hgraph::ValueTypeMetaData *named_type_argument(std::string_view name, SourceRange range,
                                                                               Frame &frame);
            [[nodiscard]] const hgraph::ValueTypeMetaData *struct_value_meta(const AppliedStruct &applied, Frame &frame);
            [[nodiscard]] const hgraph::TSValueTypeMetaData *struct_schema(const AppliedStruct &applied, Frame &frame);
            [[nodiscard]] std::vector<RuntimeStructField>    struct_fields(const AppliedStruct &applied, Frame &frame,
                                                                           bool evaluate_defaults);
            [[nodiscard]] Slot eval_construct(ast::DeclId decl, ast::TypeId type, const std::vector<ast::Argument> &arguments,
                                              bool delta, SourceRange range, Frame &frame);

            // -- constants
            [[nodiscard]] bool is_type(const Slot &slot, const hgraph::ValueTypeMetaData *meta) const noexcept
            {
                return slot.is_const() && slot.meta() == meta;
            }
            [[nodiscard]] bool is_int(const Slot &slot) const noexcept { return is_type(slot, types_.int_type); }
            [[nodiscard]] bool is_float(const Slot &slot) const noexcept { return is_type(slot, types_.float_type); }
            [[nodiscard]] bool is_bool(const Slot &slot) const noexcept { return is_type(slot, types_.bool_type); }
            [[nodiscard]] bool is_str(const Slot &slot) const noexcept { return is_type(slot, types_.str_type); }
            [[nodiscard]] static hgraph::Int as_int(const Slot &slot) { return slot.value.view().checked_as<hgraph::Int>(); }
            [[nodiscard]] static hgraph::Float as_float(const Slot &slot)
            {
                return slot.value.view().checked_as<hgraph::Float>();
            }
            [[nodiscard]] static hgraph::Bool as_bool(const Slot &slot)
            {
                return slot.value.view().checked_as<hgraph::Bool>();
            }
            [[nodiscard]] hgraph::Float as_number(const Slot &slot) const
            {
                return is_int(slot) ? static_cast<hgraph::Float>(as_int(slot)) : as_float(slot);
            }
            [[nodiscard]] Slot fold_binary(ast::BinaryOp op, const Slot &lhs, const Slot &rhs, SourceRange range);
            [[nodiscard]] Slot fold_unary(ast::UnaryOp op, const Slot &operand, SourceRange range);
            [[nodiscard]] Slot compare_sequences(const Slot &lhs, const Slot &rhs, bool negate, SourceRange range, Frame &frame);
            void               resolve_sequence(Slot &slot, const hgraph::ValueTypeMetaData *elem_meta, Frame &frame);
            [[nodiscard]] Slot constant_of(const Slot &slot, const hgraph::ValueTypeMetaData *meta, Frame &frame,
                                           const std::string &what);

            // -- wiring
            [[nodiscard]] hgraph::Wiring &wiring(SourceRange range)
            {
                if (wiring_ == nullptr) { backend(range, "a time-series expression is only wired inside eval or a run"); }
                return *wiring_;
            }
            [[nodiscard]] Slot              wire(std::string_view name, std::vector<hgraph::WiringArg> args, SourceRange range,
                                                 std::optional<bool>                output_required = std::nullopt,
                                                 const hgraph::TSValueTypeMetaData *expected        = nullptr);
            [[nodiscard]] hgraph::WiringArg argument_of(const Slot &slot, std::string name);
            [[nodiscard]] Slot              wire_constant(const Slot &slot, const hgraph::TSValueTypeMetaData *schema);
            [[nodiscard]] Slot              wire_binary(ast::BinaryOp op, const Slot &lhs, const Slot &rhs, SourceRange range);

            // -- evaluation
            [[nodiscard]] Slot eval_expr(ast::ExprId id, Frame &frame);
            [[nodiscard]] Slot eval_name(ast::ExprId id, Frame &frame);
            [[nodiscard]] Slot eval_call(const ast::Call &call, SourceRange range, Frame &frame);
            [[nodiscard]] Slot eval_intrinsic(const Slot &callee, const ast::Call &call, SourceRange range, Frame &frame);
            [[nodiscard]] Slot eval_eval(const ast::Eval &eval, SourceRange range, Frame &frame);
            [[nodiscard]] Slot call_function(ast::DeclId decl, const std::vector<ast::Argument> &arguments, SourceRange range,
                                             Frame &frame);
            [[nodiscard]] Slot invoke(ast::DeclId decl, Frame &callee);
            [[nodiscard]] Slot exec_block(ast::BlockId id, Frame &frame);
            void               exec_stmt(ast::StmtId id, Frame &frame);
            [[nodiscard]] Slot eval_if(const ast::If &node, Frame &frame);
            [[nodiscard]] Slot eval_tuple(const ast::TupleLiteral &node, SourceRange range, Frame &frame);
            [[nodiscard]] Slot eval_list(const ast::SequenceLiteral &node, SourceRange range, Frame &frame);
            [[nodiscard]] Slot eval_literal(const ast::TemporalLiteral &node, SourceRange range);
            [[nodiscard]] Slot bind_parameter(const ast::Parameter &param, const Slot &arg, Frame &callee, SourceRange range);
            [[nodiscard]] std::vector<ast::ExprId> bind_arguments(const ast::FunctionDecl          &fn,
                                                                  const std::vector<ast::Argument> &arguments, SourceRange range);
            [[nodiscard]] std::string              describe(const Slot &slot);
            [[nodiscard]] const ast::FunctionDecl &function(ast::DeclId decl) const
            { return std::get<ast::FunctionDecl>(module_.decl(decl).node); }
            [[nodiscard]] std::string slice(SourceRange range) const { return std::string{file_.slice(range)}; }

            // -- programs
            [[nodiscard]] std::optional<hgraph::Value> setting_value(const Setting &setting, const ast::Parameter &param,
                                                                     Frame &frame, SourceRange range);

            const syntax::SourceFile                      &file_;
            const ast::Module                             &module_;
            const semantics::ResolvedModule               &resolved_;
            syntax::DiagnosticSink                        &diagnostics_;
            const hgraph::stdlib::RegisteredStandardTypes &types_;
            hgraph::TypeRegistry                          &registry_;
            hgraph::Wiring                                *wiring_{nullptr};
            std::string                                    compare_detail_{};
            std::vector<ActiveTypeArgument>                active_type_arguments_{};
        };

        // ------------------------------------------------------------- types

        Compiler::AppliedStruct Compiler::apply_plain_struct(ast::DeclId decl, SourceRange range)
        {
            const auto &structure = std::get<ast::StructDecl>(module_.decl(decl).node);
            if (!structure.generics.empty())
            {
                backend(range, "generic struct '" + std::string{structure.name.text} +
                                   "' needs explicit arguments in the executable prototype");
            }
            AppliedStruct applied;
            applied.decl       = decl;
            applied.mark       = active_type_arguments_.size();
            applied.local_name = std::string{structure.name.text};
            return applied;
        }

        const hgraph::ValueTypeMetaData *Compiler::named_type_argument(std::string_view name, SourceRange range, Frame &frame)
        {
            for (auto active = active_type_arguments_.rbegin(); active != active_type_arguments_.rend(); ++active)
            {
                const ast::DeclNode                      &decl     = module_.decl(active->decl).node;
                const std::vector<ast::GenericParameter> *generics = nullptr;
                if (const auto *structure = std::get_if<ast::StructDecl>(&decl)) { generics = &structure->generics; }
                else if (const auto *fn = std::get_if<ast::FunctionDecl>(&decl)) { generics = &fn->generics; }
                else if (const auto *op = std::get_if<ast::OperatorDecl>(&decl)) { generics = &op->generics; }
                if (generics != nullptr && active->index < generics->size() && (*generics)[active->index].name.text == name)
                {
                    return active->meta;
                }
            }
            for (const ast::DeclId id : resolved_.structs)
            {
                const auto &structure = std::get<ast::StructDecl>(module_.decl(id).node);
                if (structure.name.text != name) { continue; }
                const AppliedStruct applied = apply_plain_struct(id, range);
                return struct_value_meta(applied, frame);
            }
            backend(range, "unresolved type argument '" + std::string{name} + "'");
        }

        const hgraph::ValueTypeMetaData *Compiler::type_argument(const ast::GenericArgument &argument, Frame &frame)
        {
            if (argument.type != ast::no_node) { return value_meta_for_type(argument.type, frame); }
            if (!argument.name.empty()) { return named_type_argument(argument.name.text, argument.name.range, frame); }
            backend(argument.range, "a type parameter cannot be bound from a value argument");
        }

        Compiler::AppliedStruct Compiler::apply_struct(ast::TypeId id, Frame &frame)
        {
            const ast::Type          &type    = module_.type(id);
            const semantics::Binding &binding = resolved_.type_binding(id);
            if (type.kind != ast::TypeKind::Named || binding.kind != BindingKind::Struct)
            {
                backend(type.range, "expected a resolved struct type");
            }
            const auto   &structure = std::get<ast::StructDecl>(module_.decl(binding.decl).node);
            AppliedStruct applied;
            applied.decl       = binding.decl;
            applied.mark       = active_type_arguments_.size();
            applied.local_name = std::string{structure.name.text};
            try
            {
                for (std::size_t i = 0; i < structure.generics.size(); ++i)
                {
                    const ast::GenericParameter &parameter = structure.generics[i];
                    if (parameter.is_const)
                    {
                        backend(type.arguments[i].range, "const generic struct arguments require typed constant Bundle "
                                                         "metadata in hgraph");
                    }
                    const hgraph::ValueTypeMetaData *meta = type_argument(type.arguments[i], frame);
                    ActiveTypeArgument               active;
                    active.decl  = binding.decl;
                    active.index = static_cast<std::uint32_t>(i);
                    active.meta  = meta;
                    if (type.arguments[i].type != ast::no_node) { active.source_type = type.arguments[i].type; }
                    else if (!type.arguments[i].name.empty())
                    {
                        for (auto outer = active_type_arguments_.rbegin(); outer != active_type_arguments_.rend(); ++outer)
                        {
                            const ast::DeclNode                      &outer_decl     = module_.decl(outer->decl).node;
                            const std::vector<ast::GenericParameter> *outer_generics = nullptr;
                            if (const auto *s = std::get_if<ast::StructDecl>(&outer_decl)) { outer_generics = &s->generics; }
                            else if (const auto *f = std::get_if<ast::FunctionDecl>(&outer_decl)) { outer_generics = &f->generics; }
                            else if (const auto *o = std::get_if<ast::OperatorDecl>(&outer_decl)) { outer_generics = &o->generics; }
                            if (outer_generics != nullptr && outer->index < outer_generics->size() &&
                                (*outer_generics)[outer->index].name.text == type.arguments[i].name.text)
                            {
                                active.source_type   = outer->source_type;
                                active.source_struct = outer->source_struct;
                                break;
                            }
                        }
                        if (active.source_type == ast::no_node && active.source_struct == ast::no_node)
                        {
                            for (const ast::DeclId candidate : resolved_.structs)
                            {
                                if (std::get<ast::StructDecl>(module_.decl(candidate).node).name.text ==
                                    type.arguments[i].name.text)
                                {
                                    active.source_struct = candidate;
                                    break;
                                }
                            }
                        }
                    }
                    active_type_arguments_.push_back(active);
                    applied.generic_types.push_back(meta);
                }
            }
            catch (...)
            {
                restore_types(applied.mark);
                throw;
            }
            if (!applied.generic_types.empty())
            {
                applied.local_name += '[';
                for (std::size_t i = 0; i < applied.generic_types.size(); ++i)
                {
                    if (i != 0) { applied.local_name += ','; }
                    applied.local_name += applied.generic_types[i]->name();
                }
                applied.local_name += ']';
            }
            return applied;
        }

        std::vector<Compiler::RuntimeStructField> Compiler::struct_fields(const AppliedStruct &applied, Frame &frame,
                                                                          bool evaluate_defaults)
        {
            const auto &structure = std::get<ast::StructDecl>(module_.decl(applied.decl).node);
            if (!resolved_.structure(applied.decl).valid)
            {
                backend(structure.name.range, "struct '" + std::string{structure.name.text} + "' is not valid");
            }
            std::vector<RuntimeStructField> fields;
            for (const ast::TypeId parent_type : structure.parents)
            {
                AppliedStruct parent = apply_struct(parent_type, frame);
                try
                {
                    std::vector<RuntimeStructField> inherited = struct_fields(parent, frame, evaluate_defaults);
                    fields.insert(fields.end(), std::make_move_iterator(inherited.begin()),
                                  std::make_move_iterator(inherited.end()));
                }
                catch (...)
                {
                    restore_types(parent.mark);
                    throw;
                }
                restore_types(parent.mark);
            }

            for (const ast::StructMember &member : structure.members)
            {
                if (const auto *field = std::get_if<ast::StructField>(&member))
                {
                    RuntimeStructField runtime;
                    runtime.name        = std::string{field->name.text};
                    runtime.value_meta  = value_meta_for_type(field->type, frame);
                    runtime.schema      = schema_for_type(field->type, frame);
                    runtime.optional    = field->default_value != ast::no_node &&
                                          std::holds_alternative<ast::NullLiteral>(module_.expr(field->default_value).node);
                    runtime.has_default = field->default_value != ast::no_node;
                    if (evaluate_defaults && runtime.has_default)
                    {
                        runtime.default_value = eval_expr(field->default_value, frame);
                    }
                    fields.push_back(std::move(runtime));
                    continue;
                }
                const auto &override = std::get<ast::InheritedDefault>(member);
                auto        found = std::find_if(fields.begin(), fields.end(),
                                                 [&](const RuntimeStructField &field) { return field.name == override.name.text; });
                if (found == fields.end()) { backend(override.name.range, "unresolved inherited default"); }
                found->has_default = true;
                if (evaluate_defaults) { found->default_value = eval_expr(override.value, frame); }
            }
            return fields;
        }

        const hgraph::ValueTypeMetaData *Compiler::struct_value_meta(const AppliedStruct &applied, Frame &frame)
        {
            const auto                           &structure      = std::get<ast::StructDecl>(module_.decl(applied.decl).node);
            const std::vector<RuntimeStructField> runtime_fields = struct_fields(applied, frame, false);
            std::vector<std::pair<std::string, const hgraph::ValueTypeMetaData *>> fields;
            fields.reserve(runtime_fields.size());
            for (const RuntimeStructField &field : runtime_fields) { fields.emplace_back(field.name, field.value_meta); }

            std::vector<const hgraph::ValueTypeMetaData *> parents;
            for (const ast::TypeId parent_type : structure.parents)
            {
                AppliedStruct parent = apply_struct(parent_type, frame);
                try
                {
                    parents.push_back(struct_value_meta(parent, frame));
                }
                catch (...)
                {
                    restore_types(parent.mark);
                    throw;
                }
                restore_types(parent.mark);
            }
            try
            {
                return registry_.bundle(resolved_.module_path, applied.local_name, fields, parents, structure.abstract, "__type__",
                                        applied.generic_types);
            }
            catch (const std::exception &error)
            {
                backend(structure.name.range, "cannot register struct '" + applied.local_name + "': " + error.what());
            }
        }

        const hgraph::TSValueTypeMetaData *Compiler::struct_schema(const AppliedStruct &applied, Frame &frame)
        {
            const hgraph::ValueTypeMetaData      *value_meta     = struct_value_meta(applied, frame);
            const std::vector<RuntimeStructField> runtime_fields = struct_fields(applied, frame, false);
            std::vector<std::pair<std::string, const hgraph::TSValueTypeMetaData *>> fields;
            fields.reserve(runtime_fields.size());
            for (const RuntimeStructField &field : runtime_fields) { fields.emplace_back(field.name, field.schema); }
            try
            {
                return registry_.tsb(value_meta->name(), fields);
            }
            catch (const std::exception &error)
            {
                backend(module_.decl(applied.decl).range,
                        "cannot register temporal struct '" + applied.local_name + "': " + error.what());
            }
        }

        const hgraph::ValueTypeMetaData *Compiler::scalar_meta(ast::ScalarType scalar, SourceRange range)
        {
            switch (scalar)
            {
                case ast::ScalarType::Bool: return types_.bool_type;
                case ast::ScalarType::I64: return types_.int_type;
                case ast::ScalarType::F64: return types_.float_type;
                case ast::ScalarType::Str: return types_.str_type;
                case ast::ScalarType::Date: return types_.date_type;
                case ast::ScalarType::Time: return types_.time_type;
                case ast::ScalarType::DateTime: return types_.datetime_type;
                case ast::ScalarType::Duration: return types_.timedelta_type;
                case ast::ScalarType::CivilDateTime:
                case ast::ScalarType::ZonedDateTime:
                case ast::ScalarType::ZonedTime:
                case ast::ScalarType::TimeZone: break;
            }
            backend(range, std::string{"'"} + std::string{ast::scalar_type_name(scalar)} +
                               "' is not supported by the first pass (datetime and duration are)");
        }

        std::size_t Compiler::size_argument(ast::ExprId id, Frame &frame, std::string_view what)
        {
            const Slot slot = eval_expr(id, frame);
            if (!is_int(slot) || as_int(slot) < 0)
            {
                fail(Category::Type, module_.expr(id).range,
                     std::string{what} + " must be a non-negative i64 constant");
            }
            return static_cast<std::size_t>(as_int(slot));
        }

        const hgraph::ValueTypeMetaData *Compiler::value_meta_for_type(ast::TypeId id, Frame &frame)
        {
            const ast::Type &type = module_.type(id);
            switch (type.kind)
            {
                case ast::TypeKind::Scalar: return scalar_meta(type.scalar, type.range);
                case ast::TypeKind::Named: {
                    const semantics::Binding &binding = resolved_.type_binding(id);
                    if (binding.kind == BindingKind::Generic)
                    {
                        for (auto active = active_type_arguments_.rbegin(); active != active_type_arguments_.rend(); ++active)
                        {
                            if (active->decl == binding.decl && active->index == binding.index) { return active->meta; }
                        }
                        backend(type.range, "generic type '" + std::string{type.name.text} + "' is not bound");
                    }
                    if (binding.kind == BindingKind::Struct)
                    {
                        AppliedStruct applied = apply_struct(id, frame);
                        try
                        {
                            const hgraph::ValueTypeMetaData *meta = struct_value_meta(applied, frame);
                            restore_types(applied.mark);
                            return meta;
                        }
                        catch (...)
                        {
                            restore_types(applied.mark);
                            throw;
                        }
                    }
                    backend(type.range, "unresolved named value type '" + std::string{type.name.text} + "'");
                }
                case ast::TypeKind::Tuple: {
                    std::vector<const hgraph::ValueTypeMetaData *> elements;
                    elements.reserve(type.children.size());
                    for (ast::TypeId child : type.children) { elements.push_back(value_meta_for_type(child, frame)); }
                    return registry_.tuple(elements);
                }
                case ast::TypeKind::List: {
                    const auto       *element = value_meta_for_type(type.children[0], frame);
                    const std::size_t size    = type.size == ast::no_node ? 0 : size_argument(type.size, frame, "a list size");
                    return registry_.list(element, size);
                }
                case ast::TypeKind::Set: return registry_.set(value_meta_for_type(type.children[0], frame));
                case ast::TypeKind::Map:
                    return registry_.map(value_meta_for_type(type.children[0], frame),
                                         value_meta_for_type(type.children[1], frame));
                case ast::TypeKind::Rolling: backend(type.range, "'rolling' has no value type; it is a time-series window");
                case ast::TypeKind::Atomic: return value_meta_for_type(type.children[0], frame);
            }
            backend(type.range, "unsupported type");
        }

        const hgraph::TSValueTypeMetaData *Compiler::schema_for_type(ast::TypeId id, Frame &frame)
        {
            const ast::Type &type = module_.type(id);
            switch (type.kind)
            {
                case ast::TypeKind::Scalar: return registry_.ts(scalar_meta(type.scalar, type.range));
                case ast::TypeKind::Named: {
                    const semantics::Binding &binding = resolved_.type_binding(id);
                    if (binding.kind == BindingKind::Generic)
                    {
                        for (auto active = active_type_arguments_.rbegin(); active != active_type_arguments_.rend(); ++active)
                        {
                            if (active->decl == binding.decl && active->index == binding.index)
                            {
                                if (active->source_type != ast::no_node) { return schema_for_type(active->source_type, frame); }
                                if (active->source_struct != ast::no_node)
                                {
                                    const AppliedStruct source = apply_plain_struct(active->source_struct, type.range);
                                    return struct_schema(source, frame);
                                }
                                return registry_.ts(active->meta);
                            }
                        }
                        backend(type.range, "generic type '" + std::string{type.name.text} + "' is not bound");
                    }
                    if (binding.kind == BindingKind::Struct)
                    {
                        AppliedStruct applied = apply_struct(id, frame);
                        try
                        {
                            const hgraph::TSValueTypeMetaData *schema = struct_schema(applied, frame);
                            restore_types(applied.mark);
                            return schema;
                        }
                        catch (...)
                        {
                            restore_types(applied.mark);
                            throw;
                        }
                    }
                    backend(type.range, "unresolved named time-series type '" + std::string{type.name.text} + "'");
                }
                case ast::TypeKind::Tuple:
                    backend(type.range, "a structural tuple has no first-pass schema; write "
                                        "atomic<tuple<...>> for one value");
                case ast::TypeKind::List: {
                    const auto       *element = schema_for_type(type.children[0], frame);
                    const std::size_t size    = type.size == ast::no_node ? 0 : size_argument(type.size, frame, "a list size");
                    return registry_.tsl(element, size);
                }
                case ast::TypeKind::Set: return registry_.tss(value_meta_for_type(type.children[0], frame));
                case ast::TypeKind::Map:
                    return registry_.tsd(value_meta_for_type(type.children[0], frame), schema_for_type(type.children[1], frame));
                case ast::TypeKind::Rolling: {
                    const auto *element = value_meta_for_type(type.children[0], frame);
                    const Slot  size    = eval_expr(type.size, frame);
                    if (is_type(size, types_.timedelta_type))
                    {
                        const auto range = size.value.view().checked_as<hgraph::TimeDelta>();
                        hgraph::TimeDelta min{0};
                        if (type.min_size != ast::no_node)
                        {
                            const Slot min_slot = eval_expr(type.min_size, frame);
                            if (!is_type(min_slot, types_.timedelta_type))
                            {
                                fail(Category::Type, module_.expr(type.min_size).range,
                                     "a duration window takes a duration minimum");
                            }
                            min = min_slot.value.view().checked_as<hgraph::TimeDelta>();
                        }
                        return registry_.tsw_duration(element, range, min);
                    }
                    if (!is_int(size) || as_int(size) <= 0)
                    {
                        fail(Category::Type, module_.expr(type.size).range,
                             "a rolling size is a positive i64 constant or a duration");
                    }
                    const auto max = static_cast<std::size_t>(as_int(size));
                    std::size_t min = max;
                    if (type.min_size != ast::no_node) { min = size_argument(type.min_size, frame, "a rolling minimum"); }
                    return registry_.tsw(element, max, min);
                }
                case ast::TypeKind::Atomic: return registry_.ts(value_meta_for_type(type.children[0], frame));
            }
            backend(type.range, "unsupported type");
        }

        hgraph::Value Compiler::convert(const hgraph::Value &value, const hgraph::ValueTypeMetaData *target,
                                        SourceRange range, std::string_view what)
        {
            const auto *source = value.schema();
            if (source == target) { return value; }
            if (source == types_.int_type && target == types_.float_type)
            {
                return hgraph::Value{static_cast<hgraph::Float>(value.view().checked_as<hgraph::Int>())};
            }
            const auto source_kind = source->try_value_kind();
            const auto target_kind = target->try_value_kind();
            if (source_kind == hgraph::ValueTypeKind::Tuple && target_kind == hgraph::ValueTypeKind::Tuple &&
                source->field_count == target->field_count)
            {
                hgraph::Value result{hgraph::ValuePlanFactory::instance().type_for(target)};
                auto          tuple  = result.as_tuple().begin_mutation();
                const auto    fields = value.view().as_tuple();
                for (std::size_t i = 0; i < target->field_count; ++i)
                {
                    const hgraph::Value element{fields.at(i)};
                    tuple.at(i).copy_from(convert(element, target->fields[i].type, range, what).view());
                }
                return result;
            }
            if (source_kind == hgraph::ValueTypeKind::List && target_kind == hgraph::ValueTypeKind::List)
            {
                hgraph::ListBuilder builder{hgraph::ValuePlanFactory::instance().type_for(target->element_type),
                                            *target};
                const auto          items = value.view().as_list();
                for (std::size_t i = 0; i < items.size(); ++i)
                {
                    const hgraph::Value element{items.at(i)};
                    builder.push_back(convert(element, target->element_type, range, what).view());
                }
                return builder.build();
            }
            fail(Category::Type, range,
                 std::string{what} + " expects " + std::string{target->name()} + ", got " +
                     std::string{source->name()});
        }

        // --------------------------------------------------------- constants

        Slot Compiler::fold_unary(ast::UnaryOp op, const Slot &operand, SourceRange range)
        {
            switch (op)
            {
                case ast::UnaryOp::Negate:
                    if (is_int(operand)) { return make_const(hgraph::Value{hgraph::Int{-as_int(operand)}}, range); }
                    if (is_float(operand)) { return make_const(hgraph::Value{hgraph::Float{-as_float(operand)}}, range); }
                    if (is_type(operand, types_.timedelta_type))
                    {
                        return make_const(hgraph::Value{-operand.value.view().checked_as<hgraph::TimeDelta>()}, range);
                    }
                    fail(Category::Type, range, "unary '-' needs a number, got " + std::string{operand.meta()->name()});
                case ast::UnaryOp::Not:
                    if (is_bool(operand)) { return make_const(hgraph::Value{hgraph::Bool{!as_bool(operand)}}, range); }
                    fail(Category::Type, range, "'!' needs a bool, got " + std::string{operand.meta()->name()});
            }
            backend(range, "unsupported unary operator");
        }

        Slot Compiler::fold_binary(ast::BinaryOp op, const Slot &lhs, const Slot &rhs, SourceRange range)
        {
            using ast::BinaryOp;
            const bool numeric = (is_int(lhs) || is_float(lhs)) && (is_int(rhs) || is_float(rhs));
            const bool ints    = is_int(lhs) && is_int(rhs);
            const auto type_error = [&]() -> Slot {
                fail(Category::Type, range,
                     std::string{"'"} + std::string{ast::binary_op_spelling(op)} + "' is not defined for " +
                         std::string{lhs.meta()->name()} + " and " + std::string{rhs.meta()->name()});
            };
            switch (op)
            {
                case BinaryOp::Add:
                    if (ints) { return make_const(hgraph::Value{hgraph::Int{as_int(lhs) + as_int(rhs)}}, range); }
                    if (numeric) { return make_const(hgraph::Value{as_number(lhs) + as_number(rhs)}, range); }
                    if (is_str(lhs) && is_str(rhs))
                    {
                        return make_const(hgraph::Value{lhs.value.view().checked_as<hgraph::Str>() +
                                                        rhs.value.view().checked_as<hgraph::Str>()},
                                          range);
                    }
                    if (is_type(lhs, types_.timedelta_type) && is_type(rhs, types_.timedelta_type))
                    {
                        return make_const(hgraph::Value{lhs.value.view().checked_as<hgraph::TimeDelta>() +
                                                        rhs.value.view().checked_as<hgraph::TimeDelta>()},
                                          range);
                    }
                    if (is_type(lhs, types_.datetime_type) && is_type(rhs, types_.timedelta_type))
                    {
                        return make_const(hgraph::Value{lhs.value.view().checked_as<hgraph::DateTime>() +
                                                        rhs.value.view().checked_as<hgraph::TimeDelta>()},
                                          range);
                    }
                    return type_error();
                case BinaryOp::Sub:
                    if (ints) { return make_const(hgraph::Value{hgraph::Int{as_int(lhs) - as_int(rhs)}}, range); }
                    if (numeric) { return make_const(hgraph::Value{as_number(lhs) - as_number(rhs)}, range); }
                    if (is_type(lhs, types_.timedelta_type) && is_type(rhs, types_.timedelta_type))
                    {
                        return make_const(hgraph::Value{lhs.value.view().checked_as<hgraph::TimeDelta>() -
                                                        rhs.value.view().checked_as<hgraph::TimeDelta>()},
                                          range);
                    }
                    if (is_type(lhs, types_.datetime_type) && is_type(rhs, types_.timedelta_type))
                    {
                        return make_const(hgraph::Value{lhs.value.view().checked_as<hgraph::DateTime>() -
                                                        rhs.value.view().checked_as<hgraph::TimeDelta>()},
                                          range);
                    }
                    if (is_type(lhs, types_.datetime_type) && is_type(rhs, types_.datetime_type))
                    {
                        return make_const(hgraph::Value{lhs.value.view().checked_as<hgraph::DateTime>() -
                                                        rhs.value.view().checked_as<hgraph::DateTime>()},
                                          range);
                    }
                    return type_error();
                case BinaryOp::Mul:
                    if (ints) { return make_const(hgraph::Value{hgraph::Int{as_int(lhs) * as_int(rhs)}}, range); }
                    if (numeric) { return make_const(hgraph::Value{as_number(lhs) * as_number(rhs)}, range); }
                    if (is_type(lhs, types_.timedelta_type) && is_int(rhs))
                    {
                        return make_const(
                            hgraph::Value{lhs.value.view().checked_as<hgraph::TimeDelta>() * as_int(rhs)}, range);
                    }
                    return type_error();
                case BinaryOp::Div:
                    // Like hgraph's ``div_``: integer division is a float.
                    if (numeric)
                    {
                        if (as_number(rhs) == 0.0) { fail(Category::Type, range, "division by zero"); }
                        return make_const(hgraph::Value{as_number(lhs) / as_number(rhs)}, range);
                    }
                    return type_error();
                case BinaryOp::Rem:
                    if (ints)
                    {
                        if (as_int(rhs) == 0) { fail(Category::Type, range, "division by zero"); }
                        return make_const(hgraph::Value{hgraph::Int{as_int(lhs) % as_int(rhs)}}, range);
                    }
                    return type_error();
                case BinaryOp::Equal:
                case BinaryOp::NotEqual:
                {
                    bool equal = false;
                    if (numeric) { equal = as_number(lhs) == as_number(rhs); }
                    else if (lhs.meta() == rhs.meta()) { equal = lhs.value.view().equals(rhs.value.view()); }
                    else { return type_error(); }
                    return make_const(hgraph::Value{hgraph::Bool{op == BinaryOp::Equal ? equal : !equal}}, range);
                }
                case BinaryOp::Less:
                case BinaryOp::LessEqual:
                case BinaryOp::Greater:
                case BinaryOp::GreaterEqual:
                {
                    std::partial_ordering order = std::partial_ordering::unordered;
                    if (numeric) { order = as_number(lhs) <=> as_number(rhs); }
                    else if (lhs.meta() == rhs.meta()) { order = lhs.value.view().compare(rhs.value.view()); }
                    else { return type_error(); }
                    if (order == std::partial_ordering::unordered) { return type_error(); }
                    const bool result = op == BinaryOp::Less           ? order < 0
                                        : op == BinaryOp::LessEqual    ? order <= 0
                                        : op == BinaryOp::Greater      ? order > 0
                                                                       : order >= 0;
                    return make_const(hgraph::Value{hgraph::Bool{result}}, range);
                }
                case BinaryOp::And:
                case BinaryOp::Or:
                    if (is_bool(lhs) && is_bool(rhs))
                    {
                        const bool result =
                            op == BinaryOp::And ? (as_bool(lhs) && as_bool(rhs)) : (as_bool(lhs) || as_bool(rhs));
                        return make_const(hgraph::Value{hgraph::Bool{result}}, range);
                    }
                    return type_error();
            }
            backend(range, "unsupported binary operator");
        }

        /// Gives a literal harness sequence its element schema (the callee's
        /// parameter schema or the other side of a comparison).
        void Compiler::resolve_sequence(Slot &slot, const hgraph::ValueTypeMetaData *elem_meta, Frame &frame)
        {
            if (slot.resolved) { return; }
            const auto &literal = std::get<ast::SequenceLiteral>(module_.expr(slot.literal).node);
            for (const ast::SequenceElement &element : literal.elements)
            {
                if (element.key != ast::no_node)
                {
                    fail(Category::Test, module_.expr(element.key).range,
                         "timed sequences are not supported by the first pass; write one value per cycle");
                }
                const ast::Expr &value = module_.expr(element.value);
                if (std::holds_alternative<ast::Placeholder>(value.node))
                {
                    slot.elements.emplace_back(std::nullopt);
                    continue;
                }
                const Slot item = eval_expr(element.value, frame);
                if (!item.is_const())
                {
                    fail(Category::Type, value.range, "a harness sequence element is a constant");
                }
                slot.elements.emplace_back(convert(item.value, elem_meta, value.range, "the sequence element"));
            }
            slot.elem_meta = elem_meta;
            slot.resolved  = true;
        }

        /// A constant of the value type `meta` from a constant (converted) or,
        /// for a list type inside a test, from a sequence literal, whose
        /// elements are then list elements rather than cycles.
        Slot Compiler::constant_of(const Slot &slot, const hgraph::ValueTypeMetaData *meta, Frame &frame,
                                   const std::string &what)
        {
            if (slot.kind == Slot::Kind::Sequence && meta->try_value_kind() == hgraph::ValueTypeKind::List)
            {
                Slot sequence = slot;
                resolve_sequence(sequence, meta->element_type, frame);
                hgraph::ListBuilder builder{hgraph::ValuePlanFactory::instance().type_for(meta->element_type), *meta};
                for (const std::optional<hgraph::Value> &element : sequence.elements)
                {
                    if (!element) { fail(Category::Type, slot.range, what + " is a list; '_' is not a list element"); }
                    builder.push_back(element->view());
                }
                return make_const(builder.build(), slot.range);
            }
            if (!slot.is_const()) { fail(Category::Type, slot.range, what + " is const; a constant is required"); }
            return make_const(convert(slot.value, meta, slot.range, what), slot.range);
        }

        Slot Compiler::compare_sequences(const Slot &lhs, const Slot &rhs, bool negate, SourceRange range,
                                         Frame &frame)
        {
            Slot left  = lhs;
            Slot right = rhs;
            if (!left.resolved && !right.resolved)
            {
                backend(range, "comparing two literal sequences is not supported; one side comes from eval");
            }
            if (!left.resolved) { resolve_sequence(left, right.elem_meta, frame); }
            if (!right.resolved) { resolve_sequence(right, left.elem_meta, frame); }
            bool equal = left.elements.size() == right.elements.size();
            compare_detail_.clear();
            if (!equal)
            {
                compare_detail_ = "expected " + std::to_string(right.elements.size()) + " cycles, observed " +
                                  std::to_string(left.elements.size()) + ": " + describe_sequence(left.elements);
            }
            for (std::size_t i = 0; equal && i < left.elements.size(); ++i)
            {
                const auto &a = left.elements[i];
                const auto &b = right.elements[i];
                const bool same = a.has_value() == b.has_value() &&
                                  (!a.has_value() || a->view().equals(b->view()));
                if (!same)
                {
                    equal           = false;
                    compare_detail_ = "cycle " + std::to_string(i) + ": expected " +
                                      (b.has_value() ? describe_value(*b) : std::string{"_"}) + ", observed " +
                                      (a.has_value() ? describe_value(*a) : std::string{"_"}) + " in " +
                                      describe_sequence(left.elements);
                }
            }
            return make_const(hgraph::Value{hgraph::Bool{negate ? !equal : equal}}, range);
        }

        // ------------------------------------------------------------ wiring

        Slot Compiler::wire(std::string_view name, std::vector<hgraph::WiringArg> args, SourceRange range,
                            std::optional<bool> output_required, const hgraph::TSValueTypeMetaData *expected)
        {
            hgraph::Wiring &w = wiring(range);
            try
            {
                hgraph::OperatorWireResult result = hgraph::wire_operator(
                    w, name, std::span<const hgraph::WiringArg>{args.data(), args.size()}, output_required, expected);
                if (!result.has_output)
                {
                    Slot slot;
                    slot.range = range;
                    return slot;
                }
                return make_port(result.output.erased(), range);
            }
            catch (const hgraph::OperatorResolutionError &error)
            {
                fail(Category::Operator, range, std::string{name} + ": " + error.what());
            }
            catch (const std::exception &error)
            {
                backend(range, std::string{name} + ": " + error.what());
            }
        }

        hgraph::WiringArg Compiler::argument_of(const Slot &slot, std::string name)
        {
            switch (slot.kind)
            {
                case Slot::Kind::Const: return scalar_arg(slot.value, std::move(name));
                case Slot::Kind::Port: return ts_arg(slot.port, std::move(name));
                case Slot::Kind::Null: backend(slot.range, "null needs an optional field context");
                case Slot::Kind::Delta: backend(slot.range, "a structured delta is not an ordinary operator value");
                case Slot::Kind::Struct:
                case Slot::Kind::Function:
                case Slot::Kind::Operator:
                case Slot::Kind::Intrinsic:
                    backend(slot.range, "passing a function to an operator is not supported by the first pass");
                case Slot::Kind::Sequence: backend(slot.range, "a harness sequence is only valid in eval");
                case Slot::Kind::Void: break;
            }
            backend(slot.range, "this expression produces no value");
        }

        Slot Compiler::wire_constant(const Slot &slot, const hgraph::TSValueTypeMetaData *schema)
        {
            return wire("const", {scalar_arg(slot.value, "value")}, slot.range, true, schema);
        }

        Slot Compiler::wire_binary(ast::BinaryOp op, const Slot &lhs, const Slot &rhs, SourceRange range)
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
            return wire(name, {argument_of(lhs, {}), argument_of(rhs, {})}, range);
        }

        // -------------------------------------------------------- evaluation

        Slot Compiler::eval_literal(const ast::TemporalLiteral &node, SourceRange range)
        {
            using syntax::TemporalKind;
            const syntax::TemporalValue &value = node.value;
            switch (value.kind)
            {
                case TemporalKind::Date:
                    return make_const(hgraph::Value{hgraph::Date{std::chrono::sys_days{std::chrono::days{value.micros}}}},
                                      range);
                case TemporalKind::Time: return make_const(hgraph::Value{hgraph::Time{value.micros}}, range);
                case TemporalKind::DateTime:
                    return make_const(hgraph::Value{hgraph::DateTime{std::chrono::microseconds{value.micros}}}, range);
                case TemporalKind::Duration:
                    return make_const(hgraph::Value{hgraph::TimeDelta{value.micros}}, range);
                case TemporalKind::CivilDateTime:
                case TemporalKind::ZonedDateTime:
                case TemporalKind::ZonedTime:
                case TemporalKind::TimeZone: break;
            }
            backend(range, "zoned and civil literals are not supported by the first pass");
        }

        Slot Compiler::eval_tuple(const ast::TupleLiteral &node, SourceRange range, Frame &frame)
        {
            std::vector<hgraph::Value>                   values;
            std::vector<const hgraph::ValueTypeMetaData *> metas;
            values.reserve(node.elements.size());
            metas.reserve(node.elements.size());
            for (ast::ExprId element : node.elements)
            {
                if (std::holds_alternative<ast::Placeholder>(module_.expr(element).node))
                {
                    backend(module_.expr(element).range,
                            "'_' inside a tuple is not supported by the first pass (whole-value ticks only)");
                }
                Slot item = eval_expr(element, frame);
                if (!item.is_const())
                {
                    backend(module_.expr(element).range,
                            "a tuple of time-series values is not supported by the first pass");
                }
                metas.push_back(item.meta());
                values.push_back(std::move(item.value));
            }
            const auto   *meta = registry_.tuple(metas);
            hgraph::Value result{hgraph::ValuePlanFactory::instance().type_for(meta)};
            auto          tuple = result.as_tuple().begin_mutation();
            for (std::size_t i = 0; i < values.size(); ++i) { tuple.at(i).copy_from(values[i].view()); }
            return make_const(std::move(result), range);
        }

        /// A sequence literal outside a test is a constant list; inside a test
        /// it stays a harness sequence until it meets a parameter schema.
        Slot Compiler::eval_list(const ast::SequenceLiteral &node, SourceRange range, Frame &frame)
        {
            if (frame.in_test)
            {
                Slot slot;
                slot.kind  = Slot::Kind::Sequence;
                slot.range = range;
                // The literal id is recovered by the caller (eval_expr).
                return slot;
            }
            std::vector<hgraph::Value> values;
            for (const ast::SequenceElement &element : node.elements)
            {
                if (element.key != ast::no_node)
                {
                    fail(Category::Type, module_.expr(element.key).range, "a keyed sequence is only valid in a test");
                }
                Slot item = eval_expr(element.value, frame);
                if (!item.is_const())
                {
                    backend(module_.expr(element.value).range,
                            "a list of time-series values is not supported by the first pass");
                }
                values.push_back(std::move(item.value));
            }
            if (values.empty()) { fail(Category::Type, range, "an empty list has no element type"); }
            const auto         *element_meta = values.front().schema();
            const auto         *list_meta    = registry_.list(element_meta);
            hgraph::ListBuilder builder{hgraph::ValuePlanFactory::instance().type_for(element_meta), *list_meta};
            for (const hgraph::Value &value : values)
            {
                builder.push_back(convert(value, element_meta, range, "a list element").view());
            }
            return make_const(builder.build(), range);
        }

        Slot Compiler::eval_construct(ast::DeclId decl, ast::TypeId type, const std::vector<ast::Argument> &arguments, bool delta,
                                      SourceRange range, Frame &frame)
        {
            AppliedStruct applied = type == ast::no_node ? apply_plain_struct(decl, range) : apply_struct(type, frame);
            try
            {
                const auto &structure = std::get<ast::StructDecl>(module_.decl(applied.decl).node);
                if (structure.abstract)
                {
                    fail(Category::Type, range, "abstract struct '" + std::string{structure.name.text} + "' is not constructible");
                }
                const hgraph::ValueTypeMetaData *meta   = struct_value_meta(applied, frame);
                std::vector<RuntimeStructField>  fields = struct_fields(applied, frame, true);
                std::vector<std::optional<Slot>> supplied(fields.size());
                for (const ast::Argument &argument : arguments)
                {
                    if (argument.name.empty())
                    {
                        fail(Category::Type, module_.expr(argument.value).range, "struct construction uses named arguments");
                    }
                    const auto found = std::find_if(fields.begin(), fields.end(), [&](const RuntimeStructField &field) {
                        return field.name == argument.name.text;
                    });
                    if (found == fields.end())
                    {
                        fail(Category::Name, argument.name.range,
                             "struct '" + std::string{structure.name.text} + "' has no field named '" +
                                 std::string{argument.name.text} + "'");
                    }
                    const auto index = static_cast<std::size_t>(found - fields.begin());
                    if (supplied[index].has_value())
                    {
                        fail(Category::Name, argument.name.range, "field '" + std::string{argument.name.text} + "' is given twice");
                    }
                    supplied[index] = eval_expr(argument.value, frame);
                }

                for (std::size_t i = 0; i < fields.size(); ++i)
                {
                    RuntimeStructField &field = fields[i];
                    std::optional<Slot> value = supplied[i];
                    if (!value && !delta && field.has_default) { value = field.default_value; }
                    if (!value)
                    {
                        if (delta || field.optional) { continue; }
                        fail(Category::Type, range,
                             "struct '" + std::string{structure.name.text} + "' needs field '" + field.name + "'");
                    }
                    if (value->kind == Slot::Kind::Null)
                    {
                        if (!field.optional)
                        {
                            fail(Category::Type, value->range, "required field '" + field.name + "' cannot be null");
                        }
                        if (delta)
                        {
                            backend(value->range, "clearing an optional struct field needs the "
                                                  "distinct public hgraph clear-delta operation");
                        }
                        continue;
                    }
                    if (value->kind != Slot::Kind::Const && value->kind != Slot::Kind::Delta)
                    {
                        if (value->kind != Slot::Kind::Port)
                        {
                            fail(Category::Type, value->range, "field '" + field.name + "' needs a value");
                        }
                    }
                    if (value->kind == Slot::Kind::Delta && !delta)
                    {
                        fail(Category::Type, value->range, "a sparse delta cannot initialise complete field '" + field.name + "'");
                    }
                }

                const bool temporal = std::any_of(supplied.begin(), supplied.end(), [](const std::optional<Slot> &value) {
                    return value && value->kind == Slot::Kind::Port;
                });
                if (temporal)
                {
                    if (delta)
                    {
                        backend(range, "a temporal structured delta is only available in a "
                                       "runtime function");
                    }
                    const hgraph::TSValueTypeMetaData *schema = struct_schema(applied, frame);
                    std::vector<hgraph::WiringPortRef> children;
                    children.reserve(fields.size());
                    for (std::size_t i = 0; i < fields.size(); ++i)
                    {
                        RuntimeStructField &field = fields[i];
                        std::optional<Slot> value = supplied[i];
                        if (!value && field.has_default) { value = field.default_value; }
                        if (!value || value->kind == Slot::Kind::Null)
                        {
                            children.push_back(hgraph::WiringPortRef::null_source(field.schema));
                            continue;
                        }
                        if (value->kind == Slot::Kind::Const)
                        {
                            Slot converted = make_const(
                                convert(value->value, field.value_meta, value->range, "field '" + field.name + "'"), value->range);
                            children.push_back(wire_constant(converted, field.schema).port);
                            continue;
                        }
                        if (value->kind != Slot::Kind::Port)
                        {
                            fail(Category::Type, value->range, "field '" + field.name + "' needs a temporal or scalar value");
                        }
                        if (value->port.schema != field.schema)
                        {
                            fail(Category::Type, value->range,
                                 "field '" + field.name + "' expects " + std::string{field.schema->name()} + ", got " +
                                     std::string{value->port.schema->name()});
                        }
                        children.push_back(value->port);
                    }
                    Slot result = make_port(hgraph::WiringPortRef::structural_source(schema, std::move(children)), range);
                    restore_types(applied.mark);
                    return result;
                }

                hgraph::BundleBuilder builder{hgraph::ValuePlanFactory::instance().type_for(meta)};
                for (std::size_t i = 0; i < fields.size(); ++i)
                {
                    RuntimeStructField &field = fields[i];
                    std::optional<Slot> value = supplied[i];
                    if (!value && !delta && field.has_default) { value = field.default_value; }
                    if (!value || value->kind == Slot::Kind::Null) { continue; }
                    builder.set(i, convert(value->value, field.value_meta, value->range, "field '" + field.name + "'").view());
                }
                Slot result = make_const(builder.build(), range);
                if (delta) { result.kind = Slot::Kind::Delta; }
                restore_types(applied.mark);
                return result;
            }
            catch (...)
            {
                restore_types(applied.mark);
                throw;
            }
        }

        Slot Compiler::eval_name(ast::ExprId id, Frame &frame)
        {
            const semantics::Binding &binding = resolved_.binding(id);
            const SourceRange         range   = module_.expr(id).range;
            switch (binding.kind)
            {
                case BindingKind::Local:
                {
                    const auto found = frame.locals.find(binding.stmt);
                    if (found == frame.locals.end())
                    {
                        backend(range, "'" + slice(range) + "' is not bound in this activation");
                    }
                    Slot slot  = found->second;
                    slot.range = range;
                    return slot;
                }
                case BindingKind::Parameter:
                {
                    if (binding.decl != frame.fn || binding.index >= frame.params.size())
                    {
                        backend(range, "'" + slice(range) + "' is not a parameter of this activation");
                    }
                    Slot slot  = frame.params[binding.index];
                    slot.range = range;
                    return slot;
                }
                case BindingKind::Generic:
                    backend(range, "generic parameters are not supported by the first pass");
                case BindingKind::Struct:
                {
                    Slot slot;
                    slot.kind  = Slot::Kind::Struct;
                    slot.fn    = binding.decl;
                    slot.range = range;
                    return slot;
                }
                case BindingKind::Function:
                {
                    Slot slot;
                    slot.kind  = Slot::Kind::Function;
                    slot.fn    = binding.decl;
                    slot.range = range;
                    return slot;
                }
                case BindingKind::Operator:
                {
                    Slot slot;
                    slot.kind  = Slot::Kind::Operator;
                    slot.name  = binding.registry_name;
                    slot.range = range;
                    return slot;
                }
                case BindingKind::LocalOperator:
                    backend(range, "source-defined operators are not supported by the first pass");
                case BindingKind::Intrinsic:
                {
                    Slot slot;
                    slot.kind  = Slot::Kind::Intrinsic;
                    slot.name  = binding.registry_name;
                    slot.range = range;
                    return slot;
                }
                case BindingKind::Test:
                case BindingKind::Unbound: break;
            }
            backend(range, "'" + slice(range) + "' has no value");
        }

        Slot Compiler::eval_if(const ast::If &node, Frame &frame)
        {
            const Slot condition = eval_expr(node.condition, frame);
            if (condition.is_port())
            {
                backend(module_.expr(node.condition).range,
                        "'if' over a time-series condition is not supported by the first pass; use if_then_else");
            }
            if (!is_bool(condition))
            {
                fail(Category::Type, module_.expr(node.condition).range, "an 'if' condition is a bool");
            }
            if (as_bool(condition)) { return exec_block(node.then_block, frame); }
            if (node.otherwise == ast::no_node) { return Slot{}; }
            return eval_expr(node.otherwise, frame);
        }

        Slot Compiler::eval_expr(ast::ExprId id, Frame &frame)
        {
            const ast::Expr &expr = module_.expr(id);
            return std::visit(
                [&](const auto &node) -> Slot {
                    using T = std::decay_t<decltype(node)>;
                    if constexpr (std::is_same_v<T, ast::IntLiteral>)
                    {
                        return make_const(hgraph::Value{hgraph::Int{node.value}}, expr.range);
                    }
                    else if constexpr (std::is_same_v<T, ast::FloatLiteral>)
                    {
                        return make_const(hgraph::Value{hgraph::Float{node.value}}, expr.range);
                    }
                    else if constexpr (std::is_same_v<T, ast::StringLiteral>)
                    {
                        return make_const(hgraph::Value{hgraph::Str{node.value}}, expr.range);
                    }
                    else if constexpr (std::is_same_v<T, ast::BoolLiteral>)
                    {
                        return make_const(hgraph::Value{hgraph::Bool{node.value}}, expr.range);
                    }
                    else if constexpr (std::is_same_v<T, ast::NullLiteral>) { return make_null(expr.range); }
                    else if constexpr (std::is_same_v<T, ast::TemporalLiteral>) { return eval_literal(node, expr.range); }
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
                        const Slot operand = eval_expr(node.operand, frame);
                        if (operand.is_const()) { return fold_unary(node.op, operand, expr.range); }
                        if (!operand.is_port()) { backend(expr.range, "this operand has no value"); }
                        return wire(node.op == ast::UnaryOp::Negate ? "neg_" : "not_", {argument_of(operand, {})},
                                    expr.range);
                    }
                    else if constexpr (std::is_same_v<T, ast::Binary>)
                    {
                        const Slot lhs = eval_expr(node.lhs, frame);
                        const Slot rhs = eval_expr(node.rhs, frame);
                        if (lhs.kind == Slot::Kind::Sequence || rhs.kind == Slot::Kind::Sequence)
                        {
                            if ((node.op != ast::BinaryOp::Equal && node.op != ast::BinaryOp::NotEqual) ||
                                lhs.kind != rhs.kind)
                            {
                                fail(Category::Type, expr.range, "a harness sequence only compares with '==' or '!='");
                            }
                            return compare_sequences(lhs, rhs, node.op == ast::BinaryOp::NotEqual, expr.range, frame);
                        }
                        if (lhs.is_const() && rhs.is_const()) { return fold_binary(node.op, lhs, rhs, expr.range); }
                        return wire_binary(node.op, lhs, rhs, expr.range);
                    }
                    else if constexpr (std::is_same_v<T, ast::Call>) { return eval_call(node, expr.range, frame); }
                    else if constexpr (std::is_same_v<T, ast::Index>)
                    {
                        const Slot target = eval_expr(node.target, frame);
                        const Slot index  = eval_expr(node.index, frame);
                        if (target.is_port())
                        {
                            return wire("getitem_", {argument_of(target, {}), argument_of(index, {})}, expr.range);
                        }
                        if (target.is_const() && target.value.view().is_tuple() && is_int(index))
                        {
                            const auto tuple = target.value.view().as_tuple();
                            const auto i     = as_int(index);
                            if (i < 0 || static_cast<std::size_t>(i) >= tuple.size())
                            {
                                fail(Category::Type, expr.range, "tuple index out of range");
                            }
                            return make_const(hgraph::Value{tuple.at(static_cast<std::size_t>(i))}, expr.range);
                        }
                        if (target.is_const() && target.value.view().is_list() && is_int(index))
                        {
                            const auto list = target.value.view().as_list();
                            const auto i    = as_int(index);
                            if (i < 0 || static_cast<std::size_t>(i) >= list.size())
                            {
                                fail(Category::Type, expr.range, "list index out of range");
                            }
                            return make_const(hgraph::Value{list.at(static_cast<std::size_t>(i))}, expr.range);
                        }
                        backend(expr.range, "indexing is supported on time-series values and constant tuples and lists");
                    }
                    else if constexpr (std::is_same_v<T, ast::Field>)
                    {
                        const Slot target = eval_expr(node.target, frame);
                        if (target.is_port())
                        {
                            return wire("getattr_",
                                        {argument_of(target, {}), scalar_arg(hgraph::Value{hgraph::Str{node.field.text}})},
                                        expr.range);
                        }
                        if ((target.kind == Slot::Kind::Const || target.kind == Slot::Kind::Delta) &&
                            target.value.view().is_bundle())
                        {
                            const auto field = target.value.view().as_bundle().field(node.field.text);
                            if (!field.valid()) { return make_null(expr.range); }
                            return make_const(hgraph::Value{field}, expr.range);
                        }
                        backend(expr.range, "field access needs a temporal or structured value");
                    }
                    else if constexpr (std::is_same_v<T, ast::SequenceLiteral>)
                    {
                        Slot slot = eval_list(node, expr.range, frame);
                        if (slot.kind == Slot::Kind::Sequence) { slot.literal = id; }
                        return slot;
                    }
                    else if constexpr (std::is_same_v<T, ast::TupleLiteral>) { return eval_tuple(node, expr.range, frame); }
                    else if constexpr (std::is_same_v<T, ast::AnonymousFn>)
                    {
                        backend(expr.range, "anonymous functions are not supported by the first pass");
                    }
                    else if constexpr (std::is_same_v<T, ast::If>) { return eval_if(node, frame); }
                    else if constexpr (std::is_same_v<T, ast::BlockExpr>) { return exec_block(node.block, frame); }
                    else if constexpr (std::is_same_v<T, ast::Eval>) { return eval_eval(node, expr.range, frame); }
                    else if constexpr (std::is_same_v<T, ast::Construct>)
                    {
                        const semantics::Binding &binding = resolved_.type_binding(node.type);
                        return eval_construct(binding.decl, node.type, node.arguments, node.delta, expr.range, frame);
                    }
                    else
                    {
                        static_assert(sizeof(T) == 0, "unhandled expression node");
                    }
                },
                expr.node);
        }

        // ------------------------------------------------------------- calls

        Slot Compiler::eval_call(const ast::Call &call, SourceRange range, Frame &frame)
        {
            const Slot callee = eval_expr(call.callee, frame);
            switch (callee.kind)
            {
                case Slot::Kind::Operator:
                {
                    std::vector<hgraph::WiringArg> args;
                    args.reserve(call.arguments.size());
                    for (const ast::Argument &argument : call.arguments)
                    {
                        args.push_back(argument_of(eval_expr(argument.value, frame), std::string{argument.name.text}));
                    }
                    return wire(callee.name, std::move(args), range);
                }
                case Slot::Kind::Struct: return eval_construct(callee.fn, ast::no_node, call.arguments, false, range, frame);
                case Slot::Kind::Function: return call_function(callee.fn, call.arguments, range, frame);
                case Slot::Kind::Intrinsic: return eval_intrinsic(callee, call, range, frame);
                case Slot::Kind::Const:
                case Slot::Kind::Null:
                case Slot::Kind::Delta:
                case Slot::Kind::Port:
                case Slot::Kind::Sequence:
                case Slot::Kind::Void: break;
            }
            fail(Category::Type, module_.expr(call.callee).range, "'" + slice(module_.expr(call.callee).range) +
                                                                      "' is not callable");
        }

        /// The composition-phase meaning of the prelude intrinsics (developer
        /// guide, "Interim kernel table", last paragraph).
        Slot Compiler::eval_intrinsic(const Slot &callee, const ast::Call &call, SourceRange range, Frame &frame)
        {
            const std::string &name = callee.name;
            if (name == "valid" || name == "modified" || name == "all_valid")
            {
                if (call.arguments.empty())
                {
                    fail(Category::Type, range, "'" + name + "' takes at least one time-series argument");
                }
                const char *op   = name == "modified" ? "modified" : "valid";
                const char *fold = name == "modified" ? "or_" : "and_";
                std::optional<Slot> result;
                for (const ast::Argument &argument : call.arguments)
                {
                    const Slot value = eval_expr(argument.value, frame);
                    if (!value.is_port())
                    {
                        fail(Category::Type, module_.expr(argument.value).range,
                             "'" + name + "' takes time-series arguments");
                    }
                    Slot flag = wire(op, {ts_arg(value.port)}, range);
                    result    = result ? wire(fold, {ts_arg(result->port), ts_arg(flag.port)}, range) : flag;
                }
                return *result;
            }
            if (name == "last_modified" || name == "key_set")
            {
                if (call.arguments.size() != 1)
                {
                    fail(Category::Type, range, "'" + name + "' takes one time-series argument");
                }
                const Slot value = eval_expr(call.arguments[0].value, frame);
                if (!value.is_port())
                {
                    fail(Category::Type, module_.expr(call.arguments[0].value).range,
                         "'" + name + "' takes a time-series argument");
                }
                return wire(name == "last_modified" ? "last_modified_time" : "keys_", {ts_arg(value.port)}, range);
            }
            backend(range, "'" + name + "' is a runtime traversal; it is not available in a composition body of the "
                           "first pass");
        }

        /// Positional then named arguments, then defaults: one expression per
        /// parameter (syntax guide, "Calls").
        std::vector<ast::ExprId> Compiler::bind_arguments(const ast::FunctionDecl &fn,
                                                          const std::vector<ast::Argument> &arguments,
                                                          SourceRange range)
        {
            const auto              &params = fn.signature.parameters;
            std::vector<ast::ExprId> bound(params.size(), ast::no_node);
            std::size_t              next = 0;
            for (const ast::Argument &argument : arguments)
            {
                const SourceRange where = module_.expr(argument.value).range;
                if (argument.name.empty())
                {
                    if (next >= params.size())
                    {
                        fail(Category::Type, where,
                             "'" + std::string{fn.name.text} + "' takes " + std::to_string(params.size()) + " arguments");
                    }
                    if (bound[next] != ast::no_node)
                    {
                        fail(Category::Type, where, "positional argument after a named one");
                    }
                    bound[next++] = argument.value;
                    continue;
                }
                const auto found = std::find_if(params.begin(), params.end(), [&](const ast::Parameter &param) {
                    return std::string{param.name.text} == std::string{argument.name.text};
                });
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
                    fail(Category::Type, range, "'" + std::string{fn.name.text} + "' needs an argument for '" +
                                                    std::string{params[i].name.text} + "'");
                }
            }
            return bound;
        }

        /// Places one argument value in the callee frame: constants convert to
        /// a `const` parameter's type or wire `const` at a temporal
        /// parameter's schema; ports must match the schema exactly.
        Slot Compiler::bind_parameter(const ast::Parameter &param, const Slot &arg, Frame &callee, SourceRange range)
        {
            if (param.is_const)
            {
                const auto *meta = value_meta_for_type(param.type, callee);
                Slot        slot = constant_of(arg, meta, callee, "parameter '" + std::string{param.name.text} + "'");
                slot.range       = range;
                return slot;
            }
            const auto *schema = schema_for_type(param.type, callee);
            if (arg.is_const()) { return wire_constant(make_const(convert(arg.value, schema->value_schema, arg.range,
                                                                            "parameter '" + std::string{param.name.text} + "'"),
                                                                    arg.range),
                                                         schema); }
            if (!arg.is_port())
            {
                fail(Category::Type, arg.range, "parameter '" + std::string{param.name.text} + "' takes a time-series value");
            }
            if (arg.port.schema != schema)
            {
                fail(Category::Type, arg.range,
                     "parameter '" + std::string{param.name.text} + "' expects " + std::string{schema->name()} + ", got " +
                         std::string{arg.port.schema == nullptr ? "an untyped port" : arg.port.schema->name()});
            }
            return make_port(arg.port, range);
        }

        Slot Compiler::call_function(ast::DeclId decl, const std::vector<ast::Argument> &arguments, SourceRange range,
                                     Frame &frame)
        {
            const ast::FunctionDecl &fn = function(decl);
            if (resolved_.kind(decl) == semantics::FunctionKind::Runtime)
            {
                backend(range, "'" + std::string{fn.name.text} +
                                   "' is a runtime function; the first pass composes graphs from kernel operators");
            }
            if (fn.visibility == ast::FunctionVisibility::Impl)
            {
                backend(range, "source-defined operators are not supported by the first pass");
            }
            if (!fn.generics.empty()) { backend(range, "generic functions are not supported by the first pass"); }
            const std::vector<ast::ExprId> bound = bind_arguments(fn, arguments, range);
            Frame                          callee;
            callee.fn = decl;
            callee.params.resize(bound.size());
            const auto &params = fn.signature.parameters;
            // Const parameters first: temporal parameter types may name them.
            for (std::size_t pass = 0; pass < 2; ++pass)
            {
                for (std::size_t i = 0; i < params.size(); ++i)
                {
                    if (params[i].is_const != (pass == 0)) { continue; }
                    Slot arg = bound[i] != ast::no_node ? eval_expr(bound[i], frame)
                                                        : eval_expr(params[i].default_value, callee);
                    callee.params[i] = bind_parameter(params[i], arg, callee, arg.range);
                }
            }
            Slot result  = invoke(decl, callee);
            result.range = range;
            return result;
        }

        Slot Compiler::invoke(ast::DeclId decl, Frame &callee)
        {
            const ast::FunctionDecl &fn = function(decl);
            if (fn.concise_body != ast::no_node) { return eval_expr(fn.concise_body, callee); }
            Slot result = exec_block(fn.block_body, callee);
            if (callee.returned) { return *callee.returned; }
            return result;
        }

        // -------------------------------------------------------- statements

        Slot Compiler::exec_block(ast::BlockId id, Frame &frame)
        {
            const ast::Block &block = module_.block(id);
            Slot              result;
            for (std::size_t i = 0; i < block.statements.size(); ++i)
            {
                if (frame.returned) { break; }
                const bool is_tail = block.tail != ast::no_node && i + 1 == block.statements.size();
                if (is_tail) { result = eval_expr(block.tail, frame); }
                else { exec_stmt(block.statements[i], frame); }
            }
            return result;
        }

        void Compiler::exec_stmt(ast::StmtId id, Frame &frame)
        {
            const ast::Stmt &stmt = module_.stmt(id);
            std::visit(
                [&](const auto &node) {
                    using T = std::decay_t<decltype(node)>;
                    if constexpr (std::is_same_v<T, ast::LocalDecl>)
                    {
                        Slot value = eval_expr(node.init, frame);
                        if (node.type != ast::no_node)
                        {
                            // A constant keeps its value shape; it meets a
                            // schema only at a temporal parameter.
                            if (value.is_const() || value.kind == Slot::Kind::Sequence)
                            {
                                value = constant_of(value, value_meta_for_type(node.type, frame), frame,
                                                    "'" + std::string{node.name.text} + "'");
                            }
                            else if (value.is_port())
                            {
                                const auto *schema = schema_for_type(node.type, frame);
                                if (value.port.schema != schema)
                                {
                                    fail(Category::Type, value.range,
                                         "'" + std::string{node.name.text} + "' is declared " + std::string{schema->name()} +
                                             " but the initialiser is " + std::string{value.port.schema->name()});
                                }
                            }
                        }
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
                        const auto       &decl   = module_.stmt(target);
                        if (const auto *local = std::get_if<ast::LocalDecl>(&decl.node);
                            local == nullptr || !local->mutable_)
                        {
                            fail(Category::Type, place.range, "'" + slice(place.range) + "' is not a 'var'");
                        }
                        Slot value = eval_expr(node.value, frame);
                        if (node.op != ast::AssignOp::Assign)
                        {
                            const Slot current = frame.locals.at(target);
                            const ast::BinaryOp op = node.op == ast::AssignOp::Add   ? ast::BinaryOp::Add
                                                     : node.op == ast::AssignOp::Sub ? ast::BinaryOp::Sub
                                                     : node.op == ast::AssignOp::Mul ? ast::BinaryOp::Mul
                                                                                     : ast::BinaryOp::Div;
                            value = current.is_const() && value.is_const() ? fold_binary(op, current, value, stmt.range)
                                                                           : wire_binary(op, current, value, stmt.range);
                        }
                        frame.locals[target] = std::move(value);
                    }
                    else if constexpr (std::is_same_v<T, ast::ReturnStmt>)
                    {
                        frame.returned = node.value == ast::no_node ? Slot{} : eval_expr(node.value, frame);
                    }
                    else if constexpr (std::is_same_v<T, ast::AssertStmt>)
                    {
                        const Slot condition = eval_expr(node.condition, frame);
                        if (!is_bool(condition))
                        {
                            fail(Category::Type, module_.expr(node.condition).range, "'assert' takes a bool");
                        }
                        if (!as_bool(condition))
                        {
                            std::string message = "assert failed: " + slice(module_.expr(node.condition).range);
                            if (!compare_detail_.empty()) { message += "\n" + compare_detail_; }
                            throw TestFailure{std::move(message)};
                        }
                    }
                    else if constexpr (std::is_same_v<T, ast::ExprStmt>) { (void)eval_expr(node.expr, frame); }
                    else
                    {
                        backend(stmt.range, "runtime statements are not evaluated by the first pass");
                    }
                },
                stmt.node);
        }

        // -------------------------------------------------------------- eval

        /// `eval(fn, sequences...)`: one replay node per temporal parameter,
        /// the function's body wired between them and a record node, one
        /// simulation run, and the recorded deltas as a dense sequence
        /// (developer guide, "First pass"; syntax guide, "Tests").
        Slot Compiler::eval_eval(const ast::Eval &eval, SourceRange range, Frame &frame)
        {
            const Slot callee = eval_expr(eval.callee, frame);
            if (callee.kind == Slot::Kind::Operator || callee.kind == Slot::Kind::Intrinsic)
            {
                backend(module_.expr(eval.callee).range,
                        "eval takes a module function; wrap the operator in a fn to evaluate it");
            }
            if (callee.kind != Slot::Kind::Function)
            {
                fail(Category::Type, module_.expr(eval.callee).range, "eval takes a function");
            }
            const ast::FunctionDecl &fn = function(callee.fn);
            if (resolved_.kind(callee.fn) == semantics::FunctionKind::Runtime)
            {
                backend(range, "'" + std::string{fn.name.text} + "' is a runtime function; the first pass evaluates compositions");
            }
            if (!fn.generics.empty()) { backend(range, "generic functions are not supported by the first pass"); }
            const std::vector<ast::ExprId> bound = bind_arguments(fn, eval.arguments, range);
            const auto                    &params = fn.signature.parameters;

            hgraph::Wiring w;
            hgraph::record_replay::set_config(
                w.global_state(),
                hgraph::record_replay::RecordReplayConfig{.backend = std::string{hgraph::record_replay::TESTING}});
            hgraph::Wiring *const outer = wiring_;
            wiring_                     = &w;
            struct Restore
            {
                hgraph::Wiring *&slot;
                hgraph::Wiring  *previous;
                ~Restore() { slot = previous; }
            } restore{wiring_, outer};

            Frame callee_frame;
            callee_frame.fn = callee.fn;
            callee_frame.params.resize(params.size());
            std::vector<std::vector<std::optional<hgraph::Value>>> inputs;
            std::vector<std::string>                                keys;
            std::size_t                                             cycles = 0;
            for (std::size_t pass = 0; pass < 2; ++pass)
            {
                for (std::size_t i = 0; i < params.size(); ++i)
                {
                    const ast::Parameter &param = params[i];
                    if (param.is_const != (pass == 0)) { continue; }
                    if (param.is_const)
                    {
                        Slot arg = bound[i] != ast::no_node ? eval_expr(bound[i], frame)
                                                            : eval_expr(param.default_value, callee_frame);
                        callee_frame.params[i] = bind_parameter(param, arg, callee_frame, arg.range);
                        continue;
                    }
                    const auto *schema = schema_for_type(param.type, callee_frame);
                    if (schema->kind != hgraph::TSTypeKind::TS)
                    {
                        backend(module_.type(param.type).range,
                                "eval drives ts parameters in the first pass; '" + std::string{param.name.text} + "' is " +
                                    std::string{schema->name()});
                    }
                    std::string key = "hgl::in::" + std::string{param.name.text};
                    if (bound[i] == ast::no_node)
                    {
                        // A default for a temporal parameter: a constant, present every cycle.
                        Slot value = eval_expr(param.default_value, callee_frame);
                        if (!value.is_const())
                        {
                            fail(Category::Type, value.range, "a temporal parameter's default is a constant");
                        }
                        inputs.push_back({convert(value.value, schema->value_schema, value.range,
                                                  "parameter '" + std::string{param.name.text} + "'")});
                    }
                    else
                    {
                        Slot sequence = eval_expr(bound[i], frame);
                        if (sequence.kind != Slot::Kind::Sequence)
                        {
                            fail(Category::Type, module_.expr(bound[i]).range,
                                 "eval drives '" + std::string{param.name.text} + "' with a harness sequence");
                        }
                        resolve_sequence(sequence, schema->value_schema, frame);
                        inputs.push_back(std::move(sequence.elements));
                    }
                    cycles = std::max(cycles, inputs.back().size());
                    keys.push_back(key);
                    callee_frame.params[i] =
                        wire("replay", {scalar_arg(hgraph::Value{hgraph::Str{key}}, "key")}, range, true, schema);
                }
            }

            Slot result = invoke(callee.fn, callee_frame);
            if (result.is_const()) { result = wire_constant(result, registry_.ts(result.meta())); }
            const hgraph::TSValueTypeMetaData *out_schema = nullptr;
            if (result.is_port())
            {
                out_schema = result.port.schema;
                (void)wire("record", {ts_arg(result.port), scalar_arg(hgraph::Value{hgraph::Str{"hgl::out"}}, "key")},
                           range, false);
            }
            else if (result.kind != Slot::Kind::Void)
            {
                backend(range, "'" + std::string{fn.name.text} + "' does not produce a time-series value");
            }

            std::vector<std::optional<hgraph::Value>> observed;
            try
            {
                hgraph::GraphBuilder graph = std::move(w).finish();
                for (std::size_t i = 0; i < keys.size(); ++i)
                {
                    hgraph::testing::set_replay_deltas(graph.global_state(), keys[i], inputs[i]);
                }
                hgraph::GraphExecutorBuilder builder;
                builder.graph_builder(std::move(graph)).start_time(hgraph::MIN_ST).end_time(hgraph::MAX_ET);
                hgraph::GraphExecutorValue executor = builder.make_executor();
                auto                       view     = executor.view();
                view.run();
                if (out_schema != nullptr)
                {
                    observed = hgraph::testing::get_recorded_deltas(view.graph().global_state(), "hgl::out");
                }
            }
            catch (const std::exception &error)
            {
                throw TestFailure{"eval of '" + std::string{fn.name.text} + "' failed: " + error.what()};
            }
            wiring_ = outer;
            while (observed.size() < cycles) { observed.emplace_back(std::nullopt); }

            Slot sequence;
            sequence.kind      = Slot::Kind::Sequence;
            sequence.resolved  = true;
            sequence.elements  = std::move(observed);
            sequence.elem_meta = out_schema != nullptr ? out_schema->delta_value_schema : nullptr;
            sequence.range     = range;
            return sequence;
        }

        std::string Compiler::describe(const Slot &slot)
        {
            switch (slot.kind)
            {
                case Slot::Kind::Const: return describe_value(slot.value);
                case Slot::Kind::Null: return "null";
                case Slot::Kind::Delta: return "delta " + describe_value(slot.value);
                case Slot::Kind::Port: return std::string{slot.port.schema->name()};
                case Slot::Kind::Struct:
                    return "struct " + std::string{std::get<ast::StructDecl>(module_.decl(slot.fn).node).name.text};
                case Slot::Kind::Function: return "fn " + std::string{function(slot.fn).name.text};
                case Slot::Kind::Operator: return "operator " + slot.name;
                case Slot::Kind::Intrinsic: return "intrinsic " + slot.name;
                case Slot::Kind::Sequence:
                    return slot.resolved ? describe_sequence(slot.elements) : slice(slot.range);
                case Slot::Kind::Void: break;
            }
            return {};
        }

        // ------------------------------------------------------------- tests

        std::vector<TestResult> Compiler::run_tests(const TestOptions &options)
        {
            std::vector<TestResult> results;
            for (ast::DeclId id : resolved_.tests)
            {
                const auto &test = std::get<ast::TestDecl>(module_.decl(id).node);
                if (!options.names.empty() &&
                    std::find(options.names.begin(), options.names.end(), std::string{test.name.text}) == options.names.end())
                {
                    continue;
                }
                TestResult result;
                result.name = std::string{test.name.text};
                Frame frame;
                frame.in_test = true;
                compare_detail_.clear();
                const std::size_t before = diagnostics_.size();
                try
                {
                    Slot tail     = exec_block(test.block, frame);
                    result.passed = true;
                    if (options.describe_tail && tail.kind != Slot::Kind::Void) { result.tail = describe(tail); }
                }
                catch (const TestFailure &failure)
                {
                    result.message = failure.message;
                }
                catch (const Abort &)
                {
                    result.message = "diagnostics reported";
                }
                catch (const std::exception &error)
                {
                    result.message = std::string{"error: "} + error.what();
                }
                if (diagnostics_.size() != before) { result.passed = false; }
                results.push_back(std::move(result));
            }
            return results;
        }

        // ---------------------------------------------------------- programs

        /// A `--set name=<constant expression>` value, parsed and folded as
        /// HGL in a scratch unit then converted to the parameter's type.
        std::optional<hgraph::Value> Compiler::setting_value(const Setting &setting, const ast::Parameter &param,
                                                             Frame &frame, SourceRange range)
        {
            const syntax::SourceFile scratch{"--set " + setting.name, "module hgl.cli\nfn __set() => " + setting.text + "\n"};
            syntax::DiagnosticSink   scratch_diagnostics;
            const ast::Module        scratch_module = syntax::parse(scratch, scratch_diagnostics);
            semantics::ResolvedModule scratch_resolved;
            if (!scratch_diagnostics.has_errors())
            {
                scratch_resolved = semantics::resolve(scratch, scratch_module, has_operator, scratch_diagnostics);
            }
            std::optional<hgraph::Value> value;
            if (!scratch_diagnostics.has_errors())
            {
                Compiler scratch_compiler{scratch, scratch_module, scratch_resolved, scratch_diagnostics};
                Frame    scratch_frame;
                try
                {
                    const ast::DeclId fn = scratch_resolved.functions.at(0);
                    scratch_frame.fn     = fn;
                    const Slot slot = scratch_compiler.invoke(fn, scratch_frame);
                    if (!slot.is_const())
                    {
                        scratch_diagnostics.report(Category::Type, slot.range, "a --set value is a constant expression");
                    }
                    else { value = slot.value; }
                }
                catch (const Abort &)
                {
                }
            }
            if (scratch_diagnostics.has_errors())
            {
                fail(Category::Type, range,
                     "--set " + setting.name + ": invalid value\n" + scratch_diagnostics.render(scratch));
            }
            return convert(*value, value_meta_for_type(param.type, frame), range, "'" + std::string{param.name.text} + "'");
        }

        bool Compiler::run_program(const RunOptions &options, std::ostream &out)
        {
            // The entry: the named export, or the only export with no temporal parameters.
            std::vector<ast::DeclId> candidates;
            for (ast::DeclId id : resolved_.functions)
            {
                const ast::FunctionDecl &fn = function(id);
                if (fn.visibility != ast::FunctionVisibility::Export) { continue; }
                if (!options.entry.empty())
                {
                    if (std::string{fn.name.text} == options.entry) { candidates.push_back(id); }
                    continue;
                }
                const bool all_const = std::all_of(fn.signature.parameters.begin(), fn.signature.parameters.end(),
                                                   [](const ast::Parameter &param) { return param.is_const; });
                if (all_const) { candidates.push_back(id); }
            }
            const SourceRange whole{0, 0};
            try
            {
                if (candidates.empty())
                {
                    backend(whole, options.entry.empty()
                                       ? std::string{"no entry: an entry is an export fn with only const parameters"}
                                       : "no export fn named '" + options.entry + "'");
                }
                if (candidates.size() > 1)
                {
                    backend(whole, "several entries; choose one with --entry");
                }
                const ast::DeclId        entry = candidates.front();
                const ast::FunctionDecl &fn    = function(entry);
                if (resolved_.kind(entry) == semantics::FunctionKind::Runtime)
                {
                    backend(module_.decl(entry).range, "'" + std::string{fn.name.text} + "' is a runtime function");
                }
                for (const Setting &setting : options.settings)
                {
                    const auto &params = fn.signature.parameters;
                    if (std::none_of(params.begin(), params.end(),
                                     [&](const ast::Parameter &param) { return std::string{param.name.text} == setting.name; }))
                    {
                        fail(Category::Name, whole, "--set " + setting.name + ": '" + std::string{fn.name.text} +
                                                        "' has no parameter named '" + setting.name + "'");
                    }
                }

                hgraph::Wiring w;
                wiring_ = &w;
                Frame frame;
                frame.fn = entry;
                frame.params.resize(fn.signature.parameters.size());
                for (std::size_t i = 0; i < fn.signature.parameters.size(); ++i)
                {
                    const ast::Parameter &param = fn.signature.parameters[i];
                    const SourceRange     range = param.name.range;
                    if (!param.is_const)
                    {
                        backend(range, "entry parameter '" + std::string{param.name.text} + "' is not const; nothing drives it");
                    }
                    const auto setting = std::find_if(options.settings.begin(), options.settings.end(),
                                                      [&](const Setting &s) { return s.name == std::string{param.name.text}; });
                    if (setting != options.settings.end())
                    {
                        frame.params[i] = make_const(*setting_value(*setting, param, frame, range), range);
                    }
                    else if (param.default_value != ast::no_node)
                    {
                        Slot value      = eval_expr(param.default_value, frame);
                        frame.params[i] = bind_parameter(param, value, frame, range);
                    }
                    else
                    {
                        fail(Category::Type, range, "'" + std::string{param.name.text} + "' has no default; pass --set " +
                                                        std::string{param.name.text} + "=<value>");
                    }
                }
                Slot result = invoke(entry, frame);
                if (result.is_const()) { result = wire_constant(result, registry_.ts(result.meta())); }
                if (!result.is_port())
                {
                    backend(module_.decl(entry).range, "'" + std::string{fn.name.text} + "' produces no time-series value to run");
                }
                (void)wire("hgl.print_tick", {ts_arg(result.port)}, module_.decl(entry).range, false);
                wiring_ = nullptr;

                hgraph::GraphBuilder         graph = std::move(w).finish();
                hgraph::GraphExecutorBuilder builder;
                builder.graph_builder(std::move(graph));
                const bool realtime = options.mode == RunMode::RealTime;
                builder.mode(realtime ? hgraph::GraphExecutorMode::RealTime : hgraph::GraphExecutorMode::Simulation);
                hgraph::DateTime start = hgraph::MIN_ST;
                if (options.start) { start = *options.start; }
                else if (realtime)
                {
                    start = std::chrono::time_point_cast<std::chrono::microseconds>(hgraph::engine_clock::now());
                }
                builder.start_time(start);
                if (options.end) { builder.end_time(*options.end); }
                else if (options.end_after) { builder.end_time(start + *options.end_after); }
                else { builder.end_time(hgraph::MAX_ET); }
                std::ostream *const previous = sink_stream;
                sink_stream                  = &out;
                struct RestoreSink
                {
                    std::ostream *saved;
                    ~RestoreSink() { sink_stream = saved; }
                } restore_sink{previous};
                hgraph::GraphExecutorValue executor = builder.make_executor();
                auto                       view     = executor.view();
                view.run();
                return true;
            }
            catch (const Abort &)
            {
                wiring_ = nullptr;
                return false;
            }
            catch (const std::exception &error)
            {
                wiring_ = nullptr;
                diagnostics_.report(Category::Backend, whole, std::string{"run failed: "} + error.what());
                return false;
            }
        }
    }  // namespace

    // ------------------------------------------------------------ public API

    void ensure_session()
    {
        static const bool installed = [] {
            (void)standard_types();
            hgraph::stdlib::register_standard_operators();
#if defined(HGL_HAVE_ANALYTICS)
            hgraph::analytics::register_analytics_operators();
#endif
            auto &registry = hgraph::OperatorRegistry::instance();
            registry.register_installer("hgl.wiring",
                                        [] { hgraph::register_overload<print_tick, print_tick_impl>(); });
            registry.run_installers();
            return true;
        }();
        (void)installed;
    }

    bool has_operator(std::string_view name)
    {
        ensure_session();
        return !hgraph::OperatorRegistry::instance().overload_signatures(name).empty();
    }

    std::vector<TestResult> run_tests(const syntax::SourceFile &file, const ast::Module &module,
                                      const semantics::ResolvedModule &resolved, const TestOptions &options,
                                      syntax::DiagnosticSink &diagnostics)
    {
        ensure_session();
        Compiler compiler{file, module, resolved, diagnostics};
        return compiler.run_tests(options);
    }

    bool run_program(const syntax::SourceFile &file, const ast::Module &module,
                     const semantics::ResolvedModule &resolved, const RunOptions &options,
                     syntax::DiagnosticSink &diagnostics, std::ostream &out)
    {
        ensure_session();
        Compiler compiler{file, module, resolved, diagnostics};
        return compiler.run_program(options, out);
    }

    std::string format_time(hgraph::DateTime when)
    {
        syntax::TemporalValue value;
        value.kind   = syntax::TemporalKind::DateTime;
        value.micros = when.time_since_epoch().count();
        std::string spelling = syntax::canonical_spelling(value);
        if (!spelling.empty() && spelling.front() == '@') { spelling.erase(0, 1); }
        return spelling;
    }
}  // namespace hgl::wiring
