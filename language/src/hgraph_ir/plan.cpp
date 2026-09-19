#include "hgraph_ir/plan.h"
#include <algorithm>
#include <functional>
#include <map>
#include <set>
#include <unordered_set>
namespace hgl::hgraph_ir
{
    namespace gir = hgraph_ir;
    namespace hir = ir::hir;
    namespace
    {
        using syntax::Category;
        using syntax::SourceRange;
        struct Abort
        {};
        std::string_view local_identity(std::string_view name) { return name.substr(name.find_last_of('.') + 1); }
        class Planner
        {
          public:
            Planner(gir::Module &graph, syntax::DiagnosticSink &diagnostics) : graph_(graph), diagnostics_(diagnostics) {}
            void run() {
                try {
                    graph_.callable_order = order_callables();
                } catch (const Abort &) {}
                graph_.runtime_plans.resize(graph_.callables.size());
                for (std::uint32_t index = 0; index < graph_.callables.size(); ++index) {
                    if (graph_.callables[index].kind != gir::CallableKind::RuntimeNode) continue;
                    bool materialized = false;
                    for (const auto &materialization : graph_.materializations) {
                        if (materialization.implementation.value != index) continue;
                        materialized   = true;
                        substitutions_ = &materialization.substitutions;
                        try {
                            graph_.runtime_plans[index] = runtime_info(gir::CallableId{index});
                        } catch (const Abort &) {}
                    }
                    substitutions_ = nullptr;
                    if (!materialized) {
                        try {
                            graph_.runtime_plans[index] = runtime_info(gir::CallableId{index});
                        } catch (const Abort &) {}
                    }
                }
            }

          private:
            [[noreturn]] void fail(Category category, SourceRange range, std::string message) {
                diagnostics_.report(category, range, std::move(message));
                throw Abort{};
            }
            [[noreturn]] void backend(SourceRange range, std::string message) { fail(Category::Type, range, std::move(message)); }
            const gir::Callable &callable(gir::CallableId id, SourceRange range = {}) {
                if (!id.valid() || id.value >= graph_.callables.size())
                    backend(range, "invalid graph-IR Callable handle in planning");
                return graph_.callables[id.value];
            }
            const gir::NativeFunction &native_function(gir::NativeFunctionId id, SourceRange range = {}) {
                if (!id.valid() || id.value >= graph_.native_functions.size())
                    backend(range, "invalid graph-IR NativeFunction handle in planning");
                return graph_.native_functions[id.value];
            }
            const gir::Value &planned_value(gir::ValueId id, SourceRange range = {}) {
                if (!id.valid() || id.value >= graph_.values.size()) backend(range, "invalid graph-IR Value handle in planning");
                return graph_.values[id.value];
            }
            const gir::Binding &planned_binding(gir::BindingId id, SourceRange range = {}) {
                if (!id.valid() || id.value >= graph_.bindings.size())
                    backend(range, "invalid graph-IR Binding handle in planning");
                return graph_.bindings[id.value];
            }
            const gir::Statement &planned_statement(gir::StatementId id, SourceRange range = {}) {
                if (!id.valid() || id.value >= graph_.statements.size())
                    backend(range, "invalid graph-IR Statement handle in planning");
                return graph_.statements[id.value];
            }
            const gir::Block &planned_block(gir::BlockId id, SourceRange range = {}) {
                if (!id.valid() || id.value >= graph_.blocks.size()) backend(range, "invalid graph-IR Block handle in planning");
                return graph_.blocks[id.value];
            }
            const gir::Type &graph_type(gir::TypeId id, SourceRange range = {}) {
                if (!id.valid() || id.value >= graph_.types.size()) backend(range, "invalid graph-IR Type handle in planning");
                return graph_.types[id.value];
            }
            [[nodiscard]] std::optional<std::size_t>  runtime_parameter(gir::ValueId id, gir::CallableId callable_id);
            [[nodiscard]] std::optional<std::size_t>  runtime_root_parameter(gir::ValueId id, gir::CallableId callable_id);
            [[nodiscard]] std::optional<std::string>  runtime_scalar_key(gir::ValueId id, gir::CallableId callable_id);
            [[nodiscard]] std::optional<std::int64_t> runtime_integer_constant(gir::ValueId id, gir::CallableId callable_id);
            [[nodiscard]] std::optional<std::string>  runtime_selector_key(gir::ValueId id, gir::CallableId callable_id);
            using RuntimeValidSet = std::unordered_set<std::string>;
            [[nodiscard]] bool            runtime_top_level_selector_present(gir::ValueId id, gir::CallableId callable_id,
                                                                             std::string_view name);
            void                          add_all_runtime_parameters(gir::CallableId callable_id, RuntimeInfo &info);
            [[nodiscard]] RuntimeValidSet add_all_runtime_valid(gir::CallableId callable_id, RuntimeValidSet valid);
            void collect_runtime_activation(gir::ValueId id, gir::CallableId callable_id, RuntimeInfo &info);
            void check_runtime_expr(gir::ValueId id, gir::CallableId callable_id, const RuntimeValidSet &valid);
            void check_runtime_selector(gir::ValueId id, gir::CallableId callable_id, const RuntimeValidSet &valid);
            [[nodiscard]] RuntimeValidSet runtime_true_valid(gir::ValueId id, gir::CallableId callable_id,
                                                             const RuntimeValidSet &valid);
            void check_runtime_block(gir::BlockId id, gir::CallableId callable_id, const RuntimeValidSet &valid,
                                     bool allow_when = false);
            void check_runtime_stmt(gir::StatementId id, gir::CallableId callable_id, const RuntimeValidSet &valid, bool allow_when,
                                    SourceRange fallback);
            RuntimeInfo     runtime_info(gir::CallableId decl);
            gir::CallableId current_callable_{};
            void            validate_traversal(gir::ValueId id) {
                const auto &iterator = planned_value(id, {});
                if (iterator.phase != hir::Phase::Wiring) return;
                const auto *call = std::get_if<gir::Call>(&iterator.node);
                if (!call || call->arguments.empty()) return;
                const auto &collection = planned_value(call->arguments.front().value, iterator.range);
                const auto &type       = graph_type(collection.type, collection.range);
                if (type.kind != hir::TypeKind::List || type.unbounded || !type.size.valid()) return;
                const auto &extent = graph_.const_exprs.at(type.size.value);
                if (extent.literal && std::holds_alternative<std::int64_t>(*extent.literal)) return;
                bool materialized = false;
                for (const auto &instance : graph_.materializations) {
                    if (instance.implementation != current_callable_) continue;
                    const auto bound =
                        std::ranges::find(instance.substitutions, extent.parameter_binding, &gir::Substitution::parameter);
                    if (bound == instance.substitutions.end() || bound->retained) {
                        backend(
                            iterator.range,
                            "graph traversal requires a concrete fixed-list extent; a retained generic extent is not supported");
                    }
                    materialized = true;
                }
                if (!materialized)
                    backend(iterator.range,
                            "graph traversal requires a concrete fixed-list extent; a retained generic extent is not supported");
            }
            struct PlannedCalls
            {
                std::set<std::uint32_t> calls{};
                std::set<std::uint32_t> values{};
                std::set<std::uint32_t> blocks{};
            };
            void                                  collect_calls(gir::ValueId id, PlannedCalls &calls, SourceRange fallback = {});
            void                                  collect_calls(gir::BlockId id, PlannedCalls &calls, SourceRange fallback = {});
            std::vector<gir::CallableId>          order_callables();
            const std::vector<gir::Substitution> *substitutions_{};
            gir::Module                          &graph_;
            syntax::DiagnosticSink               &diagnostics_;
        };
        std::optional<std::size_t> Planner::runtime_parameter(gir::ValueId id, gir::CallableId decl) {
            (void)planned_value(id, callable(decl).range);
            return temporal_parameter(graph_, id, decl);
        }

        std::optional<std::size_t> Planner::runtime_root_parameter(gir::ValueId id, gir::CallableId decl) {
            if (const std::optional<std::size_t> parameter = runtime_parameter(id, decl)) { return parameter; }
            const gir::Value &expression = planned_value(id, callable(decl).range);
            if (const auto *index = std::get_if<gir::Index>(&expression.node)) {
                return runtime_root_parameter(index->target, decl);
            }
            if (const auto *field = std::get_if<gir::Field>(&expression.node)) {
                return runtime_root_parameter(field->target, decl);
            }
            if (const auto *call = std::get_if<gir::Call>(&expression.node); call && call->arguments.size() == 1) {
                const auto *ref = std::get_if<gir::Reference>(&planned_value(call->callee, expression.range).node);
                if (ref && ref->kind == gir::ReferenceKind::Intrinsic && ref->registry_name == "key_set") {
                    return runtime_root_parameter(call->arguments.front().value, decl);
                }
            }
            return std::nullopt;
        }

        std::optional<std::string> Planner::runtime_scalar_key(gir::ValueId id, gir::CallableId decl) {
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

        std::optional<std::int64_t> Planner::runtime_integer_constant(gir::ValueId id, gir::CallableId decl) {
            const gir::Value &expression = planned_value(id, callable(decl).range);
            if (!expression.constant) { return std::nullopt; }
            if (const auto *integer = std::get_if<std::int64_t>(&*expression.constant)) { return *integer; }
            return std::nullopt;
        }

        std::optional<std::string> Planner::runtime_selector_key(gir::ValueId id, gir::CallableId decl) {
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

        bool Planner::runtime_top_level_selector_present(gir::ValueId id, gir::CallableId decl, std::string_view name) {
            if (!id.valid()) { return false; }
            const gir::Value &expression = planned_value(id, callable(decl).range);
            if (const auto *call = std::get_if<gir::Call>(&expression.node)) {
                const gir::Value     &callee    = planned_value(call->callee, expression.range);
                const gir::Reference *reference = std::get_if<gir::Reference>(&callee.node);
                return reference != nullptr && reference->kind == gir::ReferenceKind::Intrinsic && reference->registry_name == name;
            }
            const auto *binary = std::get_if<gir::Binary>(&expression.node);
            return binary != nullptr && binary->op == ir::hir::BinaryOp::And &&
                   (runtime_top_level_selector_present(binary->lhs, decl, name) ||
                    runtime_top_level_selector_present(binary->rhs, decl, name));
        }

        void Planner::add_all_runtime_parameters(gir::CallableId decl, RuntimeInfo &info) {
            const gir::Callable &planned = callable(decl);
            for (std::size_t index = 0; index < planned.parameters.size(); ++index) {
                if (!planned.parameters[index].is_const) {
                    info.active_parameters.insert(index);
                    info.value_active_parameters.insert(index);
                }
            }
        }

        Planner::RuntimeValidSet Planner::add_all_runtime_valid(gir::CallableId decl, RuntimeValidSet valid) {
            const gir::Callable &planned = callable(decl);
            for (std::size_t index = 0; index < planned.parameters.size(); ++index) {
                if (!planned.parameters[index].is_const) { valid.insert("parameter:" + std::to_string(index)); }
            }
            return valid;
        }

        void Planner::collect_runtime_activation(gir::ValueId id, gir::CallableId decl, RuntimeInfo &info) {
            const gir::Value &expression = planned_value(id, callable(decl).range);
            std::visit(
                [&](const auto &node) {
                    using T = std::decay_t<decltype(node)>;
                    if constexpr (std::is_same_v<T, gir::Call>) {
                        const gir::Value     &callee    = planned_value(node.callee, expression.range);
                        const gir::Reference *reference = std::get_if<gir::Reference>(&callee.node);
                        if (reference != nullptr && reference->kind == gir::ReferenceKind::Intrinsic &&
                            reference->registry_name == "modified") {
                            if (node.arguments.empty()) {
                                add_all_runtime_parameters(decl, info);
                                return;
                            }
                            for (const gir::Argument &argument : node.arguments) {
                                const std::optional<std::size_t> parameter = runtime_root_parameter(argument.value, decl);
                                if (!parameter) {
                                    backend(argument.range,
                                            "a generated runtime node requires 'modified' arguments to select a temporal input");
                                }
                                info.active_parameters.insert(*parameter);
                                const auto &source     = planned_value(argument.value, argument.range);
                                const auto *projection = std::get_if<gir::Call>(&source.node);
                                const auto *projection_callee =
                                    projection ? std::get_if<gir::Reference>(&planned_value(projection->callee, source.range).node)
                                               : nullptr;
                                if (projection_callee && projection_callee->kind == gir::ReferenceKind::Intrinsic &&
                                    projection_callee->registry_name == "key_set") {
                                    info.structural_parameters.insert(*parameter);
                                } else {
                                    info.value_active_parameters.insert(*parameter);
                                }
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

        void Planner::check_runtime_expr(gir::ValueId id, gir::CallableId decl, const RuntimeValidSet &valid) {
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
                            name == "last_modified_time" || name == "schemas" || name == "passivate" || name == "activate") {
                            // Metadata intrinsics inspect endpoint selectors; they do not read
                            // payloads. Input activity acts on the endpoint too (ADR 0010).
                            for (const gir::Argument &argument : node.arguments) {
                                check_runtime_selector(argument.value, decl, valid);
                            }
                            return;
                        }
                        check_runtime_expr(node.callee, decl, valid);
                        if (expression.operation.native_function.valid()) {
                            const auto       &target = native_function(expression.operation.native_function, expression.range);
                            std::vector<bool> bound(target.parameters.size(), false);
                            std::size_t       next = 0;
                            for (const gir::Argument &argument : node.arguments) {
                                while (next < bound.size() && bound[next]) { ++next; }
                                const auto index =
                                    argument.name.empty()
                                        ? next
                                        : static_cast<std::size_t>(
                                              std::ranges::find(target.parameters, argument.name, &gir::NativeParameter::name) -
                                              target.parameters.begin());
                                if (index >= bound.size() || bound[index]) {
                                    backend(argument.range, "invalid native argument binding in runtime validity analysis");
                                }
                                bound[index]          = true;
                                const auto &parameter = target.parameters[index];
                                if (parameter.access == hir::NativeParameterAccess::InputView &&
                                    graph_type(parameter.type, argument.range).kind == hir::TypeKind::Signal) {
                                    // A signal projection passes the endpoint, not its payload. Native
                                    // metadata queries must work before validity, just like the intrinsics.
                                    // Still validate selector indices; scalar/typed-view reads remain guarded.
                                    check_runtime_selector(argument.value, decl, valid);
                                } else {
                                    check_runtime_expr(argument.value, decl, valid);
                                }
                            }
                            return;
                        }
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
                        check_runtime_expr(node.condition, decl, valid);
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

        void Planner::check_runtime_selector(gir::ValueId id, gir::CallableId decl, const RuntimeValidSet &valid) {
            const gir::Value &expression = planned_value(id, callable(decl).range);
            if (const auto *index = std::get_if<gir::Index>(&expression.node)) {
                check_runtime_selector(index->target, decl, valid);
                check_runtime_expr(index->index, decl, valid);
                const gir::Type target = graph_type(planned_value(index->target, expression.range).type, expression.range);
                if (target.kind != hir::TypeKind::List || target.unbounded || !target.size.valid()) {
                    backend(expression.range, "safe runtime indexing currently requires a fixed-size list input");
                }
                const auto &extent   = graph_.const_exprs.at(target.size.value);
                const auto *concrete = extent.literal ? std::get_if<std::int64_t>(&*extent.literal) : nullptr;
                if (!concrete && substitutions_ && extent.parameter_binding.valid()) {
                    for (const auto &substitution : *substitutions_) {
                        if (substitution.parameter != extent.parameter_binding || substitution.retained) continue;
                        if (substitution.constant) concrete = std::get_if<std::int64_t>(&*substitution.constant);
                        if (!concrete && substitution.value.valid()) {
                            const auto &value = graph_.const_exprs.at(substitution.value.value);
                            if (value.literal) concrete = std::get_if<std::int64_t>(&*value.literal);
                        }
                    }
                }
                if (!concrete) {
                    backend(
                        expression.range,
                        "safe runtime indexing requires a concrete fixed-list extent; a retained generic extent is not supported");
                }
                const gir::Type *element = &graph_type(target.children.at(0), expression.range);
                if (element->kind == hir::TypeKind::Symbol && element->binding.valid() && substitutions_) {
                    for (const auto &substitution : *substitutions_) {
                        if (substitution.parameter == element->binding && substitution.type.valid()) {
                            element = &graph_type(substitution.type, expression.range);
                            break;
                        }
                    }
                }
                if (element->kind != hir::TypeKind::Scalar && element->kind != hir::TypeKind::Reference) {
                    backend(expression.range, "runtime indexing currently selects scalar or reference elements from a list input");
                }
                const std::int64_t size = *concrete;
                if (const std::optional<std::int64_t> literal = runtime_integer_constant(index->index, decl)) {
                    if (*literal < 0 || *literal >= size) {
                        fail(Category::Type, planned_value(index->index, expression.range).range,
                             "a fixed-list index is outside its valid range");
                    }
                    return;
                }
                const std::optional<std::string> subscript = runtime_scalar_key(index->index, decl);
                if (!subscript || !valid.contains("nonnegative:" + *subscript) ||
                    !valid.contains("below:" + *subscript + ":" + std::to_string(size))) {
                    fail(Category::Type, planned_value(index->index, expression.range).range,
                         "a dynamic fixed-list index must be guarded by 'index >= 0 && index < size'");
                }
            } else if (const auto *field = std::get_if<gir::Field>(&expression.node)) {
                check_runtime_selector(field->target, decl, valid);
            } else if (const auto *call = std::get_if<gir::Call>(&expression.node); call) {
                const auto *ref = std::get_if<gir::Reference>(&planned_value(call->callee, expression.range).node);
                if (ref && ref->kind == gir::ReferenceKind::Intrinsic && ref->registry_name == "key_set" &&
                    call->arguments.size() == 1) {
                    check_runtime_selector(call->arguments.front().value, decl, valid);
                } else if (ref && ref->kind == gir::ReferenceKind::Intrinsic &&
                           (ref->registry_name == "at" || ref->registry_name == "front" || ref->registry_name == "back")) {
                    for (const auto &argument : call->arguments) { check_runtime_expr(argument.value, decl, valid); }
                }
            }
        }

        Planner::RuntimeValidSet Planner::runtime_true_valid(gir::ValueId id, gir::CallableId decl, const RuntimeValidSet &valid) {
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
                    if (call->arguments.empty()) { return add_all_runtime_valid(decl, std::move(result)); }
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

        void Planner::check_runtime_stmt(gir::StatementId id, gir::CallableId decl, const RuntimeValidSet &valid, bool allow_when,
                                         SourceRange fallback) {
            const gir::Statement &statement = planned_statement(id, fallback);
            std::visit(
                [&](const auto &node) {
                    using T = std::decay_t<decltype(node)>;
                    if constexpr (std::is_same_v<T, gir::LocalBinding>) {
                        if (node.init.valid()) { check_runtime_expr(node.init, decl, valid); }
                    } else if constexpr (std::is_same_v<T, gir::Activation>) {
                        if (!allow_when) { backend(statement.range, "a 'when' block must be at function top level"); }
                        const bool has_valid =
                            node.condition.valid() && (runtime_top_level_selector_present(node.condition, decl, "valid") ||
                                                       runtime_top_level_selector_present(node.condition, decl, "all_valid"));
                        RuntimeValidSet body_valid = has_valid ? valid : add_all_runtime_valid(decl, valid);
                        if (node.condition.valid()) {
                            check_runtime_expr(node.condition, decl, body_valid);
                            body_valid = runtime_true_valid(node.condition, decl, body_valid);
                        }
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

        void Planner::check_runtime_block(gir::BlockId id, gir::CallableId decl, const RuntimeValidSet &valid, bool allow_when) {
            const gir::Block &block = planned_block(id, callable(decl).range);
            for (gir::StatementId statement : block.statements) {
                check_runtime_stmt(statement, decl, valid, allow_when, block.range);
            }
            if (block.tail.valid()) { check_runtime_expr(block.tail, decl, valid); }
        }

        RuntimeInfo Planner::runtime_info(gir::CallableId decl) {
            const gir::Callable &planned = callable(decl);
            RuntimeInfo          info;

            if (planned.concise_body.valid() || !planned.block_body.valid()) {
                backend(planned.range, "a runtime function needs a block body");
            }
            if (graph_type(planned.result, planned.range).kind != hir::TypeKind::Void) {
                const gir::Type result = graph_type(planned.result, planned.range);
                if (result.kind != hir::TypeKind::Scalar && result.kind != hir::TypeKind::Symbol &&
                    result.kind != hir::TypeKind::Map && result.kind != hir::TypeKind::Set && result.kind != hir::TypeKind::List &&
                    result.kind != hir::TypeKind::Reference) {
                    backend(graph_type(planned.result, planned.range).range,
                            "the runtime-node slice supports scalar, struct, collection, and ref outputs");
                }
            }

            std::size_t temporal_count = 0;
            for (const gir::Parameter &parameter : planned.parameters) {
                const gir::Binding    &binding = planned_binding(parameter.binding, planned.range);
                const gir::BindingKind expected =
                    parameter.is_const ? gir::BindingKind::ConstParameter : gir::BindingKind::SignalParameter;
                if (binding.kind != expected) { backend(binding.range, "hgraph IR runtime parameter has the wrong binding kind"); }
                const gir::Type type               = graph_type(parameter.type, planned.range);
                const bool      unresolved_generic = type.kind == hir::TypeKind::Symbol && type.binding.valid();
                if (type.kind != hir::TypeKind::Scalar && type.kind != hir::TypeKind::Atomic && type.kind != hir::TypeKind::Map &&
                    type.kind != hir::TypeKind::Set && type.kind != hir::TypeKind::List && type.kind != hir::TypeKind::Rolling &&
                    type.kind != hir::TypeKind::Reference && type.kind != hir::TypeKind::Signal && !unresolved_generic) {
                    backend(graph_type(parameter.type, planned.range).range,
                            "the runtime-node slice supports scalar, atomic, collection, ref, and signal parameters");
                }
                if (!parameter.is_const) { ++temporal_count; }
            }
            std::unordered_set<std::uint32_t> injected_bindings;
            const gir::Block                 &body = planned_block(planned.block_body, planned.range);
            for (gir::StatementId id : body.statements) {
                const gir::Statement &statement = planned_statement(id, body.range);
                std::visit(
                    [&](const auto &node) {
                        using T = std::decay_t<decltype(node)>;
                        if constexpr (std::is_same_v<T, gir::StateBinding>) {
                            const gir::Binding &binding = planned_binding(node.binding, statement.range);
                            if (binding.kind != (node.cache ? gir::BindingKind::Cache : gir::BindingKind::State)) {
                                backend(statement.range, "hgraph IR state statement refers to a non-state binding");
                            }
                            const gir::Type type = graph_type(node.type, statement.range);
                            if (type.kind != hir::TypeKind::Scalar) {
                                backend(graph_type(node.type, statement.range).range,
                                        "the first runtime-node slice supports scalar state fields");
                            }
                            if (!node.init.valid()) {
                                backend(binding.range, "a generated runtime state field needs an initializer");
                            }
                            (node.cache ? info.caches : info.states)
                                .push_back(RuntimeState{.binding = node.binding,
                                                        .name    = binding.name,
                                                        .type    = node.type,
                                                        .init    = node.init,
                                                        .range   = binding.range});
                        } else if constexpr (std::is_same_v<T, gir::Inject>) {
                            for (gir::BindingId binding_id : node.bindings) {
                                const gir::Binding &binding = planned_binding(binding_id, statement.range);
                                if (binding.kind != gir::BindingKind::Capability) {
                                    backend(binding.range, "hgraph IR inject statement refers to a non-capability binding");
                                }
                                if (!injected_bindings.insert(binding_id.value).second) {
                                    backend(binding.range, "hgraph IR runtime body repeats an injected capability binding");
                                }
                                const bool declared =
                                    std::ranges::any_of(planned.capabilities, [&](const gir::Capability &capability) {
                                        return capability.binding == binding_id;
                                    });
                                if (!declared) {
                                    backend(binding.range, "hgraph IR inject binding is absent from the callable capabilities");
                                }
                                if (binding.name == "out") {
                                    if (info.out_binding.valid()) {
                                        backend(binding.range, "runtime function injects 'out' more than once");
                                    }
                                    info.out_binding = binding_id;
                                } else if (binding.name == "logger") {
                                    if (info.logger_binding.valid()) {
                                        backend(binding.range, "runtime function injects 'logger' more than once");
                                    }
                                    info.logger_binding = binding_id;
                                } else if (binding.name == "clock") {
                                    if (info.clock_binding.valid()) {
                                        backend(binding.range, "runtime function injects 'clock' more than once");
                                    }
                                    info.clock_binding = binding_id;
                                } else if (binding.name == "scheduler") {
                                    if (info.scheduler_binding.valid()) {
                                        backend(binding.range, "runtime function injects 'scheduler' more than once");
                                    }
                                    info.scheduler_binding = binding_id;
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
                            info.activations.emplace(
                                id.value,
                                RuntimeActivation{node.condition.valid() &&
                                                      (runtime_top_level_selector_present(node.condition, decl, "modified") ||
                                                       runtime_top_level_selector_present(node.condition, decl, "scheduled")),
                                                  node.condition.valid() &&
                                                      (runtime_top_level_selector_present(node.condition, decl, "valid") ||
                                                       runtime_top_level_selector_present(node.condition, decl, "all_valid"))});
                            const bool scheduled =
                                node.condition.valid() && runtime_top_level_selector_present(node.condition, decl, "scheduled");
                            if (scheduled) { info.uses_scheduled = true; }
                            if (!node.condition.valid() ||
                                (!scheduled && !runtime_top_level_selector_present(node.condition, decl, "modified"))) {
                                add_all_runtime_parameters(decl, info);
                            } else {
                                collect_runtime_activation(node.condition, decl, info);
                            }
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
            if (info.uses_scheduled && !info.scheduler_binding.valid()) {
                backend(planned.range, "typed HIR admitted 'scheduled()' without 'inject scheduler'");
            }
            if (!info.caches.empty() && !info.states.empty()) {
                fail(Category::Type, info.caches.front().range,
                     "'cache' and 'state' cannot be combined in one runtime function yet: native static nodes reject "
                     "State together with RecordableState");
            }
            if (temporal_count == 0 && !info.scheduler_binding.valid()) {
                // A source with nothing to activate it never evaluates (ADR 0010).
                backend(planned.range, "typed HIR admitted a runtime source without the scheduler capability");
            }
            if (temporal_count != 0 && info.has_when && info.active_parameters.empty() && !info.uses_scheduled) {
                backend(planned.range, "a generated runtime function with 'when' needs a temporal parameter in 'modified(...)'");
            }
            if (!info.has_when) { add_all_runtime_parameters(decl, info); }
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
        void Planner::collect_calls(gir::ValueId id, PlannedCalls &calls, SourceRange fallback) {
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
            if (value.operation.candidate.valid()) {
                (void)callable(value.operation.candidate, value.range);
                calls.calls.insert(value.operation.candidate.value);
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

        void Planner::collect_calls(gir::BlockId id, PlannedCalls &calls, SourceRange fallback) {
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
                            if (node.condition.valid()) { collect_calls(node.condition, calls, statement.range); }
                            collect_calls(node.block, calls, statement.range);
                        } else if constexpr (std::is_same_v<T, gir::Traversal>) {
                            validate_traversal(node.iterable);
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

        std::vector<gir::CallableId> Planner::order_callables() {
            std::vector<gir::CallableId> internal;
            for (std::uint32_t index = 0; index < graph_.callables.size(); ++index) { internal.push_back(gir::CallableId{index}); }
            std::map<std::uint32_t, std::set<std::uint32_t>> deps;
            for (const gir::CallableId id : internal) {
                current_callable_       = id;
                const gir::Callable &fn = callable(id);
                PlannedCalls         calls;
                if (fn.concise_body.valid() == fn.block_body.valid()) {
                    backend(fn.range,
                            "hgraph IR callable '" + callable(id).identity + "' must have exactly one concise or block body");
                }
                if (fn.concise_body.valid()) {
                    collect_calls(fn.concise_body, calls, fn.range);
                } else {
                    collect_calls(fn.block_body, calls, fn.range);
                }
                for (const std::uint32_t call_id : calls.calls) {
                    const gir::CallableId call{call_id};
                    static_cast<void>(callable(call, fn.range));
                }
                deps[id.value] = std::move(calls.calls);
            }
            std::vector<gir::CallableId>         ordered;
            std::set<std::uint32_t>              done;
            std::set<std::uint32_t>              visiting;
            std::function<void(gir::CallableId)> visit = [&](gir::CallableId id) {
                if (done.contains(id.value)) { return; }
                if (visiting.contains(id.value)) {
                    backend(callable(id).range,
                            "'" + callable(id).identity + "' is recursive; recursive functions are not supported");
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
    }  // namespace
    std::optional<std::size_t> temporal_parameter(const Module &module, ValueId id, CallableId decl) {
        if (!id.valid() || id.value >= module.values.size() || !decl.valid() || decl.value >= module.callables.size())
            return std::nullopt;
        const auto *reference = std::get_if<Reference>(&module.values[id.value].node);
        if (!reference || reference->kind != ReferenceKind::Binding) return std::nullopt;
        const auto &parameters = module.callables[decl.value].parameters;
        for (std::size_t index = 0; index < parameters.size(); ++index) {
            if (parameters[index].binding == reference->binding && !parameters[index].is_const) return index;
        }
        return std::nullopt;
    }

    void plan(Module &module, syntax::DiagnosticSink &diagnostics) { Planner{module, diagnostics}.run(); }
}  // namespace hgl::hgraph_ir
