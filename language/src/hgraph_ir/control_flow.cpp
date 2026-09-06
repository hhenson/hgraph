#include "hgraph_ir/control_flow.h"

#include <algorithm>
#include <span>
#include <type_traits>
#include <utility>
#include <variant>

namespace hgl::hgraph_ir
{
    namespace
    {
        template <typename T> [[nodiscard]] bool contains(const std::vector<T> &values, T value) {
            return std::ranges::find(values, value) != values.end();
        }

        class BranchAnalyzer
        {
          public:
            BranchAnalyzer(const Module &module, BlockId block, std::span<const BindingId> additional_locals = {})
                : module_{module} {
                plan_.block = block;
                for (BindingId binding : additional_locals) { add_local(binding); }
                collect_locals(block);
                scan_block(block);
            }

            [[nodiscard]] ConditionalBranchPlan take() && { return std::move(plan_); }

          private:
            [[nodiscard]] const Value     &value(ValueId id) const { return module_.values.at(id.value); }
            [[nodiscard]] const Statement &statement(StatementId id) const { return module_.statements.at(id.value); }
            [[nodiscard]] const Block     &block(BlockId id) const { return module_.blocks.at(id.value); }

            void add_local(BindingId binding) {
                if (binding.valid() && !contains(locals_, binding)) { locals_.push_back(binding); }
            }

            void collect_value_locals(ValueId id) {
                if (!id.valid()) { return; }
                const Value &expression = value(id);
                std::visit(
                    [&](const auto &node) {
                        using T = std::decay_t<decltype(node)>;
                        if constexpr (std::is_same_v<T, Unary>) {
                            collect_value_locals(node.operand);
                        } else if constexpr (std::is_same_v<T, Binary>) {
                            collect_value_locals(node.lhs);
                            collect_value_locals(node.rhs);
                        } else if constexpr (std::is_same_v<T, Call> || std::is_same_v<T, HarnessEval>) {
                            collect_value_locals(node.callee);
                            for (const Argument &argument : node.arguments) { collect_value_locals(argument.value); }
                        } else if constexpr (std::is_same_v<T, Index>) {
                            collect_value_locals(node.target);
                            collect_value_locals(node.index);
                        } else if constexpr (std::is_same_v<T, Field>) {
                            collect_value_locals(node.target);
                        } else if constexpr (std::is_same_v<T, Sequence>) {
                            for (const SequenceElement &element : node.elements) {
                                collect_value_locals(element.key);
                                collect_value_locals(element.value);
                            }
                        } else if constexpr (std::is_same_v<T, Tuple>) {
                            for (ValueId element : node.elements) { collect_value_locals(element); }
                        } else if constexpr (std::is_same_v<T, Lambda>) {
                            for (BindingId parameter : node.parameters) { add_local(parameter); }
                            collect_value_locals(node.body);
                        } else if constexpr (std::is_same_v<T, Conditional>) {
                            collect_locals(node.then_block);
                            collect_value_locals(node.otherwise);
                        } else if constexpr (std::is_same_v<T, BlockValue>) {
                            collect_locals(node.block);
                        } else if constexpr (std::is_same_v<T, Construct>) {
                            for (const Argument &argument : node.arguments) { collect_value_locals(argument.value); }
                        }
                    },
                    expression.node);
            }

            void collect_locals(BlockId id) {
                if (!id.valid()) { return; }
                const Block &body = block(id);
                for (StatementId statement_id : body.statements) {
                    const Statement &item = statement(statement_id);
                    std::visit(
                        [&](const auto &node) {
                            using T = std::decay_t<decltype(node)>;
                            if constexpr (std::is_same_v<T, LocalBinding> || std::is_same_v<T, StateBinding>) {
                                add_local(node.binding);
                                collect_value_locals(node.init);
                            } else if constexpr (std::is_same_v<T, Inject>) {
                                for (BindingId binding : node.bindings) { add_local(binding); }
                            } else if constexpr (std::is_same_v<T, Lifecycle>) {
                                collect_locals(node.block);
                            } else if constexpr (std::is_same_v<T, Activation>) {
                                collect_value_locals(node.condition);
                                collect_locals(node.block);
                            } else if constexpr (std::is_same_v<T, Traversal>) {
                                for (BindingId binding : node.bindings) { add_local(binding); }
                                collect_value_locals(node.iterable);
                                collect_locals(node.block);
                            } else if constexpr (std::is_same_v<T, Assignment>) {
                                collect_value_locals(node.place);
                                collect_value_locals(node.value);
                            } else if constexpr (std::is_same_v<T, Return>) {
                                collect_value_locals(node.value);
                            } else if constexpr (std::is_same_v<T, Assert>) {
                                collect_value_locals(node.condition);
                            } else if constexpr (std::is_same_v<T, Evaluate>) {
                                collect_value_locals(node.value);
                            }
                        },
                        item.node);
                }
                collect_value_locals(body.tail);
            }

            void capture(const Value &expression, BindingId binding) {
                if (!binding.valid() || contains(locals_, binding) || contains(defined_outer_, binding)) { return; }
                const auto existing = std::ranges::find_if(
                    plan_.captures, [&](const ConditionalCapture &candidate) { return candidate.binding == binding; });
                if (existing == plan_.captures.end()) {
                    const TypeId type =
                        binding.value < module_.bindings.size() ? module_.bindings[binding.value].type : expression.type;
                    plan_.captures.push_back(ConditionalCapture{binding, type, expression.phase});
                }
            }

            void scan_value(ValueId id) {
                if (!id.valid()) { return; }
                const Value &expression = value(id);
                std::visit(
                    [&](const auto &node) {
                        using T = std::decay_t<decltype(node)>;
                        if constexpr (std::is_same_v<T, Reference>) {
                            if (node.kind == ReferenceKind::Binding) { capture(expression, node.binding); }
                        } else if constexpr (std::is_same_v<T, Unary>) {
                            scan_value(node.operand);
                        } else if constexpr (std::is_same_v<T, Binary>) {
                            scan_value(node.lhs);
                            scan_value(node.rhs);
                        } else if constexpr (std::is_same_v<T, Call>) {
                            scan_value(node.callee);
                            for (const Argument &argument : node.arguments) { scan_value(argument.value); }
                        } else if constexpr (std::is_same_v<T, Index>) {
                            scan_value(node.target);
                            scan_value(node.index);
                        } else if constexpr (std::is_same_v<T, Field>) {
                            scan_value(node.target);
                        } else if constexpr (std::is_same_v<T, Sequence>) {
                            for (const SequenceElement &element : node.elements) {
                                scan_value(element.key);
                                scan_value(element.value);
                            }
                        } else if constexpr (std::is_same_v<T, Tuple>) {
                            for (ValueId element : node.elements) { scan_value(element); }
                        } else if constexpr (std::is_same_v<T, Lambda>) {
                            const auto defined = defined_outer_;
                            scan_value(node.body);
                            defined_outer_ = defined;
                        } else if constexpr (std::is_same_v<T, Conditional>) {
                            scan_value(node.condition);

                            const auto incoming = defined_outer_;
                            defined_outer_      = incoming;
                            scan_block(node.then_block);
                            const auto when_true = defined_outer_;

                            defined_outer_ = incoming;
                            if (node.otherwise.valid()) { scan_value(node.otherwise); }
                            const auto when_false = defined_outer_;

                            defined_outer_ = incoming;
                            for (BindingId binding : when_true) {
                                if (contains(when_false, binding) && !contains(defined_outer_, binding)) {
                                    defined_outer_.push_back(binding);
                                }
                            }
                        } else if constexpr (std::is_same_v<T, BlockValue>) {
                            scan_block(node.block);
                        } else if constexpr (std::is_same_v<T, HarnessEval>) {
                            scan_value(node.callee);
                            for (const Argument &argument : node.arguments) { scan_value(argument.value); }
                        } else if constexpr (std::is_same_v<T, Construct>) {
                            for (const Argument &argument : node.arguments) { scan_value(argument.value); }
                        }
                    },
                    expression.node);
            }

            [[nodiscard]] BindingId direct_assignment_target(ValueId id) const {
                if (!id.valid()) { return {}; }
                const auto *reference = std::get_if<Reference>(&value(id).node);
                return reference != nullptr && reference->kind == ReferenceKind::Binding ? reference->binding : BindingId{};
            }

            [[nodiscard]] BindingId place_root(ValueId id) const {
                if (!id.valid()) { return {}; }
                const Value &place = value(id);
                if (const auto *reference = std::get_if<Reference>(&place.node)) {
                    return reference->kind == ReferenceKind::Binding ? reference->binding : BindingId{};
                }
                if (const auto *index = std::get_if<Index>(&place.node)) { return place_root(index->target); }
                if (const auto *field = std::get_if<Field>(&place.node)) { return place_root(field->target); }
                return {};
            }

            void scan_block(BlockId id) {
                if (!id.valid()) { return; }
                const Block &body = block(id);
                for (StatementId statement_id : body.statements) {
                    const Statement &item = statement(statement_id);
                    std::visit(
                        [&](const auto &node) {
                            using T = std::decay_t<decltype(node)>;
                            if constexpr (std::is_same_v<T, LocalBinding> || std::is_same_v<T, StateBinding>) {
                                scan_value(node.init);
                            } else if constexpr (std::is_same_v<T, Lifecycle>) {
                                const auto defined = defined_outer_;
                                scan_block(node.block);
                                defined_outer_ = defined;
                            } else if constexpr (std::is_same_v<T, Activation>) {
                                scan_value(node.condition);
                                const auto defined = defined_outer_;
                                scan_block(node.block);
                                defined_outer_ = defined;
                            } else if constexpr (std::is_same_v<T, Traversal>) {
                                scan_value(node.iterable);
                                const auto defined = defined_outer_;
                                scan_block(node.block);
                                defined_outer_ = defined;
                            } else if constexpr (std::is_same_v<T, Assignment>) {
                                const BindingId target = place_root(node.place);
                                if (node.op != AssignOp::Assign || direct_assignment_target(node.place) != target) {
                                    scan_value(node.place);
                                }
                                scan_value(node.value);
                                if (target.valid() && !contains(locals_, target)) {
                                    if (!contains(plan_.assigned_outer, target)) { plan_.assigned_outer.push_back(target); }
                                    if (!contains(defined_outer_, target)) { defined_outer_.push_back(target); }
                                }
                            } else if constexpr (std::is_same_v<T, Return>) {
                                plan_.returns = true;
                                scan_value(node.value);
                            } else if constexpr (std::is_same_v<T, Assert>) {
                                scan_value(node.condition);
                            } else if constexpr (std::is_same_v<T, Evaluate>) {
                                scan_value(node.value);
                            }
                        },
                        item.node);
                }
                scan_value(body.tail);
            }

            const Module          &module_;
            ConditionalBranchPlan  plan_{};
            std::vector<BindingId> locals_{};
            std::vector<BindingId> defined_outer_{};
        };

        void append_capture(std::vector<ConditionalCapture> &captures, const ConditionalCapture &capture) {
            const bool exists = std::ranges::any_of(
                captures, [&](const ConditionalCapture &candidate) { return candidate.binding == capture.binding; });
            if (!exists) { captures.push_back(capture); }
        }
    }  // namespace

    ConditionalPlan analyze_temporal_conditional(const Module &module, ValueId value_id) {
        ConditionalPlan plan;
        plan.value = value_id;
        if (!value_id.valid() || value_id.value >= module.values.size()) { return plan; }

        const Value &value  = module.values[value_id.value];
        const auto  *branch = std::get_if<Conditional>(&value.node);
        if (branch == nullptr) { return plan; }

        plan.condition = branch->condition;
        plan.result    = value.type;
        plan.when_true = BranchAnalyzer{module, branch->then_block}.take();
        for (const ConditionalCapture &capture : plan.when_true.captures) { append_capture(plan.captures, capture); }
        for (BindingId binding : plan.when_true.assigned_outer) {
            if (!contains(plan.assigned_outer, binding)) { plan.assigned_outer.push_back(binding); }
        }

        plan.has_otherwise = branch->otherwise.valid();
        if (branch->otherwise.valid() && branch->otherwise.value < module.values.size()) {
            const Value &otherwise = module.values[branch->otherwise.value];
            if (const auto *block = std::get_if<BlockValue>(&otherwise.node)) {
                plan.when_false = BranchAnalyzer{module, block->block}.take();
                for (const ConditionalCapture &capture : plan.when_false->captures) { append_capture(plan.captures, capture); }
                for (BindingId binding : plan.when_false->assigned_outer) {
                    if (!contains(plan.assigned_outer, binding)) { plan.assigned_outer.push_back(binding); }
                }
            }
        }

        // A branch that does not assign an escaping binding must receive the
        // incoming connection in order to forward it. Add those implicit
        // inputs after the explicit read captures so both backends share one
        // stable parameter order. Definite-assignment analysis separately
        // rejects the case where no incoming binding exists.
        for (BindingId binding : plan.assigned_outer) {
            const bool true_forwards  = temporal_branch_forwards(plan, plan.when_true, binding);
            const bool false_forwards = temporal_branch_forwards(plan, plan.when_false.value_or(ConditionalBranchPlan{}), binding);
            if (!true_forwards && !false_forwards) { continue; }
            const auto existing = std::ranges::find_if(
                plan.captures, [&](const ConditionalCapture &candidate) { return candidate.binding == binding; });
            if (existing == plan.captures.end()) {
                plan.captures.push_back(ConditionalCapture{
                    .binding = binding,
                    .type    = module.bindings.at(binding.value).type,
                    .phase   = ir::hir::Phase::Wiring,
                });
            }
        }
        return plan;
    }

    bool temporal_branch_forwards(const ConditionalPlan &plan, const ConditionalBranchPlan &branch, BindingId binding) {
        return contains(plan.assigned_outer, binding) && !contains(branch.assigned_outer, binding);
    }

    std::vector<ConditionalResultSlot> plan_temporal_conditional_results(const Module &module, const ConditionalPlan &plan,
                                                                         bool expression_used) {
        std::vector<ConditionalResultSlot> results;
        results.reserve(plan.assigned_outer.size() + (expression_used ? 1U : 0U));

        const auto unique_name = [&](std::string base) {
            if (base.empty()) { base = "value"; }
            std::string candidate = base;
            std::size_t suffix    = 1U;
            while (std::ranges::any_of(results, [&](const ConditionalResultSlot &slot) { return slot.field_name == candidate; })) {
                candidate = base + "_" + std::to_string(suffix++);
            }
            return candidate;
        };

        if (expression_used && plan.result.valid() && plan.result.value < module.types.size() &&
            module.types[plan.result.value].kind != ir::hir::TypeKind::Void) {
            results.push_back(ConditionalResultSlot{
                .source     = ConditionalResultSource::Expression,
                .type       = plan.result,
                .field_name = unique_name("value"),
            });
        }

        for (BindingId binding : plan.assigned_outer) {
            const Binding &item = module.bindings.at(binding.value);
            results.push_back(ConditionalResultSlot{
                .source     = ConditionalResultSource::Binding,
                .type       = item.type,
                .binding    = binding,
                .field_name = unique_name(item.name),
            });
        }
        return results;
    }

    TraversalPlan analyze_traversal(const Module &module, const Traversal &traversal) {
        ConditionalBranchPlan nested = BranchAnalyzer{module, traversal.block, traversal.bindings}.take();
        return TraversalPlan{
            .block          = nested.block,
            .captures       = std::move(nested.captures),
            .assigned_outer = std::move(nested.assigned_outer),
            .returns        = nested.returns,
        };
    }
}  // namespace hgl::hgraph_ir
