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
            BranchAnalyzer(const Module &module, BlockId block, std::span<const BindingId> additional_locals = {},
                           std::optional<ConditionalContinuationPlan> continuation = std::nullopt)
                : module_{module} {
                plan_.block = block;
                for (BindingId binding : additional_locals) { add_local(binding); }
                collect_locals(block);
                if (continuation) { collect_locals(*continuation); }

                plan_.falls_through = scan_block(block);
                if (plan_.falls_through && continuation) {
                    plan_.continuation = std::move(continuation);
                    (void)scan_continuation(*plan_.continuation);
                    // The continuation is the complete remainder of the
                    // callable. Reaching its end supplies that callable's
                    // result (including the implicit result of a void body).
                    plan_.falls_through = false;
                }
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
                collect_locals(body.statements);
                collect_value_locals(body.tail);
            }

            void collect_locals(const ConditionalContinuationPlan &continuation) {
                collect_locals(continuation.statements);
                collect_value_locals(continuation.tail);
            }

            void collect_locals(std::span<const StatementId> statements) {
                for (StatementId statement_id : statements) {
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

            [[nodiscard]] bool scan_value(ValueId id) {
                if (!id.valid()) { return true; }
                const Value &expression = value(id);
                return std::visit(
                    [&](const auto &node) -> bool {
                        using T = std::decay_t<decltype(node)>;
                        if constexpr (std::is_same_v<T, Reference>) {
                            if (node.kind == ReferenceKind::Binding) { capture(expression, node.binding); }
                        } else if constexpr (std::is_same_v<T, Unary>) {
                            (void)scan_value(node.operand);
                        } else if constexpr (std::is_same_v<T, Binary>) {
                            (void)scan_value(node.lhs);
                            (void)scan_value(node.rhs);
                        } else if constexpr (std::is_same_v<T, Call>) {
                            (void)scan_value(node.callee);
                            for (const Argument &argument : node.arguments) { (void)scan_value(argument.value); }
                        } else if constexpr (std::is_same_v<T, Index>) {
                            (void)scan_value(node.target);
                            (void)scan_value(node.index);
                        } else if constexpr (std::is_same_v<T, Field>) {
                            (void)scan_value(node.target);
                        } else if constexpr (std::is_same_v<T, Sequence>) {
                            for (const SequenceElement &element : node.elements) {
                                (void)scan_value(element.key);
                                (void)scan_value(element.value);
                            }
                        } else if constexpr (std::is_same_v<T, Tuple>) {
                            for (ValueId element : node.elements) { (void)scan_value(element); }
                        } else if constexpr (std::is_same_v<T, Lambda>) {
                            const auto defined = defined_outer_;
                            (void)scan_value(node.body);
                            defined_outer_ = defined;
                        } else if constexpr (std::is_same_v<T, Conditional>) {
                            (void)scan_value(node.condition);

                            const auto incoming   = defined_outer_;
                            defined_outer_        = incoming;
                            const bool then_falls = scan_block(node.then_block);
                            const auto when_true  = defined_outer_;

                            defined_outer_             = incoming;
                            const bool otherwise_falls = node.otherwise.valid() ? scan_value(node.otherwise) : true;
                            const auto when_false      = defined_outer_;

                            defined_outer_ = incoming;
                            if (then_falls && otherwise_falls) {
                                for (BindingId binding : when_true) {
                                    if (contains(when_false, binding) && !contains(defined_outer_, binding)) {
                                        defined_outer_.push_back(binding);
                                    }
                                }
                            } else if (then_falls) {
                                defined_outer_ = when_true;
                            } else if (otherwise_falls) {
                                defined_outer_ = when_false;
                            }
                            return then_falls || otherwise_falls;
                        } else if constexpr (std::is_same_v<T, BlockValue>) {
                            return scan_block(node.block);
                        } else if constexpr (std::is_same_v<T, HarnessEval>) {
                            (void)scan_value(node.callee);
                            for (const Argument &argument : node.arguments) { (void)scan_value(argument.value); }
                        } else if constexpr (std::is_same_v<T, Construct>) {
                            for (const Argument &argument : node.arguments) { (void)scan_value(argument.value); }
                        }
                        return true;
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

            [[nodiscard]] bool scan_block(BlockId id) {
                if (!id.valid()) { return true; }
                const Block &body = block(id);
                if (!scan_statements(body.statements)) { return false; }
                return scan_value(body.tail);
            }

            [[nodiscard]] bool scan_continuation(const ConditionalContinuationPlan &continuation) {
                if (!scan_statements(continuation.statements)) { return false; }
                return scan_value(continuation.tail);
            }

            [[nodiscard]] bool scan_statements(std::span<const StatementId> statements) {
                for (StatementId statement_id : statements) {
                    const Statement &item          = statement(statement_id);
                    const bool       falls_through = std::visit(
                        [&](const auto &node) -> bool {
                            using T = std::decay_t<decltype(node)>;
                            if constexpr (std::is_same_v<T, LocalBinding> || std::is_same_v<T, StateBinding>) {
                                return scan_value(node.init);
                            } else if constexpr (std::is_same_v<T, Lifecycle>) {
                                const auto defined = defined_outer_;
                                (void)scan_block(node.block);
                                defined_outer_ = defined;
                            } else if constexpr (std::is_same_v<T, Activation>) {
                                (void)scan_value(node.condition);
                                const auto defined = defined_outer_;
                                (void)scan_block(node.block);
                                defined_outer_ = defined;
                            } else if constexpr (std::is_same_v<T, Traversal>) {
                                (void)scan_value(node.iterable);
                                const auto defined = defined_outer_;
                                (void)scan_block(node.block);
                                defined_outer_ = defined;
                            } else if constexpr (std::is_same_v<T, Assignment>) {
                                const BindingId target = place_root(node.place);
                                if (node.op != AssignOp::Assign || direct_assignment_target(node.place) != target) {
                                    (void)scan_value(node.place);
                                }
                                (void)scan_value(node.value);
                                if (target.valid() && !contains(locals_, target)) {
                                    if (!contains(plan_.assigned_outer, target)) { plan_.assigned_outer.push_back(target); }
                                    if (!contains(defined_outer_, target)) { defined_outer_.push_back(target); }
                                }
                            } else if constexpr (std::is_same_v<T, Return>) {
                                plan_.returns = true;
                                (void)scan_value(node.value);
                                return false;
                            } else if constexpr (std::is_same_v<T, Assert>) {
                                return scan_value(node.condition);
                            } else if constexpr (std::is_same_v<T, Evaluate>) {
                                return scan_value(node.value);
                            }
                            return true;
                        },
                        item.node);
                    if (!falls_through) { return false; }
                }
                return true;
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

    ConditionalContinuationPlan plan_temporal_continuation(const Module &module, BlockId enclosing, std::size_t first_statement,
                                                           TypeId result) {
        ConditionalContinuationPlan continuation;
        continuation.result = result;
        if (!enclosing.valid() || enclosing.value >= module.blocks.size()) { return continuation; }
        const Block &block = module.blocks[enclosing.value];
        const auto   first = std::min(first_statement, block.statements.size());
        continuation.statements.assign(block.statements.begin() + static_cast<std::ptrdiff_t>(first), block.statements.end());
        continuation.tail = block.tail;
        return continuation;
    }

    ConditionalPlan analyze_temporal_conditional(const Module &module, ValueId value_id,
                                                 std::optional<ConditionalContinuationPlan> continuation) {
        ConditionalPlan plan;
        plan.value = value_id;
        if (!value_id.valid() || value_id.value >= module.values.size()) { return plan; }

        const Value &value  = module.values[value_id.value];
        const auto  *branch = std::get_if<Conditional>(&value.node);
        if (branch == nullptr) { return plan; }

        plan.condition                        = branch->condition;
        plan.result                           = value.type;
        plan.when_true                        = BranchAnalyzer{module, branch->then_block}.take();
        bool                   branch_returns = plan.when_true.returns;
        std::optional<BlockId> otherwise_block;
        plan.has_otherwise = branch->otherwise.valid();
        if (branch->otherwise.valid() && branch->otherwise.value < module.values.size()) {
            const Value &otherwise = module.values[branch->otherwise.value];
            if (const auto *block = std::get_if<BlockValue>(&otherwise.node)) {
                otherwise_block = block->block;
                plan.when_false = BranchAnalyzer{module, *otherwise_block}.take();
                branch_returns  = branch_returns || plan.when_false->returns;
            }
        }

        // Once either temporal branch returns from its enclosing callable,
        // every path that can fall through must compose the callable suffix
        // in the selected child graph. Re-analyzing the complete path keeps
        // captures and assignments sequenced rather than unioning two
        // independent lexical analyses.
        if (continuation && branch_returns && (!plan.has_otherwise || otherwise_block)) {
            plan.result                = continuation->result;
            plan.when_true             = BranchAnalyzer{module, branch->then_block, {}, continuation}.take();
            plan.when_false            = BranchAnalyzer{module, otherwise_block.value_or(BlockId{}), {}, continuation}.take();
            plan.returns_from_callable = !plan.when_true.falls_through && !plan.when_false->falls_through;
        }

        for (const ConditionalCapture &capture : plan.when_true.captures) { append_capture(plan.captures, capture); }
        for (BindingId binding : plan.when_true.assigned_outer) {
            if (!contains(plan.assigned_outer, binding)) { plan.assigned_outer.push_back(binding); }
        }

        if (plan.when_false) {
            for (const ConditionalCapture &capture : plan.when_false->captures) { append_capture(plan.captures, capture); }
            for (BindingId binding : plan.when_false->assigned_outer) {
                if (!contains(plan.assigned_outer, binding)) { plan.assigned_outer.push_back(binding); }
            }
        }

        if (plan.returns_from_callable) {
            // All child paths terminate the callable, so assignments are
            // branch-local implementation details rather than outputs that
            // escape back into an enclosing continuation.
            plan.assigned_outer.clear();
            plan.when_true.assigned_outer.clear();
            plan.when_false->assigned_outer.clear();
            return plan;
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
        if (plan.returns_from_callable && plan.result.valid() && plan.result.value < module.types.size() &&
            module.types[plan.result.value].kind != ir::hir::TypeKind::Void) {
            results.push_back(ConditionalResultSlot{
                .source     = ConditionalResultSource::FunctionReturn,
                .type       = plan.result,
                .field_name = "value",
            });
            return results;
        }
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
