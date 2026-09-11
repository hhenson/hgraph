#include "hgraph_ir/control_flow.h"

#include <algorithm>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_set>
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
                for (const ConditionalContinuationSegment &segment : continuation.segments) {
                    collect_locals(segment.statements);
                    collect_value_locals(segment.tail);
                }
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
                            return scan_value(node.operand);
                        } else if constexpr (std::is_same_v<T, Binary>) {
                            return scan_value(node.lhs) && scan_value(node.rhs);
                        } else if constexpr (std::is_same_v<T, Call>) {
                            if (!scan_value(node.callee)) { return false; }
                            for (const Argument &argument : node.arguments) {
                                if (!scan_value(argument.value)) { return false; }
                            }
                        } else if constexpr (std::is_same_v<T, Index>) {
                            return scan_value(node.target) && scan_value(node.index);
                        } else if constexpr (std::is_same_v<T, Field>) {
                            return scan_value(node.target);
                        } else if constexpr (std::is_same_v<T, Sequence>) {
                            for (const SequenceElement &element : node.elements) {
                                if (!scan_value(element.key) || !scan_value(element.value)) { return false; }
                            }
                        } else if constexpr (std::is_same_v<T, Tuple>) {
                            for (ValueId element : node.elements) {
                                if (!scan_value(element)) { return false; }
                            }
                        } else if constexpr (std::is_same_v<T, Lambda>) {
                            const auto defined = defined_outer_;
                            (void)scan_value(node.body);
                            defined_outer_ = defined;
                        } else if constexpr (std::is_same_v<T, Conditional>) {
                            if (!scan_value(node.condition)) { return false; }

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
                            if (!scan_value(node.callee)) { return false; }
                            for (const Argument &argument : node.arguments) {
                                if (!scan_value(argument.value)) { return false; }
                            }
                        } else if constexpr (std::is_same_v<T, Construct>) {
                            for (const Argument &argument : node.arguments) {
                                if (!scan_value(argument.value)) { return false; }
                            }
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
                for (const ConditionalContinuationSegment &segment : continuation.segments) {
                    if (!scan_statements(segment.statements) || !scan_value(segment.tail)) { return false; }
                }
                return true;
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
                                if (!scan_value(node.condition)) { return false; }
                                const auto defined = defined_outer_;
                                (void)scan_block(node.block);
                                defined_outer_ = defined;
                            } else if constexpr (std::is_same_v<T, Traversal>) {
                                if (!scan_value(node.iterable)) { return false; }
                                const auto defined = defined_outer_;
                                (void)scan_block(node.block);
                                defined_outer_ = defined;
                            } else if constexpr (std::is_same_v<T, Assignment>) {
                                const BindingId target = place_root(node.place);
                                if (node.op != AssignOp::Assign || direct_assignment_target(node.place) != target) {
                                    if (!scan_value(node.place)) { return false; }
                                }
                                if (!scan_value(node.value)) { return false; }
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

        [[nodiscard]] syntax::SourceRange binding_range(const Module &module, BindingId binding, syntax::SourceRange fallback) {
            return binding.valid() && binding.value < module.bindings.size() ? module.bindings[binding.value].range : fallback;
        }

        [[nodiscard]] const Value *value_at(const Module &module, ValueId id) {
            return id.valid() && id.value < module.values.size() ? &module.values[id.value] : nullptr;
        }

        [[nodiscard]] const Type *type_at(const Module &module, TypeId id) {
            return id.valid() && id.value < module.types.size() ? &module.types[id.value] : nullptr;
        }

        [[nodiscard]] const Parameter *pack_parameter(const Module &module, const Value *value) {
            if (value == nullptr) { return nullptr; }
            const auto *reference = std::get_if<Reference>(&value->node);
            if (reference == nullptr || reference->kind != ReferenceKind::Binding || !reference->binding.valid()) {
                return nullptr;
            }
            for (const Callable &callable : module.callables) {
                const auto found = std::ranges::find(callable.parameters, reference->binding, &Parameter::binding);
                if (found != callable.parameters.end() && found->pack != ParameterPack::None) { return &*found; }
            }
            return nullptr;
        }

        /// The name an intrinsic call resolves to, or empty when the callee is
        /// not an intrinsic reference.
        [[nodiscard]] std::string_view intrinsic_name(const Module &module, ValueId callee) {
            const Value *target = value_at(module, callee);
            if (target == nullptr) { return {}; }
            const auto *reference = std::get_if<Reference>(&target->node);
            if (reference == nullptr || reference->kind != ReferenceKind::Intrinsic) { return {}; }
            return reference->registry_name.empty() ? reference->identity : reference->registry_name;
        }

        /// The first-pass rules a temporal conditional breaks. The message
        /// text is owned here; both backends only forward it.
        void record_conditional_issues(const Module &module, const Value &value, ConditionalPlan &plan) {
            if (plan.has_otherwise && !plan.when_false) {
                plan.issues.push_back(PlanIssue{
                    .range        = value.range,
                    .message      = "temporal 'else if' is not supported in this compiler stage; use a block 'else'",
                    .context_free = true,
                });
            }
            // Whether a branch return terminates the callable depends on the
            // continuation the backend supplies, so this rule stays with the plan.
            if (!plan.returns_from_callable && (plan.when_true.returns || (plan.when_false && plan.when_false->returns))) {
                plan.issues.push_back(PlanIssue{
                    .range        = value.range,
                    .message      = "return from a time-series 'if' branch is not supported in this compiler stage",
                    .context_free = false,
                });
            }
            for (const ConditionalCapture &capture : plan.captures) {
                if (capture.phase == ir::hir::Phase::Wiring) { continue; }
                plan.issues.push_back(PlanIssue{
                    .range = binding_range(module, capture.binding, value.range),
                    .message =
                        "capturing scalar configuration in a time-series 'if' branch is not supported in this compiler stage",
                    .context_free = true,
                });
            }
        }
    }  // namespace

    ConditionalContinuationPlan plan_temporal_continuation(const Module &module, BlockId enclosing, std::size_t first_statement,
                                                           TypeId result, ValueId conditional) {
        ConditionalContinuationPlan continuation;
        continuation.result = result;
        if (!enclosing.valid() || enclosing.value >= module.blocks.size()) { return continuation; }
        const Block                   &block = module.blocks[enclosing.value];
        const auto                     first = std::min(first_statement, block.statements.size());
        ConditionalContinuationSegment segment;
        segment.statements.assign(block.statements.begin() + static_cast<std::ptrdiff_t>(first), block.statements.end());
        if (block.tail != conditional) { segment.tail = block.tail; }
        if (!segment.statements.empty() || segment.tail.valid()) { continuation.segments.push_back(std::move(segment)); }
        return continuation;
    }

    ConditionalContinuationPlan prepend_temporal_continuation(const Module &module, BlockId enclosing, std::size_t first_statement,
                                                              ConditionalContinuationPlan following, ValueId conditional) {
        ConditionalContinuationPlan prefix =
            plan_temporal_continuation(module, enclosing, first_statement, following.result, conditional);
        if (prefix.segments.empty()) { return following; }
        return prepend_temporal_continuation(prefix.segments.front(), 0U, std::move(following), conditional);
    }

    ConditionalContinuationPlan prepend_temporal_continuation(const ConditionalContinuationSegment &enclosing,
                                                              std::size_t first_statement, ConditionalContinuationPlan following,
                                                              ValueId conditional) {
        const auto                     first = std::min(first_statement, enclosing.statements.size());
        ConditionalContinuationSegment segment;
        segment.statements.assign(enclosing.statements.begin() + static_cast<std::ptrdiff_t>(first), enclosing.statements.end());
        if (enclosing.tail != conditional) { segment.tail = enclosing.tail; }
        if (!segment.statements.empty() || segment.tail.valid()) {
            following.segments.insert(following.segments.begin(), std::move(segment));
        }
        return following;
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
            // escape back into an enclosing continuation. Retain each
            // branch's assignments so generated child compositions can
            // materialize their local bindings.
            plan.assigned_outer.clear();
            record_conditional_issues(module, value, plan);
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
        record_conditional_issues(module, value, plan);
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

    TraversalPlan analyze_traversal(const Module &module, const Traversal &traversal, syntax::SourceRange range) {
        ConditionalBranchPlan nested = BranchAnalyzer{module, traversal.block, traversal.bindings}.take();
        TraversalPlan         plan{
            .block          = nested.block,
            .captures       = std::move(nested.captures),
            .assigned_outer = std::move(nested.assigned_outer),
            .returns        = nested.returns,
        };
        if (range.begin == range.end && traversal.block.valid() && traversal.block.value < module.blocks.size()) {
            range = module.blocks[traversal.block.value].range;
        }
        const auto issue = [&](syntax::SourceRange at, std::string message) {
            plan.issues.push_back(PlanIssue{.range = at, .message = std::move(message), .context_free = true});
        };
        if (!plan.assigned_outer.empty()) { issue(range, "assignment escaping a graph 'for' body is not defined yet"); }
        if (plan.returns) { issue(range, "return from a graph 'for' body is not defined yet"); }

        // The iterator forms the first pass defines for a graph-phase loop:
        // one-argument values(...) over a temporal map, or elements(...) or
        // items(...) over a temporal list.
        // Runtime loops (a traversal inside node evaluation) are unrestricted.
        const Value *iterable = value_at(module, traversal.iterable);
        if (iterable == nullptr || iterable->phase != ir::hir::Phase::Wiring) { return plan; }
        bool dynamic = true;
        if (const auto *call = std::get_if<Call>(&iterable->node)) {
            const std::string_view name = intrinsic_name(module, call->callee);
            if (name == "keys" || name == "values" || name == "elements" || name == "items") {
                if (call->arguments.size() != 1U) {
                    issue(iterable->range, "graph-phase iterator predicates are not defined yet");
                } else {
                    const Value     *source     = value_at(module, call->arguments.front().value);
                    const Type      *collection = source != nullptr ? type_at(module, source->type) : nullptr;
                    const Parameter *pack       = pack_parameter(module, source);
                    if (pack != nullptr) {
                        dynamic = false;
                    } else if (collection == nullptr ||
                               (collection->kind != ir::hir::TypeKind::List && collection->kind != ir::hir::TypeKind::Map)) {
                        issue(iterable->range, "graph-phase iteration currently supports temporal maps and lists");
                    } else {
                        dynamic = !(collection->kind == ir::hir::TypeKind::List && collection->size.valid());
                    }
                }
                const Value     *source = call->arguments.empty() ? nullptr : value_at(module, call->arguments.front().value);
                const Parameter *pack   = pack_parameter(module, source);
                if (name == "keys" && (pack == nullptr || pack->pack != ParameterPack::Keyword)) {
                    issue(iterable->range, "graph-phase keys(...) traversal is not defined yet; use values(...) or items(...)");
                }
            }
        }
        if (!dynamic) { return plan; }
        // A fixed list is unrolled at wiring time, so its body may read
        // scalar configuration; a per-key child graph cannot yet.
        for (const ConditionalCapture &capture : plan.captures) {
            if (capture.phase == ir::hir::Phase::Wiring) { continue; }
            issue(binding_range(module, capture.binding, range),
                  "capturing scalar configuration in a dynamic graph 'for' body is not supported yet");
        }
        return plan;
    }

    namespace
    {
        /// Walks every callable and test body once and reports the
        /// context-free first-pass rules. The graph-phase rules (loops,
        /// temporal conditionals, assignment places, anonymous functions,
        /// intrinsics) apply to composition bodies only: a runtime traversal,
        /// predicate lambda, or indexed output assignment is ordinary node
        /// evaluation. Clearing an optional field through a sparse delta is
        /// rejected in either phase.
        class FirstPassRules
        {
          public:
            FirstPassRules(const Module &module, syntax::DiagnosticSink &diagnostics)
                : module_{module}, diagnostics_{diagnostics} {}

            void run() {
                for (const Callable &callable : module_.callables) {
                    runtime_ = callable.kind != CallableKind::Composition;
                    visit_value(callable.concise_body);
                    visit_block(callable.block_body);
                }
                runtime_ = false;
                for (const TestPlan &test : module_.tests) { visit_block(test.body); }
            }

          private:
            /// Report-once: a nested temporal conditional is analyzed both by
            /// its enclosing plan and by this visitor's own descent, and the
            /// sink does not deduplicate, so the same rule at the same range
            /// is reported exactly once here.
            void report(syntax::SourceRange range, std::string message) {
                if (!reported_.insert(std::to_string(range.begin) + ':' + std::to_string(range.end) + ':' + message).second) {
                    return;
                }
                diagnostics_.report(syntax::Category::Backend, range, std::move(message));
            }

            void report(const PlanIssue &issue) {
                if (issue.context_free) { report(issue.range, issue.message); }
            }

            /// The operator a call resolves to, by registry spelling.
            [[nodiscard]] std::string_view operator_name(const Value &value, const Call &call) const {
                if (!value.operation.registry_name.empty()) { return value.operation.registry_name; }
                const Value *target = value_at(module_, call.callee);
                if (target == nullptr) { return {}; }
                const auto *reference = std::get_if<Reference>(&target->node);
                if (reference == nullptr || reference->kind != ReferenceKind::Operator) { return {}; }
                return reference->registry_name.empty() ? reference->identity : reference->registry_name;
            }

            /// `map(inputs..., fn(params...) => body)`: the shape both backends
            /// lower as one per-key child graph.
            void check_map_lambda_call(const Value &value, const Call &call) {
                std::vector<const Value *> inputs;
                const Value               *anonymous = nullptr;
                for (const Argument &argument : call.arguments) {
                    const Value *item = value_at(module_, argument.value);
                    if (item == nullptr) { continue; }
                    if (std::holds_alternative<Lambda>(item->node)) {
                        if (anonymous != nullptr) { report(item->range, "map takes one anonymous function"); }
                        anonymous = item;
                    } else {
                        inputs.push_back(item);
                    }
                }
                if (anonymous == nullptr) { return; }
                if (inputs.empty()) { report(value.range, "map needs at least one temporal map input"); }
                for (const Value *input : inputs) {
                    const Type *type = type_at(module_, input->type);
                    if (type == nullptr || type->kind != ir::hir::TypeKind::Map) {
                        report(input->range, "the first anonymous map slice takes temporal map inputs");
                    }
                }
                const auto &lambda = std::get<Lambda>(anonymous->node);
                if (lambda.parameters.size() != inputs.size()) {
                    diagnostics_.report(syntax::Category::Type, anonymous->range,
                                        "the map function parameter count must match its mapped inputs");
                }
                const Value *body   = value_at(module_, lambda.body);
                const TypeId result = lambda.result.valid() ? lambda.result : (body != nullptr ? body->type : TypeId{});
                if (type_at(module_, result) == nullptr) {
                    report(anonymous->range, "the anonymous map result type cannot be inferred");
                }
            }

            [[nodiscard]] const StructContract *structure(TypeId id) const {
                const Type *type = type_at(module_, id);
                while (type != nullptr && type->kind == ir::hir::TypeKind::Atomic && type->children.size() == 1U) {
                    type = type_at(module_, type->children.front());
                }
                if (type == nullptr) { return nullptr; }
                const auto found = std::ranges::find_if(
                    module_.structures, [&](const StructContract &item) { return item.identity == type->nominal_identity; });
                return found == module_.structures.end() ? nullptr : &*found;
            }

            [[nodiscard]] bool is_null(ValueId id) const {
                const Value *value = value_at(module_, id);
                if (value == nullptr) { return false; }
                if (value->constant && std::holds_alternative<ir::hir::NullValue>(*value->constant)) { return true; }
                const auto *literal = std::get_if<Literal>(&value->node);
                return literal != nullptr && std::holds_alternative<ir::hir::NullValue>(literal->value);
            }

            void check_delta_clear(const Construct &construct) {
                if (!construct.delta) { return; }
                const StructContract *contract = structure(construct.type);
                if (contract == nullptr) { return; }
                for (const Argument &argument : construct.arguments) {
                    if (!is_null(argument.value)) { continue; }
                    const auto field =
                        std::ranges::find_if(contract->fields, [&](const StructField &item) { return item.name == argument.name; });
                    if (field == contract->fields.end() || !field->optional) { continue; }
                    // An omitted delta field means no change; there is no public
                    // hgraph operation yet that distinguishes an explicit clear.
                    report(value_at(module_, argument.value)->range,
                           "clearing an optional struct field needs the distinct public hgraph clear-delta operation");
                }
            }

            void visit_block(BlockId id) {
                if (!id.valid() || id.value >= module_.blocks.size()) { return; }
                const Block &block = module_.blocks[id.value];
                for (StatementId statement : block.statements) { visit_statement(statement); }
                visit_value(block.tail);
            }

            void visit_statement(StatementId id) {
                if (!id.valid() || id.value >= module_.statements.size()) { return; }
                const Statement &statement = module_.statements[id.value];
                std::visit(
                    [&](const auto &node) {
                        using T = std::decay_t<decltype(node)>;
                        if constexpr (std::is_same_v<T, LocalBinding> || std::is_same_v<T, StateBinding>) {
                            visit_value(node.init);
                        } else if constexpr (std::is_same_v<T, Lifecycle>) {
                            visit_block(node.block);
                        } else if constexpr (std::is_same_v<T, Activation>) {
                            visit_value(node.condition);
                            visit_block(node.block);
                        } else if constexpr (std::is_same_v<T, Traversal>) {
                            if (!runtime_) {
                                const TraversalPlan plan = analyze_traversal(module_, node, statement.range);
                                for (const PlanIssue &issue : plan.issues) { report(issue); }
                            }
                            visit_value(node.iterable);
                            visit_block(node.block);
                        } else if constexpr (std::is_same_v<T, Assignment>) {
                            const Value *place     = value_at(module_, node.place);
                            const auto  *reference = place != nullptr ? std::get_if<Reference>(&place->node) : nullptr;
                            if (!runtime_ && place != nullptr &&
                                (reference == nullptr || reference->kind != ReferenceKind::Binding)) {
                                report(place->range, "assignment targets a local in the first pass");
                            }
                            visit_value(node.place);
                            visit_value(node.value);
                        } else if constexpr (std::is_same_v<T, Return>) {
                            visit_value(node.value);
                        } else if constexpr (std::is_same_v<T, Assert>) {
                            visit_value(node.condition);
                        } else if constexpr (std::is_same_v<T, Evaluate>) {
                            visit_value(node.value);
                        }
                    },
                    statement.node);
            }

            void visit_value(ValueId id) {
                const Value *value = value_at(module_, id);
                if (value == nullptr || !visited_.insert(id.value).second) { return; }
                std::visit(
                    [&](const auto &node) {
                        using T = std::decay_t<decltype(node)>;
                        if constexpr (std::is_same_v<T, Unary>) {
                            visit_value(node.operand);
                        } else if constexpr (std::is_same_v<T, Binary>) {
                            visit_value(node.lhs);
                            visit_value(node.rhs);
                        } else if constexpr (std::is_same_v<T, Call>) {
                            const std::string_view intrinsic = intrinsic_name(module_, node.callee);
                            if (!runtime_ && !intrinsic.empty() && value->phase == ir::hir::Phase::Wiring &&
                                !composition_intrinsic(intrinsic)) {
                                report(value->range, "'" + std::string{intrinsic} +
                                                         "' is a runtime traversal; it is not available in a composition "
                                                         "body of the first pass");
                            }
                            const std::string_view operator_spelling = operator_name(*value, node);
                            const bool             map_call          = operator_spelling == "map_" || operator_spelling == "map";
                            if (map_call && !runtime_) { check_map_lambda_call(*value, node); }
                            visit_value(node.callee);
                            for (const Argument &argument : node.arguments) { visit_value(argument.value); }
                        } else if constexpr (std::is_same_v<T, Index>) {
                            visit_value(node.target);
                            visit_value(node.index);
                        } else if constexpr (std::is_same_v<T, Field>) {
                            visit_value(node.target);
                        } else if constexpr (std::is_same_v<T, Sequence>) {
                            for (const SequenceElement &element : node.elements) {
                                visit_value(element.key);
                                visit_value(element.value);
                            }
                        } else if constexpr (std::is_same_v<T, Tuple>) {
                            for (ValueId element : node.elements) { visit_value(element); }
                        } else if constexpr (std::is_same_v<T, Lambda>) {
                            // The frontend admits an anonymous function only where
                            // a callable parameter gives it a contextual type.
                            visit_value(node.body);
                        } else if constexpr (std::is_same_v<T, Conditional>) {
                            if (!runtime_ && value->phase == ir::hir::Phase::Wiring) {
                                const ConditionalPlan plan = analyze_temporal_conditional(module_, id);
                                for (const PlanIssue &issue : plan.issues) { report(issue); }
                            }
                            visit_value(node.condition);
                            visit_block(node.then_block);
                            visit_value(node.otherwise);
                        } else if constexpr (std::is_same_v<T, BlockValue>) {
                            visit_block(node.block);
                        } else if constexpr (std::is_same_v<T, HarnessEval>) {
                            visit_value(node.callee);
                            for (const Argument &argument : node.arguments) { visit_value(argument.value); }
                        } else if constexpr (std::is_same_v<T, Construct>) {
                            check_delta_clear(node);
                            for (const Argument &argument : node.arguments) { visit_value(argument.value); }
                        }
                    },
                    value->node);
            }

            [[nodiscard]] static bool composition_intrinsic(std::string_view name) noexcept {
                return name == "valid" || name == "modified" || name == "all_valid" || name == "last_modified" ||
                       name == "last_modified_time" || name == "key_set" || name == "keys" || name == "values" ||
                       name == "elements" || name == "items";
            }

            const Module                     &module_;
            syntax::DiagnosticSink           &diagnostics_;
            std::unordered_set<std::uint32_t> visited_{};
            std::unordered_set<std::string>   reported_{};
            bool                              runtime_{false};
        };
    }  // namespace

    void report_first_pass_rules(const Module &module, syntax::DiagnosticSink &diagnostics) {
        FirstPassRules{module, diagnostics}.run();
    }
}  // namespace hgl::hgraph_ir
