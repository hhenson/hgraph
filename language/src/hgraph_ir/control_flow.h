#ifndef HGL_HGRAPH_IR_CONTROL_FLOW_H
#define HGL_HGRAPH_IR_CONTROL_FLOW_H

#include "hgraph_ir/ir.h"
#include "syntax/diagnostic.h"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace hgl::hgraph_ir
{
    /// A fail-closed rule the shared analysis found while planning. Both
    /// execution backends report every issue through their own diagnostic
    /// channel and never re-derive the rule: the message text lives here only,
    /// so the direct and generated backends cannot drift apart.
    struct PlanIssue
    {
        syntax::SourceRange range{};
        std::string         message{};
        /// True when the rule does not depend on how a backend reached the
        /// construct. `report_first_pass_rules` reports these once during
        /// hgraph IR lowering, so `hgl check` already rejects them; the
        /// context-dependent rules remain in the plan for the backend that
        /// supplied the context (a callable continuation, for instance).
        bool context_free{true};
    };

    /// Rules only an execution backend can decide because they depend on the
    /// values it has to materialize (a zoned literal folded into a constant
    /// comparison never reaches one). The wording still has a single owner.
    namespace first_pass
    {
        inline constexpr std::string_view unsupported_temporal_literal =
            "zoned and civil literals are not supported by the first pass";
    }  // namespace first_pass

    /// An outer lexical binding read by a nested conditional branch. The
    /// phase is retained at the use site so execution backends can distinguish
    /// temporal boundary inputs from scalar configuration captures.
    struct ConditionalCapture
    {
        BindingId      binding{};
        TypeId         type{};
        ir::hir::Phase phase{ir::hir::Phase::Unknown};
    };

    /// One lexical suffix in the path from a nested temporal conditional back
    /// to the end of its enclosing callable. A nested path can cross several
    /// blocks, so retaining each suffix separately preserves the order and
    /// effect of intermediate tail expressions.
    struct ConditionalContinuationSegment
    {
        std::vector<StatementId> statements{};
        ValueId                  tail{};

        friend bool operator==(const ConditionalContinuationSegment &, const ConditionalContinuationSegment &) = default;
    };

    /// The ordered callable path that must execute on every branch which falls
    /// through a temporal conditional containing an early return. Keeping the
    /// path in execution IR lets both backends place it inside the selected
    /// child graph without recovering syntax context.
    struct ConditionalContinuationPlan
    {
        std::vector<ConditionalContinuationSegment> segments{};
        TypeId                                      result{};
    };

    struct ConditionalBranchPlan
    {
        BlockId                                    block{};
        std::optional<ConditionalContinuationPlan> continuation{};
        std::vector<ConditionalCapture>            captures{};
        std::vector<BindingId>                     assigned_outer{};
        /// At least one path in this branch contains an explicit return.
        bool returns{false};
        /// At least one path reaches the end of this planned branch. A
        /// callable continuation, when attached, consumes that fallthrough.
        bool falls_through{true};
    };

    /// Backend-independent plan for one graph-phase conditional. Captures are
    /// the stable first-use union of the two branch signatures.
    struct ConditionalPlan
    {
        ValueId                              value{};
        ValueId                              condition{};
        TypeId                               result{};
        ConditionalBranchPlan                when_true{};
        bool                                 has_otherwise{false};
        std::optional<ConditionalBranchPlan> when_false{};
        std::vector<ConditionalCapture>      captures{};
        std::vector<BindingId>               assigned_outer{};
        /// Every selected child supplies the enclosing callable's result.
        bool returns_from_callable{false};
        /// Rules the conditional breaks; a backend reports them before lowering.
        std::vector<PlanIssue> issues{};
    };

    /// Copy the suffix beginning at first_statement from an enclosing block.
    /// Backends pass the enclosing callable's result type and the conditional
    /// being planned. If that value is the enclosing block's tail, it is not
    /// copied into its own continuation. The plan contains only stable
    /// HGraph-IR IDs and can outlive traversal of the parent block.
    [[nodiscard]] ConditionalContinuationPlan plan_temporal_continuation(const Module &module, BlockId enclosing,
                                                                         std::size_t first_statement, TypeId result,
                                                                         ValueId conditional = {});

    /// Prepend the suffix of a nested enclosing block to an existing callable
    /// continuation. Empty suffixes are omitted; an empty plan still records
    /// that falling through supplies the current expression as the callable
    /// result.
    [[nodiscard]] ConditionalContinuationPlan prepend_temporal_continuation(const Module &module, BlockId enclosing,
                                                                            std::size_t                 first_statement,
                                                                            ConditionalContinuationPlan following,
                                                                            ValueId                     conditional = {});

    /// Prepend a suffix of an already materialized segment. Execution
    /// backends use this while walking a continuation so another nested
    /// temporal conditional can split the path without reconstructing syntax
    /// or flattening its remaining enclosing segments.
    [[nodiscard]] ConditionalContinuationPlan prepend_temporal_continuation(const ConditionalContinuationSegment &enclosing,
                                                                            std::size_t                           first_statement,
                                                                            ConditionalContinuationPlan           following,
                                                                            ValueId                               conditional = {});

    /// Analyze an HGraph-IR Conditional value. The input module is already
    /// structurally valid; a non-conditional value or non-block else arm is
    /// represented as an incomplete plan for the backends to diagnose against
    /// the original source range.
    [[nodiscard]] ConditionalPlan
    analyze_temporal_conditional(const Module &module, ValueId value,
                                 std::optional<ConditionalContinuationPlan> continuation = std::nullopt);

    /// Whether this branch supplies an escaping result by retaining the
    /// binding that entered the conditional instead of assigning a new one.
    /// An omitted false branch is represented by an empty branch plan and
    /// therefore forwards every escaping binding.
    [[nodiscard]] bool temporal_branch_forwards(const ConditionalPlan &plan, const ConditionalBranchPlan &branch,
                                                BindingId binding);

    enum class ConditionalResultSource : std::uint8_t {
        Expression,
        Binding,
        FunctionReturn,
    };

    /// One field in the common output contract shared by both temporal
    /// branches. A single slot is returned directly; several slots are packed
    /// into a compiler-generated structural TSB in this stable order.
    struct ConditionalResultSlot
    {
        ConditionalResultSource source{ConditionalResultSource::Expression};
        TypeId                  type{};
        BindingId               binding{};
        std::string             field_name{};
    };

    /// Derive the backend-independent result signature for a temporal
    /// conditional. Whether its expression value is consumed is a property of
    /// the enclosing expression/statement, so callers supply that fact while
    /// assignment escapes come from ConditionalPlan analysis.
    [[nodiscard]] std::vector<ConditionalResultSlot>
    plan_temporal_conditional_results(const Module &module, const ConditionalPlan &plan, bool expression_used);

    /// Captures and control-flow effects for one traversal body. Loop bindings
    /// are treated as body locals; assigned_outer therefore names only values
    /// whose meaning would cross iterations.
    struct TraversalPlan
    {
        BlockId                         block{};
        std::vector<ConditionalCapture> captures{};
        std::vector<BindingId>          assigned_outer{};
        bool                            returns{false};
        /// Rules the graph-phase loop breaks: escaping assignment or return,
        /// scalar captures in a dynamic body, and the iterator forms the first
        /// pass does not define. A backend reports them before lowering.
        std::vector<PlanIssue> issues{};
    };

    /// `range` is the traversal statement's range, used for the loop-level
    /// issues; the loop block's range is used when it is omitted.
    [[nodiscard]] TraversalPlan analyze_traversal(const Module &module, const Traversal &traversal, syntax::SourceRange range = {});

    /// Report, once, every context-free rule of the first pass that both
    /// execution backends would otherwise re-derive: the control-flow plans'
    /// context-free issues for every graph-phase conditional and loop in a
    /// composition callable or test, assignment places that are not plain
    /// bindings, the shape of a `map(...)` call with an anonymous function,
    /// runtime-only intrinsics in a composition body, and clearing an optional
    /// struct field through a sparse delta (in either phase). Hgraph IR
    /// lowering calls this after the module is complete, so `hgl check`
    /// rejects the constructs and neither backend needs an opinion of its own.
    void report_first_pass_rules(const Module &module, syntax::DiagnosticSink &diagnostics);
}  // namespace hgl::hgraph_ir

#endif  // HGL_HGRAPH_IR_CONTROL_FLOW_H
