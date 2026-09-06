#ifndef HGL_HGRAPH_IR_CONTROL_FLOW_H
#define HGL_HGRAPH_IR_CONTROL_FLOW_H

#include "hgraph_ir/ir.h"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace hgl::hgraph_ir
{
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
    };

    [[nodiscard]] TraversalPlan analyze_traversal(const Module &module, const Traversal &traversal);
}  // namespace hgl::hgraph_ir

#endif  // HGL_HGRAPH_IR_CONTROL_FLOW_H
