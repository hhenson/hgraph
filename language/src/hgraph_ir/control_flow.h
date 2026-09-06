#ifndef HGL_HGRAPH_IR_CONTROL_FLOW_H
#define HGL_HGRAPH_IR_CONTROL_FLOW_H

#include "hgraph_ir/ir.h"

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

    struct ConditionalBranchPlan
    {
        BlockId                         block{};
        std::vector<ConditionalCapture> captures{};
        std::vector<BindingId>          assigned_outer{};
        bool                            returns{false};
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
    };

    /// Analyze an HGraph-IR Conditional value. The input module is already
    /// structurally valid; a non-conditional value or non-block else arm is
    /// represented as an incomplete plan for the backends to diagnose against
    /// the original source range.
    [[nodiscard]] ConditionalPlan analyze_temporal_conditional(const Module &module, ValueId value);

    enum class ConditionalResultSource : std::uint8_t {
        Expression,
        Binding,
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
