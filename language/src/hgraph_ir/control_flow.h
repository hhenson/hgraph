#ifndef HGL_HGRAPH_IR_CONTROL_FLOW_H
#define HGL_HGRAPH_IR_CONTROL_FLOW_H

#include "hgraph_ir/ir.h"

#include <optional>
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
        std::optional<ConditionalBranchPlan> when_false{};
        std::vector<ConditionalCapture>      captures{};
    };

    /// Analyze an HGraph-IR Conditional value. The input module is already
    /// structurally valid; a non-conditional value or non-block else arm is
    /// represented as an incomplete plan for the backends to diagnose against
    /// the original source range.
    [[nodiscard]] ConditionalPlan analyze_temporal_conditional(const Module &module, ValueId value);

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
