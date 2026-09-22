#ifndef HGL_HGRAPH_IR_USES_H
#define HGL_HGRAPH_IR_USES_H

#include "hgraph_ir/ir.h"

#include <cstdint>
#include <unordered_map>

namespace hgl::hgraph_ir
{
    /// Which bindings a body reaches: every `Reference` to a binding inside
    /// the visited statements and values, counted per binding. This is the
    /// reachability question a backend asks before it names a parameter,
    /// keeps a state local, or leaves a loop binding without a use, and the
    /// question a diagnostic asks before reporting an unused declaration.
    /// It is computed on the hgraph IR, so both backends see one answer.
    struct BindingUses
    {
        /// Every reference, including the target of a plain assignment.
        std::unordered_map<std::uint32_t, int> references{};
        /// References that read the binding: everything except the bare
        /// target of a plain `=`. A compound assignment reads its target.
        std::unordered_map<std::uint32_t, int> reads{};

        [[nodiscard]] int count(BindingId binding) const noexcept {
            const auto found = references.find(binding.value);
            return found == references.end() ? 0 : found->second;
        }
        [[nodiscard]] int read_count(BindingId binding) const noexcept {
            const auto found = reads.find(binding.value);
            return found == reads.end() ? 0 : found->second;
        }
        [[nodiscard]] bool uses(BindingId binding) const noexcept { return count(binding) > 0; }
        [[nodiscard]] bool is_read(BindingId binding) const noexcept { return read_count(binding) > 0; }
    };

    /// Add every binding reference reachable from the block, statement, or
    /// value to `uses`. Nested blocks, lambda bodies, conditional branches,
    /// and traversal bodies are included; an invalid id adds nothing.
    void collect_binding_uses(const Module &module, BlockId block, BindingUses &uses);
    void collect_binding_uses(const Module &module, StatementId statement, BindingUses &uses);
    void collect_binding_uses(const Module &module, ValueId value, BindingUses &uses);

    [[nodiscard]] BindingUses binding_uses(const Module &module, BlockId block);
    [[nodiscard]] BindingUses binding_uses(const Module &module, ValueId value);
}  // namespace hgl::hgraph_ir

#endif  // HGL_HGRAPH_IR_USES_H
