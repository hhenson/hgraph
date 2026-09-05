#ifndef HGL_IR_GENERIC_SUBSTITUTION_H
#define HGL_IR_GENERIC_SUBSTITUTION_H

#include "ir/canonical_types.h"

#include <cstdint>
#include <optional>
#include <unordered_map>
#include <vector>

namespace hgl::ir::detail
{
    /// One source-level generic substitution. This is the shared unification
    /// boundary for call matching, constraint solving, and later IR lowering;
    /// it does not rank overload candidates.
    class GenericSubstitution
    {
      public:
        GenericSubstitution(hir::Module &module, CanonicalTypes &types);

        [[nodiscard]] bool unify(hir::TypeId pattern, hir::TypeId actual);
        [[nodiscard]] bool unify_value(hir::ExprId pattern, hir::ExprId actual);

        [[nodiscard]] bool                       bind_type(hir::SymbolId parameter, hir::TypeId value);
        [[nodiscard]] bool                       bind_value(hir::SymbolId parameter, hir::ExprId value);
        [[nodiscard]] bool                       has_type(hir::SymbolId parameter) const noexcept;
        [[nodiscard]] bool                       has_value(hir::SymbolId parameter) const noexcept;
        [[nodiscard]] std::optional<hir::TypeId> type_binding(hir::SymbolId parameter) const noexcept;
        [[nodiscard]] std::optional<hir::ExprId> value_binding(hir::SymbolId parameter) const noexcept;

        [[nodiscard]] hir::TypeId                    apply(hir::TypeId input);
        [[nodiscard]] hir::ExprId                    apply_value(hir::ExprId input) const noexcept;
        [[nodiscard]] std::vector<hir::Substitution> materialize(const std::vector<hir::GenericParameter> &generics) const;

      private:
        hir::Module                                   &module_;
        CanonicalTypes                                &types_;
        std::unordered_map<std::uint32_t, hir::TypeId> type_bindings_{};
        std::unordered_map<std::uint32_t, hir::ExprId> value_bindings_{};
    };
}  // namespace hgl::ir::detail

#endif  // HGL_IR_GENERIC_SUBSTITUTION_H
