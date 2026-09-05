#ifndef HGL_IR_CANONICAL_TYPES_H
#define HGL_IR_CANONICAL_TYPES_H

#include "ir/hir.h"
#include "syntax/diagnostic.h"

#include <string>
#include <unordered_map>
#include <vector>

namespace hgl::ir::detail
{
    /// Owns structural interning and source-to-canonical rewriting for one HIR
    /// module. Source ranges remain on the original type nodes; semantic
    /// clients use the canonical representatives produced here.
    class CanonicalTypes
    {
      public:
        CanonicalTypes(hir::Module &module, syntax::DiagnosticSink &diagnostics);

        void initialize();

        [[nodiscard]] hir::TypeId canonical(hir::TypeId id) const noexcept;
        [[nodiscard]] hir::TypeId intern(hir::Type value);
        [[nodiscard]] hir::TypeId make(hir::TypeKind kind, std::vector<hir::TypeId> children = {}, hir::SymbolId symbol = {});
        [[nodiscard]] hir::TypeId scalar(hir::ScalarType value);
        [[nodiscard]] hir::TypeId void_type() const noexcept { return void_type_; }

        [[nodiscard]] bool        same(hir::TypeId lhs, hir::TypeId rhs) const noexcept;
        [[nodiscard]] bool        numeric(hir::TypeId id) const noexcept;
        [[nodiscard]] bool        boolean(hir::TypeId id) const noexcept;
        [[nodiscard]] bool        assignable(hir::TypeId expected, hir::TypeId actual) const noexcept;
        [[nodiscard]] bool        same_value(hir::ExprId lhs, hir::ExprId rhs) const;
        [[nodiscard]] std::string name(hir::TypeId id) const;

      private:
        [[nodiscard]] std::string value_key(hir::ExprId id) const;
        [[nodiscard]] std::string type_key(const hir::Type &value) const;
        [[nodiscard]] hir::TypeId canonicalize(hir::TypeId id);
        void                      rewrite_signature(hir::Signature &signature);
        void                      rewrite_type_references();

        hir::Module                                 &module_;
        syntax::DiagnosticSink                      &diagnostics_;
        std::unordered_map<std::string, hir::TypeId> type_intern_{};
        std::vector<hir::TypeId>                     source_canonical_{};
        std::vector<bool>                            source_visiting_{};
        hir::TypeId                                  void_type_{};
    };
}  // namespace hgl::ir::detail

#endif  // HGL_IR_CANONICAL_TYPES_H
