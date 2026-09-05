#ifndef HGL_IR_TYPE_CHECK_H
#define HGL_IR_TYPE_CHECK_H

#include "ir/hir.h"
#include "syntax/diagnostic.h"

#include <functional>
#include <optional>
#include <string>
#include <vector>

namespace hgl::ir
{
    struct OperatorArgument
    {
        std::string                  name{};
        hir::TypeId                  type{};
        hir::Phase                   phase{hir::Phase::Unknown};
        hir::ValueKind               value_kind{hir::ValueKind::Unknown};
        std::optional<hir::Constant> constant{};
    };

    struct OperatorQuery
    {
        std::string                   identity{};
        std::vector<OperatorArgument> arguments{};
        hir::TypeId                   expected_result{};
        syntax::SourceRange           range{};
    };

    struct OperatorSelection
    {
        /// The native candidate's copied diagnostic label. No candidate
        /// pointer or provider lease may cross into HIR.
        std::string                    candidate_label{};
        hir::TypeId                    result{};
        std::vector<hir::Substitution> substitutions{};
        bool                           deferred{false};
        std::string                    error{};
    };

    /// Registry adapter used by type completion. The type checker owns HGL
    /// generic inference and phase rules; this callback owns native overload
    /// ranking so the language cannot drift from hgraph's OperatorRegistry.
    using OperatorResolver = std::function<OperatorSelection(const hir::Module &, const OperatorQuery &)>;

    /// Complete canonical types, substitutions, semantic call identities,
    /// phases, typed constants, effects, and admitted capabilities. The pass
    /// changes completion to Typed only when every value expression has a
    /// type and every call has a semantic identity, or reports diagnostics and
    /// leaves the module Resolved.
    [[nodiscard]] bool complete_hir(hir::Module &module, const OperatorResolver &resolve_operator,
                                    syntax::DiagnosticSink &diagnostics);
}  // namespace hgl::ir

#endif  // HGL_IR_TYPE_CHECK_H
