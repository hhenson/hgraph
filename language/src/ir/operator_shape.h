#ifndef HGL_IR_OPERATOR_SHAPE_H
#define HGL_IR_OPERATOR_SHAPE_H

#include "ir/hir.h"

#include <algorithm>
#include <cstddef>

namespace hgl::ir::detail
{
    /// An implementation may declare more parameters than its operator
    /// contract (a superset, runtime spec WIR-22): the contract's come first,
    /// and the extra ones follow, with or without defaults.
    [[nodiscard]] inline bool extends_contract(const hir::FunctionDecl &implementation, std::size_t declared) {
        return implementation.signature.parameters.size() >= declared;
    }

    /// Whether a call that supplies only the contract's parameters can reach
    /// the implementation: every extra parameter has a default. One that
    /// requires an argument the call does not supply does not match (WIR-22);
    /// that is not an error.
    [[nodiscard]] inline bool extras_defaulted(const hir::FunctionDecl &implementation, std::size_t declared) {
        const auto &parameters = implementation.signature.parameters;
        return std::all_of(parameters.begin() + static_cast<std::ptrdiff_t>(std::min(declared, parameters.size())),
                           parameters.end(), [](const hir::Parameter &extra) { return extra.default_value.valid(); });
    }

    /// A candidate that a call supplying ``supplied`` contract arguments can
    /// reach: it extends the contract, and every extra parameter has a default.
    [[nodiscard]] inline bool reachable_with(const hir::FunctionDecl &implementation, std::size_t supplied) {
        return extends_contract(implementation, supplied) && extras_defaulted(implementation, supplied);
    }
}  // namespace hgl::ir::detail

#endif  // HGL_IR_OPERATOR_SHAPE_H
