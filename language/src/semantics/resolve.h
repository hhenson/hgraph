#ifndef HGL_SEMANTICS_RESOLVE_H
#define HGL_SEMANTICS_RESOLVE_H

#include "syntax/ast.h"
#include "syntax/diagnostic.h"
#include "syntax/source.h"

#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <vector>

/// Name resolution, kernel imports, function classification, and the phase
/// rules of `test` bodies for one compilation unit (developer guide,
/// "Frontend components" and "Interim kernel table"; syntax guide, "Scopes
/// and name lookup"). The result annotates the syntax tree: one `Binding`
/// per name occurrence and one `FunctionKind` per function declaration.
namespace hgl::semantics
{
    namespace ast = syntax::ast;

    enum class BindingKind : std::uint8_t
    {
        Unbound,
        Local,      ///< `let`/`var`/`for` binding: `stmt` (+ `second` for a pair pattern)
        Parameter,  ///< `decl` is the function, `index` the parameter
        Generic,    ///< `decl` is the function, `index` the generic parameter
        Function,   ///< `decl` is the `fn`
        Operator,   ///< an imported kernel operator: `registry_name`
        LocalOperator,  ///< `decl` is the `operator` declaration
        Test,       ///< `decl` is the `test` (not a value)
        Intrinsic,  ///< a prelude intrinsic: `registry_name` holds its name
    };

    struct Binding
    {
        BindingKind   kind{BindingKind::Unbound};
        ast::DeclId   decl{ast::no_node};
        ast::StmtId   stmt{ast::no_node};
        std::uint32_t index{0};
        bool          second{false};
        std::string   registry_name{};
    };

    enum class FunctionKind : std::uint8_t
    {
        Composition,
        Runtime,
    };

    struct ImportedOperator
    {
        std::string   name;           ///< the short name in scope
        std::string   module;         ///< `hgraph.std` / `hgraph.analytics`
        std::string   registry_name;  ///< the hgraph registry name
        syntax::SourceRange range{};
    };

    struct ModuleAlias
    {
        std::string alias;
        std::string module;
    };

    struct ResolvedModule
    {
        std::string                   module_path;
        std::vector<Binding>          bindings;  ///< indexed by ExprId
        std::vector<FunctionKind>     kinds;     ///< indexed by DeclId
        std::vector<ImportedOperator> imports;
        std::vector<ModuleAlias>      aliases;
        std::vector<ast::DeclId>      functions;
        std::vector<ast::DeclId>      operators;
        std::vector<ast::DeclId>      tests;

        [[nodiscard]] const Binding &binding(ast::ExprId id) const noexcept { return bindings[id]; }
        [[nodiscard]] FunctionKind   kind(ast::DeclId id) const noexcept { return kinds[id]; }
    };

    /// The prelude intrinsics (syntax guide, "Scopes and name lookup", step
    /// 5), bound after every declaration so any of them may be shadowed.
    [[nodiscard]] bool is_intrinsic(std::string_view name) noexcept;

    /// What the resolver may ask of the hgraph operator registry: whether a
    /// registry name exists. Supplied by the caller so the resolver itself
    /// does not depend on hgraph (tests pass a table).
    using OperatorLookup = std::function<bool(std::string_view)>;

    [[nodiscard]] ResolvedModule resolve(const syntax::SourceFile &file, const ast::Module &module,
                                         const OperatorLookup &has_operator, syntax::DiagnosticSink &diagnostics);
}  // namespace hgl::semantics

#endif  // HGL_SEMANTICS_RESOLVE_H
