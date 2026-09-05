#ifndef HGL_CODEGEN_CPP_EMITTER_H
#define HGL_CODEGEN_CPP_EMITTER_H

#include "hgraph_ir/ir.h"
#include "semantics/resolve.h"
#include "syntax/ast.h"
#include "syntax/diagnostic.h"
#include "syntax/source.h"

#include <optional>
#include <string>
#include <vector>

/// The C++ backend, first pass (developer guide, "C++ backend, first pass"):
/// one header/source pair of public hgraph authoring code per module. It is
/// planned from hgraph IR and prints names and types without linking the hgraph
/// runtime, so what it emits is checked by the native compiler that builds the
/// package. A temporary syntax adapter still supplies bodies and local
/// annotations during the Stage E migration. Tests run generated graphs beside
/// `hgl test` and exercise generated runtime nodes directly through hgraph's
/// public harness.
namespace hgl::codegen
{
    namespace ast = syntax::ast;

    struct EmitOptions
    {
        /// The header's file name as the source includes it (`prices.h`).
        std::string header_name{};
        /// Tool version for the generated banner.
        std::string tool_version{};
        /// When set, a Python wrapper module is produced that imports this
        /// native module (the nanobind module built by `hgl_add_module`) and
        /// exposes every exported function through `hgraph.operator_function`.
        std::string python_native_module{};
    };

    struct EmittedModule
    {
        /// The C++ namespace (`examples::prices` for `module examples.prices`).
        std::string              namespace_name{};
        /// The registry-name prefix (`examples.prices`).
        std::string              module_name{};
        std::string              header{};
        std::string              source{};
        /// Empty unless `EmitOptions::python_native_module` was set.
        std::string              python{};
        /// Exported function names, in declaration order.
        std::vector<std::string> exports{};
    };

    /// Emit the module. Returns nullopt after reporting a diagnostic; every
    /// construct outside the first pass is reported as a `backend`
    /// diagnostic that names the construct.
    [[nodiscard]] std::optional<EmittedModule> emit_cpp(const syntax::SourceFile &file, const hgraph_ir::Module &graph,
                                                        const ast::Module &module, const semantics::ResolvedModule &resolved,
                                                        const EmitOptions &options, syntax::DiagnosticSink &diagnostics);
}  // namespace hgl::codegen

#endif  // HGL_CODEGEN_CPP_EMITTER_H
