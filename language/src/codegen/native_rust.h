#ifndef HGL_CODEGEN_NATIVE_RUST_H
#define HGL_CODEGEN_NATIVE_RUST_H

#include "hgraph_ir/ir.h"
#include "syntax/diagnostic.h"
#include <optional>
#include <string>

namespace hgl::codegen
{
    /// Generate a Rust implementation trait from checked concrete value declarations.
    /// Unsupported shapes are diagnostics, never erased into scalar signatures.
    std::optional<std::string> emit_native_rust(const hgraph_ir::Module &module, syntax::DiagnosticSink &diagnostics);
}  // namespace hgl::codegen
#endif
