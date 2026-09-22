#ifndef HGL_CODEGEN_CPP_EMITTER_H
#define HGL_CODEGEN_CPP_EMITTER_H

#include "hgraph_ir/ir.h"
#include "syntax/diagnostic.h"
#include "syntax/source.h"

#include <cstddef>
#include <optional>
#include <string>
#include <vector>

/// The C++ backend, first pass (developer guide, "C++ backend, first pass"):
/// a public header, optionally split implementation sources, and one canonical
/// JSON descriptor per module. They are planned from hgraph IR and print names
/// and types without linking the hgraph
/// runtime, so what it emits is checked by the native compiler that builds the
/// package. Body-local let, var, and state binding types also come from hgraph
/// IR. Internal callable dependencies are read from reachable hgraph-IR body
/// operations. Composition bodies and their anonymous functions are emitted
/// from graph-IR values, statements, blocks, and lexical bindings. Declaration
/// order and source locations also come directly from typed hgraph-IR handles.
/// Tests run generated graphs beside `hgl test` and exercise generated runtime
/// nodes directly through hgraph's public harness.
namespace hgl::codegen
{
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
        /// Only the test/interactive harness may emit module-private test helpers.
        bool include_test_contexts{false};
        /// Number of separately compiled implementation parts; one keeps the
        /// traditional header/source pair. Registration order is unchanged.
        std::size_t source_parts{1};
    };

    struct EmittedModule
    {
        /// The C++ namespace (`examples::prices` for `module examples.prices`).
        std::string namespace_name{};
        /// The registry-name prefix (`examples.prices`).
        std::string module_name{};
        std::string header{};
        std::string source{};
        /// Build-private shared definitions and implementation parts, emitted
        /// only when source_parts > 1. Neither changes the public descriptor.
        std::string              implementation_header{};
        std::vector<std::string> implementation_sources{};
        /// Canonical UTF-8 JSON for `<stem>.hgl-module.json`.
        std::string descriptor{};
        /// Canonical descriptor fingerprint also embedded in a dynamic module
        /// lifecycle table and checked before activation.
        std::string descriptor_fingerprint{};
        /// Empty unless `EmitOptions::python_native_module` was set.
        std::string python{};
        /// Exported function names, in declaration order.
        std::vector<std::string> exports{};
    };

    /// The C++ namespace generated code for this module lives in
    /// (`examples::prices` for `module examples.prices`; a segment that is a
    /// C++ keyword or a name the generated code reserves gets a trailing
    /// underscore). This is the only place that spelling is decided:
    /// `hgl emit-cpp --print-namespace` prints it and the descriptor's
    /// registration symbol carries it, so build tooling never re-derives it.
    [[nodiscard]] std::string module_namespace(const hgraph_ir::Module &graph);

    /// Emit the module. Returns nullopt after reporting a diagnostic; every
    /// construct outside the first pass is reported as a `backend`
    /// diagnostic that names the construct.
    [[nodiscard]] std::optional<EmittedModule> emit_cpp(const syntax::SourceFile &file, const hgraph_ir::Module &graph,
                                                        const EmitOptions &options, syntax::DiagnosticSink &diagnostics);
}  // namespace hgl::codegen

#endif  // HGL_CODEGEN_CPP_EMITTER_H
