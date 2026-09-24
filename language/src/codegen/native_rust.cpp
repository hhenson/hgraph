#include "codegen/native_rust.h"

#include <set>
#include <string_view>

namespace hgl::codegen
{
    std::optional<std::string> emit_native_rust(const hgraph_ir::Module &module, syntax::DiagnosticSink &diagnostics) {
        std::string           result = "// Generated from checked HGL native declarations; do not edit.\n"
                                       "/// Implementation of the shared native value interface.\n"
                                       "pub trait Native {\n";
        std::set<std::string> names;
        bool                  valid = true;
        for (const auto &function : module.native_functions) {
            if (!function.source_defined) { continue; }
            const auto reject = [&](std::string message) {
                diagnostics.report(syntax::Category::Backend, function.range, std::move(message));
                valid = false;
            };
            if (function.execution_role != NativeExecutionRole::Value || !function.cpp_body.empty() || !function.generics.empty() ||
                function.throws) {
                reject("Rust interface emission requires concrete declaration-only native const fn without throws");
                continue;
            }
            const std::string name = function.identity.substr(function.identity.rfind("::") + 2);
            if (!names.insert(name).second) {
                reject("Rust interface overload families require a typed provider contract; not implemented yet");
                continue;
            }
            const auto type_name = [&](hgraph_ir::TypeId id) -> std::string {
                const auto &type = module.types.at(id.value);
                if (type.kind == ir::hir::TypeKind::Void) { return "()"; }
                if (type.kind == ir::hir::TypeKind::Scalar) {
                    switch (type.scalar) {
                        case ir::hir::ScalarType::Bool: return "bool";
                        case ir::hir::ScalarType::I64: return "i64";
                        case ir::hir::ScalarType::F64: return "f64";
                        default: break;
                    }
                }
                reject("Rust native interface currently supports bool, i64 and f64 values");
                return {};
            };
            result += "    /// `" + function.identity + "`, with value-level arguments and result.\n";
            result += "    fn r#" + name + "(";
            bool first = true;
            for (const auto &parameter : function.parameters) {
                if (parameter.access != ir::hir::NativeParameterAccess::Value) {
                    reject("a Rust value parameter cannot implicitly borrow an input");
                }
                if (!first) { result += ", "; }
                first = false;
                result += "r#" + parameter.name + ": " + type_name(parameter.type);
            }
            result += ") -> " + type_name(function.result) + ";\n";
        }
        result += "}\n";
        if (!valid || names.empty()) {
            if (names.empty() && valid) {
                diagnostics.report(syntax::Category::Backend, {}, "Rust native interface contains no value declarations");
            }
            return std::nullopt;
        }
        return result;
    }
}  // namespace hgl::codegen
