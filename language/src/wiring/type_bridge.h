#ifndef HGL_WIRING_TYPE_BRIDGE_H
#define HGL_WIRING_TYPE_BRIDGE_H

#include "hgraph_ir/ir.h"
#include "syntax/diagnostic.h"

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/value/value.h>

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

namespace hgl::wiring
{
    /// Materialize backend-neutral hgraph-IR types as canonical hgraph runtime
    /// metadata. This is the only direct-wiring layer that understands both
    /// representations; evaluators consume its results rather than rebuilding
    /// types from source syntax.
    class TypeBridge
    {
      public:
        TypeBridge(const hgraph_ir::Module &module, syntax::DiagnosticSink &diagnostics);

        [[nodiscard]] const hgraph::ValueTypeMetaData   *value(hgraph_ir::TypeId type);
        [[nodiscard]] const hgraph::TSValueTypeMetaData *schema(hgraph_ir::TypeId type);
        [[nodiscard]] std::optional<hgraph::Value>       literal(hgraph_ir::ConstExprId expression);

      private:
        struct Bindings
        {
            std::unordered_map<std::uint32_t, hgraph_ir::TypeId>      types{};
            std::unordered_map<std::uint32_t, hgraph_ir::ConstExprId> values{};

            [[nodiscard]] bool empty() const noexcept { return types.empty() && values.empty(); }
        };

        [[nodiscard]] const hgraph::ValueTypeMetaData   *value(hgraph_ir::TypeId type, const Bindings &bindings);
        [[nodiscard]] const hgraph::TSValueTypeMetaData *schema(hgraph_ir::TypeId type, const Bindings &bindings);
        [[nodiscard]] const hgraph_ir::StructContract   *structure(std::string_view identity) const noexcept;
        [[nodiscard]] std::optional<Bindings>          bind(const hgraph_ir::Type &type, const hgraph_ir::StructContract &structure,
                                                            const Bindings &outer);
        [[nodiscard]] std::optional<std::int64_t>      integer(hgraph_ir::ConstExprId expression, syntax::SourceRange range,
                                                               std::string_view role);
        [[nodiscard]] const hgraph::ValueTypeMetaData *nominal_value(const hgraph_ir::Type &type, const Bindings &outer);
        [[nodiscard]] const hgraph::TSValueTypeMetaData *nominal_schema(const hgraph_ir::Type &type, const Bindings &outer);
        void                                             report(syntax::SourceRange range, std::string message);

        const hgraph_ir::Module                                               &module_;
        syntax::DiagnosticSink                                                &diagnostics_;
        hgraph::TypeRegistry                                                  &registry_;
        std::unordered_map<std::uint32_t, const hgraph::ValueTypeMetaData *>   values_{};
        std::unordered_map<std::uint32_t, const hgraph::TSValueTypeMetaData *> schemas_{};
    };
}  // namespace hgl::wiring

#endif  // HGL_WIRING_TYPE_BRIDGE_H
