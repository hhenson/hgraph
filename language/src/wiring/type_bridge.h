#ifndef HGL_WIRING_TYPE_BRIDGE_H
#define HGL_WIRING_TYPE_BRIDGE_H

#include "hgraph_ir/ir.h"
#include "syntax/diagnostic.h"

#include <hgraph/lib/std/standard_types.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/value/value.h>

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

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

        /// One applied struct: its contract, the generic bindings its fields see,
        /// its argument schemas, and the registry names they give it.
        struct Specialization
        {
            const hgraph_ir::StructContract               *contract{nullptr};
            Bindings                                       applied{};
            std::vector<const hgraph::ValueTypeMetaData *> generic_types{};
            std::string                                    module_name{};
            std::string                                    local_name{};

            [[nodiscard]] std::string qualified() const { return module_name + "::" + local_name; }
        };

        [[nodiscard]] const hgraph::ValueTypeMetaData   *value(hgraph_ir::TypeId type, const Bindings &bindings);
        [[nodiscard]] const hgraph::TSValueTypeMetaData *schema(hgraph_ir::TypeId type, const Bindings &bindings);
        [[nodiscard]] const hgraph_ir::StructContract   *structure(std::string_view identity) const noexcept;
        [[nodiscard]] std::optional<Bindings>          bind(const hgraph_ir::Type &type, const hgraph_ir::StructContract &structure,
                                                            const Bindings &outer);
        [[nodiscard]] std::optional<std::int64_t>      integer(hgraph_ir::ConstExprId expression, syntax::SourceRange range,
                                                               std::string_view role);
        [[nodiscard]] const hgraph::ValueTypeMetaData *nominal_value(const hgraph_ir::Type &type, const Bindings &outer);
        [[nodiscard]] std::optional<Specialization>      specialize(const hgraph_ir::Type &type, const Bindings &outer);
        [[nodiscard]] hgraph_ir::TypeId                  resolved(hgraph_ir::TypeId type, const Bindings &bindings) const;
        [[nodiscard]] const hgraph::ValueTypeMetaData   *field_value(const hgraph_ir::StructField &field, const Bindings &applied);
        [[nodiscard]] std::optional<Specialization> recursive_target(const hgraph_ir::StructField &field, const Bindings &applied);
        [[nodiscard]] const hgraph::ValueTypeMetaData *register_value(const Specialization &specialization,
                                                                      syntax::SourceRange   range);
        [[nodiscard]] const hgraph::ValueTypeMetaData *recursive_value(Specialization root, syntax::SourceRange range);
        [[nodiscard]] const hgraph::ValueTypeMetaData *registered(const Specialization &specialization, syntax::SourceRange range);
        [[nodiscard]] const hgraph::TSValueTypeMetaData *nominal_schema(const hgraph_ir::Type &type, const Bindings &outer);
        void                                             refresh_registry();
        void                                             report(syntax::SourceRange range, std::string message);

        const hgraph_ir::Module                                               &module_;
        syntax::DiagnosticSink                                                &diagnostics_;
        hgraph::TypeRegistry                                                  &registry_;
        hgraph::stdlib::RegisteredStandardTypes                                types_{};
        std::uint64_t                                                          generation_{0};
        std::unordered_map<std::uint32_t, const hgraph::ValueTypeMetaData *>   values_{};
        std::unordered_map<std::uint32_t, const hgraph::TSValueTypeMetaData *> schemas_{};
        /// Contracts by identity, so a nominal type finds its contract without a scan.
        std::unordered_map<std::string_view, const hgraph_ir::StructContract *> structures_{};
    };
}  // namespace hgl::wiring

#endif  // HGL_WIRING_TYPE_BRIDGE_H
