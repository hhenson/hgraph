#ifndef HGL_SEMANTICS_MODULE_CATALOG_H
#define HGL_SEMANTICS_MODULE_CATALOG_H

#include "native_contract.h"
#include <algorithm>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

namespace hgl::semantics
{
    /// Canonical scalar spellings at the package-catalog boundary. Keep this
    /// data model independent of both the syntax AST and compiler IR so a
    /// validated descriptor can be inspected without pulling in a frontend.
    enum class ImportedScalarType : std::uint8_t {
        Bool,
        I64,
        F64,
        Str,
        Date,
        Time,
        DateTime,
        Duration,
        CivilDateTime,
        ZonedDateTime,
        ZonedTime,
        TimeZone,
    };

    /// Native call phases copied out of a validated module descriptor. This
    /// catalog is a data-only name-resolution input; it owns no descriptor
    /// arena references and never loads package code.
    enum class NativeCallPhase : std::uint8_t {
        Wiring,
        Start,
        Evaluation,
        Stop,
    };

    enum class ImportedTypeKind : std::uint8_t {
        Scalar,
        Symbol,
        List,
        Set,
        Map,
        Rolling,
        Signal,
        Schema,
        /// `atomic<T>`. An imported struct's recursive edge is one (ADR 0012),
        /// so a layout can carry it even though no signature does.
        Atomic,
    };

    enum class ImportedConstantKind : std::uint8_t {
        None,
        Parameter,
        I64,
    };

    struct ImportedConstant
    {
        ImportedConstantKind kind{ImportedConstantKind::None};
        std::string          binding_identity{};
        std::int64_t         i64{};

        friend bool operator==(const ImportedConstant &, const ImportedConstant &) = default;
    };

    struct ImportedType
    {
        ImportedTypeKind          kind{ImportedTypeKind::Scalar};
        ImportedScalarType        scalar{ImportedScalarType::Bool};
        /// A generic parameter this type binds to, `m.fn::T`.
        std::string               binding_identity{};
        /// A nominal struct this type names, `m.Quote` (ADR 0013). Distinct
        /// from `binding_identity`: a parameter is substituted, a struct is
        /// registered under its owner's identity.
        std::string               nominal_identity{};
        std::vector<ImportedType> children{};
        ImportedConstant          size{};
        ImportedConstant          min_size{};
        bool                      unbounded{false};

        ImportedType() = default;
        ImportedType(ImportedScalarType value) : scalar{value} {}

        friend bool operator==(const ImportedType &, const ImportedType &) = default;
        friend bool operator==(const ImportedType &lhs, ImportedScalarType rhs) noexcept {
            return lhs.kind == ImportedTypeKind::Scalar && lhs.scalar == rhs;
        }
    };

    enum class NativeParameterAccess : std::uint8_t {
        Value,
        InputView,
    };

    struct ImportedGeneric
    {
        std::string                 name{};
        std::string                 binding_identity{};
        bool                        is_const{false};
        std::optional<ImportedType> type{};
    };

    struct ImportedParameter
    {
        std::string           name{};
        ImportedType          type{};
        bool                  is_const{false};
        NativeParameterAccess access{NativeParameterAccess::Value};
    };

    /// One native overload exported by an importable module. identity is the
    /// public overload-family identity; candidate_identity is unique within
    /// the descriptor and remains stable through typed lowering.
    struct ImportedFunction
    {
        std::string                    module_identity{};
        std::string                    name{};
        std::string                    identity{};
        std::string                    candidate_identity{};
        std::string                    cpp_symbol{};
        std::vector<ImportedGeneric>   generics{};
        std::vector<ImportedParameter> parameters{};
        std::optional<ImportedType>    result{};
        std::vector<NativeCallPhase>   phases{};
        /// Descriptor exception policy "translated": the call may raise.
        bool                     throws{false};
        std::vector<std::string> capabilities{};
        NativeExecutionRole      execution_role{NativeExecutionRole::LegacyValue};
        std::vector<std::string> public_headers{};
        std::vector<std::string> cmake_packages{};
        std::vector<std::string> imported_targets{};
        std::vector<std::string> runtime_images{};
        std::string              descriptor_fingerprint{};
        std::string              support_error{};
    };

    struct ImportedOperatorParameter
    {
        std::string  name{};
        std::string  binding_identity{};
        ImportedType type{};
        bool         is_const{false};
    };

    /// A public operator contract, not one of its implementation candidates.
    /// Identities and supported signature types are owned independently of the
    /// descriptor arena. A nonempty support_error makes the whole signature
    /// unavailable: consumers must not use a partially copied contract as an
    /// unconstrained replacement. Binding these records into HIR is separate.
    struct ImportedOperatorContract
    {
        std::string                            module_identity{};
        std::string                            name{};
        std::string                            identity{};
        std::string                            registry_name{};
        std::vector<ImportedGeneric>           generics{};
        std::vector<ImportedOperatorParameter> parameters{};
        std::optional<ImportedType>            result{};
        std::string                            descriptor_fingerprint{};
        std::string                            support_error{};
    };

    /// No constraint; an absent child of an imported requirement.
    inline constexpr std::uint32_t no_imported_constraint = 0xFFFFFFFFU;

    enum class ImportedConstraintKind : std::uint8_t {
        Symbol,
        Type,
        Value,
        Set,
        Call,
        Each,
        Operator,
        Relation,
        Not,
        Logic,
    };

    /// One node of an imported `where` requirement (ADR 0013). The shape
    /// mirrors the descriptor's normalized constraint and the typed HIR's, so
    /// lowering rebuilds it for the existing solver rather than a second
    /// checker. Children index the owning record's `constraints` arena.
    struct ImportedConstraint
    {
        ImportedConstraintKind      kind{ImportedConstraintKind::Symbol};
        std::string                 identity{};           ///< Symbol, Call, Operator, Each binding
        std::string                 registry_name{};      ///< Operator
        std::string                 operator_spelling{};  ///< Relation, Logic
        std::string                 relation_category{};  ///< Relation
        std::optional<ImportedType> type{};               ///< Type, Operator result
        ImportedConstant            value{};              ///< Value
        std::uint32_t               lhs{no_imported_constraint};
        std::uint32_t               rhs{no_imported_constraint};
        std::uint32_t               operand{no_imported_constraint};
        std::uint32_t               source{no_imported_constraint};
        std::uint32_t               body{no_imported_constraint};
        std::vector<std::uint32_t>  elements{};
        std::vector<std::uint32_t>  arguments{};
    };

    /// One field of an imported struct's layout. A recursive edge (ADR 0012)
    /// names its target by identity through `type`, exactly as the descriptor
    /// records it.
    struct ImportedStructField
    {
        std::string  name{};
        ImportedType type{};
        bool         optional{false};
        bool         recursive{false};
    };

    /// A struct another module exports (ADR 0013). The importer rebuilds the
    /// type from this layout and registers it under `identity`, the owning
    /// module's qualified name -- there is no copy under the importer's
    /// namespace. `public_headers` follows ImportedFunction: what a consumer
    /// needs in order to use it.
    struct ImportedStruct
    {
        std::string                      module_identity{};
        std::string                      name{};
        std::string                      identity{};
        bool                             abstract{false};
        std::vector<ImportedGeneric>     generics{};
        std::vector<ImportedStructField> fields{};
        std::vector<ImportedType>        parents{};
        std::vector<std::string>         public_headers{};
        /// The `where` requirement, rebuilt for the solver when this family is
        /// applied (ADR 0013). `requirements` indexes `constraints`.
        std::vector<ImportedConstraint>  constraints{};
        std::uint32_t                    requirements{no_imported_constraint};
        std::string                      descriptor_fingerprint{};
        std::string                      support_error{};
    };

    struct ImportableModule
    {
        std::string                           identity{};
        std::string                           descriptor_fingerprint{};
        std::vector<ImportedFunction>         functions{};
        std::vector<ImportedOperatorContract> operators{};
        std::vector<ImportedStruct>           structs{};
    };

    struct CatalogError
    {
        std::string path{};
        std::string message{};
    };

    /// The locked, explicitly supplied module surface for one compilation.
    /// Discovery and transitive-closure policy belong to the driver/package
    /// target; the semantic resolver receives only this deterministic value.
    class ModuleCatalog
    {
      public:
        [[nodiscard]] std::optional<CatalogError> add(ImportableModule module) {
            if (module.identity.empty()) { return CatalogError{"$.module.identity", "module identity must not be empty"}; }
            if (find(module.identity) != nullptr) {
                return CatalogError{"$.module.identity", "module '" + module.identity + "' is supplied more than once"};
            }
            for (ImportedFunction &function : module.functions) {
                if (function.candidate_identity.empty()) { function.candidate_identity = function.identity; }
            }
            std::ranges::sort(module.functions, [](const ImportedFunction &lhs, const ImportedFunction &rhs) {
                return std::tie(lhs.name, lhs.candidate_identity) < std::tie(rhs.name, rhs.candidate_identity);
            });
            for (std::size_t index = 1; index < module.functions.size(); ++index) {
                if (module.functions[index - 1U].candidate_identity == module.functions[index].candidate_identity) {
                    return CatalogError{"$.native.declarations",
                                        "module '" + module.identity + "' exports native overload candidate '" +
                                            module.functions[index].candidate_identity + "' more than once"};
                }
            }
            std::ranges::sort(module.operators, {}, &ImportedOperatorContract::name);
            for (std::size_t index = 0; index < module.operators.size(); ++index) {
                const ImportedOperatorContract &contract = module.operators[index];
                if (index != 0U && module.operators[index - 1U].name == contract.name) {
                    return CatalogError{"$.interface",
                                        "module '" + module.identity + "' exports operator '" + contract.name + "' more than once"};
                }
                if (std::ranges::binary_search(module.functions, contract.name, {}, &ImportedFunction::name)) {
                    return CatalogError{"$.interface", "module '" + module.identity +
                                                           "' exports both an operator and a native function named '" +
                                                           contract.name + "'"};
                }
            }
            std::ranges::sort(module.structs, {}, &ImportedStruct::name);
            for (std::size_t index = 0; index < module.structs.size(); ++index) {
                const ImportedStruct &structure = module.structs[index];
                if (index != 0U && module.structs[index - 1U].name == structure.name) {
                    return CatalogError{"$.interface",
                                        "module '" + module.identity + "' exports struct '" + structure.name + "' more than once"};
                }
                // A struct and a callable of one name would make a qualified
                // spelling ambiguous between a type and a value position.
                if (std::ranges::binary_search(module.functions, structure.name, {}, &ImportedFunction::name) ||
                    std::ranges::binary_search(module.operators, structure.name, {}, &ImportedOperatorContract::name)) {
                    return CatalogError{"$.interface", "module '" + module.identity +
                                                           "' exports both a struct and a callable named '" +
                                                           structure.name + "'"};
                }
            }
            modules_.push_back(std::move(module));
            std::ranges::sort(modules_, {}, &ImportableModule::identity);
            return std::nullopt;
        }

        [[nodiscard]] const ImportableModule *find(std::string_view identity) const noexcept {
            const auto found = std::ranges::lower_bound(modules_, identity, {}, &ImportableModule::identity);
            return found != modules_.end() && found->identity == identity ? &*found : nullptr;
        }

        [[nodiscard]] const ImportedFunction *find_function(std::string_view module, std::string_view name) const noexcept {
            const std::span<const ImportedFunction> functions = find_functions(module, name);
            return functions.empty() ? nullptr : &functions.front();
        }

        [[nodiscard]] const ImportedOperatorContract *find_operator(std::string_view module, std::string_view name) const noexcept {
            const ImportableModule *owner = find(module);
            if (owner == nullptr) { return nullptr; }
            const auto found = std::ranges::lower_bound(owner->operators, name, {}, &ImportedOperatorContract::name);
            return found != owner->operators.end() && found->name == name ? &*found : nullptr;
        }

        /// A struct by its qualified identity, `m.Quote` (ADR 0013). Walking an
        /// imported struct's ancestry needs this: a parent is recorded as a
        /// nominal identity, not as a module and name.
        [[nodiscard]] const ImportedStruct *find_struct_by_identity(std::string_view identity) const noexcept {
            for (const ImportableModule &module : modules_) {
                if (identity.size() <= module.identity.size() + 1U) { continue; }
                if (!identity.starts_with(module.identity) || identity[module.identity.size()] != '.') { continue; }
                const std::string_view name = identity.substr(module.identity.size() + 1U);
                const auto found = std::ranges::lower_bound(module.structs, name, {}, &ImportedStruct::name);
                if (found != module.structs.end() && found->name == name) { return &*found; }
            }
            return nullptr;
        }

        /// A struct another module exports (ADR 0013); nullptr when the module
        /// is absent or exports no struct of that name.
        [[nodiscard]] const ImportedStruct *find_struct(std::string_view module, std::string_view name) const noexcept {
            const ImportableModule *owner = find(module);
            if (owner == nullptr) { return nullptr; }
            const auto found = std::ranges::lower_bound(owner->structs, name, {}, &ImportedStruct::name);
            return found != owner->structs.end() && found->name == name ? &*found : nullptr;
        }

        [[nodiscard]] std::span<const ImportedFunction> find_functions(std::string_view module,
                                                                       std::string_view name) const noexcept {
            const ImportableModule *owner = find(module);
            if (owner == nullptr) { return {}; }
            const auto found = std::ranges::lower_bound(owner->functions, name, {}, &ImportedFunction::name);
            if (found == owner->functions.end() || found->name != name) { return {}; }
            const auto last = std::ranges::upper_bound(found, owner->functions.end(), name, {}, &ImportedFunction::name);
            return {&*found, static_cast<std::size_t>(last - found)};
        }

        [[nodiscard]] const std::vector<ImportableModule> &modules() const noexcept { return modules_; }

      private:
        std::vector<ImportableModule> modules_{};
    };
}  // namespace hgl::semantics

#endif  // HGL_SEMANTICS_MODULE_CATALOG_H
