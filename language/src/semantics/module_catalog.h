#ifndef HGL_SEMANTICS_MODULE_CATALOG_H
#define HGL_SEMANTICS_MODULE_CATALOG_H

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
        std::string               binding_identity{};
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
        std::vector<std::string>       public_headers{};
        std::vector<std::string>       cmake_packages{};
        std::vector<std::string>       imported_targets{};
        std::vector<std::string>       runtime_images{};
        std::string                    descriptor_fingerprint{};
        std::string                    support_error{};
    };

    struct ImportableModule
    {
        std::string                   identity{};
        std::string                   descriptor_fingerprint{};
        std::vector<ImportedFunction> functions{};
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
