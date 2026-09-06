#ifndef HGL_SEMANTICS_MODULE_CATALOG_H
#define HGL_SEMANTICS_MODULE_CATALOG_H

#include <algorithm>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
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

    struct ImportedParameter
    {
        std::string        name{};
        ImportedScalarType type{ImportedScalarType::Bool};
        bool               is_const{false};
    };

    /// One exact native function exported by an importable module. The first
    /// executable slice admits effect-free canonical scalar signatures only.
    /// Other descriptor declarations remain visible through support_error so
    /// name resolution fails with the actual unsupported boundary rather than
    /// pretending that the declaration does not exist.
    struct ImportedFunction
    {
        std::string                       module_identity{};
        std::string                       name{};
        std::string                       identity{};
        std::string                       cpp_symbol{};
        std::vector<ImportedParameter>    parameters{};
        std::optional<ImportedScalarType> result{};
        std::vector<NativeCallPhase>      phases{};
        std::vector<std::string>          public_headers{};
        std::vector<std::string>          cmake_packages{};
        std::vector<std::string>          imported_targets{};
        std::vector<std::string>          runtime_images{};
        std::string                       descriptor_fingerprint{};
        std::string                       support_error{};
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
            std::ranges::sort(module.functions, {}, &ImportedFunction::name);
            for (std::size_t index = 1; index < module.functions.size(); ++index) {
                if (module.functions[index - 1U].name == module.functions[index].name) {
                    return CatalogError{"$.native.declarations", "module '" + module.identity + "' exports native function '" +
                                                                     module.functions[index].name + "' more than once"};
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
            const ImportableModule *owner = find(module);
            if (owner == nullptr) { return nullptr; }
            const auto found = std::ranges::lower_bound(owner->functions, name, {}, &ImportedFunction::name);
            return found != owner->functions.end() && found->name == name ? &*found : nullptr;
        }

        [[nodiscard]] const std::vector<ImportableModule> &modules() const noexcept { return modules_; }

      private:
        std::vector<ImportableModule> modules_{};
    };
}  // namespace hgl::semantics

#endif  // HGL_SEMANTICS_MODULE_CATALOG_H
