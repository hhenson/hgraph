#ifndef HGL_DESCRIPTOR_MODULE_DESCRIPTOR_H
#define HGL_DESCRIPTOR_MODULE_DESCRIPTOR_H

#include "hgraph_ir/ir.h"

#include <cstdint>
#include <string>
#include <vector>

namespace hgl::descriptor
{
    inline constexpr std::uint32_t module_descriptor_format_version = 1;

    enum class DeclarationCategory : std::uint8_t {
        Structure,
        Operator,
        Function,
    };

    enum class ExecutionKind : std::uint8_t {
        None,
        Composition,
        RuntimeNode,
    };

    struct InterfaceDeclaration
    {
        DeclarationCategory category{DeclarationCategory::Function};
        std::string         identity{};
        std::string         registry_name{};
        ExecutionKind       execution{ExecutionKind::None};
        bool                abstract{false};

        friend bool operator==(const InterfaceDeclaration &, const InterfaceDeclaration &) = default;
    };

    struct Implementation
    {
        std::string   identity{};
        std::string   operator_identity{};
        std::string   operator_registry_name{};
        ExecutionKind execution{ExecutionKind::Composition};

        friend bool operator==(const Implementation &, const Implementation &) = default;
    };

    struct BuildMetadata
    {
        std::vector<std::string> public_headers{};
        std::vector<std::string> cmake_packages{};
        std::vector<std::string> imported_targets{};
        std::string              registration_symbol{};

        friend bool operator==(const BuildMetadata &, const BuildMetadata &) = default;
    };

    /// The first data-only descriptor boundary. Interface signatures and
    /// constraints are intentionally a following schema addition: this
    /// checkpoint versions the envelope, public/provider inventories, and
    /// build boundary without claiming descriptor-only type checking.
    struct ModuleDescriptor
    {
        std::uint32_t                     format_version{module_descriptor_format_version};
        std::string                       module_identity{};
        std::string                       language_version{};
        std::vector<InterfaceDeclaration> interface{};
        std::string                       provider_identity{};
        std::vector<Implementation>       implementations{};
        std::vector<std::string>          provider_requirements{};
        BuildMetadata                     build{};

        friend bool operator==(const ModuleDescriptor &, const ModuleDescriptor &) = default;
    };

    struct DescribeOptions
    {
        std::string              language_version{};
        std::string              provider_identity{};
        std::vector<std::string> public_headers{};
        std::vector<std::string> cmake_packages{};
        std::vector<std::string> imported_targets{};
        std::string              registration_symbol{};
    };

    /// Build a normalized module descriptor from the execution-facing IR.
    /// Set-like inventories are sorted and deduplicated so serialization is
    /// independent of incidental insertion order.
    [[nodiscard]] ModuleDescriptor describe_module(const hgraph_ir::Module &module, DescribeOptions options);

    /// Serialize canonical, reviewable UTF-8 JSON with a trailing newline.
    /// Object key order and array order are deterministic.
    [[nodiscard]] std::string to_json(const ModuleDescriptor &descriptor);
}  // namespace hgl::descriptor

#endif  // HGL_DESCRIPTOR_MODULE_DESCRIPTOR_H
