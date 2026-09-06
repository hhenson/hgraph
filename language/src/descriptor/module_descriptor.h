#ifndef HGL_DESCRIPTOR_MODULE_DESCRIPTOR_H
#define HGL_DESCRIPTOR_MODULE_DESCRIPTOR_H

#include "hgraph_ir/ir.h"

#include <cstdint>
#include <limits>
#include <optional>
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

    using SchemaId                         = std::uint32_t;
    inline constexpr SchemaId no_schema_id = std::numeric_limits<SchemaId>::max();

    enum class TypeCategory : std::uint8_t {
        Void,
        Scalar,
        Symbol,
        Tuple,
        List,
        Set,
        Map,
        Rolling,
        Atomic,
        Iterator,
        Callable,
        Capability,
        HarnessSequence,
        Deferred,
    };

    enum class TypeArgumentCategory : std::uint8_t {
        Type,
        Constant,
    };

    struct TypeArgument
    {
        TypeArgumentCategory category{TypeArgumentCategory::Type};
        SchemaId             reference{no_schema_id};

        friend bool operator==(const TypeArgument &, const TypeArgument &) = default;
    };

    /// One canonical type reachable from the public interface or provider
    /// inventory. References index ModuleDescriptor::types; generic symbols
    /// retain their stable declaration-scoped binding identity.
    struct TypeRecord
    {
        TypeCategory              category{TypeCategory::Deferred};
        std::string               scalar_name{};
        std::string               nominal_identity{};
        std::string               binding_identity{};
        std::vector<SchemaId>     children{};
        std::vector<TypeArgument> arguments{};
        SchemaId                  size{no_schema_id};
        SchemaId                  min_size{no_schema_id};
        bool                      unbounded{false};

        friend bool operator==(const TypeRecord &, const TypeRecord &) = default;
    };

    enum class ConstantExpressionCategory : std::uint8_t {
        Literal,
        Parameter,
        Unary,
        Binary,
        Index,
        Field,
        Sequence,
        Tuple,
        Construct,
    };

    struct ConstantElement
    {
        SchemaId key{no_schema_id};
        SchemaId value{no_schema_id};

        friend bool operator==(const ConstantElement &, const ConstantElement &) = default;
    };

    struct ConstantArgument
    {
        std::string name{};
        SchemaId    value{no_schema_id};

        friend bool operator==(const ConstantArgument &, const ConstantArgument &) = default;
    };

    /// A structured compile-time expression. Literal kinds remain explicit in
    /// JSON, so an i64, f64, temporal value, null, and placeholder never rely
    /// on JSON's narrower value model for their type identity.
    struct ConstantExpressionRecord
    {
        ConstantExpressionCategory       category{ConstantExpressionCategory::Literal};
        std::optional<ir::hir::Constant> literal{};
        std::string                      parameter_identity{};
        std::string                      operator_spelling{};
        SchemaId                         lhs{no_schema_id};
        SchemaId                         rhs{no_schema_id};
        std::string                      member{};
        std::vector<ConstantElement>     elements{};
        std::vector<SchemaId>            items{};
        SchemaId                         constructed_type{no_schema_id};
        std::vector<ConstantArgument>    arguments{};
        bool                             delta{false};

        friend bool operator==(const ConstantExpressionRecord &, const ConstantExpressionRecord &) = default;
    };

    enum class ConstraintCategory : std::uint8_t {
        Symbol,
        Type,
        Value,
        Set,
        Call,
        Operator,
        Relation,
        Not,
        Logic,
    };

    /// A normalized constraint node. Constraint references index
    /// ModuleDescriptor::constraints; type/value references use their
    /// corresponding schema arenas.
    struct ConstraintRecord
    {
        ConstraintCategory    category{ConstraintCategory::Symbol};
        std::string           identity{};
        std::string           registry_name{};
        std::string           operator_spelling{};
        std::string           relation_category{};
        SchemaId              type{no_schema_id};
        SchemaId              value{no_schema_id};
        SchemaId              lhs{no_schema_id};
        SchemaId              rhs{no_schema_id};
        SchemaId              operand{no_schema_id};
        SchemaId              result{no_schema_id};
        std::vector<SchemaId> elements{};
        std::vector<SchemaId> arguments{};

        friend bool operator==(const ConstraintRecord &, const ConstraintRecord &) = default;
    };

    struct GenericParameter
    {
        std::string name{};
        std::string binding_identity{};
        bool        is_const{false};
        SchemaId    type{no_schema_id};

        friend bool operator==(const GenericParameter &, const GenericParameter &) = default;
    };

    struct Parameter
    {
        std::string name{};
        std::string binding_identity{};
        bool        is_const{false};
        SchemaId    type{no_schema_id};
        SchemaId    default_value{no_schema_id};

        friend bool operator==(const Parameter &, const Parameter &) = default;
    };

    struct Signature
    {
        std::vector<GenericParameter> generics{};
        std::vector<Parameter>        parameters{};
        SchemaId                      result{no_schema_id};
        SchemaId                      requirements{no_schema_id};

        friend bool operator==(const Signature &, const Signature &) = default;
    };

    struct StructField
    {
        std::string name{};
        SchemaId    type{no_schema_id};
        SchemaId    default_value{no_schema_id};
        std::string origin_identity{};
        bool        optional{false};

        friend bool operator==(const StructField &, const StructField &) = default;
    };

    struct InterfaceDeclaration
    {
        DeclarationCategory      category{DeclarationCategory::Function};
        std::string              identity{};
        std::string              registry_name{};
        ExecutionKind            execution{ExecutionKind::None};
        bool                     abstract{false};
        Signature                signature{};
        std::vector<SchemaId>    parents{};
        std::vector<StructField> fields{};

        friend bool operator==(const InterfaceDeclaration &, const InterfaceDeclaration &) = default;
    };

    struct Implementation
    {
        std::string   identity{};
        std::string   operator_identity{};
        std::string   operator_registry_name{};
        ExecutionKind execution{ExecutionKind::Composition};
        Signature     signature{};

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

    /// A data-only package boundary: public/provider declarations refer to
    /// structured type, compile-time expression, and constraint arenas. Native
    /// lifecycle and ownership/effect policy remain separate ABI additions.
    struct ModuleDescriptor
    {
        std::uint32_t                         format_version{module_descriptor_format_version};
        std::string                           module_identity{};
        std::string                           language_version{};
        std::vector<InterfaceDeclaration>     interface{};
        std::string                           provider_identity{};
        std::vector<Implementation>           implementations{};
        std::vector<std::string>              provider_requirements{};
        std::vector<TypeRecord>               types{};
        std::vector<ConstantExpressionRecord> constant_expressions{};
        std::vector<ConstraintRecord>         constraints{};
        BuildMetadata                         build{};

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
    /// Public declarations are visited by stable identity; set-like package
    /// inventories are sorted and deduplicated. Only schema records reachable
    /// from the public interface and provider inventory are retained.
    [[nodiscard]] ModuleDescriptor describe_module(const hgraph_ir::Module &module, DescribeOptions options);

    /// Serialize canonical, reviewable UTF-8 JSON with a trailing newline.
    /// Object key order and array order are deterministic.
    [[nodiscard]] std::string to_json(const ModuleDescriptor &descriptor);
}  // namespace hgl::descriptor

#endif  // HGL_DESCRIPTOR_MODULE_DESCRIPTOR_H
