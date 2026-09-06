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
        Reference,
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

    enum class NativeTypeCategory : std::uint8_t {
        OpaqueState,
        AtomicValue,
    };

    /// A reviewed native type associated with a nominal HGL identity. Opaque
    /// state never crosses a temporal port; an atomic value additionally needs
    /// the hgraph value/storage operations described by the native interface.
    struct NativeTypeDeclaration
    {
        NativeTypeCategory category{NativeTypeCategory::OpaqueState};
        std::string        identity{};
        std::string        cpp_type{};
        std::string        public_header{};

        friend bool operator==(const NativeTypeDeclaration &, const NativeTypeDeclaration &) = default;
    };

    enum class NativeDeclarationCategory : std::uint8_t {
        Function,
        Constructor,
        Lifecycle,
    };

    enum class NativePhase : std::uint8_t {
        Wiring,
        Start,
        Evaluation,
        Stop,
    };

    enum class NativeEffect : std::uint8_t {
        Mutation,
        InputOutput,
        Blocking,
        Allocation,
    };

    enum class NativeOwnership : std::uint8_t {
        Value,
        Owned,
        Shared,
        Borrowed,
    };

    enum class NativeExceptionPolicy : std::uint8_t {
        NoThrow,
        Translated,
    };

    enum class NativeThreadSafety : std::uint8_t {
        NodeLocal,
        ThreadSafe,
        Serialized,
    };

    struct NativeValuePolicy
    {
        NativeOwnership ownership{NativeOwnership::Value};
        /// Parameter name whose lifetime bounds this borrowed value. Empty for
        /// value/owned/shared results and for call-confined borrowed arguments.
        std::string dependent_on{};
        bool        mutable_value{false};

        friend bool operator==(const NativeValuePolicy &, const NativeValuePolicy &) = default;
    };

    struct NativeParameterPolicy
    {
        std::string       name{};
        NativeValuePolicy value{};

        friend bool operator==(const NativeParameterPolicy &, const NativeParameterPolicy &) = default;
    };

    /// One exact native operation. Its HGL signature uses the shared schema
    /// arenas; the remaining fields make phase, effects, ownership, exception
    /// handling, and direct C++ lowering explicit.
    struct NativeDeclaration
    {
        NativeDeclarationCategory          category{NativeDeclarationCategory::Function};
        std::string                        identity{};
        std::string                        cpp_symbol{};
        Signature                          signature{};
        std::vector<NativePhase>           phases{};
        std::vector<NativeEffect>          effects{};
        std::vector<NativeParameterPolicy> parameters{};
        NativeValuePolicy                  result{};
        NativeExceptionPolicy              exception_policy{NativeExceptionPolicy::NoThrow};
        NativeThreadSafety                 thread_safety{NativeThreadSafety::NodeLocal};

        friend bool operator==(const NativeDeclaration &, const NativeDeclaration &) = default;
    };

    struct LifecycleMetadata
    {
        std::uint32_t abi_version{0};
        std::string   query_symbol{};

        friend bool operator==(const LifecycleMetadata &, const LifecycleMetadata &) = default;
    };

    struct BuildMetadata
    {
        std::vector<std::string> public_headers{};
        std::vector<std::string> cmake_packages{};
        std::vector<std::string> imported_targets{};
        std::vector<std::string> runtime_images{};
        std::string              registration_symbol{};
        LifecycleMetadata        lifecycle{};

        friend bool operator==(const BuildMetadata &, const BuildMetadata &) = default;
    };

    /// A data-only package boundary: public/provider and exact-native
    /// declarations refer to structured type, compile-time expression, and
    /// constraint arenas. The fingerprint is sealed separately so it can be
    /// computed over the canonical descriptor with that field empty.
    struct ModuleDescriptor
    {
        std::uint32_t                         format_version{module_descriptor_format_version};
        std::string                           module_identity{};
        std::string                           language_version{};
        std::string                           descriptor_fingerprint{};
        std::vector<InterfaceDeclaration>     interface{};
        std::string                           provider_identity{};
        std::vector<Implementation>           implementations{};
        std::vector<std::string>              provider_requirements{};
        std::vector<TypeRecord>               types{};
        std::vector<ConstantExpressionRecord> constant_expressions{};
        std::vector<ConstraintRecord>         constraints{};
        std::vector<NativeTypeDeclaration>    native_types{};
        std::vector<NativeDeclaration>        native_declarations{};
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
        std::vector<std::string> runtime_images{};
        std::string              registration_symbol{};
        LifecycleMetadata        lifecycle{};
    };

    /// Build a normalized module descriptor from the execution-facing IR.
    /// Public declarations are visited by stable identity; set-like package
    /// inventories are sorted and deduplicated. Only schema records reachable
    /// from the public interface and provider inventory are retained.
    [[nodiscard]] ModuleDescriptor describe_module(const hgraph_ir::Module &module, DescribeOptions options);

    /// Serialize canonical, reviewable UTF-8 JSON with a trailing newline.
    /// Object key order and array order are deterministic.
    [[nodiscard]] std::string to_json(const ModuleDescriptor &descriptor);

    /// Compute the canonical version-one semantic-model SHA-256 fingerprint
    /// with the fingerprint field empty, then prefix the lowercase digest with
    /// `sha256:`. Compatible unknown JSON members are outside this projection.
    [[nodiscard]] std::string fingerprint(const ModuleDescriptor &descriptor);

    /// Replace descriptor_fingerprint with fingerprint(descriptor).
    void seal(ModuleDescriptor &descriptor);
}  // namespace hgl::descriptor

#endif  // HGL_DESCRIPTOR_MODULE_DESCRIPTOR_H
