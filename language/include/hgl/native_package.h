#ifndef HGL_NATIVE_PACKAGE_H
#define HGL_NATIVE_PACKAGE_H

#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#if defined(_WIN32)
    #if defined(HGL_NATIVE_PACKAGE_BUILD)
        #define HGL_NATIVE_PACKAGE_API __declspec(dllexport)
    #else
        #define HGL_NATIVE_PACKAGE_API __declspec(dllimport)
    #endif
#elif defined(__GNUC__) || defined(__clang__)
    #define HGL_NATIVE_PACKAGE_API __attribute__((visibility("default")))
#else
    #define HGL_NATIVE_PACKAGE_API
#endif

namespace hgl::native
{
    /// Canonical scalar values admitted by the first native-call ABI.
    enum class ScalarType : std::uint8_t {
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

    enum class ValueTypeCategory : std::uint8_t {
        Scalar,
        Native,
        TypeParameter,
        List,
        Set,
        Map,
        Rolling,
        Signal,
    };

    /// One HGL value pattern in a native signature. Type parameters name a
    /// generic declared by the same Declaration. Collection patterns describe
    /// the HGL source type; an InputView parameter receives the corresponding
    /// live hgraph input view rather than a materialized payload.
    struct ValueType
    {
        ValueTypeCategory      category{ValueTypeCategory::Scalar};
        ScalarType             scalar{ScalarType::Bool};
        std::string            native_identity{};
        std::string            parameter_name{};
        std::vector<ValueType> children{};
        std::string            size_parameter{};
        std::string            min_size_parameter{};
        bool                   unbounded{false};

        [[nodiscard]] static ValueType canonical(ScalarType type) { return ValueType{.scalar = type}; }

        [[nodiscard]] static ValueType native(std::string identity) {
            return ValueType{.category = ValueTypeCategory::Native, .native_identity = std::move(identity)};
        }

        [[nodiscard]] static ValueType type_parameter(std::string name) {
            return ValueType{.category = ValueTypeCategory::TypeParameter, .parameter_name = std::move(name)};
        }

        [[nodiscard]] static ValueType list(ValueType element, std::string size = {}, bool is_unbounded = false) {
            return ValueType{.category       = ValueTypeCategory::List,
                             .children       = {std::move(element)},
                             .size_parameter = std::move(size),
                             .unbounded      = is_unbounded};
        }

        [[nodiscard]] static ValueType set(ValueType element) {
            return ValueType{.category = ValueTypeCategory::Set, .children = {std::move(element)}};
        }

        [[nodiscard]] static ValueType map(ValueType key, ValueType value) {
            return ValueType{.category = ValueTypeCategory::Map, .children = {std::move(key), std::move(value)}};
        }

        [[nodiscard]] static ValueType rolling(ValueType element, std::string size, std::string min_size = {}) {
            return ValueType{.category           = ValueTypeCategory::Rolling,
                             .children           = {std::move(element)},
                             .size_parameter     = std::move(size),
                             .min_size_parameter = std::move(min_size)};
        }

        /// A payload-erased live hgraph input. This type is valid only for a
        /// non-const parameter whose access policy is InputView.
        [[nodiscard]] static ValueType signal() { return ValueType{.category = ValueTypeCategory::Signal}; }

        friend bool operator==(const ValueType &, const ValueType &) = default;
    };

    enum class TypeCategory : std::uint8_t {
        OpaqueState,
        AtomicValue,
    };

    /// A nominal native type exposed by one package. OpaqueState is private
    /// node state; AtomicValue additionally requires public hgraph value and
    /// storage metadata supplied by the package.
    struct Type
    {
        TypeCategory category{TypeCategory::OpaqueState};
        std::string  identity{};
        std::string  cpp_type{};
        std::string  public_header{};
    };

    enum class DeclarationCategory : std::uint8_t {
        Function,
        Constructor,
        Lifecycle,
    };

    enum class Phase : std::uint8_t {
        Wiring,
        Start,
        Evaluation,
        Stop,
    };

    enum class Effect : std::uint8_t {
        Mutation,
        InputOutput,
        Blocking,
        Allocation,
    };

    enum class Ownership : std::uint8_t {
        Value,
        Owned,
        Shared,
        Borrowed,
    };

    enum class ParameterAccess : std::uint8_t {
        Value,
        InputView,
    };

    enum class ExceptionPolicy : std::uint8_t {
        NoThrow,
        Translated,
    };

    enum class ThreadSafety : std::uint8_t {
        NodeLocal,
        ThreadSafe,
        Serialized,
    };

    struct ValuePolicy
    {
        Ownership   ownership{Ownership::Value};
        std::string dependent_on{};
        bool        mutable_value{false};
    };

    struct Parameter
    {
        std::string     name{};
        ValueType       type{};
        bool            is_const{false};
        ValuePolicy     policy{};
        ParameterAccess access{ParameterAccess::Value};
    };

    struct GenericParameter
    {
        std::string              name{};
        bool                     is_const{false};
        std::optional<ValueType> type{};
    };

    /// One native overload. Declarations may share identity when their HGL
    /// signatures differ. cpp_symbol names either a directly callable public
    /// C++ overload or a package-owned wrapper which has normalized exception
    /// and ownership details into this contract.
    struct Declaration
    {
        DeclarationCategory           category{DeclarationCategory::Function};
        std::string                   identity{};
        std::string                   cpp_symbol{};
        std::vector<GenericParameter> generics{};
        std::vector<Parameter>        parameters{};
        std::optional<ValueType>      result_type{};
        ValuePolicy                   result_policy{};
        std::vector<Phase>            phases{};
        std::vector<Effect>           effects{};
        ExceptionPolicy               exception_policy{ExceptionPolicy::NoThrow};
        ThreadSafety                  thread_safety{ThreadSafety::NodeLocal};
    };

    struct Lifecycle
    {
        std::uint32_t abi_version{};
        std::string   query_symbol{};
    };

    struct Build
    {
        std::vector<std::string> public_headers{};
        std::vector<std::string> cmake_packages{};
        std::vector<std::string> imported_targets{};
        std::vector<std::string> runtime_images{};
        std::string              registration_symbol{};
        std::optional<Lifecycle> lifecycle{};
    };

    /// The deliberately small native-package authoring model. It cannot name
    /// arbitrary HGL schema shapes, raw pointers, or callbacks. Its generic
    /// surface is deliberately limited to structural collection-view patterns.
    /// descriptor_json() normalizes set-like inventories, seals the descriptor,
    /// and applies the compiler's ordinary descriptor validator.
    struct Package
    {
        std::string              module_identity{};
        std::string              language_version{};
        std::string              provider_identity{};
        std::vector<std::string> provider_requirements{};
        std::vector<Type>        types{};
        std::vector<Declaration> declarations{};
        Build                    build{};
    };

    /// Produce canonical, fingerprinted module-descriptor JSON. Throws
    /// std::invalid_argument when the package violates the native safety
    /// envelope.
    [[nodiscard]] HGL_NATIVE_PACKAGE_API std::string descriptor_json(const Package &package);

    /// Write descriptor_json(package) to path. Throws std::system_error for an
    /// I/O failure and std::invalid_argument for an invalid package.
    HGL_NATIVE_PACKAGE_API void write_descriptor(const Package &package, const std::filesystem::path &path);
}  // namespace hgl::native

#endif  // HGL_NATIVE_PACKAGE_H
