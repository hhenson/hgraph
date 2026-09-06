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
    };

    /// One exact value type in a native signature. Native identities must be
    /// declared by the same Package; temporal and container types are not
    /// representable in this API.
    struct ValueType
    {
        ValueTypeCategory category{ValueTypeCategory::Scalar};
        ScalarType        scalar{ScalarType::Bool};
        std::string       native_identity{};

        [[nodiscard]] static ValueType canonical(ScalarType type) { return ValueType{.scalar = type}; }

        [[nodiscard]] static ValueType native(std::string identity) {
            return ValueType{.category = ValueTypeCategory::Native, .native_identity = std::move(identity)};
        }

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
        std::string name{};
        ValueType   type{};
        bool        is_const{false};
        ValuePolicy policy{};
    };

    /// One complete, nongeneric native declaration. cpp_symbol names either a
    /// directly callable public C++ symbol or a package-owned wrapper which
    /// has already normalized overload, template, exception, and ownership
    /// details into this exact contract.
    struct Declaration
    {
        DeclarationCategory      category{DeclarationCategory::Function};
        std::string              identity{};
        std::string              cpp_symbol{};
        std::vector<Parameter>   parameters{};
        std::optional<ValueType> result_type{};
        ValuePolicy              result_policy{};
        std::vector<Phase>       phases{};
        std::vector<Effect>      effects{};
        ExceptionPolicy          exception_policy{ExceptionPolicy::NoThrow};
        ThreadSafety             thread_safety{ThreadSafety::NodeLocal};
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
    /// arbitrary HGL schema shapes, generic declarations, raw pointers, or
    /// callbacks. descriptor_json() normalizes set-like inventories, seals the
    /// descriptor, and applies the compiler's ordinary descriptor validator.
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
