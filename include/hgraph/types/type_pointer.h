#ifndef HGRAPH_TYPES_TYPE_POINTER_H
#define HGRAPH_TYPES_TYPE_POINTER_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/metadata/type_record.h>
#include <hgraph/util/tagged_ptr.h>

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string_view>
#include <type_traits>

namespace hgraph
{
    class ValueTypeRef;
    class NodeTypeRef;
    class GraphTypeRef;
    class ExecutorTypeRef;
    class ClockTypeRef;
    template <TypeRole Role> class BasicTSTypeRef;
    enum class AccessMode : std::uintptr_t
    {
        ReadOnly = 0,
        Writable = 1,
        Mutation = 2,
    };

    static_assert(alignof(TypeRecord) >= 4, "TypeRecord must provide two low tag bits");

    using TaggedTypeRecordPtr = tagged_ptr<const TypeRecord, 2, AccessMode>;

    namespace detail
    {
        struct TypePointerLayoutAccess;
    }

    class HGRAPH_EXPORT AnyPtr
    {
    public:
        constexpr AnyPtr() noexcept = default;

        [[nodiscard]] static AnyPtr typed_null(const TypeRecord &record);
        [[nodiscard]] static AnyPtr read_only(const TypeRecord &record, const void *data);
        [[nodiscard]] static AnyPtr writable(const TypeRecord &record, void *data);

        [[nodiscard]] constexpr const TypeRecord *record() const noexcept { return type_.ptr(); }
        [[nodiscard]] constexpr const void *data() const noexcept { return data_; }
        [[nodiscard]] constexpr AccessMode access_mode() const noexcept { return type_.enum_value(); }

        [[nodiscard]] bool well_formed() const noexcept;
        [[nodiscard]] constexpr bool is_unbound() const noexcept { return type_.raw_bits() == 0 && data_ == nullptr; }
        [[nodiscard]] bool bound() const noexcept;
        [[nodiscard]] bool is_typed_null() const noexcept;
        [[nodiscard]] bool has_value() const noexcept;
        [[nodiscard]] bool valid() const noexcept;
        [[nodiscard]] explicit operator bool() const noexcept { return valid(); }

        [[nodiscard]] const SchemaHeader *schema() const noexcept;
        [[nodiscard]] const MemoryUtils::StoragePlan *plan() const noexcept;
        [[nodiscard]] const void *ops() const noexcept;
        [[nodiscard]] const DebugDescriptor *debug() const noexcept;
        [[nodiscard]] TypeClassification classification() const noexcept;
        [[nodiscard]] TypeFamily family() const noexcept;
        [[nodiscard]] TypeRole role() const noexcept;
        [[nodiscard]] TypeKind kind() const noexcept;
        [[nodiscard]] TypeCapabilities capabilities() const noexcept;
        [[nodiscard]] std::string_view semantic_name() const noexcept;
        [[nodiscard]] std::string_view implementation_name() const noexcept;
        [[nodiscard]] std::string_view effective_name() const noexcept;

        [[nodiscard]] constexpr bool read_only_access() const noexcept { return access_mode() == AccessMode::ReadOnly; }
        [[nodiscard]] constexpr bool writable_access() const noexcept
        {
            return access_mode() == AccessMode::Writable || access_mode() == AccessMode::Mutation;
        }
        [[nodiscard]] constexpr bool mutation_access() const noexcept { return access_mode() == AccessMode::Mutation; }

        [[nodiscard]] AnyPtr as_read_only() const;
        [[nodiscard]] AnyPtr begin_mutation() const;
        [[nodiscard]] AnyPtr end_mutation() const;
        [[nodiscard]] void *mutable_data() const;

        [[nodiscard]] friend constexpr bool operator==(const AnyPtr &lhs, const AnyPtr &rhs) noexcept
        {
            return lhs.record() == rhs.record() && lhs.data_ == rhs.data_;
        }

        [[nodiscard]] constexpr bool same_access_as(const AnyPtr &other) const noexcept
        {
            return access_mode() == other.access_mode();
        }

        [[nodiscard]] constexpr bool same_state_as(const AnyPtr &other) const noexcept
        {
            return type_.raw_bits() == other.type_.raw_bits() && data_ == other.data_;
        }

    private:
        template <TypeFamily Family, TypeRole Role> friend class TypedPtr;
        friend class ValueTypeRef;
        friend class NodeTypeRef;
        friend class GraphTypeRef;
        friend class ExecutorTypeRef;
        friend class ClockTypeRef;
        template <TypeRole Role> friend class BasicTSTypeRef;
        friend struct detail::TypePointerLayoutAccess;

        constexpr AnyPtr(const TypeRecord *record, const void *data, AccessMode access) noexcept
            : type_(record, access), data_(data)
        {
        }

        TaggedTypeRecordPtr type_{};
        const void *data_{nullptr};
    };

    namespace detail
    {
        struct TypePointerLayoutAccess
        {
            static constexpr std::size_t type_offset = offsetof(AnyPtr, type_);
            static constexpr std::size_t data_offset = offsetof(AnyPtr, data_);
        };
    } // namespace detail

    static_assert(detail::TypePointerLayoutAccess::type_offset == 0);
    static_assert(detail::TypePointerLayoutAccess::data_offset == sizeof(void *));
    static_assert(sizeof(AnyPtr) == 2 * sizeof(void *));
    static_assert(alignof(AnyPtr) == alignof(void *));
    static_assert(std::is_standard_layout_v<AnyPtr>);
    static_assert(std::is_trivially_copyable_v<AnyPtr>);

    template <TypeFamily Family, TypeRole Role = TypeRole::Invalid> class TypedPtr
    {
        static_assert(Family != TypeFamily::Invalid, "TypedPtr requires a concrete family");
        static_assert(Role >= TypeRole::Invalid && Role <= TypeRole::Output, "TypedPtr requires a known role");

    public:
        constexpr TypedPtr() noexcept = default;

        template <TypeRole OtherRole>
            requires(Role == TypeRole::Invalid && OtherRole != TypeRole::Invalid)
        constexpr TypedPtr(const TypedPtr<Family, OtherRole> &other) noexcept : value_(other.to_any())
        {
        }

        [[nodiscard]] static TypedPtr checked(AnyPtr value)
        {
            if (value.is_unbound())
            {
                return TypedPtr(value, UncheckedTag{});
            }
            if (!value.well_formed())
            {
                throw std::invalid_argument("TypedPtr requires a well-formed AnyPtr");
            }
            const TypeClassification classification = value.record()->classification();
            if (classification.family != Family)
            {
                throw std::invalid_argument("TypedPtr family mismatch");
            }
            if constexpr (Role != TypeRole::Invalid)
            {
                if (classification.role != Role)
                {
                    throw std::invalid_argument("TypedPtr role mismatch");
                }
            }
            return TypedPtr(value, UncheckedTag{});
        }

        [[nodiscard]] constexpr AnyPtr to_any() const noexcept { return value_; }
        constexpr operator AnyPtr() const noexcept { return value_; }

        [[nodiscard]] constexpr const TypeRecord *record() const noexcept { return value_.record(); }
        [[nodiscard]] constexpr const void *data() const noexcept { return value_.data(); }
        [[nodiscard]] constexpr AccessMode access_mode() const noexcept { return value_.access_mode(); }
        [[nodiscard]] bool well_formed() const noexcept { return value_.well_formed(); }
        [[nodiscard]] constexpr bool is_unbound() const noexcept { return value_.is_unbound(); }
        [[nodiscard]] constexpr bool bound() const noexcept { return record() != nullptr; }
        [[nodiscard]] constexpr bool is_typed_null() const noexcept
        {
            return record() != nullptr && data() == nullptr && read_only_access();
        }
        [[nodiscard]] constexpr bool has_value() const noexcept { return record() != nullptr && data() != nullptr; }
        [[nodiscard]] constexpr bool valid() const noexcept { return has_value(); }
        [[nodiscard]] constexpr explicit operator bool() const noexcept { return valid(); }

        [[nodiscard]] constexpr const SchemaHeader *schema() const noexcept
        {
            return record() != nullptr ? record()->schema : nullptr;
        }
        [[nodiscard]] constexpr const MemoryUtils::StoragePlan *plan() const noexcept
        {
            return record() != nullptr ? record()->plan : nullptr;
        }
        [[nodiscard]] constexpr const void *ops() const noexcept
        {
            return record() != nullptr ? record()->ops : nullptr;
        }
        [[nodiscard]] constexpr const DebugDescriptor *debug() const noexcept
        {
            return record() != nullptr ? record()->debug : nullptr;
        }
        [[nodiscard]] constexpr TypeClassification classification() const noexcept
        {
            return record() != nullptr ? record()->classification() : TypeClassification{};
        }
        [[nodiscard]] constexpr TypeFamily family() const noexcept { return classification().family; }
        [[nodiscard]] constexpr TypeRole role() const noexcept { return classification().role; }
        [[nodiscard]] constexpr TypeKind kind() const noexcept { return classification().kind; }
        [[nodiscard]] constexpr TypeCapabilities capabilities() const noexcept
        {
            return record() != nullptr ? record()->capabilities : TypeCapabilities::None;
        }
        [[nodiscard]] std::string_view semantic_name() const noexcept
        {
            return record() != nullptr ? record()->semantic_name() : std::string_view{};
        }
        [[nodiscard]] std::string_view implementation_name() const noexcept
        {
            return record() != nullptr ? record()->implementation_name() : std::string_view{};
        }
        [[nodiscard]] std::string_view effective_name() const noexcept
        {
            return record() != nullptr ? record()->effective_name() : std::string_view{};
        }

        [[nodiscard]] constexpr bool read_only_access() const noexcept { return value_.read_only_access(); }
        [[nodiscard]] constexpr bool writable_access() const noexcept { return value_.writable_access(); }
        [[nodiscard]] constexpr bool mutation_access() const noexcept { return value_.mutation_access(); }

        [[nodiscard]] TypedPtr as_read_only() const { return TypedPtr(value_.as_read_only(), UncheckedTag{}); }
        [[nodiscard]] TypedPtr begin_mutation() const { return TypedPtr(value_.begin_mutation(), UncheckedTag{}); }
        [[nodiscard]] TypedPtr end_mutation() const { return TypedPtr(value_.end_mutation(), UncheckedTag{}); }
        [[nodiscard]] void *mutable_data() const { return value_.mutable_data(); }

        [[nodiscard]] friend constexpr bool operator==(const TypedPtr &lhs, const TypedPtr &rhs) noexcept
        {
            return lhs.value_ == rhs.value_;
        }

        [[nodiscard]] constexpr bool same_access_as(const TypedPtr &other) const noexcept
        {
            return value_.same_access_as(other.value_);
        }

        [[nodiscard]] constexpr bool same_state_as(const TypedPtr &other) const noexcept
        {
            return value_.same_state_as(other.value_);
        }

    private:
        friend class ValueTypeRef;
        friend class NodeTypeRef;
        friend class GraphTypeRef;
        friend class ExecutorTypeRef;
        friend class ClockTypeRef;
        template <TypeRole OtherRole> friend class BasicTSTypeRef;

        struct UncheckedTag
        {
        };

        constexpr TypedPtr(AnyPtr value, UncheckedTag) noexcept : value_(value) {}

        AnyPtr value_{};
    };

    using ValuePtr = TypedPtr<TypeFamily::Value, TypeRole::Instance>;
    using TSDataPtr = TypedPtr<TypeFamily::TimeSeries, TypeRole::Data>;
    using TSInputPtr = TypedPtr<TypeFamily::TimeSeries, TypeRole::Input>;
    using TSOutputPtr = TypedPtr<TypeFamily::TimeSeries, TypeRole::Output>;
    using NodePtr = TypedPtr<TypeFamily::Node, TypeRole::Runtime>;
    using GraphPtr = TypedPtr<TypeFamily::Graph, TypeRole::Runtime>;
    using ExecutorPtr = TypedPtr<TypeFamily::Executor, TypeRole::Runtime>;
    using ClockPtr = TypedPtr<TypeFamily::Clock, TypeRole::Runtime>;

    static_assert(sizeof(ValuePtr) == 2 * sizeof(void *));
    static_assert(sizeof(TSDataPtr) == 2 * sizeof(void *));
    static_assert(sizeof(TSInputPtr) == 2 * sizeof(void *));
    static_assert(sizeof(TSOutputPtr) == 2 * sizeof(void *));
    static_assert(sizeof(NodePtr) == 2 * sizeof(void *));
    static_assert(sizeof(GraphPtr) == 2 * sizeof(void *));
    static_assert(sizeof(ExecutorPtr) == 2 * sizeof(void *));
    static_assert(sizeof(ClockPtr) == 2 * sizeof(void *));
} // namespace hgraph

#endif // HGRAPH_TYPES_TYPE_POINTER_H
