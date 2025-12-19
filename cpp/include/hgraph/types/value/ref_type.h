//
// Created by Howard Henson on 13/12/2025.
//

#ifndef HGRAPH_VALUE_REF_TYPE_H
#define HGRAPH_VALUE_REF_TYPE_H

#include <hgraph/types/value/type_meta.h>
#include <hgraph/types/value/scalar_type.h>
#include <hgraph/util/date_time.h>
#include <memory>
#include <cassert>
#include <vector>
#include <functional>

namespace hgraph::value {

    // Forward declarations
    class ModificationTracker;

    /**
     * ValueRef - A non-owning reference to another value's data and tracking
     *
     * This is a view-like wrapper that contains pointers to:
     * - The source value's data
     * - The source value's modification tracker (optional)
     * - The schema of the referenced type
     *
     * Equality/hashing is based on the data pointer for uniqueness.
     *
     * Note: Ownership/lifetime management is deferred - the caller must ensure
     * the referenced value outlives this reference.
     */
    struct ValueRef {
        void* data{nullptr};                    // Pointer to the value's data
        void* tracker{nullptr};                 // Pointer to modification tracker storage (optional)
        const TypeMeta* schema{nullptr};        // Type of the referenced value
        void* owner{nullptr};                   // Pointer to owning TSOutput (for REF resolution)

        ValueRef() = default;

        ValueRef(void* d, void* t, const TypeMeta* s, void* o = nullptr)
            : data(d), tracker(t), schema(s), owner(o) {}

        [[nodiscard]] bool valid() const { return data != nullptr && schema != nullptr; }
        [[nodiscard]] bool has_tracker() const { return tracker != nullptr; }
        [[nodiscard]] bool has_owner() const { return owner != nullptr; }

        // Equality based on data pointer (uniqueness)
        bool operator==(const ValueRef& other) const { return data == other.data; }
        bool operator!=(const ValueRef& other) const { return data != other.data; }

        // Hash based on data pointer
        [[nodiscard]] size_t hash() const {
            return std::hash<void*>{}(data);
        }
    };

    /**
     * RefTypeMeta - Extended TypeMeta for reference types
     *
     * REF types wrap pointers to other values. There are two structural cases:
     *
     * 1. Atomic refs (REF[TS], REF[TSS], REF[TSW], REF[TSD]):
     *    - Always a single pointer (bound reference)
     *    - item_count == 0
     *
     * 2. Composite refs (REF[TSL], REF[TSB]):
     *    - Can be bound (single pointer to TSL/TSB) OR
     *    - Can be unbound (collection of references)
     *    - item_count > 0 indicates potential unbound structure
     */
    struct RefTypeMeta : TypeMeta {
        const TypeMeta* value_type;       // Type of what we reference
        size_t item_count;                // 0 = atomic only, >0 = can be unbound with this many items

        [[nodiscard]] bool is_atomic() const {
            return item_count == 0;
        }

        [[nodiscard]] bool can_be_unbound() const {
            return item_count > 0;
        }
    };

    /**
     * RefStorage - Internal storage for a type-erased reference
     *
     * Three variants:
     * - EMPTY: No reference (null pointer)
     * - BOUND: Single ValueRef pointing to a value
     * - UNBOUND: Collection of RefStorage items (for composite refs)
     */
    class RefStorage {
    public:
        enum class Kind : uint8_t { EMPTY = 0, BOUND = 1, UNBOUND = 2 };

        // Default: empty reference
        RefStorage() noexcept : _kind(Kind::EMPTY) {}

        // Bound reference (single pointer)
        explicit RefStorage(ValueRef target) : _kind(Kind::BOUND) {
            new (&_target) ValueRef(target);
        }

        // Unbound reference (collection)
        explicit RefStorage(std::vector<RefStorage> items) : _kind(Kind::UNBOUND) {
            new (&_items) std::vector<RefStorage>(std::move(items));
        }

        ~RefStorage() {
            destroy();
        }

        // Copy
        RefStorage(const RefStorage& other) : _kind(other._kind) {
            copy_from(other);
        }

        RefStorage& operator=(const RefStorage& other) {
            if (this != &other) {
                destroy();
                _kind = other._kind;
                copy_from(other);
            }
            return *this;
        }

        // Move
        RefStorage(RefStorage&& other) noexcept : _kind(other._kind) {
            move_from(std::move(other));
        }

        RefStorage& operator=(RefStorage&& other) noexcept {
            if (this != &other) {
                destroy();
                _kind = other._kind;
                move_from(std::move(other));
            }
            return *this;
        }

        // Query
        [[nodiscard]] Kind kind() const noexcept { return _kind; }
        [[nodiscard]] bool is_empty() const noexcept { return _kind == Kind::EMPTY; }
        [[nodiscard]] bool is_bound() const noexcept { return _kind == Kind::BOUND; }
        [[nodiscard]] bool is_unbound() const noexcept { return _kind == Kind::UNBOUND; }

        // Bound access (throws if not bound)
        [[nodiscard]] const ValueRef& target() const {
            if (_kind != Kind::BOUND) {
                throw std::runtime_error("RefStorage::target() called on non-bound reference");
            }
            return _target;
        }

        [[nodiscard]] ValueRef& target() {
            if (_kind != Kind::BOUND) {
                throw std::runtime_error("RefStorage::target() called on non-bound reference");
            }
            return _target;
        }

        // Unbound access (throws if not unbound)
        [[nodiscard]] const std::vector<RefStorage>& items() const {
            if (_kind != Kind::UNBOUND) {
                throw std::runtime_error("RefStorage::items() called on non-unbound reference");
            }
            return _items;
        }

        [[nodiscard]] std::vector<RefStorage>& items() {
            if (_kind != Kind::UNBOUND) {
                throw std::runtime_error("RefStorage::items() called on non-unbound reference");
            }
            return _items;
        }

        [[nodiscard]] size_t item_count() const {
            if (_kind == Kind::UNBOUND) {
                return _items.size();
            }
            return 0;
        }

        [[nodiscard]] const RefStorage& item(size_t index) const {
            return items()[index];
        }

        [[nodiscard]] RefStorage& item(size_t index) {
            return items()[index];
        }

        // Validity check (is the reference pointing to something valid?)
        [[nodiscard]] bool is_valid() const {
            switch (_kind) {
                case Kind::EMPTY:
                    return false;
                case Kind::BOUND:
                    return _target.valid();
                case Kind::UNBOUND:
                    // Valid if any item is valid
                    for (const auto& item : _items) {
                        if (item.is_valid()) return true;
                    }
                    return false;
            }
            return false;
        }

        // Equality (based on what we're pointing to)
        bool operator==(const RefStorage& other) const {
            if (_kind != other._kind) return false;
            switch (_kind) {
                case Kind::EMPTY:
                    return true;
                case Kind::BOUND:
                    return _target == other._target;
                case Kind::UNBOUND:
                    return _items == other._items;
            }
            return false;
        }

        bool operator!=(const RefStorage& other) const {
            return !(*this == other);
        }

        // Hash
        [[nodiscard]] size_t hash() const {
            switch (_kind) {
                case Kind::EMPTY:
                    return 0;
                case Kind::BOUND:
                    return _target.hash();
                case Kind::UNBOUND: {
                    size_t h = 0;
                    for (const auto& item : _items) {
                        h = h * 31 + item.hash();
                    }
                    return h;
                }
            }
            return 0;
        }

        // Factory methods
        static RefStorage make_empty() {
            return RefStorage{};
        }

        static RefStorage make_bound(ValueRef target) {
            return RefStorage{target};
        }

        static RefStorage make_unbound(std::vector<RefStorage> items) {
            return RefStorage{std::move(items)};
        }

        static RefStorage make_unbound(size_t count) {
            std::vector<RefStorage> items(count);
            return RefStorage{std::move(items)};
        }

    private:
        void destroy() noexcept {
            switch (_kind) {
                case Kind::EMPTY:
                    break;
                case Kind::BOUND:
                    _target.~ValueRef();
                    break;
                case Kind::UNBOUND:
                    _items.~vector();
                    break;
            }
        }

        void copy_from(const RefStorage& other) {
            switch (other._kind) {
                case Kind::EMPTY:
                    break;
                case Kind::BOUND:
                    new (&_target) ValueRef(other._target);
                    break;
                case Kind::UNBOUND:
                    new (&_items) std::vector<RefStorage>(other._items);
                    break;
            }
        }

        void move_from(RefStorage&& other) noexcept {
            switch (other._kind) {
                case Kind::EMPTY:
                    break;
                case Kind::BOUND:
                    new (&_target) ValueRef(std::move(other._target));
                    break;
                case Kind::UNBOUND:
                    new (&_items) std::vector<RefStorage>(std::move(other._items));
                    break;
            }
        }

        Kind _kind;
        union {
            ValueRef _target;                    // For BOUND
            std::vector<RefStorage> _items;      // For UNBOUND
        };
    };

    /**
     * RefTypeOps - Operations for reference types
     */
    struct RefTypeOps {
        static void construct(void* dest, const TypeMeta* meta) {
            new (dest) RefStorage();
        }

        static void destruct(void* dest, const TypeMeta*) {
            static_cast<RefStorage*>(dest)->~RefStorage();
        }

        static void copy_construct(void* dest, const void* src, const TypeMeta*) {
            new (dest) RefStorage(*static_cast<const RefStorage*>(src));
        }

        static void move_construct(void* dest, void* src, const TypeMeta*) {
            new (dest) RefStorage(std::move(*static_cast<RefStorage*>(src)));
        }

        static void copy_assign(void* dest, const void* src, const TypeMeta*) {
            *static_cast<RefStorage*>(dest) = *static_cast<const RefStorage*>(src);
        }

        static void move_assign(void* dest, void* src, const TypeMeta*) {
            *static_cast<RefStorage*>(dest) = std::move(*static_cast<RefStorage*>(src));
        }

        static bool equals(const void* a, const void* b, const TypeMeta*) {
            return *static_cast<const RefStorage*>(a) == *static_cast<const RefStorage*>(b);
        }

        static bool less_than(const void* a, const void* b, const TypeMeta*) {
            // References don't have natural ordering - compare by hash
            return static_cast<const RefStorage*>(a)->hash() < static_cast<const RefStorage*>(b)->hash();
        }

        static size_t hash(const void* v, const TypeMeta*) {
            return static_cast<const RefStorage*>(v)->hash();
        }

        static std::string to_string(const void* v, const TypeMeta* /*meta*/) {
            auto* ref = static_cast<const RefStorage*>(v);
            switch (ref->kind()) {
                case RefStorage::Kind::EMPTY:
                    return "REF[empty]";
                case RefStorage::Kind::BOUND: {
                    const auto& target = ref->target();
                    if (target.valid() && target.schema) {
                        return "REF[bound: " + target.schema->to_string_at(target.data) + "]";
                    }
                    return "REF[bound: <invalid>]";
                }
                case RefStorage::Kind::UNBOUND: {
                    std::string result = "REF[unbound: ";
                    result += std::to_string(ref->item_count()) + " items]";
                    return result;
                }
            }
            return "REF[unknown]";
        }

        static std::string type_name(const TypeMeta* meta) {
            auto* ref_meta = static_cast<const RefTypeMeta*>(meta);
            // Format: REF[target_type]
            std::string result = "REF[";
            if (ref_meta->value_type) {
                result += ref_meta->value_type->type_name_str();
            } else {
                result += "?";
            }
            result += "]";
            return result;
        }

        static const TypeOps ops;
    };

    inline const TypeOps RefTypeOps::ops = {
        .construct = RefTypeOps::construct,
        .destruct = RefTypeOps::destruct,
        .copy_construct = RefTypeOps::copy_construct,
        .move_construct = RefTypeOps::move_construct,
        .copy_assign = RefTypeOps::copy_assign,
        .move_assign = RefTypeOps::move_assign,
        .equals = RefTypeOps::equals,
        .less_than = RefTypeOps::less_than,
        .hash = RefTypeOps::hash,
        .to_string = RefTypeOps::to_string,
        .type_name = RefTypeOps::type_name,
        .to_python = nullptr,
        .from_python = nullptr,
    };

    /**
     * RefTypeBuilder - Builds RefTypeMeta
     *
     * Usage for atomic ref (REF[TS[int]]):
     *   auto meta = RefTypeBuilder()
     *       .value_type(int_ts_meta)
     *       .build("RefInt");
     *
     * Usage for composite ref (REF[TSL[TS[int]]]):
     *   auto meta = RefTypeBuilder()
     *       .value_type(tsl_meta)
     *       .item_count(5)
     *       .build("RefTSL5");
     */
    class RefTypeBuilder {
    public:
        RefTypeBuilder& value_type(const TypeMeta* type) {
            _value_type = type;
            return *this;
        }

        RefTypeBuilder& item_count(size_t count) {
            _item_count = count;
            return *this;
        }

        std::unique_ptr<RefTypeMeta> build(const char* type_name = nullptr) {
            auto meta = std::make_unique<RefTypeMeta>();

            meta->size = sizeof(RefStorage);
            meta->alignment = alignof(RefStorage);
            meta->flags = TypeFlags::Hashable | TypeFlags::Equatable;
            meta->kind = TypeKind::Ref;
            meta->ops = &RefTypeOps::ops;
            meta->type_info = nullptr;
            meta->name = type_name;
            meta->numpy_format = nullptr;  // Refs are not numpy-compatible
            meta->value_type = _value_type;
            meta->item_count = _item_count;

            return meta;
        }

    private:
        const TypeMeta* _value_type{nullptr};
        size_t _item_count{0};
    };

} // namespace hgraph::value

#endif // HGRAPH_VALUE_REF_TYPE_H
