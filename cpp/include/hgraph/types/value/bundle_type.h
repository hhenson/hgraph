//
// Created by Howard Henson on 12/12/2025.
//

#ifndef HGRAPH_VALUE_BUNDLE_TYPE_H
#define HGRAPH_VALUE_BUNDLE_TYPE_H

#include <hgraph/types/value/type_meta.h>
#include <hgraph/types/value/scalar_type.h>
#include <vector>
#include <string>
#include <unordered_map>
#include <memory>
#include <cstring>

namespace hgraph::value {

    /**
     * FieldMeta - Metadata for a single field in a bundle
     */
    struct FieldMeta {
        std::string name;
        size_t offset;          // Offset within bundle storage
        const TypeMeta* type;   // Type of this field
    };

    /**
     * BundleTypeMeta - Extended TypeMeta for bundle (struct-like) types
     *
     * Stores the schema: field names, offsets, and types.
     * Memory layout is computed to respect alignment.
     */
    struct BundleTypeMeta : TypeMeta {
        std::vector<FieldMeta> fields;
        std::unordered_map<std::string, size_t> name_to_index;

        [[nodiscard]] size_t field_count() const { return fields.size(); }

        [[nodiscard]] const FieldMeta* field_by_index(size_t i) const {
            return i < fields.size() ? &fields[i] : nullptr;
        }

        [[nodiscard]] const FieldMeta* field_by_name(const std::string& name) const {
            auto it = name_to_index.find(name);
            return it != name_to_index.end() ? &fields[it->second] : nullptr;
        }

        // Get typed pointer to a field within bundle storage
        [[nodiscard]] TypedPtr field_ptr(void* bundle_storage, size_t field_index) const {
            if (field_index >= fields.size()) return {};
            const auto& f = fields[field_index];
            return {static_cast<char*>(bundle_storage) + f.offset, f.type};
        }

        [[nodiscard]] ConstTypedPtr field_ptr(const void* bundle_storage, size_t field_index) const {
            if (field_index >= fields.size()) return {};
            const auto& f = fields[field_index];
            return {static_cast<const char*>(bundle_storage) + f.offset, f.type};
        }

        [[nodiscard]] TypedPtr field_ptr(void* bundle_storage, const std::string& name) const {
            auto it = name_to_index.find(name);
            return it != name_to_index.end() ? field_ptr(bundle_storage, it->second) : TypedPtr{};
        }

        [[nodiscard]] ConstTypedPtr field_ptr(const void* bundle_storage, const std::string& name) const {
            auto it = name_to_index.find(name);
            return it != name_to_index.end() ? field_ptr(bundle_storage, it->second) : ConstTypedPtr{};
        }
    };

    // Forward declare ops generator
    struct BundleTypeOps;

    /**
     * BundleTypeBuilder - Builds BundleTypeMeta from field specifications
     *
     * Usage:
     *   auto meta = BundleTypeBuilder()
     *       .add_field<int>("x")
     *       .add_field<double>("y")
     *       .add_field("nested", other_bundle_meta)
     *       .build();
     */
    class BundleTypeBuilder {
    public:
        BundleTypeBuilder() = default;

        // Add a scalar field
        template<typename T>
        BundleTypeBuilder& add_field(const std::string& name) {
            return add_field(name, scalar_type_meta<T>());
        }

        // Add a field with existing TypeMeta (for nesting)
        BundleTypeBuilder& add_field(const std::string& name, const TypeMeta* field_type) {
            _pending_fields.push_back({name, field_type});
            return *this;
        }

        // Build the final BundleTypeMeta
        std::unique_ptr<BundleTypeMeta> build(const char* type_name = nullptr);

    private:
        struct PendingField {
            std::string name;
            const TypeMeta* type;
        };

        std::vector<PendingField> _pending_fields;
    };

    /**
     * BundleTypeOps - Operations for bundle types
     */
    struct BundleTypeOps {
        static void construct(void* dest, const TypeMeta* meta) {
            auto* bundle_meta = static_cast<const BundleTypeMeta*>(meta);
            for (const auto& field : bundle_meta->fields) {
                void* field_ptr = static_cast<char*>(dest) + field.offset;
                field.type->construct_at(field_ptr);
            }
        }

        static void destruct(void* dest, const TypeMeta* meta) {
            auto* bundle_meta = static_cast<const BundleTypeMeta*>(meta);
            // Destruct in reverse order
            for (auto it = bundle_meta->fields.rbegin(); it != bundle_meta->fields.rend(); ++it) {
                void* field_ptr = static_cast<char*>(dest) + it->offset;
                it->type->destruct_at(field_ptr);
            }
        }

        static void copy_construct(void* dest, const void* src, const TypeMeta* meta) {
            auto* bundle_meta = static_cast<const BundleTypeMeta*>(meta);
            for (const auto& field : bundle_meta->fields) {
                void* dest_field = static_cast<char*>(dest) + field.offset;
                const void* src_field = static_cast<const char*>(src) + field.offset;
                field.type->copy_construct_at(dest_field, src_field);
            }
        }

        static void move_construct(void* dest, void* src, const TypeMeta* meta) {
            auto* bundle_meta = static_cast<const BundleTypeMeta*>(meta);
            for (const auto& field : bundle_meta->fields) {
                void* dest_field = static_cast<char*>(dest) + field.offset;
                void* src_field = static_cast<char*>(src) + field.offset;
                field.type->move_construct_at(dest_field, src_field);
            }
        }

        static void copy_assign(void* dest, const void* src, const TypeMeta* meta) {
            auto* bundle_meta = static_cast<const BundleTypeMeta*>(meta);
            for (const auto& field : bundle_meta->fields) {
                void* dest_field = static_cast<char*>(dest) + field.offset;
                const void* src_field = static_cast<const char*>(src) + field.offset;
                field.type->copy_assign_at(dest_field, src_field);
            }
        }

        static void move_assign(void* dest, void* src, const TypeMeta* meta) {
            auto* bundle_meta = static_cast<const BundleTypeMeta*>(meta);
            for (const auto& field : bundle_meta->fields) {
                void* dest_field = static_cast<char*>(dest) + field.offset;
                void* src_field = static_cast<char*>(src) + field.offset;
                field.type->move_assign_at(dest_field, src_field);
            }
        }

        static bool equals(const void* a, const void* b, const TypeMeta* meta) {
            auto* bundle_meta = static_cast<const BundleTypeMeta*>(meta);
            for (const auto& field : bundle_meta->fields) {
                const void* a_field = static_cast<const char*>(a) + field.offset;
                const void* b_field = static_cast<const char*>(b) + field.offset;
                if (!field.type->equals_at(a_field, b_field)) {
                    return false;
                }
            }
            return true;
        }

        static bool less_than(const void* a, const void* b, const TypeMeta* meta) {
            auto* bundle_meta = static_cast<const BundleTypeMeta*>(meta);
            for (const auto& field : bundle_meta->fields) {
                const void* a_field = static_cast<const char*>(a) + field.offset;
                const void* b_field = static_cast<const char*>(b) + field.offset;
                if (field.type->less_than_at(a_field, b_field)) return true;
                if (field.type->less_than_at(b_field, a_field)) return false;
                // Equal, continue to next field
            }
            return false;  // All fields equal
        }

        static size_t hash(const void* v, const TypeMeta* meta) {
            auto* bundle_meta = static_cast<const BundleTypeMeta*>(meta);
            size_t result = 0;
            for (const auto& field : bundle_meta->fields) {
                const void* field_ptr = static_cast<const char*>(v) + field.offset;
                size_t field_hash = field.type->hash_at(field_ptr);
                // Combine hashes (boost::hash_combine style)
                result ^= field_hash + 0x9e3779b9 + (result << 6) + (result >> 2);
            }
            return result;
        }

        static const TypeOps ops;
    };

    inline const TypeOps BundleTypeOps::ops = {
        .construct = BundleTypeOps::construct,
        .destruct = BundleTypeOps::destruct,
        .copy_construct = BundleTypeOps::copy_construct,
        .move_construct = BundleTypeOps::move_construct,
        .copy_assign = BundleTypeOps::copy_assign,
        .move_assign = BundleTypeOps::move_assign,
        .equals = BundleTypeOps::equals,
        .less_than = BundleTypeOps::less_than,
        .hash = BundleTypeOps::hash,
        .to_python = nullptr,
        .from_python = nullptr,
    };

    // Align offset to required alignment
    inline size_t align_offset(size_t offset, size_t alignment) {
        return (offset + alignment - 1) & ~(alignment - 1);
    }

    inline std::unique_ptr<BundleTypeMeta> BundleTypeBuilder::build(const char* type_name) {
        auto meta = std::make_unique<BundleTypeMeta>();

        // Compute layout
        size_t current_offset = 0;
        size_t max_alignment = 1;
        TypeFlags combined_flags = TypeFlags::Equatable | TypeFlags::Comparable | TypeFlags::Hashable;
        bool all_trivially_copyable = true;
        bool all_trivially_destructible = true;
        bool all_buffer_compatible = true;

        for (size_t i = 0; i < _pending_fields.size(); ++i) {
            const auto& pf = _pending_fields[i];

            // Align for this field
            current_offset = align_offset(current_offset, pf.type->alignment);
            max_alignment = std::max(max_alignment, pf.type->alignment);

            // Add field
            meta->fields.push_back({
                .name = pf.name,
                .offset = current_offset,
                .type = pf.type,
            });
            meta->name_to_index[pf.name] = i;

            current_offset += pf.type->size;

            // Accumulate flags
            if (!pf.type->is_trivially_copyable()) all_trivially_copyable = false;
            if (!pf.type->is_trivially_destructible()) all_trivially_destructible = false;
            if (!pf.type->is_buffer_compatible()) all_buffer_compatible = false;
            if (!has_flag(pf.type->flags, TypeFlags::Equatable)) {
                combined_flags = combined_flags & ~TypeFlags::Equatable;
            }
            if (!has_flag(pf.type->flags, TypeFlags::Comparable)) {
                combined_flags = combined_flags & ~TypeFlags::Comparable;
            }
            if (!has_flag(pf.type->flags, TypeFlags::Hashable)) {
                combined_flags = combined_flags & ~TypeFlags::Hashable;
            }
        }

        // Final size (aligned to max alignment for arrays)
        size_t total_size = align_offset(current_offset, max_alignment);

        // Build flags
        TypeFlags flags = combined_flags;
        if (all_trivially_copyable) flags = flags | TypeFlags::TriviallyCopyable;
        if (all_trivially_destructible) flags = flags | TypeFlags::TriviallyDestructible;
        if (all_buffer_compatible) flags = flags | TypeFlags::BufferCompatible;

        // Fill in base TypeMeta
        meta->size = total_size;
        meta->alignment = max_alignment;
        meta->flags = flags;
        meta->kind = TypeKind::Bundle;
        meta->ops = &BundleTypeOps::ops;
        meta->type_info = nullptr;
        meta->name = type_name;

        return meta;
    }

    /**
     * BundleValue - A value instance backed by a BundleTypeMeta
     *
     * Provides isolated field access - each field behaves as
     * though it were the only value being accessed.
     */
    class BundleValue {
    public:
        BundleValue() = default;

        explicit BundleValue(const BundleTypeMeta* meta)
            : _meta(meta) {
            if (_meta && _meta->size > 0) {
                _storage = ::operator new(_meta->size, std::align_val_t{_meta->alignment});
                _meta->construct_at(_storage);
            }
        }

        // Create with external storage (non-owning view)
        BundleValue(void* storage, const BundleTypeMeta* meta, bool owning = false)
            : _storage(storage), _meta(meta), _owns_storage(owning) {}

        ~BundleValue() {
            if (_owns_storage && _storage && _meta) {
                _meta->destruct_at(_storage);
                ::operator delete(_storage, std::align_val_t{_meta->alignment});
            }
        }

        // Move only
        BundleValue(BundleValue&& other) noexcept
            : _storage(other._storage)
            , _meta(other._meta)
            , _owns_storage(other._owns_storage) {
            other._storage = nullptr;
            other._owns_storage = false;
        }

        BundleValue& operator=(BundleValue&& other) noexcept {
            if (this != &other) {
                if (_owns_storage && _storage && _meta) {
                    _meta->destruct_at(_storage);
                    ::operator delete(_storage, std::align_val_t{_meta->alignment});
                }
                _storage = other._storage;
                _meta = other._meta;
                _owns_storage = other._owns_storage;
                other._storage = nullptr;
                other._owns_storage = false;
            }
            return *this;
        }

        BundleValue(const BundleValue&) = delete;
        BundleValue& operator=(const BundleValue&) = delete;

        [[nodiscard]] bool valid() const { return _storage && _meta; }
        [[nodiscard]] const BundleTypeMeta* meta() const { return _meta; }
        [[nodiscard]] void* storage() { return _storage; }
        [[nodiscard]] const void* storage() const { return _storage; }

        // Field access by index
        [[nodiscard]] TypedPtr field(size_t index) {
            return _meta ? _meta->field_ptr(_storage, index) : TypedPtr{};
        }

        [[nodiscard]] ConstTypedPtr field(size_t index) const {
            return _meta ? _meta->field_ptr(_storage, index) : ConstTypedPtr{};
        }

        // Field access by name
        [[nodiscard]] TypedPtr field(const std::string& name) {
            return _meta ? _meta->field_ptr(_storage, name) : TypedPtr{};
        }

        [[nodiscard]] ConstTypedPtr field(const std::string& name) const {
            return _meta ? _meta->field_ptr(_storage, name) : ConstTypedPtr{};
        }

        // Typed field access
        template<typename T>
        [[nodiscard]] T& get(size_t index) {
            return field(index).as<T>();
        }

        template<typename T>
        [[nodiscard]] const T& get(size_t index) const {
            return field(index).as<T>();
        }

        template<typename T>
        [[nodiscard]] T& get(const std::string& name) {
            return field(name).as<T>();
        }

        template<typename T>
        [[nodiscard]] const T& get(const std::string& name) const {
            return field(name).as<T>();
        }

        template<typename T>
        void set(size_t index, const T& value) {
            auto f = field(index);
            if (f.valid()) f.as<T>() = value;
        }

        template<typename T>
        void set(const std::string& name, const T& value) {
            auto f = field(name);
            if (f.valid()) f.as<T>() = value;
        }

        // Whole-value operations
        [[nodiscard]] bool equals(const BundleValue& other) const {
            if (!valid() || !other.valid() || _meta != other._meta) return false;
            return _meta->equals_at(_storage, other._storage);
        }

        [[nodiscard]] size_t hash() const {
            return valid() ? _meta->hash_at(_storage) : 0;
        }

    private:
        void* _storage{nullptr};
        const BundleTypeMeta* _meta{nullptr};
        bool _owns_storage{true};
    };

} // namespace hgraph::value

#endif // HGRAPH_VALUE_BUNDLE_TYPE_H
