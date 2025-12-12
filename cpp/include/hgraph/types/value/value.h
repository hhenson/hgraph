//
// Created by Howard Henson on 12/12/2025.
//

#ifndef HGRAPH_VALUE_VALUE_H
#define HGRAPH_VALUE_VALUE_H

#include <hgraph/types/value/type_meta.h>
#include <hgraph/types/value/bundle_type.h>
#include <hgraph/types/value/list_type.h>
#include <hgraph/types/value/set_type.h>
#include <hgraph/types/value/dict_type.h>
#include <memory>
#include <stdexcept>
#include <cassert>

namespace hgraph::value {

    // Forward declarations
    class Value;
    class ValueView;
    class ConstValueView;

    /**
     * ConstValueView - Non-owning const view of a value with type information
     *
     * Provides:
     * - Type checking via schema comparison
     * - Type-safe access to data
     * - Navigation into nested types (fields, elements)
     *
     * This is the primary way to inspect values at any nesting level.
     */
    class ConstValueView {
    public:
        ConstValueView() = default;

        ConstValueView(const void* data, const TypeMeta* schema)
            : _data(data), _schema(schema) {}

        // Type information
        [[nodiscard]] const TypeMeta* schema() const { return _schema; }
        [[nodiscard]] TypeKind kind() const { return _schema ? _schema->kind : TypeKind::Scalar; }
        [[nodiscard]] bool valid() const { return _data && _schema; }

        // Type checking - compare schemas for type conformance
        [[nodiscard]] bool is_type(const TypeMeta* other) const {
            return _schema == other;
        }

        [[nodiscard]] bool same_type_as(const ConstValueView& other) const {
            return _schema == other._schema;
        }

        // Kind checks
        [[nodiscard]] bool is_scalar() const { return valid() && _schema->kind == TypeKind::Scalar; }
        [[nodiscard]] bool is_bundle() const { return valid() && _schema->kind == TypeKind::Bundle; }
        [[nodiscard]] bool is_list() const { return valid() && _schema->kind == TypeKind::List; }
        [[nodiscard]] bool is_set() const { return valid() && _schema->kind == TypeKind::Set; }
        [[nodiscard]] bool is_dict() const { return valid() && _schema->kind == TypeKind::Dict; }

        // Raw data access
        [[nodiscard]] const void* data() const { return _data; }

        // Check if schema matches type T (for scalar types)
        template<typename T>
        [[nodiscard]] bool is_scalar_type() const {
            return valid() && _schema == scalar_type_meta<T>();
        }

        // Typed data access with debug assertion
        // Use when you're confident about the type (e.g., after navigation)
        template<typename T>
        [[nodiscard]] const T& as() const {
            assert(valid() && "as<T>() called on invalid view");
            assert(is_scalar_type<T>() && "as<T>() type mismatch - use try_as<T>() or checked_as<T>()");
            return *static_cast<const T*>(_data);
        }

        // Safe typed access - returns nullptr if type doesn't match
        template<typename T>
        [[nodiscard]] const T* try_as() const {
            if (!is_scalar_type<T>()) return nullptr;
            return static_cast<const T*>(_data);
        }

        // Checked typed access - throws if type doesn't match
        template<typename T>
        [[nodiscard]] const T& checked_as() const {
            if (!valid()) {
                throw std::runtime_error("checked_as<T>() called on invalid view");
            }
            if (!is_scalar_type<T>()) {
                throw std::runtime_error("checked_as<T>() type mismatch");
            }
            return *static_cast<const T*>(_data);
        }

        // Bundle field access
        [[nodiscard]] ConstValueView field(size_t index) const {
            if (!is_bundle()) return {};
            auto* bundle_meta = static_cast<const BundleTypeMeta*>(_schema);
            auto ptr = bundle_meta->field_ptr(_data, index);
            return {ptr.ptr, ptr.meta};
        }

        [[nodiscard]] ConstValueView field(const std::string& name) const {
            if (!is_bundle()) return {};
            auto* bundle_meta = static_cast<const BundleTypeMeta*>(_schema);
            auto ptr = bundle_meta->field_ptr(_data, name);
            return {ptr.ptr, ptr.meta};
        }

        [[nodiscard]] size_t field_count() const {
            if (!is_bundle()) return 0;
            return static_cast<const BundleTypeMeta*>(_schema)->field_count();
        }

        [[nodiscard]] const FieldMeta* field_meta(size_t index) const {
            if (!is_bundle()) return nullptr;
            return static_cast<const BundleTypeMeta*>(_schema)->field_by_index(index);
        }

        [[nodiscard]] const FieldMeta* field_meta(const std::string& name) const {
            if (!is_bundle()) return nullptr;
            return static_cast<const BundleTypeMeta*>(_schema)->field_by_name(name);
        }

        // List element access
        [[nodiscard]] ConstValueView element(size_t index) const {
            if (!is_list()) return {};
            auto* list_meta = static_cast<const ListTypeMeta*>(_schema);
            auto ptr = list_meta->element_ptr(_data, index);
            return {ptr.ptr, ptr.meta};
        }

        [[nodiscard]] size_t list_size() const {
            if (!is_list()) return 0;
            return static_cast<const ListTypeMeta*>(_schema)->count;
        }

        [[nodiscard]] const TypeMeta* element_type() const {
            if (is_list()) {
                return static_cast<const ListTypeMeta*>(_schema)->element_type;
            }
            if (is_set()) {
                return static_cast<const SetTypeMeta*>(_schema)->element_type;
            }
            return nullptr;
        }

        // Set operations
        [[nodiscard]] size_t set_size() const {
            if (!is_set()) return 0;
            return static_cast<const SetStorage*>(_data)->size();
        }

        template<typename T>
        [[nodiscard]] bool set_contains(const T& value) const {
            if (!is_set()) return false;
            return static_cast<const SetStorage*>(_data)->contains(&value);
        }

        // Dict operations
        [[nodiscard]] size_t dict_size() const {
            if (!is_dict()) return 0;
            return static_cast<const DictStorage*>(_data)->size();
        }

        template<typename K>
        [[nodiscard]] bool dict_contains(const K& key) const {
            if (!is_dict()) return false;
            return static_cast<const DictStorage*>(_data)->contains(&key);
        }

        template<typename K>
        [[nodiscard]] ConstValueView dict_get(const K& key) const {
            if (!is_dict()) return {};
            auto* dict_meta = static_cast<const DictTypeMeta*>(_schema);
            auto* storage = static_cast<const DictStorage*>(_data);
            const void* val = storage->get(&key);
            return val ? ConstValueView{val, dict_meta->value_type} : ConstValueView{};
        }

        [[nodiscard]] const TypeMeta* key_type() const {
            if (!is_dict()) return nullptr;
            return static_cast<const DictTypeMeta*>(_schema)->key_type;
        }

        [[nodiscard]] const TypeMeta* value_type() const {
            if (!is_dict()) return nullptr;
            return static_cast<const DictTypeMeta*>(_schema)->value_type;
        }

        // Comparison operations (uses schema's ops)
        [[nodiscard]] bool equals(const ConstValueView& other) const {
            if (!valid() || !other.valid()) return false;
            if (_schema != other._schema) return false;
            return _schema->equals_at(_data, other._data);
        }

        [[nodiscard]] bool less_than(const ConstValueView& other) const {
            if (!valid() || !other.valid()) return false;
            if (_schema != other._schema) return false;
            return _schema->less_than_at(_data, other._data);
        }

        [[nodiscard]] size_t hash() const {
            return valid() ? _schema->hash_at(_data) : 0;
        }

        // Implicit conversion to TypedPtr for compatibility
        operator ConstTypedPtr() const {
            return {_data, _schema};
        }

    protected:
        const void* _data{nullptr};
        const TypeMeta* _schema{nullptr};
    };

    /**
     * ValueView - Non-owning mutable view of a value with type information
     *
     * Extends ConstValueView with mutation capabilities.
     */
    class ValueView : public ConstValueView {
    public:
        ValueView() = default;

        ValueView(void* data, const TypeMeta* schema)
            : ConstValueView(data, schema), _mutable_data(data) {}

        // Mutable data access
        [[nodiscard]] void* data() { return _mutable_data; }

        // Typed mutable access with debug assertion
        template<typename T>
        [[nodiscard]] T& as() {
            assert(valid() && "as<T>() called on invalid view");
            assert(is_scalar_type<T>() && "as<T>() type mismatch - use try_as<T>() or checked_as<T>()");
            return *static_cast<T*>(_mutable_data);
        }

        // Safe typed mutable access - returns nullptr if type doesn't match
        template<typename T>
        [[nodiscard]] T* try_as() {
            if (!is_scalar_type<T>()) return nullptr;
            return static_cast<T*>(_mutable_data);
        }

        // Checked typed mutable access - throws if type doesn't match
        template<typename T>
        [[nodiscard]] T& checked_as() {
            if (!valid()) {
                throw std::runtime_error("checked_as<T>() called on invalid view");
            }
            if (!is_scalar_type<T>()) {
                throw std::runtime_error("checked_as<T>() type mismatch");
            }
            return *static_cast<T*>(_mutable_data);
        }

        // Set value (type-erased)
        void copy_from(const ConstValueView& other) {
            if (valid() && other.valid() && _schema == other.schema()) {
                _schema->copy_assign_at(_mutable_data, other.data());
            }
        }

        // Bundle field access (mutable)
        [[nodiscard]] ValueView field(size_t index) {
            if (!is_bundle()) return {};
            auto* bundle_meta = static_cast<const BundleTypeMeta*>(_schema);
            auto ptr = bundle_meta->field_ptr(_mutable_data, index);
            return {ptr.ptr, ptr.meta};
        }

        [[nodiscard]] ValueView field(const std::string& name) {
            if (!is_bundle()) return {};
            auto* bundle_meta = static_cast<const BundleTypeMeta*>(_schema);
            auto ptr = bundle_meta->field_ptr(_mutable_data, name);
            return {ptr.ptr, ptr.meta};
        }

        // List element access (mutable)
        [[nodiscard]] ValueView element(size_t index) {
            if (!is_list()) return {};
            auto* list_meta = static_cast<const ListTypeMeta*>(_schema);
            auto ptr = list_meta->element_ptr(_mutable_data, index);
            return {ptr.ptr, ptr.meta};
        }

        // Set operations (mutable)
        template<typename T>
        bool set_add(const T& value) {
            if (!is_set()) return false;
            return static_cast<SetStorage*>(_mutable_data)->add(&value);
        }

        template<typename T>
        bool set_remove(const T& value) {
            if (!is_set()) return false;
            return static_cast<SetStorage*>(_mutable_data)->remove(&value);
        }

        void set_clear() {
            if (is_set()) {
                static_cast<SetStorage*>(_mutable_data)->clear();
            }
        }

        // Dict operations (mutable)
        template<typename K, typename V>
        void dict_insert(const K& key, const V& value) {
            if (is_dict()) {
                static_cast<DictStorage*>(_mutable_data)->insert(&key, &value);
            }
        }

        template<typename K>
        bool dict_remove(const K& key) {
            if (!is_dict()) return false;
            return static_cast<DictStorage*>(_mutable_data)->remove(&key);
        }

        template<typename K>
        [[nodiscard]] ValueView dict_get(const K& key) {
            if (!is_dict()) return {};
            auto* dict_meta = static_cast<const DictTypeMeta*>(_schema);
            auto* storage = static_cast<DictStorage*>(_mutable_data);
            void* val = storage->get(&key);
            return val ? ValueView{val, dict_meta->value_type} : ValueView{};
        }

        void dict_clear() {
            if (is_dict()) {
                static_cast<DictStorage*>(_mutable_data)->clear();
            }
        }

        // Implicit conversion to TypedPtr
        operator TypedPtr() {
            return {_mutable_data, _schema};
        }

    private:
        void* _mutable_data{nullptr};
    };

    /**
     * Value - Owning value with type information
     *
     * The "owner type" - holds storage and reference to schema.
     * Can create views at any nesting level.
     */
    class Value {
    public:
        Value() = default;

        // Create value of given type (allocates and constructs)
        explicit Value(const TypeMeta* schema)
            : _schema(schema) {
            if (_schema && _schema->size > 0) {
                _storage = ::operator new(_schema->size, std::align_val_t{_schema->alignment});
                _schema->construct_at(_storage);
            }
        }

        ~Value() {
            if (_storage && _schema) {
                _schema->destruct_at(_storage);
                ::operator delete(_storage, std::align_val_t{_schema->alignment});
            }
        }

        // Move only
        Value(Value&& other) noexcept
            : _storage(other._storage)
            , _schema(other._schema) {
            other._storage = nullptr;
            other._schema = nullptr;
        }

        Value& operator=(Value&& other) noexcept {
            if (this != &other) {
                if (_storage && _schema) {
                    _schema->destruct_at(_storage);
                    ::operator delete(_storage, std::align_val_t{_schema->alignment});
                }
                _storage = other._storage;
                _schema = other._schema;
                other._storage = nullptr;
                other._schema = nullptr;
            }
            return *this;
        }

        Value(const Value&) = delete;
        Value& operator=(const Value&) = delete;

        // Copy construct from another value
        static Value copy(const Value& other) {
            if (!other.valid()) return {};
            Value result(other._schema);
            other._schema->copy_assign_at(result._storage, other._storage);
            return result;
        }

        static Value copy(const ConstValueView& view) {
            if (!view.valid()) return {};
            Value result(view.schema());
            view.schema()->copy_assign_at(result._storage, view.data());
            return result;
        }

        // Validity and type
        [[nodiscard]] bool valid() const { return _storage && _schema; }
        [[nodiscard]] const TypeMeta* schema() const { return _schema; }
        [[nodiscard]] TypeKind kind() const { return _schema ? _schema->kind : TypeKind::Scalar; }

        // Type checking
        [[nodiscard]] bool is_type(const TypeMeta* other) const {
            return _schema == other;
        }

        [[nodiscard]] bool same_type_as(const Value& other) const {
            return _schema == other._schema;
        }

        [[nodiscard]] bool same_type_as(const ConstValueView& view) const {
            return _schema == view.schema();
        }

        // Get views
        [[nodiscard]] ValueView view() {
            return {_storage, _schema};
        }

        [[nodiscard]] ConstValueView view() const {
            return {_storage, _schema};
        }

        [[nodiscard]] ConstValueView const_view() const {
            return {_storage, _schema};
        }

        // Check if schema matches type T
        template<typename T>
        [[nodiscard]] bool is_scalar_type() const {
            return valid() && _schema == scalar_type_meta<T>();
        }

        // Direct typed access with debug assertion
        template<typename T>
        [[nodiscard]] T& as() {
            assert(valid() && "as<T>() called on invalid Value");
            assert(is_scalar_type<T>() && "as<T>() type mismatch");
            return *static_cast<T*>(_storage);
        }

        template<typename T>
        [[nodiscard]] const T& as() const {
            assert(valid() && "as<T>() called on invalid Value");
            assert(is_scalar_type<T>() && "as<T>() type mismatch");
            return *static_cast<const T*>(_storage);
        }

        // Safe typed access - returns nullptr if type doesn't match
        template<typename T>
        [[nodiscard]] T* try_as() {
            if (!is_scalar_type<T>()) return nullptr;
            return static_cast<T*>(_storage);
        }

        template<typename T>
        [[nodiscard]] const T* try_as() const {
            if (!is_scalar_type<T>()) return nullptr;
            return static_cast<const T*>(_storage);
        }

        // Checked typed access - throws if type doesn't match
        template<typename T>
        [[nodiscard]] T& checked_as() {
            if (!valid()) throw std::runtime_error("checked_as<T>() on invalid Value");
            if (!is_scalar_type<T>()) throw std::runtime_error("checked_as<T>() type mismatch");
            return *static_cast<T*>(_storage);
        }

        template<typename T>
        [[nodiscard]] const T& checked_as() const {
            if (!valid()) throw std::runtime_error("checked_as<T>() on invalid Value");
            if (!is_scalar_type<T>()) throw std::runtime_error("checked_as<T>() type mismatch");
            return *static_cast<const T*>(_storage);
        }

        // Raw storage access
        [[nodiscard]] void* data() { return _storage; }
        [[nodiscard]] const void* data() const { return _storage; }

        // Comparison
        [[nodiscard]] bool equals(const Value& other) const {
            return view().equals(other.view());
        }

        [[nodiscard]] bool equals(const ConstValueView& other) const {
            return view().equals(other);
        }

        [[nodiscard]] size_t hash() const {
            return view().hash();
        }

    private:
        void* _storage{nullptr};
        const TypeMeta* _schema{nullptr};
    };

    /**
     * Helper function to create a Value of a scalar type
     */
    template<typename T>
    Value make_scalar(const T& val) {
        Value v(scalar_type_meta<T>());
        v.as<T>() = val;
        return v;
    }

} // namespace hgraph::value

#endif // HGRAPH_VALUE_VALUE_H
