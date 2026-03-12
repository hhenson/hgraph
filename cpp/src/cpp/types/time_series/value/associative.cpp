#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/value/associative.h>
#include <hgraph/types/time_series/value/state.h>
#include <hgraph/types/value/validity_bitmap.h>

#include <algorithm>
#include <cstring>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>

namespace hgraph
{

    namespace detail
    {

        struct SetState
        {
            size_t     size{0};
            size_t     capacity{0};
            std::byte *elements{nullptr};
        };

        struct MapState
        {
            SetState   keys;
            std::byte *values{nullptr};
            std::byte *value_validity{nullptr};
        };

        struct SetDispatch final : SetViewDispatch
        {
            explicit SetDispatch(const value::TypeMeta &schema)
                : m_schema(schema),
                  m_element_builder(ValueBuilderFactory::checked_builder_for(schema.element_type))
            {
                if (schema.element_type == nullptr) {
                    throw std::runtime_error("Set schema requires an element schema");
                }
            }

            [[nodiscard]] size_t size(const void *data) const noexcept override { return state(data)->size; }
            [[nodiscard]] const value::TypeMeta &element_schema() const noexcept override { return *m_schema.get().element_type; }
            [[nodiscard]] const ViewDispatch &element_dispatch() const noexcept override { return m_element_builder.get().dispatch(); }
            [[nodiscard]] void *element_data(void *data, size_t index) const override
            {
                if (index >= state(data)->size) { throw std::out_of_range("Set index out of range"); }
                return state(data)->elements + index * element_stride();
            }
            [[nodiscard]] const void *element_data(const void *data, size_t index) const override
            {
                if (index >= state(data)->size) { throw std::out_of_range("Set index out of range"); }
                return state(data)->elements + index * element_stride();
            }

            [[nodiscard]] bool contains(const void *data, const void *element) const override
            {
                return find_index(data, element) != npos;
            }

            [[nodiscard]] bool add(void *data, const void *element) const override
            {
                SetState *set = state(data);
                if (contains(data, element)) { return false; }
                reserve(set, set->size + 1);
                void *slot = set->elements + set->size * element_stride();
                m_element_builder.get().construct(slot);
                element_dispatch().assign(slot, element);
                ++set->size;
                return true;
            }

            [[nodiscard]] bool remove(void *data, const void *element) const override
            {
                SetState *set = state(data);
                const size_t index = find_index(data, element);
                if (index == npos) { return false; }

                destroy_slot(set->elements + index * element_stride());
                if (index + 1 != set->size) {
                    void *dst = set->elements + index * element_stride();
                    void *src = set->elements + (set->size - 1) * element_stride();
                    m_element_builder.get().move_construct(dst, src, m_element_builder);
                    destroy_slot(src);
                }
                --set->size;
                return true;
            }

            void clear(void *data) const override
            {
                SetState *set = state(data);
                for (size_t i = 0; i < set->size; ++i) {
                    destroy_slot(set->elements + i * element_stride());
                }
                set->size = 0;
            }

            [[nodiscard]] size_t hash(const void *data) const override
            {
                size_t result = 0;
                for (size_t i = 0; i < size(data); ++i) {
                    result ^= element_dispatch().hash(element_data(data, i));
                }
                return result;
            }

            [[nodiscard]] std::string to_string(const void *data) const override
            {
                std::string result = "{";
                for (size_t i = 0; i < size(data); ++i) {
                    if (i > 0) { result += ", "; }
                    result += element_dispatch().to_string(element_data(data, i));
                }
                result += "}";
                return result;
            }

            [[nodiscard]] std::partial_ordering compare(const void *lhs, const void *rhs) const override
            {
                const SetState *a = state(lhs);
                const SetState *b = state(rhs);
                if (a->size != b->size) { return std::partial_ordering::unordered; }
                for (size_t i = 0; i < a->size; ++i) {
                    if (!contains(rhs, a->elements + i * element_stride())) {
                        return std::partial_ordering::unordered;
                    }
                }
                return std::partial_ordering::equivalent;
            }

            [[nodiscard]] nb::object to_python(const void *data, const value::TypeMeta *schema) const override
            {
                static_cast<void>(schema);
                nb::set result;
                for (size_t i = 0; i < size(data); ++i) {
                    result.add(element_dispatch().to_python(element_data(data, i), &element_schema()));
                }
                return nb::frozenset(result);
            }

            void from_python(void *dst, const nb::object &src, const value::TypeMeta *schema) const override
            {
                static_cast<void>(schema);
                if (!nb::isinstance<nb::set>(src) && !nb::isinstance<nb::frozenset>(src) &&
                    !nb::isinstance<nb::list>(src) && !nb::isinstance<nb::tuple>(src)) {
                    throw std::runtime_error("Set value expects a set, frozenset, list, or tuple");
                }

                clear(dst);
                nb::iterator it = nb::iter(src);
                while (it != nb::iterator::sentinel()) {
                    nb::handle item = *it;
                    if (item.is_none()) {
                        throw std::runtime_error("Set value does not allow None elements");
                    }
                    void *temp = m_element_builder.get().allocate();
                    try {
                        m_element_builder.get().construct(temp);
                        element_dispatch().from_python(temp, nb::borrow<nb::object>(item), &element_schema());
                        static_cast<void>(add(dst, temp));
                        if (m_element_builder.get().requires_destroy()) {
                            m_element_builder.get().destroy(temp);
                        }
                        m_element_builder.get().deallocate(temp);
                    } catch (...) {
                        if (temp != nullptr) {
                            if (m_element_builder.get().requires_destroy()) {
                                m_element_builder.get().destroy(temp);
                            }
                            m_element_builder.get().deallocate(temp);
                        }
                        throw;
                    }
                    ++it;
                }
            }

            void assign(void *dst, const void *src) const override
            {
                clear(dst);
                const SetState *set = state(src);
                reserve(state(dst), set->size);
                for (size_t i = 0; i < set->size; ++i) {
                    static_cast<void>(add(dst, set->elements + i * element_stride()));
                }
            }

            void set_from_cpp(void *dst, const void *src, const value::TypeMeta *src_schema) const override
            {
                static_cast<void>(dst);
                static_cast<void>(src);
                static_cast<void>(src_schema);
                throw std::invalid_argument("Set value set_from_cpp is not implemented");
            }

            void move_from_cpp(void *dst, void *src, const value::TypeMeta *src_schema) const override
            {
                static_cast<void>(dst);
                static_cast<void>(src);
                static_cast<void>(src_schema);
                throw std::invalid_argument("Set value move_from_cpp is not implemented");
            }

            void construct(void *memory) const { std::construct_at(state(memory)); }

            void destroy(void *memory) const noexcept
            {
                SetState *set = state(memory);
                clear(set);
                if (set->elements != nullptr) {
                    ::operator delete(set->elements, std::align_val_t{m_element_builder.get().alignment()});
                }
                std::destroy_at(set);
            }

            void copy_construct(void *dst, const void *src) const
            {
                std::construct_at(state(dst));
                assign(dst, src);
            }

            void move_construct(void *dst, void *src) const
            {
                std::construct_at(state(dst), std::move(*state(src)));
                state(src)->elements = nullptr;
                state(src)->size = 0;
                state(src)->capacity = 0;
            }

          private:
            static constexpr size_t npos = static_cast<size_t>(-1);

            [[nodiscard]] size_t element_stride() const noexcept
            {
                const size_t alignment = m_element_builder.get().alignment();
                const size_t size = m_element_builder.get().size();
                return ((size + alignment - 1) / alignment) * alignment;
            }

            [[nodiscard]] size_t find_index(const void *data, const void *element) const
            {
                const SetState *set = state(data);
                for (size_t i = 0; i < set->size; ++i) {
                    if (std::is_eq(element_dispatch().compare(set->elements + i * element_stride(), element))) {
                        return i;
                    }
                }
                return npos;
            }

            void reserve(SetState *set, size_t min_capacity) const
            {
                if (min_capacity <= set->capacity) { return; }

                const size_t new_capacity = std::max<size_t>(min_capacity, std::max<size_t>(4, set->capacity * 2));
                std::byte *new_elements = static_cast<std::byte *>(
                    ::operator new(new_capacity * element_stride(), std::align_val_t{m_element_builder.get().alignment()}));

                size_t i = 0;
                try {
                    for (; i < set->size; ++i) {
                        m_element_builder.get().move_construct(new_elements + i * element_stride(),
                                                               set->elements + i * element_stride(),
                                                               m_element_builder);
                    }
                } catch (...) {
                    for (size_t j = 0; j < i; ++j) {
                        destroy_slot(new_elements + j * element_stride());
                    }
                    ::operator delete(new_elements, std::align_val_t{m_element_builder.get().alignment()});
                    throw;
                }

                for (size_t j = 0; j < set->size; ++j) {
                    destroy_slot(set->elements + j * element_stride());
                }
                if (set->elements != nullptr) {
                    ::operator delete(set->elements, std::align_val_t{m_element_builder.get().alignment()});
                }

                set->elements = new_elements;
                set->capacity = new_capacity;
            }

            void destroy_slot(void *slot) const noexcept
            {
                if (m_element_builder.get().requires_destroy()) {
                    m_element_builder.get().destroy(slot);
                }
            }

            [[nodiscard]] static SetState *state(void *memory) noexcept
            {
                return std::launder(reinterpret_cast<SetState *>(memory));
            }

            [[nodiscard]] static const SetState *state(const void *memory) noexcept
            {
                return std::launder(reinterpret_cast<const SetState *>(memory));
            }

            std::reference_wrapper<const value::TypeMeta> m_schema;
            std::reference_wrapper<const ValueBuilder>    m_element_builder;
        };

        struct MapDispatch final : MapViewDispatch
        {
            explicit MapDispatch(const value::TypeMeta &schema)
                : m_schema(schema),
                  m_key_builder(ValueBuilderFactory::checked_builder_for(schema.key_type)),
                  m_value_builder(ValueBuilderFactory::checked_builder_for(schema.element_type))
            {
                if (schema.key_type == nullptr || schema.element_type == nullptr) {
                    throw std::runtime_error("Map schema requires key and value schemas");
                }
            }

            [[nodiscard]] size_t size(const void *data) const noexcept override { return state(data)->keys.size; }
            [[nodiscard]] const value::TypeMeta &key_schema() const noexcept override { return *m_schema.get().key_type; }
            [[nodiscard]] const value::TypeMeta &value_schema() const noexcept override { return *m_schema.get().element_type; }
            [[nodiscard]] const ViewDispatch &key_dispatch() const noexcept override { return m_key_builder.get().dispatch(); }
            [[nodiscard]] const ViewDispatch &value_dispatch() const noexcept override { return m_value_builder.get().dispatch(); }

            [[nodiscard]] size_t find(const void *data, const void *key) const override
            {
                const MapState *map = state(data);
                for (size_t i = 0; i < map->keys.size; ++i) {
                    if (std::is_eq(key_dispatch().compare(key_data(data, i), key))) { return i; }
                }
                return npos;
            }

            [[nodiscard]] void *key_data(void *data, size_t index) const override
            {
                if (index >= state(data)->keys.size) { throw std::out_of_range("Map key index out of range"); }
                return state(data)->keys.elements + index * key_stride();
            }

            [[nodiscard]] const void *key_data(const void *data, size_t index) const override
            {
                if (index >= state(data)->keys.size) { throw std::out_of_range("Map key index out of range"); }
                return state(data)->keys.elements + index * key_stride();
            }

            [[nodiscard]] void *value_data(void *data, size_t index) const override
            {
                if (index >= state(data)->keys.size) { throw std::out_of_range("Map value index out of range"); }
                return state(data)->values + index * value_stride();
            }

            [[nodiscard]] const void *value_data(const void *data, size_t index) const override
            {
                if (index >= state(data)->keys.size) { throw std::out_of_range("Map value index out of range"); }
                return state(data)->values + index * value_stride();
            }

            [[nodiscard]] bool value_valid(const void *data, size_t index) const override
            {
                return value::validity_bit_get(state(data)->value_validity, index);
            }

            void set_value_valid(void *data, size_t index, bool valid) const override
            {
                if (!valid && value_valid(data, index)) {
                    reset_value(value_data(data, index));
                }
                value::validity_bit_set(state(data)->value_validity, index, valid);
            }

            [[nodiscard]] bool set_item(void *data, const void *key, const void *value, bool value_is_valid) const override
            {
                MapState *map = state(data);
                if (const size_t index = find(data, key); index != npos) {
                    if (!value_is_valid) {
                        set_value_valid(data, index, false);
                    } else {
                        value_dispatch().assign(value_data(data, index), value);
                        value::validity_bit_set(map->value_validity, index, true);
                    }
                    return false;
                }

                reserve(map, map->keys.size + 1);
                void *new_key = map->keys.elements + map->keys.size * key_stride();
                void *new_value = map->values + map->keys.size * value_stride();
                m_key_builder.get().construct(new_key);
                key_dispatch().assign(new_key, key);
                m_value_builder.get().construct(new_value);
                if (value_is_valid) {
                    value_dispatch().assign(new_value, value);
                }
                value::validity_bit_set(map->value_validity, map->keys.size, value_is_valid);
                ++map->keys.size;
                return true;
            }

            [[nodiscard]] bool remove(void *data, const void *key) const override
            {
                MapState *map = state(data);
                const size_t index = find(data, key);
                if (index == npos) { return false; }

                destroy_key(key_data(data, index));
                destroy_value(value_data(data, index));

                if (index + 1 != map->keys.size) {
                    void *dst_key = map->keys.elements + index * key_stride();
                    void *src_key = map->keys.elements + (map->keys.size - 1) * key_stride();
                    void *dst_value = map->values + index * value_stride();
                    void *src_value = map->values + (map->keys.size - 1) * value_stride();

                    m_key_builder.get().move_construct(dst_key, src_key, m_key_builder);
                    m_value_builder.get().move_construct(dst_value, src_value, m_value_builder);
                    destroy_key(src_key);
                    destroy_value(src_value);
                    value::validity_bit_set(map->value_validity, index, value::validity_bit_get(map->value_validity, map->keys.size - 1));
                }

                --map->keys.size;
                value::validity_bit_set(map->value_validity, map->keys.size, false);
                return true;
            }

            void clear(void *data) const override
            {
                MapState *map = state(data);
                for (size_t i = 0; i < map->keys.size; ++i) {
                    destroy_key(map->keys.elements + i * key_stride());
                    destroy_value(map->values + i * value_stride());
                }
                value::validity_set_all(map->value_validity, map->keys.capacity, false);
                map->keys.size = 0;
            }

            [[nodiscard]] size_t hash(const void *data) const override
            {
                size_t result = 0;
                constexpr size_t null_hash_seed = 0x9e3779b97f4a7c15ULL;
                const MapState *map = state(data);
                for (size_t i = 0; i < map->keys.size; ++i) {
                    size_t pair_hash = key_dispatch().hash(key_data(data, i));
                    pair_hash ^= value_valid(data, i) ? value_dispatch().hash(value_data(data, i)) << 1 : (null_hash_seed << 1);
                    result ^= pair_hash;
                }
                return result;
            }

            [[nodiscard]] std::string to_string(const void *data) const override
            {
                std::string result = "{";
                const MapState *map = state(data);
                for (size_t i = 0; i < map->keys.size; ++i) {
                    if (i > 0) { result += ", "; }
                    result += key_dispatch().to_string(key_data(data, i));
                    result += ": ";
                    result += value_valid(data, i) ? value_dispatch().to_string(value_data(data, i)) : "None";
                }
                result += "}";
                return result;
            }

            [[nodiscard]] std::partial_ordering compare(const void *lhs, const void *rhs) const override
            {
                const MapState *a = state(lhs);
                const MapState *b = state(rhs);
                if (a->keys.size != b->keys.size) { return std::partial_ordering::unordered; }
                for (size_t i = 0; i < a->keys.size; ++i) {
                    const size_t rhs_index = find(rhs, key_data(lhs, i));
                    if (rhs_index == npos) { return std::partial_ordering::unordered; }
                    if (value_valid(lhs, i) != value_valid(rhs, rhs_index)) { return std::partial_ordering::unordered; }
                    if (value_valid(lhs, i)) {
                        const std::partial_ordering order =
                            value_dispatch().compare(value_data(lhs, i), value_data(rhs, rhs_index));
                        if (order != std::partial_ordering::equivalent) { return std::partial_ordering::unordered; }
                    }
                }
                return std::partial_ordering::equivalent;
            }

            [[nodiscard]] nb::object to_python(const void *data, const value::TypeMeta *schema) const override
            {
                static_cast<void>(schema);
                nb::dict result;
                const MapState *map = state(data);
                for (size_t i = 0; i < map->keys.size; ++i) {
                    nb::object py_key = key_dispatch().to_python(key_data(data, i), &key_schema());
                    nb::object py_value =
                        value_valid(data, i) ? value_dispatch().to_python(value_data(data, i), &value_schema()) : nb::none();
                    result[py_key] = py_value;
                }
                return result;
            }

            void from_python(void *dst, const nb::object &src, const value::TypeMeta *schema) const override
            {
                static_cast<void>(schema);
                if (!nb::isinstance<nb::dict>(src) && !nb::hasattr(src, "items")) {
                    throw std::runtime_error("Map value expects a dict or dict-like object");
                }

                clear(dst);
                const nb::object items = nb::hasattr(src, "items") ? src.attr("items")() : src;
                nb::iterator it = nb::iter(items);
                while (it != nb::iterator::sentinel()) {
                    const nb::tuple pair = nb::cast<nb::tuple>(*it);
                    if (pair.size() != 2) { throw std::runtime_error("Map items() must yield key/value pairs"); }
                    if (pair[0].is_none()) { throw std::runtime_error("Map value does not allow None keys"); }

                    void *temp_key = m_key_builder.get().allocate();
                    void *temp_value = nullptr;
                    try {
                        m_key_builder.get().construct(temp_key);
                        key_dispatch().from_python(temp_key, nb::borrow<nb::object>(pair[0]), &key_schema());

                        if (pair[1].is_none()) {
                            static_cast<void>(set_item(dst, temp_key, nullptr, false));
                        } else {
                            temp_value = m_value_builder.get().allocate();
                            m_value_builder.get().construct(temp_value);
                            value_dispatch().from_python(temp_value, nb::borrow<nb::object>(pair[1]), &value_schema());
                            static_cast<void>(set_item(dst, temp_key, temp_value, true));
                        }
                    } catch (...) {
                        if (temp_value != nullptr) {
                            if (m_value_builder.get().requires_destroy()) {
                                m_value_builder.get().destroy(temp_value);
                            }
                            m_value_builder.get().deallocate(temp_value);
                        }
                        if (temp_key != nullptr) {
                            if (m_key_builder.get().requires_destroy()) {
                                m_key_builder.get().destroy(temp_key);
                            }
                            m_key_builder.get().deallocate(temp_key);
                        }
                        throw;
                    }

                    if (temp_value != nullptr) {
                        if (m_value_builder.get().requires_destroy()) {
                            m_value_builder.get().destroy(temp_value);
                        }
                        m_value_builder.get().deallocate(temp_value);
                    }
                    if (m_key_builder.get().requires_destroy()) {
                        m_key_builder.get().destroy(temp_key);
                    }
                    m_key_builder.get().deallocate(temp_key);
                    ++it;
                }
            }

            void assign(void *dst, const void *src) const override
            {
                clear(dst);
                const MapState *map = state(src);
                reserve(state(dst), map->keys.size);
                for (size_t i = 0; i < map->keys.size; ++i) {
                    static_cast<void>(set_item(dst, key_data(src, i), value_data(src, i), value_valid(src, i)));
                }
            }

            void set_from_cpp(void *dst, const void *src, const value::TypeMeta *src_schema) const override
            {
                static_cast<void>(dst);
                static_cast<void>(src);
                static_cast<void>(src_schema);
                throw std::invalid_argument("Map value set_from_cpp is not implemented");
            }

            void move_from_cpp(void *dst, void *src, const value::TypeMeta *src_schema) const override
            {
                static_cast<void>(dst);
                static_cast<void>(src);
                static_cast<void>(src_schema);
                throw std::invalid_argument("Map value move_from_cpp is not implemented");
            }

            void construct(void *memory) const
            {
                std::construct_at(state(memory));
            }

            void destroy(void *memory) const noexcept
            {
                MapState *map = state(memory);
                clear(map);
                if (map->keys.elements != nullptr) {
                    ::operator delete(map->keys.elements, std::align_val_t{m_key_builder.get().alignment()});
                }
                if (map->values != nullptr) {
                    ::operator delete(map->values, std::align_val_t{m_value_builder.get().alignment()});
                }
                if (map->value_validity != nullptr) {
                    ::operator delete(map->value_validity);
                }
                std::destroy_at(map);
            }

            void copy_construct(void *dst, const void *src) const
            {
                std::construct_at(state(dst));
                assign(dst, src);
            }

            void move_construct(void *dst, void *src) const
            {
                std::construct_at(state(dst), std::move(*state(src)));
                state(src)->keys.elements = nullptr;
                state(src)->values = nullptr;
                state(src)->value_validity = nullptr;
                state(src)->keys.size = 0;
                state(src)->keys.capacity = 0;
            }

          private:
            static constexpr size_t npos = static_cast<size_t>(-1);

            [[nodiscard]] size_t key_stride() const noexcept
            {
                const size_t alignment = m_key_builder.get().alignment();
                const size_t size = m_key_builder.get().size();
                return ((size + alignment - 1) / alignment) * alignment;
            }

            [[nodiscard]] size_t value_stride() const noexcept
            {
                const size_t alignment = m_value_builder.get().alignment();
                const size_t size = m_value_builder.get().size();
                return ((size + alignment - 1) / alignment) * alignment;
            }

            void reserve(MapState *map, size_t min_capacity) const
            {
                if (min_capacity <= map->keys.capacity) { return; }
                const size_t new_capacity = std::max<size_t>(min_capacity, std::max<size_t>(4, map->keys.capacity * 2));

                std::byte *new_keys = static_cast<std::byte *>(
                    ::operator new(new_capacity * key_stride(), std::align_val_t{m_key_builder.get().alignment()}));
                std::byte *new_values = static_cast<std::byte *>(
                    ::operator new(new_capacity * value_stride(), std::align_val_t{m_value_builder.get().alignment()}));
                std::byte *new_validity = static_cast<std::byte *>(::operator new(value::validity_mask_bytes(new_capacity)));
                value::validity_set_all(new_validity, new_capacity, false);

                size_t i = 0;
                try {
                    for (; i < map->keys.size; ++i) {
                        m_key_builder.get().move_construct(new_keys + i * key_stride(), map->keys.elements + i * key_stride(), m_key_builder);
                        m_value_builder.get().move_construct(new_values + i * value_stride(), map->values + i * value_stride(), m_value_builder);
                        value::validity_bit_set(new_validity, i, value::validity_bit_get(map->value_validity, i));
                    }
                } catch (...) {
                    for (size_t j = 0; j < i; ++j) {
                        destroy_key(new_keys + j * key_stride());
                        destroy_value(new_values + j * value_stride());
                    }
                    ::operator delete(new_keys, std::align_val_t{m_key_builder.get().alignment()});
                    ::operator delete(new_values, std::align_val_t{m_value_builder.get().alignment()});
                    ::operator delete(new_validity);
                    throw;
                }

                for (size_t j = 0; j < map->keys.size; ++j) {
                    destroy_key(map->keys.elements + j * key_stride());
                    destroy_value(map->values + j * value_stride());
                }
                if (map->keys.elements != nullptr) {
                    ::operator delete(map->keys.elements, std::align_val_t{m_key_builder.get().alignment()});
                }
                if (map->values != nullptr) {
                    ::operator delete(map->values, std::align_val_t{m_value_builder.get().alignment()});
                }
                if (map->value_validity != nullptr) {
                    ::operator delete(map->value_validity);
                }

                map->keys.elements = new_keys;
                map->values = new_values;
                map->value_validity = new_validity;
                map->keys.capacity = new_capacity;
            }

            void destroy_key(void *slot) const noexcept
            {
                if (m_key_builder.get().requires_destroy()) {
                    m_key_builder.get().destroy(slot);
                }
            }

            void destroy_value(void *slot) const noexcept
            {
                if (m_value_builder.get().requires_destroy()) {
                    m_value_builder.get().destroy(slot);
                }
            }

            void reset_value(void *slot) const
            {
                destroy_value(slot);
                m_value_builder.get().construct(slot);
            }

            [[nodiscard]] static MapState *state(void *memory) noexcept
            {
                return std::launder(reinterpret_cast<MapState *>(memory));
            }

            [[nodiscard]] static const MapState *state(const void *memory) noexcept
            {
                return std::launder(reinterpret_cast<const MapState *>(memory));
            }

            std::reference_wrapper<const value::TypeMeta> m_schema;
            std::reference_wrapper<const ValueBuilder>    m_key_builder;
            std::reference_wrapper<const ValueBuilder>    m_value_builder;
        };

        template <typename TDispatch> struct AssociativeStateOps final : StateOps
        {
            explicit AssociativeStateOps(const TDispatch &dispatch) noexcept
                : m_dispatch(dispatch)
            {
            }

            void expand_builder(ValueBuilder &builder, const value::TypeMeta &schema) const noexcept override
            {
                static_cast<void>(schema);
                if constexpr (std::is_same_v<TDispatch, SetDispatch>) {
                    builder.cache_layout(sizeof(SetState), alignof(SetState));
                } else {
                    builder.cache_layout(sizeof(MapState), alignof(MapState));
                }
                builder.cache_lifecycle(true, true, false);
            }

            [[nodiscard]] const ViewDispatch &view_dispatch(const value::TypeMeta &schema) const noexcept override
            {
                static_cast<void>(schema);
                return m_dispatch;
            }

            [[nodiscard]] bool requires_destroy(const value::TypeMeta &schema) const noexcept override
            {
                static_cast<void>(schema);
                return true;
            }

            [[nodiscard]] bool requires_deallocate(const value::TypeMeta &schema) const noexcept override
            {
                static_cast<void>(schema);
                return true;
            }

            [[nodiscard]] bool stores_inline_in_value_handle(const value::TypeMeta &schema) const noexcept override
            {
                static_cast<void>(schema);
                return false;
            }

            void construct(void *memory) const override { m_dispatch.get().construct(memory); }
            void destroy(void *memory) const noexcept override { m_dispatch.get().destroy(memory); }
            void copy_construct(void *dst, const void *src) const override { m_dispatch.get().copy_construct(dst, src); }
            void move_construct(void *dst, void *src) const override { m_dispatch.get().move_construct(dst, src); }

            std::reference_wrapper<const TDispatch> m_dispatch;
        };

        struct CachedBuilderEntry
        {
            std::shared_ptr<const ViewDispatch> dispatch;
            std::shared_ptr<const StateOps>     state_ops;
            std::shared_ptr<const ValueBuilder> builder;
        };

        const ValueBuilder *associative_builder_for(const value::TypeMeta *schema)
        {
            if (schema == nullptr) { return nullptr; }
            if (schema->kind != value::TypeKind::Set && schema->kind != value::TypeKind::Map) { return nullptr; }

            static std::mutex cache_mutex;
            static std::unordered_map<const value::TypeMeta *, CachedBuilderEntry> cache;

            std::lock_guard lock(cache_mutex);
            if (auto it = cache.find(schema); it != cache.end()) {
                return it->second.builder.get();
            }

            CachedBuilderEntry entry;
            if (schema->kind == value::TypeKind::Set) {
                auto dispatch = std::make_shared<SetDispatch>(*schema);
                auto state_ops = std::make_shared<AssociativeStateOps<SetDispatch>>(*dispatch);
                auto builder = std::make_shared<ValueBuilder>(*schema, *state_ops);
                entry.dispatch = std::move(dispatch);
                entry.state_ops = std::move(state_ops);
                entry.builder = std::move(builder);
            } else {
                auto dispatch = std::make_shared<MapDispatch>(*schema);
                auto state_ops = std::make_shared<AssociativeStateOps<MapDispatch>>(*dispatch);
                auto builder = std::make_shared<ValueBuilder>(*schema, *state_ops);
                entry.dispatch = std::move(dispatch);
                entry.state_ops = std::move(state_ops);
                entry.builder = std::move(builder);
            }

            auto [it, inserted] = cache.emplace(schema, std::move(entry));
            static_cast<void>(inserted);
            return it->second.builder.get();
        }

    }  // namespace detail

    SetView::SetView(const View &view)
        : View(view)
    {
        if (!view.valid()) { return; }
        if (view.schema() == nullptr || view.schema()->kind != value::TypeKind::Set) {
            throw std::runtime_error("SetView requires a set schema");
        }
    }

    size_t SetView::size() const
    {
        const auto *dispatch = set_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("SetView::size on invalid view"); }
        return dispatch->size(data());
    }

    bool SetView::empty() const
    {
        return size() == 0;
    }

    const value::TypeMeta *SetView::element_schema() const
    {
        const auto *dispatch = set_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("SetView::element_schema on invalid view"); }
        return &dispatch->element_schema();
    }

    View SetView::at(size_t index)
    {
        const auto *dispatch = set_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("SetView::at on invalid view"); }
        return View{&dispatch->element_dispatch(), dispatch->element_data(data(), index), &dispatch->element_schema()};
    }

    View SetView::at(size_t index) const
    {
        const auto *dispatch = set_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("SetView::at on invalid view"); }
        return View{&dispatch->element_dispatch(), const_cast<void *>(dispatch->element_data(data(), index)), &dispatch->element_schema()};
    }

    bool SetView::contains(const View &value) const
    {
        const auto *dispatch = set_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("SetView::contains on invalid view"); }
        if (!value.valid() || value.schema() != &dispatch->element_schema()) {
            throw std::invalid_argument("SetView::contains requires a valid matching-schema element");
        }
        return dispatch->contains(data(), data_of(value));
    }

    bool SetView::add(const View &value)
    {
        const auto *dispatch = set_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("SetView::add on invalid view"); }
        if (!value.valid() || value.schema() != &dispatch->element_schema()) {
            throw std::invalid_argument("SetView::add requires a valid matching-schema element");
        }
        return dispatch->add(data(), data_of(value));
    }

    bool SetView::remove(const View &value)
    {
        const auto *dispatch = set_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("SetView::remove on invalid view"); }
        if (!value.valid() || value.schema() != &dispatch->element_schema()) {
            throw std::invalid_argument("SetView::remove requires a valid matching-schema element");
        }
        return dispatch->remove(data(), data_of(value));
    }

    void SetView::clear()
    {
        const auto *dispatch = set_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("SetView::clear on invalid view"); }
        dispatch->clear(data());
    }

    const detail::SetViewDispatch *SetView::set_dispatch() const noexcept
    {
        return valid() ? static_cast<const detail::SetViewDispatch *>(dispatch()) : nullptr;
    }

    MapView::MapView(const View &view)
        : View(view)
    {
        if (!view.valid()) { return; }
        if (view.schema() == nullptr || view.schema()->kind != value::TypeKind::Map) {
            throw std::runtime_error("MapView requires a map schema");
        }
    }

    size_t MapView::size() const
    {
        const auto *dispatch = map_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("MapView::size on invalid view"); }
        return dispatch->size(data());
    }

    bool MapView::empty() const
    {
        return size() == 0;
    }

    const value::TypeMeta *MapView::key_schema() const
    {
        const auto *dispatch = map_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("MapView::key_schema on invalid view"); }
        return &dispatch->key_schema();
    }

    const value::TypeMeta *MapView::value_schema() const
    {
        const auto *dispatch = map_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("MapView::value_schema on invalid view"); }
        return &dispatch->value_schema();
    }

    bool MapView::contains(const View &key) const
    {
        const auto *dispatch = map_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("MapView::contains on invalid view"); }
        if (!key.valid() || key.schema() != &dispatch->key_schema()) {
            throw std::invalid_argument("MapView::contains requires a valid matching-schema key");
        }
        return dispatch->find(data(), data_of(key)) != static_cast<size_t>(-1);
    }

    View MapView::at(const View &key)
    {
        const auto *dispatch = map_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("MapView::at on invalid view"); }
        if (!key.valid() || key.schema() != &dispatch->key_schema()) {
            throw std::invalid_argument("MapView::at requires a valid matching-schema key");
        }
        const size_t index = dispatch->find(data(), data_of(key));
        if (index == static_cast<size_t>(-1)) { throw std::out_of_range("MapView::at key not found"); }
        if (!dispatch->value_valid(data(), index)) {
            return View::invalid_for(&dispatch->value_schema());
        }
        return View{&dispatch->value_dispatch(), dispatch->value_data(data(), index), &dispatch->value_schema()};
    }

    View MapView::at(const View &key) const
    {
        const auto *dispatch = map_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("MapView::at on invalid view"); }
        if (!key.valid() || key.schema() != &dispatch->key_schema()) {
            throw std::invalid_argument("MapView::at requires a valid matching-schema key");
        }
        const size_t index = dispatch->find(data(), data_of(key));
        if (index == static_cast<size_t>(-1)) { throw std::out_of_range("MapView::at key not found"); }
        if (!dispatch->value_valid(data(), index)) {
            return View::invalid_for(&dispatch->value_schema());
        }
        return View{&dispatch->value_dispatch(),
                    const_cast<void *>(dispatch->value_data(data(), index)),
                    &dispatch->value_schema()};
    }

    void MapView::set(const View &key, const View &value)
    {
        const auto *dispatch = map_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("MapView::set on invalid view"); }
        if (!key.valid() || key.schema() != &dispatch->key_schema()) {
            throw std::invalid_argument("MapView::set requires a valid matching-schema key");
        }
        if (value.schema() != nullptr && value.schema() != &dispatch->value_schema()) {
            throw std::invalid_argument("MapView::set requires a matching value schema");
        }
        dispatch->set_item(data(), data_of(key), value.valid() ? data_of(value) : nullptr, value.valid());
    }

    bool MapView::remove(const View &key)
    {
        const auto *dispatch = map_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("MapView::remove on invalid view"); }
        if (!key.valid() || key.schema() != &dispatch->key_schema()) {
            throw std::invalid_argument("MapView::remove requires a valid matching-schema key");
        }
        return dispatch->remove(data(), data_of(key));
    }

    void MapView::clear()
    {
        const auto *dispatch = map_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("MapView::clear on invalid view"); }
        dispatch->clear(data());
    }

    const detail::MapViewDispatch *MapView::map_dispatch() const noexcept
    {
        return valid() ? static_cast<const detail::MapViewDispatch *>(dispatch()) : nullptr;
    }

}  // namespace hgraph
