#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/value/list.h>
#include <hgraph/types/time_series/value/state.h>

#include <algorithm>
#include <cstring>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <unordered_map>

namespace hgraph
{

    namespace detail
    {

        struct FixedListState
        {
            size_t mutation_depth{0};
        };

        struct ListDispatchBase : ListViewDispatch
        {
            ListDispatchBase(const value::TypeMeta &schema, const ValueBuilder &element_builder) noexcept
                : m_schema(schema),
                  m_element_builder(element_builder),
                  m_element_requires_destroy(element_builder.requires_destroy())
            {
            }

            [[nodiscard]] const value::TypeMeta &element_schema() const noexcept override
            {
                return *m_schema.get().element_type;
            }

            [[nodiscard]] const ViewDispatch &element_dispatch() const noexcept override
            {
                return m_element_builder.get().dispatch();
            }

            [[nodiscard]] const ValueBuilder &element_builder() const noexcept
            {
                return m_element_builder;
            }

            [[nodiscard]] bool element_requires_destroy() const noexcept
            {
                return m_element_requires_destroy;
            }

            [[nodiscard]] static size_t align_up(size_t value, size_t alignment) noexcept
            {
                return alignment <= 1 ? value : ((value + alignment - 1) / alignment) * alignment;
            }

            [[nodiscard]] size_t hash(const void *data) const override
            {
                size_t result                    = 0;
                constexpr size_t null_hash_seed = 0x9e3779b97f4a7c15ULL;
                for (size_t i = 0; i < size(data); ++i) {
                    size_t element_hash = null_hash_seed + i;
                    if (element_valid(data, i)) {
                        element_hash = element_dispatch().hash(element_data(data, i));
                    }
                    result ^= element_hash + 0x9e3779b9 + (result << 6) + (result >> 2);
                }
                return result;
            }

            [[nodiscard]] std::string to_string(const void *data) const override
            {
                std::string result = "[";
                for (size_t i = 0; i < size(data); ++i) {
                    if (i > 0) { result += ", "; }
                    if (!element_valid(data, i)) {
                        result += "None";
                    } else {
                        result += element_dispatch().to_string(element_data(data, i));
                    }
                }
                result += "]";
                return result;
            }

            [[nodiscard]] std::partial_ordering compare(const void *lhs, const void *rhs) const override
            {
                const size_t lhs_size = size(lhs);
                const size_t rhs_size = size(rhs);
                const size_t count    = std::min(lhs_size, rhs_size);

                for (size_t i = 0; i < count; ++i) {
                    const bool lhs_valid = element_valid(lhs, i);
                    const bool rhs_valid = element_valid(rhs, i);
                    if (!lhs_valid || !rhs_valid) {
                        if (lhs_valid != rhs_valid) { return std::partial_ordering::unordered; }
                        continue;
                    }

                    const std::partial_ordering element_order =
                        element_dispatch().compare(element_data(lhs, i), element_data(rhs, i));
                    if (std::is_lt(element_order) || std::is_gt(element_order) ||
                        element_order == std::partial_ordering::unordered) {
                        return element_order;
                    }
                }

                if (lhs_size < rhs_size) { return std::partial_ordering::less; }
                if (lhs_size > rhs_size) { return std::partial_ordering::greater; }
                return std::partial_ordering::equivalent;
            }

            [[nodiscard]] nb::object to_python(const void *data, const value::TypeMeta *schema) const override
            {
                nb::list result;
                for (size_t i = 0; i < size(data); ++i) {
                    if (!element_valid(data, i)) {
                        result.append(nb::none());
                    } else {
                        result.append(element_dispatch().to_python(element_data(data, i), &element_schema()));
                    }
                }
                if (schema != nullptr && schema->is_variadic_tuple()) {
                    return nb::tuple(result);
                }
                return nb::object(result);
            }

            void from_python(void *dst, const nb::object &src, const value::TypeMeta *schema) const override
            {
                static_cast<void>(schema);
                if (!nb::isinstance<nb::list>(src) && !nb::isinstance<nb::tuple>(src)) {
                    throw std::runtime_error("List value expects a list or tuple");
                }

                const nb::sequence sequence = nb::cast<nb::sequence>(src);
                const size_t count = nb::len(sequence);

                if (is_fixed()) {
                    const size_t copy_count = std::min(count, size(dst));
                    for (size_t i = 0; i < copy_count; ++i) {
                        nb::object element = sequence[i];
                        if (element.is_none()) {
                            set_element_valid(dst, i, false);
                        } else {
                            element_dispatch().from_python(element_data(dst, i), element, &element_schema());
                            set_element_valid(dst, i, true);
                        }
                    }
                    return;
                }

                resize(dst, count);
                for (size_t i = 0; i < count; ++i) {
                    nb::object element = sequence[i];
                    if (element.is_none()) {
                        set_element_valid(dst, i, false);
                    } else {
                        element_dispatch().from_python(element_data(dst, i), element, &element_schema());
                        set_element_valid(dst, i, true);
                    }
                }
            }

            void assign(void *dst, const void *src) const override
            {
                if (size(dst) != size(src)) {
                    if (is_fixed()) {
                        throw std::runtime_error("Fixed-size list assignment requires matching size");
                    }
                    resize(dst, size(src));
                }

                for (size_t i = 0; i < size(src); ++i) {
                    if (!element_valid(src, i)) {
                        set_element_valid(dst, i, false);
                        continue;
                    }
                    element_dispatch().assign(element_data(dst, i), element_data(src, i));
                    set_element_valid(dst, i, true);
                }
            }

            void set_from_cpp(void *dst, const void *src, const value::TypeMeta *src_schema) const override
            {
                static_cast<void>(dst);
                static_cast<void>(src);
                static_cast<void>(src_schema);
                throw std::invalid_argument("List set_from_cpp is not implemented");
            }

            void move_from_cpp(void *dst, void *src, const value::TypeMeta *src_schema) const override
            {
                static_cast<void>(dst);
                static_cast<void>(src);
                static_cast<void>(src_schema);
                throw std::invalid_argument("List move_from_cpp is not implemented");
            }

          protected:
            [[nodiscard]] size_t element_stride() const noexcept
            {
                const size_t alignment = m_element_builder.get().alignment();
                const size_t size = m_element_builder.get().size();
                return ((size + alignment - 1) / alignment) * alignment;
            }

            void construct_elements(std::byte *base, size_t count) const
            {
                for (size_t i = 0; i < count; ++i) {
                    m_element_builder.get().construct(base + element_stride() * i);
                }
            }

            void destroy_elements(std::byte *base, size_t count) const noexcept
            {
                if (!element_requires_destroy()) { return; }
                for (size_t i = 0; i < count; ++i) {
                    m_element_builder.get().destroy(base + element_stride() * i);
                }
            }

            void copy_elements(std::byte *dst, const std::byte *src, size_t count) const
            {
                for (size_t i = 0; i < count; ++i) {
                    m_element_builder.get().copy_construct(dst + element_stride() * i,
                                                           src + element_stride() * i,
                                                           m_element_builder);
                }
            }

            void move_elements(std::byte *dst, std::byte *src, size_t count) const
            {
                for (size_t i = 0; i < count; ++i) {
                    m_element_builder.get().move_construct(dst + element_stride() * i,
                                                           src + element_stride() * i,
                                                           m_element_builder);
                }
            }

            void invalidate_element(std::byte *base, size_t index) const
            {
                std::byte *element = base + element_stride() * index;
                if (element_requires_destroy()) {
                    m_element_builder.get().destroy(element);
                }
                m_element_builder.get().construct(element);
            }

            std::reference_wrapper<const value::TypeMeta>  m_schema;
            std::reference_wrapper<const ValueBuilder>     m_element_builder;
            // The element builder is stable for the life of the dispatch, so cache
            // lifecycle traits once to keep slot-management code readable.
            bool                                          m_element_requires_destroy{true};
        };

        template <MutationTracking TTracking> struct FixedListDispatch final : ListDispatchBase
        {
            static constexpr MutationTracking tracking_mode = TTracking;
            static constexpr bool tracks_deltas_v = TTracking == MutationTracking::Delta;

            FixedListDispatch(const value::TypeMeta &schema, const ValueBuilder &element_builder) noexcept
                : ListDispatchBase(schema, element_builder),
                  m_fixed_size(schema.fixed_size)
            {
            }

            [[nodiscard]] size_t size(const void *data) const noexcept override
            {
                static_cast<void>(data);
                return m_fixed_size;
            }

            [[nodiscard]] bool is_fixed() const noexcept override
            {
                return true;
            }

            void begin_mutation(void *data) const override
            {
                if constexpr (tracks_deltas_v) {
                    FixedListState *list = state(data);
                    if (list->mutation_depth++ == 0) {
                        value::validity_set_all(updated_memory(data), m_fixed_size, false);
                    }
                }
            }

            void end_mutation(void *data) const override
            {
                if constexpr (tracks_deltas_v) {
                    FixedListState *list = state(data);
                    if (list->mutation_depth == 0) {
                        throw std::runtime_error("Fixed list mutation depth underflow");
                    }
                    --list->mutation_depth;
                }
            }

            [[nodiscard]] void *element_data(void *data, size_t index) const override
            {
                if (index >= m_fixed_size) { throw std::out_of_range("Fixed list index out of range"); }
                return static_cast<std::byte *>(elements_memory(data)) + this->element_stride() * index;
            }

            [[nodiscard]] const void *element_data(const void *data, size_t index) const override
            {
                if (index >= m_fixed_size) { throw std::out_of_range("Fixed list index out of range"); }
                return static_cast<const std::byte *>(elements_memory(data)) + this->element_stride() * index;
            }

            [[nodiscard]] bool element_valid(const void *data, size_t index) const override
            {
                if (index >= m_fixed_size) { throw std::out_of_range("Fixed list index out of range"); }
                return value::validity_bit_get(validity_memory(data), index);
            }

            [[nodiscard]] bool slot_updated(const void *data, size_t index) const noexcept override
            {
                if constexpr (tracks_deltas_v) {
                    return index < m_fixed_size && value::validity_bit_get(updated_memory(data), index);
                } else {
                    static_cast<void>(data);
                    static_cast<void>(index);
                    return false;
                }
            }

            [[nodiscard]] bool slot_added(const void *data, size_t index) const noexcept override
            {
                static_cast<void>(data);
                static_cast<void>(index);
                return false;
            }

            void set_element_valid(void *data, size_t index, bool valid) const override
            {
                if (index >= m_fixed_size) { throw std::out_of_range("Fixed list index out of range"); }
                if (!valid && value::validity_bit_get(validity_memory(data), index)) {
                    this->invalidate_element(static_cast<std::byte *>(elements_memory(data)), index);
                }
                value::validity_bit_set(validity_memory(data), index, valid);
                if constexpr (tracks_deltas_v) {
                    if (state(data)->mutation_depth > 0) {
                        value::validity_bit_set(updated_memory(data), index, true);
                    }
                }
            }

            void resize(void *data, size_t new_size) const override
            {
                static_cast<void>(data);
                if (new_size != m_fixed_size) {
                    throw std::runtime_error("Fixed-size list length cannot be changed");
                }
            }

            void clear(void *data) const override
            {
                for (size_t i = 0; i < m_fixed_size; ++i) {
                    set_element_valid(data, i, false);
                }
            }

            [[nodiscard]] size_t allocation_size() const noexcept
            {
                return header_size() + this->element_stride() * m_fixed_size + value::validity_mask_bytes(m_fixed_size) +
                       (tracks_deltas_v ? value::validity_mask_bytes(m_fixed_size) : 0);
            }

            [[nodiscard]] size_t allocation_alignment() const noexcept
            {
                return tracks_deltas_v ? std::max(this->element_builder().alignment(), alignof(FixedListState))
                                       : this->element_builder().alignment();
            }

            void construct(void *memory) const
            {
                if constexpr (tracks_deltas_v) { std::construct_at(state(memory)); }
                this->construct_elements(static_cast<std::byte *>(elements_memory(memory)), m_fixed_size);
                value::validity_set_all(validity_memory(memory), m_fixed_size, true);
                if constexpr (tracks_deltas_v) { value::validity_set_all(updated_memory(memory), m_fixed_size, false); }
            }

            void destroy(void *memory) const noexcept
            {
                this->destroy_elements(static_cast<std::byte *>(elements_memory(memory)), m_fixed_size);
                if constexpr (tracks_deltas_v) { std::destroy_at(state(memory)); }
            }

            void copy_construct(void *dst, const void *src) const
            {
                if constexpr (tracks_deltas_v) { std::construct_at(state(dst)); }
                this->copy_elements(static_cast<std::byte *>(elements_memory(dst)),
                                    static_cast<const std::byte *>(elements_memory(src)),
                                    m_fixed_size);
                std::memcpy(validity_memory(dst), validity_memory(src), value::validity_mask_bytes(m_fixed_size));
                if constexpr (tracks_deltas_v) { value::validity_set_all(updated_memory(dst), m_fixed_size, false); }
            }

            void move_construct(void *dst, void *src) const
            {
                if constexpr (tracks_deltas_v) { std::construct_at(state(dst)); }
                this->move_elements(static_cast<std::byte *>(elements_memory(dst)),
                                    static_cast<std::byte *>(elements_memory(src)),
                                    m_fixed_size);
                std::memcpy(validity_memory(dst), validity_memory(src), value::validity_mask_bytes(m_fixed_size));
                if constexpr (tracks_deltas_v) {
                    value::validity_set_all(updated_memory(dst), m_fixed_size, false);
                    state(src)->mutation_depth = 0;
                }
            }

          private:
            [[nodiscard]] size_t header_size() const noexcept
            {
                return tracks_deltas_v ? align_up(sizeof(FixedListState), this->element_builder().alignment()) : 0;
            }

            [[nodiscard]] FixedListState *state(void *data) const noexcept
            {
                return std::launder(reinterpret_cast<FixedListState *>(data));
            }

            [[nodiscard]] const FixedListState *state(const void *data) const noexcept
            {
                return std::launder(reinterpret_cast<const FixedListState *>(data));
            }

            [[nodiscard]] void *elements_memory(void *data) const noexcept
            {
                return static_cast<std::byte *>(data) + header_size();
            }

            [[nodiscard]] const void *elements_memory(const void *data) const noexcept
            {
                return static_cast<const std::byte *>(data) + header_size();
            }

            [[nodiscard]] std::byte *validity_memory(void *data) const noexcept
            {
                return static_cast<std::byte *>(elements_memory(data)) + this->element_stride() * m_fixed_size;
            }

            [[nodiscard]] const std::byte *validity_memory(const void *data) const noexcept
            {
                return static_cast<const std::byte *>(elements_memory(data)) + this->element_stride() * m_fixed_size;
            }

            [[nodiscard]] std::byte *updated_memory(void *data) const noexcept
            {
                return validity_memory(data) + value::validity_mask_bytes(m_fixed_size);
            }

            [[nodiscard]] const std::byte *updated_memory(const void *data) const noexcept
            {
                return validity_memory(data) + value::validity_mask_bytes(m_fixed_size);
            }

            size_t m_fixed_size{0};
        };

        struct PlainDynamicListState
        {
            std::byte *data{nullptr};
            std::byte *validity{nullptr};
            size_t     size{0};
            size_t     capacity{0};
        };

        struct DeltaDynamicListState
        {
            std::byte *data{nullptr};
            std::byte *validity{nullptr};
            std::byte *updated{nullptr};
            std::byte *added{nullptr};
            size_t     size{0};
            size_t     capacity{0};
            size_t     mutation_depth{0};
        };

        template <MutationTracking TTracking> struct DynamicListDispatch final : ListDispatchBase
        {
            static constexpr MutationTracking tracking_mode = TTracking;
            static constexpr bool tracks_deltas_v = TTracking == MutationTracking::Delta;
            using DynamicListState = std::conditional_t<tracks_deltas_v, DeltaDynamicListState, PlainDynamicListState>;

            DynamicListDispatch(const value::TypeMeta &schema, const ValueBuilder &element_builder) noexcept
                : ListDispatchBase(schema, element_builder)
            {
            }

            [[nodiscard]] size_t size(const void *data) const noexcept override
            {
                return state(data)->size;
            }

            [[nodiscard]] bool is_fixed() const noexcept override
            {
                return false;
            }

            [[nodiscard]] bool tracks_deltas() const noexcept { return tracks_deltas_v; }

            void begin_mutation(void *data) const override
            {
                if constexpr (tracks_deltas_v) {
                    DynamicListState *list = state(data);
                    if (list->mutation_depth++ == 0) {
                        if (list->updated != nullptr) { value::validity_set_all(list->updated, list->capacity, false); }
                        if (list->added != nullptr) { value::validity_set_all(list->added, list->capacity, false); }
                    }
                }
            }

            void end_mutation(void *data) const override
            {
                if constexpr (tracks_deltas_v) {
                    DynamicListState *list = state(data);
                    if (list->mutation_depth == 0) {
                        throw std::runtime_error("Dynamic list mutation depth underflow");
                    }
                    --list->mutation_depth;
                }
            }

            [[nodiscard]] void *element_data(void *data, size_t index) const override
            {
                const size_t list_size = size(data);
                if (index >= list_size) { throw std::out_of_range("Dynamic list index out of range"); }
                return data_memory(data) + this->element_stride() * index;
            }

            [[nodiscard]] const void *element_data(const void *data, size_t index) const override
            {
                const size_t list_size = size(data);
                if (index >= list_size) { throw std::out_of_range("Dynamic list index out of range"); }
                return data_memory(data) + this->element_stride() * index;
            }

            [[nodiscard]] bool element_valid(const void *data, size_t index) const override
            {
                const size_t list_size = size(data);
                if (index >= list_size) { throw std::out_of_range("Dynamic list index out of range"); }
                return value::validity_bit_get(validity_memory(data), index);
            }

            [[nodiscard]] bool slot_updated(const void *data, size_t index) const noexcept override
            {
                if constexpr (tracks_deltas_v) {
                    const DynamicListState *list = state(data);
                    return index < list->size && list->updated != nullptr &&
                           value::validity_bit_get(list->updated, index) &&
                           !(list->added != nullptr && value::validity_bit_get(list->added, index));
                } else {
                    static_cast<void>(data);
                    static_cast<void>(index);
                    return false;
                }
            }

            [[nodiscard]] bool slot_added(const void *data, size_t index) const noexcept override
            {
                if constexpr (tracks_deltas_v) {
                    const DynamicListState *list = state(data);
                    return index < list->size && list->added != nullptr && value::validity_bit_get(list->added, index);
                } else {
                    static_cast<void>(data);
                    static_cast<void>(index);
                    return false;
                }
            }

            void set_element_valid(void *data, size_t index, bool valid) const override
            {
                if (index >= size(data)) { throw std::out_of_range("Dynamic list index out of range"); }
                if (!valid && value::validity_bit_get(validity_memory(data), index)) {
                    this->invalidate_element(data_memory(data), index);
                }
                value::validity_bit_set(validity_memory(data), index, valid);
                if constexpr (tracks_deltas_v) {
                    if (state(data)->mutation_depth > 0 &&
                        (state(data)->added == nullptr || !value::validity_bit_get(state(data)->added, index))) {
                        value::validity_bit_set(state(data)->updated, index, true);
                    }
                }
            }

            void resize(void *data, size_t new_size) const override
            {
                if (new_size > capacity(data)) {
                    reserve(data, new_size);
                }
                const size_t current_size = size(data);
                if (new_size > current_size) {
                    this->construct_elements(data_memory(data) + this->element_stride() * current_size, new_size - current_size);
                    value::validity_set_range(validity_memory(data), current_size, new_size - current_size, true);
                    if constexpr (tracks_deltas_v) {
                        value::validity_set_range(state(data)->updated, current_size, new_size - current_size, false);
                        if (state(data)->mutation_depth > 0) {
                            value::validity_set_range(state(data)->added, current_size, new_size - current_size, true);
                        } else {
                            value::validity_set_range(state(data)->added, current_size, new_size - current_size, false);
                        }
                    }
                } else if (new_size < current_size) {
                    this->destroy_elements(data_memory(data) + this->element_stride() * new_size, current_size - new_size);
                    value::validity_set_range(validity_memory(data), new_size, current_size - new_size, false);
                    if constexpr (tracks_deltas_v) {
                        value::validity_set_range(state(data)->updated, new_size, current_size - new_size, false);
                        value::validity_set_range(state(data)->added, new_size, current_size - new_size, false);
                    }
                }
                set_size(data, new_size);
            }

            void clear(void *data) const override
            {
                resize(data, 0);
            }

            void construct(void *memory) const
            {
                std::construct_at(state(memory));
            }

            void destroy(void *memory) const noexcept
            {
                this->destroy_elements(data_memory(memory), size(memory));
                if (data_memory(memory) != nullptr) {
                    ::operator delete(data_memory(memory), std::align_val_t{this->m_element_builder.get().alignment()});
                }
                if (validity_memory(memory) != nullptr) {
                    ::operator delete(validity_memory(memory));
                }
                if constexpr (tracks_deltas_v) {
                    if (state(memory)->updated != nullptr) {
                        ::operator delete(state(memory)->updated);
                    }
                    if (state(memory)->added != nullptr) {
                        ::operator delete(state(memory)->added);
                    }
                }
                std::destroy_at(state(memory));
            }

            void copy_construct(void *dst, const void *src) const
            {
                construct(dst);
                reserve(dst, size(src));
                this->copy_elements(data_memory(dst), data_memory(src), size(src));
                std::memcpy(validity_memory(dst), validity_memory(src), value::validity_mask_bytes(size(src)));
                if constexpr (tracks_deltas_v) {
                    if (capacity(dst) > 0) {
                        value::validity_set_all(state(dst)->updated, capacity(dst), false);
                        value::validity_set_all(state(dst)->added, capacity(dst), false);
                    }
                }
                set_size(dst, size(src));
            }

            void move_construct(void *dst, void *src) const
            {
                std::construct_at(state(dst), std::move(*state(src)));
                state(src)->data = nullptr;
                state(src)->validity = nullptr;
                state(src)->size = 0;
                state(src)->capacity = 0;
                if constexpr (tracks_deltas_v) {
                    state(src)->updated = nullptr;
                    state(src)->added = nullptr;
                    state(src)->mutation_depth = 0;
                }
            }

          private:
            [[nodiscard]] DynamicListState *state(void *data) const noexcept
            {
                return std::launder(reinterpret_cast<DynamicListState *>(data));
            }

            [[nodiscard]] const DynamicListState *state(const void *data) const noexcept
            {
                return std::launder(reinterpret_cast<const DynamicListState *>(data));
            }

            [[nodiscard]] std::byte *data_memory(void *data) const noexcept
            {
                return state(data)->data;
            }

            [[nodiscard]] const std::byte *data_memory(const void *data) const noexcept
            {
                return state(data)->data;
            }

            [[nodiscard]] std::byte *validity_memory(void *data) const noexcept
            {
                return state(data)->validity;
            }

            [[nodiscard]] const std::byte *validity_memory(const void *data) const noexcept
            {
                return state(data)->validity;
            }

            [[nodiscard]] size_t capacity(const void *data) const noexcept
            {
                return state(data)->capacity;
            }

            void set_size(void *data, size_t new_size) const noexcept
            {
                state(data)->size = new_size;
            }

            void reserve(void *data, size_t new_capacity) const
            {
                const size_t current_capacity = capacity(data);
                if (new_capacity <= current_capacity) { return; }

                std::byte *new_data = static_cast<std::byte *>(
                    ::operator new(this->element_stride() * new_capacity,
                                   std::align_val_t{this->m_element_builder.get().alignment()}));
                std::byte *new_validity = static_cast<std::byte *>(::operator new(value::validity_mask_bytes(new_capacity)));
                std::byte *new_updated = nullptr;
                std::byte *new_added = nullptr;
                if constexpr (tracks_deltas_v) {
                    new_updated = static_cast<std::byte *>(::operator new(value::validity_mask_bytes(new_capacity)));
                    new_added = static_cast<std::byte *>(::operator new(value::validity_mask_bytes(new_capacity)));
                }

                const size_t current_size = size(data);
                size_t i = 0;
                try {
                    for (; i < current_size; ++i) {
                        this->m_element_builder.get().move_construct(new_data + this->element_stride() * i,
                                                                     data_memory(data) + this->element_stride() * i,
                                                                     this->m_element_builder);
                    }
                } catch (...) {
                    this->destroy_elements(new_data, i);
                    ::operator delete(new_data, std::align_val_t{this->m_element_builder.get().alignment()});
                    ::operator delete(new_validity);
                    if (new_updated != nullptr) { ::operator delete(new_updated); }
                    if (new_added != nullptr) { ::operator delete(new_added); }
                    throw;
                }

                if (data_memory(data) != nullptr) {
                    this->destroy_elements(data_memory(data), current_size);
                    ::operator delete(data_memory(data), std::align_val_t{this->m_element_builder.get().alignment()});
                }

                const size_t old_bytes = value::validity_mask_bytes(current_size);
                if (old_bytes > 0 && validity_memory(data) != nullptr) {
                    std::memcpy(new_validity, validity_memory(data), old_bytes);
                }
                value::validity_set_range(new_validity, current_size, new_capacity - current_size, false);
                if constexpr (tracks_deltas_v) {
                    const size_t delta_bytes = value::validity_mask_bytes(current_capacity);
                    if (delta_bytes > 0 && state(data)->updated != nullptr) {
                        std::memcpy(new_updated, state(data)->updated, delta_bytes);
                    }
                    value::validity_set_range(new_updated, current_capacity, new_capacity - current_capacity, false);
                    if (delta_bytes > 0 && state(data)->added != nullptr) {
                        std::memcpy(new_added, state(data)->added, delta_bytes);
                    }
                    value::validity_set_range(new_added, current_capacity, new_capacity - current_capacity, false);
                }
                if (validity_memory(data) != nullptr) {
                    ::operator delete(validity_memory(data));
                }
                if constexpr (tracks_deltas_v) {
                    if (state(data)->updated != nullptr) {
                        ::operator delete(state(data)->updated);
                    }
                    if (state(data)->added != nullptr) {
                        ::operator delete(state(data)->added);
                    }
                }

                state(data)->data = new_data;
                state(data)->validity = new_validity;
                state(data)->capacity = new_capacity;
                if constexpr (tracks_deltas_v) {
                    state(data)->updated = new_updated;
                    state(data)->added = new_added;
                }
            }
        };

        template <typename TDispatch> struct FixedListStateOps final : StateOps
        {
            explicit FixedListStateOps(const TDispatch &dispatch) noexcept
                : m_dispatch(dispatch)
            {
            }

            void expand_builder(ValueBuilder &builder, const value::TypeMeta &schema) const noexcept override
            {
                static_cast<void>(schema);
                builder.cache_layout(m_dispatch.get().allocation_size(), m_dispatch.get().allocation_alignment());
                builder.cache_lifecycle(true, true, false);
            }

            [[nodiscard]] const ViewDispatch &view_dispatch(const value::TypeMeta &schema) const noexcept override
            {
                static_cast<void>(schema);
                return m_dispatch.get();
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

        template <typename TDispatch> struct DynamicListStateOps final : StateOps
        {
            explicit DynamicListStateOps(const TDispatch &dispatch) noexcept
                : m_dispatch(dispatch)
            {
            }

            void expand_builder(ValueBuilder &builder, const value::TypeMeta &schema) const noexcept override
            {
                static_cast<void>(schema);
                if constexpr (TDispatch::tracking_mode == MutationTracking::Delta) {
                    builder.cache_layout(sizeof(DeltaDynamicListState), alignof(DeltaDynamicListState));
                } else {
                    builder.cache_layout(sizeof(PlainDynamicListState), alignof(PlainDynamicListState));
                }
                builder.cache_lifecycle(true, true, false);
            }

            [[nodiscard]] const ViewDispatch &view_dispatch(const value::TypeMeta &schema) const noexcept override
            {
                static_cast<void>(schema);
                return m_dispatch.get();
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

        struct ListBuilderKey
        {
            const value::TypeMeta *schema{nullptr};
            MutationTracking       tracking{MutationTracking::Delta};

            [[nodiscard]] bool operator==(const ListBuilderKey &other) const noexcept
            {
                return schema == other.schema && tracking == other.tracking;
            }
        };

        struct ListBuilderKeyHash
        {
            [[nodiscard]] size_t operator()(const ListBuilderKey &key) const noexcept
            {
                return std::hash<const value::TypeMeta *>{}(key.schema) ^
                       (static_cast<size_t>(key.tracking) << 1U);
            }
        };

        const ValueBuilder *list_builder_for(const value::TypeMeta *schema, MutationTracking tracking)
        {
            if (schema == nullptr || schema->kind != value::TypeKind::List) { return nullptr; }

            static std::mutex cache_mutex;
            static std::unordered_map<ListBuilderKey, CachedBuilderEntry, ListBuilderKeyHash> cache;

            std::lock_guard lock(cache_mutex);
            const ListBuilderKey key{schema, tracking};
            if (auto it = cache.find(key); it != cache.end()) {
                return it->second.builder.get();
            }

            if (schema->element_type == nullptr) {
                throw std::runtime_error("List schema requires an element schema");
            }

            const ValueBuilder &element_builder = ValueBuilderFactory::checked_builder_for(schema->element_type, tracking);
            CachedBuilderEntry entry;

            if (schema->is_fixed_size()) {
                if (tracking == MutationTracking::Delta) {
                    auto dispatch = std::make_shared<FixedListDispatch<MutationTracking::Delta>>(*schema, element_builder);
                    auto state_ops =
                        std::make_shared<FixedListStateOps<FixedListDispatch<MutationTracking::Delta>>>(*dispatch);
                    auto builder = std::make_shared<ValueBuilder>(*schema, tracking, *state_ops);
                    entry.dispatch = std::move(dispatch);
                    entry.state_ops = std::move(state_ops);
                    entry.builder = std::move(builder);
                } else {
                    auto dispatch = std::make_shared<FixedListDispatch<MutationTracking::Plain>>(*schema, element_builder);
                    auto state_ops =
                        std::make_shared<FixedListStateOps<FixedListDispatch<MutationTracking::Plain>>>(*dispatch);
                    auto builder = std::make_shared<ValueBuilder>(*schema, tracking, *state_ops);
                    entry.dispatch = std::move(dispatch);
                    entry.state_ops = std::move(state_ops);
                    entry.builder = std::move(builder);
                }
            } else {
                if (tracking == MutationTracking::Delta) {
                    auto dispatch =
                        std::make_shared<DynamicListDispatch<MutationTracking::Delta>>(*schema, element_builder);
                    auto state_ops =
                        std::make_shared<DynamicListStateOps<DynamicListDispatch<MutationTracking::Delta>>>(*dispatch);
                    auto builder = std::make_shared<ValueBuilder>(*schema, tracking, *state_ops);
                    entry.dispatch = std::move(dispatch);
                    entry.state_ops = std::move(state_ops);
                    entry.builder = std::move(builder);
                } else {
                    auto dispatch =
                        std::make_shared<DynamicListDispatch<MutationTracking::Plain>>(*schema, element_builder);
                    auto state_ops =
                        std::make_shared<DynamicListStateOps<DynamicListDispatch<MutationTracking::Plain>>>(*dispatch);
                    auto builder = std::make_shared<ValueBuilder>(*schema, tracking, *state_ops);
                    entry.dispatch = std::move(dispatch);
                    entry.state_ops = std::move(state_ops);
                    entry.builder = std::move(builder);
                }
            }

            auto [it, inserted] = cache.emplace(key, std::move(entry));
            static_cast<void>(inserted);
            return it->second.builder.get();
        }

    }  // namespace detail

    ListView::ListView(const View &view)
        : View(view)
    {
        if (!view.valid()) { return; }
        if (view.schema() == nullptr || view.schema()->kind != value::TypeKind::List) {
            throw std::runtime_error("ListView requires a list schema");
        }
    }

    ListMutationView ListView::begin_mutation()
    {
        return ListMutationView{*this};
    }

    ListDeltaView ListView::delta()
    {
        return ListDeltaView{*this};
    }

    ListDeltaView ListView::delta() const
    {
        return ListDeltaView{*this};
    }

    void ListView::begin_mutation_scope()
    {
        const auto *dispatch = list_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("ListView::begin_mutation on invalid view"); }
        dispatch->begin_mutation(data());
    }

    void ListView::end_mutation_scope() noexcept
    {
        const auto *dispatch = list_dispatch();
        if (dispatch == nullptr) { return; }
        dispatch->end_mutation(data());
    }

    size_t ListView::size() const
    {
        const auto *dispatch = list_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("ListView::size on invalid view"); }
        return dispatch->size(data());
    }

    bool ListView::empty() const
    {
        return size() == 0;
    }

    bool ListView::is_fixed() const
    {
        const auto *dispatch = list_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("ListView::is_fixed on invalid view"); }
        return dispatch->is_fixed();
    }

    const value::TypeMeta *ListView::element_schema() const
    {
        const auto *dispatch = list_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("ListView::element_schema on invalid view"); }
        return &dispatch->element_schema();
    }

    View ListView::at(size_t index)
    {
        const auto *dispatch = list_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("ListView::at on invalid view"); }
        if (index >= dispatch->size(data())) { throw std::out_of_range("ListView::at index out of range"); }

        if (!dispatch->element_valid(data(), index)) {
            return View::invalid_for(&dispatch->element_schema());
        }
        return View{&dispatch->element_dispatch(), dispatch->element_data(data(), index), &dispatch->element_schema()};
    }

    View ListView::at(size_t index) const
    {
        const auto *dispatch = list_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("ListView::at on invalid view"); }
        if (index >= dispatch->size(data())) { throw std::out_of_range("ListView::at index out of range"); }

        if (!dispatch->element_valid(data(), index)) {
            return View::invalid_for(&dispatch->element_schema());
        }
        return View{&dispatch->element_dispatch(), const_cast<void *>(dispatch->element_data(data(), index)), &dispatch->element_schema()};
    }

    View ListView::operator[](size_t index)
    {
        return at(index);
    }

    View ListView::operator[](size_t index) const
    {
        return at(index);
    }

    View ListView::front()
    {
        return at(0);
    }

    View ListView::front() const
    {
        return at(0);
    }

    View ListView::back()
    {
        return at(size() - 1);
    }

    View ListView::back() const
    {
        return at(size() - 1);
    }

    ListDeltaView::ListDeltaView(const View &view)
        : View(view)
    {
        if (!view.valid()) { return; }
        if (view.schema() == nullptr || view.schema()->kind != value::TypeKind::List) {
            throw std::runtime_error("ListDeltaView requires a list schema");
        }
    }

    Range<size_t> ListDeltaView::updated_indices() const
    {
        const auto *dispatch = list_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("ListDeltaView::updated_indices on invalid view"); }
        return Range<size_t>{this, dispatch->size(data()), &ListDeltaView::slot_is_updated, &ListDeltaView::project_index};
    }

    Range<View> ListDeltaView::updated_values() const
    {
        const auto *dispatch = list_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("ListDeltaView::updated_values on invalid view"); }
        return Range<View>{this, dispatch->size(data()), &ListDeltaView::slot_is_updated, &ListDeltaView::project_value};
    }

    Range<size_t> ListDeltaView::added_indices() const
    {
        const auto *dispatch = list_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("ListDeltaView::added_indices on invalid view"); }
        return Range<size_t>{this, dispatch->size(data()), &ListDeltaView::slot_is_added, &ListDeltaView::project_index};
    }

    Range<View> ListDeltaView::added_values() const
    {
        const auto *dispatch = list_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("ListDeltaView::added_values on invalid view"); }
        return Range<View>{this, dispatch->size(data()), &ListDeltaView::slot_is_added, &ListDeltaView::project_value};
    }

    bool ListDeltaView::slot_is_updated(const void *context, size_t index)
    {
        const auto &delta = *static_cast<const ListDeltaView *>(context);
        const auto *dispatch = delta.list_dispatch();
        return dispatch != nullptr && dispatch->slot_updated(delta.data(), index);
    }

    bool ListDeltaView::slot_is_added(const void *context, size_t index)
    {
        const auto &delta = *static_cast<const ListDeltaView *>(context);
        const auto *dispatch = delta.list_dispatch();
        return dispatch != nullptr && dispatch->slot_added(delta.data(), index);
    }

    size_t ListDeltaView::project_index(const void *context, size_t index)
    {
        static_cast<void>(context);
        return index;
    }

    View ListDeltaView::project_value(const void *context, size_t index)
    {
        return static_cast<const ListDeltaView *>(context)->as_list().at(index);
    }

    const detail::ListViewDispatch *ListDeltaView::list_dispatch() const noexcept
    {
        return valid() ? static_cast<const detail::ListViewDispatch *>(dispatch()) : nullptr;
    }

    ListMutationView::ListMutationView(ListView &view)
        : ListView(view)
    {
        begin_mutation_scope();
    }

    ListMutationView::ListMutationView(ListMutationView &&other) noexcept
        : ListView(other), m_owns_scope(other.m_owns_scope)
    {
        other.m_owns_scope = false;
    }

    ListMutationView::~ListMutationView()
    {
        if (m_owns_scope) {
            end_mutation_scope();
        }
    }

    void ListMutationView::set(size_t index, const View &value)
    {
        const auto *dispatch = list_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("ListMutationView::set on invalid view"); }
        if (index >= dispatch->size(data())) { throw std::out_of_range("ListMutationView::set index out of range"); }
        if (value.schema() != nullptr && value.schema() != &dispatch->element_schema()) {
            throw std::invalid_argument("ListMutationView::set requires matching element schema");
        }

        if (!value.valid()) {
            dispatch->set_element_valid(data(), index, false);
            return;
        }

        dispatch->element_dispatch().assign(dispatch->element_data(data(), index), data_of(value));
        dispatch->set_element_valid(data(), index, true);
    }

    void ListMutationView::resize(size_t new_size)
    {
        const auto *dispatch = list_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("ListMutationView::resize on invalid view"); }
        dispatch->resize(data(), new_size);
    }

    void ListMutationView::clear()
    {
        const auto *dispatch = list_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("ListMutationView::clear on invalid view"); }
        dispatch->clear(data());
    }

    void ListMutationView::push_back(const View &value)
    {
        const size_t index = size();
        resize(index + 1);
        set(index, value);
    }

    const detail::ListViewDispatch *ListView::list_dispatch() const noexcept
    {
        return valid() ? static_cast<const detail::ListViewDispatch *>(dispatch()) : nullptr;
    }

}  // namespace hgraph
