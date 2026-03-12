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

        struct FixedListDispatch final : ListDispatchBase
        {
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

            [[nodiscard]] void *element_data(void *data, size_t index) const override
            {
                if (index >= m_fixed_size) { throw std::out_of_range("Fixed list index out of range"); }
                return static_cast<std::byte *>(data) + element_stride() * index;
            }

            [[nodiscard]] const void *element_data(const void *data, size_t index) const override
            {
                if (index >= m_fixed_size) { throw std::out_of_range("Fixed list index out of range"); }
                return static_cast<const std::byte *>(data) + element_stride() * index;
            }

            [[nodiscard]] bool element_valid(const void *data, size_t index) const override
            {
                if (index >= m_fixed_size) { throw std::out_of_range("Fixed list index out of range"); }
                return value::validity_bit_get(validity_memory(data), index);
            }

            void set_element_valid(void *data, size_t index, bool valid) const override
            {
                if (index >= m_fixed_size) { throw std::out_of_range("Fixed list index out of range"); }
                if (!valid && value::validity_bit_get(validity_memory(data), index)) {
                    invalidate_element(static_cast<std::byte *>(data), index);
                }
                value::validity_bit_set(validity_memory(data), index, valid);
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
                return element_stride() * m_fixed_size + value::validity_mask_bytes(m_fixed_size);
            }

            void construct(void *memory) const
            {
                construct_elements(static_cast<std::byte *>(memory), m_fixed_size);
                value::validity_set_all(validity_memory(memory), m_fixed_size, true);
            }

            void destroy(void *memory) const noexcept
            {
                destroy_elements(static_cast<std::byte *>(memory), m_fixed_size);
            }

            void copy_construct(void *dst, const void *src) const
            {
                copy_elements(static_cast<std::byte *>(dst), static_cast<const std::byte *>(src), m_fixed_size);
                std::memcpy(validity_memory(dst), validity_memory(src), value::validity_mask_bytes(m_fixed_size));
            }

            void move_construct(void *dst, void *src) const
            {
                move_elements(static_cast<std::byte *>(dst), static_cast<std::byte *>(src), m_fixed_size);
                std::memcpy(validity_memory(dst), validity_memory(src), value::validity_mask_bytes(m_fixed_size));
            }

          private:
            [[nodiscard]] std::byte *validity_memory(void *data) const noexcept
            {
                return static_cast<std::byte *>(data) + element_stride() * m_fixed_size;
            }

            [[nodiscard]] const std::byte *validity_memory(const void *data) const noexcept
            {
                return static_cast<const std::byte *>(data) + element_stride() * m_fixed_size;
            }

            size_t m_fixed_size{0};
        };

        struct DynamicListDispatch final : ListDispatchBase
        {
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

            [[nodiscard]] void *element_data(void *data, size_t index) const override
            {
                if (index >= state(data)->size) { throw std::out_of_range("Dynamic list index out of range"); }
                return state(data)->data + element_stride() * index;
            }

            [[nodiscard]] const void *element_data(const void *data, size_t index) const override
            {
                if (index >= state(data)->size) { throw std::out_of_range("Dynamic list index out of range"); }
                return state(data)->data + element_stride() * index;
            }

            [[nodiscard]] bool element_valid(const void *data, size_t index) const override
            {
                if (index >= state(data)->size) { throw std::out_of_range("Dynamic list index out of range"); }
                return value::validity_bit_get(state(data)->validity, index);
            }

            void set_element_valid(void *data, size_t index, bool valid) const override
            {
                if (index >= state(data)->size) { throw std::out_of_range("Dynamic list index out of range"); }
                if (!valid && value::validity_bit_get(state(data)->validity, index)) {
                    invalidate_element(state(data)->data, index);
                }
                value::validity_bit_set(state(data)->validity, index, valid);
            }

            void resize(void *data, size_t new_size) const override
            {
                DynamicListState *list = state(data);
                if (new_size > list->capacity) {
                    reserve(list, new_size);
                }
                if (new_size > list->size) {
                    construct_elements(list->data + element_stride() * list->size, new_size - list->size);
                    value::validity_set_range(list->validity, list->size, new_size - list->size, true);
                } else if (new_size < list->size) {
                    destroy_elements(list->data + element_stride() * new_size, list->size - new_size);
                }
                list->size = new_size;
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
                DynamicListState *list = state(memory);
                destroy_elements(list->data, list->size);
                if (list->data != nullptr) {
                    ::operator delete(list->data, std::align_val_t{m_element_builder.get().alignment()});
                }
                if (list->validity != nullptr) {
                    ::operator delete(list->validity);
                }
                std::destroy_at(list);
            }

            void copy_construct(void *dst, const void *src) const
            {
                std::construct_at(state(dst));
                DynamicListState *out = state(dst);
                const DynamicListState *in = state(src);
                reserve(out, in->size);
                copy_elements(out->data, in->data, in->size);
                std::memcpy(out->validity, in->validity, value::validity_mask_bytes(in->size));
                out->size = in->size;
            }

            void move_construct(void *dst, void *src) const
            {
                std::construct_at(state(dst), std::move(*state(src)));
                state(src)->data = nullptr;
                state(src)->validity = nullptr;
                state(src)->size = 0;
                state(src)->capacity = 0;
            }

          private:
            static DynamicListState *state(void *data) noexcept
            {
                return std::launder(reinterpret_cast<DynamicListState *>(data));
            }

            static const DynamicListState *state(const void *data) noexcept
            {
                return std::launder(reinterpret_cast<const DynamicListState *>(data));
            }

            void reserve(DynamicListState *list, size_t new_capacity) const
            {
                if (new_capacity <= list->capacity) { return; }

                std::byte *new_data = static_cast<std::byte *>(
                    ::operator new(element_stride() * new_capacity, std::align_val_t{m_element_builder.get().alignment()}));
                std::byte *new_validity = static_cast<std::byte *>(::operator new(value::validity_mask_bytes(new_capacity)));

                size_t i = 0;
                try {
                    for (; i < list->size; ++i) {
                        m_element_builder.get().move_construct(new_data + element_stride() * i,
                                                               list->data + element_stride() * i,
                                                               m_element_builder);
                    }
                } catch (...) {
                    destroy_elements(new_data, i);
                    ::operator delete(new_data, std::align_val_t{m_element_builder.get().alignment()});
                    ::operator delete(new_validity);
                    throw;
                }

                if (list->data != nullptr) {
                    destroy_elements(list->data, list->size);
                    ::operator delete(list->data, std::align_val_t{m_element_builder.get().alignment()});
                }

                const size_t old_bytes = value::validity_mask_bytes(list->size);
                if (old_bytes > 0 && list->validity != nullptr) {
                    std::memcpy(new_validity, list->validity, old_bytes);
                }
                value::validity_set_range(new_validity, list->size, new_capacity - list->size, false);
                if (list->validity != nullptr) {
                    ::operator delete(list->validity);
                }

                list->data = new_data;
                list->validity = new_validity;
                list->capacity = new_capacity;
            }
        };

        struct FixedListStateOps final : StateOps
        {
            explicit FixedListStateOps(const FixedListDispatch &dispatch) noexcept
                : m_dispatch(dispatch)
            {
            }

            void expand_builder(ValueBuilder &builder, const value::TypeMeta &schema) const noexcept override
            {
                static_cast<void>(schema);
                builder.cache_layout(m_dispatch.get().allocation_size(), m_dispatch.get().element_builder().alignment());
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

            std::reference_wrapper<const FixedListDispatch> m_dispatch;
        };

        struct DynamicListStateOps final : StateOps
        {
            explicit DynamicListStateOps(const DynamicListDispatch &dispatch) noexcept
                : m_dispatch(dispatch)
            {
            }

            void expand_builder(ValueBuilder &builder, const value::TypeMeta &schema) const noexcept override
            {
                static_cast<void>(schema);
                builder.cache_layout(sizeof(DynamicListState), alignof(DynamicListState));
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

            std::reference_wrapper<const DynamicListDispatch> m_dispatch;
        };

        struct CachedBuilderEntry
        {
            std::shared_ptr<const ViewDispatch> dispatch;
            std::shared_ptr<const StateOps>     state_ops;
            std::shared_ptr<const ValueBuilder> builder;
        };

        const ValueBuilder *list_builder_for(const value::TypeMeta *schema)
        {
            if (schema == nullptr || schema->kind != value::TypeKind::List) { return nullptr; }

            static std::mutex cache_mutex;
            static std::unordered_map<const value::TypeMeta *, CachedBuilderEntry> cache;

            std::lock_guard lock(cache_mutex);
            if (auto it = cache.find(schema); it != cache.end()) {
                return it->second.builder.get();
            }

            if (schema->element_type == nullptr) {
                throw std::runtime_error("List schema requires an element schema");
            }

            const ValueBuilder &element_builder = ValueBuilderFactory::checked_builder_for(schema->element_type);
            CachedBuilderEntry entry;

            if (schema->is_fixed_size()) {
                auto dispatch = std::make_shared<FixedListDispatch>(*schema, element_builder);
                auto state_ops = std::make_shared<FixedListStateOps>(*dispatch);
                auto builder = std::make_shared<ValueBuilder>(*schema, *state_ops);
                entry.dispatch = std::move(dispatch);
                entry.state_ops = std::move(state_ops);
                entry.builder = std::move(builder);
            } else {
                auto dispatch = std::make_shared<DynamicListDispatch>(*schema, element_builder);
                auto state_ops = std::make_shared<DynamicListStateOps>(*dispatch);
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

    ListView::ListView(const View &view)
        : View(view)
    {
        if (!view.valid()) { return; }
        if (view.schema() == nullptr || view.schema()->kind != value::TypeKind::List) {
            throw std::runtime_error("ListView requires a list schema");
        }
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

    void ListView::set(size_t index, const View &value)
    {
        const auto *dispatch = list_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("ListView::set on invalid view"); }
        if (index >= dispatch->size(data())) { throw std::out_of_range("ListView::set index out of range"); }
        if (value.schema() != nullptr && value.schema() != &dispatch->element_schema()) {
            throw std::invalid_argument("ListView::set requires matching element schema");
        }

        if (!value.valid()) {
            dispatch->set_element_valid(data(), index, false);
            return;
        }

        dispatch->element_dispatch().assign(dispatch->element_data(data(), index), data_of(value));
        dispatch->set_element_valid(data(), index, true);
    }

    void ListView::resize(size_t new_size)
    {
        const auto *dispatch = list_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("ListView::resize on invalid view"); }
        dispatch->resize(data(), new_size);
    }

    void ListView::clear()
    {
        const auto *dispatch = list_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("ListView::clear on invalid view"); }
        dispatch->clear(data());
    }

    void ListView::push_back(const View &value)
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
