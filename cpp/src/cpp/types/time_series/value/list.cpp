#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/value/list.h>
#include <hgraph/types/time_series/value/state.h>

#include <algorithm>
#include <compare>
#include <cstring>
#include <stdexcept>
#include <string>
#include <utility>

namespace hgraph
{
    namespace
    {
        [[nodiscard]] const ValueBuilder &element_builder_for(const value::TypeMeta &schema)
        {
            return ValueBuilderFactory::checked_builder_for(&schema);
        }
    }  // namespace

    ListStateBase::ListStateBase(const value::TypeMeta &element_schema) noexcept
        : m_element_schema(element_schema)
    {
    }

    ListStateBase::ListStateBase(const ListStateBase &other) noexcept
        : m_element_schema(other.m_element_schema)
    {
    }

    ListStateBase::ListStateBase(ListStateBase &&other) noexcept
        : m_element_schema(other.m_element_schema)
    {
    }

    ListStateBase &ListStateBase::operator=(const ListStateBase &other) noexcept
    {
        if (this != &other) {
            m_element_schema = other.m_element_schema;
        }
        return *this;
    }

    ListStateBase &ListStateBase::operator=(ListStateBase &&other) noexcept
    {
        if (this != &other) {
            m_element_schema = other.m_element_schema;
        }
        return *this;
    }

    const value::TypeMeta &ListStateBase::element_schema() const noexcept
    {
        return m_element_schema.get();
    }

    size_t ListStateBase::hash() const
    {
        size_t result                    = 0;
        constexpr size_t null_hash_seed = 0x9e3779b97f4a7c15ULL;
        for (size_t i = 0; i < size(); ++i) {
            size_t element_hash = null_hash_seed + i;
            if (element_valid(i)) {
                element_hash = element_dispatch(i)->hash();
            }
            result ^= element_hash + 0x9e3779b9 + (result << 6) + (result >> 2);
        }
        return result;
    }

    std::string ListStateBase::to_string() const
    {
        std::string result = "[";
        for (size_t i = 0; i < size(); ++i) {
            if (i > 0) {
                result += ", ";
            }
            if (!element_valid(i)) {
                result += "None";
            } else {
                result += element_dispatch(i)->to_string();
            }
        }
        result += "]";
        return result;
    }

    std::partial_ordering ListStateBase::operator<=>(const detail::ViewDispatch &other) const
    {
        const auto &rhs  = static_cast<const detail::ListViewDispatch &>(other);
        const size_t lhs_size = size();
        const size_t rhs_size = rhs.size();
        const size_t count    = std::min(lhs_size, rhs_size);

        for (size_t i = 0; i < count; ++i) {
            const bool lhs_valid = element_valid(i);
            const bool rhs_valid = rhs.element_valid(i);
            if (!lhs_valid || !rhs_valid) {
                if (lhs_valid != rhs_valid) {
                    return std::partial_ordering::unordered;
                }
                continue;
            }

            const std::partial_ordering element_order = *element_dispatch(i) <=> *rhs.element_dispatch(i);
            if (std::is_lt(element_order) || std::is_gt(element_order) ||
                element_order == std::partial_ordering::unordered) {
                return element_order;
            }
        }

        if (lhs_size < rhs_size) {
            return std::partial_ordering::less;
        }
        if (lhs_size > rhs_size) {
            return std::partial_ordering::greater;
        }
        return std::partial_ordering::equivalent;
    }

    nb::object ListStateBase::to_python(const value::TypeMeta *schema) const
    {
        nb::list result;
        for (size_t i = 0; i < size(); ++i) {
            if (!element_valid(i)) {
                result.append(nb::none());
            } else {
                result.append(element_dispatch(i)->to_python(&element_schema()));
            }
        }
        if (schema != nullptr && schema->is_variadic_tuple()) {
            return nb::tuple(result);
        }
        return nb::object(result);
    }

    void ListStateBase::from_python(const nb::object &src, const value::TypeMeta *schema)
    {
        if (!nb::isinstance<nb::list>(src) && !nb::isinstance<nb::tuple>(src)) {
            throw std::runtime_error("ListStateBase::from_python expects a list or tuple");
        }

        static_cast<void>(schema);
        const nb::sequence sequence = nb::cast<nb::sequence>(src);
        const size_t       count    = nb::len(sequence);

        if (is_fixed()) {
            const size_t copy_count = std::min(count, size());
            for (size_t i = 0; i < copy_count; ++i) {
                nb::object element = sequence[i];
                if (element.is_none()) {
                    set_element_valid(i, false);
                } else {
                    View slot{element_dispatch(i), &element_schema()};
                    slot.from_python(element);
                    set_element_valid(i, true);
                }
            }
            return;
        }

        resize(count, element_builder_for(element_schema()));
        for (size_t i = 0; i < count; ++i) {
            nb::object element = sequence[i];
            if (element.is_none()) {
                set_element_valid(i, false);
            } else {
                View slot{element_dispatch(i), &element_schema()};
                slot.from_python(element);
                set_element_valid(i, true);
            }
        }
    }

    void ListStateBase::assign_from(const detail::ViewDispatch &other)
    {
        const auto &rhs = static_cast<const detail::ListViewDispatch &>(other);
        if (size() != rhs.size()) {
            if (is_fixed()) {
                throw std::runtime_error("ListStateBase::assign_from requires matching fixed-list size");
            }
            resize(rhs.size(), element_builder_for(element_schema()));
        }

        for (size_t i = 0; i < rhs.size(); ++i) {
            if (!rhs.element_valid(i)) {
                set_element_valid(i, false);
                continue;
            }
            element_dispatch(i)->assign_from(*rhs.element_dispatch(i));
            set_element_valid(i, true);
        }
    }

    void ListStateBase::set_from_cpp(const void *src, const value::TypeMeta *src_schema)
    {
        static_cast<void>(src);
        static_cast<void>(src_schema);
        throw std::invalid_argument("ListStateBase::set_from_cpp is not implemented for list schemas");
    }

    void ListStateBase::move_from_cpp(void *src, const value::TypeMeta *src_schema)
    {
        static_cast<void>(src);
        static_cast<void>(src_schema);
        throw std::invalid_argument("ListStateBase::move_from_cpp is not implemented for list schemas");
    }

    View ListStateBase::element_view(size_t index) noexcept
    {
        return View{element_valid(index) ? element_dispatch(index) : nullptr, &element_schema()};
    }

    View ListStateBase::element_view(size_t index) const noexcept
    {
        return View{element_valid(index) ? const_cast<detail::ViewDispatch *>(element_dispatch(index)) : nullptr, &element_schema()};
    }

    void ListStateBase::assign_element(size_t index, const View &value)
    {
        if (value.schema() != nullptr && value.schema() != &element_schema()) {
            throw std::invalid_argument("ListStateBase::assign_element requires matching element schema");
        }
        if (!value.valid()) {
            set_element_valid(index, false);
            return;
        }

        element_dispatch(index)->assign_from(*value.m_dispatch);
        set_element_valid(index, true);
    }

    FixedListState::FixedListState(const value::TypeMeta &element_schema, size_t fixed_size) noexcept
        : ListStateBase(element_schema), m_fixed_size(fixed_size)
    {
    }

    FixedListState::~FixedListState()
    {
    }

    size_t FixedListState::size() const noexcept
    {
        return m_fixed_size;
    }

    bool FixedListState::is_fixed() const noexcept
    {
        return true;
    }

    bool FixedListState::element_valid(size_t index) const
    {
        if (index >= m_fixed_size) {
            throw std::out_of_range("FixedListState::element_valid index out of range");
        }
        return value::validity_bit_get(validity_memory(), index);
    }

    detail::ViewDispatch *FixedListState::element_dispatch(size_t index)
    {
        if (index >= m_fixed_size) {
            throw std::out_of_range("FixedListState::element_dispatch index out of range");
        }
        return std::launder(reinterpret_cast<detail::ViewDispatch *>(element_memory(index)));
    }

    const detail::ViewDispatch *FixedListState::element_dispatch(size_t index) const
    {
        if (index >= m_fixed_size) {
            throw std::out_of_range("FixedListState::element_dispatch index out of range");
        }
        return std::launder(reinterpret_cast<const detail::ViewDispatch *>(element_memory(index)));
    }

    void FixedListState::set_element_valid(size_t index, bool valid)
    {
        if (index >= m_fixed_size) {
            throw std::out_of_range("FixedListState::set_element_valid index out of range");
        }
        value::validity_bit_set(validity_memory(), index, valid);
    }

    void FixedListState::resize(size_t new_size, const ValueBuilder &element_builder)
    {
        static_cast<void>(element_builder);
        if (new_size != m_fixed_size) {
            throw std::runtime_error("FixedListState::resize cannot change fixed-size list length");
        }
    }

    void FixedListState::clear(const ValueBuilder &element_builder)
    {
        static_cast<void>(element_builder);
        throw std::runtime_error("FixedListState::clear is not supported on fixed-size lists");
    }

    size_t FixedListState::allocation_size(const ValueBuilder &element_builder, size_t fixed_size) noexcept
    {
        const size_t element_alignment = element_builder.alignment();
        const size_t stride            = ((element_builder.size() + element_alignment - 1) / element_alignment) * element_alignment;
        const size_t elements_offset   = ((sizeof(FixedListState) + element_alignment - 1) / element_alignment) * element_alignment;
        const size_t total_size        = elements_offset + stride * fixed_size + value::validity_mask_bytes(fixed_size);
        const size_t alignment         = allocation_alignment(element_builder);
        return ((total_size + alignment - 1) / alignment) * alignment;
    }

    size_t FixedListState::allocation_alignment(const ValueBuilder &element_builder) noexcept
    {
        return std::max(alignof(FixedListState), element_builder.alignment());
    }

    size_t FixedListState::element_stride() const noexcept
    {
        const ValueBuilder &element_builder = element_builder_for(element_schema());
        const size_t        alignment       = element_builder.alignment();
        return ((element_builder.size() + alignment - 1) / alignment) * alignment;
    }

    size_t FixedListState::elements_offset() const noexcept
    {
        const size_t alignment = element_builder_for(element_schema()).alignment();
        return ((sizeof(FixedListState) + alignment - 1) / alignment) * alignment;
    }

    size_t FixedListState::validity_offset() const noexcept
    {
        return elements_offset() + element_stride() * m_fixed_size;
    }

    std::byte *FixedListState::element_memory(size_t index) noexcept
    {
        return reinterpret_cast<std::byte *>(this) + elements_offset() + element_stride() * index;
    }

    const std::byte *FixedListState::element_memory(size_t index) const noexcept
    {
        return reinterpret_cast<const std::byte *>(this) + elements_offset() + element_stride() * index;
    }

    std::byte *FixedListState::validity_memory() noexcept
    {
        return reinterpret_cast<std::byte *>(this) + validity_offset();
    }

    const std::byte *FixedListState::validity_memory() const noexcept
    {
        return reinterpret_cast<const std::byte *>(this) + validity_offset();
    }

    void FixedListState::construct_elements(const ValueBuilder &element_builder)
    {
        for (size_t i = 0; i < m_fixed_size; ++i) {
            element_builder.construct(element_memory(i));
        }
        value::validity_set_all(validity_memory(), m_fixed_size, true);
    }

    void FixedListState::copy_from(const FixedListState &other, const ValueBuilder &element_builder)
    {
        for (size_t i = 0; i < m_fixed_size; ++i) {
            element_builder.copy_construct(element_memory(i), other.element_memory(i), element_builder);
        }
        const size_t bytes = value::validity_mask_bytes(m_fixed_size);
        if (bytes > 0) {
            std::memcpy(validity_memory(), other.validity_memory(), bytes);
        }
    }

    void FixedListState::move_from(FixedListState &other, const ValueBuilder &element_builder)
    {
        for (size_t i = 0; i < m_fixed_size; ++i) {
            element_builder.move_construct(element_memory(i), other.element_memory(i), element_builder);
        }
        const size_t bytes = value::validity_mask_bytes(m_fixed_size);
        if (bytes > 0) {
            std::memcpy(validity_memory(), other.validity_memory(), bytes);
        }
    }

    void FixedListState::destroy_elements(const ValueBuilder &element_builder) noexcept
    {
        for (size_t i = 0; i < m_fixed_size; ++i) {
            element_builder.destroy(element_memory(i));
        }
    }

    DynamicListState::DynamicListState(const value::TypeMeta &element_schema) noexcept
        : ListStateBase(element_schema)
    {
    }

    DynamicListState::~DynamicListState()
    {
    }

    size_t DynamicListState::size() const noexcept
    {
        return m_size;
    }

    bool DynamicListState::is_fixed() const noexcept
    {
        return false;
    }

    bool DynamicListState::element_valid(size_t index) const
    {
        if (index >= m_size) {
            throw std::out_of_range("DynamicListState::element_valid index out of range");
        }
        return value::validity_bit_get(m_validity.data(), index);
    }

    detail::ViewDispatch *DynamicListState::element_dispatch(size_t index)
    {
        if (index >= m_size) {
            throw std::out_of_range("DynamicListState::element_dispatch index out of range");
        }
        return std::launder(reinterpret_cast<detail::ViewDispatch *>(element_memory(index)));
    }

    const detail::ViewDispatch *DynamicListState::element_dispatch(size_t index) const
    {
        if (index >= m_size) {
            throw std::out_of_range("DynamicListState::element_dispatch index out of range");
        }
        return std::launder(reinterpret_cast<const detail::ViewDispatch *>(element_memory(index)));
    }

    void DynamicListState::set_element_valid(size_t index, bool valid)
    {
        if (index >= m_size) {
            throw std::out_of_range("DynamicListState::set_element_valid index out of range");
        }
        value::validity_bit_set(m_validity.data(), index, valid);
    }

    void DynamicListState::resize(size_t new_size, const ValueBuilder &element_builder)
    {
        if (new_size == m_size) {
            return;
        }

        if (new_size < m_size) {
            destroy_range(new_size, m_size, element_builder);
            m_size = new_size;
            m_validity.resize(value::validity_mask_bytes(new_size));
            if (!m_validity.empty()) {
                value::validity_clear_unused_trailing_bits(m_validity.data(), new_size);
            }
            return;
        }

        reserve(new_size, element_builder);
        for (size_t i = m_size; i < new_size; ++i) {
            element_builder.construct(element_memory(i));
        }

        const size_t old_size = m_size;
        m_size                = new_size;
        m_validity.resize(value::validity_mask_bytes(new_size), std::byte{0});
        if (new_size > old_size) {
            value::validity_set_range(m_validity.data(), old_size, new_size - old_size, true);
        }
        if (!m_validity.empty()) {
            value::validity_clear_unused_trailing_bits(m_validity.data(), new_size);
        }
    }

    void DynamicListState::clear(const ValueBuilder &element_builder)
    {
        destroy_range(0, m_size, element_builder);
        m_size = 0;
        m_validity.clear();
    }

    size_t DynamicListState::element_stride() const noexcept
    {
        const ValueBuilder &element_builder = element_builder_for(element_schema());
        const size_t        alignment       = element_builder.alignment();
        return ((element_builder.size() + alignment - 1) / alignment) * alignment;
    }

    std::byte *DynamicListState::element_memory(size_t index) noexcept
    {
        return m_data.data() + element_stride() * index;
    }

    const std::byte *DynamicListState::element_memory(size_t index) const noexcept
    {
        return m_data.data() + element_stride() * index;
    }

    void DynamicListState::reserve(size_t new_capacity, const ValueBuilder &element_builder)
    {
        if (new_capacity <= m_capacity) {
            return;
        }

        const size_t target_capacity = std::max(new_capacity, std::max<size_t>(m_capacity * 2, 1));
        const size_t stride          = element_stride();
        std::vector<std::byte> new_data(target_capacity * stride);

        for (size_t i = 0; i < m_size; ++i) {
            element_builder.move_construct(new_data.data() + i * stride, element_memory(i), element_builder);
            element_builder.destroy(element_memory(i));
        }

        m_data     = std::move(new_data);
        m_capacity = target_capacity;
    }

    void DynamicListState::destroy_range(size_t begin, size_t end, const ValueBuilder &element_builder) noexcept
    {
        for (size_t i = begin; i < end; ++i) {
            element_builder.destroy(element_memory(i));
        }
    }

    void DynamicListState::copy_from(const DynamicListState &other, const ValueBuilder &element_builder)
    {
        reserve(other.m_size, element_builder);
        for (size_t i = 0; i < other.m_size; ++i) {
            element_builder.copy_construct(element_memory(i), other.element_memory(i), element_builder);
        }
        m_size     = other.m_size;
        m_validity = other.m_validity;
    }

    void DynamicListState::move_from(DynamicListState &other) noexcept
    {
        m_data           = std::move(other.m_data);
        m_validity       = std::move(other.m_validity);
        m_size           = other.m_size;
        m_capacity       = other.m_capacity;
        other.m_size     = 0;
        other.m_capacity = 0;
    }

    ListView::ListView(const View &view)
        : View(view)
    {
        if (!view.valid()) {
            return;
        }

        const value::TypeMeta *schema = view.schema();
        if (schema == nullptr || schema->kind != value::TypeKind::List) {
            throw std::runtime_error("ListView requires a list schema");
        }
    }

    ListView::ListView(detail::ListViewDispatch &state, const value::TypeMeta *schema) noexcept
        : View(&state, schema)
    {
    }

    size_t ListView::size() const
    {
        if (!valid()) {
            throw std::runtime_error("ListView::size() on invalid view");
        }
        return list_dispatch()->size();
    }

    bool ListView::empty() const
    {
        return size() == 0;
    }

    bool ListView::is_fixed() const
    {
        if (!valid()) {
            throw std::runtime_error("ListView::is_fixed() on invalid view");
        }
        return list_dispatch()->is_fixed();
    }

    const value::TypeMeta *ListView::element_schema() const
    {
        if (!valid()) {
            throw std::runtime_error("ListView::element_schema() on invalid view");
        }
        return &list_dispatch()->element_schema();
    }

    View ListView::at(size_t index)
    {
        if (!valid()) {
            throw std::runtime_error("ListView::at() on invalid view");
        }
        if (index >= size()) {
            throw std::out_of_range("ListView::at index out of range");
        }
        return View{
            list_dispatch()->element_valid(index) ? list_dispatch()->element_dispatch(index) : nullptr,
            element_schema(),
        };
    }

    View ListView::at(size_t index) const
    {
        if (!valid()) {
            throw std::runtime_error("ListView::at() on invalid view");
        }
        if (index >= size()) {
            throw std::out_of_range("ListView::at index out of range");
        }
        return View{
            list_dispatch()->element_valid(index) ? const_cast<detail::ViewDispatch *>(list_dispatch()->element_dispatch(index)) : nullptr,
            element_schema(),
        };
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
        if (!valid()) {
            throw std::runtime_error("ListView::set() on invalid view");
        }
        if (index >= size()) {
            throw std::out_of_range("ListView::set index out of range");
        }
        if (value.schema() != nullptr && value.schema() != element_schema()) {
            throw std::invalid_argument("ListView::set requires matching element schema");
        }
        if (!value.valid()) {
            list_dispatch()->set_element_valid(index, false);
            return;
        }

        list_dispatch()->element_dispatch(index)->assign_from(*value.m_dispatch);
        list_dispatch()->set_element_valid(index, true);
    }

    void ListView::resize(size_t new_size)
    {
        if (!valid()) {
            throw std::runtime_error("ListView::resize() on invalid view");
        }
        list_dispatch()->resize(new_size, element_builder_for(*element_schema()));
    }

    void ListView::clear()
    {
        if (!valid()) {
            throw std::runtime_error("ListView::clear() on invalid view");
        }
        list_dispatch()->clear(element_builder_for(*element_schema()));
    }

    void ListView::push_back(const View &value)
    {
        const size_t index = size();
        resize(index + 1);
        set(index, value);
    }

    detail::ListViewDispatch *ListView::list_dispatch() noexcept
    {
        return valid() ? static_cast<detail::ListViewDispatch *>(dispatch()) : nullptr;
    }

    const detail::ListViewDispatch *ListView::list_dispatch() const noexcept
    {
        return valid() ? static_cast<const detail::ListViewDispatch *>(dispatch()) : nullptr;
    }

}  // namespace hgraph
