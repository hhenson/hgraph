#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/value/record.h>
#include <hgraph/types/time_series/value/state.h>
#include <hgraph/types/value/validity_bitmap.h>

#include <algorithm>
#include <cstring>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace hgraph
{

    namespace detail
    {

        struct RecordFieldLayout
        {
            std::string_view name;
            std::reference_wrapper<const value::TypeMeta> schema;
            std::reference_wrapper<const ValueBuilder>    builder;
            size_t                                        offset{0};
            bool                                          requires_destroy{true};
        };

        struct RecordDispatchBase : RecordViewDispatch
        {
            explicit RecordDispatchBase(const value::TypeMeta &schema)
                : m_schema(schema)
            {
                if (schema.kind != value::TypeKind::Tuple && schema.kind != value::TypeKind::Bundle) {
                    throw std::invalid_argument("RecordDispatchBase requires a tuple or bundle schema");
                }

                size_t offset = 0;
                size_t max_alignment = 1;
                m_fields.reserve(schema.field_count);
                for (size_t i = 0; i < schema.field_count; ++i) {
                    const value::BundleFieldInfo &field = schema.fields[i];
                    if (field.type == nullptr) { throw std::runtime_error("Record schema requires field schemas"); }

                    const ValueBuilder &field_builder = ValueBuilderFactory::checked_builder_for(field.type);
                    const size_t field_alignment = field_builder.alignment();
                    offset = align_up(offset, field_alignment);

                    m_fields.push_back(RecordFieldLayout{
                        field.name != nullptr ? std::string_view{field.name} : std::string_view{},
                        std::cref(*field.type),
                        std::cref(field_builder),
                        offset,
                        field_builder.requires_destroy(),
                    });
                    offset += field_builder.size();
                    max_alignment = std::max(max_alignment, field_alignment);
                }

                m_payload_size = align_up(offset, max_alignment);
                m_alignment = max_alignment;
                m_allocation_size = align_up(m_payload_size + value::validity_mask_bytes(m_fields.size()), max_alignment);
                m_requires_destroy = std::ranges::any_of(m_fields, [](const RecordFieldLayout &field) {
                    return field.requires_destroy;
                });
            }

            [[nodiscard]] size_t size() const noexcept override
            {
                return m_fields.size();
            }

            [[nodiscard]] const value::TypeMeta &field_schema(size_t index) const override
            {
                return field(index).schema;
            }

            [[nodiscard]] const ViewDispatch &field_dispatch(size_t index) const override
            {
                return field(index).builder.get().dispatch();
            }

            [[nodiscard]] std::string_view field_name(size_t index) const noexcept override
            {
                return field(index).name;
            }

            [[nodiscard]] void *field_data(void *data, size_t index) const override
            {
                return static_cast<std::byte *>(data) + field(index).offset;
            }

            [[nodiscard]] const void *field_data(const void *data, size_t index) const override
            {
                return static_cast<const std::byte *>(data) + field(index).offset;
            }

            [[nodiscard]] bool field_valid(const void *data, size_t index) const override
            {
                return value::validity_bit_get(validity_memory(data), checked_index(index));
            }

            void set_field_valid(void *data, size_t index, bool valid) const override
            {
                const size_t checked = checked_index(index);
                if (!valid && field_valid(data, checked)) {
                    reset_field(data, checked);
                }
                value::validity_bit_set(validity_memory(data), checked, valid);
            }

            [[nodiscard]] size_t allocation_size() const noexcept
            {
                return m_allocation_size;
            }

            [[nodiscard]] size_t alignment() const noexcept
            {
                return m_alignment;
            }

            [[nodiscard]] bool requires_destroy() const noexcept
            {
                return m_requires_destroy;
            }

            void construct(void *memory) const
            {
                for (size_t i = 0; i < m_fields.size(); ++i) {
                    field(i).builder.get().construct(field_data(memory, i));
                }
                value::validity_set_all(validity_memory(memory), m_fields.size(), true);
            }

            void destroy(void *memory) const noexcept
            {
                if (!m_requires_destroy) { return; }
                for (size_t i = 0; i < m_fields.size(); ++i) {
                    if (field(i).requires_destroy) {
                        field(i).builder.get().destroy(field_data(memory, i));
                    }
                }
            }

            void copy_construct(void *dst, const void *src) const
            {
                for (size_t i = 0; i < m_fields.size(); ++i) {
                    field(i).builder.get().copy_construct(field_data(dst, i), field_data(src, i), field(i).builder);
                }
                copy_validity(dst, src);
            }

            void move_construct(void *dst, void *src) const
            {
                for (size_t i = 0; i < m_fields.size(); ++i) {
                    field(i).builder.get().move_construct(field_data(dst, i), field_data(src, i), field(i).builder);
                }
                copy_validity(dst, src);
            }

            [[nodiscard]] size_t hash(const void *data) const override
            {
                size_t result = 0;
                constexpr size_t null_hash_seed = 0x9e3779b97f4a7c15ULL;
                for (size_t i = 0; i < m_fields.size(); ++i) {
                    size_t field_hash = null_hash_seed + i;
                    if (field_valid(data, i)) {
                        field_hash = field_dispatch(i).hash(field_data(data, i));
                    }
                    result ^= field_hash + 0x9e3779b9 + (result << 6) + (result >> 2);
                }
                return result;
            }

            [[nodiscard]] std::partial_ordering compare(const void *lhs, const void *rhs) const override
            {
                for (size_t i = 0; i < m_fields.size(); ++i) {
                    const bool lhs_valid = field_valid(lhs, i);
                    const bool rhs_valid = field_valid(rhs, i);
                    if (!lhs_valid || !rhs_valid) {
                        if (lhs_valid != rhs_valid) { return std::partial_ordering::unordered; }
                        continue;
                    }
                    const std::partial_ordering order = field_dispatch(i).compare(field_data(lhs, i), field_data(rhs, i));
                    if (std::is_lt(order) || std::is_gt(order) || order == std::partial_ordering::unordered) {
                        return order;
                    }
                }
                return std::partial_ordering::equivalent;
            }

            void assign(void *dst, const void *src) const override
            {
                for (size_t i = 0; i < m_fields.size(); ++i) {
                    if (!field_valid(src, i)) {
                        set_field_valid(dst, i, false);
                        continue;
                    }
                    field_dispatch(i).assign(field_data(dst, i), field_data(src, i));
                    value::validity_bit_set(validity_memory(dst), i, true);
                }
            }

            void set_from_cpp(void *dst, const void *src, const value::TypeMeta *src_schema) const override
            {
                static_cast<void>(dst);
                static_cast<void>(src);
                static_cast<void>(src_schema);
                throw std::invalid_argument("Record value set_from_cpp is not implemented");
            }

            void move_from_cpp(void *dst, void *src, const value::TypeMeta *src_schema) const override
            {
                static_cast<void>(dst);
                static_cast<void>(src);
                static_cast<void>(src_schema);
                throw std::invalid_argument("Record value move_from_cpp is not implemented");
            }

          protected:
            [[nodiscard]] const RecordFieldLayout &field(size_t index) const
            {
                return m_fields[checked_index(index)];
            }

            [[nodiscard]] static size_t align_up(size_t value, size_t alignment) noexcept
            {
                return alignment <= 1 ? value : ((value + alignment - 1) / alignment) * alignment;
            }

            [[nodiscard]] std::byte *validity_memory(void *data) const noexcept
            {
                return static_cast<std::byte *>(data) + m_payload_size;
            }

            [[nodiscard]] const std::byte *validity_memory(const void *data) const noexcept
            {
                return static_cast<const std::byte *>(data) + m_payload_size;
            }

            void copy_validity(void *dst, const void *src) const
            {
                const size_t bytes = value::validity_mask_bytes(m_fields.size());
                if (bytes > 0) {
                    std::memcpy(validity_memory(dst), validity_memory(src), bytes);
                }
            }

            void reset_field(void *data, size_t index) const
            {
                void *slot = field_data(data, index);
                const auto &layout = field(index);
                if (layout.requires_destroy) {
                    layout.builder.get().destroy(slot);
                }
                layout.builder.get().construct(slot);
            }

            [[nodiscard]] size_t checked_index(size_t index) const
            {
                if (index >= m_fields.size()) { throw std::out_of_range("Record field index out of range"); }
                return index;
            }

            std::reference_wrapper<const value::TypeMeta> m_schema;
            std::vector<RecordFieldLayout>                m_fields;
            size_t                                        m_payload_size{0};
            size_t                                        m_alignment{alignof(std::max_align_t)};
            size_t                                        m_allocation_size{0};
            bool                                          m_requires_destroy{true};
        };

        struct TupleDispatch final : RecordDispatchBase
        {
            explicit TupleDispatch(const value::TypeMeta &schema)
                : RecordDispatchBase(schema)
            {
            }

            [[nodiscard]] std::string to_string(const void *data) const override
            {
                std::string result = "(";
                for (size_t i = 0; i < size(); ++i) {
                    if (i > 0) { result += ", "; }
                    result += field_valid(data, i) ? field_dispatch(i).to_string(field_data(data, i)) : "None";
                }
                result += ")";
                return result;
            }

            [[nodiscard]] nb::object to_python(const void *data, const value::TypeMeta *schema) const override
            {
                static_cast<void>(schema);
                nb::list result;
                for (size_t i = 0; i < size(); ++i) {
                    if (!field_valid(data, i)) {
                        result.append(nb::none());
                    } else {
                        result.append(field_dispatch(i).to_python(field_data(data, i), &field_schema(i)));
                    }
                }
                return nb::tuple(result);
            }

            void from_python(void *dst, const nb::object &src, const value::TypeMeta *schema) const override
            {
                static_cast<void>(schema);
                if (!nb::isinstance<nb::tuple>(src) && !nb::isinstance<nb::list>(src)) {
                    throw std::runtime_error("Tuple value expects a tuple or list");
                }

                nb::sequence sequence = nb::cast<nb::sequence>(src);
                const size_t count = std::min<size_t>(nb::len(sequence), size());
                for (size_t i = 0; i < size(); ++i) {
                    if (i >= count) {
                        set_field_valid(dst, i, false);
                        continue;
                    }
                    nb::object element = sequence[i];
                    if (element.is_none()) {
                        set_field_valid(dst, i, false);
                    } else {
                        field_dispatch(i).from_python(field_data(dst, i), element, &field_schema(i));
                        set_field_valid(dst, i, true);
                    }
                }
            }
        };

        struct BundleDispatch final : RecordDispatchBase
        {
            explicit BundleDispatch(const value::TypeMeta &schema)
                : RecordDispatchBase(schema)
            {
            }

            [[nodiscard]] std::string to_string(const void *data) const override
            {
                std::string result = "{";
                for (size_t i = 0; i < size(); ++i) {
                    if (i > 0) { result += ", "; }
                    result += std::string(field_name(i));
                    result += ": ";
                    result += field_valid(data, i) ? field_dispatch(i).to_string(field_data(data, i)) : "None";
                }
                result += "}";
                return result;
            }

            [[nodiscard]] nb::object to_python(const void *data, const value::TypeMeta *schema) const override
            {
                static_cast<void>(schema);
                nb::dict result;
                for (size_t i = 0; i < size(); ++i) {
                    const std::string_view name = field_name(i);
                    if (name.empty()) { continue; }
                    if (!field_valid(data, i)) {
                        result[nb::str(name.data(), name.size())] = nb::none();
                    } else {
                        result[nb::str(name.data(), name.size())] =
                            field_dispatch(i).to_python(field_data(data, i), &field_schema(i));
                    }
                }
                return result;
            }

            void from_python(void *dst, const nb::object &src, const value::TypeMeta *schema) const override
            {
                static_cast<void>(schema);
                if (nb::isinstance<nb::dict>(src)) {
                    const nb::dict map = nb::cast<nb::dict>(src);
                    for (size_t i = 0; i < size(); ++i) {
                        const std::string_view name = field_name(i);
                        if (name.empty() || !map.contains(nb::str(name.data(), name.size()))) {
                            set_field_valid(dst, i, false);
                            continue;
                        }
                        nb::object value = map[nb::str(name.data(), name.size())];
                        if (value.is_none()) {
                            set_field_valid(dst, i, false);
                        } else {
                            field_dispatch(i).from_python(field_data(dst, i), value, &field_schema(i));
                            set_field_valid(dst, i, true);
                        }
                    }
                    return;
                }

                if (nb::isinstance<nb::tuple>(src) || nb::isinstance<nb::list>(src)) {
                    const nb::sequence sequence = nb::cast<nb::sequence>(src);
                    const size_t count = std::min<size_t>(nb::len(sequence), size());
                    for (size_t i = 0; i < size(); ++i) {
                        if (i >= count) {
                            set_field_valid(dst, i, false);
                            continue;
                        }
                        nb::object value = sequence[i];
                        if (value.is_none()) {
                            set_field_valid(dst, i, false);
                        } else {
                            field_dispatch(i).from_python(field_data(dst, i), value, &field_schema(i));
                            set_field_valid(dst, i, true);
                        }
                    }
                    return;
                }

                for (size_t i = 0; i < size(); ++i) {
                    const std::string_view name = field_name(i);
                    if (name.empty() || !nb::hasattr(src, name.data())) {
                        set_field_valid(dst, i, false);
                        continue;
                    }
                    nb::object value = nb::getattr(src, name.data());
                    if (value.is_none()) {
                        set_field_valid(dst, i, false);
                    } else {
                        field_dispatch(i).from_python(field_data(dst, i), value, &field_schema(i));
                        set_field_valid(dst, i, true);
                    }
                }
            }
        };

        template <typename TDispatch> struct RecordStateOps final : StateOps
        {
            explicit RecordStateOps(const TDispatch &dispatch) noexcept
                : m_dispatch(dispatch)
            {
            }

            void expand_builder(ValueBuilder &builder, const value::TypeMeta &schema) const noexcept override
            {
                static_cast<void>(schema);
                builder.cache_layout(m_dispatch.get().allocation_size(), m_dispatch.get().alignment());
                builder.cache_lifecycle(m_dispatch.get().requires_destroy(), true, false);
            }

            [[nodiscard]] const ViewDispatch &view_dispatch(const value::TypeMeta &schema) const noexcept override
            {
                static_cast<void>(schema);
                return m_dispatch;
            }

            [[nodiscard]] bool requires_destroy(const value::TypeMeta &schema) const noexcept override
            {
                static_cast<void>(schema);
                return m_dispatch.get().requires_destroy();
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

        const ValueBuilder *record_builder_for(const value::TypeMeta *schema)
        {
            if (schema == nullptr) { return nullptr; }
            if (schema->kind != value::TypeKind::Tuple && schema->kind != value::TypeKind::Bundle) { return nullptr; }

            static std::mutex cache_mutex;
            static std::unordered_map<const value::TypeMeta *, CachedBuilderEntry> cache;

            std::lock_guard lock(cache_mutex);
            if (auto it = cache.find(schema); it != cache.end()) {
                return it->second.builder.get();
            }

            CachedBuilderEntry entry;
            if (schema->kind == value::TypeKind::Tuple) {
                auto dispatch = std::make_shared<TupleDispatch>(*schema);
                auto state_ops = std::make_shared<RecordStateOps<TupleDispatch>>(*dispatch);
                auto builder = std::make_shared<ValueBuilder>(*schema, *state_ops);
                entry.dispatch = std::move(dispatch);
                entry.state_ops = std::move(state_ops);
                entry.builder = std::move(builder);
            } else {
                auto dispatch = std::make_shared<BundleDispatch>(*schema);
                auto state_ops = std::make_shared<RecordStateOps<BundleDispatch>>(*dispatch);
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

    TupleView::TupleView(const View &view)
        : View(view)
    {
        if (!view.valid()) { return; }
        if (view.schema() == nullptr ||
            (view.schema()->kind != value::TypeKind::Tuple && view.schema()->kind != value::TypeKind::Bundle)) {
            throw std::runtime_error("TupleView requires a tuple-compatible record schema");
        }
    }

    size_t TupleView::size() const
    {
        const auto *dispatch = record_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("TupleView::size on invalid view"); }
        return dispatch->size();
    }

    bool TupleView::empty() const
    {
        return size() == 0;
    }

    View TupleView::at(size_t index)
    {
        const auto *dispatch = record_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("TupleView::at on invalid view"); }
        if (!dispatch->field_valid(data(), index)) {
            return View::invalid_for(&dispatch->field_schema(index));
        }
        return View{&dispatch->field_dispatch(index), dispatch->field_data(data(), index), &dispatch->field_schema(index)};
    }

    View TupleView::at(size_t index) const
    {
        const auto *dispatch = record_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("TupleView::at on invalid view"); }
        if (!dispatch->field_valid(data(), index)) {
            return View::invalid_for(&dispatch->field_schema(index));
        }
        return View{&dispatch->field_dispatch(index),
                    const_cast<void *>(dispatch->field_data(data(), index)),
                    &dispatch->field_schema(index)};
    }

    View TupleView::operator[](size_t index)
    {
        return at(index);
    }

    View TupleView::operator[](size_t index) const
    {
        return at(index);
    }

    void TupleView::set(size_t index, const View &value)
    {
        const auto *dispatch = record_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("TupleView::set on invalid view"); }
        if (value.schema() != nullptr && value.schema() != &dispatch->field_schema(index)) {
            throw std::invalid_argument("TupleView::set requires matching field schema");
        }
        if (!value.valid()) {
            dispatch->set_field_valid(data(), index, false);
            return;
        }
        dispatch->field_dispatch(index).assign(dispatch->field_data(data(), index), data_of(value));
        dispatch->set_field_valid(data(), index, true);
    }

    const detail::RecordViewDispatch *TupleView::record_dispatch() const noexcept
    {
        return valid() ? static_cast<const detail::RecordViewDispatch *>(dispatch()) : nullptr;
    }

    BundleView::BundleView(const View &view)
        : TupleView(view)
    {
        if (!view.valid()) { return; }
        if (view.schema() == nullptr || view.schema()->kind != value::TypeKind::Bundle) {
            throw std::runtime_error("BundleView requires a bundle schema");
        }
    }

    bool BundleView::has_field(std::string_view name) const noexcept
    {
        if (!valid()) { return false; }
        const auto *dispatch = record_dispatch();
        if (dispatch == nullptr) { return false; }
        for (size_t i = 0; i < dispatch->size(); ++i) {
            if (dispatch->field_name(i) == name) { return true; }
        }
        return false;
    }

    View BundleView::field(std::string_view name)
    {
        return at(field_index(name));
    }

    View BundleView::field(std::string_view name) const
    {
        return at(field_index(name));
    }

    void BundleView::set_field(std::string_view name, const View &value)
    {
        set(field_index(name), value);
    }

    size_t BundleView::field_index(std::string_view name) const
    {
        const auto *dispatch = record_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("BundleView::field on invalid view"); }
        for (size_t i = 0; i < dispatch->size(); ++i) {
            if (dispatch->field_name(i) == name) { return i; }
        }
        throw std::out_of_range("BundleView::field unknown name");
    }

}  // namespace hgraph
