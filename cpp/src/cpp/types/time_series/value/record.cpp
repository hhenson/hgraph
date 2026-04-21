#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/value/record.h>
#include <hgraph/types/time_series/value/builder.h>
#include <hgraph/types/value/type_meta_bindings.h>
#include <hgraph/types/value/validity_bitmap.h>

#include <algorithm>
#include <concepts>
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
        [[nodiscard]] static MutationTracking nested_value_tracking(const value::TypeMeta &schema,
                                                                   MutationTracking       tracking) noexcept
        {
            if (tracking != MutationTracking::Delta) { return MutationTracking::Plain; }

            switch (schema.kind) {
                case value::TypeKind::Atomic: return MutationTracking::Plain;
                default: return MutationTracking::Delta;
            }
        }

        [[nodiscard]] MutationTracking record_field_tracking(const value::TypeMeta &schema, MutationTracking tracking) noexcept
        {
            return nested_value_tracking(schema, tracking);
        }

        struct RecordStateHeader
        {
            size_t mutation_depth{0};
        };

        struct RecordFieldLayout
        {
            std::string_view name;
            std::reference_wrapper<const value::TypeMeta> schema;
            std::reference_wrapper<const ValueBuilder>    builder;
            size_t                                        offset{0};
            bool                                          requires_destruct{true};
        };

        template <MutationTracking TTracking> struct RecordDispatchBase : RecordViewDispatch
        {
            static constexpr MutationTracking tracking_mode = TTracking;
            static constexpr bool tracks_deltas_v = TTracking == MutationTracking::Delta;

            [[nodiscard]] MutationTracking tracking() const noexcept override
            {
                return tracking_mode;
            }

            explicit RecordDispatchBase(const value::TypeMeta &schema)
                : m_schema(schema)
            {
                if (schema.kind != value::TypeKind::Tuple && schema.kind != value::TypeKind::Bundle) {
                    throw std::invalid_argument("RecordDispatchBase requires a tuple or bundle schema");
                }

                size_t offset = 0;
                size_t max_alignment = tracks_deltas_v ? alignof(RecordStateHeader) : 1;
                m_fields.reserve(schema.field_count);
                for (size_t i = 0; i < schema.field_count; ++i) {
                    const value::BundleFieldInfo &field = schema.fields[i];
                    if (field.type == nullptr) { throw std::runtime_error("Record schema requires field schemas"); }

                    const ValueBuilder &field_builder =
                        ValueBuilderFactory::checked_builder_for(field.type, record_field_tracking(*field.type, tracking_mode));
                    const size_t field_alignment = field_builder.alignment();
                    offset = align_up(offset, field_alignment);

                    std::string_view field_name = field.name != nullptr ? std::string_view{field.name} : std::string_view{};
                    m_fields.push_back(RecordFieldLayout{
                        field_name,
                        std::cref(*field.type),
                        std::cref(field_builder),
                        offset,
                        field_builder.requires_destruct(),
                    });
                    if (!field_name.empty()) {
                        m_name_to_index[field_name] = i;
                    }
                    offset += field_builder.size();
                    max_alignment = std::max(max_alignment, field_alignment);
                }

                m_payload_size = align_up(offset, max_alignment);
                m_header_size = tracks_deltas_v ? align_up(sizeof(RecordStateHeader), max_alignment) : 0;
                m_alignment = max_alignment;
                m_allocation_size =
                    align_up(m_header_size + m_payload_size + value::validity_mask_bytes(m_fields.size()) +
                                 (tracks_deltas_v ? value::validity_mask_bytes(m_fields.size()) : 0),
                             max_alignment);
                m_requires_destruct = std::ranges::any_of(m_fields, [](const RecordFieldLayout &field) {
                    return field.requires_destruct;
                });
            }

            void begin_mutation(void *data) const override
            {
                if constexpr (tracks_deltas_v) {
                    RecordStateHeader *header = state(data);
                    if (header->mutation_depth++ == 0) {
                        value::validity_set_all(updated_memory(data), m_fields.size(), false);
                    }
                }
            }

            void end_mutation(void *data) const override
            {
                if constexpr (tracks_deltas_v) {
                    RecordStateHeader *header = state(data);
                    if (header->mutation_depth == 0) {
                        throw std::runtime_error("Record mutation depth underflow");
                    }
                    --header->mutation_depth;
                }
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
                return static_cast<std::byte *>(payload_memory(data)) + field(index).offset;
            }

            [[nodiscard]] const void *field_data(const void *data, size_t index) const override
            {
                return static_cast<const std::byte *>(payload_memory(data)) + field(index).offset;
            }

            [[nodiscard]] bool field_valid(const void *data, size_t index) const override
            {
                return value::validity_bit_get(validity_memory(data), checked_index(index));
            }

            [[nodiscard]] bool field_updated(const void *data, size_t index) const noexcept override
            {
                const size_t checked = index;
                if constexpr (tracks_deltas_v) {
                    return checked < m_fields.size() && value::validity_bit_get(updated_memory(data), checked);
                } else {
                    static_cast<void>(data);
                    static_cast<void>(checked);
                    return false;
                }
            }

            void set_field_valid(void *data, size_t index, bool valid) const override
            {
                const size_t checked = checked_index(index);
                if (!valid && field_valid(data, checked)) {
                    reset_field(data, checked);
                }
                value::validity_bit_set(validity_memory(data), checked, valid);
                if constexpr (tracks_deltas_v) {
                    if (state(data)->mutation_depth > 0) {
                        value::validity_bit_set(updated_memory(data), checked, true);
                    }
                }
            }

            [[nodiscard]] size_t find_field(std::string_view name) const noexcept override
            {
                auto it = m_name_to_index.find(name);
                return it != m_name_to_index.end() ? it->second : SIZE_MAX;
            }

            [[nodiscard]] size_t allocation_size() const noexcept
            {
                return m_allocation_size;
            }

            [[nodiscard]] size_t alignment() const noexcept
            {
                return m_alignment;
            }

            [[nodiscard]] bool requires_destruct() const noexcept
            {
                return m_requires_destruct;
            }

            void construct(void *memory) const
            {
                if constexpr (tracks_deltas_v) { std::construct_at(state(memory)); }
                for (size_t i = 0; i < m_fields.size(); ++i) {
                    field(i).builder.get().construct(field_data(memory, i));
                }
                value::validity_set_all(validity_memory(memory), m_fields.size(), true);
                if constexpr (tracks_deltas_v) { value::validity_set_all(updated_memory(memory), m_fields.size(), false); }
            }

            void destruct(void *memory) const noexcept
            {
                if (!m_requires_destruct) { return; }
                for (size_t i = 0; i < m_fields.size(); ++i) {
                    if (field(i).requires_destruct) {
                        field(i).builder.get().destruct(field_data(memory, i));
                    }
                }
                if constexpr (tracks_deltas_v) { std::destroy_at(state(memory)); }
            }

            void copy_construct(void *dst, const void *src) const
            {
                if constexpr (tracks_deltas_v) { std::construct_at(state(dst)); }
                for (size_t i = 0; i < m_fields.size(); ++i) {
                    field(i).builder.get().copy_construct(field_data(dst, i), field_data(src, i), field(i).builder);
                }
                copy_validity(dst, src);
                if constexpr (tracks_deltas_v) { value::validity_set_all(updated_memory(dst), m_fields.size(), false); }
            }

            void move_construct(void *dst, void *src) const
            {
                if constexpr (tracks_deltas_v) { std::construct_at(state(dst)); }
                for (size_t i = 0; i < m_fields.size(); ++i) {
                    field(i).builder.get().move_construct(field_data(dst, i), field_data(src, i), field(i).builder);
                }
                copy_validity(dst, src);
                if constexpr (tracks_deltas_v) {
                    value::validity_set_all(updated_memory(dst), m_fields.size(), false);
                    state(src)->mutation_depth = 0;
                }
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

            void copy_from(void *dst, const View &src) const override
            {
                if (this == detail::ViewAccess::dispatch(src)) {
                    assign(dst, detail::ViewAccess::data(src));
                    return;
                }

                const auto source = src.as_tuple();
                for (size_t i = 0; i < m_fields.size(); ++i) {
                    const View source_field = source.at(i);
                    if (!source_field.has_value()) {
                        set_field_valid(dst, i, false);
                        continue;
                    }

                    View destination_field{&field_dispatch(i), field_data(dst, i), &field_schema(i)};
                    destination_field.copy_from(source_field);
                    value::validity_bit_set(validity_memory(dst), i, true);
                }
            }

            void set_from_cpp(void *dst, const void *src, const value::TypeMeta *src_schema) const override
            {
                if (src_schema == &m_schema.get()) {
                    assign(dst, src);
                    return;
                }
                throw std::invalid_argument("Record value set_from_cpp requires a matching record schema");
            }

            void move_from_cpp(void *dst, void *src, const value::TypeMeta *src_schema) const override
            {
                if (src_schema == &m_schema.get()) {
                    assign(dst, src);
                    for (size_t i = 0; i < m_fields.size(); ++i) {
                        set_field_valid(src, i, false);
                    }
                    return;
                }
                throw std::invalid_argument("Record value move_from_cpp requires a matching record schema");
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
                return static_cast<std::byte *>(payload_memory(data)) + m_payload_size;
            }

            [[nodiscard]] const std::byte *validity_memory(const void *data) const noexcept
            {
                return static_cast<const std::byte *>(payload_memory(data)) + m_payload_size;
            }

            [[nodiscard]] std::byte *updated_memory(void *data) const noexcept
            {
                return validity_memory(data) + value::validity_mask_bytes(m_fields.size());
            }

            [[nodiscard]] const std::byte *updated_memory(const void *data) const noexcept
            {
                return validity_memory(data) + value::validity_mask_bytes(m_fields.size());
            }

            [[nodiscard]] RecordStateHeader *state(void *data) const noexcept
            {
                return std::launder(reinterpret_cast<RecordStateHeader *>(data));
            }

            [[nodiscard]] const RecordStateHeader *state(const void *data) const noexcept
            {
                return std::launder(reinterpret_cast<const RecordStateHeader *>(data));
            }

            [[nodiscard]] void *payload_memory(void *data) const noexcept
            {
                return static_cast<std::byte *>(data) + m_header_size;
            }

            [[nodiscard]] const void *payload_memory(const void *data) const noexcept
            {
                return static_cast<const std::byte *>(data) + m_header_size;
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
                if (layout.requires_destruct) {
                    layout.builder.get().destruct(slot);
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
            /// Cached field-name → index lookup.  Populated once at
            /// construction for O(1) name-based field access.
            std::unordered_map<std::string_view, size_t>  m_name_to_index;
            size_t                                        m_payload_size{0};
            size_t                                        m_header_size{0};
            size_t                                        m_alignment{alignof(std::max_align_t)};
            size_t                                        m_allocation_size{0};
            bool                                          m_requires_destruct{true};
        };

        template <MutationTracking TTracking> struct TupleDispatch final : RecordDispatchBase<TTracking>
        {
            static constexpr MutationTracking tracking_mode = TTracking;

            explicit TupleDispatch(const value::TypeMeta &schema)
                : RecordDispatchBase<TTracking>(schema)
            {
            }

            [[nodiscard]] std::string to_string(const void *data) const override
            {
                std::string result = "(";
                for (size_t i = 0; i < this->size(); ++i) {
                    if (i > 0) { result += ", "; }
                    result += this->field_valid(data, i) ? this->field_dispatch(i).to_string(this->field_data(data, i)) : "None";
                }
                result += ")";
                return result;
            }

            [[nodiscard]] nb::object to_python(const void *data, const value::TypeMeta *schema) const override
            {
                static_cast<void>(schema);
                nb::list result;
                for (size_t i = 0; i < this->size(); ++i) {
                    if (!this->field_valid(data, i)) {
                        result.append(nb::none());
                    } else {
                        result.append(this->field_dispatch(i).to_python(this->field_data(data, i), &this->field_schema(i)));
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
                const size_t count = std::min<size_t>(nb::len(sequence), this->size());
                for (size_t i = 0; i < this->size(); ++i) {
                    if (i >= count) {
                        this->set_field_valid(dst, i, false);
                        continue;
                    }
                    nb::object element = sequence[i];
                    if (element.is_none()) {
                        this->set_field_valid(dst, i, false);
                    } else {
                        this->field_dispatch(i).from_python(this->field_data(dst, i), element, &this->field_schema(i));
                        this->set_field_valid(dst, i, true);
                    }
                }
            }
        };

        template <MutationTracking TTracking> struct BundleDispatch final : RecordDispatchBase<TTracking>
        {
            static constexpr MutationTracking tracking_mode = TTracking;

            explicit BundleDispatch(const value::TypeMeta &schema)
                : RecordDispatchBase<TTracking>(schema)
            {
            }

            [[nodiscard]] std::string to_string(const void *data) const override
            {
                std::string result = "{";
                for (size_t i = 0; i < this->size(); ++i) {
                    if (i > 0) { result += ", "; }
                    result += std::string(this->field_name(i));
                    result += ": ";
                    result += this->field_valid(data, i) ? this->field_dispatch(i).to_string(this->field_data(data, i)) : "None";
                }
                result += "}";
                return result;
            }

            [[nodiscard]] nb::object to_python(const void *data, const value::TypeMeta *schema) const override
            {
                if (schema != nullptr) {
                    nb::object py_class = value::get_compound_scalar_class(schema);
                    if (py_class.is_valid()) {
                        nb::dict kwargs;
                        for (size_t i = 0; i < this->size(); ++i) {
                            const std::string_view name = this->field_name(i);
                            if (name.empty()) { continue; }
                            kwargs[nb::str(name.data(), name.size())] =
                                this->field_valid(data, i)
                                    ? this->field_dispatch(i).to_python(this->field_data(data, i), &this->field_schema(i))
                                    : nb::none();
                        }
                        return py_class(**kwargs);
                    }
                }

                nb::dict result;
                for (size_t i = 0; i < this->size(); ++i) {
                    const std::string_view name = this->field_name(i);
                    if (name.empty()) { continue; }
                    if (!this->field_valid(data, i)) {
                        result[nb::str(name.data(), name.size())] = nb::none();
                    } else {
                        result[nb::str(name.data(), name.size())] =
                            this->field_dispatch(i).to_python(this->field_data(data, i), &this->field_schema(i));
                    }
                }
                return result;
            }

            void from_python(void *dst, const nb::object &src, const value::TypeMeta *schema) const override
            {
                static_cast<void>(schema);
                if (nb::isinstance<nb::dict>(src)) {
                    const nb::dict map = nb::cast<nb::dict>(src);
                    for (size_t i = 0; i < this->size(); ++i) {
                        const std::string_view name = this->field_name(i);
                        if (name.empty() || !map.contains(nb::str(name.data(), name.size()))) {
                            this->set_field_valid(dst, i, false);
                            continue;
                        }
                        nb::object value = map[nb::str(name.data(), name.size())];
                        if (value.is_none()) {
                            this->set_field_valid(dst, i, false);
                        } else {
                            this->field_dispatch(i).from_python(this->field_data(dst, i), value, &this->field_schema(i));
                            this->set_field_valid(dst, i, true);
                        }
                    }
                    return;
                }

                if (nb::isinstance<nb::tuple>(src) || nb::isinstance<nb::list>(src)) {
                    const nb::sequence sequence = nb::cast<nb::sequence>(src);
                    const size_t count = std::min<size_t>(nb::len(sequence), this->size());
                    for (size_t i = 0; i < this->size(); ++i) {
                        if (i >= count) {
                            this->set_field_valid(dst, i, false);
                            continue;
                        }
                        nb::object value = sequence[i];
                        if (value.is_none()) {
                            this->set_field_valid(dst, i, false);
                        } else {
                            this->field_dispatch(i).from_python(this->field_data(dst, i), value, &this->field_schema(i));
                            this->set_field_valid(dst, i, true);
                        }
                    }
                    return;
                }

                for (size_t i = 0; i < this->size(); ++i) {
                    const std::string_view name = this->field_name(i);
                    if (name.empty() || !nb::hasattr(src, name.data())) {
                        this->set_field_valid(dst, i, false);
                        continue;
                    }
                    nb::object value = nb::getattr(src, name.data());
                    if (value.is_none()) {
                        this->set_field_valid(dst, i, false);
                    } else {
                        this->field_dispatch(i).from_python(this->field_data(dst, i), value, &this->field_schema(i));
                        this->set_field_valid(dst, i, true);
                    }
                }
            }
        };

        template <typename TDispatch> struct RecordStateOps final : ValueBuilderOps
        {
            explicit RecordStateOps(const TDispatch &dispatch) noexcept
                : m_dispatch(dispatch)
            {
            }

            [[nodiscard]] BuilderLayout layout(const value::TypeMeta &schema) const noexcept override
            {
                static_cast<void>(schema);
                return BuilderLayout{m_dispatch.get().allocation_size(), m_dispatch.get().alignment()};
            }

            [[nodiscard]] const ViewDispatch &view_dispatch(const value::TypeMeta &schema) const noexcept override
            {
                static_cast<void>(schema);
                return m_dispatch.get();
            }

            [[nodiscard]] bool requires_destruct(const value::TypeMeta &schema) const noexcept override
            {
                static_cast<void>(schema);
                return m_dispatch.get().requires_destruct();
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
            void destruct(void *memory) const noexcept override { m_dispatch.get().destruct(memory); }
            void copy_construct(void *dst, const void *src) const override { m_dispatch.get().copy_construct(dst, src); }
            void move_construct(void *dst, void *src) const override { m_dispatch.get().move_construct(dst, src); }

            std::reference_wrapper<const TDispatch> m_dispatch;
        };

        struct CachedBuilderEntry
        {
            std::shared_ptr<const ViewDispatch> dispatch;
            std::shared_ptr<const ValueBuilderOps>     state_ops;
            std::shared_ptr<const ValueBuilder> builder;
        };

        struct RecordBuilderKey
        {
            const value::TypeMeta *schema{nullptr};
            MutationTracking       tracking{MutationTracking::Delta};

            [[nodiscard]] bool operator==(const RecordBuilderKey &other) const noexcept
            {
                return schema == other.schema && tracking == other.tracking;
            }
        };

        struct RecordBuilderKeyHash
        {
            [[nodiscard]] size_t operator()(const RecordBuilderKey &key) const noexcept
            {
                return std::hash<const value::TypeMeta *>{}(key.schema) ^
                       (static_cast<size_t>(key.tracking) << 1U);
            }
        };

        const ValueBuilder *record_builder_for(const value::TypeMeta *schema, MutationTracking tracking)
        {
            if (schema == nullptr) { return nullptr; }
            if (schema->kind != value::TypeKind::Tuple && schema->kind != value::TypeKind::Bundle) { return nullptr; }

            static std::recursive_mutex cache_mutex;
            static std::unordered_map<RecordBuilderKey, CachedBuilderEntry, RecordBuilderKeyHash> cache;

            std::lock_guard lock(cache_mutex);
            const RecordBuilderKey key{schema, tracking};
            if (auto it = cache.find(key); it != cache.end()) {
                return it->second.builder.get();
            }

            CachedBuilderEntry entry;
            if (schema->kind == value::TypeKind::Tuple) {
                if (tracking == MutationTracking::Delta) {
                    auto dispatch = std::make_shared<TupleDispatch<MutationTracking::Delta>>(*schema);
                    auto state_ops =
                        std::make_shared<RecordStateOps<TupleDispatch<MutationTracking::Delta>>>(*dispatch);
                    auto builder = std::make_shared<ValueBuilder>(*schema, tracking, *state_ops);
                    entry.dispatch = std::move(dispatch);
                    entry.state_ops = std::move(state_ops);
                    entry.builder = std::move(builder);
                } else {
                    auto dispatch = std::make_shared<TupleDispatch<MutationTracking::Plain>>(*schema);
                    auto state_ops =
                        std::make_shared<RecordStateOps<TupleDispatch<MutationTracking::Plain>>>(*dispatch);
                    auto builder = std::make_shared<ValueBuilder>(*schema, tracking, *state_ops);
                    entry.dispatch = std::move(dispatch);
                    entry.state_ops = std::move(state_ops);
                    entry.builder = std::move(builder);
                }
            } else {
                if (tracking == MutationTracking::Delta) {
                    auto dispatch = std::make_shared<BundleDispatch<MutationTracking::Delta>>(*schema);
                    auto state_ops =
                        std::make_shared<RecordStateOps<BundleDispatch<MutationTracking::Delta>>>(*dispatch);
                    auto builder = std::make_shared<ValueBuilder>(*schema, tracking, *state_ops);
                    entry.dispatch = std::move(dispatch);
                    entry.state_ops = std::move(state_ops);
                    entry.builder = std::move(builder);
                } else {
                    auto dispatch = std::make_shared<BundleDispatch<MutationTracking::Plain>>(*schema);
                    auto state_ops =
                        std::make_shared<RecordStateOps<BundleDispatch<MutationTracking::Plain>>>(*dispatch);
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

    TupleView::TupleView(const View &view)
        : View(view)
    {
        if (!view.has_value()) { return; }
        if (view.schema() == nullptr ||
            (view.schema()->kind != value::TypeKind::Tuple && view.schema()->kind != value::TypeKind::Bundle)) {
            throw std::runtime_error("TupleView requires a tuple-compatible record schema");
        }
    }

    TupleMutationView TupleView::begin_mutation()
    {
        return TupleMutationView{*this};
    }

    TupleDeltaView TupleView::delta()
    {
        return TupleDeltaView{*this};
    }

    TupleDeltaView TupleView::delta() const
    {
        return TupleDeltaView{*this};
    }

    void TupleView::begin_mutation_scope()
    {
        const auto *dispatch = record_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("TupleView::begin_mutation on invalid view"); }
        dispatch->begin_mutation(data());
    }

    void TupleView::end_mutation_scope() noexcept
    {
        const auto *dispatch = record_dispatch();
        if (dispatch == nullptr) { return; }
        dispatch->end_mutation(data());
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

    TupleDeltaView::TupleDeltaView(const View &view)
        : View(view)
    {
        if (!view.has_value()) { return; }
        if (view.schema() == nullptr ||
            (view.schema()->kind != value::TypeKind::Tuple && view.schema()->kind != value::TypeKind::Bundle)) {
            throw std::runtime_error("TupleDeltaView requires a tuple-compatible record schema");
        }
    }

    Range<size_t> TupleDeltaView::updated_indices() const
    {
        const auto *dispatch = record_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("TupleDeltaView::updated_indices on invalid view"); }
        return Range<size_t>{this, dispatch->size(), &TupleDeltaView::slot_is_updated, &TupleDeltaView::project_index};
    }

    Range<View> TupleDeltaView::updated_values() const
    {
        const auto *dispatch = record_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("TupleDeltaView::updated_values on invalid view"); }
        return Range<View>{this, dispatch->size(), &TupleDeltaView::slot_is_updated, &TupleDeltaView::project_value};
    }

    bool TupleDeltaView::slot_is_updated(const void *context, size_t index)
    {
        const auto &delta = *static_cast<const TupleDeltaView *>(context);
        const auto *dispatch = delta.record_dispatch();
        return dispatch != nullptr && dispatch->field_updated(delta.data(), index);
    }

    size_t TupleDeltaView::project_index(const void *context, size_t index)
    {
        static_cast<void>(context);
        return index;
    }

    View TupleDeltaView::project_value(const void *context, size_t index)
    {
        return TupleView{*static_cast<const TupleDeltaView *>(context)}.at(index);
    }

    const detail::RecordViewDispatch *TupleDeltaView::record_dispatch() const noexcept
    {
        return has_value() ? static_cast<const detail::RecordViewDispatch *>(dispatch()) : nullptr;
    }

    TupleMutationView::TupleMutationView(TupleView &view)
        : TupleView(view)
    {
        begin_mutation_scope();
    }

    TupleMutationView::TupleMutationView(TupleMutationView &&other) noexcept
        : TupleView(other), m_owns_scope(other.m_owns_scope)
    {
        other.m_owns_scope = false;
    }

    TupleMutationView::~TupleMutationView()
    {
        if (m_owns_scope) {
            end_mutation_scope();
        }
    }

    void TupleMutationView::set(size_t index, const View &value)
    {
        const auto *dispatch = record_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("TupleMutationView::set on invalid view"); }
        if (value.schema() != nullptr && value.schema() != &dispatch->field_schema(index)) {
            throw std::runtime_error("TupleMutationView::set requires matching field schema");
        }
        if (!value.has_value()) {
            dispatch->set_field_valid(data(), index, false);
            return;
        }
        View destination{&dispatch->field_dispatch(index), dispatch->field_data(data(), index), &dispatch->field_schema(index)};
        destination.copy_from(value);
        dispatch->set_field_valid(data(), index, true);
    }

    const detail::RecordViewDispatch *TupleView::record_dispatch() const noexcept
    {
        return has_value() ? static_cast<const detail::RecordViewDispatch *>(dispatch()) : nullptr;
    }

    BundleView::BundleView(const View &view)
        : TupleView(view)
    {
        if (!view.has_value()) { return; }
        if (view.schema() == nullptr || view.schema()->kind != value::TypeKind::Bundle) {
            throw std::runtime_error("BundleView requires a bundle schema");
        }
    }

    BundleMutationView BundleView::begin_mutation()
    {
        return BundleMutationView{*this};
    }

    BundleDeltaView BundleView::delta()
    {
        return BundleDeltaView{*this};
    }

    BundleDeltaView BundleView::delta() const
    {
        return BundleDeltaView{*this};
    }

    bool BundleView::has_field(std::string_view name) const noexcept
    {
        if (!has_value()) { return false; }
        const auto *dispatch = record_dispatch();
        if (dispatch == nullptr) { return false; }
        return dispatch->find_field(name) != SIZE_MAX;
    }

    View BundleView::field(std::string_view name)
    {
        return at(field_index(name));
    }

    View BundleView::field(std::string_view name) const
    {
        return at(field_index(name));
    }

    BundleDeltaView::BundleDeltaView(const View &view)
        : TupleDeltaView(view)
    {
        if (!view.has_value()) { return; }
        if (view.schema() == nullptr || view.schema()->kind != value::TypeKind::Bundle) {
            throw std::runtime_error("BundleDeltaView requires a bundle schema");
        }
    }

    Range<std::string_view> BundleDeltaView::updated_keys() const
    {
        const auto *dispatch = record_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("BundleDeltaView::updated_keys on invalid view"); }
        return Range<std::string_view>{this, dispatch->size(), &TupleDeltaView::slot_is_updated, &BundleDeltaView::project_key};
    }

    std::string_view BundleDeltaView::project_key(const void *context, size_t index)
    {
        const auto *dispatch = static_cast<const BundleDeltaView *>(context)->record_dispatch();
        return dispatch->field_name(index);
    }

    BundleMutationView::BundleMutationView(BundleView &view)
        : BundleView(view)
    {
        begin_mutation_scope();
    }

    BundleMutationView::BundleMutationView(BundleMutationView &&other) noexcept
        : BundleView(other), m_owns_scope(other.m_owns_scope)
    {
        other.m_owns_scope = false;
    }

    BundleMutationView::~BundleMutationView()
    {
        if (m_owns_scope) {
            end_mutation_scope();
        }
    }

    void BundleMutationView::set(size_t index, const View &value)
    {
        const auto *dispatch = record_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("BundleMutationView::set on invalid view"); }
        if (value.schema() != nullptr && value.schema() != &dispatch->field_schema(index)) {
            throw std::runtime_error("BundleMutationView::set requires matching field schema");
        }
        if (!value.has_value()) {
            dispatch->set_field_valid(data(), index, false);
            return;
        }
        View destination{&dispatch->field_dispatch(index), dispatch->field_data(data(), index), &dispatch->field_schema(index)};
        destination.copy_from(value);
        dispatch->set_field_valid(data(), index, true);
    }

    void BundleMutationView::set_field(std::string_view name, const View &value)
    {
        set(field_index(name), value);
    }

    size_t BundleView::field_index(std::string_view name) const
    {
        const auto *dispatch = record_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("BundleView::field on invalid view"); }
        const size_t index = dispatch->find_field(name);
        if (index == SIZE_MAX) { throw std::out_of_range("BundleView::field unknown name"); }
        return index;
    }

}  // namespace hgraph
