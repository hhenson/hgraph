#include <hgraph/v2/types/metadata/type_registry.h>

#include <cstdint>
#include <utility>

namespace hgraph::v2
{
    namespace
    {
        constexpr ValueTypeFlags kCompositeSeedFlags = ValueTypeFlags::TriviallyConstructible |
                                                       ValueTypeFlags::TriviallyDestructible |
                                                       ValueTypeFlags::TriviallyCopyable | ValueTypeFlags::Hashable |
                                                       ValueTypeFlags::Equatable | ValueTypeFlags::Comparable |
                                                       ValueTypeFlags::BufferCompatible;

        struct LayoutInfo
        {
            size_t size{0};
            size_t alignment{1};
        };

        [[nodiscard]] constexpr size_t align_to(size_t offset, size_t alignment) noexcept
        {
            if (alignment <= 1)
            {
                return offset;
            }
            const size_t mask = alignment - 1;
            return (offset + mask) & ~mask;
        }

        void append_pointer_signature(std::string &target, const void *ptr)
        {
            target += std::to_string(reinterpret_cast<uintptr_t>(ptr));
        }

        [[nodiscard]] std::string make_tuple_signature(const std::vector<const ValueTypeMetaData *> &element_types)
        {
            std::string signature = "tuple(";
            for (const ValueTypeMetaData *type : element_types)
            {
                append_pointer_signature(signature, type);
                signature.push_back(';');
            }
            signature.push_back(')');
            return signature;
        }

        [[nodiscard]] std::string
        make_bundle_signature(const std::vector<std::pair<std::string, const ValueTypeMetaData *>> &fields)
        {
            std::string signature = "bundle(";
            for (const auto &[name, type] : fields)
            {
                signature += name;
                signature.push_back(':');
                append_pointer_signature(signature, type);
                signature.push_back(';');
            }
            signature.push_back(')');
            return signature;
        }

        [[nodiscard]] std::string make_unary_signature(std::string_view kind, const ValueTypeMetaData *element_type)
        {
            std::string signature{kind};
            signature.push_back('(');
            append_pointer_signature(signature, element_type);
            signature.push_back(')');
            return signature;
        }

        [[nodiscard]] std::string
        make_binary_signature(std::string_view kind, const ValueTypeMetaData *lhs, const ValueTypeMetaData *rhs)
        {
            std::string signature{kind};
            signature.push_back('(');
            append_pointer_signature(signature, lhs);
            signature.push_back(',');
            append_pointer_signature(signature, rhs);
            signature.push_back(')');
            return signature;
        }

        [[nodiscard]] std::string
        make_sized_signature(std::string_view kind, const ValueTypeMetaData *element_type, size_t size, bool extra = false)
        {
            std::string signature{kind};
            signature.push_back('(');
            append_pointer_signature(signature, element_type);
            signature.push_back(',');
            signature += std::to_string(size);
            signature.push_back(',');
            signature += extra ? "1" : "0";
            signature.push_back(')');
            return signature;
        }

        [[nodiscard]] std::string make_unary_ts_signature(std::string_view kind, const void *meta)
        {
            std::string signature{kind};
            signature.push_back('(');
            append_pointer_signature(signature, meta);
            signature.push_back(')');
            return signature;
        }

        [[nodiscard]] std::string make_sized_ts_signature(std::string_view kind, const void *meta, size_t size)
        {
            std::string signature{kind};
            signature.push_back('(');
            append_pointer_signature(signature, meta);
            signature.push_back(',');
            signature += std::to_string(size);
            signature.push_back(')');
            return signature;
        }

        [[nodiscard]] std::string make_dict_signature(const ValueTypeMetaData *key_type, const TSValueTypeMetaData *value_ts)
        {
            std::string signature = "dict(";
            append_pointer_signature(signature, key_type);
            signature.push_back(',');
            append_pointer_signature(signature, value_ts);
            signature.push_back(')');
            return signature;
        }

        [[nodiscard]] std::string
        make_window_signature(const ValueTypeMetaData *value_type, bool duration_based, int64_t first, int64_t second)
        {
            std::string signature = "window(";
            append_pointer_signature(signature, value_type);
            signature.push_back(',');
            signature += duration_based ? "1" : "0";
            signature.push_back(',');
            signature += std::to_string(first);
            signature.push_back(',');
            signature += std::to_string(second);
            signature.push_back(')');
            return signature;
        }

        [[nodiscard]] std::string make_ts_bundle_signature(const std::vector<std::pair<std::string, const TSValueTypeMetaData *>> &fields)
        {
            std::string signature = "tsb(";
            for (const auto &[name, type] : fields)
            {
                signature += name;
                signature.push_back(':');
                append_pointer_signature(signature, type);
                signature.push_back(';');
            }
            signature.push_back(')');
            return signature;
        }

        [[nodiscard]] ValueTypeFlags intersect_with(ValueTypeFlags flags, const ValueTypeMetaData *meta) noexcept
        {
            if (!meta)
            {
                return flags;
            }

            if (!meta->is_trivially_constructible())
            {
                flags = flags & ~ValueTypeFlags::TriviallyConstructible;
            }
            if (!meta->is_trivially_destructible())
            {
                flags = flags & ~ValueTypeFlags::TriviallyDestructible;
            }
            if (!meta->is_trivially_copyable())
            {
                flags = flags & ~ValueTypeFlags::TriviallyCopyable;
            }
            if (!meta->is_hashable())
            {
                flags = flags & ~ValueTypeFlags::Hashable;
            }
            if (!meta->is_equatable())
            {
                flags = flags & ~ValueTypeFlags::Equatable;
            }
            if (!meta->is_comparable())
            {
                flags = flags & ~ValueTypeFlags::Comparable;
            }
            if (!meta->is_buffer_compatible())
            {
                flags = flags & ~ValueTypeFlags::BufferCompatible;
            }

            return flags;
        }

        [[nodiscard]] ValueTypeFlags compute_composite_flags(const std::vector<const ValueTypeMetaData *> &element_types)
        {
            ValueTypeFlags flags = kCompositeSeedFlags;
            for (const ValueTypeMetaData *meta : element_types)
            {
                flags = intersect_with(flags, meta);
            }
            return flags;
        }

        [[nodiscard]] ValueTypeFlags
        compute_bundle_flags(const std::vector<std::pair<std::string, const ValueTypeMetaData *>> &fields)
        {
            ValueTypeFlags flags = kCompositeSeedFlags;
            for (const auto &[_, type] : fields)
            {
                flags = intersect_with(flags, type);
            }
            return flags;
        }

        [[nodiscard]] LayoutInfo compute_tuple_layout(const std::vector<const ValueTypeMetaData *> &element_types,
                                                      ValueFieldMetaData *fields)
        {
            LayoutInfo layout{};
            for (size_t index = 0; index < element_types.size(); ++index)
            {
                const ValueTypeMetaData *type = element_types[index];
                const size_t field_alignment = type ? type->alignment : 1;
                layout.size = align_to(layout.size, field_alignment);
                if (fields)
                {
                    fields[index].name = nullptr;
                    fields[index].index = index;
                    fields[index].offset = layout.size;
                    fields[index].type = type;
                }
                layout.size += type ? type->size : 0;
                if (field_alignment > layout.alignment)
                {
                    layout.alignment = field_alignment;
                }
            }
            layout.size = align_to(layout.size, layout.alignment);
            return layout;
        }

        [[nodiscard]] LayoutInfo compute_fixed_list_layout(const ValueTypeMetaData *element_type, size_t fixed_size)
        {
            LayoutInfo layout{};
            if (!element_type || fixed_size == 0)
            {
                return layout;
            }

            layout.alignment = element_type->alignment;
            for (size_t index = 0; index < fixed_size; ++index)
            {
                layout.size = align_to(layout.size, element_type->alignment);
                layout.size += element_type->size;
            }
            layout.size = align_to(layout.size, layout.alignment);
            return layout;
        }

        [[nodiscard]] ValueTypeFlags list_flags(const ValueTypeMetaData *element_type,
                                                size_t fixed_size,
                                                bool variadic_tuple) noexcept
        {
            ValueTypeFlags flags = variadic_tuple ? ValueTypeFlags::VariadicTuple : ValueTypeFlags::None;
            if (!element_type)
            {
                return flags;
            }

            if (fixed_size > 0)
            {
                if (element_type->is_trivially_constructible())
                {
                    flags |= ValueTypeFlags::TriviallyConstructible;
                }
                if (element_type->is_trivially_destructible())
                {
                    flags |= ValueTypeFlags::TriviallyDestructible;
                }
                if (element_type->is_trivially_copyable())
                {
                    flags |= ValueTypeFlags::TriviallyCopyable;
                }
                if (element_type->is_buffer_compatible())
                {
                    flags |= ValueTypeFlags::BufferCompatible;
                }
            }
            if (element_type->is_hashable())
            {
                flags |= ValueTypeFlags::Hashable;
            }
            if (element_type->is_equatable())
            {
                flags |= ValueTypeFlags::Equatable;
            }
            if (element_type->is_comparable())
            {
                flags |= ValueTypeFlags::Comparable;
            }

            return flags;
        }

        [[nodiscard]] ValueTypeFlags set_flags(const ValueTypeMetaData *element_type) noexcept
        {
            if (!element_type)
            {
                return ValueTypeFlags::None;
            }

            ValueTypeFlags flags = ValueTypeFlags::None;
            if (element_type->is_hashable() && element_type->is_equatable())
            {
                flags |= ValueTypeFlags::Hashable | ValueTypeFlags::Equatable;
            }
            return flags;
        }

        [[nodiscard]] ValueTypeFlags map_flags(const ValueTypeMetaData *key_type, const ValueTypeMetaData *value_type) noexcept
        {
            ValueTypeFlags flags = ValueTypeFlags::None;
            if (key_type && value_type && key_type->is_hashable() && key_type->is_equatable())
            {
                flags |= ValueTypeFlags::Equatable;
                if (value_type->is_hashable() && value_type->is_equatable())
                {
                    flags |= ValueTypeFlags::Hashable;
                }
            }
            return flags;
        }
    }  // namespace

    TypeRegistry &TypeRegistry::instance()
    {
        static TypeRegistry registry;
        return registry;
    }

    const ValueTypeMetaData *TypeRegistry::value_type(std::string_view name) const
    {
        const auto it = value_name_cache_.find(std::string(name));
        return it == value_name_cache_.end() ? nullptr : it->second;
    }

    const TSValueTypeMetaData *TypeRegistry::time_series_type(std::string_view name) const
    {
        const auto it = ts_name_cache_.find(std::string(name));
        return it == ts_name_cache_.end() ? nullptr : it->second;
    }

    const char *TypeRegistry::store_name(std::string_view name)
    {
        auto stored = std::make_unique<std::string>(name);
        const char *ptr = stored->c_str();
        name_storage_.push_back(std::move(stored));
        return ptr;
    }

    const char *TypeRegistry::store_name_interned(std::string_view name)
    {
        if (name.empty())
        {
            return nullptr;
        }

        for (const auto &entry : name_storage_)
        {
            if (*entry == name)
            {
                return entry->c_str();
            }
        }
        return store_name(name);
    }

    ValueFieldMetaData *TypeRegistry::store_value_fields(std::unique_ptr<ValueFieldMetaData[]> fields)
    {
        ValueFieldMetaData *ptr = fields.get();
        value_field_storage_.push_back(std::move(fields));
        return ptr;
    }

    TSFieldMetaData *TypeRegistry::store_ts_fields(std::unique_ptr<TSFieldMetaData[]> fields)
    {
        TSFieldMetaData *ptr = fields.get();
        ts_field_storage_.push_back(std::move(fields));
        return ptr;
    }

    void TypeRegistry::register_value_alias(std::string_view name, const ValueTypeMetaData *meta)
    {
        if (name.empty() || !meta)
        {
            return;
        }

        value_name_cache_[std::string(name)] = meta;
        if (!meta->display_name)
        {
            const_cast<ValueTypeMetaData *>(meta)->display_name = store_name_interned(name);
        }
    }

    void TypeRegistry::register_ts_alias(std::string_view name, const TSValueTypeMetaData *meta)
    {
        if (name.empty() || !meta)
        {
            return;
        }

        ts_name_cache_[std::string(name)] = meta;
        if (!meta->display_name)
        {
            const_cast<TSValueTypeMetaData *>(meta)->display_name = store_name_interned(name);
        }
    }

    const ValueTypeMetaData *TypeRegistry::register_scalar_impl(std::type_index type_key,
                                                                std::string_view name,
                                                                size_t size,
                                                                size_t alignment,
                                                                ValueTypeFlags flags)
    {
        if (const auto it = scalar_cache_.find(type_key); it != scalar_cache_.end())
        {
            register_value_alias(name, it->second);
            return it->second;
        }

        auto meta = std::make_unique<ValueTypeMetaData>(
            ValueTypeKind::Atomic, size, alignment, flags, store_name_interned(name));
        ValueTypeMetaData *ptr = meta.get();
        value_storage_.push_back(std::move(meta));
        scalar_cache_[type_key] = ptr;
        register_value_alias(name, ptr);
        return ptr;
    }

    const ValueTypeMetaData *TypeRegistry::synthetic_atomic(std::string_view name,
                                                            size_t size,
                                                            size_t alignment,
                                                            ValueTypeFlags flags)
    {
        if (const auto it = synthetic_scalar_cache_.find(std::string(name)); it != synthetic_scalar_cache_.end())
        {
            return it->second;
        }

        auto meta = std::make_unique<ValueTypeMetaData>(
            ValueTypeKind::Atomic, size, alignment, flags, store_name_interned(name));
        ValueTypeMetaData *ptr = meta.get();
        value_storage_.push_back(std::move(meta));
        synthetic_scalar_cache_[std::string(name)] = ptr;
        register_value_alias(name, ptr);
        return ptr;
    }

    const ValueTypeMetaData *TypeRegistry::tuple(const std::vector<const ValueTypeMetaData *> &element_types)
    {
        const std::string signature = make_tuple_signature(element_types);
        if (const auto it = tuple_cache_.find(signature); it != tuple_cache_.end())
        {
            return it->second;
        }

        std::unique_ptr<ValueFieldMetaData[]> fields =
            element_types.empty() ? nullptr : std::make_unique<ValueFieldMetaData[]>(element_types.size());
        const LayoutInfo layout = compute_tuple_layout(element_types, fields.get());
        ValueFieldMetaData *fields_ptr = fields ? store_value_fields(std::move(fields)) : nullptr;

        auto meta =
            std::make_unique<ValueTypeMetaData>(ValueTypeKind::Tuple, layout.size, layout.alignment, compute_composite_flags(element_types));
        meta->fields = fields_ptr;
        meta->field_count = element_types.size();

        ValueTypeMetaData *ptr = meta.get();
        value_storage_.push_back(std::move(meta));
        tuple_cache_.emplace(signature, ptr);
        return ptr;
    }

    const ValueTypeMetaData *
    TypeRegistry::bundle(const std::vector<std::pair<std::string, const ValueTypeMetaData *>> &fields, std::string_view name)
    {
        const std::string signature = make_bundle_signature(fields);
        if (const auto it = bundle_cache_.find(signature); it != bundle_cache_.end())
        {
            register_value_alias(name, it->second);
            return it->second;
        }

        std::unique_ptr<ValueFieldMetaData[]> stored_fields =
            fields.empty() ? nullptr : std::make_unique<ValueFieldMetaData[]>(fields.size());
        LayoutInfo layout{};
        for (size_t index = 0; index < fields.size(); ++index)
        {
            const auto &[field_name, field_type] = fields[index];
            const size_t field_alignment = field_type ? field_type->alignment : 1;
            layout.size = align_to(layout.size, field_alignment);
            stored_fields[index].name = store_name_interned(field_name);
            stored_fields[index].index = index;
            stored_fields[index].offset = layout.size;
            stored_fields[index].type = field_type;
            layout.size += field_type ? field_type->size : 0;
            if (field_alignment > layout.alignment)
            {
                layout.alignment = field_alignment;
            }
        }
        layout.size = align_to(layout.size, layout.alignment);
        ValueFieldMetaData *fields_ptr = stored_fields ? store_value_fields(std::move(stored_fields)) : nullptr;

        auto meta = std::make_unique<ValueTypeMetaData>(
            ValueTypeKind::Bundle, layout.size, layout.alignment, compute_bundle_flags(fields), store_name_interned(name));
        meta->fields = fields_ptr;
        meta->field_count = fields.size();

        ValueTypeMetaData *ptr = meta.get();
        value_storage_.push_back(std::move(meta));
        bundle_cache_.emplace(signature, ptr);
        register_value_alias(name, ptr);
        return ptr;
    }

    const ValueTypeMetaData *
    TypeRegistry::list(const ValueTypeMetaData *element_type, size_t fixed_size, bool variadic_tuple)
    {
        const std::string signature = make_sized_signature("list", element_type, fixed_size, variadic_tuple);
        if (const auto it = list_cache_.find(signature); it != list_cache_.end())
        {
            return it->second;
        }

        const LayoutInfo layout = compute_fixed_list_layout(element_type, fixed_size);
        auto meta = std::make_unique<ValueTypeMetaData>(
            ValueTypeKind::List,
            fixed_size > 0 ? layout.size : 0,
            fixed_size > 0 ? layout.alignment : 1,
            list_flags(element_type, fixed_size, variadic_tuple));
        meta->element_type = element_type;
        meta->fixed_size = fixed_size;

        ValueTypeMetaData *ptr = meta.get();
        value_storage_.push_back(std::move(meta));
        list_cache_.emplace(signature, ptr);
        return ptr;
    }

    const ValueTypeMetaData *TypeRegistry::set(const ValueTypeMetaData *element_type)
    {
        const std::string signature = make_unary_signature("set", element_type);
        if (const auto it = set_cache_.find(signature); it != set_cache_.end())
        {
            return it->second;
        }

        auto meta = std::make_unique<ValueTypeMetaData>(ValueTypeKind::Set, 0, 1, set_flags(element_type));
        meta->element_type = element_type;

        ValueTypeMetaData *ptr = meta.get();
        value_storage_.push_back(std::move(meta));
        set_cache_.emplace(signature, ptr);
        return ptr;
    }

    const ValueTypeMetaData *TypeRegistry::map(const ValueTypeMetaData *key_type, const ValueTypeMetaData *value_type)
    {
        const std::string signature = make_binary_signature("map", key_type, value_type);
        if (const auto it = map_cache_.find(signature); it != map_cache_.end())
        {
            return it->second;
        }

        auto meta = std::make_unique<ValueTypeMetaData>(ValueTypeKind::Map, 0, 1, map_flags(key_type, value_type));
        meta->key_type = key_type;
        meta->element_type = value_type;

        ValueTypeMetaData *ptr = meta.get();
        value_storage_.push_back(std::move(meta));
        map_cache_.emplace(signature, ptr);
        return ptr;
    }

    const ValueTypeMetaData *TypeRegistry::cyclic_buffer(const ValueTypeMetaData *element_type, size_t capacity)
    {
        const std::string signature = make_sized_signature("cyclic_buffer", element_type, capacity);
        if (const auto it = cyclic_buffer_cache_.find(signature); it != cyclic_buffer_cache_.end())
        {
            return it->second;
        }

        auto meta = std::make_unique<ValueTypeMetaData>(ValueTypeKind::CyclicBuffer, 0, 1, ValueTypeFlags::None);
        meta->element_type = element_type;
        meta->fixed_size = capacity;

        ValueTypeMetaData *ptr = meta.get();
        value_storage_.push_back(std::move(meta));
        cyclic_buffer_cache_.emplace(signature, ptr);
        return ptr;
    }

    const ValueTypeMetaData *TypeRegistry::queue(const ValueTypeMetaData *element_type, size_t max_capacity)
    {
        const std::string signature = make_sized_signature("queue", element_type, max_capacity);
        if (const auto it = queue_cache_.find(signature); it != queue_cache_.end())
        {
            return it->second;
        }

        auto meta = std::make_unique<ValueTypeMetaData>(ValueTypeKind::Queue, 0, 1, ValueTypeFlags::None);
        meta->element_type = element_type;
        meta->fixed_size = max_capacity;

        ValueTypeMetaData *ptr = meta.get();
        value_storage_.push_back(std::move(meta));
        queue_cache_.emplace(signature, ptr);
        return ptr;
    }

    const TSValueTypeMetaData *TypeRegistry::signal()
    {
        if (signal_meta_)
        {
            return signal_meta_;
        }

        auto meta = std::make_unique<TSValueTypeMetaData>(TSValueTypeKind::Signal, register_scalar<bool>("bool"), store_name_interned("SIGNAL"));
        TSValueTypeMetaData *ptr = meta.get();
        ts_storage_.push_back(std::move(meta));
        signal_meta_ = ptr;
        register_ts_alias("SIGNAL", ptr);
        return ptr;
    }

    const TSValueTypeMetaData *TypeRegistry::ts(const ValueTypeMetaData *value_type)
    {
        const std::string signature = make_unary_ts_signature("ts", value_type);
        if (const auto it = ts_cache_.find(signature); it != ts_cache_.end())
        {
            return it->second;
        }

        auto meta = std::make_unique<TSValueTypeMetaData>(TSValueTypeKind::Value, value_type);
        TSValueTypeMetaData *ptr = meta.get();
        ts_storage_.push_back(std::move(meta));
        ts_cache_.emplace(signature, ptr);
        return ptr;
    }

    const TSValueTypeMetaData *TypeRegistry::tss(const ValueTypeMetaData *element_type)
    {
        const std::string signature = make_unary_ts_signature("tss", element_type);
        if (const auto it = tss_cache_.find(signature); it != tss_cache_.end())
        {
            return it->second;
        }

        auto meta = std::make_unique<TSValueTypeMetaData>(TSValueTypeKind::Set, element_type ? set(element_type) : nullptr);
        TSValueTypeMetaData *ptr = meta.get();
        ts_storage_.push_back(std::move(meta));
        tss_cache_.emplace(signature, ptr);
        return ptr;
    }

    const TSValueTypeMetaData *TypeRegistry::tsd(const ValueTypeMetaData *key_type, const TSValueTypeMetaData *value_ts)
    {
        const std::string signature = make_dict_signature(key_type, value_ts);
        if (const auto it = tsd_cache_.find(signature); it != tsd_cache_.end())
        {
            return it->second;
        }

        auto meta = std::make_unique<TSValueTypeMetaData>(
            TSValueTypeKind::Dict, key_type && value_ts ? map(key_type, value_ts->value_type) : nullptr);
        meta->set_dict(key_type, value_ts);

        TSValueTypeMetaData *ptr = meta.get();
        ts_storage_.push_back(std::move(meta));
        tsd_cache_.emplace(signature, ptr);
        return ptr;
    }

    const TSValueTypeMetaData *TypeRegistry::tsl(const TSValueTypeMetaData *element_ts, size_t fixed_size)
    {
        const std::string signature = make_sized_ts_signature("tsl", element_ts, fixed_size);
        if (const auto it = tsl_cache_.find(signature); it != tsl_cache_.end())
        {
            return it->second;
        }

        auto meta = std::make_unique<TSValueTypeMetaData>(
            TSValueTypeKind::List, element_ts && element_ts->value_type ? list(element_ts->value_type, fixed_size) : nullptr);
        meta->set_list(element_ts, fixed_size);

        TSValueTypeMetaData *ptr = meta.get();
        ts_storage_.push_back(std::move(meta));
        tsl_cache_.emplace(signature, ptr);
        return ptr;
    }

    const TSValueTypeMetaData *TypeRegistry::tsw(const ValueTypeMetaData *value_type, size_t period, size_t min_period)
    {
        const std::string signature = make_window_signature(
            value_type, false, static_cast<int64_t>(period), static_cast<int64_t>(min_period));
        if (const auto it = tsw_cache_.find(signature); it != tsw_cache_.end())
        {
            return it->second;
        }

        auto meta = std::make_unique<TSValueTypeMetaData>(TSValueTypeKind::Window, value_type);
        meta->set_tick_window(period, min_period);

        TSValueTypeMetaData *ptr = meta.get();
        ts_storage_.push_back(std::move(meta));
        tsw_cache_.emplace(signature, ptr);
        return ptr;
    }

    const TSValueTypeMetaData *TypeRegistry::tsw_duration(const ValueTypeMetaData *value_type,
                                                          engine_time_delta_t time_range,
                                                          engine_time_delta_t min_time_range)
    {
        const std::string signature =
            make_window_signature(value_type, true, time_range.count(), min_time_range.count());
        if (const auto it = tsw_cache_.find(signature); it != tsw_cache_.end())
        {
            return it->second;
        }

        auto meta = std::make_unique<TSValueTypeMetaData>(TSValueTypeKind::Window, value_type);
        meta->set_duration_window(time_range, min_time_range);

        TSValueTypeMetaData *ptr = meta.get();
        ts_storage_.push_back(std::move(meta));
        tsw_cache_.emplace(signature, ptr);
        return ptr;
    }

    const TSValueTypeMetaData *
    TypeRegistry::tsb(const std::vector<std::pair<std::string, const TSValueTypeMetaData *>> &fields, std::string_view name)
    {
        const std::string signature = make_ts_bundle_signature(fields);
        if (const auto it = tsb_cache_.find(signature); it != tsb_cache_.end())
        {
            register_ts_alias(name, it->second);
            return it->second;
        }

        std::vector<std::pair<std::string, const ValueTypeMetaData *>> value_fields;
        value_fields.reserve(fields.size());
        for (const auto &[field_name, ts_type] : fields)
        {
            value_fields.emplace_back(field_name, ts_type ? ts_type->value_type : nullptr);
        }

        std::unique_ptr<TSFieldMetaData[]> stored_fields =
            fields.empty() ? nullptr : std::make_unique<TSFieldMetaData[]>(fields.size());
        for (size_t index = 0; index < fields.size(); ++index)
        {
            stored_fields[index].name = store_name_interned(fields[index].first);
            stored_fields[index].index = index;
            stored_fields[index].type = fields[index].second;
        }
        TSFieldMetaData *fields_ptr = stored_fields ? store_ts_fields(std::move(stored_fields)) : nullptr;

        auto meta = std::make_unique<TSValueTypeMetaData>(
            TSValueTypeKind::Bundle, bundle(value_fields, name), store_name_interned(name));
        meta->set_bundle(fields_ptr, fields.size(), store_name_interned(name));

        TSValueTypeMetaData *ptr = meta.get();
        ts_storage_.push_back(std::move(meta));
        tsb_cache_.emplace(signature, ptr);
        register_ts_alias(name, ptr);
        return ptr;
    }

    const TSValueTypeMetaData *TypeRegistry::ref(const TSValueTypeMetaData *referenced_ts)
    {
        const std::string signature = make_unary_ts_signature("ref", referenced_ts);
        if (const auto it = ref_cache_.find(signature); it != ref_cache_.end())
        {
            return it->second;
        }

        if (!time_series_reference_meta_)
        {
            time_series_reference_meta_ =
                synthetic_atomic("TimeSeriesReference", 0, 1, ValueTypeFlags::Hashable | ValueTypeFlags::Equatable);
        }

        auto meta = std::make_unique<TSValueTypeMetaData>(TSValueTypeKind::Reference, time_series_reference_meta_);
        meta->set_reference(referenced_ts);

        TSValueTypeMetaData *ptr = meta.get();
        ts_storage_.push_back(std::move(meta));
        ref_cache_.emplace(signature, ptr);
        return ptr;
    }

    bool TypeRegistry::contains_ref(const TSValueTypeMetaData *meta)
    {
        if (!meta)
        {
            return false;
        }

        switch (meta->kind)
        {
            case TSValueTypeKind::Reference: return true;
            case TSValueTypeKind::Bundle:
                for (size_t index = 0; index < meta->field_count(); ++index)
                {
                    if (contains_ref(meta->fields()[index].type))
                    {
                        return true;
                    }
                }
                return false;
            case TSValueTypeKind::Dict:
            case TSValueTypeKind::List: return contains_ref(meta->element_ts());
            default: return false;
        }
    }

    const TSValueTypeMetaData *TypeRegistry::dereference(const TSValueTypeMetaData *meta)
    {
        if (!meta)
        {
            return nullptr;
        }

        if (const auto it = deref_cache_.find(meta); it != deref_cache_.end())
        {
            return it->second;
        }

        const TSValueTypeMetaData *result = meta;
        switch (meta->kind)
        {
            case TSValueTypeKind::Reference:
                result = dereference(meta->referenced_ts());
                break;

            case TSValueTypeKind::Bundle:
            {
                if (!contains_ref(meta))
                {
                    result = meta;
                    break;
                }

                std::vector<std::pair<std::string, const TSValueTypeMetaData *>> deref_fields;
                deref_fields.reserve(meta->field_count());
                for (size_t index = 0; index < meta->field_count(); ++index)
                {
                    deref_fields.emplace_back(meta->fields()[index].name, dereference(meta->fields()[index].type));
                }

                std::string deref_name = meta->bundle_name() ? std::string(meta->bundle_name()) : std::string{};
                if (!deref_name.empty())
                {
                    deref_name += "_deref";
                }
                result = tsb(deref_fields, deref_name);
                break;
            }

            case TSValueTypeKind::List:
            {
                const TSValueTypeMetaData *element = dereference(meta->element_ts());
                result = element == meta->element_ts() ? meta : tsl(element, meta->fixed_size());
                break;
            }

            case TSValueTypeKind::Dict:
            {
                const TSValueTypeMetaData *value_ts = dereference(meta->element_ts());
                result = value_ts == meta->element_ts() ? meta : tsd(meta->key_type(), value_ts);
                break;
            }

            default:
                result = meta;
                break;
        }

        deref_cache_[meta] = result;
        return result;
    }
}  // namespace hgraph::v2
