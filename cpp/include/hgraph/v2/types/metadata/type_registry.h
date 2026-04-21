//
// Created by Howard Henson on 20/04/2026.
//

#ifndef HGRAPH_CPP_ROOT_TYPE_REGISTRY_H
#define HGRAPH_CPP_ROOT_TYPE_REGISTRY_H

#include <hgraph/v2/types/metadata/ts_value_type_meta_data.h>
#include <hgraph/v2/types/metadata/value_type_meta_data.h>

#include <memory>
#include <string>
#include <string_view>
#include <typeindex>
#include <unordered_map>
#include <vector>

namespace hgraph::v2
{
    /**
     * Interns v2 value and time-series schemas.
     *
     * The registry is the source of truth for schema identity. Equivalent
     * schemas resolve to the same pointer, which makes pointer equality a safe
     * fast path for graph building and runtime dispatch.
     */
    class TypeRegistry
    {
    public:
        static TypeRegistry &instance();

        TypeRegistry(const TypeRegistry &) = delete;
        TypeRegistry &operator=(const TypeRegistry &) = delete;
        TypeRegistry(TypeRegistry &&) = delete;
        TypeRegistry &operator=(TypeRegistry &&) = delete;

        template <typename T>
        const ValueTypeMetaData *register_scalar(std::string_view name = {},
                                                 ValueTypeFlags extra_flags = ValueTypeFlags::None);

        [[nodiscard]] const ValueTypeMetaData *value_type(std::string_view name) const;
        [[nodiscard]] const TSValueTypeMetaData *time_series_type(std::string_view name) const;

        const ValueTypeMetaData *tuple(const std::vector<const ValueTypeMetaData *> &element_types);
        const ValueTypeMetaData *bundle(const std::vector<std::pair<std::string, const ValueTypeMetaData *>> &fields,
                                        std::string_view name = {});
        const ValueTypeMetaData *list(const ValueTypeMetaData *element_type,
                                      size_t fixed_size = 0,
                                      bool variadic_tuple = false);
        const ValueTypeMetaData *set(const ValueTypeMetaData *element_type);
        const ValueTypeMetaData *map(const ValueTypeMetaData *key_type, const ValueTypeMetaData *value_type);
        const ValueTypeMetaData *cyclic_buffer(const ValueTypeMetaData *element_type, size_t capacity);
        const ValueTypeMetaData *queue(const ValueTypeMetaData *element_type, size_t max_capacity = 0);

        const TSValueTypeMetaData *signal();
        const TSValueTypeMetaData *ts(const ValueTypeMetaData *value_type);
        const TSValueTypeMetaData *tss(const ValueTypeMetaData *element_type);
        const TSValueTypeMetaData *tsd(const ValueTypeMetaData *key_type, const TSValueTypeMetaData *value_ts);
        const TSValueTypeMetaData *tsl(const TSValueTypeMetaData *element_ts, size_t fixed_size = 0);
        const TSValueTypeMetaData *tsw(const ValueTypeMetaData *value_type, size_t period, size_t min_period = 0);
        const TSValueTypeMetaData *tsw_duration(const ValueTypeMetaData *value_type,
                                                engine_time_delta_t time_range,
                                                engine_time_delta_t min_time_range = engine_time_delta_t{0});
        const TSValueTypeMetaData *tsb(const std::vector<std::pair<std::string, const TSValueTypeMetaData *>> &fields,
                                       std::string_view name = {});
        const TSValueTypeMetaData *ref(const TSValueTypeMetaData *referenced_ts);

        [[nodiscard]] static bool contains_ref(const TSValueTypeMetaData *meta);
        const TSValueTypeMetaData *dereference(const TSValueTypeMetaData *meta);

    private:
        TypeRegistry() = default;

        const char *store_name(std::string_view name);
        const char *store_name_interned(std::string_view name);
        ValueFieldMetaData *store_value_fields(std::unique_ptr<ValueFieldMetaData[]> fields);
        TSFieldMetaData *store_ts_fields(std::unique_ptr<TSFieldMetaData[]> fields);

        const ValueTypeMetaData *register_scalar_impl(std::type_index type_key,
                                                      std::string_view name,
                                                      size_t size,
                                                      size_t alignment,
                                                      ValueTypeFlags flags);
        const ValueTypeMetaData *synthetic_atomic(std::string_view name,
                                                  size_t size,
                                                  size_t alignment,
                                                  ValueTypeFlags flags);

        void register_value_alias(std::string_view name, const ValueTypeMetaData *meta);
        void register_ts_alias(std::string_view name, const TSValueTypeMetaData *meta);

        std::vector<std::unique_ptr<std::string>> name_storage_;
        std::vector<std::unique_ptr<ValueFieldMetaData[]>> value_field_storage_;
        std::vector<std::unique_ptr<TSFieldMetaData[]>> ts_field_storage_;
        std::vector<std::unique_ptr<ValueTypeMetaData>> value_storage_;
        std::vector<std::unique_ptr<TSValueTypeMetaData>> ts_storage_;

        std::unordered_map<std::type_index, const ValueTypeMetaData *> scalar_cache_;
        std::unordered_map<std::string, const ValueTypeMetaData *> synthetic_scalar_cache_;
        std::unordered_map<std::string, const ValueTypeMetaData *> value_name_cache_;
        std::unordered_map<std::string, const TSValueTypeMetaData *> ts_name_cache_;

        std::unordered_map<std::string, const ValueTypeMetaData *> tuple_cache_;
        std::unordered_map<std::string, const ValueTypeMetaData *> bundle_cache_;
        std::unordered_map<std::string, const ValueTypeMetaData *> list_cache_;
        std::unordered_map<std::string, const ValueTypeMetaData *> set_cache_;
        std::unordered_map<std::string, const ValueTypeMetaData *> map_cache_;
        std::unordered_map<std::string, const ValueTypeMetaData *> cyclic_buffer_cache_;
        std::unordered_map<std::string, const ValueTypeMetaData *> queue_cache_;

        std::unordered_map<std::string, const TSValueTypeMetaData *> ts_cache_;
        std::unordered_map<std::string, const TSValueTypeMetaData *> tss_cache_;
        std::unordered_map<std::string, const TSValueTypeMetaData *> tsd_cache_;
        std::unordered_map<std::string, const TSValueTypeMetaData *> tsl_cache_;
        std::unordered_map<std::string, const TSValueTypeMetaData *> tsw_cache_;
        std::unordered_map<std::string, const TSValueTypeMetaData *> tsb_cache_;
        std::unordered_map<std::string, const TSValueTypeMetaData *> ref_cache_;
        std::unordered_map<const TSValueTypeMetaData *, const TSValueTypeMetaData *> deref_cache_;

        const TSValueTypeMetaData *signal_meta_{nullptr};
        const ValueTypeMetaData *time_series_reference_meta_{nullptr};
    };

    template <typename T>
    const ValueTypeMetaData *TypeRegistry::register_scalar(std::string_view name, ValueTypeFlags extra_flags)
    {
        return register_scalar_impl(
            std::type_index(typeid(T)), name, sizeof(T), alignof(T), compute_scalar_flags<T>() | extra_flags);
    }
}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_TYPE_REGISTRY_H
