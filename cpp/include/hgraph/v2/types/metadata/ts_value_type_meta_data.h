//
// Created by Howard Henson on 20/04/2026.
//

#ifndef HGRAPH_CPP_ROOT_TS_VALUE_TYPE_META_DATA_H
#define HGRAPH_CPP_ROOT_TS_VALUE_TYPE_META_DATA_H

#include <hgraph/util/date_time.h>
#include <hgraph/v2/types/metadata/type_meta_data.h>
#include <hgraph/v2/types/metadata/value_type_meta_data.h>

#include <cstddef>
#include <cstdint>

namespace hgraph::v2
{
    struct TSValueTypeMetaData;

    enum class TSValueTypeKind : uint8_t
    {
        Value,
        Set,
        Dict,
        List,
        Window,
        Bundle,
        Reference,
        Signal,
    };

    struct TSFieldMetaData
    {
        const char *name{nullptr};
        size_t index{0};
        const TSValueTypeMetaData *type{nullptr};
    };

    struct TSValueTypeMetaData final : TypeMetaData
    {
        union WindowParams
        {
            struct
            {
                size_t period;
                size_t min_period;
            } tick;
            struct
            {
                engine_time_delta_t time_range;
                engine_time_delta_t min_time_range;
            } duration;

            constexpr WindowParams()
                : tick{0, 0}
            {
            }
        };

        struct EmptyData
        {
        };

        struct DictData
        {
            const ValueTypeMetaData *key_type{nullptr};
            const TSValueTypeMetaData *value_ts{nullptr};
        };

        struct ListData
        {
            const TSValueTypeMetaData *element_ts{nullptr};
            size_t fixed_size{0};
        };

        struct WindowData
        {
            bool is_duration_based{false};
            WindowParams window{};
        };

        struct BundleData
        {
            const TSFieldMetaData *fields{nullptr};
            size_t field_count{0};
            const char *bundle_name{nullptr};
        };

        struct ReferenceData
        {
            const TSValueTypeMetaData *referenced_ts{nullptr};
        };

        union KindData
        {
            EmptyData empty;
            DictData dict;
            ListData list;
            WindowData window;
            BundleData bundle;
            ReferenceData reference;

            constexpr KindData()
                : empty{}
            {
            }
        };

        constexpr TSValueTypeMetaData() noexcept
            : TypeMetaData(MetaCategory::TimeSeries)
        {
        }

        constexpr TSValueTypeMetaData(TSValueTypeKind kind_,
                                      const ValueTypeMetaData *value_type_ = nullptr,
                                      const char *display_name_ = nullptr) noexcept
            : TypeMetaData(MetaCategory::TimeSeries, display_name_)
            , kind(kind_)
            , value_type(value_type_)
        {
        }

        TSValueTypeKind kind{TSValueTypeKind::Signal};
        const ValueTypeMetaData *value_type{nullptr};
        KindData data{};

        constexpr void set_dict(const ValueTypeMetaData *key_type, const TSValueTypeMetaData *value_ts) noexcept
        {
            data.dict = DictData{key_type, value_ts};
        }

        constexpr void set_list(const TSValueTypeMetaData *element_ts, size_t fixed_size) noexcept
        {
            data.list = ListData{element_ts, fixed_size};
        }

        constexpr void set_tick_window(size_t period, size_t min_period) noexcept
        {
            data.window = WindowData{false, WindowParams{}};
            data.window.window.tick.period = period;
            data.window.window.tick.min_period = min_period;
        }

        constexpr void set_duration_window(engine_time_delta_t time_range,
                                           engine_time_delta_t min_time_range) noexcept
        {
            data.window = WindowData{true, WindowParams{}};
            data.window.window.duration.time_range = time_range;
            data.window.window.duration.min_time_range = min_time_range;
        }

        constexpr void set_bundle(const TSFieldMetaData *fields, size_t field_count, const char *bundle_name) noexcept
        {
            data.bundle = BundleData{fields, field_count, bundle_name};
        }

        constexpr void set_reference(const TSValueTypeMetaData *referenced_ts) noexcept
        {
            data.reference = ReferenceData{referenced_ts};
        }

        [[nodiscard]] constexpr const ValueTypeMetaData *key_type() const noexcept
        {
            return kind == TSValueTypeKind::Dict ? data.dict.key_type : nullptr;
        }

        [[nodiscard]] constexpr const TSValueTypeMetaData *element_ts() const noexcept
        {
            switch (kind)
            {
                case TSValueTypeKind::Dict: return data.dict.value_ts;
                case TSValueTypeKind::List: return data.list.element_ts;
                case TSValueTypeKind::Reference: return data.reference.referenced_ts;
                default: return nullptr;
            }
        }

        [[nodiscard]] constexpr size_t fixed_size() const noexcept
        {
            return kind == TSValueTypeKind::List ? data.list.fixed_size : 0;
        }

        [[nodiscard]] constexpr bool is_duration_based() const noexcept
        {
            return kind == TSValueTypeKind::Window && data.window.is_duration_based;
        }

        [[nodiscard]] constexpr size_t period() const noexcept
        {
            return kind == TSValueTypeKind::Window && !data.window.is_duration_based ? data.window.window.tick.period : 0;
        }

        [[nodiscard]] constexpr size_t min_period() const noexcept
        {
            return kind == TSValueTypeKind::Window && !data.window.is_duration_based
                       ? data.window.window.tick.min_period
                       : 0;
        }

        [[nodiscard]] constexpr engine_time_delta_t time_range() const noexcept
        {
            return kind == TSValueTypeKind::Window && data.window.is_duration_based
                       ? data.window.window.duration.time_range
                       : engine_time_delta_t{0};
        }

        [[nodiscard]] constexpr engine_time_delta_t min_time_range() const noexcept
        {
            return kind == TSValueTypeKind::Window && data.window.is_duration_based
                       ? data.window.window.duration.min_time_range
                       : engine_time_delta_t{0};
        }

        [[nodiscard]] constexpr const TSFieldMetaData *fields() const noexcept
        {
            return kind == TSValueTypeKind::Bundle ? data.bundle.fields : nullptr;
        }

        [[nodiscard]] constexpr size_t field_count() const noexcept
        {
            return kind == TSValueTypeKind::Bundle ? data.bundle.field_count : 0;
        }

        [[nodiscard]] constexpr const char *bundle_name() const noexcept
        {
            return kind == TSValueTypeKind::Bundle ? data.bundle.bundle_name : nullptr;
        }

        [[nodiscard]] constexpr const TSValueTypeMetaData *referenced_ts() const noexcept
        {
            return kind == TSValueTypeKind::Reference ? data.reference.referenced_ts : nullptr;
        }

        [[nodiscard]] constexpr bool is_collection() const noexcept
        {
            return kind == TSValueTypeKind::Set || kind == TSValueTypeKind::Dict || kind == TSValueTypeKind::List ||
                   kind == TSValueTypeKind::Bundle;
        }

        [[nodiscard]] constexpr bool is_scalar_ts() const noexcept
        {
            return kind == TSValueTypeKind::Value || kind == TSValueTypeKind::Window || kind == TSValueTypeKind::Signal;
        }
    };
}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_TS_VALUE_TYPE_META_DATA_H
