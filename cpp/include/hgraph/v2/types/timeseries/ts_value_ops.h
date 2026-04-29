#ifndef HGRAPH_CPP_ROOT_TS_VALUE_OPS_H
#define HGRAPH_CPP_ROOT_TS_VALUE_OPS_H

#include <hgraph/v2/types/metadata/ts_value_type_meta_data.h>
#include <hgraph/v2/types/timeseries/ts_state.h>
#include <hgraph/v2/types/utils/intern_table.h>
#include <hgraph/v2/types/value/value_ops.h>

#include <memory>
#include <stdexcept>
#include <string_view>
#include <utility>
#include <vector>

namespace hgraph::v2
{
    struct TsValueOps;
    using TsValueTypeBinding = TypeBinding<TSValueTypeMetaData, TsValueOps>;

    struct TsBundleFieldOps
    {
        const char               *name{nullptr};
        size_t                    index{0};
        size_t                    state_offset{0};
        const TsValueTypeBinding *binding{nullptr};

        [[nodiscard]] bool operator==(const TsBundleFieldOps &) const = default;

        [[nodiscard]] const TsValueTypeBinding &checked_binding() const {
            if (binding != nullptr) { return *binding; }
            throw std::logic_error("TsBundleFieldOps is missing a child TS binding");
        }
    };

    struct TsBundleOps
    {
        std::vector<TsBundleFieldOps> fields{};
        const char                   *bundle_name{nullptr};

        [[nodiscard]] bool operator==(const TsBundleOps &) const = default;

        [[nodiscard]] size_t field_count() const noexcept { return fields.size(); }

        [[nodiscard]] const TsBundleFieldOps *field(size_t index) const noexcept {
            return index < fields.size() ? &fields[index] : nullptr;
        }

        [[nodiscard]] const TsBundleFieldOps &checked_field(size_t index) const {
            if (const auto *result = field(index); result != nullptr) { return *result; }
            throw std::out_of_range("TsBundleOps field index out of range");
        }

        [[nodiscard]] const TsBundleFieldOps *field(std::string_view name) const noexcept {
            for (const auto &field_ops : fields) {
                if (field_ops.name != nullptr && name == field_ops.name) { return &field_ops; }
            }
            return nullptr;
        }
    };

    struct TsListOps
    {
        const TsValueTypeBinding *element_binding{nullptr};
        size_t                    fixed_size{0};
        size_t                    element_state_stride{0};

        [[nodiscard]] bool operator==(const TsListOps &) const = default;

        [[nodiscard]] const TsValueTypeBinding &checked_element_binding() const {
            if (element_binding != nullptr) { return *element_binding; }
            throw std::logic_error("TsListOps is missing an element TS binding");
        }

        [[nodiscard]] bool is_fixed() const noexcept { return fixed_size != 0; }
        [[nodiscard]] bool has_inline_child_states() const noexcept { return element_state_stride != 0; }
    };

    struct TsSetOps
    {
        const ValueTypeMetaData *element_type{nullptr};

        [[nodiscard]] bool operator==(const TsSetOps &) const = default;
    };

    struct TsDictOps
    {
        const ValueTypeMetaData  *key_type{nullptr};
        const TsValueTypeBinding *value_binding{nullptr};

        [[nodiscard]] bool operator==(const TsDictOps &) const = default;

        [[nodiscard]] const TsValueTypeBinding &checked_value_binding() const {
            if (value_binding != nullptr) { return *value_binding; }
            throw std::logic_error("TsDictOps is missing a child TS binding");
        }
    };

    struct TsWindowOps
    {
        bool                     is_duration_based{false};
        size_t                   period{0};
        size_t                   min_period{0};
        engine_time_delta_t      time_range{0};
        engine_time_delta_t      min_time_range{0};
        const ValueTypeMetaData *element_type{nullptr};

        [[nodiscard]] bool operator==(const TsWindowOps &) const = default;
    };

    /**
     * Shared TS-schema operations and projection metadata.
     *
     * The first TS endpoint pass reuses the underlying value storage plan
     * directly. The TS binding therefore needs one extra piece of shared
     * metadata beyond the plan itself: the value-layer binding used to project
     * the same memory back to `ValueView`.
     *
     * Input/output-specific TS ops can extend this base record later without
     * changing the core idea that the bound handle carries the TS identity.
     */
    struct TsValueOps
    {
        union SpecificOps {
            TsBundleOps bundle;
            TsListOps   list;
            TsSetOps    set;
            TsDictOps   dict;
            TsWindowOps window;

            constexpr SpecificOps() noexcept {}
            ~SpecificOps() {}
        };

        const ValueTypeBinding *value_binding{nullptr};
        TSValueTypeKind         kind{TSValueTypeKind::Signal};
        SpecificOps             specific{};

        TsValueOps() noexcept = default;

        TsValueOps(const TsValueOps &other) : value_binding(other.value_binding), kind(other.kind) { copy_specific_from(other); }

        TsValueOps(TsValueOps &&other) noexcept : value_binding(other.value_binding), kind(other.kind) {
            move_specific_from(std::move(other));
        }

        TsValueOps &operator=(const TsValueOps &other) {
            if (this == &other) { return *this; }

            TsValueOps temp(other);
            destroy_specific();
            value_binding = temp.value_binding;
            kind          = temp.kind;
            move_specific_from(std::move(temp));
            return *this;
        }

        TsValueOps &operator=(TsValueOps &&other) noexcept {
            if (this == &other) { return *this; }

            TsValueOps temp(std::move(other));
            destroy_specific();
            value_binding = temp.value_binding;
            kind          = temp.kind;
            move_specific_from(std::move(temp));
            return *this;
        }

        ~TsValueOps() { destroy_specific(); }

        [[nodiscard]] constexpr bool valid() const noexcept { return value_binding != nullptr; }

        [[nodiscard]] const ValueTypeBinding &checked_value_binding() const {
            if (value_binding != nullptr) { return *value_binding; }
            throw std::logic_error("TsValueOps is missing an underlying value binding");
        }

        [[nodiscard]] const ValueTypeMetaData *value_type() const noexcept {
            return value_binding != nullptr ? value_binding->type_meta : nullptr;
        }

        [[nodiscard]] bool is_tsb() const noexcept { return kind == TSValueTypeKind::Bundle; }
        [[nodiscard]] bool is_tsl() const noexcept { return kind == TSValueTypeKind::List; }
        [[nodiscard]] bool is_tss() const noexcept { return kind == TSValueTypeKind::Set; }
        [[nodiscard]] bool is_tsd() const noexcept { return kind == TSValueTypeKind::Dict; }
        [[nodiscard]] bool is_tsw() const noexcept { return kind == TSValueTypeKind::Window; }

        [[nodiscard]] const TsBundleOps &checked_bundle_ops() const {
            if (is_tsb()) { return specific.bundle; }
            throw std::logic_error("TsValueOps is missing bundle operations");
        }

        [[nodiscard]] const TsListOps &checked_list_ops() const {
            if (is_tsl()) { return specific.list; }
            throw std::logic_error("TsValueOps is missing list operations");
        }

        [[nodiscard]] const TsSetOps &checked_set_ops() const {
            if (is_tss()) { return specific.set; }
            throw std::logic_error("TsValueOps is missing set operations");
        }

        [[nodiscard]] const TsDictOps &checked_dict_ops() const {
            if (is_tsd()) { return specific.dict; }
            throw std::logic_error("TsValueOps is missing dict operations");
        }

        [[nodiscard]] const TsWindowOps &checked_window_ops() const {
            if (is_tsw()) { return specific.window; }
            throw std::logic_error("TsValueOps is missing window operations");
        }

      private:
        void destroy_specific() noexcept {
            switch (kind) {
                case TSValueTypeKind::Bundle: std::destroy_at(std::addressof(specific.bundle)); break;
                case TSValueTypeKind::List: std::destroy_at(std::addressof(specific.list)); break;
                case TSValueTypeKind::Set: std::destroy_at(std::addressof(specific.set)); break;
                case TSValueTypeKind::Dict: std::destroy_at(std::addressof(specific.dict)); break;
                case TSValueTypeKind::Window: std::destroy_at(std::addressof(specific.window)); break;
                case TSValueTypeKind::Value:
                case TSValueTypeKind::Reference:
                case TSValueTypeKind::Signal: break;
            }
        }

        void copy_specific_from(const TsValueOps &other) {
            switch (other.kind) {
                case TSValueTypeKind::Bundle: std::construct_at(std::addressof(specific.bundle), other.specific.bundle); break;
                case TSValueTypeKind::List: std::construct_at(std::addressof(specific.list), other.specific.list); break;
                case TSValueTypeKind::Set: std::construct_at(std::addressof(specific.set), other.specific.set); break;
                case TSValueTypeKind::Dict: std::construct_at(std::addressof(specific.dict), other.specific.dict); break;
                case TSValueTypeKind::Window: std::construct_at(std::addressof(specific.window), other.specific.window); break;
                case TSValueTypeKind::Value:
                case TSValueTypeKind::Reference:
                case TSValueTypeKind::Signal: break;
            }
        }

        void move_specific_from(TsValueOps &&other) noexcept {
            switch (other.kind) {
                case TSValueTypeKind::Bundle:
                    std::construct_at(std::addressof(specific.bundle), std::move(other.specific.bundle));
                    break;
                case TSValueTypeKind::List: std::construct_at(std::addressof(specific.list), std::move(other.specific.list)); break;
                case TSValueTypeKind::Set: std::construct_at(std::addressof(specific.set), std::move(other.specific.set)); break;
                case TSValueTypeKind::Dict: std::construct_at(std::addressof(specific.dict), std::move(other.specific.dict)); break;
                case TSValueTypeKind::Window:
                    std::construct_at(std::addressof(specific.window), std::move(other.specific.window));
                    break;
                case TSValueTypeKind::Value:
                case TSValueTypeKind::Reference:
                case TSValueTypeKind::Signal: break;
            }
        }
    };

    namespace detail
    {
        [[nodiscard]] const TsValueTypeBinding &checked_ts_binding(const TSValueTypeMetaData *type);

        struct TsValueOpsKey
        {
            const TSValueTypeMetaData *type{nullptr};
            const ValueTypeBinding    *value_binding{nullptr};

            [[nodiscard]] bool operator==(const TsValueOpsKey &) const noexcept = default;
        };

        struct TsValueOpsKeyHash
        {
            [[nodiscard]] size_t operator()(const TsValueOpsKey &key) const noexcept {
                size_t seed = std::hash<const TSValueTypeMetaData *>{}(key.type);
                seed ^=
                    std::hash<const ValueTypeBinding *>{}(key.value_binding) + 0x9e3779b97f4a7c15ULL + (seed << 6U) + (seed >> 2U);
                return seed;
            }
        };

        [[nodiscard]] inline const TsBundleOps *ts_bundle_ops_for(const TSValueTypeMetaData &type) {
            if (type.kind != TSValueTypeKind::Bundle) { return nullptr; }

            static InternTable<const TSValueTypeMetaData *, TsBundleOps> registry;
            return &registry.intern(&type, [&]() {
                const MemoryUtils::StoragePlan &kind_plan = detail::ts_kind_state_plan(type);
                TsBundleOps                     ops;
                ops.bundle_name = type.bundle_name();
                ops.fields.reserve(type.field_count());
                const TSFieldMetaData *fields = type.fields();
                for (size_t index = 0; index < type.field_count(); ++index) {
                    ops.fields.push_back(TsBundleFieldOps{
                        .name         = fields[index].name,
                        .index        = fields[index].index,
                        .state_offset = kind_plan.component(index).offset,
                        .binding      = &checked_ts_binding(fields[index].type),
                    });
                }
                return ops;
            });
        }

        [[nodiscard]] inline const TsListOps *ts_list_ops_for(const TSValueTypeMetaData &type) {
            if (type.kind != TSValueTypeKind::List) { return nullptr; }

            static InternTable<const TSValueTypeMetaData *, TsListOps> registry;
            return &registry.intern(&type, [&]() {
                const MemoryUtils::StoragePlan &kind_plan = detail::ts_kind_state_plan(type);
                return TsListOps{
                    .element_binding      = &checked_ts_binding(type.element_ts()),
                    .fixed_size           = type.fixed_size(),
                    .element_state_stride = kind_plan.is_array() ? kind_plan.array_stride() : 0,
                };
            });
        }

        [[nodiscard]] inline const TsSetOps *ts_set_ops_for(const TSValueTypeMetaData &type) {
            if (type.kind != TSValueTypeKind::Set) { return nullptr; }

            static InternTable<const TSValueTypeMetaData *, TsSetOps> registry;
            return &registry.emplace(&type, type.value_type != nullptr ? type.value_type->element_type : nullptr);
        }

        [[nodiscard]] inline const TsDictOps *ts_dict_ops_for(const TSValueTypeMetaData &type) {
            if (type.kind != TSValueTypeKind::Dict) { return nullptr; }

            static InternTable<const TSValueTypeMetaData *, TsDictOps> registry;
            return &registry.intern(&type, [&]() {
                return TsDictOps{
                    .key_type      = type.key_type(),
                    .value_binding = &checked_ts_binding(type.element_ts()),
                };
            });
        }

        [[nodiscard]] inline const TsWindowOps *ts_window_ops_for(const TSValueTypeMetaData &type) {
            if (type.kind != TSValueTypeKind::Window) { return nullptr; }

            static InternTable<const TSValueTypeMetaData *, TsWindowOps> registry;
            return &registry.emplace(&type, TsWindowOps{
                                                .is_duration_based = type.is_duration_based(),
                                                .period            = type.period(),
                                                .min_period        = type.min_period(),
                                                .time_range        = type.time_range(),
                                                .min_time_range    = type.min_time_range(),
                                                .element_type      = type.value_type,
                                            });
        }

        [[nodiscard]] inline InternTable<TsValueOpsKey, TsValueOps, TsValueOpsKeyHash> &ts_value_ops_registry() noexcept {
            static InternTable<TsValueOpsKey, TsValueOps, TsValueOpsKeyHash> registry;
            return registry;
        }
    }  // namespace detail

    [[nodiscard]] inline const TsValueOps &ts_value_ops(const TSValueTypeMetaData &type, const ValueTypeBinding &value_binding) {
        return detail::ts_value_ops_registry().intern(
            detail::TsValueOpsKey{
                .type          = &type,
                .value_binding = &value_binding,
            },
            [&]() {
                TsValueOps ops;
                ops.value_binding = &value_binding;
                switch (type.kind) {
                    case TSValueTypeKind::Bundle:
                        std::construct_at(std::addressof(ops.specific.bundle), *detail::ts_bundle_ops_for(type));
                        ops.kind = TSValueTypeKind::Bundle;
                        break;
                    case TSValueTypeKind::List:
                        std::construct_at(std::addressof(ops.specific.list), *detail::ts_list_ops_for(type));
                        ops.kind = TSValueTypeKind::List;
                        break;
                    case TSValueTypeKind::Set:
                        std::construct_at(std::addressof(ops.specific.set), *detail::ts_set_ops_for(type));
                        ops.kind = TSValueTypeKind::Set;
                        break;
                    case TSValueTypeKind::Dict:
                        std::construct_at(std::addressof(ops.specific.dict), *detail::ts_dict_ops_for(type));
                        ops.kind = TSValueTypeKind::Dict;
                        break;
                    case TSValueTypeKind::Window:
                        std::construct_at(std::addressof(ops.specific.window), *detail::ts_window_ops_for(type));
                        ops.kind = TSValueTypeKind::Window;
                        break;
                    case TSValueTypeKind::Value: ops.kind = TSValueTypeKind::Value; break;
                    case TSValueTypeKind::Reference: ops.kind = TSValueTypeKind::Reference; break;
                    case TSValueTypeKind::Signal: break;
                }
                return ops;
            });
    }
}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_TS_VALUE_OPS_H
