#ifndef HGL_ORDINARY_VALUES_H
#define HGL_ORDINARY_VALUES_H

#include <hgl/execution_error.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/temporal.h>
#include <hgraph/types/metadata/type_realization.h>
#include <hgraph/types/value/mutable_container_ops.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_input.h>
#include <ankerl/unordered_dense.h>

#include <cstdint>
#include <charconv>
#include <cmath>
#include <limits>
#include <functional>
#include <span>
#include <stdexcept>
#include <utility>
#include <unordered_set>

namespace hgl::ordinary
{
    // Payload consumption is separate from retaining a typed observation.
    // Construct the diagnostic only on failure; the presence guard allocates
    // nothing on success and preserves the payload's existing copy semantics.
    template <typename T> [[nodiscard]] T required_scalar(const hgraph::ValueView &value) {
        if (!value.has_value()) { throw hgl::ExecutionError{"value.unset_read", "ordinary scalar value is absent"}; }
        return value.as<T>();
    }
    // Only explicit publication predicates use this marker. Allocation and
    // storage failures retain their original exception identity.
    class PublicationProfileError : public std::invalid_argument {
      public:
        using std::invalid_argument::invalid_argument;
    };

    template <typename Element, std::int64_t Size = -1> struct List {};
    template <typename Shape> struct Delta {};
    template <typename Shape> struct Held {};
    template <typename Shape> struct Origin {};

    // HGL lifts ordinary generic arguments recursively while retaining their
    // independent ordinary identity. Explicit Atomic fields bypass this alias.
    template <typename T> struct TemporalType { using type = hgraph::TS<T>; };
    template <typename T> using Temporal = typename TemporalType<T>::type;
    template <hgraph::fixed_string Name, typename... Constraints>
    struct TemporalType<hgraph::ScalarVar<Name, Constraints...>> {
        using type = hgraph::TsVar<Name, Temporal<Constraints>...>;
    };
    template <typename T, std::int64_t N> struct TemporalType<List<T, N>> {
        using type = hgraph::TSL<Temporal<T>, N < 0 ? hgraph::unbounded_tsl_size : static_cast<std::size_t>(N)>;
    };
    template <std::size_t Index> consteval auto positional_name() {
        constexpr auto digits = [] { std::size_t n = Index, count = 1; while (n >= 10) { n /= 10; ++count; } return count; }();
        char text[digits + 1]{};
        auto n = Index;
        for (auto i = digits; i != 0; --i) { text[i - 1] = static_cast<char>('0' + n % 10); n /= 10; }
        return hgraph::fixed_string{ text };
    }
    template <typename Tuple, typename Indices> struct TemporalTuple;
    template <typename... Ts, std::size_t... Indices>
    struct TemporalTuple<hgraph::FixedTuple<Ts...>, std::index_sequence<Indices...>> {
        using type = hgraph::UnNamedTSB<hgraph::Field<positional_name<Indices>(), Temporal<Ts>>...>;
    };
    template <typename... Ts> struct TemporalType<hgraph::FixedTuple<Ts...>>
        : TemporalTuple<hgraph::FixedTuple<Ts...>, std::index_sequence_for<Ts...>> {};
    template <typename K> struct TemporalType<hgraph::Set<K>> { using type = hgraph::TSS<K>; };
    template <typename K, typename V> struct TemporalType<hgraph::Map<K, V>> { using type = hgraph::TSD<K, Temporal<V>>; };
    template <typename Parents> struct TemporalParents;
    template <typename... Parents> struct TemporalParents<hgraph::BundleParents<Parents...>> {
        using type = hgraph::BundleParents<Temporal<Parents>...>;
    };
    template <hgraph::fixed_string NS, hgraph::fixed_string Name, bool Abstract, typename Parents, typename Args, typename... Fields>
    struct TemporalType<hgraph::NominalBundle<NS, Name, Abstract, Parents, Args, Fields...>> {
        using origin = hgraph::NominalBundle<NS, Name, Abstract, Parents, Args, Fields...>;
        using held = hgraph::HeldNominalBundle<origin, typename TemporalParents<Parents>::type,
            hgraph::Field<Fields::name_sv, Temporal<typename Fields::schema>>...>;
        using type = hgraph::NominalTSB<held, hgraph::Field<Fields::name_sv, Temporal<typename Fields::schema>>...>;
    };

    inline void validate_key_schema(const hgraph::ValueTypeMetaData *schema,
                                    std::unordered_set<const hgraph::ValueTypeMetaData *> &visiting) {
        if (schema->is_owned() || schema->is_abstract_bundle() || !schema->is_hashable() || !schema->is_equatable()) {
            throw std::invalid_argument("unsupported collection key type");
        }
        const auto kind = schema->try_value_kind();
        if (kind == hgraph::ValueTypeKind::Tuple || kind == hgraph::ValueTypeKind::Bundle) {
            if (!visiting.insert(schema).second) { throw std::invalid_argument("recursive collection key type"); }
            for (std::size_t i = 0; i < schema->field_count; ++i) { validate_key_schema(schema->fields[i].type, visiting); }
            visiting.erase(schema);
            return;
        }
        const std::array leaves{hgraph::scalar_descriptor<hgraph::Bool>::value_meta(),
            hgraph::scalar_descriptor<hgraph::Int>::value_meta(), hgraph::scalar_descriptor<hgraph::Float>::value_meta(),
            hgraph::scalar_descriptor<hgraph::Str>::value_meta(), hgraph::scalar_descriptor<hgraph::Date>::value_meta(),
            hgraph::scalar_descriptor<hgraph::Time>::value_meta(), hgraph::scalar_descriptor<hgraph::DateTime>::value_meta(),
            hgraph::scalar_descriptor<hgraph::TimeDelta>::value_meta(), hgraph::scalar_descriptor<hgraph::CivilDateTime>::value_meta(),
            hgraph::scalar_descriptor<hgraph::ZoneId>::value_meta(), hgraph::scalar_descriptor<hgraph::ZonedDateTime>::value_meta(),
            hgraph::scalar_descriptor<hgraph::ZonedTime>::value_meta()};
        if (!schema->is_enum() && std::ranges::find(leaves, schema) == leaves.end()) {
            throw std::invalid_argument("unsupported collection key type");
        }
    }

    inline void validate_key_schema(const hgraph::ValueTypeMetaData *schema) {
        std::unordered_set<const hgraph::ValueTypeMetaData *> visiting;
        validate_key_schema(schema, visiting);
    }

    inline void validate_atomic_schema(const hgraph::ValueTypeMetaData *schema,
                                       std::unordered_set<const hgraph::ValueTypeMetaData *> &visiting) {
        const std::array leaves{hgraph::scalar_descriptor<hgraph::Bool>::value_meta(),
            hgraph::scalar_descriptor<hgraph::Int>::value_meta(), hgraph::scalar_descriptor<hgraph::Float>::value_meta(),
            hgraph::scalar_descriptor<hgraph::Str>::value_meta(), hgraph::scalar_descriptor<hgraph::Date>::value_meta(),
            hgraph::scalar_descriptor<hgraph::Time>::value_meta(), hgraph::scalar_descriptor<hgraph::DateTime>::value_meta(),
            hgraph::scalar_descriptor<hgraph::TimeDelta>::value_meta(),
            hgraph::scalar_descriptor<hgraph::CivilDateTime>::value_meta(),
            hgraph::scalar_descriptor<hgraph::ZoneId>::value_meta(),
            hgraph::scalar_descriptor<hgraph::ZonedDateTime>::value_meta(),
            hgraph::scalar_descriptor<hgraph::ZonedTime>::value_meta()};
        if (schema->is_enum() || std::ranges::find(leaves, schema) != leaves.end()) { return; }
        if (schema->is_owned()) {
            if (schema->element_type->is_abstract_bundle()) { throw std::invalid_argument("recursive family publication payload"); }
            if (!visiting.contains(schema->element_type)) { validate_atomic_schema(schema->element_type, visiting); }
            return;
        }
        if (!visiting.insert(schema).second) { throw std::invalid_argument("recursive atomic publication payload"); }
        const auto kind = schema->try_value_kind();
        if (schema->is_abstract_bundle()) {
            const auto snapshot = hgraph::TypeRealizationSnapshot::capture(hgraph::TypeRegistry::instance());
            for (const auto *member : snapshot->alternatives(schema)) { validate_atomic_schema(member, visiting); }
        } else if (kind == hgraph::ValueTypeKind::List && !schema->is_variadic_tuple() && !hgraph::TypeRegistry::is_array(schema)) {
            validate_atomic_schema(schema->element_type, visiting);
        } else if (kind == hgraph::ValueTypeKind::Set) {
            validate_key_schema(schema->element_type);
        } else if (kind == hgraph::ValueTypeKind::Map) {
            validate_key_schema(schema->key_type);
            validate_atomic_schema(schema->element_type, visiting);
        } else if ((kind == hgraph::ValueTypeKind::Bundle || kind == hgraph::ValueTypeKind::Tuple) &&
                   !schema->is_abstract_bundle()) {
            for (std::size_t index = 0; index < schema->field_count; ++index) {
                validate_atomic_schema(schema->fields[index].type, visiting);
            }
        } else { throw std::invalid_argument("unsupported atomic publication payload"); }
        visiting.erase(schema);
    }

    inline void validate_scalar_key(const hgraph::ValueView &key) {
        if (key.schema() == hgraph::scalar_descriptor<hgraph::Float>::value_meta() && std::isnan(key.checked_as<hgraph::Float>())) {
            throw PublicationProfileError("NaN collection keys are outside the publication profile");
        }
        const auto kind = key.schema()->try_value_kind();
        if (kind == hgraph::ValueTypeKind::Tuple || kind == hgraph::ValueTypeKind::Bundle) {
            const auto *ops = hgraph::checked_value_ops<hgraph::IndexedValueOps>(key.binding(), "collection key");
            for (std::size_t i = 0; i < ops->size(ops->context, key.data()); ++i) {
                if (ops->element_valid && !ops->element_valid(ops->context, key.data(), i)) { continue; }
                const hgraph::ValueView child{ops->element_binding(ops->context, key.data(), i), ops->element_at(ops->context, key.data(), i)};
                if (child.valid()) { validate_scalar_key(child); }
            }
        }
    }

    // One owning hash set spans every argument in a delta recipe. This catches
    // duplicates and overlap after provider-dependent keys become real values.
    class ScalarKeySet {
      public:
        explicit ScalarKeySet(hgraph::ValueTypeRef binding) : binding_{binding}, seen_{binding} { validate_key_schema(binding.schema()); }
        void insert(const hgraph::ValueView &key) {
            if (key.schema() != binding_.schema()) { throw std::invalid_argument("delta key requires its exact declared type"); }
            validate_scalar_key(key);
            if (!seen_.insert(key)) { throw std::invalid_argument("duplicate or overlapping delta member, key or index"); }
        }
      private:
        hgraph::ValueTypeRef binding_;
        hgraph::SetBuilder seen_;
    };

    using OptionalField = std::function<bool(const hgraph::ValueTypeMetaData *, std::size_t)>;

    inline void validate_complete_value(const hgraph::ValueView &root, const OptionalField &optional = {}) {
        struct Frame { hgraph::ValueTypeRef binding; const void *data; bool leaving{false}; };
        std::vector<Frame> pending{{root.binding(), root.data()}};
        std::unordered_set<const void *> ancestors;
        while (!pending.empty()) {
            const auto frame = pending.back();
            pending.pop_back();
            if (frame.leaving) { ancestors.erase(frame.data); continue; }
            const auto value = hgraph::ValueView{frame.binding, frame.data}.concrete();
            if (!value.valid()) { throw PublicationProfileError("incomplete atomic publication payload"); }
            // Only indirect edges can form value cycles. Inline children may
            // share their parent's address, so tracking every field is wrong.
            if (frame.binding.schema()->is_owned()) {
                if (!ancestors.insert(value.data()).second) { throw PublicationProfileError("cyclic atomic publication payload"); }
                pending.push_back({value.binding(), value.data(), true});
            }
            const auto kind = value.schema()->try_value_kind();
            if (kind == hgraph::ValueTypeKind::Map) {
                for (const auto [key, child] : value.as_map()) { validate_scalar_key(key); pending.push_back({key.binding(), key.data()}); pending.push_back({child.binding(), child.data()}); }
            } else if (kind == hgraph::ValueTypeKind::Set) {
                for (const auto key : value.as_set()) { validate_scalar_key(key); pending.push_back({key.binding(), key.data()}); }
            } else if (kind == hgraph::ValueTypeKind::List || kind == hgraph::ValueTypeKind::Tuple || kind == hgraph::ValueTypeKind::Bundle) {
                const auto *ops = hgraph::checked_value_ops<hgraph::IndexedValueOps>(value.binding(), "atomic publication value");
                const auto count = ops->size(ops->context, value.data());
                for (std::size_t index = 0; index < count; ++index) {
                    const hgraph::ValueView child{ops->element_binding(ops->context, value.data(), index),
                        ops->element_at(ops->context, value.data(), index)};
                    if (!child.valid() || (ops->element_valid != nullptr && !ops->element_valid(ops->context, value.data(), index))) {
                        if (kind == hgraph::ValueTypeKind::Bundle && optional && optional(value.schema(), index)) { continue; }
                        throw PublicationProfileError("incomplete atomic publication payload");
                    }
                    pending.push_back({child.binding(), child.data()});
                }
            }
        }
    }

    inline void validate_delta_shape(const hgraph::TSValueTypeMetaData *root) {
        const auto *boolean = hgraph::scalar_descriptor<hgraph::Bool>::value_meta();
        const auto *integer = hgraph::scalar_descriptor<hgraph::Int>::value_meta();
        const std::array leaves{boolean, integer, hgraph::scalar_descriptor<hgraph::Float>::value_meta(),
            hgraph::scalar_descriptor<hgraph::Str>::value_meta(), hgraph::scalar_descriptor<hgraph::Date>::value_meta(),
            hgraph::scalar_descriptor<hgraph::Time>::value_meta(), hgraph::scalar_descriptor<hgraph::DateTime>::value_meta(),
            hgraph::scalar_descriptor<hgraph::TimeDelta>::value_meta(),
            hgraph::scalar_descriptor<hgraph::CivilDateTime>::value_meta(),
            hgraph::scalar_descriptor<hgraph::ZoneId>::value_meta(),
            hgraph::scalar_descriptor<hgraph::ZonedDateTime>::value_meta(),
            hgraph::scalar_descriptor<hgraph::ZonedTime>::value_meta()};
        std::vector<const hgraph::TSValueTypeMetaData *> pending{root};
        while (!pending.empty()) {
            const auto *shape = pending.back();
            pending.pop_back();
            switch (shape->kind) {
                case hgraph::TSTypeKind::TS:
                    if (std::ranges::find(leaves, shape->value_schema) == leaves.end()) {
                        std::unordered_set<const hgraph::ValueTypeMetaData *> visiting;
                        validate_atomic_schema(shape->value_schema, visiting);
                    }
                    break;
                case hgraph::TSTypeKind::TSW: {
                    std::unordered_set<const hgraph::ValueTypeMetaData *> visiting;
                    validate_atomic_schema(shape->delta_value_schema, visiting);
                    break;
                }
                case hgraph::TSTypeKind::TSS:
                    validate_key_schema(shape->value_schema->element_type);
                    break;
                case hgraph::TSTypeKind::TSL:
                    pending.push_back(shape->element_ts());
                    break;
                case hgraph::TSTypeKind::TSD:
                    validate_key_schema(shape->key_type());
                    pending.push_back(shape->element_ts());
                    break;
                case hgraph::TSTypeKind::TSB:
                    for (std::size_t index = 0; index < shape->field_count(); ++index) { pending.push_back(shape->fields()[index].type); }
                    break;
                default: throw std::invalid_argument("unsupported ordinary delta shape");
            }
        }
    }

    // A nominal generic argument records its exact temporal source, separately
    // from the ordinary payload schema. These are interned build-time metadata;
    // no marker value is stored in a publication or inspected during evaluation.
    inline const hgraph::ValueTypeMetaData *ordinary_nominal_origin(const hgraph::ValueTypeMetaData *value) {
        const auto *hierarchy = value->bundle_hierarchy;
        return hierarchy != nullptr && hierarchy->ordinary_origin != nullptr ? hierarchy->ordinary_origin : value;
    }

    inline const hgraph::ValueTypeMetaData *origin_schema(const hgraph::TSValueTypeMetaData *shape) {
        using namespace hgraph;
        std::string kind;
        std::vector<std::pair<std::string, const ValueTypeMetaData *>> children;
        switch (shape->kind) {
            case TSTypeKind::TS: kind = "TS"; break;
            case TSTypeKind::TSS: kind = "TSS"; break;
            case TSTypeKind::TSW:
                kind = shape->is_duration_based()
                    ? "TSWduration:" + std::to_string(shape->time_range().count()) + ":" + std::to_string(shape->min_time_range().count())
                    : "TSWtick:" + std::to_string(shape->period()) + ":" + std::to_string(shape->min_period());
                break;
            case TSTypeKind::TSL:
                kind = "TSL";
                children.emplace_back("element", origin_schema(shape->element_ts()));
                break;
            case TSTypeKind::TSD:
                kind = "TSD";
                children.emplace_back("element", origin_schema(shape->element_ts()));
                break;
            case TSTypeKind::TSB:
                kind = "TSB";
                for (std::size_t i = 0; i < shape->field_count(); ++i) {
                    children.emplace_back(shape->fields()[i].name, origin_schema(shape->fields()[i].type));
                }
                break;
            default: throw std::invalid_argument("unsupported ordinary originating shape");
        }
        return TypeRegistry::instance().bundle("hgl.origin", kind + "[" + std::string{shape->name()} + "]",
            children, {}, false, "__type__", {shape->value_schema});
    }

    inline const hgraph::TSValueTypeMetaData *origin_source(const hgraph::ValueTypeMetaData *value) {
        using namespace hgraph;
        if (!value || !value->is_named_bundle()) { return nullptr; }
        const auto &arguments = value->bundle_generic_arguments();
        if (arguments.size() != 1U) { return nullptr; }
        const auto *held = arguments.front();
        auto &registry = TypeRegistry::instance();
        // Window parameters are cold nominal metadata, never publication fields.
        const std::string_view name = value->name();
        const bool duration_window = name.starts_with("hgl.origin::TSWduration:");
        if (duration_window || name.starts_with("hgl.origin::TSWtick:")) {
            const auto parameters = name.substr(duration_window ? 24U : 20U);
            const auto separator = parameters.find(':');
            const auto end = parameters.find('[');
            if (separator == std::string_view::npos || end == std::string_view::npos || separator >= end) { return nullptr; }
            std::int64_t maximum{}, minimum{};
            const auto a = std::from_chars(parameters.data(), parameters.data() + separator, maximum);
            const auto b = std::from_chars(parameters.data() + separator + 1U, parameters.data() + end, minimum);
            if (a.ec != std::errc{} || b.ec != std::errc{} || a.ptr != parameters.data() + separator ||
                b.ptr != parameters.data() + end || maximum <= 0 || minimum < 0 || minimum > maximum) { return nullptr; }
            return duration_window ? registry.tsw_duration(held->element_type, TimeDelta{maximum}, TimeDelta{minimum})
                                   : registry.tsw(held->element_type, static_cast<std::size_t>(maximum), static_cast<std::size_t>(minimum));
        }
        if (value->name().starts_with("hgl.origin::TS[")) { return registry.ts(held); }
        if (value->name().starts_with("hgl.origin::TSS[")) { return registry.tss(held->element_type); }
        if (value->name().starts_with("hgl.origin::TSL[")) {
            const auto *element = value->field_count == 1U ? origin_source(value->fields[0].type) : nullptr;
            return element ? registry.tsl(element, held->is_fixed_size() ? held->fixed_size : hgraph::unbounded_tsl_size) : nullptr;
        }
        if (value->name().starts_with("hgl.origin::TSD[")) {
            const auto *element = value->field_count == 1U ? origin_source(value->fields[0].type) : nullptr;
            return element ? registry.tsd(held->key_type, element) : nullptr;
        }
        if (value->name().starts_with("hgl.origin::TSB[")) {
            std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
            for (std::size_t i = 0; i < value->field_count; ++i) {
                const auto *child = origin_source(value->fields[i].type);
                if (!child) { return nullptr; }
                fields.emplace_back(value->fields[i].name, child);
            }
            return held->is_named_bundle() ? registry.tsb(held, fields) : registry.un_named_tsb(fields);
        }
        return nullptr;
    }

    // The enclosing identity preserves the originating temporal shape even
    // when two native delta payloads happen to have identical storage schemas.
    inline const hgraph::ValueTypeMetaData *delta_schema(const hgraph::TSValueTypeMetaData *shape) {
        validate_delta_shape(shape);
        if ((shape->kind == hgraph::TSTypeKind::TS || shape->kind == hgraph::TSTypeKind::TSW)) { return shape->delta_value_schema; }
        return hgraph::TypeRegistry::instance().bundle(
            "hgl.delta", std::string{shape->name()}, {{"payload", shape->delta_value_schema}},
            {}, false, "__type__", {origin_schema(shape)});
    }

    // Called only while preparing a concrete node, never from a hook.
    inline hgraph::ValueTypeRef storage_binding(const hgraph::ValueTypeMetaData *schema) {
        auto &factory = hgraph::ValuePlanFactory::instance();
        if (schema->is_owned()) { return factory.type_for(schema); }
        if (schema->is_abstract_bundle()) { return hgraph::value_type_for_wiring(schema); }
        const auto kind = schema->try_value_kind();
        if (kind == hgraph::ValueTypeKind::List) {
            const auto element = storage_binding(schema->element_type);
            return hgraph::intern_value_type(*schema, hgraph::mutable_list_plan(element), hgraph::mutable_list_ops());
        }
        if (kind == hgraph::ValueTypeKind::Map) {
            return hgraph::compact_map_type(factory.type_for(schema->key_type), storage_binding(schema->element_type));
        }
        if (kind == hgraph::ValueTypeKind::Set) {
            return hgraph::compact_set_type(factory.type_for(schema->element_type));
        }
        if (kind == hgraph::ValueTypeKind::Bundle || kind == hgraph::ValueTypeKind::Tuple) {
            std::vector<hgraph::ValueTypeRef> fields;
            fields.reserve(schema->field_count);
            for (std::size_t i = 0; i < schema->field_count; ++i) { fields.push_back(storage_binding(schema->fields[i].type)); }
            return factory.realized_composite_type_for(schema, fields);
        }
        return factory.type_for(schema);
    }

    class PreparedValuePlan
    {
      public:
        PreparedValuePlan() = default;
        explicit PreparedValuePlan(const hgraph::ValueTypeMetaData *schema) : PreparedValuePlan(storage_binding(schema)) {}
        explicit PreparedValuePlan(hgraph::ValueTypeRef binding) : binding_{binding} {
            const auto kind = binding.schema()->try_value_kind();
            if (kind == hgraph::ValueTypeKind::List || kind == hgraph::ValueTypeKind::Bundle || kind == hgraph::ValueTypeKind::Tuple) {
                indexed_ = hgraph::checked_value_ops<hgraph::IndexedValueOps>(binding, "HGL ordinary indexed value");
            }
            if (binding.schema()->is_owned() || binding.schema()->is_abstract_bundle()) { return; }
            if (binding.ops()->kind == hgraph::ValueOpsKind::MutableList) {
                list_ = hgraph::checked_value_ops<hgraph::MutableListValueOps>(binding, "HGL ordinary mutable list");
            }
            if (kind == hgraph::ValueTypeKind::Map) {
                map_ = hgraph::checked_value_ops<hgraph::MapValueOps>(binding, "HGL ordinary map");
            }
            if (kind == hgraph::ValueTypeKind::Bundle || kind == hgraph::ValueTypeKind::Tuple) {
                fields_.reserve(binding.schema()->field_count);
                for (std::size_t index = 0; index < binding.schema()->field_count; ++index) {
                    fields_.emplace_back(indexed_->element_binding(indexed_->context, nullptr, index));
                }
            }
            if (binding.schema()->element_type != nullptr) {
                element_ = kind == hgraph::ValueTypeKind::Set
                    ? hgraph::ValuePlanFactory::instance().type_for(binding.schema()->element_type)
                    : storage_binding(binding.schema()->element_type);
            }
            if (binding.schema()->key_type != nullptr) {
                key_ = hgraph::ValuePlanFactory::instance().type_for(binding.schema()->key_type);
            }
            if (kind == hgraph::ValueTypeKind::List) { list_builder_binding_ = hgraph::compact_list_type(element_, *binding.schema()); }
        }
        [[nodiscard]] hgraph::ValueTypeRef binding() const noexcept { return binding_; }
        [[nodiscard]] hgraph::ValueTypeRef field_binding(std::size_t index) const { return fields_.at(index).binding(); }
        [[nodiscard]] const PreparedValuePlan &field_plan(std::size_t index) const { return fields_.at(index); }
        [[nodiscard]] hgraph::ValueTypeRef element_binding() const noexcept { return element_; }
        [[nodiscard]] hgraph::ValueTypeRef key_binding() const noexcept { return key_; }
        [[nodiscard]] hgraph::Value retain(const hgraph::ValueView &value) const {
            if (!value.has_value()) { return hgraph::Value::typed_null(binding_); }
            if (binding_.schema()->is_abstract_bundle()) { return hgraph::Value{binding_, value.concrete()}; }
            return hgraph::Value{binding_, value};
        }
        [[nodiscard]] hgraph::Value empty_list() const {
            if (!binding_.schema()->is_fixed_size()) { return hgraph::Value{binding_}; }
            hgraph::ListBuilder builder{element_, *binding_.schema()};
            builder.append_default(binding_.schema()->fixed_size);
            auto storage = builder.build_storage();
            return list(storage);
        }
        [[nodiscard]] hgraph::Value list(const hgraph::ListStorage &storage) const {
            return retain(hgraph::ValueView{list_builder_binding_, &storage});
        }
        [[nodiscard]] hgraph::ValueView map_index(const hgraph::ValueView &value, const hgraph::ValueView &key) const {
            if (!value.has_value()) { throw std::logic_error("ordinary scalar value is absent"); }
            if (map_ == nullptr) { throw std::invalid_argument("ordinary value storage is not a map"); }
            if (!map_->contains(map_->context, value.data(), key.data())) { throw std::out_of_range("ordinary map key not present"); }
            return hgraph::ValueView{element_, map_->value_at(map_->context, value.data(), key.data())};
        }
        [[nodiscard]] bool map_contains(const hgraph::ValueView &value, const hgraph::ValueView &key) const {
            if (!value.has_value()) { throw std::logic_error("ordinary scalar value is absent"); }
            if (map_ == nullptr) { throw std::invalid_argument("ordinary value storage is not a map"); }
            return map_->contains(map_->context, value.data(), key.data());
        }
        [[nodiscard]] hgraph::KeyValueRange<hgraph::ValueView, hgraph::ValueView> items(const hgraph::ValueView &value) const {
            if (!value.has_value()) { throw std::logic_error("ordinary scalar value is absent"); }
            if (map_ == nullptr) { throw std::invalid_argument("ordinary value storage is not a map"); }
            return map_->make_kv_range(map_->context, value.data());
        }
        [[nodiscard]] hgraph::Value bundle(std::span<const std::pair<std::size_t, hgraph::ValueView>> fields) const {
            hgraph::BundleBuilder result{binding_};
            for (const auto &[index, value] : fields) { if (value.has_value()) { result.set(index, value); } }
            return result.build();
        }
        [[nodiscard]] std::int64_t len(const hgraph::ValueView &value, bool retained_observation = true) const {
            if (!value.has_value()) {
                if (!retained_observation) { throw std::logic_error("ordinary scalar value is absent"); }
                throw hgl::ExecutionError{"value.unset_read", "ordinary scalar value is absent"};
            }
            if (indexed_ == nullptr) { throw std::invalid_argument("ordinary value storage is not indexed"); }
            const auto size = indexed_->size(indexed_->context, value.data());
            if (size > static_cast<std::size_t>(std::numeric_limits<std::int64_t>::max())) {
                throw std::overflow_error("ordinary list length is not representable as i64");
            }
            return static_cast<std::int64_t>(size);
        }
        [[nodiscard]] hgraph::ValueView index(const hgraph::ValueView &value, std::int64_t index) const {
            if (!value.has_value()) { throw std::logic_error("ordinary scalar value is absent"); }
            if (index < 0 || index >= len(value)) { throw std::out_of_range("ordinary value index out of bounds"); }
            const auto offset = static_cast<std::size_t>(index);
            if (indexed_->element_valid != nullptr && !indexed_->element_valid(indexed_->context, value.data(), offset)) {
                return hgraph::ValueView{indexed_->element_binding(indexed_->context, value.data(), offset), nullptr};
            }
            return hgraph::ValueView{indexed_->element_binding(indexed_->context, value.data(), offset),
                                     indexed_->element_at(indexed_->context, value.data(), offset)};
        }
        [[nodiscard]] hgraph::ValueView index_mutable(const hgraph::ValueView &value, std::int64_t index) const {
            if (!value.has_value()) { throw std::logic_error("ordinary scalar value is absent"); }
            if (index < 0 || index >= len(value)) { throw std::out_of_range("ordinary value index out of bounds"); }
            const auto offset = static_cast<std::size_t>(index);
            auto writable = value.begin_mutation();
            // IndexedValueOps permits dense representations to use the read
            // accessor for writable storage obtained through begin_mutation.
            auto *element = indexed_->mutable_element_at != nullptr
                ? indexed_->mutable_element_at(indexed_->context, writable.mutable_data(), offset)
                : const_cast<void *>(indexed_->element_at(indexed_->context, writable.mutable_data(), offset));
            return hgraph::ValueView{indexed_->element_binding(indexed_->context, value.data(), offset),
                                     element};
        }
        // Replacing a slot is a mutation of its owning parent, not permission
        // to mutate the internals of a read-only child container.
        void replace_index(const hgraph::ValueView &parent, std::int64_t index, const hgraph::ValueView &source) const {
            const auto child = this->index(parent, index);
            hgraph::Value retained{child.binding(), source};
            auto target = index_mutable(parent, index);
            target.binding().copy_assign_at(const_cast<void *>(target.data()), retained.view().data());
        }
        void push(const hgraph::ValueView &list, const hgraph::ValueView &element) const {
            if (list_ == nullptr) { throw std::invalid_argument("ordinary list storage does not support growth"); }
            auto retained = hgraph::Value{element_, element};
            auto writable = list.begin_mutation();
            list_->push_back(list_->context, writable.mutable_data(), retained.view().binding(), retained.view().data());
        }
      private:
        hgraph::ValueTypeRef binding_{};
        const hgraph::IndexedValueOps *indexed_{};
        const hgraph::MutableListValueOps *list_{};
        const hgraph::MapValueOps *map_{};
        std::vector<PreparedValuePlan> fields_{};
        hgraph::ValueTypeRef element_{};
        hgraph::ValueTypeRef list_builder_binding_{};
        hgraph::ValueTypeRef key_{};
    };

    // Private compiler normalization of a matched temporal observation into
    // its declared ordinary value. All shape and nominal checks are cold;
    // evaluation uses only prepared accessors, indices and child operations.
    class PreparedObservationPlan {
      public:
        PreparedObservationPlan() = default;
        PreparedObservationPlan(const hgraph::TSValueTypeMetaData *shape, const hgraph::ValueTypeMetaData *ordinary, bool to_ordinary = true)
            : source_{to_ordinary ? shape->value_schema : ordinary}, target_{to_ordinary ? ordinary : shape->value_schema} {
            using namespace hgraph;
            if (shape->value_schema == ordinary && shape->kind != TSTypeKind::TSB &&
                shape->kind != TSTypeKind::TSL && shape->kind != TSTypeKind::TSD) {
                operation_ = &retain_same; endpoint_operation_ = &retain_endpoint_leaf; return;
            }
            switch (shape->kind) {
                case TSTypeKind::TSB: {
                    const auto kind = ordinary->try_value_kind();
                    if (kind != ValueTypeKind::Tuple && kind != ValueTypeKind::Bundle) { mismatch(); }
                    if (shape->field_count() != ordinary->field_count) { mismatch(); }
                    if (ordinary->is_named_bundle()) {
                        if (shape->value_schema != ordinary && ordinary_nominal_origin(shape->value_schema) != ordinary) { mismatch(); }
                    } else if (shape->value_schema->is_named_bundle()) { mismatch(); }
                    indices_.resize(ordinary->field_count);
                    children_.reserve(ordinary->field_count);
                    ankerl::unordered_dense::map<std::string_view, std::size_t> temporal_names;
                    if (ordinary->is_named_bundle()) {
                        temporal_names.reserve(shape->field_count());
                        for (std::size_t index = 0; index < shape->field_count(); ++index) {
                            const auto *name = shape->fields()[index].name;
                            if (name == nullptr || !temporal_names.try_emplace(name, index).second) { mismatch(); }
                        }
                    }
                    std::vector<bool> matched(shape->field_count());
                    for (std::size_t index = 0; index < ordinary->field_count; ++index) {
                        std::size_t source_index = index;
                        if (ordinary->is_named_bundle()) {
                            const auto *name = ordinary->fields[index].name;
                            if (name == nullptr) { mismatch(); }
                            const auto found = temporal_names.find(name);
                            if (found == temporal_names.end() || matched[found->second]) { mismatch(); }
                            source_index = found->second;
                            matched[source_index] = true;
                        } else if (kind == ValueTypeKind::Tuple && shape->fields()[index].name != std::to_string(index)) { mismatch(); }
                        if (to_ordinary) { indices_[index] = source_index; }
                        else { indices_[source_index] = index; }
                    }
                    for (std::size_t index = 0; index < ordinary->field_count; ++index) {
                        const auto temporal_index = to_ordinary ? indices_[index] : index;
                        const auto ordinary_index = to_ordinary ? index : indices_[index];
                        children_.emplace_back(shape->fields()[temporal_index].type, ordinary->fields[ordinary_index].type, to_ordinary);
                    }
                    operation_ = &retain_bundle;
                    endpoint_operation_ = &retain_endpoint_bundle;
                    break;
                }
                case TSTypeKind::TSL:
                    if (ordinary->try_value_kind() != ValueTypeKind::List || ordinary->is_fixed_size() == shape->is_unbounded_tsl() ||
                        (ordinary->is_fixed_size() && ordinary->fixed_size != shape->fixed_size())) { mismatch(); }
                    children_.emplace_back(shape->element_ts(), ordinary->element_type, to_ordinary);
                    operation_ = &retain_list;
                    endpoint_operation_ = &retain_endpoint_list;
                    break;
                case TSTypeKind::TSD:
                    if (ordinary->try_value_kind() != ValueTypeKind::Map || ordinary->key_type != shape->key_type()) { mismatch(); }
                    children_.emplace_back(shape->element_ts(), ordinary->element_type, to_ordinary);
                    operation_ = &retain_map;
                    endpoint_operation_ = &retain_endpoint_map;
                    break;
                default: mismatch();
            }
        }
        [[nodiscard]] hgraph::Value retain(const hgraph::ValueView &value) const {
            if (!value.has_value()) { return hgraph::Value::typed_null(target_.binding()); }
            return operation_(*this, value);
        }
        [[nodiscard]] hgraph::Value retain_endpoint(const hgraph::TSInputView &endpoint) const {
            if (!endpoint.valid()) { return hgraph::Value::typed_null(target_.binding()); }
            return endpoint_operation_(*this, endpoint);
        }
        [[nodiscard]] hgraph::ValueTypeRef binding() const noexcept { return target_.binding(); }
      private:
        [[noreturn]] static void mismatch() { throw std::invalid_argument("ordinary observation schema does not match its temporal origin"); }
        static hgraph::Value retain_same(const PreparedObservationPlan &plan, const hgraph::ValueView &value) {
            return plan.target_.retain(value);
        }
        static hgraph::Value retain_bundle(const PreparedObservationPlan &plan, const hgraph::ValueView &value) {
            auto source = plan.source_.retain(value);
            std::vector<hgraph::Value> children;
            children.reserve(plan.children_.size());
            std::vector<std::pair<std::size_t, hgraph::ValueView>> fields;
            fields.reserve(plan.children_.size());
            for (std::size_t index = 0; index < plan.children_.size(); ++index) {
                children.push_back(plan.children_[index].retain(plan.source_.index(source.view(), static_cast<std::int64_t>(plan.indices_[index]))));
                if (children.back().has_value()) { fields.emplace_back(index, children.back().view()); }
            }
            return plan.target_.bundle(fields);
        }
        static hgraph::Value retain_list(const PreparedObservationPlan &plan, const hgraph::ValueView &value) {
            auto source = plan.source_.retain(value);
            hgraph::ListBuilder builder{plan.target_.element_binding(), *plan.target_.binding().schema()};
            const auto size = plan.source_.len(source.view());
            for (std::int64_t index = 0; index < size; ++index) {
                auto child = plan.children_.front().retain(plan.source_.index(source.view(), index));
                if (child.has_value()) { builder.push_back(child.view()); } else { builder.push_back_unset(); }
            }
            auto storage = builder.build_storage();
            return plan.target_.list(storage);
        }
        static hgraph::Value retain_map(const PreparedObservationPlan &plan, const hgraph::ValueView &value) {
            auto source = plan.source_.retain(value);
            hgraph::MapBuilder builder{plan.target_.key_binding(), plan.target_.element_binding()};
            for (const auto &[key, value_child] : plan.source_.items(source.view())) {
                auto child = plan.children_.front().retain(value_child);
                if (child.has_value()) { builder.set_item(key, child.view()); } else { builder.set_item_unset(key); }
            }
            auto storage = builder.build_storage();
            return hgraph::Value{plan.target_.binding(), &storage, hgraph::Value::AdoptStorage{}};
        }
        static hgraph::Value retain_endpoint_leaf(const PreparedObservationPlan &plan, const hgraph::TSInputView &endpoint) {
            return plan.target_.retain(endpoint.value());
        }
        static hgraph::Value retain_endpoint_bundle(const PreparedObservationPlan &plan, const hgraph::TSInputView &endpoint) {
            std::vector<hgraph::Value> children;
            children.reserve(plan.children_.size());
            std::vector<std::pair<std::size_t, hgraph::ValueView>> fields;
            fields.reserve(plan.children_.size());
            for (std::size_t index = 0; index < plan.children_.size(); ++index) {
                children.push_back(plan.children_[index].retain_endpoint(endpoint.indexed_child_at(plan.indices_[index])));
                if (children.back().has_value()) { fields.emplace_back(index, children.back().view()); }
            }
            return plan.target_.bundle(fields);
        }
        static hgraph::Value retain_endpoint_list(const PreparedObservationPlan &plan, const hgraph::TSInputView &endpoint) {
            hgraph::ListBuilder builder{plan.target_.element_binding(), *plan.target_.binding().schema()};
            const auto size = endpoint.as_list().size();
            for (std::size_t index = 0; index < size; ++index) {
                auto child = plan.children_.front().retain_endpoint(endpoint.indexed_child_at(index));
                if (child.has_value()) { builder.push_back(child.view()); } else { builder.push_back_unset(); }
            }
            auto storage = builder.build_storage();
            return plan.target_.list(storage);
        }
        static hgraph::Value retain_endpoint_map(const PreparedObservationPlan &plan, const hgraph::TSInputView &endpoint) {
            hgraph::MapBuilder builder{plan.target_.key_binding(), plan.target_.element_binding()};
            const auto map = endpoint.as_dict();
            for (const auto &[key, child_endpoint] : map.items()) {
                auto child = plan.children_.front().retain_endpoint(child_endpoint);
                if (child.has_value()) { builder.set_item(key, child.view()); } else { builder.set_item_unset(key); }
            }
            auto storage = builder.build_storage();
            return hgraph::Value{plan.target_.binding(), &storage, hgraph::Value::AdoptStorage{}};
        }
        PreparedValuePlan source_{};
        PreparedValuePlan target_{};
        std::vector<std::size_t> indices_{};
        std::vector<PreparedObservationPlan> children_{};
        hgraph::Value (*operation_)(const PreparedObservationPlan &, const hgraph::ValueView &){};
        hgraph::Value (*endpoint_operation_)(const PreparedObservationPlan &, const hgraph::TSInputView &){};
    };

    // A source type variable resolves to a concrete temporal shape during
    // preparation. Select its recursive publication operations there, while
    // preserving the exact ordinary source bindings independently of the
    // temporal parent's held schema.
    class PreparedPublicationPlan
    {
      public:
        PreparedPublicationPlan() = default;
        PreparedPublicationPlan(const hgraph::TSValueTypeMetaData *shape,
                                const hgraph::ValueTypeMetaData *source) : value_{source} {
            if (shape->kind == hgraph::TSTypeKind::TSB) {
                const auto kind = source->try_value_kind();
                if (kind != hgraph::ValueTypeKind::Bundle && kind != hgraph::ValueTypeKind::Tuple) { throw std::invalid_argument("ordinary publication requires indexed fields"); }
                if (source->field_count != shape->field_count()) { throw std::invalid_argument("ordinary publication field count mismatch"); }
                if (source->is_named_bundle() && shape->value_schema->is_named_bundle() &&
                    ordinary_nominal_origin(source) != ordinary_nominal_origin(shape->value_schema)) {
                    throw std::invalid_argument("ordinary publication nominal origin mismatch");
                }
                ankerl::unordered_dense::map<std::string_view, std::size_t> temporal_names;
                temporal_names.reserve(shape->field_count());
                for (std::size_t index = 0; index < shape->field_count(); ++index) {
                    const auto *name = shape->fields()[index].name;
                    if (name == nullptr || !temporal_names.try_emplace(name, index).second) {
                        throw std::invalid_argument("ordinary publication field name mismatch");
                    }
                }
                destination_indices_.reserve(source->field_count);
                children_.reserve(source->field_count);
                std::vector<bool> matched(shape->field_count());
                for (std::size_t index = 0; index < source->field_count; ++index) {
                    const auto positional = kind == hgraph::ValueTypeKind::Tuple ? std::to_string(index) : std::string{};
                    if (kind != hgraph::ValueTypeKind::Tuple && source->fields[index].name == nullptr) {
                        throw std::invalid_argument("ordinary publication field name mismatch");
                    }
                    const std::string_view name = kind == hgraph::ValueTypeKind::Tuple
                        ? std::string_view{positional} : std::string_view{source->fields[index].name};
                    const auto found = temporal_names.find(name);
                    if (found == temporal_names.end() || matched[found->second]) { throw std::invalid_argument("ordinary publication field name mismatch"); }
                    const auto target_index = found->second;
                    matched[target_index] = true;
                    destination_indices_.push_back(target_index);
                    children_.emplace_back(shape->fields()[target_index].type, source->fields[index].type);
                }
                publish_ = &publish_bundle;
            } else if (shape->kind == hgraph::TSTypeKind::TSL) {
                if (shape->is_unbounded_tsl()) { throw PublicationProfileError{"generic complete growing List publication is outside the fixed structural profile"}; }
                if (source->try_value_kind() != hgraph::ValueTypeKind::List ||
                    source->is_fixed_size() == shape->is_unbounded_tsl() ||
                    (source->is_fixed_size() && source->fixed_size != shape->fixed_size())) { throw std::invalid_argument("ordinary publication list extent mismatch"); }
                children_.emplace_back(shape->element_ts(), source->element_type);
                publish_ = &publish_list;
            } else if (shape->kind == hgraph::TSTypeKind::TSD) {
                if (source->try_value_kind() != hgraph::ValueTypeKind::Map || source->key_type != shape->key_type()) { throw std::invalid_argument("ordinary publication map key schema mismatch"); }
                children_.emplace_back(shape->element_ts(), source->element_type);
                publish_ = &publish_map;
            } else {
                if (source != shape->value_schema) { throw std::invalid_argument("ordinary publication leaf schema mismatch"); }
                publish_ = &publish_leaf;
            }
        }
        void apply(const hgraph::TSOutputView &out, const hgraph::ValueView &source) const {
            auto retained = value_.retain(source);
            publish_(*this, out, retained.view());
        }
      private:
        static void publish_leaf(const PreparedPublicationPlan &, const hgraph::TSOutputView &out,
                                 const hgraph::ValueView &value) {
            auto mutation = out.begin_mutation(out.evaluation_time());
            static_cast<void>(mutation.copy_value_from(value));
        }
        static void publish_child(const PreparedPublicationPlan &plan, const hgraph::TSOutputView &out,
                                  const hgraph::ValueView &value) {
            if (value.has_value()) { plan.publish_(plan, out, value); }
            else {
                auto mutation = out.begin_mutation(out.evaluation_time());
                static_cast<void>(mutation.invalidate());
            }
        }
        static void require_live(const PreparedPublicationPlan &plan, const hgraph::ValueView &value) {
            if (!value.has_value()) { throw PublicationProfileError{"ordinary structural publication requires a valid retained value"}; }
            for (std::int64_t index = 0; index < plan.value_.len(value); ++index) {
                if (plan.value_.index(value, index).has_value()) { return; }
            }
            throw PublicationProfileError{"ordinary structural publication requires a nonempty value with a valid child"};
        }
        static void publish_bundle(const PreparedPublicationPlan &plan, const hgraph::TSOutputView &out,
                                   const hgraph::ValueView &value) {
            require_live(plan, value);
            auto bundle = out.as_bundle();
            for (std::size_t index = 0; index < plan.children_.size(); ++index) {
                publish_child(plan.children_[index], bundle.at(plan.destination_indices_[index]), plan.value_.index(value, static_cast<std::int64_t>(index)));
            }
        }
        static void publish_list(const PreparedPublicationPlan &plan, const hgraph::TSOutputView &out,
                                 const hgraph::ValueView &value) {
            require_live(plan, value);
            auto list = out.as_list();
            const auto size = static_cast<std::size_t>(plan.value_.len(value));
            for (std::size_t index = 0; index < size; ++index) {
                publish_child(plan.children_.front(), list.at(index), plan.value_.index(value, static_cast<std::int64_t>(index)));
            }
        }
        static void publish_map(const PreparedPublicationPlan &plan, const hgraph::TSOutputView &out,
                                const hgraph::ValueView &value) {
            if (!value.has_value()) { throw PublicationProfileError{"ordinary structural publication requires a valid retained value"}; }
            bool live = false;
            for (const auto [key, child] : plan.value_.items(value)) {
                static_cast<void>(key);
                live = live || child.has_value();
            }
            if (!live) { throw PublicationProfileError{"ordinary structural publication requires a nonempty value with a valid child"}; }
            auto dict = out.as_dict();
            std::vector<hgraph::Value> removals;
            for (const auto key : dict.keys()) {
                if (!plan.value_.map_contains(value, key)) { removals.emplace_back(key); }
            }
            auto mutation = dict.begin_mutation(out.evaluation_time());
            for (const auto &key : removals) { static_cast<void>(mutation.erase(key.view())); }
            for (const auto [key, child] : plan.value_.items(value)) {
                if (!child.has_value() && !dict.contains(key)) {
                    throw PublicationProfileError{"ordinary structural publication cannot create invalid map membership"};
                }
                auto data = mutation.at(key);
                publish_child(plan.children_.front(), hgraph::TSOutputView{out.output(), data, out.evaluation_time()}, child);
            }
        }
        PreparedValuePlan value_{};
        std::vector<std::size_t> destination_indices_{};
        std::vector<PreparedPublicationPlan> children_{};
        void (*publish_)(const PreparedPublicationPlan &, const hgraph::TSOutputView &, const hgraph::ValueView &){
            [](const PreparedPublicationPlan &, const hgraph::TSOutputView &, const hgraph::ValueView &) { throw std::logic_error("unprepared ordinary publication plan"); }};
    };

    class PreparedDeltaPlan
    {
      public:
        PreparedDeltaPlan() = default;
        explicit PreparedDeltaPlan(const hgraph::TSValueTypeMetaData *shape)
            : value_{delta_schema(shape)}, native_{hgraph::ValuePlanFactory::instance().type_for(shape->delta_value_schema)} {
            if ((shape->kind == hgraph::TSTypeKind::TS || shape->kind == hgraph::TSTypeKind::TSW)) {
                capture_ = [](const PreparedValuePlan &plan, const hgraph::ValueView &delta) { return plan.retain(delta); };
                payload_ = [](const PreparedValuePlan &, const hgraph::ValueView &delta) { return hgraph::ValueView{delta.binding(), delta.data()}; };
            } else {
                capture_ = [](const PreparedValuePlan &plan, const hgraph::ValueView &delta) {
                    hgraph::BundleBuilder result{plan.binding()};
                    result.set(0, delta);
                    return result.build();
                };
                payload_ = [](const PreparedValuePlan &plan, const hgraph::ValueView &delta) { return plan.index(delta, 0); };
            }
        }
        [[nodiscard]] hgraph::ValueTypeRef binding() const noexcept { return value_.binding(); }
        [[nodiscard]] hgraph::ValueTypeRef native_binding() const noexcept { return native_.binding(); }
        [[nodiscard]] const PreparedValuePlan &native_plan() const noexcept { return native_; }
        [[nodiscard]] hgraph::Value capture(const hgraph::ValueView &delta) const { return capture_(value_, delta); }
        [[nodiscard]] hgraph::ValueView payload(const hgraph::ValueView &delta) const { return payload_(value_, delta); }
      private:
        PreparedValuePlan value_{};
        PreparedValuePlan native_{};
        hgraph::Value (*capture_)(const PreparedValuePlan &, const hgraph::ValueView &){
            [](const PreparedValuePlan &, const hgraph::ValueView &) -> hgraph::Value { throw std::logic_error("unprepared delta plan"); }};
        hgraph::ValueView (*payload_)(const PreparedValuePlan &, const hgraph::ValueView &){
            [](const PreparedValuePlan &, const hgraph::ValueView &) -> hgraph::ValueView { throw std::logic_error("unprepared delta plan"); }};
    };
}

namespace hgraph
{
    template <typename Shape> struct scalar_descriptor<hgl::ordinary::Origin<Shape>> {
        static constexpr bool is_concrete() noexcept { return schema_descriptor<Shape>::is_concrete(); }
        static const ValueTypeMetaData *value_meta() {
            if constexpr (is_concrete()) { return hgl::ordinary::origin_schema(schema_descriptor<Shape>::ts_meta()); }
            else { return nullptr; }
        }
    };
    template <typename Shape> struct scalar_descriptor<hgl::ordinary::Held<Shape>> {
        static constexpr bool is_concrete() noexcept { return schema_descriptor<Shape>::is_concrete(); }
        static const ValueTypeMetaData *value_meta() {
            if constexpr (is_concrete()) { return schema_descriptor<Shape>::ts_meta()->value_schema; }
            else { return nullptr; }
        }
    };
    template <typename Element, std::int64_t Size> struct scalar_descriptor<hgl::ordinary::List<Element, Size>> {
        static constexpr bool is_concrete() noexcept { return scalar_descriptor<Element>::is_concrete(); }
        static const ValueTypeMetaData *value_meta() {
            if constexpr (!is_concrete()) { return nullptr; }
            const auto *element = scalar_descriptor<Element>::value_meta();
            if constexpr (Size < 0) { return TypeRegistry::instance().list(element); }
            else { return TypeRegistry::instance().fixed_list(element, static_cast<std::size_t>(Size)); }
        }
    };
    template <typename Shape> struct scalar_descriptor<hgl::ordinary::Delta<Shape>> {
        static constexpr bool is_concrete() noexcept { return schema_descriptor<Shape>::is_concrete(); }
        static const ValueTypeMetaData *value_meta() {
            if constexpr (is_concrete()) { return hgl::ordinary::delta_schema(schema_descriptor<Shape>::ts_meta()); }
            else { return nullptr; }
        }
    };
}
#endif
