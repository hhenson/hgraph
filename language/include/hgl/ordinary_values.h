#ifndef HGL_ORDINARY_VALUES_H
#define HGL_ORDINARY_VALUES_H

#include <hgraph/types/static_schema.h>
#include <hgraph/types/temporal.h>
#include <hgraph/types/value/mutable_container_ops.h>
#include <hgraph/types/value/value_builder.h>

#include <cstdint>
#include <cmath>
#include <limits>
#include <span>
#include <utility>
#include <unordered_set>

namespace hgl::ordinary
{
    template <typename Element, std::int64_t Size = -1> struct List {};
    template <typename Shape> struct Delta {};
    template <typename Shape> struct Held {};
    template <typename Shape> struct Origin {};

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
        if (!visiting.insert(schema).second) { throw std::invalid_argument("recursive atomic publication payload"); }
        const auto kind = schema->try_value_kind();
        if (kind == hgraph::ValueTypeKind::List && !schema->is_variadic_tuple() && !hgraph::TypeRegistry::is_array(schema)) {
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
            throw std::invalid_argument("NaN collection keys are outside the publication profile");
        }
    }

    // One owning hash set spans every argument in a delta recipe. This catches
    // duplicates and overlap after provider-dependent keys become real values.
    class ScalarKeySet {
      public:
        explicit ScalarKeySet(hgraph::ValueTypeRef binding) : binding_{binding}, seen_{binding} {}
        void insert(const hgraph::ValueView &key) {
            if (key.schema() != binding_.schema()) { throw std::invalid_argument("delta key requires its exact declared type"); }
            validate_scalar_key(key);
            if (!seen_.insert(key)) { throw std::invalid_argument("duplicate or overlapping delta member, key or index"); }
        }
      private:
        hgraph::ValueTypeRef binding_;
        hgraph::SetBuilder seen_;
    };

    inline void validate_complete_value(const hgraph::ValueView &value) {
        if (!value.valid()) { throw std::invalid_argument("incomplete atomic publication payload"); }
        const auto kind = value.schema()->try_value_kind();
        if (kind == hgraph::ValueTypeKind::Map) {
            for (const auto [key, child] : value.as_map()) { validate_scalar_key(key); validate_complete_value(child); }
        } else if (kind == hgraph::ValueTypeKind::Set) {
            for (const auto key : value.as_set()) { validate_scalar_key(key); }
        } else if (kind == hgraph::ValueTypeKind::List || kind == hgraph::ValueTypeKind::Tuple || kind == hgraph::ValueTypeKind::Bundle) {
            const auto *ops = hgraph::checked_value_ops<hgraph::IndexedValueOps>(value.binding(), "atomic publication value");
            const auto count = ops->size(ops->context, value.data());
            for (std::size_t index = 0; index < count; ++index) {
                if (ops->element_valid != nullptr && !ops->element_valid(ops->context, value.data(), index)) {
                    throw std::invalid_argument("incomplete atomic publication payload");
                }
                validate_complete_value(hgraph::ValueView{ops->element_binding(ops->context, value.data(), index),
                    ops->element_at(ops->context, value.data(), index)});
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
                case hgraph::TSTypeKind::TSS:
                    if (!shape->value_schema->element_type->is_enum() &&
                        std::ranges::find(leaves, shape->value_schema->element_type) == leaves.end()) {
                        throw std::invalid_argument("ordinary set deltas require scalar members");
                    }
                    break;
                case hgraph::TSTypeKind::TSL:
                    if (shape->is_unbounded_tsl()) { throw std::invalid_argument("ordinary deltas require a fixed list shape"); }
                    pending.push_back(shape->element_ts());
                    break;
                case hgraph::TSTypeKind::TSD:
                    if (!shape->key_type()->is_enum() && std::ranges::find(leaves, shape->key_type()) == leaves.end()) {
                        throw std::invalid_argument("ordinary map deltas require scalar keys");
                    }
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
    inline const hgraph::ValueTypeMetaData *origin_schema(const hgraph::TSValueTypeMetaData *shape) {
        using namespace hgraph;
        std::string kind;
        std::vector<std::pair<std::string, const ValueTypeMetaData *>> children;
        switch (shape->kind) {
            case TSTypeKind::TS: kind = "TS"; break;
            case TSTypeKind::TSS: kind = "TSS"; break;
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
        if (value->name().starts_with("hgl.origin::TS[")) { return registry.ts(held); }
        if (value->name().starts_with("hgl.origin::TSS[")) { return registry.tss(held->element_type); }
        if (value->name().starts_with("hgl.origin::TSL[")) {
            const auto *element = value->field_count == 1U ? origin_source(value->fields[0].type) : nullptr;
            return element ? registry.tsl(element, held->fixed_size) : nullptr;
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
            return held->is_named_bundle() ? registry.tsb(held->name(), fields) : registry.un_named_tsb(fields);
        }
        return nullptr;
    }

    // The enclosing identity preserves the originating temporal shape even
    // when two native delta payloads happen to have identical storage schemas.
    inline const hgraph::ValueTypeMetaData *delta_schema(const hgraph::TSValueTypeMetaData *shape) {
        validate_delta_shape(shape);
        if (shape->kind == hgraph::TSTypeKind::TS) { return shape->delta_value_schema; }
        return hgraph::TypeRegistry::instance().bundle(
            "hgl.delta", std::string{shape->name()}, {{"payload", shape->delta_value_schema}},
            {}, false, "__type__", {origin_schema(shape)});
    }

    // Called only while preparing a concrete node, never from a hook.
    inline hgraph::ValueTypeRef storage_binding(const hgraph::ValueTypeMetaData *schema) {
        auto &factory = hgraph::ValuePlanFactory::instance();
        const auto kind = schema->try_value_kind();
        if (kind == hgraph::ValueTypeKind::List) {
            const auto element = storage_binding(schema->element_type);
            if (schema->is_fixed_size()) { return factory.realized_fixed_list_type_for(schema, element); }
            return hgraph::intern_value_type(*schema, hgraph::mutable_list_plan(element), hgraph::mutable_list_ops());
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
            if (binding.ops()->kind == hgraph::ValueOpsKind::MutableList) {
                list_ = hgraph::checked_value_ops<hgraph::MutableListValueOps>(binding, "HGL ordinary mutable list");
            }
            if (kind == hgraph::ValueTypeKind::Bundle || kind == hgraph::ValueTypeKind::Tuple) {
                fields_.reserve(binding.schema()->field_count);
                for (std::size_t index = 0; index < binding.schema()->field_count; ++index) {
                    fields_.emplace_back(indexed_->element_binding(indexed_->context, nullptr, index));
                }
            }
            if (binding.schema()->element_type != nullptr) { element_ = storage_binding(binding.schema()->element_type); }
            if (binding.schema()->key_type != nullptr) { key_ = storage_binding(binding.schema()->key_type); }
        }
        [[nodiscard]] hgraph::ValueTypeRef binding() const noexcept { return binding_; }
        [[nodiscard]] hgraph::ValueTypeRef field_binding(std::size_t index) const { return fields_.at(index).binding(); }
        [[nodiscard]] const PreparedValuePlan &field_plan(std::size_t index) const { return fields_.at(index); }
        [[nodiscard]] hgraph::ValueTypeRef element_binding() const noexcept { return element_; }
        [[nodiscard]] hgraph::ValueTypeRef key_binding() const noexcept { return key_; }
        [[nodiscard]] hgraph::Value retain(const hgraph::ValueView &value) const { return hgraph::Value{binding_, value}; }
        [[nodiscard]] hgraph::Value empty_list() const { return hgraph::Value{binding_}; }
        [[nodiscard]] hgraph::Value bundle(std::span<const std::pair<std::size_t, hgraph::ValueView>> fields) const {
            hgraph::BundleBuilder result{binding_};
            for (const auto &[index, value] : fields) { result.set(index, value); }
            return result.build();
        }
        [[nodiscard]] std::int64_t len(const hgraph::ValueView &value) const {
            const auto size = indexed_->size(indexed_->context, value.data());
            if (size > static_cast<std::size_t>(std::numeric_limits<std::int64_t>::max())) {
                throw std::overflow_error("ordinary list length is not representable as i64");
            }
            return static_cast<std::int64_t>(size);
        }
        [[nodiscard]] hgraph::ValueView index(const hgraph::ValueView &value, std::int64_t index) const {
            if (index < 0 || index >= len(value)) { throw std::out_of_range("ordinary value index out of bounds"); }
            const auto offset = static_cast<std::size_t>(index);
            if (indexed_->element_valid != nullptr && !indexed_->element_valid(indexed_->context, value.data(), offset)) {
                return hgraph::ValueView{indexed_->element_binding(indexed_->context, value.data(), offset), nullptr};
            }
            return hgraph::ValueView{indexed_->element_binding(indexed_->context, value.data(), offset),
                                     indexed_->element_at(indexed_->context, value.data(), offset)};
        }
        [[nodiscard]] hgraph::ValueView index_mutable(const hgraph::ValueView &value, std::int64_t index) const {
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
        void push(const hgraph::ValueView &list, const hgraph::ValueView &element) const {
            auto retained = hgraph::Value{element_, element};
            auto writable = list.begin_mutation();
            list_->push_back(list_->context, writable.mutable_data(), retained.view().binding(), retained.view().data());
        }
      private:
        hgraph::ValueTypeRef binding_{};
        const hgraph::IndexedValueOps *indexed_{};
        const hgraph::MutableListValueOps *list_{};
        std::vector<PreparedValuePlan> fields_{};
        hgraph::ValueTypeRef element_{};
        hgraph::ValueTypeRef key_{};
    };

    class PreparedDeltaPlan
    {
      public:
        PreparedDeltaPlan() = default;
        explicit PreparedDeltaPlan(const hgraph::TSValueTypeMetaData *shape)
            : value_{delta_schema(shape)}, native_{hgraph::ValuePlanFactory::instance().type_for(shape->delta_value_schema)} {
            if (shape->kind == hgraph::TSTypeKind::TS) {
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
