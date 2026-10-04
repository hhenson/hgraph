#ifndef HGL_ORDINARY_PATTERNS_H
#define HGL_ORDINARY_PATTERNS_H

#include <hgl/ordinary_values.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/type_pattern.h>

namespace hgl::ordinary
{
    // Recover a structural temporal shape from held metadata. Atomic boundaries
    // require exact Origin metadata or a previously bound source instead. This
    // wiring-only conversion never inspects a stored payload.
    inline const hgraph::TSValueTypeMetaData *held_source(const hgraph::ValueTypeMetaData *value) {
        using namespace hgraph;
        if (!value) { return nullptr; }
        auto &registry = TypeRegistry::instance();
        const std::array leaves{scalar_descriptor<Bool>::value_meta(), scalar_descriptor<Int>::value_meta(),
            scalar_descriptor<Float>::value_meta(), scalar_descriptor<Str>::value_meta(),
            scalar_descriptor<Date>::value_meta(), scalar_descriptor<Time>::value_meta(),
            scalar_descriptor<DateTime>::value_meta(), scalar_descriptor<TimeDelta>::value_meta()};
        if (std::ranges::find(leaves, value) != leaves.end()) { return registry.ts(value); }
        switch (value->value_kind()) {
            case ValueTypeKind::Set:
                if (value->element_type == leaves[0] || value->element_type == leaves[1]) {
                    return registry.tss(value->element_type);
                }
                return nullptr;
            case ValueTypeKind::List: {
                if (!value->is_fixed_size() || value->is_variadic_tuple() || TypeRegistry::is_array(value)) { return nullptr; }
                const auto *element = held_source(value->element_type);
                return element ? registry.tsl(element, value->fixed_size) : nullptr;
            }
            case ValueTypeKind::Map: {
                if (value->key_type != leaves[1]) { return nullptr; }
                const auto *element = held_source(value->element_type);
                return element ? registry.tsd(value->key_type, element) : nullptr;
            }
            case ValueTypeKind::Tuple:
            case ValueTypeKind::Bundle: {
                // Structural deltas are ordinary schemas but are not temporal
                // source types. Their sole argument records a source shape.
                if (value->name().starts_with("hgl.delta::")) { return nullptr; }
                std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
                fields.reserve(value->field_count);
                for (std::size_t i = 0; i < value->field_count; ++i) {
                    const auto *child = held_source(value->fields[i].type);
                    if (!child) { return nullptr; }
                    fields.emplace_back(value->value_kind() == ValueTypeKind::Tuple ? std::to_string(i)
                                                                                   : value->fields[i].name, child);
                }
                return value->is_named_bundle() ? registry.tsb(value->name(), fields) : registry.un_named_tsb(fields);
            }
            default: return nullptr;
        }
    }

    inline const hgraph::ValueTypeMetaData *held_schema(const hgraph::TSValueTypeMetaData *shape) {
        validate_delta_shape(shape);
        return shape->value_schema;
    }

    inline const hgraph::TSValueTypeMetaData *delta_source(const hgraph::ValueTypeMetaData *value) {
        if (!value) { return nullptr; }
        if (value->name().starts_with("hgl.delta::")) {
            const auto &arguments = value->bundle_generic_arguments();
            if (arguments.size() != 1) { return nullptr; }
            const auto *source = origin_source(arguments[0]);
            return source && delta_schema(source) == value ? source : nullptr;
        }
        // Composite payloads do not identify an atomic source. Only scalar
        // leaves have an unambiguous inverse without an already bound shape.
        if (value->try_value_kind() != hgraph::ValueTypeKind::Atomic) { return nullptr; }
        const auto *source = held_source(value);
        return source && source->kind == hgraph::TSTypeKind::TS ? source : nullptr;
    }

    template <typename Shape>
    inline hgraph::ScalarPattern projection(std::string name,
        const hgraph::TSValueTypeMetaData *(*source)(const hgraph::ValueTypeMetaData *),
        const hgraph::ValueTypeMetaData *(*value)(const hgraph::TSValueTypeMetaData *)) {
        hgraph::ScalarPattern pattern;
        pattern.kind = hgraph::ScalarPattern::Kind::SchemaProjection;
        pattern.name = std::move(name);
        pattern.projected = std::make_shared<const hgraph::TypePattern>(hgraph::to_pattern<Shape>());
        pattern.project_source = source;
        pattern.project_value = value;
        return pattern;
    }

    template <typename T>
    inline void unify(const hgraph::ValueTypeMetaData *value, hgraph::ResolutionMap &map) {
        auto candidate = map;
        if (!hgraph::scalar_pattern_match(hgraph::to_scalar_pattern<T>(), value, candidate)) {
            throw std::logic_error("ordinary value does not match its exact declared type");
        }
        map = std::move(candidate);
    }
}

namespace hgraph
{
    template <typename Shape> struct scalar_pattern_lower<hgl::ordinary::Origin<Shape>> {
        static ScalarPattern lower() {
            return hgl::ordinary::projection<Shape>("origin", hgl::ordinary::origin_source, hgl::ordinary::origin_schema);
        }
    };
    template <typename Shape> struct scalar_resolver<hgl::ordinary::Origin<Shape>> {
        static const ValueTypeMetaData *resolve(const ResolutionMap &map) {
            const auto *shape = ts_resolver<Shape>::resolve(map);
            return shape ? hgl::ordinary::origin_schema(shape) : nullptr;
        }
    };
    template <typename Shape> struct scalar_unifier<hgl::ordinary::Origin<Shape>> {
        static void unify(const ValueTypeMetaData *value, ResolutionMap &map) {
            hgl::ordinary::unify<hgl::ordinary::Origin<Shape>>(value, map);
        }
    };
    template <typename Shape> struct scalar_pattern_lower<hgl::ordinary::Held<Shape>> {
        static ScalarPattern lower() {
            return hgl::ordinary::projection<Shape>("held", hgl::ordinary::held_source, hgl::ordinary::held_schema);
        }
    };
    template <typename Shape> struct scalar_pattern_lower<hgl::ordinary::Delta<Shape>> {
        static ScalarPattern lower() {
            return hgl::ordinary::projection<Shape>("delta", hgl::ordinary::delta_source, hgl::ordinary::delta_schema);
        }
    };
    template <typename Element, std::int64_t Size> struct scalar_pattern_lower<hgl::ordinary::List<Element, Size>> {
        static ScalarPattern lower() {
            if constexpr (Size < 0) { return ScalarPattern::list(to_scalar_pattern<Element>()); }
            else { return ScalarPattern::list(to_scalar_pattern<Element>(), static_cast<std::size_t>(Size)); }
        }
    };
    template <typename Shape> struct scalar_resolver<hgl::ordinary::Held<Shape>> {
        static const ValueTypeMetaData *resolve(const ResolutionMap &map) {
            const auto *shape = ts_resolver<Shape>::resolve(map);
            return shape ? hgl::ordinary::held_schema(shape) : nullptr;
        }
    };
    template <typename Shape> struct scalar_resolver<hgl::ordinary::Delta<Shape>> {
        static const ValueTypeMetaData *resolve(const ResolutionMap &map) {
            const auto *shape = ts_resolver<Shape>::resolve(map);
            return shape ? hgl::ordinary::delta_schema(shape) : nullptr;
        }
    };
    template <typename Element, std::int64_t Size> struct scalar_resolver<hgl::ordinary::List<Element, Size>> {
        static const ValueTypeMetaData *resolve(const ResolutionMap &map) {
            const auto *element = scalar_resolver<Element>::resolve(map);
            if (!element) { return nullptr; }
            if constexpr (Size < 0) { return TypeRegistry::instance().list(element); }
            else { return TypeRegistry::instance().fixed_list(element, static_cast<std::size_t>(Size)); }
        }
    };
    template <typename Shape> struct scalar_unifier<hgl::ordinary::Held<Shape>> {
        static void unify(const ValueTypeMetaData *value, ResolutionMap &map) {
            hgl::ordinary::unify<hgl::ordinary::Held<Shape>>(value, map);
        }
    };
    template <typename Shape> struct scalar_unifier<hgl::ordinary::Delta<Shape>> {
        static void unify(const ValueTypeMetaData *value, ResolutionMap &map) {
            hgl::ordinary::unify<hgl::ordinary::Delta<Shape>>(value, map);
        }
    };
    template <typename Element, std::int64_t Size> struct scalar_unifier<hgl::ordinary::List<Element, Size>> {
        static void unify(const ValueTypeMetaData *value, ResolutionMap &map) {
            hgl::ordinary::unify<hgl::ordinary::List<Element, Size>>(value, map);
        }
    };

    template <fixed_string Name, typename Element, std::int64_t Size>
    class Scalar<Name, hgl::ordinary::List<Element, Size>> {
      public:
        using schema = hgl::ordinary::List<Element, Size>;
        static constexpr auto field_name = Name;
        explicit Scalar(const ValueView &view) noexcept : binding_{view.binding()}, data_{view.data()} {}
        [[nodiscard]] ValueView value() const noexcept { return ValueView{binding_, data_}; }
      private:
        ValueTypeRef binding_{};
        const void *data_{};
    };
}
#endif
