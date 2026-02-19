#include <hgraph/types/time_series/ts_meta_schema_cache.h>

#include <hgraph/types/time_series/link_target.h>
#include <hgraph/types/time_series/ref_link.h>
#include <hgraph/types/value/type_registry.h>

#include <functional>

namespace hgraph {
namespace {

using value::TypeMeta;
using value::TypeRegistry;

const TypeMeta* ensure_object_meta() {
    auto& registry = TypeRegistry::instance();
    if (const TypeMeta* meta = registry.get_by_name("object")) {
        return meta;
    }
    return registry.register_type<nb::object>("object");
}

value::type_ops make_link_target_ops() {
    using value::type_ops;
    type_ops ops{};

    ops.construct = [](void* dst, const TypeMeta*) {
        new (dst) LinkTarget();
    };
    ops.destroy = [](void* obj, const TypeMeta*) {
        static_cast<LinkTarget*>(obj)->~LinkTarget();
    };
    ops.copy = [](void* dst, const void* src, const TypeMeta*) {
        *static_cast<LinkTarget*>(dst) = *static_cast<const LinkTarget*>(src);
    };
    ops.move = [](void* dst, void* src, const TypeMeta*) {
        *static_cast<LinkTarget*>(dst) = std::move(*static_cast<LinkTarget*>(src));
    };
    ops.move_construct = [](void* dst, void* src, const TypeMeta*) {
        new (dst) LinkTarget(std::move(*static_cast<LinkTarget*>(src)));
    };
    ops.equals = [](const void* a, const void* b, const TypeMeta*) {
        const auto& lhs = *static_cast<const LinkTarget*>(a);
        const auto& rhs = *static_cast<const LinkTarget*>(b);
        return lhs.is_linked == rhs.is_linked && lhs.value_data == rhs.value_data && lhs.meta == rhs.meta;
    };
    ops.hash = [](const void* obj, const TypeMeta*) {
        const auto& value = *static_cast<const LinkTarget*>(obj);
        size_t h = std::hash<bool>{}(value.is_linked);
        h ^= std::hash<const void*>{}(value.value_data) << 1;
        h ^= std::hash<const void*>{}(value.meta) << 2;
        return h;
    };
    ops.to_string = [](const void* obj, const TypeMeta*) {
        const auto& value = *static_cast<const LinkTarget*>(obj);
        return value.is_linked ? std::string("LinkTarget(linked)") : std::string("LinkTarget(unlinked)");
    };
    ops.to_python = [](const void*, const TypeMeta*) {
        return nb::none();
    };
    ops.from_python = [](void* dst, const nb::object&, const TypeMeta*) {
        *static_cast<LinkTarget*>(dst) = LinkTarget();
    };

    ops.kind = value::TypeKind::Atomic;
    ops.specific.atomic = {
        [](const void* a, const void* b, const TypeMeta*) {
            return std::less<const void*>{}(a, b);
        }
    };

    return ops;
}

value::type_ops make_ref_link_ops() {
    using value::type_ops;
    type_ops ops{};

    ops.construct = [](void* dst, const TypeMeta*) {
        new (dst) REFLink();
    };
    ops.destroy = [](void* obj, const TypeMeta*) {
        static_cast<REFLink*>(obj)->~REFLink();
    };
    ops.copy = [](void* dst, const void* src, const TypeMeta*) {
        *static_cast<REFLink*>(dst) = *static_cast<const REFLink*>(src);
    };
    ops.move = [](void* dst, void* src, const TypeMeta*) {
        *static_cast<REFLink*>(dst) = std::move(*static_cast<REFLink*>(src));
    };
    ops.move_construct = [](void* dst, void* src, const TypeMeta*) {
        new (dst) REFLink(std::move(*static_cast<REFLink*>(src)));
    };
    ops.equals = [](const void* a, const void* b, const TypeMeta*) {
        const auto& lhs = *static_cast<const REFLink*>(a);
        const auto& rhs = *static_cast<const REFLink*>(b);
        return lhs.is_linked == rhs.is_linked && lhs.has_target() == rhs.has_target();
    };
    ops.hash = [](const void* obj, const TypeMeta*) {
        const auto& value = *static_cast<const REFLink*>(obj);
        size_t h = std::hash<bool>{}(value.is_linked);
        h ^= std::hash<bool>{}(value.has_target()) << 1;
        return h;
    };
    ops.to_string = [](const void* obj, const TypeMeta*) {
        const auto& value = *static_cast<const REFLink*>(obj);
        return value.is_linked ? std::string("REFLink(linked)") : std::string("REFLink(unlinked)");
    };
    ops.to_python = [](const void*, const TypeMeta*) {
        return nb::none();
    };
    ops.from_python = [](void* dst, const nb::object&, const TypeMeta*) {
        *static_cast<REFLink*>(dst) = REFLink();
    };

    ops.kind = value::TypeKind::Atomic;
    ops.specific.atomic = {
        [](const void* a, const void* b, const TypeMeta*) {
            return std::less<const void*>{}(a, b);
        }
    };

    return ops;
}

const TypeMeta* link_target_type_meta() {
    static const TypeMeta* meta = TypeRegistry::instance().register_type<LinkTarget>("LinkTarget", make_link_target_ops());
    return meta;
}

const TypeMeta* ref_link_type_meta() {
    static const TypeMeta* meta = TypeRegistry::instance().register_type<REFLink>("REFLink", make_ref_link_ops());
    return meta;
}

bool has_delta_descendants(const TSMeta* meta) {
    if (meta == nullptr) {
        return false;
    }

    switch (meta->kind) {
        case TSKind::TSS:
        case TSKind::TSD:
            return true;
        case TSKind::TSB:
            for (size_t i = 0; i < meta->field_count(); ++i) {
                if (has_delta_descendants(meta->fields()[i].ts_type)) {
                    return true;
                }
            }
            return false;
        case TSKind::TSL:
            return has_delta_descendants(meta->element_ts());
        default:
            return false;
    }
}

}  // namespace

TSMetaSchemaCache& TSMetaSchemaCache::instance() {
    static TSMetaSchemaCache cache;
    return cache;
}

const TSMetaSchemaSet& TSMetaSchemaCache::get(const TSMeta* meta) {
    std::lock_guard guard(mutex_);
    auto it = cache_.find(meta);
    if (it != cache_.end()) {
        return it->second;
    }

    auto [inserted_it, _] = cache_.emplace(meta, generate(meta));
    return inserted_it->second;
}

TSMetaSchemaSet TSMetaSchemaCache::generate(const TSMeta* meta) {
    TSMetaSchemaSet out;
    if (meta == nullptr) {
        return out;
    }

    out.value_schema = meta->value_type;
    out.time_schema = generate_time_schema_impl(meta);
    out.observer_schema = generate_observer_schema_impl(meta);
    out.delta_schema = generate_delta_schema_impl(meta);
    out.link_schema = generate_link_schema_impl(meta, false);
    out.input_link_schema = generate_link_schema_impl(meta, true);
    out.active_schema = generate_active_schema_impl(meta);

    return out;
}

const TypeMeta* TSMetaSchemaCache::generate_time_schema_impl(const TSMeta* meta) {
    auto& registry = TypeRegistry::instance();
    const TypeMeta* time_meta = value::scalar_type_meta<engine_time_t>();

    if (meta == nullptr) {
        return time_meta;
    }

    switch (meta->kind) {
        case TSKind::TSValue:
        case TSKind::TSS:
        case TSKind::TSW:
        case TSKind::REF:
        case TSKind::SIGNAL:
            return time_meta;

        case TSKind::TSB:
            {
                auto builder = registry.tuple();
                builder.add_element(time_meta);
                for (size_t i = 0; i < meta->field_count(); ++i) {
                    builder.add_element(generate_time_schema_impl(meta->fields()[i].ts_type));
                }
                return builder.build();
            }

        case TSKind::TSL:
            {
                const TypeMeta* child_time = generate_time_schema_impl(meta->element_ts());
                const TypeMeta* child_collection =
                    meta->fixed_size() > 0 ? registry.fixed_list(child_time, meta->fixed_size()).build()
                                         : registry.list(child_time).build();
                auto builder = registry.tuple();
                builder.add_element(time_meta);
                builder.add_element(child_collection);
                return builder.build();
            }

        case TSKind::TSD:
            {
                const TypeMeta* child_time = generate_time_schema_impl(meta->element_ts());
                auto builder = registry.tuple();
                builder.add_element(time_meta);
                builder.add_element(registry.list(child_time).build());
                return builder.build();
            }
    }

    return time_meta;
}

const TypeMeta* TSMetaSchemaCache::generate_observer_schema_impl(const TSMeta* meta) {
    auto& registry = TypeRegistry::instance();
    const TypeMeta* observer_leaf = ensure_object_meta();

    if (meta == nullptr) {
        return observer_leaf;
    }

    switch (meta->kind) {
        case TSKind::TSValue:
        case TSKind::TSS:
        case TSKind::TSW:
        case TSKind::REF:
        case TSKind::SIGNAL:
            return observer_leaf;

        case TSKind::TSB:
            {
                auto builder = registry.tuple();
                builder.add_element(observer_leaf);
                for (size_t i = 0; i < meta->field_count(); ++i) {
                    builder.add_element(generate_observer_schema_impl(meta->fields()[i].ts_type));
                }
                return builder.build();
            }

        case TSKind::TSL:
            {
                const TypeMeta* child_observer = generate_observer_schema_impl(meta->element_ts());
                const TypeMeta* child_collection =
                    meta->fixed_size() > 0 ? registry.fixed_list(child_observer, meta->fixed_size()).build()
                                         : registry.list(child_observer).build();
                auto builder = registry.tuple();
                builder.add_element(observer_leaf);
                builder.add_element(child_collection);
                return builder.build();
            }

        case TSKind::TSD:
            {
                const TypeMeta* child_observer = generate_observer_schema_impl(meta->element_ts());
                auto builder = registry.tuple();
                builder.add_element(observer_leaf);
                builder.add_element(registry.list(child_observer).build());
                return builder.build();
            }
    }

    return observer_leaf;
}

const TypeMeta* TSMetaSchemaCache::generate_link_schema_impl(const TSMeta* meta, bool input_mode) {
    auto& registry = TypeRegistry::instance();
    const TypeMeta* leaf = input_mode ? link_target_type_meta() : ref_link_type_meta();

    if (meta == nullptr) {
        return leaf;
    }

    switch (meta->kind) {
        case TSKind::TSValue:
        case TSKind::TSW:
        case TSKind::TSS:
        case TSKind::SIGNAL:
            return leaf;

        case TSKind::TSD:
            {
                // Dynamic dict containers carry a container link slot (0) plus
                // per-key child link storage in slot 1.
                const TypeMeta* child_link = generate_link_schema_impl(meta->element_ts(), input_mode);
                auto builder = registry.tuple();
                builder.add_element(leaf);  // container slot 0
                builder.add_element(registry.list(child_link != nullptr ? child_link : leaf).build());
                return builder.build();
            }

        case TSKind::REF:
            {
                // REF carries its own link slot (slot 0) and a nested link tree
                // for referred children (slot 1). This supports non-peered child
                // binding under REF containers.
                const TypeMeta* child_link = generate_link_schema_impl(meta->element_ts(), input_mode);
                auto builder = registry.tuple();
                builder.add_element(leaf);
                builder.add_element(child_link != nullptr ? child_link : leaf);
                return builder.build();
            }

        case TSKind::TSL:
            if (meta->fixed_size() > 0) {
                const TypeMeta* child_link = generate_link_schema_impl(meta->element_ts(), input_mode);
                auto builder = registry.tuple();
                builder.add_element(leaf);  // container slot 0
                builder.add_element(registry.fixed_list(child_link, meta->fixed_size()).build());
                return builder.build();
            }
            return leaf;

        case TSKind::TSB:
            {
                auto builder = registry.tuple();
                builder.add_element(leaf);  // container slot 0
                for (size_t i = 0; i < meta->field_count(); ++i) {
                    builder.add_element(generate_link_schema_impl(meta->fields()[i].ts_type, input_mode));
                }
                return builder.build();
            }
    }

    return leaf;
}

const TypeMeta* TSMetaSchemaCache::generate_active_schema_impl(const TSMeta* meta) {
    auto& registry = TypeRegistry::instance();
    const TypeMeta* bool_meta = value::scalar_type_meta<bool>();

    if (meta == nullptr) {
        return bool_meta;
    }

    switch (meta->kind) {
        case TSKind::TSValue:
        case TSKind::TSS:
        case TSKind::TSW:
        case TSKind::REF:
        case TSKind::SIGNAL:
            return bool_meta;

        case TSKind::TSB:
            {
                auto builder = registry.tuple();
                builder.add_element(bool_meta);
                for (size_t i = 0; i < meta->field_count(); ++i) {
                    builder.add_element(generate_active_schema_impl(meta->fields()[i].ts_type));
                }
                return builder.build();
            }

        case TSKind::TSL:
            {
                const TypeMeta* child_active = generate_active_schema_impl(meta->element_ts());
                const TypeMeta* child_collection =
                    meta->fixed_size() > 0 ? registry.fixed_list(child_active, meta->fixed_size()).build()
                                         : registry.list(child_active).build();
                auto builder = registry.tuple();
                builder.add_element(bool_meta);
                builder.add_element(child_collection);
                return builder.build();
            }

        case TSKind::TSD:
            {
                const TypeMeta* child_active = generate_active_schema_impl(meta->element_ts());
                const TypeMeta* child_collection = registry.list(child_active).build();
                auto builder = registry.tuple();
                builder.add_element(bool_meta);
                builder.add_element(child_collection);
                return builder.build();
            }
    }

    return bool_meta;
}

const TypeMeta* TSMetaSchemaCache::generate_delta_schema_impl(const TSMeta* meta) {
    auto& registry = TypeRegistry::instance();
    const TypeMeta* object_meta = ensure_object_meta();

    if (meta == nullptr || !has_delta_descendants(meta)) {
        return nullptr;
    }

    switch (meta->kind) {
        case TSKind::TSS:
            {
                auto builder = registry.tuple();
                builder.add_element(meta->value_type);
                builder.add_element(meta->value_type);
                return builder.build();
            }

        case TSKind::TSD:
            {
                auto builder = registry.tuple();
                const TypeMeta* value_type = meta->element_ts() != nullptr ? meta->element_ts()->value_type : object_meta;
                builder.add_element(registry.map(meta->key_type(), value_type).build());
                builder.add_element(registry.set(meta->key_type()).build());
                builder.add_element(registry.set(meta->key_type()).build());

                const TypeMeta* child_delta = generate_delta_schema_impl(meta->element_ts());
                if (child_delta != nullptr) {
                    builder.add_element(registry.list(child_delta).build());
                }
                return builder.build();
            }

        case TSKind::TSB:
            {
                auto builder = registry.tuple();
                for (size_t i = 0; i < meta->field_count(); ++i) {
                    const TypeMeta* child = generate_delta_schema_impl(meta->fields()[i].ts_type);
                    builder.add_element(child != nullptr ? child : object_meta);
                }
                return builder.build();
            }

        case TSKind::TSL:
            {
                const TypeMeta* child_delta = generate_delta_schema_impl(meta->element_ts());
                if (child_delta == nullptr) {
                    return nullptr;
                }
                if (meta->fixed_size() > 0) {
                    return registry.fixed_list(child_delta, meta->fixed_size()).build();
                }
                return registry.list(child_delta).build();
            }

        default:
            return nullptr;
    }
}

}  // namespace hgraph
