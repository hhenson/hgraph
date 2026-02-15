#include <hgraph/api/python/py_ts_runtime_internal.h>

#include <hgraph/python/chrono.h>
#include <hgraph/types/time_series/link_target.h>
#include <hgraph/types/time_series/ref_link.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_meta_schema_cache.h>
#include <hgraph/types/time_series/ts_ops.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/value/type_registry.h>
#include <hgraph/types/value/value.h>

#include <nanobind/stl/string.h>
#include <nanobind/stl/optional.h>
#include <nanobind/stl/vector.h>

#include <cstdint>
#include <optional>

namespace hgraph {
namespace {

using value::View;

std::vector<size_t> ts_path_to_link_path(const TSMeta* root_meta, const std::vector<size_t>& ts_path) {
    std::vector<size_t> out;
    const TSMeta* meta = root_meta;

    for (size_t index : ts_path) {
        if (meta == nullptr) {
            break;
        }

        switch (meta->kind) {
            case TSKind::TSB:
                out.push_back(index + 1);  // slot 0 is container link
                if (meta->fields() == nullptr || index >= meta->field_count()) {
                    return out;
                }
                meta = meta->fields()[index].ts_type;
                break;

            case TSKind::TSL:
                if (meta->fixed_size() > 0) {
                    out.push_back(index);
                }
                meta = meta->element_ts();
                break;

            case TSKind::TSD:
            case TSKind::REF:
                meta = meta->element_ts();
                break;

            default:
                return out;
        }
    }

    if (meta != nullptr && meta->kind == TSKind::TSB) {
        out.push_back(0);
    }

    return out;
}

std::optional<View> navigate_const(View view, const std::vector<size_t>& indices) {
    View current = view;
    for (size_t index : indices) {
        if (!current.valid()) {
            return std::nullopt;
        }

        if (current.is_tuple()) {
            auto tuple = current.as_tuple();
            if (index >= tuple.size()) {
                return std::nullopt;
            }
            current = tuple.at(index);
            continue;
        }

        if (current.is_list()) {
            auto list = current.as_list();
            if (index >= list.size()) {
                return std::nullopt;
            }
            current = list.at(index);
            continue;
        }

        return std::nullopt;
    }
    return current;
}

std::optional<View> resolve_link_payload(const TSView& ts_view) {
    const ViewData& vd = ts_view.view_data();
    auto* link_root = static_cast<const value::Value*>(vd.link_data);
    if (link_root == nullptr || !link_root->has_value()) {
        return std::nullopt;
    }

    const auto link_path = ts_path_to_link_path(vd.meta, vd.path.indices);
    return navigate_const(link_root->view(), link_path);
}

std::vector<size_t> linked_source_indices(const TSView& ts_view) {
    auto payload = resolve_link_payload(ts_view);
    if (!payload.has_value() || !payload->valid()) {
        return {};
    }

    const value::TypeMeta* ref_link_meta = value::TypeRegistry::instance().get_by_name("REFLink");
    if (ref_link_meta == nullptr || payload->schema() != ref_link_meta) {
        return {};
    }

    const auto* link = static_cast<const REFLink*>(payload->data());
    if (link == nullptr || !link->is_linked) {
        return {};
    }
    return link->source.path.indices;
}

std::vector<size_t> linked_target_indices(const TSView& ts_view) {
    auto payload = resolve_link_payload(ts_view);
    if (!payload.has_value() || !payload->valid()) {
        return {};
    }

    const value::TypeMeta* link_target_meta = value::TypeRegistry::instance().get_by_name("LinkTarget");
    if (link_target_meta == nullptr || payload->schema() != link_target_meta) {
        return {};
    }

    const auto* link = static_cast<const LinkTarget*>(payload->data());
    if (link == nullptr || !link->is_linked) {
        return {};
    }
    return link->target_path.indices;
}

}  // namespace

void ts_runtime_internal_register_with_nanobind(nb::module_& m) {
    using namespace nanobind::literals;

    auto test_mod = m.def_submodule("_ts_runtime", "Private TS runtime scaffolding bindings for tests");

    test_mod.def(
        "ops_ptr_for_kind",
        [](TSKind kind) { return reinterpret_cast<uintptr_t>(get_ts_ops(kind)); },
        "kind"_a);
    test_mod.def(
        "ops_ptr_for_meta",
        [](const TSMeta* meta) { return reinterpret_cast<uintptr_t>(get_ts_ops(meta)); },
        "meta"_a);
    test_mod.def(
        "ops_kind_for_kind",
        [](TSKind kind) { return get_ts_ops(kind)->kind; },
        "kind"_a);
    test_mod.def(
        "ops_has_window_for_kind",
        [](TSKind kind) { return get_ts_ops(kind)->window_ops() != nullptr; },
        "kind"_a);
    test_mod.def(
        "ops_has_set_for_kind",
        [](TSKind kind) { return get_ts_ops(kind)->set_ops() != nullptr; },
        "kind"_a);
    test_mod.def(
        "ops_has_dict_for_kind",
        [](TSKind kind) { return get_ts_ops(kind)->dict_ops() != nullptr; },
        "kind"_a);
    test_mod.def(
        "ops_has_list_for_kind",
        [](TSKind kind) { return get_ts_ops(kind)->list_ops() != nullptr; },
        "kind"_a);
    test_mod.def(
        "ops_has_bundle_for_kind",
        [](TSKind kind) { return get_ts_ops(kind)->bundle_ops() != nullptr; },
        "kind"_a);
    test_mod.def(
        "schema_value_meta",
        [](const TSMeta* meta) -> const value::TypeMeta* {
            if (meta == nullptr) {
                return nullptr;
            }
            return TSMetaSchemaCache::instance().get(meta).value_schema;
        },
        "meta"_a,
        nb::rv_policy::reference);
    test_mod.def(
        "schema_time_meta",
        [](const TSMeta* meta) -> const value::TypeMeta* {
            if (meta == nullptr) {
                return nullptr;
            }
            return TSMetaSchemaCache::instance().get(meta).time_schema;
        },
        "meta"_a,
        nb::rv_policy::reference);
    test_mod.def(
        "schema_observer_meta",
        [](const TSMeta* meta) -> const value::TypeMeta* {
            if (meta == nullptr) {
                return nullptr;
            }
            return TSMetaSchemaCache::instance().get(meta).observer_schema;
        },
        "meta"_a,
        nb::rv_policy::reference);
    test_mod.def(
        "schema_delta_meta",
        [](const TSMeta* meta) -> const value::TypeMeta* {
            if (meta == nullptr) {
                return nullptr;
            }
            return TSMetaSchemaCache::instance().get(meta).delta_schema;
        },
        "meta"_a,
        nb::rv_policy::reference);
    test_mod.def(
        "schema_link_meta",
        [](const TSMeta* meta) -> const value::TypeMeta* {
            if (meta == nullptr) {
                return nullptr;
            }
            return TSMetaSchemaCache::instance().get(meta).link_schema;
        },
        "meta"_a,
        nb::rv_policy::reference);
    test_mod.def(
        "schema_input_link_meta",
        [](const TSMeta* meta) -> const value::TypeMeta* {
            if (meta == nullptr) {
                return nullptr;
            }
            return TSMetaSchemaCache::instance().get(meta).input_link_schema;
        },
        "meta"_a,
        nb::rv_policy::reference);
    test_mod.def(
        "schema_active_meta",
        [](const TSMeta* meta) -> const value::TypeMeta* {
            if (meta == nullptr) {
                return nullptr;
            }
            return TSMetaSchemaCache::instance().get(meta).active_schema;
        },
        "meta"_a,
        nb::rv_policy::reference);

    nb::class_<TSOutput>(test_mod, "TSOutput")
        .def("__init__", [](TSOutput* self, const TSMeta* meta, size_t port_index) {
            new (self) TSOutput(meta, nullptr, port_index);
        }, "meta"_a, "port_index"_a = 0)
        .def("output_view",
             [](TSOutput& self, engine_time_t current_time) { return self.output_view(current_time); },
             "current_time"_a, nb::keep_alive<0, 1>())
        .def("output_view",
             [](TSOutput& self, engine_time_t current_time, const TSMeta* schema) {
                 return self.output_view(current_time, schema);
             },
             "current_time"_a, "schema"_a, nb::keep_alive<0, 1>());

    nb::class_<TSInput>(test_mod, "TSInput")
        .def("__init__", [](TSInput* self, const TSMeta* meta) {
            new (self) TSInput(meta, nullptr);
        }, "meta"_a)
        .def("input_view",
             [](TSInput& self, engine_time_t current_time) { return self.input_view(current_time); },
             "current_time"_a, nb::keep_alive<0, 1>())
        .def("input_view",
             [](TSInput& self, engine_time_t current_time, const TSMeta* schema) {
                 return self.input_view(current_time, schema);
             },
             "current_time"_a, "schema"_a, nb::keep_alive<0, 1>())
        .def("bind", &TSInput::bind, "output"_a, "current_time"_a)
        .def("unbind", &TSInput::unbind, "current_time"_a)
        .def("set_active", nb::overload_cast<bool>(&TSInput::set_active), "active"_a)
        .def("active", nb::overload_cast<>(&TSInput::active, nb::const_))
        .def("active_at", [](const TSInput& self, const TSInputView& view) {
            return self.active(view.as_ts_view());
        }, "view"_a);

    auto ts_view_cls = nb::class_<TSView>(test_mod, "TSView")
        .def("__bool__", [](const TSView& self) { return static_cast<bool>(self); })
        .def("fq_path_str", [](const TSView& self) { return self.fq_path().to_string(); })
        .def("short_indices", [](const TSView& self) { return self.short_path().indices; })
        .def("at", [](const TSView& self, size_t index) { return self.child_at(index); }, "index"_a, nb::keep_alive<0, 1>())
        .def("field", [](const TSView& self, std::string_view name) { return self.child_by_name(name); }, "name"_a, nb::keep_alive<0, 1>())
        .def("at_key", [](const TSView& self, const value::View& key) { return self.child_by_key(key); }, "key"_a, nb::keep_alive<0, 1>())
        .def("count", &TSView::child_count)
        .def("size", &TSView::size)
        .def("to_python", &TSView::to_python)
        .def("delta_to_python", &TSView::delta_to_python)
        .def("set_value", &TSView::set_value, "value"_a)
        .def("from_python", &TSView::from_python, "value"_a)
        .def("kind", &TSView::kind)
        .def("is_window", &TSView::is_window)
        .def("is_set", &TSView::is_set)
        .def("is_dict", &TSView::is_dict)
        .def("is_list", &TSView::is_list)
        .def("is_bundle", &TSView::is_bundle);

    nb::class_<TSWView, TSView>(test_mod, "TSWView")
        .def("__bool__", [](const TSWView& self) { return static_cast<bool>(self); })
        .def("to_python", [](const TSWView& self) { return self.to_python(); })
        .def("value_times_count", &TSWView::value_times_count)
        .def("value_times", [](const TSWView& self) {
            std::vector<engine_time_t> out;
            const engine_time_t* times = self.value_times();
            const size_t count = self.value_times_count();
            if (times == nullptr || count == 0) {
                return out;
            }
            out.reserve(count);
            for (size_t i = 0; i < count; ++i) {
                out.push_back(times[i]);
            }
            return out;
        })
        .def("first_modified_time", &TSWView::first_modified_time)
        .def("has_removed_value", &TSWView::has_removed_value)
        .def("removed_value", &TSWView::removed_value)
        .def("removed_value_count", &TSWView::removed_value_count)
        .def("size", &TSWView::size)
        .def("min_size", &TSWView::min_size)
        .def("length", &TSWView::length);

    nb::class_<TSSView, TSView>(test_mod, "TSSView")
        .def("__bool__", [](const TSSView& self) { return static_cast<bool>(self); })
        .def("to_python", [](const TSSView& self) { return self.to_python(); })
        .def("add", &TSSView::add, "elem"_a)
        .def("remove", &TSSView::remove, "elem"_a)
        .def("clear", &TSSView::clear);

    nb::class_<TSDView, TSView>(test_mod, "TSDView")
        .def("__bool__", [](const TSDView& self) { return static_cast<bool>(self); })
        .def("to_python", [](const TSDView& self) { return self.to_python(); })
        .def("at_key", &TSDView::by_key, "key"_a, nb::keep_alive<0, 1>())
        .def("count", &TSDView::count)
        .def("size", &TSDView::size)
        .def("remove", &TSDView::remove, "key"_a)
        .def("create", &TSDView::create, "key"_a, nb::keep_alive<0, 1>())
        .def("set", &TSDView::set, "key"_a, "value"_a, nb::keep_alive<0, 1>());

    nb::class_<TSLView, TSView>(test_mod, "TSLView")
        .def("__bool__", [](const TSLView& self) { return static_cast<bool>(self); })
        .def("to_python", [](const TSLView& self) { return self.to_python(); })
        .def("at", &TSLView::at, "index"_a, nb::keep_alive<0, 1>())
        .def("count", &TSLView::count)
        .def("size", &TSLView::size);

    nb::class_<TSBView, TSView>(test_mod, "TSBView")
        .def("__bool__", [](const TSBView& self) { return static_cast<bool>(self); })
        .def("to_python", [](const TSBView& self) { return self.to_python(); })
        .def("at", nb::overload_cast<size_t>(&TSBView::at, nb::const_), "index"_a, nb::keep_alive<0, 1>())
        .def("field", &TSBView::field, "name"_a, nb::keep_alive<0, 1>())
        .def("count", &TSBView::count)
        .def("size", &TSBView::size);

    ts_view_cls
        .def("try_as_window", &TSView::try_as_window, nb::keep_alive<0, 1>())
        .def("try_as_set", &TSView::try_as_set, nb::keep_alive<0, 1>())
        .def("try_as_dict", &TSView::try_as_dict, nb::keep_alive<0, 1>())
        .def("try_as_list", &TSView::try_as_list, nb::keep_alive<0, 1>())
        .def("try_as_bundle", &TSView::try_as_bundle, nb::keep_alive<0, 1>())
        .def("as_window", &TSView::as_window, nb::keep_alive<0, 1>())
        .def("as_set", &TSView::as_set, nb::keep_alive<0, 1>())
        .def("as_dict", &TSView::as_dict, nb::keep_alive<0, 1>())
        .def("as_list", &TSView::as_list, nb::keep_alive<0, 1>())
        .def("as_bundle", &TSView::as_bundle, nb::keep_alive<0, 1>());

    auto ts_output_view_cls = nb::class_<TSOutputView>(test_mod, "TSOutputView")
        .def("__bool__", [](const TSOutputView& self) { return static_cast<bool>(self); })
        .def("__len__", &TSOutputView::child_count)
        .def("__getitem__", [](const TSOutputView& self, size_t index) { return self.child_at(index); }, "index"_a,
             nb::keep_alive<0, 1>())
        .def("__getitem__", [](const TSOutputView& self, std::string_view name) { return self.child_by_name(name); },
             "name"_a, nb::keep_alive<0, 1>())
        .def("__getitem__", [](const TSOutputView& self, const value::View& key) { return self.child_by_key(key); },
             "key"_a, nb::keep_alive<0, 1>())
        .def("fq_path_str", [](const TSOutputView& self) { return self.fq_path().to_string(); })
        .def("short_indices", [](const TSOutputView& self) { return self.short_path().indices; })
        .def("at", &TSOutputView::child_at, "index"_a, nb::keep_alive<0, 1>())
        .def("field", &TSOutputView::child_by_name, "name"_a, nb::keep_alive<0, 1>())
        .def("at_key", &TSOutputView::child_by_key, "key"_a, nb::keep_alive<0, 1>())
        .def("count", &TSOutputView::child_count)
        .def("child_at", &TSOutputView::child_at, "index"_a, nb::keep_alive<0, 1>())
        .def("child_by_name", &TSOutputView::child_by_name, "name"_a, nb::keep_alive<0, 1>())
        .def("child_by_key", &TSOutputView::child_by_key, "key"_a, nb::keep_alive<0, 1>())
        .def("size", &TSOutputView::child_count)
        .def("set_value", &TSOutputView::set_value, "value"_a)
        .def("to_python", [](const TSOutputView& self) { return self.to_python(); })
        .def("delta_to_python", [](const TSOutputView& self) { return self.delta_to_python(); })
        .def("py_value", [](const TSOutputView& self) { return self.to_python(); })
        .def("py_delta_value", [](const TSOutputView& self) { return self.delta_to_python(); })
        .def_prop_ro("value", [](const TSOutputView& self) { return self.to_python(); })
        .def_prop_ro("delta_value", [](const TSOutputView& self) { return self.delta_to_python(); })
        .def_prop_ro("valid", &TSOutputView::valid)
        .def_prop_ro("all_valid", &TSOutputView::all_valid)
        .def_prop_ro("modified", &TSOutputView::modified)
        .def_prop_ro("last_modified_time", &TSOutputView::last_modified_time)
        .def("from_python", [](TSOutputView& self, const nb::object& value_obj) {
            self.from_python(value_obj);
        }, "value"_a)
        .def("apply_result", [](TSOutputView& self, const nb::object& value_obj) {
            self.from_python(value_obj);
        }, "value"_a)
        .def("can_apply_result", [](const TSOutputView& self, const nb::object&) {
            return static_cast<bool>(self);
        }, "value"_a)
        .def("invalidate", &TSOutputView::invalidate)
        .def("sampled", [](const TSOutputView& self) { return self.as_ts_view().sampled(); })
        .def("kind", [](const TSOutputView& self) { return self.as_ts_view().kind(); })
        .def("is_window", [](const TSOutputView& self) { return self.as_ts_view().is_window(); })
        .def("is_set", [](const TSOutputView& self) { return self.as_ts_view().is_set(); })
        .def("is_dict", [](const TSOutputView& self) { return self.as_ts_view().is_dict(); })
        .def("is_list", [](const TSOutputView& self) { return self.as_ts_view().is_list(); })
        .def("is_bundle", [](const TSOutputView& self) { return self.as_ts_view().is_bundle(); })
        .def("set_sampled", [](TSOutputView& self, bool sampled) {
            self.as_ts_view().view_data().sampled = sampled;
        }, "sampled"_a)
        .def("is_bound", [](const TSOutputView& self) { return self.as_ts_view().is_bound(); })
        .def("has_window_ops", [](const TSOutputView& self) { return self.try_as_window().has_value(); })
        .def("window_value_times_count", [](const TSOutputView& self) {
            auto window = self.try_as_window();
            return window.has_value() ? window->value_times_count() : size_t{0};
        })
        .def("window_value_times", [](const TSOutputView& self) {
            std::vector<engine_time_t> out;
            auto window = self.try_as_window();
            if (!window.has_value()) {
                return out;
            }
            const engine_time_t* times = window->value_times();
            const size_t count = window->value_times_count();
            if (times == nullptr || count == 0) {
                return out;
            }
            out.reserve(count);
            for (size_t i = 0; i < count; ++i) {
                out.push_back(times[i]);
            }
            return out;
        })
        .def("window_first_modified_time", [](const TSOutputView& self) {
            auto window = self.try_as_window();
            return window.has_value() ? window->first_modified_time() : MIN_DT;
        })
        .def("window_has_removed_value", [](const TSOutputView& self) {
            auto window = self.try_as_window();
            return window.has_value() && window->has_removed_value();
        })
        .def("window_removed_value_count", [](const TSOutputView& self) {
            auto window = self.try_as_window();
            return window.has_value() ? window->removed_value_count() : size_t{0};
        })
        .def("window_size", [](const TSOutputView& self) {
            auto window = self.try_as_window();
            return window.has_value() ? window->size() : size_t{0};
        })
        .def("window_min_size", [](const TSOutputView& self) {
            auto window = self.try_as_window();
            return window.has_value() ? window->min_size() : size_t{0};
        })
        .def("window_length", [](const TSOutputView& self) {
            auto window = self.try_as_window();
            return window.has_value() ? window->length() : size_t{0};
        })
        .def("has_set_ops", [](const TSOutputView& self) { return self.try_as_set().has_value(); })
        .def("set_add", [](TSOutputView& self, const value::View& elem) {
            auto set = self.try_as_set();
            return set.has_value() && set->add(elem);
        }, "elem"_a)
        .def("set_remove", [](TSOutputView& self, const value::View& elem) {
            auto set = self.try_as_set();
            return set.has_value() && set->remove(elem);
        }, "elem"_a)
        .def("set_clear", [](TSOutputView& self) {
            auto set = self.try_as_set();
            if (set.has_value()) {
                set->clear();
            }
        })
        .def("has_dict_ops", [](const TSOutputView& self) { return self.try_as_dict().has_value(); })
        .def("dict_remove", [](TSOutputView& self, const value::View& key) {
            auto dict = self.try_as_dict();
            return dict.has_value() && dict->remove(key);
        }, "key"_a)
        .def("dict_create", [](TSOutputView& self, const value::View& key) {
            auto dict = self.try_as_dict();
            return dict.has_value() ? dict->create(key) : TSOutputView{};
        }, "key"_a, nb::keep_alive<0, 1>())
        .def("dict_set", [](TSOutputView& self, const value::View& key, const value::View& value_view) {
            auto dict = self.try_as_dict();
            return dict.has_value() ? dict->set(key, value_view) : TSOutputView{};
        }, "key"_a, "value"_a, nb::keep_alive<0, 1>())
        .def("linked_source_indices", [](const TSOutputView& self) {
            return linked_source_indices(self.as_ts_view());
        });

    nb::class_<TSWOutputView, TSOutputView>(test_mod, "TSWOutputView")
        .def("__bool__", [](const TSWOutputView& self) { return static_cast<bool>(self); })
        .def("value_times_count", &TSWOutputView::value_times_count)
        .def("value_times", [](const TSWOutputView& self) {
            std::vector<engine_time_t> out;
            const engine_time_t* times = self.value_times();
            const size_t count = self.value_times_count();
            if (times == nullptr || count == 0) {
                return out;
            }
            out.reserve(count);
            for (size_t i = 0; i < count; ++i) {
                out.push_back(times[i]);
            }
            return out;
        })
        .def("first_modified_time", &TSWOutputView::first_modified_time)
        .def("has_removed_value", &TSWOutputView::has_removed_value)
        .def("removed_value_count", &TSWOutputView::removed_value_count)
        .def("size", &TSWOutputView::size)
        .def("min_size", &TSWOutputView::min_size)
        .def("length", &TSWOutputView::length);

    nb::class_<TSSOutputView, TSOutputView>(test_mod, "TSSOutputView")
        .def("__bool__", [](const TSSOutputView& self) { return static_cast<bool>(self); })
        .def("add", &TSSOutputView::add, "elem"_a)
        .def("remove", &TSSOutputView::remove, "elem"_a)
        .def("clear", &TSSOutputView::clear);

    nb::class_<TSDOutputView, TSOutputView>(test_mod, "TSDOutputView")
        .def("__bool__", [](const TSDOutputView& self) { return static_cast<bool>(self); })
        .def("at_key", &TSDOutputView::at_key, "key"_a, nb::keep_alive<0, 1>())
        .def("count", &TSDOutputView::count)
        .def("size", &TSDOutputView::size)
        .def("remove", &TSDOutputView::remove, "key"_a)
        .def("create", &TSDOutputView::create, "key"_a, nb::keep_alive<0, 1>())
        .def("set", &TSDOutputView::set, "key"_a, "value"_a, nb::keep_alive<0, 1>());

    nb::class_<TSLOutputView, TSOutputView>(test_mod, "TSLOutputView")
        .def("__bool__", [](const TSLOutputView& self) { return static_cast<bool>(self); })
        .def("at", &TSLOutputView::at, "index"_a, nb::keep_alive<0, 1>())
        .def("count", &TSLOutputView::count)
        .def("size", &TSLOutputView::size);

    nb::class_<TSBOutputView, TSOutputView>(test_mod, "TSBOutputView")
        .def("__bool__", [](const TSBOutputView& self) { return static_cast<bool>(self); })
        .def("at", nb::overload_cast<size_t>(&TSBOutputView::at, nb::const_), "index"_a, nb::keep_alive<0, 1>())
        .def("field", &TSBOutputView::field, "name"_a, nb::keep_alive<0, 1>())
        .def("count", &TSBOutputView::count)
        .def("size", &TSBOutputView::size);

    ts_output_view_cls
        .def("try_as_window", &TSOutputView::try_as_window, nb::keep_alive<0, 1>())
        .def("try_as_set", &TSOutputView::try_as_set, nb::keep_alive<0, 1>())
        .def("try_as_dict", &TSOutputView::try_as_dict, nb::keep_alive<0, 1>())
        .def("try_as_list", &TSOutputView::try_as_list, nb::keep_alive<0, 1>())
        .def("try_as_bundle", &TSOutputView::try_as_bundle, nb::keep_alive<0, 1>())
        .def("as_window", &TSOutputView::as_window, nb::keep_alive<0, 1>())
        .def("as_set", &TSOutputView::as_set, nb::keep_alive<0, 1>())
        .def("as_dict", &TSOutputView::as_dict, nb::keep_alive<0, 1>())
        .def("as_list", &TSOutputView::as_list, nb::keep_alive<0, 1>())
        .def("as_bundle", &TSOutputView::as_bundle, nb::keep_alive<0, 1>());

    auto ts_input_view_cls = nb::class_<TSInputView>(test_mod, "TSInputView")
        .def("__bool__", [](const TSInputView& self) { return static_cast<bool>(self); })
        .def("__len__", &TSInputView::child_count)
        .def("__getitem__", [](const TSInputView& self, size_t index) { return self.child_at(index); }, "index"_a,
             nb::keep_alive<0, 1>())
        .def("__getitem__", [](const TSInputView& self, std::string_view name) { return self.child_by_name(name); },
             "name"_a, nb::keep_alive<0, 1>())
        .def("__getitem__", [](const TSInputView& self, const value::View& key) { return self.child_by_key(key); },
             "key"_a, nb::keep_alive<0, 1>())
        .def("fq_path_str", [](const TSInputView& self) { return self.fq_path().to_string(); })
        .def("short_indices", [](const TSInputView& self) { return self.short_path().indices; })
        .def("at", &TSInputView::child_at, "index"_a, nb::keep_alive<0, 1>())
        .def("field", &TSInputView::child_by_name, "name"_a, nb::keep_alive<0, 1>())
        .def("at_key", &TSInputView::child_by_key, "key"_a, nb::keep_alive<0, 1>())
        .def("count", &TSInputView::child_count)
        .def("child_at", &TSInputView::child_at, "index"_a, nb::keep_alive<0, 1>())
        .def("child_by_name", &TSInputView::child_by_name, "name"_a, nb::keep_alive<0, 1>())
        .def("child_by_key", &TSInputView::child_by_key, "key"_a, nb::keep_alive<0, 1>())
        .def("size", &TSInputView::child_count)
        .def("bind", &TSInputView::bind, "output"_a)
        .def("bind_output", &TSInputView::bind, "output"_a)
        .def("unbind", &TSInputView::unbind)
        .def("un_bind_output", [](TSInputView& self, bool) { self.unbind(); }, "unbind_refs"_a = true)
        .def("start", []() {})
        .def("to_python", [](const TSInputView& self) { return self.to_python(); })
        .def("delta_to_python", [](const TSInputView& self) { return self.delta_to_python(); })
        .def("py_value", [](const TSInputView& self) { return self.to_python(); })
        .def("py_delta_value", [](const TSInputView& self) { return self.delta_to_python(); })
        .def_prop_ro("value", [](const TSInputView& self) { return self.to_python(); })
        .def_prop_ro("delta_value", [](const TSInputView& self) { return self.delta_to_python(); })
        .def_prop_ro("valid", &TSInputView::valid)
        .def_prop_ro("all_valid", &TSInputView::all_valid)
        .def_prop_ro("modified", &TSInputView::modified)
        .def_prop_ro("last_modified_time", &TSInputView::last_modified_time)
        .def_prop_ro("has_peer", &TSInputView::is_bound)
        .def("sampled", [](const TSInputView& self) { return self.as_ts_view().sampled(); })
        .def("kind", [](const TSInputView& self) { return self.as_ts_view().kind(); })
        .def("is_window", [](const TSInputView& self) { return self.as_ts_view().is_window(); })
        .def("is_set", [](const TSInputView& self) { return self.as_ts_view().is_set(); })
        .def("is_dict", [](const TSInputView& self) { return self.as_ts_view().is_dict(); })
        .def("is_list", [](const TSInputView& self) { return self.as_ts_view().is_list(); })
        .def("is_bundle", [](const TSInputView& self) { return self.as_ts_view().is_bundle(); })
        .def("set_sampled", [](TSInputView& self, bool sampled) {
            self.as_ts_view().view_data().sampled = sampled;
        }, "sampled"_a)
        .def("is_bound", &TSInputView::is_bound)
        .def("has_window_ops", [](const TSInputView& self) { return self.try_as_window().has_value(); })
        .def("window_value_times_count", [](const TSInputView& self) {
            auto window = self.try_as_window();
            return window.has_value() ? window->value_times_count() : size_t{0};
        })
        .def("window_first_modified_time", [](const TSInputView& self) {
            auto window = self.try_as_window();
            return window.has_value() ? window->first_modified_time() : MIN_DT;
        })
        .def("window_has_removed_value", [](const TSInputView& self) {
            auto window = self.try_as_window();
            return window.has_value() && window->has_removed_value();
        })
        .def("window_removed_value_count", [](const TSInputView& self) {
            auto window = self.try_as_window();
            return window.has_value() ? window->removed_value_count() : size_t{0};
        })
        .def("window_size", [](const TSInputView& self) {
            auto window = self.try_as_window();
            return window.has_value() ? window->size() : size_t{0};
        })
        .def("window_min_size", [](const TSInputView& self) {
            auto window = self.try_as_window();
            return window.has_value() ? window->min_size() : size_t{0};
        })
        .def("window_length", [](const TSInputView& self) {
            auto window = self.try_as_window();
            return window.has_value() ? window->length() : size_t{0};
        })
        .def("has_set_ops", [](const TSInputView& self) { return self.try_as_set().has_value(); })
        .def("has_dict_ops", [](const TSInputView& self) { return self.try_as_dict().has_value(); })
        .def("make_active", &TSInputView::make_active)
        .def("make_passive", &TSInputView::make_passive)
        .def_prop_ro("active", &TSInputView::active)
        .def("linked_target_indices", [](const TSInputView& self) {
            return linked_target_indices(self.as_ts_view());
        });

    nb::class_<TSWInputView, TSInputView>(test_mod, "TSWInputView")
        .def("__bool__", [](const TSWInputView& self) { return static_cast<bool>(self); })
        .def("value_times_count", &TSWInputView::value_times_count)
        .def("first_modified_time", &TSWInputView::first_modified_time)
        .def("has_removed_value", &TSWInputView::has_removed_value)
        .def("removed_value_count", &TSWInputView::removed_value_count)
        .def("size", &TSWInputView::size)
        .def("min_size", &TSWInputView::min_size)
        .def("length", &TSWInputView::length);

    nb::class_<TSSInputView, TSInputView>(test_mod, "TSSInputView")
        .def("__bool__", [](const TSSInputView& self) { return static_cast<bool>(self); });

    nb::class_<TSDInputView, TSInputView>(test_mod, "TSDInputView")
        .def("__bool__", [](const TSDInputView& self) { return static_cast<bool>(self); })
        .def("at_key", &TSDInputView::at_key, "key"_a, nb::keep_alive<0, 1>())
        .def("count", &TSDInputView::count)
        .def("size", &TSDInputView::size);

    nb::class_<TSLInputView, TSInputView>(test_mod, "TSLInputView")
        .def("__bool__", [](const TSLInputView& self) { return static_cast<bool>(self); })
        .def("at", &TSLInputView::at, "index"_a, nb::keep_alive<0, 1>())
        .def("count", &TSLInputView::count)
        .def("size", &TSLInputView::size);

    nb::class_<TSBInputView, TSInputView>(test_mod, "TSBInputView")
        .def("__bool__", [](const TSBInputView& self) { return static_cast<bool>(self); })
        .def("at", nb::overload_cast<size_t>(&TSBInputView::at, nb::const_), "index"_a, nb::keep_alive<0, 1>())
        .def("field", &TSBInputView::field, "name"_a, nb::keep_alive<0, 1>())
        .def("count", &TSBInputView::count)
        .def("size", &TSBInputView::size);

    ts_input_view_cls
        .def("try_as_window", &TSInputView::try_as_window, nb::keep_alive<0, 1>())
        .def("try_as_set", &TSInputView::try_as_set, nb::keep_alive<0, 1>())
        .def("try_as_dict", &TSInputView::try_as_dict, nb::keep_alive<0, 1>())
        .def("try_as_list", &TSInputView::try_as_list, nb::keep_alive<0, 1>())
        .def("try_as_bundle", &TSInputView::try_as_bundle, nb::keep_alive<0, 1>())
        .def("as_window", &TSInputView::as_window, nb::keep_alive<0, 1>())
        .def("as_set", &TSInputView::as_set, nb::keep_alive<0, 1>())
        .def("as_dict", &TSInputView::as_dict, nb::keep_alive<0, 1>())
        .def("as_list", &TSInputView::as_list, nb::keep_alive<0, 1>())
        .def("as_bundle", &TSInputView::as_bundle, nb::keep_alive<0, 1>());
}

}  // namespace hgraph
