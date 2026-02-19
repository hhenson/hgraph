#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/api/python/py_time_series.h>

#include <fmt/format.h>
#include <hgraph/nodes/base_python_node.h>
#include <hgraph/runtime/evaluation_engine.h>
#include <hgraph/types/constants.h>
#include <hgraph/types/error_type.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/time_series/ts_ops.h>
#include <hgraph/types/traits.h>
#include <hgraph/types/tsb.h>
#include <hgraph/util/date_time.h>

namespace hgraph
{
	namespace {
	        engine_time_t node_time(const Node &node) {
            if (auto *et = node.cached_evaluation_time_ptr(); et != nullptr) {
                return *et;
            }
            auto g = node.graph();
            return g != nullptr ? g->evaluation_time() : MIN_DT;
        }

	        TSInputView node_input_field_view(Node &node, std::string_view key) {
	            TSInputView root = node.input(node_time(node));
	            if (!root) {
	                return {};
	            }
	            auto bundle = root.try_as_bundle();
	            if (!bundle.has_value()) {
	                return {};
	            }
	            return bundle->field(key);
	        }
	    }  // namespace

    BasePythonNode::BasePythonNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::s_ptr signature,
                                   nb::dict scalars, const TSMeta* input_meta, const TSMeta* output_meta,
                                   const TSMeta* error_output_meta, const TSMeta* recordable_state_meta,
                                   nb::callable eval_fn, nb::callable start_fn, nb::callable stop_fn)
        : Node(node_ndx, std::move(owning_graph_id), std::move(signature), std::move(scalars),
               input_meta, output_meta, error_output_meta, recordable_state_meta),
          _eval_fn{std::move(eval_fn)},
          _start_fn{std::move(start_fn)}, _stop_fn{std::move(stop_fn)} {}

    void BasePythonNode::_initialise_kwargs() {
        // Assuming Injector and related types are properly defined, and scalars is a map-like container
        _kwargs = {};

        bool  has_injectables{signature().injectables != 0};
        auto *injectable_map = has_injectables ? &(*signature().injectable_inputs) : nullptr;
        auto g = graph();

        nb::object node_wrapper{};
        auto       get_node_wrapper = [&]() -> nb::object {
            if (!node_wrapper) { node_wrapper = wrap_node(shared_from_this()); }
            return node_wrapper;
        };
        auto &signature_args = signature().args;
        for (const auto &[key_, value] : scalars()) {
            std::string key{nb::cast<std::string>(key_)};
            // Only include scalars that are in signature.args (same as Python implementation)
            if (std::ranges::find(signature_args, key) == std::ranges::end(signature_args)) { continue; }
            try {
                if (injectable_map && injectable_map->contains(key)) {
                    auto       injectable = injectable_map->at(key);
                    nb::object wrapped_value;

                    if ((injectable & InjectableTypesEnum::NODE) != InjectableTypesEnum::NONE) {
                        wrapped_value = get_node_wrapper();
	                    } else if ((injectable & InjectableTypesEnum::OUTPUT) != InjectableTypesEnum::NONE) {
	                        auto out = output(node_time(*this));
	                        if (out) {
	                            wrapped_value = wrap_output_view(out);
	                        } else {
	                            wrapped_value = nb::none();
	                        }
                    } else if ((injectable & InjectableTypesEnum::SCHEDULER) != InjectableTypesEnum::NONE) {
                        auto sched    = scheduler();
                        wrapped_value = wrap_node_scheduler(sched);
                    } else if ((injectable & InjectableTypesEnum::ENGINE_API) != InjectableTypesEnum::NONE) {
                        if (g) {
                            auto engine_api = g->evaluation_engine_api();
                            if (engine_api) {
                                wrapped_value = wrap_evaluation_engine_api(engine_api);
                            } else {
                                wrapped_value = nb::none();
                            }
                        } else {
                            wrapped_value = nb::none();
                        }
                    } else if ((injectable & InjectableTypesEnum::CLOCK) != InjectableTypesEnum::NONE) {
                        if (g) {
                            auto clock = g->evaluation_clock();
                            if (clock) {
                                wrapped_value = wrap_evaluation_clock(clock);
                            } else {
                                wrapped_value = nb::none();
                            }
                        } else {
                            wrapped_value = nb::none();
                        }
                    } else if ((injectable & InjectableTypesEnum::TRAIT) != InjectableTypesEnum::NONE) {
                        wrapped_value = g ? wrap_traits(&g->traits(), g->shared_from_this()) : nb::none();
                    } else if ((injectable & InjectableTypesEnum::RECORDABLE_STATE) != InjectableTypesEnum::NONE) {
                        auto recordable = recordable_state(node_time(*this));
                        if (recordable) {
                            wrapped_value = nb::cast(recordable);
                        } else {
                            wrapped_value = nb::none();
                        }
                    } else if ((injectable & InjectableTypesEnum::STATE) != InjectableTypesEnum::NONE) {
                        wrapped_value = value(get_node_wrapper());
                    } else if ((injectable & InjectableTypesEnum::LOGGER) != InjectableTypesEnum::NONE) {
                        wrapped_value = value(get_node_wrapper());
                    } else {
                        throw std::runtime_error(
                            fmt::format("Unsupported injectable mask {} for '{}'",
                                        static_cast<int>(injectable), key));
                    }

                    _kwargs[key_] = wrapped_value;
                    continue;
                }
                _kwargs[key_] = value;
            } catch (const nb::python_error &e) {
                throw NodeException::capture_error(e, *this, std::string("Initialising kwargs for '" + key + "'"));
            } catch (const std::exception &e) {
                throw NodeException::capture_error(e, *this, std::string("Initialising kwargs for '" + key + "'"));
            }
        }
        try {
            _initialise_kwarg_inputs();
        } catch (const nb::python_error &e) {
            throw NodeException::capture_error(e, *this, "Initialising kwargs");
        } catch (const std::exception &e) { throw NodeException::capture_error(e, *this, "Initialising kwargs"); }
        _index_kwarg_time_views();
    }

    void BasePythonNode::_initialise_kwarg_inputs() {
        // This can be called during wiring in the current flow, would be worth looking into that to clean up, but for now protect
        if (graph() == nullptr) { return; }
        // If is not a compute node or sink node, there are no inputs to map
        TSInputView root = input(node_time(*this));
        if (!root) { return; }
        auto root_bundle = root.try_as_bundle();
        if (!root_bundle.has_value()) { return; }
        if (!signature().time_series_inputs.has_value()) { return; }
        auto &signature_args = signature().args;
        // Match main branch behavior: iterate over time_series_inputs
        for (const auto &[key, _] : *signature().time_series_inputs) {
            if (std::ranges::find(signature_args, key) == std::ranges::end(signature_args)) {
                continue;
            }

            auto input_view = root_bundle->field(key);
            if (!input_view) {
                continue;
            }
            _kwargs[key.c_str()] = wrap_input_view(input_view);
        }

    }

    void BasePythonNode::_initialise_state() {
        if (!has_recordable_state()) { return; }

        // Get RecordReplayContext and check if RECOVER mode is active
        nb::object context_cls = get_record_replay_context();
        nb::object context     = context_cls.attr("instance")();
        nb::object mode        = context.attr("mode");

        // Get RecordReplayEnum to check for RECOVER flag
        nb::object enum_cls     = get_record_replay_enum();
        nb::object recover_flag = enum_cls.attr("RECOVER");

        // Check if RECOVER is in mode (using bitwise 'in' operation via Python __contains__)
        nb::object anded{mode.attr("__and__")(recover_flag)};
        bool       is_recover_mode = nb::cast<bool>(nb::bool_(anded));

        if (!is_recover_mode) { return; }

        // Get the evaluation clock as a Python object
        nb::object clock = wrap_evaluation_clock(graph()->evaluation_clock());

        // Get the fully qualified recordable ID
        nb::object  fq_recordable_id_fn = get_fq_recordable_id_fn();
        nb::object  traits_obj       = wrap_traits(&graph()->traits(), graph()->shared_from_this());
        std::string record_replay_id = signature().record_replay_id.value_or("");
        nb::object  recordable_id    = fq_recordable_id_fn(traits_obj, nb::str(record_replay_id.c_str()));

        // Get evaluation time minus MIN_TD
        engine_time_t eval_time = graph()->evaluation_time();
        engine_time_t tm        = eval_time - MIN_TD;

        // Get as_of time
        nb::object get_as_of = get_as_of_fn();
        nb::object as_of     = get_as_of(clock);

        // Get the recordable_state type from the Python signature object
        // Cast the C++ signature to a Python object to access the recordable_state property
        nb::object py_signature          = nb::cast(&signature());
        nb::object recordable_state_type = py_signature.attr("recordable_state");
        nb::object tsb_type              = recordable_state_type.attr("tsb_type").attr("py_type");

        // Call replay_const to restore state
        nb::object replay_const   = get_replay_const_fn();
        nb::object restored_state = replay_const(nb::str("__state__"), tsb_type, nb::arg("recordable_id") = recordable_id,
                                                 nb::arg("tm") = nb::cast(tm), nb::arg("as_of") = as_of);

        // Set the value on recordable_state
        auto recordable = recordable_state(node_time(*this));
        if (recordable) {
            recordable.from_python(restored_state.attr("value"));
        }
    }

    class ContextManager
    {
      public:
        explicit ContextManager(BasePythonNode &node) {
            if (node.signature().context_inputs.has_value() && !node.signature().context_inputs->empty()) {
                contexts_.reserve(node.signature().context_inputs->size());
                for (const auto &context_key : *node.signature().context_inputs) {
                    auto context_view = node_input_field_view(node, context_key);
                    if (context_view && context_view.valid()) {
                        nb::object context_value = context_view.to_python();
                        context_value.attr("__enter__")();
                        contexts_.push_back(context_value);
                    }
                }
            }
        }

        ~ContextManager() {
            std::exception_ptr first_error;
            for (auto it = contexts_.rbegin(); it != contexts_.rend(); ++it) {
                try {
                    it->attr("__exit__")(nb::none(), nb::none(), nb::none());
                } catch (const nb::python_error &e) {
                    if (!first_error) { first_error = std::current_exception(); }
                } catch (const std::exception &e) {
                    if (!first_error) { first_error = std::current_exception(); }
                }
            }
            if (first_error) { std::rethrow_exception(first_error); }
        }

      private:
        std::vector<nb::object> contexts_;
        std::exception_ptr      cached_error_;
    };

    void BasePythonNode::do_eval() {
        _refresh_kwarg_time_views();
        ContextManager context_manager(*this);
        if (std::getenv("HGRAPH_DEBUG_TSD_GET_ITEMS_KW") != nullptr) {
            try {
                nb::object ts_obj = _kwargs.contains(nb::str("ts")) ? _kwargs[nb::str("ts")] : nb::none();
                auto type_name = [](const nb::object& obj) -> std::string {
                    if (obj.is_none()) {
                        return "<none>";
                    }
                    try {
                        return nb::cast<std::string>(nb::str(obj.attr("__class__").attr("__name__")));
                    } catch (...) {
                        return "<unknown>";
                    }
                };

                std::string ts_type = type_name(ts_obj);
                std::string ref_type = "<none>";
                std::string out_type = "<none>";
                std::string ts_path = "<none>";
                int ts_kind = -1;
                std::string bound_path = "<none>";
                int bound_kind = -1;
                if (!ts_obj.is_none()) {
                    nb::object ref_val = nb::getattr(ts_obj, "value", nb::none());
                    ref_type = type_name(ref_val);
                    if (!ref_val.is_none()) {
                        nb::object out_val = nb::getattr(ref_val, "output", nb::none());
                        out_type = type_name(out_val);
                    }

                    TSInputView ts_input = unwrap_input_view(ts_obj);
                    if (ts_input) {
                        ts_path = ts_input.short_path().to_string();
                        if (const TSMeta* meta = ts_input.ts_meta(); meta != nullptr) {
                            ts_kind = static_cast<int>(meta->kind);
                        }
                        ViewData bound{};
                        if (resolve_bound_target_view_data(ts_input.as_ts_view().view_data(), bound)) {
                            bound_path = bound.path.to_string();
                            TSView bound_view(bound, ts_input.current_time());
                            if (const TSMeta* meta = bound_view.ts_meta(); meta != nullptr) {
                                bound_kind = static_cast<int>(meta->kind);
                            }
                        }
                    }
                }

                std::fprintf(stderr,
                             "[tsd_kw] node=%lld name=%s ts=%s ref=%s out=%s ts_path=%s ts_kind=%d bound_path=%s bound_kind=%d\n",
                             static_cast<long long>(node_ndx()),
                             signature().name.c_str(),
                             ts_type.c_str(),
                             ref_type.c_str(),
                             out_type.c_str(),
                             ts_path.c_str(),
                             ts_kind,
                             bound_path.c_str(),
                             bound_kind);
            } catch (...) {
                std::fprintf(stderr,
                             "[tsd_kw] node=%lld name=%s probe_error\n",
                             static_cast<long long>(node_ndx()),
                             signature().name.c_str());
            }
        }
        try {
            if (std::getenv("HGRAPH_DEBUG_TSD_GET_ITEMS_STATE") != nullptr &&
                signature().name == "tsd_get_items") {
                auto repr_or = [](const nb::object& obj) -> std::string {
                    try {
                        return nb::cast<std::string>(nb::repr(obj));
                    } catch (...) {
                        return "<repr_error>";
                    }
                };

                nb::object key_added = nb::none();
                nb::object key_removed = nb::none();
                nb::object source_removed = nb::none();
                nb::object source_key_set = nb::none();
                nb::object ref_modified_items = nb::none();
                nb::object ref_ref_modified_items = nb::none();
                nb::object ref_in_source_removed = nb::none();
                nb::object ref_branch_decisions = nb::none();
                try {
                    if (_kwargs.contains(nb::str("key"))) {
                        nb::object key_obj = _kwargs[nb::str("key")];
                        if (!key_obj.is_none()) {
                            key_added = nb::getattr(key_obj, "added", nb::none())();
                            key_removed = nb::getattr(key_obj, "removed", nb::none())();
                        }
                    }
                } catch (...) {}
                try {
                    if (_kwargs.contains(nb::str("ts"))) {
                        nb::object ts_obj = _kwargs[nb::str("ts")];
                        nb::object ts_value = nb::getattr(ts_obj, "value", nb::none());
                        nb::object ts_output = nb::getattr(ts_value, "output", nb::none());
                        if (!ts_output.is_none()) {
                            nb::object key_set = nb::getattr(ts_output, "key_set", nb::none());
                            if (!key_set.is_none()) {
                                source_key_set = key_set;
                                source_removed = nb::getattr(key_set, "removed", nb::none())();
                            }
                        }
                    }
                } catch (...) {}
                try {
                    if (_kwargs.contains(nb::str("_ref"))) {
                        nb::object ref_obj = _kwargs[nb::str("_ref")];
                        if (!ref_obj.is_none()) {
                            ref_modified_items = nb::list(nb::iter(nb::getattr(ref_obj, "modified_items", nb::none())()));
                        }
                    }
                } catch (...) {}
                try {
                    if (_kwargs.contains(nb::str("_ref_ref"))) {
                        nb::object ref_ref_obj = _kwargs[nb::str("_ref_ref")];
                        if (!ref_ref_obj.is_none()) {
                            ref_ref_modified_items =
                                nb::list(nb::iter(nb::getattr(ref_ref_obj, "modified_items", nb::none())()));
                        }
                    }
                } catch (...) {}
                try {
                    if (!ref_modified_items.is_none() && !source_removed.is_none()) {
                        nb::list in_removed;
                        for (const auto& item_h : nb::iter(ref_modified_items)) {
                            nb::object item = nb::cast<nb::object>(item_h);
                            nb::object key_obj = item[0];
                            const bool in_set = PySequence_Contains(source_removed.ptr(), key_obj.ptr()) == 1;
                            in_removed.append(nb::make_tuple(key_obj, nb::bool_(in_set)));
                        }
                        ref_in_source_removed = in_removed;
                    }
                } catch (...) {}
                try {
                    if (!ref_modified_items.is_none()) {
                        nb::list decisions;
                        for (const auto& item_h : nb::iter(ref_modified_items)) {
                            nb::object item = nb::cast<nb::object>(item_h);
                            nb::object key_obj = item[0];
                            nb::object ref_input = item[1];
                            nb::object ref_value = nb::getattr(ref_input, "value", nb::none());

                            bool has_output = false;
                            bool output_is_reference = false;
                            bool is_empty = true;
                            bool in_source_key_set = false;
                            bool in_ref_ref = false;

                            if (!ref_value.is_none()) {
                                has_output = nb::cast<bool>(nb::getattr(ref_value, "has_output", nb::bool_(false)));
                                is_empty = nb::cast<bool>(nb::getattr(ref_value, "is_empty", nb::bool_(true)));
                                if (has_output) {
                                    nb::object out_obj = nb::getattr(ref_value, "output", nb::none());
                                    if (!out_obj.is_none()) {
                                        nb::object is_ref_callable = nb::getattr(out_obj, "is_reference", nb::none());
                                        if (!is_ref_callable.is_none()) {
                                            output_is_reference = nb::cast<bool>(is_ref_callable());
                                        }
                                    }
                                }
                            }

                            if (!source_key_set.is_none()) {
                                in_source_key_set = PySequence_Contains(source_key_set.ptr(), key_obj.ptr()) == 1;
                            }

                            if (_kwargs.contains(nb::str("_ref_ref"))) {
                                nb::object ref_ref_obj = _kwargs[nb::str("_ref_ref")];
                                if (!ref_ref_obj.is_none()) {
                                    in_ref_ref = PySequence_Contains(ref_ref_obj.ptr(), key_obj.ptr()) == 1;
                                }
                            }

                            decisions.append(nb::make_tuple(
                                key_obj,
                                nb::bool_(has_output),
                                nb::bool_(output_is_reference),
                                nb::bool_(is_empty),
                                nb::bool_(in_source_key_set),
                                nb::bool_(in_ref_ref)));
                        }
                        ref_branch_decisions = decisions;
                    }
                } catch (...) {}

                std::fprintf(stderr,
                             "[tsd_get_items_state] node=%lld time=%lld key_added=%s key_removed=%s source_removed=%s ref_mod=%s ref_in_removed=%s ref_decisions=%s ref_ref_mod=%s\n",
                             static_cast<long long>(node_ndx()),
                             static_cast<long long>(node_time(*this).time_since_epoch().count()),
                             repr_or(key_added).c_str(),
                             repr_or(key_removed).c_str(),
                             repr_or(source_removed).c_str(),
                             repr_or(ref_modified_items).c_str(),
                             repr_or(ref_in_source_removed).c_str(),
                             repr_or(ref_branch_decisions).c_str(),
                             repr_or(ref_ref_modified_items).c_str());
            }
            if (std::getenv("HGRAPH_DEBUG_TSD_GET_ITEM_STATE") != nullptr &&
                signature().name == "tsd_get_item_default") {
                auto repr_or = [](const nb::object& obj) -> std::string {
                    try {
                        return nb::cast<std::string>(nb::repr(obj));
                    } catch (...) {
                        return "<repr_error>";
                    }
                };
                nb::object key_repr_obj = nb::none();
                nb::object key_value_obj = nb::none();
                nb::object ts_modified = nb::none();
                nb::object ts_valid = nb::none();
                nb::object ts_is_empty = nb::none();
                nb::object key_modified = nb::none();
                nb::object ref_modified = nb::none();
                nb::object ref_ref_modified = nb::none();
                nb::object ref_bound = nb::none();
                nb::object ref_ref_bound = nb::none();
                try {
                    if (_kwargs.contains(nb::str("key"))) {
                        nb::object key_obj = _kwargs[nb::str("key")];
                        key_repr_obj = key_obj;
                        key_value_obj = nb::getattr(key_obj, "value", nb::none());
                        key_modified = nb::getattr(key_obj, "modified", nb::none());
                    }
                } catch (...) {}
                try {
                    if (_kwargs.contains(nb::str("ts"))) {
                        nb::object ts_obj = _kwargs[nb::str("ts")];
                        ts_modified = nb::getattr(ts_obj, "modified", nb::none());
                        ts_valid = nb::getattr(ts_obj, "valid", nb::none());
                        nb::object ts_value = nb::getattr(ts_obj, "value", nb::none());
                        ts_is_empty = nb::getattr(ts_value, "is_empty", nb::none());
                    }
                } catch (...) {}
                try {
                    if (_kwargs.contains(nb::str("_ref"))) {
                        nb::object ref_obj = _kwargs[nb::str("_ref")];
                        ref_modified = nb::getattr(ref_obj, "modified", nb::none());
                        ref_bound = nb::getattr(ref_obj, "bound", nb::none());
                    }
                } catch (...) {}
                try {
                    if (_kwargs.contains(nb::str("_ref_ref"))) {
                        nb::object ref_ref_obj = _kwargs[nb::str("_ref_ref")];
                        ref_ref_modified = nb::getattr(ref_ref_obj, "modified", nb::none());
                        ref_ref_bound = nb::getattr(ref_ref_obj, "bound", nb::none());
                    }
                } catch (...) {}
                std::fprintf(stderr,
                             "[tsd_get_item_state] node=%lld time=%lld key=%s key_value=%s ts_mod=%s ts_valid=%s ts_empty=%s key_mod=%s ref_mod=%s ref_bound=%s ref_ref_mod=%s ref_ref_bound=%s\n",
                             static_cast<long long>(node_ndx()),
                             static_cast<long long>(node_time(*this).time_since_epoch().count()),
                             repr_or(key_repr_obj).c_str(),
                             repr_or(key_value_obj).c_str(),
                             repr_or(ts_modified).c_str(),
                             repr_or(ts_valid).c_str(),
                             repr_or(ts_is_empty).c_str(),
                             repr_or(key_modified).c_str(),
                             repr_or(ref_modified).c_str(),
                             repr_or(ref_bound).c_str(),
                             repr_or(ref_ref_modified).c_str(),
                             repr_or(ref_ref_bound).c_str());
            }
            auto out{_eval_fn(**_kwargs)};
            if (std::getenv("HGRAPH_DEBUG_TSD_GET_ITEMS_OUT") != nullptr &&
                signature().name == "tsd_get_items") {
                std::string out_repr{"<repr_error>"};
                try {
                    out_repr = nb::cast<std::string>(nb::repr(out));
                } catch (...) {}
                std::fprintf(stderr,
                             "[tsd_get_items_out] node=%lld time=%lld out=%s\n",
                             static_cast<long long>(node_ndx()),
                             static_cast<long long>(node_time(*this).time_since_epoch().count()),
                             out_repr.c_str());
            }
            if (std::getenv("HGRAPH_DEBUG_TSD_GET_ITEMS_OUT_KEYS") != nullptr &&
                signature().name == "tsd_get_items") {
                auto safe_str = [](const nb::handle& h) -> std::string {
                    try {
                        return nb::cast<std::string>(nb::str(h));
                    } catch (...) {
                        return "<str_error>";
                    }
                };
                if (nb::isinstance<nb::dict>(out)) {
                    nb::dict out_dict = nb::cast<nb::dict>(out);
                    std::string keys_repr;
                    bool first = true;
                    for (const auto& kv : out_dict) {
                        if (!first) {
                            keys_repr += ",";
                        }
                        first = false;
                        keys_repr += safe_str(kv.first);
                    }
                    std::string post_ref_ref{"<none>"};
                    try {
                        if (_kwargs.contains(nb::str("_ref_ref"))) {
                            nb::object ref_ref_obj = _kwargs[nb::str("_ref_ref")];
                            if (!ref_ref_obj.is_none()) {
                                nb::object items = nb::getattr(ref_ref_obj, "modified_items", nb::none())();
                                post_ref_ref = safe_str(items);
                            }
                        }
                    } catch (...) {
                        post_ref_ref = "<error>";
                    }
                    std::fprintf(stderr,
                                 "[tsd_get_items_out_keys] node=%lld time=%lld keys=[%s] post_ref_ref_mod=%s\n",
                                 static_cast<long long>(node_ndx()),
                                 static_cast<long long>(node_time(*this).time_since_epoch().count()),
                                 keys_repr.c_str(),
                                 post_ref_ref.c_str());
                }
            }
            if (std::getenv("HGRAPH_DEBUG_TSD_GET_ITEM_OUT") != nullptr &&
                signature().name == "tsd_get_item_default") {
                std::string out_repr{"<repr_error>"};
                try {
                    out_repr = nb::cast<std::string>(nb::repr(out));
                } catch (...) {}
                std::string key_repr{"<none>"};
                std::string key_value_repr{"<none>"};
                std::string ref_bound_post{"<none>"};
                std::string ref_mod_post{"<none>"};
                std::string ref_ref_bound_post{"<none>"};
                std::string ref_ref_mod_post{"<none>"};
                try {
                    if (_kwargs.contains(nb::str("key"))) {
                        nb::object key_obj = _kwargs[nb::str("key")];
                        key_repr = nb::cast<std::string>(nb::repr(key_obj));
                        nb::object key_value = nb::getattr(key_obj, "value", nb::none());
                        if (!key_value.is_none()) {
                            key_value_repr = nb::cast<std::string>(nb::repr(key_value));
                        }
                    }
                } catch (...) {}
                try {
                    if (_kwargs.contains(nb::str("_ref"))) {
                        nb::object ref_obj = _kwargs[nb::str("_ref")];
                        ref_bound_post = nb::cast<std::string>(nb::repr(nb::getattr(ref_obj, "bound", nb::none())));
                        ref_mod_post = nb::cast<std::string>(nb::repr(nb::getattr(ref_obj, "modified", nb::none())));
                    }
                } catch (...) {}
                try {
                    if (_kwargs.contains(nb::str("_ref_ref"))) {
                        nb::object ref_ref_obj = _kwargs[nb::str("_ref_ref")];
                        ref_ref_bound_post = nb::cast<std::string>(nb::repr(nb::getattr(ref_ref_obj, "bound", nb::none())));
                        ref_ref_mod_post = nb::cast<std::string>(nb::repr(nb::getattr(ref_ref_obj, "modified", nb::none())));
                    }
                } catch (...) {}
                std::fprintf(stderr,
                             "[tsd_get_item_out] node=%lld time=%lld key=%s key_value=%s out=%s ref_bound=%s ref_mod=%s ref_ref_bound=%s ref_ref_mod=%s\n",
                             static_cast<long long>(node_ndx()),
                             static_cast<long long>(node_time(*this).time_since_epoch().count()),
                             key_repr.c_str(),
                             key_value_repr.c_str(),
                             out_repr.c_str(),
                             ref_bound_post.c_str(),
                             ref_mod_post.c_str(),
                             ref_ref_bound_post.c_str(),
                             ref_ref_mod_post.c_str());
            }
            if (!out.is_none()) {
                auto out_port = output(node_time(*this));
                if (out_port) { out_port.from_python(out); }
            }
        } catch (nb::python_error &e) { throw NodeException::capture_error(e, *this, "During Python node evaluation"); }
    }

    void BasePythonNode::do_start() {
        _refresh_kwarg_time_views();
        if (_start_fn.is_valid() && !_start_fn.is_none()) {
            // Get the callable signature parameters using inspect.signature
            // This matches Python's approach: signature(self.start_fn).parameters.keys()
            // Using __code__.co_varnames includes local variables, not just parameters
            auto inspect = nb::module_::import_("inspect");
            auto sig     = inspect.attr("signature")(_start_fn);
            auto params  = sig.attr("parameters").attr("keys")();

            // Filter kwargs to only include parameters in start_fn signature
            nb::dict filtered_kwargs;
            for (auto k : params) {
                if (_kwargs.contains(k)) { filtered_kwargs[k] = _kwargs[k]; }
            }
            // Call start_fn with filtered kwargs
            _start_fn(**filtered_kwargs);
        }
    }

    void BasePythonNode::do_stop() {
        _refresh_kwarg_time_views();
        if (_stop_fn.is_valid() and !_stop_fn.is_none()) {
            // Get the callable signature parameters using inspect.signature
            // This matches Python's approach: signature(self.stop_fn).parameters.keys()
            // Using __code__.co_varnames includes local variables, not just parameters
            auto inspect = nb::module_::import_("inspect");
            auto sig     = inspect.attr("signature")(_stop_fn);
            auto params  = sig.attr("parameters").attr("keys")();

            // Filter kwargs to only include parameters in stop_fn signature
            nb::dict filtered_kwargs;
            for (auto k : params) {
                if (_kwargs.contains(k)) { filtered_kwargs[k] = _kwargs[k]; }
            }

            // Call stop_fn with filtered kwargs
            _stop_fn(**filtered_kwargs);
        }
    }

    void BasePythonNode::initialise() {}

    void BasePythonNode::start() {
        if (graph() == nullptr) { throw std::runtime_error("BasePythonNode::start: missing owning graph"); }
        _initialise_kwargs();
        _initialise_inputs();
        _initialise_state();
        // Now call parent class
        Node::start();
    }

    void BasePythonNode::dispose() {
        _kwargs.clear();
        _kwarg_wrapped_inputs.clear();
        _kwarg_wrapped_outputs.clear();
    }

    void BasePythonNode::_index_kwarg_time_views() {
        _kwarg_wrapped_inputs.clear();
        _kwarg_wrapped_outputs.clear();

        const auto track_wrapped_input = [&](const nb::handle& value) {
            _kwarg_wrapped_inputs.push_back(WrappedInputRef{nb::borrow<nb::object>(value)});
        };

        const auto track_wrapped_output = [&](const nb::handle& value) {
            _kwarg_wrapped_outputs.push_back(WrappedOutputRef{nb::borrow<nb::object>(value)});
        };

        for (const auto& [_, value] : _kwargs) {
            if (nb::isinstance<PyTimeSeriesInput>(value)) {
                track_wrapped_input(value);
                continue;
            }

            if (nb::isinstance<PyTimeSeriesOutput>(value)) {
                track_wrapped_output(value);
                continue;
            }
        }
    }

    void BasePythonNode::_refresh_kwarg_time_views() {
        const auto et = node_time(*this);
        for (const WrappedInputRef& tracked : _kwarg_wrapped_inputs) {
            if (tracked.owner.is_valid()) {
                auto& wrapped = nb::cast<PyTimeSeriesInput&>(tracked.owner);
                wrapped.view().set_current_time(et);
                wrapped.input_view().set_current_time(et);
            }
        }
        for (const WrappedOutputRef& tracked : _kwarg_wrapped_outputs) {
            if (tracked.owner.is_valid()) {
                auto& wrapped = nb::cast<PyTimeSeriesOutput&>(tracked.owner);
                wrapped.view().set_current_time(et);
                wrapped.output_view().set_current_time(et);
            }
        }
    }
}  // namespace hgraph
