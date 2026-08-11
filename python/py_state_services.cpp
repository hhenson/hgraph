/**
 * State + services bindings: _GlobalState and record/replay configuration,
 * services/adaptors/mesh/context wiring entry points, and the runtime view
 * classes handed to python user nodes (OutputView, TimeSeries,
 * RuntimeGlobalState, RecordableStateView, clock/scheduler), including the
 * enum/sentinel slot setters.
 */
#include "py_runtime.h"
#include "py_wiring.h"
#include "py_bindings.h"

#include <hgraph/types/metadata/type_realization.h>

namespace nb = nanobind;
using namespace hgraph;
using namespace hgraph::python_bridge;

namespace hgraph::python_bridge
{
    namespace
    {
        /** Raw tp_getset getter for the hot per-tick TimeSeries reads
            (value/delta_value/modified/valid): a direct C call from
            CPython's attribute lookup, where def_prop_ro routes through an
            nb_func vectorcall (~15-20ns per read on the python-node
            boundary). */

        /** Temporarily reactivate the registering Python call's lease while
            its notification runs. The queue holder keeps the call-owned guard
            alive until all deferred callbacks using it have drained. */
        class PyNotificationLeaseScope
        {
          public:
            explicit PyNotificationLeaseScope(const PyTsLease &lease) noexcept
                : guard_(lease.guard), generation_(lease.generation)
            {
                if (guard_ != nullptr && guard_->alive)
                {
                    previous_generation_ = guard_->generation;
                    guard_->generation   = generation_;
                    active_              = true;
                }
            }

            PyNotificationLeaseScope(const PyNotificationLeaseScope &) = delete;
            PyNotificationLeaseScope &operator=(const PyNotificationLeaseScope &) = delete;

            ~PyNotificationLeaseScope() noexcept
            {
                if (active_ && guard_->alive) { guard_->generation = previous_generation_; }
            }

          private:
            std::shared_ptr<PyTsGuard> guard_{};
            std::uint64_t              generation_{0};
            std::uint64_t              previous_generation_{0};
            bool                       active_{false};
        };

        /** Wrap a Python callable for the C++ notification queues. Invocation
            and normal queue draining happen under the executor phase guard;
            the holder retains a local guard for exceptional out-of-phase
            destruction. */
        [[nodiscard]] inline std::function<void()> py_notification_thunk(nb::object fn,
                                                                         PyTsLease lease)
        {
            struct GilSafeCallable
            {
                PyObject *fn;
                PyTsLease lease;

                GilSafeCallable(nb::object object, PyTsLease call_lease)
                    : fn(object.release().ptr()), lease(std::move(call_lease))
                {
                    lease.retain_for_deferred_call();
                }

                ~GilSafeCallable()
                {
                    if (fn != nullptr)
                    {
                        nb::gil_scoped_acquire gil;
                        Py_DECREF(fn);
                    }
                    lease.release_from_deferred_call();
                }
            };
            auto holder = std::make_shared<GilSafeCallable>(std::move(fn), std::move(lease));
            return [holder]() {
                PyNotificationLeaseScope active_lease{holder->lease};
                nb::borrow<nb::object>(nb::handle(holder->fn))();
            };
        }

        template <auto Member>
        PyObject *py_ts_raw_get(PyObject *self, void *) noexcept
        {
            return py_error_on_exception<PyObject *>(nullptr, [&] {
                auto *ts = nb::inst_ptr<PyTimeSeries>(self);
                if constexpr (std::is_same_v<decltype((ts->*Member)()), bool>)
                {
                    return PyBool_FromLong((ts->*Member)() ? 1 : 0);
                }
                else
                {
                    return (ts->*Member)().release().ptr();
                }
            });
        }

        std::vector<WiringPortRef> wiring_ports(nb::list inputs)
        {
            std::vector<WiringPortRef> result;
            result.reserve(nb::len(inputs));
            for (nb::handle input : inputs)
            {
                result.push_back(nb::cast<PyPort &>(input).ref);
            }
            return result;
        }
    }

    void bind_state_and_services(nb::module_ &m)
    {
    nb::class_<GlobalState>(m, "_GlobalState")
        .def(nb::init<>())
        .def("__len__", [](GlobalState &self) { return self.view().size(); })
        .def("__contains__", [](GlobalState &self, const std::string &key) { return self.view().contains(key); })
        .def("__getitem__",
             [](GlobalState &self, const std::string &key) -> nb::object {
                 const GlobalStateView state = self.view();
                 if (!state.contains(key)) { throw nb::key_error(key.c_str()); }
                 return value_to_py(state.get(key));
             })
        .def("get",
             [](GlobalState &self, const std::string &key, nb::object fallback) -> nb::object {
                 const GlobalStateView state = self.view();
                 return state.contains(key) ? value_to_py(state.get(key)) : fallback;
             },
             nb::arg("key"), nb::arg("default") = nb::none())
        .def("_set_memory_recording_entry",
             [](GlobalState &self, const std::string &key, std::size_t index,
                DateTime when, nb::handle python_delta) {
                 ValueView buffer = self.view().get(key);
                 if (!buffer.valid()) { throw nb::key_error(key.c_str()); }
                 const auto entries = buffer.as_list();
                 if (index >= entries.size())
                 {
                     throw nb::index_error("recording entry index is out of range");
                 }
                 const auto current = entries.at(index).as_indexed_view();
                 const auto *delta_schema = current.at(1).schema();
                 nb::object canonical = nb::borrow(python_delta);
                 if (delta_schema != nullptr &&
                     delta_schema->value_kind() == ValueTypeKind::Bundle &&
                     delta_schema->field_count == 3 &&
                     std::string_view{delta_schema->fields[0].name} == "removed" &&
                     std::string_view{delta_schema->fields[1].name} == "modified" &&
                     std::string_view{delta_schema->fields[2].name} == "removed_strict")
                 {
                     nb::set removed;
                     nb::set removed_strict;
                     nb::dict modified;
                     for (auto [item_key, item_value] : nb::cast<nb::dict>(python_delta))
                     {
                         if (removed_sentinel_slot().is_valid() &&
                             item_value.ptr() == removed_sentinel_slot().ptr())
                         {
                             removed_strict.add(item_key);
                         }
                         else if (remove_if_exists_sentinel_slot().is_valid() &&
                                  item_value.ptr() == remove_if_exists_sentinel_slot().ptr())
                         {
                             removed.add(item_key);
                         }
                         else { modified[item_key] = item_value; }
                     }
                     nb::dict shaped;
                     shaped["removed"]        = std::move(removed);
                     shaped["modified"]       = std::move(modified);
                     shaped["removed_strict"] = std::move(removed_strict);
                     canonical = std::move(shaped);
                 }
                 Value delta = py_to_value_as(canonical, delta_schema);
                 Value replacement = testing::make_sparse_entry(
                     delta_schema, when, delta);
                 buffer.as_list().begin_mutation().set(index, replacement.view());
             },
             nb::arg("key"), nb::arg("index"), nb::arg("when"),
             nb::arg("delta"))
        .def("__setitem__",
             [](GlobalState &self, const std::string &key, nb::handle value) {
                 self.view().set(key, py_to_value(value));
             })
        .def("__delitem__",
             [](GlobalState &self, const std::string &key) {
                 if (!self.view().erase(key)) { throw nb::key_error(key.c_str()); }
             })
        .def("keys", [](GlobalState &self) {
            nb::list result;
            const GlobalStateView state = self.view();
            const ValueView       value = state.as_value().view();
            const auto            map   = value.as_map();
            const auto            keys  = map.keys();
            for (const ValueView key : keys)
            {
                result.append(value_to_py(key));
            }
            return result;
        });

    // Record/replay configuration is copied with the Python thread's seed.
    m.def("_set_record_replay_config", [](GlobalState &state, const std::string &model) {
        auto config = record_replay::config(state.view());
        config.model = model;
        record_replay::set_config(state.view(), std::move(config));
    });
    m.def("_set_pooled_compound_scalar_storage",
          [](GlobalState &state, bool enabled) {
              set_pooled_compound_scalar_storage(state.view(), enabled);
          },
          nb::arg("state"), nb::arg("enabled") = true);
    m.def("_set_as_of", [](GlobalState &state, nb::object value) {
        auto config = record_replay::config(state.view());
        config.as_of = value.is_none() ? std::optional<DateTime>{}
                                        : std::optional<DateTime>{nb::cast<DateTime>(value)};
        record_replay::set_config(state.view(), std::move(config));
    }, nb::arg("state"), nb::arg("value").none());
    m.def("_set_time_zone_provider", [](GlobalState &state) {
        set_time_zone_provider(state.view(), make_time_zone_provider());
    });
    m.def("_set_table_schema_date_key", [](GlobalState &state, const std::string &key) {
        auto config = record_replay::config(state.view());
        config.date_key = key;
        record_replay::set_config(state.view(), std::move(config));
    });
    m.def("_set_table_schema_as_of_key", [](GlobalState &state, const std::string &key) {
        auto config = record_replay::config(state.view());
        config.as_of_key = key;
        record_replay::set_config(state.view(), std::move(config));
    });
    m.def("_table_schema_keys", [](GlobalState &state) {
        const auto config = record_replay::config(state.view());
        return nb::make_tuple(config.date_key, config.as_of_key);
    });
    m.attr("MODE_NONE")          = static_cast<unsigned>(record_replay::Mode::None);
    m.attr("MODE_RECORD")        = static_cast<unsigned>(record_replay::Mode::Record);
    m.attr("MODE_REPLAY")        = static_cast<unsigned>(record_replay::Mode::Replay);
    m.attr("MODE_COMPARE")       = static_cast<unsigned>(record_replay::Mode::Compare);
    m.attr("MODE_REPLAY_OUTPUT") = static_cast<unsigned>(record_replay::Mode::ReplayOutput);
    m.attr("MODE_RESET")         = static_cast<unsigned>(record_replay::Mode::Reset);
    m.attr("MODE_RECOVER")       = static_cast<unsigned>(record_replay::Mode::Recover);
    nb::class_<record_replay::scope>(m, "RecordReplayScope")
        .def(nb::init<record_replay::Mode, std::string>(), nb::arg("mode"), nb::arg("recordable_id") = std::string{});
    m.def("record_replay_scope", [](unsigned mode, const std::string &recordable_id) {
        return new record_replay::scope{static_cast<record_replay::Mode>(mode), recordable_id};
    }, nb::arg("mode"), nb::arg("recordable_id") = std::string{}, nb::rv_policy::take_ownership);
    m.def("current_record_replay_mode", [] {
        const auto &state = record_replay::current_scope();
        return nb::make_tuple(static_cast<unsigned>(state.mode), state.recordable_id);
    });
    m.def("_comparison_summary", [](GlobalState &state, const std::string &fq_key) {
        const auto summary = record_replay::comparison_summary(state.view(), fq_key);
        return nb::make_tuple(summary.compared, summary.mismatches);
    });
    m.def("frame_store_contains", [](const std::string &key) { return record_replay::store_contains(key); });
    m.def("frame_store_read", [](const std::string &key) { return frame_to_py(record_replay::store_read(key)); });

    // Context publishing (same-wiring; the C++ design record's semantics).
    // --- services (runtime identity; services.rst rulings 2026-07-05) ---
    nb::class_<PyServiceDesc>(m, "ServiceDescriptor")
        .def_prop_ro("flavour", [](const PyServiceDesc &self) {
            switch (self.descriptor->flavour)
            {
                case ServiceFlavour::Reference: return "reference";
                case ServiceFlavour::Subscription: return "subscription";
                case ServiceFlavour::RequestReply: return "request_reply";
                case ServiceFlavour::Adaptor: return "adaptor";
                case ServiceFlavour::ServiceAdaptor: return "service_adaptor";
            }
            return "unknown";
        })
        .def_prop_ro("name", [](const PyServiceDesc &self) { return self.descriptor->name; });
    // (TsType kind/size introspection for the python sequence protocol)
    m.def("service_descriptor",
          [](const std::string &name, const std::string &flavour, std::optional<PyTsType> output,
             std::optional<PyTsType> key_ts, std::optional<PyTsType> value, std::optional<PyTsType> request,
             std::optional<PyTsType> response, const std::string &default_path,
             const std::string &specialization) {
              RuntimeServiceDescriptor descriptor;
              descriptor.name           = name;
              descriptor.specialization = specialization;
              descriptor.default_path   = default_path;
              if (flavour == "reference")
              {
                  descriptor.flavour       = ServiceFlavour::Reference;
                  descriptor.output_schema = output.value().meta;
              }
              else if (flavour == "subscription")
              {
                  descriptor.flavour      = ServiceFlavour::Subscription;
                  descriptor.key_type     = key_ts.value().meta->value_schema;
                  descriptor.value_schema = value.value().meta;
              }
              else if (flavour == "request_reply")
              {
                  descriptor.flavour         = ServiceFlavour::RequestReply;
                  descriptor.request_schema  = request.value().meta;
                  descriptor.response_schema = response.has_value() ? response->meta : nullptr;
              }
              else if (flavour == "adaptor")
              {
                  descriptor.flavour = ServiceFlavour::Adaptor;
                  if (request.has_value()) { descriptor.input_schema = request->meta; }   // adaptor input
                  if (output.has_value()) { descriptor.output_schema = output->meta; }
              }
              else if (flavour == "service_adaptor")
              {
                  if (!request.has_value())
                  {
                      throw nb::value_error("service adaptor requires a request schema");
                  }
                  descriptor.flavour      = ServiceFlavour::ServiceAdaptor;
                  descriptor.input_schema = request->meta;
                  descriptor.output_schema = output.has_value() ? output->meta : nullptr;
              }
              else { throw nb::value_error("unknown service flavour"); }
              return PyServiceDesc{&intern_service_descriptor(std::move(descriptor))};
          },
          nb::arg("name"), nb::arg("flavour"), nb::arg("output") = nb::none(), nb::arg("key_ts") = nb::none(),
          nb::arg("value") = nb::none(), nb::arg("request") = nb::none(), nb::arg("response") = nb::none(),
          nb::arg("default_path") = std::string{}, nb::arg("specialization") = std::string{});
    m.def("find_service", [](const std::string &name) -> nb::object {
        const auto *descriptor = find_service_descriptor(name);
        return descriptor != nullptr ? nb::cast(PyServiceDesc{descriptor}) : nb::none();
    });
    m.def("service_client", [](PyWiring &w, const PyServiceDesc &desc, const std::string &path,
                               std::optional<PyPort> ts) -> nb::object {
        switch (desc.descriptor->flavour)
        {
            case ServiceFlavour::Reference:
                return nb::cast(PyPort{reference_service_client(w.wiring_ref(), *desc.descriptor, path)});
            case ServiceFlavour::Subscription:
                return nb::cast(PyPort{subscription_service_subscribe(w.wiring_ref(), *desc.descriptor, path,
                                                                      ts.value().ref)});
            case ServiceFlavour::RequestReply: {
                WiringPortRef output = request_reply_service_call(
                    w.wiring_ref(), *desc.descriptor, path, ts.value().ref);
                return output.schema != nullptr ? nb::cast(PyPort{std::move(output)}) : nb::none();
            }
            case ServiceFlavour::Adaptor:
            case ServiceFlavour::ServiceAdaptor:
                throw std::logic_error("service_client does not accept adaptor descriptors");
        }
        throw std::logic_error("unreachable");
    }, nb::arg("w"), nb::arg("desc"), nb::arg("path") = std::string{}, nb::arg("ts") = nb::none());
    m.def("register_service_impl", [](PyWiring &w, const PyServiceDesc &desc, const std::string &path,
                                      const PyWiredFn &impl, nb::list inputs, bool default_fallback) {
        // Extra time-series inputs supplied at registration, mirroring
        // register_adaptor_impl. They follow the flavour's transport input.
        auto implementation_inputs = wiring_ports(inputs);
        switch (desc.descriptor->flavour)
        {
            case ServiceFlavour::Reference:
                register_reference_service_impl(w.wiring_ref(), *desc.descriptor, path, impl.fn,
                                                implementation_inputs, default_fallback);
                return;
            case ServiceFlavour::Subscription:
                register_subscription_service_impl(w.wiring_ref(), *desc.descriptor, path, impl.fn,
                                                   implementation_inputs, default_fallback);
                return;
            case ServiceFlavour::RequestReply:
                register_request_reply_service_impl(w.wiring_ref(), *desc.descriptor, path, impl.fn,
                                                    implementation_inputs, default_fallback);
                return;
            case ServiceFlavour::Adaptor:
            case ServiceFlavour::ServiceAdaptor:
                throw std::logic_error("register_service_impl does not accept adaptor descriptors");
        }
    }, nb::arg("w"), nb::arg("desc"), nb::arg("path") = std::string{}, nb::arg("impl"),
       nb::arg("inputs") = nb::list(), nb::arg("default_fallback") = false);

    m.def("mesh_scope_exists", [](const std::string &name) {
        return OperatorRegistry::instance().resolve_mesh_scope(name) != nullptr;
    }, nb::arg("name") = std::string{});

    // Internal primitives behind Python's MeshWiringPort. The public Python
    // surface is mesh_(func)[key] / get_mesh(func), matching release/0.5.
    m.def("mesh_ref", [](PyWiring &w, nb::handle key, const std::string &name) {
        const TSValueTypeMetaData *out_schema = OperatorRegistry::instance().resolve_mesh_scope(name);
        if (out_schema == nullptr)
        {
            throw std::logic_error("mesh_(func)[key] used outside a mesh scope (no enclosing mesh is being wired)");
        }
        const ValueTypeMetaData *key_type = OperatorRegistry::instance().resolve_mesh_key_scope(name);
        if (key_type == nullptr) { throw std::logic_error("mesh scope has no resolved key type"); }

        WiringPortRef key_ref;
        if (nb::isinstance<PyPort>(key))
        {
            key_ref = nb::cast<const PyPort &>(key).ref;
            if (TypeRegistry::instance().dereference(key_ref.schema) != TypeRegistry::instance().ts(key_type))
            {
                throw std::invalid_argument("mesh lookup key type does not match the enclosing mesh key type");
            }
        }
        else
        {
            WiringArg arg;
            arg.kind         = WiringArg::Kind::Scalar;
            arg.scalar_value = py_to_value_as(key, key_type);
            arg.scalar_meta  = key_type;
            const std::array<WiringArg, 1> args{std::move(arg)};
            key_ref = wire_operator(w.wiring_ref(), "const", args, true,
                                    TypeRegistry::instance().ts(key_type)).output.erased();
        }

        WiringPortRef placeholder =
            wire_operator(w.wiring_ref(), "nothing", std::span<const WiringArg>{}, true, out_schema)
                .output.erased();
        return PyPort{stdlib::higher_order_impl_detail::mesh_ref_erased(
            w.wiring_ref(), key_ref, placeholder, name)};
    }, nb::arg("w"), nb::arg("key"), nb::arg("name") = std::string{});

    m.def("mesh_key_set_ref", [](PyWiring &w, const std::string &name) {
        const ValueTypeMetaData *key_type = OperatorRegistry::instance().resolve_mesh_key_scope(name);
        if (key_type == nullptr)
        {
            throw std::logic_error("get_mesh used outside a mesh scope (no enclosing mesh is being wired)");
        }
        const TSValueTypeMetaData *key_set_schema = TypeRegistry::instance().tss(key_type);
        WiringPortRef placeholder =
            wire_operator(w.wiring_ref(), "nothing", std::span<const WiringArg>{}, true, key_set_schema)
                .output.erased();
        return PyPort{stdlib::higher_order_impl_detail::mesh_key_set_ref_erased(
            w.wiring_ref(), placeholder, key_type, name)};
    }, nb::arg("w"), nb::arg("name") = std::string{});

    m.def("service_impl_input", [](PyWiring &w, const PyServiceDesc &desc, const std::string &path) {
        return PyPort{service_impl_input(w.wiring_ref(), *desc.descriptor, path)};
    }, nb::arg("w"), nb::arg("desc"), nb::arg("path") = std::string{});
    m.def("service_impl_output", [](PyWiring &w, const PyServiceDesc &desc, const std::string &path,
                                    const PyPort &out) {
        service_impl_output(w.wiring_ref(), *desc.descriptor, path, out.ref);
    }, nb::arg("w"), nb::arg("desc"), nb::arg("path") = std::string{}, nb::arg("out"));
    m.def("register_multi_service_impl", [](PyWiring &w, nb::list descs, const std::string &path,
                                            const PyWiredFn &impl, nb::list inputs,
                                            bool default_fallback) {
        std::vector<const RuntimeServiceDescriptor *> descriptors;
        descriptors.reserve(nb::len(descs));
        for (nb::handle desc : descs) { descriptors.push_back(nb::cast<PyServiceDesc &>(desc).descriptor); }
        auto implementation_inputs = wiring_ports(inputs);
        register_multi_service_impl(w.wiring_ref(),
                                    std::span<const RuntimeServiceDescriptor *const>{descriptors.data(),
                                                                                     descriptors.size()},
                                    path, impl.fn, implementation_inputs, default_fallback);
    }, nb::arg("w"), nb::arg("descs"), nb::arg("path") = std::string{}, nb::arg("impl"),
       nb::arg("inputs") = nb::list(), nb::arg("default_fallback") = false);

    m.def("adaptor_client", [](PyWiring &w, const PyServiceDesc &desc, const std::string &path,
                               std::optional<PyPort> in) -> nb::object {
        const WiringPortRef *in_ref = in.has_value() ? &in->ref : nullptr;
        WiringPortRef        out    = adaptor_client(w.wiring_ref(), *desc.descriptor, path, in_ref);
        if (out.schema == nullptr) { return nb::none(); }
        return nb::cast(PyPort{std::move(out)});
    }, nb::arg("w"), nb::arg("desc"), nb::arg("path") = std::string{}, nb::arg("in") = nb::none());
    m.def("adaptor_from_graph", [](PyWiring &w, const PyServiceDesc &desc, const std::string &path) {
        return PyPort{adaptor_from_graph(w.wiring_ref(), *desc.descriptor, path)};
    }, nb::arg("w"), nb::arg("desc"), nb::arg("path") = std::string{});
    m.def("adaptor_to_graph", [](PyWiring &w, const PyServiceDesc &desc, const std::string &path,
                                 const PyPort &out) {
        adaptor_to_graph(w.wiring_ref(), *desc.descriptor, path, out.ref);
    }, nb::arg("w"), nb::arg("desc"), nb::arg("path") = std::string{}, nb::arg("out"));
    m.def("register_adaptor_impl",
          [](PyWiring &w, const PyServiceDesc &desc, const std::string &path,
             const PyWiredFn &impl, bool automatic, nb::list inputs, bool default_fallback) {
              auto implementation_inputs = wiring_ports(inputs);
              register_adaptor_impl(
                  w.wiring_ref(), *desc.descriptor, path, impl.fn,
                  automatic ? AdaptorImplMode::Automatic : AdaptorImplMode::Manual,
                  implementation_inputs, default_fallback);
          },
          nb::arg("w"), nb::arg("desc"),
          nb::arg("path") = std::string{}, nb::arg("impl"),
          nb::arg("automatic") = false, nb::arg("inputs") = nb::list(),
          nb::arg("default_fallback") = false);
    m.def("register_unbound_adaptor_impl",
          [](PyWiring &w, const PyWiredFn &impl, nb::list inputs) {
              auto implementation_inputs = wiring_ports(inputs);
              register_unbound_adaptor_impl(
                  w.wiring_ref(), impl.fn, implementation_inputs);
          },
          nb::arg("w"), nb::arg("impl"), nb::arg("inputs") = nb::list());
    m.def("service_adaptor_client", [](PyWiring &w, const PyServiceDesc &desc,
                                        const std::string &path, const PyPort &in) -> nb::object {
        WiringPortRef out = service_adaptor_client(
            w.wiring_ref(), *desc.descriptor, path, in.ref);
        return out.schema != nullptr ? nb::cast(PyPort{std::move(out)}) : nb::none();
    }, nb::arg("w"), nb::arg("desc"), nb::arg("path") = std::string{}, nb::arg("in"));
    m.def("service_adaptor_client_from_graph", [](PyWiring &w, const PyServiceDesc &desc,
                                                    const std::string &path, const PyPort &in,
                                                    const PyPort &request_id) {
        service_adaptor_client_from_graph(
            w.wiring_ref(), *desc.descriptor, path, in.ref, request_id.ref);
    }, nb::arg("w"), nb::arg("desc"), nb::arg("path") = std::string{},
       nb::arg("in"), nb::arg("request_id"));
    m.def("service_adaptor_client_to_graph", [](PyWiring &w, const PyServiceDesc &desc,
                                                  const std::string &path,
                                                  const PyPort &request_id) {
        return PyPort{service_adaptor_client_to_graph(
            w.wiring_ref(), *desc.descriptor, path, request_id.ref)};
    }, nb::arg("w"), nb::arg("desc"), nb::arg("path") = std::string{},
       nb::arg("request_id"));
    m.def("service_adaptor_from_graph", [](PyWiring &w, const PyServiceDesc &desc,
                                            const std::string &path) {
        return PyPort{service_adaptor_from_graph(w.wiring_ref(), *desc.descriptor, path)};
    }, nb::arg("w"), nb::arg("desc"), nb::arg("path") = std::string{});
    m.def("service_adaptor_to_graph", [](PyWiring &w, const PyServiceDesc &desc,
                                          const std::string &path, const PyPort &out) {
        service_adaptor_to_graph(w.wiring_ref(), *desc.descriptor, path, out.ref);
    }, nb::arg("w"), nb::arg("desc"), nb::arg("path") = std::string{}, nb::arg("out"));
    m.def("register_service_adaptor_impl", [](PyWiring &w, const PyServiceDesc &desc,
                                               const std::string &path, const PyWiredFn &impl,
                                               bool default_fallback) {
        register_service_adaptor_impl(w.wiring_ref(), *desc.descriptor, path, impl.fn,
                                      default_fallback);
    }, nb::arg("w"), nb::arg("desc"), nb::arg("path") = std::string{}, nb::arg("impl"),
       nb::arg("default_fallback") = false);

    m.def("push_context", [](PyWiring &w, const std::string &name, const PyPort &port) {
        if (name.empty()) { throw nb::value_error("context requires a non-empty name"); }
        graph_wiring_detail::push_context_source(w.wiring_ref(), name, port.ref);
    });
    m.def("pop_context", [] { graph_wiring_detail::pop_context_source(); });
    m.def("get_context", [](PyWiring &w, const std::string &name) {
        return PyPort{graph_wiring_detail::resolve_context_source(w.wiring_ref(), name)};
    });
    m.def("capture_outer_source", [](PyWiring &w, const PyPort &port) {
        return PyPort{w.wiring_ref().capture_outer_source(port.ref)};
    });
    m.def("same_wiring", [](PyWiring &lhs, PyWiring &rhs) {
        return &lhs.wiring_ref() == &rhs.wiring_ref();
    });
    m.def("has_context", [](PyWiring &w, const std::string &name) {
        return graph_wiring_detail::has_context_source(w.wiring_ref(), name);
    });
    m.attr("IN_MEMORY")       = std::string{record_replay::IN_MEMORY};
    m.attr("IN_MEMORY_DENSE")  = std::string{record_replay::IN_MEMORY_DENSE};
    m.attr("DATA_FRAME")       = std::string{record_replay::DATA_FRAME};
    m.attr("MIN_ST")     = nb::cast(MIN_ST);
    m.attr("MIN_TD")     = nb::cast(MIN_TD);
    m.attr("MAX_DT")     = nb::cast(MAX_DT);
    m.attr("MAX_ET")     = nb::cast(MAX_ET);

    nb::class_<PyOutput>(
        m, "OutputView",
        "A mutable, callback-scoped view of a Python node output.\n\n"
        "OutputView is injected through TS_OUT and the collection-specific "
        "output markers. Assign value for ordinary output, or mutate child "
        "views for collection outputs. The view expires when the node "
        "callback returns and must not be retained.")
        .def_prop_ro("owning_node", &PyOutput::owning_node,
                     "The node that owns this output view.")
        .def_prop_ro("owning_graph", &PyOutput::owning_graph,
                     "The graph that owns this output view.")
        .def_prop_ro("valid", &PyOutput::valid,
                     "Whether the output currently has a value.")
        .def_prop_ro("all_valid", &PyOutput::all_valid,
                     "Whether every direct child currently has a value.")
        .def_prop_ro("modified", &PyOutput::modified,
                     "Whether the output ticks in the current cycle.")
        .def_prop_ro("last_modified_time", &PyOutput::last_modified_time,
                     "The evaluation time of the most recent output tick.")
        .def_prop_ro("size", &PyOutput::window_size,
                     "The configured tick count or duration of a window output.")
        .def_prop_ro("min_size", &PyOutput::window_min_size,
                     "The configured minimum tick count or duration of a window output.")
        .def_prop_ro("value_times", &PyOutput::value_times,
                     "A NumPy-compatible datetime64 buffer containing window evaluation times.")
        .def_prop_ro("first_modified_time", &PyOutput::first_modified_time,
                     "The evaluation time of the oldest value retained by a window output.")
        .def_prop_ro("has_removed_value", &PyOutput::has_removed_value,
                     "Whether a window value was evicted in the current cycle.")
        .def_prop_ro("removed_value", &PyOutput::removed_value,
                     "The window value evicted in the current cycle, or None.")
        .def_prop_ro("delta_value", &PyOutput::delta_value,
                     "The change published in the current cycle.")
        .def("is_reference", &PyOutput::is_reference,
             "Return whether this is a reference-valued output.")
        .def_prop_rw("value", &PyOutput::value, &PyOutput::set_value,
                     "The current output value. Assigning publishes a value; "
                     "assigning None invalidates the output.",
                     nb::for_setter(nb::arg("value").none()))
        .def("can_apply_result", &PyOutput::can_apply_result, nb::arg("result"),
             "Return whether the Python value can be applied to this output.")
        .def("get_or_create", &PyOutput::get_or_create, nb::arg("key"),
             "Return the keyed child output, creating it when absent.")
        .def("get", &PyOutput::get, nb::arg("key"),
             "Return a keyed child output, or None when the key is absent.")
        .def("pop", &PyOutput::pop, nb::arg("key"),
             "Remove a dictionary child output and return it, or None when absent.")
        .def_prop_ro("key_set", &PyOutput::key_set,
                     "A zero-copy set output over the keys of a dictionary output.")
        .def("keys", &PyOutput::keys,
             "Return collection indices, keys, or bundle field names.")
        .def("values", &PyOutput::values,
             "Return current child outputs, or scalar values for a set output.")
        .def("items", &PyOutput::items,
             "Return current collection key/child-output pairs.")
        .def("modified_keys", &PyOutput::modified_keys,
             "Return collection keys modified in the current cycle.")
        .def("modified_values", &PyOutput::modified_values,
             "Return child outputs modified in the current cycle.")
        .def("modified_items", &PyOutput::modified_items,
             "Return collection key/child-output pairs modified in the current cycle.")
        .def("valid_keys", &PyOutput::valid_keys,
             "Return collection keys whose child outputs are valid.")
        .def("valid_values", &PyOutput::valid_values,
             "Return valid child outputs.")
        .def("valid_items", &PyOutput::valid_items,
             "Return collection key/child-output pairs whose children are valid.")
        .def("added_keys", &PyOutput::added_keys,
             "Return dictionary keys added in the current cycle.")
        .def("added_values", &PyOutput::added_values,
             "Return dictionary child outputs added in the current cycle.")
        .def("added_items", &PyOutput::added_items,
             "Return dictionary key/child-output pairs added in the current cycle.")
        .def("clear", &PyOutput::clear,
             "Clear the current collection value and publish removals.")
        .def("invalidate", &PyOutput::invalidate,
             "Invalidate the output without assigning a replacement value.")
        .def("removed_keys", &PyOutput::removed_keys,
             "Return keys removed in the current cycle.")
        .def("removed_values", &PyOutput::removed_values,
             "Return dictionary child outputs removed in the current cycle.")
        .def("removed_items", &PyOutput::removed_items,
             "Return dictionary key/child-output pairs removed in the current cycle.")
        .def("added", &PyOutput::set_added,
             "Return values added to a set output in the current cycle.")
        .def("removed", &PyOutput::set_removed,
             "Return values removed from a set output in the current cycle.")
        .def("was_added", &PyOutput::was_added, nb::arg("item"),
             "Return whether a set value was added in the current cycle.")
        .def("was_removed", &PyOutput::was_removed, nb::arg("item"),
             "Return whether a set value was removed in the current cycle.")
        .def("key_from_value", &PyOutput::key_from_value, nb::arg("value"),
             "Return the dictionary key, list index, or bundle field name for a child output.")
        .def("add", &PyOutput::add, nb::arg("value"),
             "Add a value to a set output and return whether it changed.")
        .def("remove", &PyOutput::remove, nb::arg("value"),
             "Remove a value from a set output and return whether it changed.")
        .def("__getitem__", &PyOutput::child, nb::arg("key"))
        .def("__setitem__", &PyOutput::set_child_value,
             nb::arg("key"), nb::arg("value"))
        .def("__delitem__", &PyOutput::erase, nb::arg("key"))
        .def("__contains__", &PyOutput::contains, nb::arg("key"))
        .def("__len__", &PyOutput::size)
        .def("__iter__", [](const PyOutput &self) -> nb::object {
            auto view = self.checked();
            nb::object source;
            switch (view.schema()->kind)
            {
                case TSTypeKind::TSD: source = self.keys(); break;
                case TSTypeKind::TSL:
                case TSTypeKind::TSB:
                case TSTypeKind::TSS: source = self.values(); break;
                default: throw nb::type_error("this output kind is not iterable");
            }
            PyObject *iterator = PyObject_GetIter(source.ptr());
            if (iterator == nullptr) { nb::raise_python_error(); }
            return nb::steal(iterator);
        })
        .def_prop_ro("as_schema", [](nb::object self_obj) -> nb::object {
            auto &self = nb::cast<PyOutput &>(self_obj);
            if (self.checked().schema()->kind != TSTypeKind::TSB)
            {
                throw nb::attribute_error("as_schema");
            }
            return self_obj;
        }, "The same bundle output viewed through its declared schema.")
        .def("__getattr__",
             [](const PyOutput &self, const std::string &name) -> PyOutput {
                 if (self.checked().schema()->kind != TSTypeKind::TSB)
                 {
                     throw nb::attribute_error(name.c_str());
                 }
                 try
                 {
                     return self.child(nb::str(name.c_str()));
                 }
                 catch (const std::out_of_range &)
                 {
                     throw nb::attribute_error(name.c_str());
                 }
             });
    nb::class_<PyRecordableState>(
        m, "RecordableStateView",
        "A mutable, callback-scoped view of persistent RECORDABLE_STATE.\n\n"
        "Values written through this view participate in the configured "
        "record/replay model. The view expires when the node callback returns "
        "and must not be retained.")
        .def_prop_ro("valid", &PyRecordableState::valid,
                     "Whether the state currently has a value.")
        .def_prop_ro("modified", &PyRecordableState::modified,
                     "Whether the state changed in the current cycle.")
        .def_prop_rw("value", &PyRecordableState::value,
                     &PyRecordableState::set_value,
                     "The current persistent value. Assigning records a new "
                     "state value through the native output.")
        .def("__getitem__", &PyRecordableState::child,
             "Return a statically addressed child state view.")
        .def("__getattr__",
             [](const PyRecordableState &self, const std::string &name) {
                 return self.child(nb::str(name.c_str()));
             });
    nb::class_<PyRuntimeGlobalState>(
        m, "RuntimeGlobalState",
        "A mutable, callback-scoped mapping over graph GlobalState.\n\n"
        "The runner copies configuration into native state before execution "
        "and copies it back afterwards. This view is valid only during the "
        "callback in which it is supplied.")
        .def("__len__", [](const PyRuntimeGlobalState &self) { return self.checked().size(); })
        .def("__contains__", [](const PyRuntimeGlobalState &self, const std::string &key) {
            return self.checked().contains(key);
        })
        .def("__getitem__", [](const PyRuntimeGlobalState &self, const std::string &key) -> nb::object {
            const GlobalStateView state = self.checked();
            if (!state.contains(key)) { throw nb::key_error(key.c_str()); }
            return value_to_py(state.get(key));
        })
        .def("get", [](const PyRuntimeGlobalState &self, const std::string &key,
                       nb::object fallback) -> nb::object {
            const GlobalStateView state = self.checked();
            return state.contains(key) ? value_to_py(state.get(key)) : fallback;
        }, nb::arg("key"), nb::arg("default") = nb::none())
        .def("__setitem__", [](const PyRuntimeGlobalState &self, const std::string &key, nb::handle value) {
            self.checked().set(key, py_to_value(value));
        })
        .def("__delitem__", [](const PyRuntimeGlobalState &self, const std::string &key) {
            if (!self.checked().erase(key)) { throw nb::key_error(key.c_str()); }
        })
        .def("keys", [](const PyRuntimeGlobalState &self) {
            nb::list result;
            const GlobalStateView state = self.checked();
            const ValueView       value = state.as_value().view();
            const auto            map   = value.as_map();
            const auto            keys  = map.keys();
            for (const ValueView key : keys)
            {
                result.append(value_to_py(key));
            }
            return result;
        });
    m.def("_table_schema_keys", [](const PyRuntimeGlobalState &state) {
        const auto config = record_replay::config(state.checked());
        return nb::make_tuple(config.date_key, config.as_of_key);
    });
    m.def("_temporal_at_zone",
          [](GlobalState &state, Instant instant, ZoneId zone) {
              return at_zone(
                  instant, zone, time_zone_provider(state.view()));
          });
    m.def("_temporal_at_zone",
          [](const PyRuntimeGlobalState &state, Instant instant,
             ZoneId zone) {
              const GlobalStateView view = state.checked();
              return at_zone(
                  instant, zone, time_zone_provider(view));
          });
    m.def("_temporal_resolve",
          [](GlobalState &state, CivilDateTime local, ZoneId zone,
             AmbiguousTimePolicy ambiguous,
             NonexistentTimePolicy nonexistent) {
              return resolve(
                  local, zone, time_zone_provider(state.view()),
                  ambiguous, nonexistent);
          },
          nb::arg("state"), nb::arg("local"), nb::arg("zone"),
          nb::arg("ambiguous") = AmbiguousTimePolicy::Reject,
          nb::arg("nonexistent") = NonexistentTimePolicy::Reject);
    m.def("_temporal_resolve",
          [](const PyRuntimeGlobalState &state, CivilDateTime local,
             ZoneId zone, AmbiguousTimePolicy ambiguous,
             NonexistentTimePolicy nonexistent) {
              const GlobalStateView view = state.checked();
              return resolve(
                  local, zone, time_zone_provider(view),
                  ambiguous, nonexistent);
          },
          nb::arg("state"), nb::arg("local"), nb::arg("zone"),
          nb::arg("ambiguous") = AmbiguousTimePolicy::Reject,
          nb::arg("nonexistent") = NonexistentTimePolicy::Reject);
    m.def("_temporal_convert_zone",
          [](GlobalState &state, ZonedDateTime value, ZoneId zone) {
              return convert_zone(
                  value, zone, time_zone_provider(state.view()));
          });
    m.def("_temporal_convert_zone",
          [](const PyRuntimeGlobalState &state, ZonedDateTime value,
             ZoneId zone) {
              const GlobalStateView view = state.checked();
              return convert_zone(
                  value, zone, time_zone_provider(view));
          });
    m.def("_temporal_checked_add_zoned",
          [](GlobalState &state, ZonedDateTime value,
             Duration delta) {
              return checked_add(
                  value, delta, time_zone_provider(state.view()));
          });
    m.def("_temporal_checked_add_zoned",
          [](const PyRuntimeGlobalState &state, ZonedDateTime value,
             Duration delta) {
              const GlobalStateView view = state.checked();
              return checked_add(
                  value, delta, time_zone_provider(view));
          });
    static PyGetSetDef py_ts_getset[] = {
        {"value", &py_ts_raw_get<&PyTimeSeries::value>, nullptr,
         "The current Python value, or None when the input is invalid.",
         nullptr},
        {"delta_value", &py_ts_raw_get<&PyTimeSeries::delta_value>, nullptr,
         "The change observed in the current evaluation cycle.", nullptr},
        {"modified", &py_ts_raw_get<&PyTimeSeries::modified>, nullptr,
         "Whether the input ticked in the current evaluation cycle.", nullptr},
        {"valid", &py_ts_raw_get<&PyTimeSeries::valid>, nullptr,
         "Whether the input currently has a usable value.", nullptr},
        {nullptr, nullptr, nullptr, nullptr, nullptr},
    };
    static PyType_Slot py_ts_slots[] = {
        {Py_tp_getset, static_cast<void *>(py_ts_getset)},
        {0, nullptr},
    };
    nb::class_<PyTimeSeries>(
        m, "TimeSeries", nb::type_slots(py_ts_slots),
        "A read-only, callback-scoped native time-series input view.\n\n"
        "Use value for the current value, delta_value for the current change, "
        "and modified to determine whether the input ticked this cycle. "
        "Collection views expose child and change-oriented methods. The view "
        "expires when the node callback returns and must not be retained.")
        .def_prop_ro("_kind", [](const PyTimeSeries &self) { return static_cast<int>(self.kind()); })
        .def_prop_ro("owning_node", &PyTimeSeries::owning_node,
                     "The node consuming this input.")
        .def_prop_ro("owning_graph", &PyTimeSeries::owning_graph,
                     "The graph containing the consuming node.")
        .def("is_reference", &PyTimeSeries::is_reference,
             "Return whether this input carries a time-series reference.")
        // hgraph's runtime activity control: a node may passivate/reactivate
        // its own input subscription (the C++ In views expose the same).
        .def("make_passive",
             [](PyTimeSeries &self) {
                 self.require_alive();
                 self.view.make_passive();
             },
             "Stop this input from scheduling its node when it ticks.")
        .def("make_active",
             [](PyTimeSeries &self) {
                 self.require_alive();
                 self.view.make_active();
             },
             "Resume scheduling the node when this input ticks.")
        // The activity STATE query completing the trio (upstream parity —
        // the annotation-class ABC surface is deliberately not replicated,
        // but instance behaviour must be: ruling 2026-08-01).
        .def_prop_ro("active",
                     [](const PyTimeSeries &self) {
                         self.require_alive();
                         return self.view.active();
                     },
                     "Whether ticks on this input currently schedule its node.")
        // value/delta_value/modified/valid are raw tp_getset slots above —
        // a def_prop_ro here would REPLACE the raw descriptor in the type
        // dictionary and silently restore the vectorcall path.
        .def_prop_ro("all_valid", &PyTimeSeries::all_valid,
                     "Whether every direct child currently has a value.")
        .def_prop_ro("size", &PyTimeSeries::window_size,
                     "The configured tick count or duration of a window input.")
        .def_prop_ro("min_size", &PyTimeSeries::window_min_size,
                     "The configured minimum tick count or duration of a window input.")
        .def_prop_ro("value_times", &PyTimeSeries::value_times,
                     "A NumPy-compatible datetime64 buffer containing window evaluation times.")
        .def_prop_ro("first_modified_time", &PyTimeSeries::first_modified_time,
                     "The evaluation time of the oldest value retained by a window input.")
        .def_prop_ro("has_removed_value", &PyTimeSeries::has_removed_value,
                     "Whether a window value was evicted in the current cycle.")
        .def_prop_ro("removed_value", &PyTimeSeries::removed_value,
                     "The window value evicted in the current cycle, or None.")
        .def_prop_ro("last_modified_time", &PyTimeSeries::last_modified_time,
                     "The evaluation time of the most recent tick.")
        .def("added", &PyTimeSeries::added,
             "Return values added to a set in the current cycle.")
        .def("removed", &PyTimeSeries::removed,
             "Return values removed from a set in the current cycle.")
        .def("was_added", &PyTimeSeries::was_added, nb::arg("item"),
             "Return whether a set value was added in the current cycle.")
        .def("was_removed", &PyTimeSeries::was_removed, nb::arg("item"),
             "Return whether a set value was removed in the current cycle.")
        .def("keys", &PyTimeSeries::keys,
             "Return the current keys or field names of a collection input.")
        .def("items", &PyTimeSeries::items,
             "Return the current key/child-input pairs of a collection input.")
        .def("modified_keys", &PyTimeSeries::modified_keys,
             "Return collection keys modified in the current cycle.")
        .def("modified_items", &PyTimeSeries::modified_items,
             "Return key/value pairs modified in the current cycle.")
        .def("modified_values", &PyTimeSeries::modified_values,
             "Return collection values modified in the current cycle.")
        .def("valid_keys", &PyTimeSeries::valid_keys,
             "Return collection keys whose child inputs are valid.")
        .def("valid_items", &PyTimeSeries::valid_items,
             "Return collection key/child-input pairs whose children are valid.")
        .def("valid_values", &PyTimeSeries::valid_values,
             "Return valid collection child inputs.")
        .def("added_keys", &PyTimeSeries::added_keys,
             "Return dictionary keys added in the current cycle.")
        .def("added_items", &PyTimeSeries::added_items,
             "Return dictionary key/child-input pairs added in the current cycle.")
        .def("added_values", &PyTimeSeries::added_values,
             "Return dictionary child inputs added in the current cycle.")
        .def("values", &PyTimeSeries::values,
             "Return the current child values of a collection input.")
        .def("removed_keys", &PyTimeSeries::removed_keys,
             "Return dictionary keys removed in the current cycle.")
        .def("removed_items", &PyTimeSeries::removed_items,
             "Return dictionary key/child-input pairs removed in the current cycle.")
        .def("removed_values", &PyTimeSeries::removed_values,
             "Return dictionary child inputs removed in the current cycle.")
        .def("get", &PyTimeSeries::get, nb::arg("key"),
             "Return a dictionary child input, or None when the key is absent.")
        .def_prop_ro("key_set", &PyTimeSeries::key_set,
                     "A zero-copy set view over the keys of a dictionary input.")
        .def("key_from_value", &PyTimeSeries::key_from_value, nb::arg("value"),
             "Return the dictionary key, list index, or bundle field name for a child input.")
        .def("__getitem__", &PyTimeSeries::child_at, nb::arg("key"))
        .def_prop_ro("as_schema", [](nb::object self_obj) -> nb::object {
            auto &self = nb::cast<PyTimeSeries &>(self_obj);
            if (self.kind() != TSTypeKind::TSB)
            {
                throw nb::attribute_error("as_schema");
            }
            return self_obj;
        }, "The same bundle input viewed through its declared schema.")
        .def("__getattr__", [](nb::object self_obj, const std::string &name) -> nb::object {
            auto &self = nb::cast<PyTimeSeries &>(self_obj);
            if (self.kind() != TSTypeKind::TSB) { throw nb::attribute_error(name.c_str()); }
            // hgraph's TSB.as_schema: typed field access (the same view).
            if (name == "as_schema") { return self_obj; }
            try
            {
                return nb::cast(self.child_at(nb::cast(name)));
            }
            catch (const std::out_of_range &)
            {
                // hgraph parity: an absent bundle field is an ATTRIBUTE error
                // (the same exception a TSL attribute probe raises).
                throw nb::attribute_error(name.c_str());
            }
        })
        .def("__contains__", &PyTimeSeries::contains, nb::arg("key"))
        .def("__len__", &PyTimeSeries::size)
        .def("__iter__", [](const PyTimeSeries &self) -> nb::object {
            nb::object source;
            switch (self.kind())
            {
                case TSTypeKind::TSD: source = self.keys(); break;
                case TSTypeKind::TSL:
                case TSTypeKind::TSB:
                case TSTypeKind::TSS: source = self.values(); break;
                default: throw nb::type_error("this time-series kind is not iterable");
            }
            PyObject *iterator = PyObject_GetIter(source.ptr());
            if (iterator == nullptr) { nb::raise_python_error(); }
            return nb::steal(iterator);
        })
        .def("__str__", [](const PyTimeSeries &self) { return nb::str(self.value()); })
        .def("__repr__", [](const PyTimeSeries &self) { return nb::str("TimeSeries({})").format(self.value()); });
    m.def("_set_cmp_result_enum", [](nb::object enum_class) { cmp_result_enum_slot() = std::move(enum_class); });
    m.def("_set_divide_by_zero_enum",
          [](nb::object enum_class) { divide_by_zero_enum_slot() = std::move(enum_class); });
    m.def("_set_removed_sentinel", [](nb::object sentinel) { PyTimeSeries::removed_slot() = std::move(sentinel); });
    m.def("_set_remove_if_exists_sentinel",
          [](nb::object sentinel) { remove_if_exists_sentinel_slot() = std::move(sentinel); });
    m.def("_set_removed_class", [](nb::object cls) { python_bridge::removed_class_slot() = std::move(cls); });
    m.def("_set_set_delta_class", [](nb::object cls) { python_bridge::set_delta_class_slot() = std::move(cls); });
    m.def("_set_delta_shaper", [](nb::object fn) { python_bridge::delta_shaper_slot() = std::move(fn); });
    nb::class_<PyArrowStream>(m, "ArrowStream")
        .def("__arrow_c_stream__",
             [](const PyArrowStream &self, nb::handle) { return self.capsule(); },
             nb::arg("requested_schema") = nb::none());
    nb::class_<PySeriesArray>(m, "ArrowSeriesArray")
        .def("__arrow_c_array__",
             [](const PySeriesArray &self, nb::handle) { return self.arrow_c_array(); },
             nb::arg("requested_schema") = nb::none());
    install_value_conversion_hooks();   // bind module-owned conversion onto the type-erased ops
    // Wiring-time scalar values as one list-of-Any (part of node identity).
    m.def("any_list", [](nb::list values) {
        auto &registry = TypeRegistry::instance();
        const auto *schema  = registry.mutable_list(registry.any());
        const auto type = ValuePlanFactory::instance().type_for(schema);
        Value      result{type};
        MutableListView list{result.begin_mutation()};
        for (nb::handle item : values)
        {
            Value boxed_value{ValuePlanFactory::instance().type_for(registry.any())};
            MutableAnyView{boxed_value.begin_mutation()}.set(py_to_value(item));
            list.push_back(boxed_value.view());
        }
        return PyScalarValue{std::move(result)};
    });

    nb::class_<PyEvalClock>(
        m, "EvaluationClock",
        "A callback-scoped view of graph evaluation time.\n\n"
        "Inject with CLOCK or EvaluationClock. In simulation, now follows "
        "evaluation time; in real-time mode it represents wall-clock time.")
        .def_prop_ro("evaluation_time", [](const PyEvalClock &clock) { return clock.evaluation_time(); },
                     "The logical time of the current evaluation cycle.")
        .def_prop_ro("now", [](const PyEvalClock &clock) { return clock.now(); },
                     "The clock's current time for the active evaluation mode.")
        .def_prop_ro("cycle_time", [](const PyEvalClock &clock) { return clock.cycle_time(); },
                     "Elapsed wall time since the current cycle began.")
        .def_prop_ro("next_cycle_evaluation_time",
                     [](const PyEvalClock &clock) { return clock.next_cycle_evaluation_time(); },
                     "The logical time immediately following this evaluation "
                     "cycle (evaluation_time + MIN_TD).");
    nb::class_<PyEvaluationEngineApi>(
        m, "EvaluationEngineApi",
        "A callback-scoped control view for the running graph executor.\n\n"
        "Inject this type into a Python node to inspect the run interval, "
        "request a graceful stop, or schedule one-shot cycle-boundary "
        "notifications. Do not retain the view after the callback returns.")
        // One-shot cycle-boundary notifications: python sugar over the
        // C++-primary EngineControlView facility (C++-first ruling; the
        // wrapper re-acquires the GIL because drains run outside the
        // cycle hold). Lifecycle observers themselves stay C++-only.
        .def("add_before_evaluation_notification",
             [](PyEvaluationEngineApi &self, nb::object fn) {
                 self.checked().add_before_evaluation_notification(
                     py_notification_thunk(std::move(fn), self.lease));
             },
             "Run a callable once before the next graph evaluation cycle.")
        .def("add_after_evaluation_notification",
             [](PyEvaluationEngineApi &self, nb::object fn) {
                 self.checked().add_after_evaluation_notification(
                     py_notification_thunk(std::move(fn), self.lease));
             },
             "Run a callable once after the current graph evaluation cycle.")
        .def_prop_ro("evaluation_mode", [](const PyEvaluationEngineApi &self) {
            return self.checked().mode() == GraphExecutorMode::RealTime ? "real_time" : "simulation";
        }, "The active execution mode: 'simulation' or 'real_time'.")
        .def_prop_ro("start_time", [](const PyEvaluationEngineApi &self) { return self.checked().start_time(); },
                     "The inclusive start of the configured run interval.")
        .def_prop_ro("end_time", [](const PyEvaluationEngineApi &self) { return self.checked().end_time(); },
                     "The exclusive end of the configured run interval.")
        .def_prop_ro("evaluation_clock", [](const PyEvaluationEngineApi &self) {
            return PyEvalClock{self.checked().evaluation_clock(), self.lease};
        }, "The callback-scoped clock for the running graph.")
        .def_prop_ro("is_stop_requested",
                     [](const PyEvaluationEngineApi &self) { return self.checked().stop_requested(); },
                     "Whether a graceful graph stop has been requested.")
        .def("request_engine_stop", [](const PyEvaluationEngineApi &self) { self.checked().request_stop(); },
             "Request a graceful stop after the current evaluation cycle.");
    nb::enum_<NodeKind>(m, "NodeType",
                        "The runtime role of a graph node.")
        .value("COMPUTE", NodeKind::Compute)
        .value("PUSH_SOURCE", NodeKind::PushSource)
        .value("PULL_SOURCE", NodeKind::PullSource)
        .value("SINK", NodeKind::Sink)
        .value("NESTED", NodeKind::Nested);
    nb::class_<PyGraph>(
        m, "Graph",
        "A read-only, callback-scoped view of a running graph.\n\n"
        "Graph views are supplied for inspection and diagnostics. Graph "
        "lifecycle is owned by the executor; retain identifiers rather than "
        "retaining this view beyond its callback.")
        .def_prop_ro("graph_id", [](const PyGraph &self) {
            const auto id = self.graph_id();
            nb::tuple result = nb::steal<nb::tuple>(
                PyTuple_New(static_cast<Py_ssize_t>(id.size())));
            for (std::size_t index = 0; index < id.size(); ++index)
            {
                if (PyTuple_SetItem(result.ptr(), static_cast<Py_ssize_t>(index),
                                    nb::cast(id[index]).release().ptr()) != 0)
                {
                    throw nb::python_error();
                }
            }
            return result;
        }, "The hierarchical numeric path identifying this graph.")
        .def_prop_ro("label", [](const PyGraph &self) {
            const GraphView graph = self.checked();
            const auto *schema = graph.schema();
            return schema != nullptr && schema->display_name != nullptr
                       ? std::string{schema->display_name}
                       : std::string{};
        }, "The graph's diagnostic display name.")
        .def_prop_ro("started", [](const PyGraph &self) {
            return self.checked().started();
        }, "Whether graph start has completed.")
        .def_prop_ro("evaluating", [](const PyGraph &self) {
            return self.checked().evaluating();
        }, "Whether the graph is currently evaluating a cycle.")
        .def_prop_ro("evaluation_clock", [](const PyGraph &self) {
            return PyEvalClock{self.checked().executor().evaluation_clock(), self.lease};
        }, "The callback-scoped clock shared by this graph's executor.")
        .def_prop_ro("parent_node", [](const PyGraph &self) -> nb::object {
            const GraphView graph = self.checked();
            if (!graph.is_nested()) { return nb::none(); }
            const NodeView parent = graph.as_nested().parent_node();
            return nb::cast(PyNode{parent.pointer(), NodeScheduler{}, self.lease});
        }, "The node owning this nested graph, or None for the root graph.")
        .def_prop_ro("nodes", [](const PyGraph &self) {
            const GraphView graph = self.checked();
            nb::list result;
            for (std::size_t index = 0; index < graph.node_count(); ++index)
            {
                const NodeView node = graph.node_at(index);
                result.append(nb::cast(
                    PyNode{node.pointer(), NodeScheduler{}, self.lease}));
            }
            return result;
        }, "A snapshot list of callback-scoped views over this graph's nodes.");
    nb::class_<PyNode>(
        m, "Node",
        "A callback-scoped view of the currently running node.\n\n"
        "Inject with NODE to inspect identity or request scheduling. Runtime "
        "topology and lifecycle remain executor-owned, and this view must not "
        "be retained beyond the callback.")
        .def_prop_ro("node_ndx", [](const PyNode &self) { return self.checked().node_index(); },
                     "Compatibility alias for node_index.")
        .def_prop_ro("node_index", [](const PyNode &self) { return self.checked().node_index(); },
                     "The node's zero-based index within its graph.")
        .def_prop_ro("node_id", [](const PyNode &self) {
            const auto id = self.node_id();
            nb::tuple result = nb::steal<nb::tuple>(PyTuple_New(static_cast<Py_ssize_t>(id.size())));
            for (std::size_t index = 0; index < id.size(); ++index)
            {
                if (PyTuple_SetItem(result.ptr(), static_cast<Py_ssize_t>(index),
                                    nb::cast(id[index]).release().ptr()) != 0)
                {
                    throw nb::python_error();
                }
            }
            return result;
        }, "The hierarchical numeric path identifying this node.")
        .def_prop_ro("owning_graph_id", [](const PyNode &self) {
            const auto id = self.node_id();
            const std::size_t size = id.empty() ? 0 : id.size() - 1;
            nb::tuple result = nb::steal<nb::tuple>(PyTuple_New(static_cast<Py_ssize_t>(size)));
            for (std::size_t index = 0; index < size; ++index)
            {
                if (PyTuple_SetItem(result.ptr(), static_cast<Py_ssize_t>(index),
                                    nb::cast(id[index]).release().ptr()) != 0)
                {
                    throw nb::python_error();
                }
            }
            return result;
        }, "The hierarchical numeric path identifying the containing graph.")
        .def_prop_ro("label", [](const PyNode &self) { return std::string{self.checked().label()}; },
                     "The node's diagnostic display name.")
        .def_prop_ro("graph", [](const PyNode &self) {
            const GraphView graph = self.checked().graph();
            return PyGraph{graph.pointer(), self.lease};
        }, "The callback-scoped view of the containing graph.")
        .def_prop_ro("node_type", [](const PyNode &self) { return self.checked().node_kind(); },
                     "The runtime role of this node.")
        .def_prop_ro("started", [](const PyNode &self) { return self.checked().started(); },
                     "Whether node start has completed.")
        .def_prop_ro("has_input", [](const PyNode &self) { return self.checked().has_input(); },
                     "Whether the node owns an input time-series tree.")
        .def_prop_ro("has_output", [](const PyNode &self) { return self.checked().has_output(); },
                     "Whether the node owns an output time-series tree.")
        .def("notify", &PyNode::notify,
             "Schedule this node at the supplied evaluation time.")
        .def("notify_next_cycle", &PyNode::notify_next_cycle,
             "Schedule this node for the next evaluation cycle.");
    nb::class_<PyScheduler>(
        m, "Scheduler",
        "A callback-scoped scheduler for the current node.\n\n"
        "Inject with SCHEDULER. A tagged schedule replaces the existing event "
        "with the same tag; reset() cancels outstanding schedules.")
        // hgraph's SCHEDULER.schedule(when: datetime | timedelta, tag=None,
        // on_wall_clock=False). Two overloads distinguish absolute times from
        // relative deltas; a non-empty tag replaces any prior event under it.
        .def("schedule",
             [](const PyScheduler &self, DateTime when, std::optional<std::string> tag, bool on_wall_clock) {
                 self.scheduler.schedule(when, std::move(tag), on_wall_clock);
             },
             nb::arg("when"), nb::arg("tag") = nb::none(), nb::arg("on_wall_clock") = false,
             "Schedule at an absolute evaluation time.")
        .def("schedule",
             [](const PyScheduler &self, TimeDelta delta, std::optional<std::string> tag, bool on_wall_clock) {
                 self.scheduler.schedule(delta, std::move(tag), on_wall_clock);
             },
             nb::arg("when"), nb::arg("tag") = nb::none(), nb::arg("on_wall_clock") = false,
             "Schedule after a duration relative to the current evaluation time.")
        .def("schedule_delta", [](const PyScheduler &self, TimeDelta delta) { self.scheduler.schedule(delta); },
             "Schedule after a duration relative to the current evaluation time.")
        .def("reset", [](const PyScheduler &self) { self.scheduler.reset(); },
             "Cancel every outstanding schedule for this node.")
        .def("has_tag", [](const PyScheduler &self, std::string_view tag) { return self.scheduler.has_tag(tag); },
             "Return whether an event is currently scheduled under the tag.")
        .def_prop_ro("is_scheduled", [](const PyScheduler &self) { return self.scheduler.is_scheduled(); },
                     "Whether this node has any outstanding schedule.")
        .def_prop_ro("is_scheduled_now", [](const PyScheduler &self) { return self.scheduler.is_scheduled_now(); },
                     "Whether this node is scheduled for the current cycle.");
    }
}  // namespace hgraph::python_bridge
