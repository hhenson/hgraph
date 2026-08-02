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
    // surface is mesh_(func)[key] / get_mesh(func), matching ext/main.
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

    nb::class_<PyOutput>(m, "OutputView")
        .def_prop_ro("owning_node", &PyOutput::owning_node)
        .def_prop_ro("owning_graph", &PyOutput::owning_graph)
        .def_prop_ro("valid", &PyOutput::valid)
        .def_prop_ro("all_valid", &PyOutput::all_valid)
        .def_prop_ro("modified", &PyOutput::modified)
        .def_prop_ro("last_modified_time", &PyOutput::last_modified_time)
        .def_prop_ro("delta_value", &PyOutput::delta_value)
        .def("is_reference", &PyOutput::is_reference)
        .def_prop_rw("value", &PyOutput::value, &PyOutput::set_value,
                     nb::for_setter(nb::arg("value").none()))
        .def("can_apply_result", &PyOutput::can_apply_result)
        .def("get_or_create", &PyOutput::get_or_create)
        .def("clear", &PyOutput::clear)
        .def("invalidate", &PyOutput::invalidate)
        .def("removed_keys", &PyOutput::removed_keys)
        .def("add", &PyOutput::add)
        .def("remove", &PyOutput::remove)
        .def("__getitem__", &PyOutput::child)
        .def("__delitem__", &PyOutput::erase)
        .def("__contains__", &PyOutput::contains)
        .def("__len__", &PyOutput::size)
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
    nb::class_<PyRecordableState>(m, "RecordableStateView")
        .def_prop_ro("valid", &PyRecordableState::valid)
        .def_prop_ro("modified", &PyRecordableState::modified)
        .def_prop_rw("value", &PyRecordableState::value,
                     &PyRecordableState::set_value)
        .def("__getitem__", &PyRecordableState::child)
        .def("__getattr__",
             [](const PyRecordableState &self, const std::string &name) {
                 return self.child(nb::str(name.c_str()));
             });
    nb::class_<PyRuntimeGlobalState>(m, "RuntimeGlobalState")
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
        {"value", &py_ts_raw_get<&PyTimeSeries::value>, nullptr, nullptr, nullptr},
        {"delta_value", &py_ts_raw_get<&PyTimeSeries::delta_value>, nullptr, nullptr, nullptr},
        {"modified", &py_ts_raw_get<&PyTimeSeries::modified>, nullptr, nullptr, nullptr},
        {"valid", &py_ts_raw_get<&PyTimeSeries::valid>, nullptr, nullptr, nullptr},
        {nullptr, nullptr, nullptr, nullptr, nullptr},
    };
    static PyType_Slot py_ts_slots[] = {
        {Py_tp_getset, static_cast<void *>(py_ts_getset)},
        {0, nullptr},
    };
    nb::class_<PyTimeSeries>(m, "TimeSeries", nb::type_slots(py_ts_slots))
        .def_prop_ro("_kind", [](const PyTimeSeries &self) { return static_cast<int>(self.kind()); })
        .def_prop_ro("owning_node", &PyTimeSeries::owning_node)
        .def_prop_ro("owning_graph", &PyTimeSeries::owning_graph)
        .def("is_reference", &PyTimeSeries::is_reference)
        // hgraph's runtime activity control: a node may passivate/reactivate
        // its own input subscription (the C++ In views expose the same).
        .def("make_passive",
             [](PyTimeSeries &self) {
                 self.require_alive();
                 self.view.make_passive();
             })
        .def("make_active",
             [](PyTimeSeries &self) {
                 self.require_alive();
                 self.view.make_active();
             })
        // The activity STATE query completing the trio (upstream parity —
        // the annotation-class ABC surface is deliberately not replicated,
        // but instance behaviour must be: ruling 2026-08-01).
        .def_prop_ro("active",
                     [](const PyTimeSeries &self) {
                         self.require_alive();
                         return self.view.active();
                     })
        // value/delta_value/modified/valid are raw tp_getset slots above —
        // a def_prop_ro here would REPLACE the raw descriptor in the type
        // dictionary and silently restore the vectorcall path.
        .def_prop_ro("all_valid", &PyTimeSeries::all_valid)
        // TSW eviction surface (hgraph's removed_value pair).
        .def_prop_ro("has_removed_value",
                     [](const PyTimeSeries &self) {
                         const auto &view = self.checked();
                         if (view.schema()->kind != TSTypeKind::TSW)
                         {
                             throw nb::attribute_error("has_removed_value");
                         }
                         return view.as_window().has_removed_value();
                     })
        .def_prop_ro("removed_value",
                     [](const PyTimeSeries &self) -> nb::object {
                         const auto &view = self.checked();
                         if (view.schema()->kind != TSTypeKind::TSW)
                         {
                             throw nb::attribute_error("removed_value");
                         }
                         auto window = view.as_window();
                         return window.has_removed_value() ? value_to_py(window.removed_value()) : nb::none();
                     })
        .def_prop_ro("last_modified_time", &PyTimeSeries::last_modified_time)
        .def("added", &PyTimeSeries::added)
        .def("removed", &PyTimeSeries::removed)
        .def("keys", &PyTimeSeries::keys)
        .def("modified_keys", &PyTimeSeries::modified_keys)
        .def("modified_items", &PyTimeSeries::modified_items)
        .def("modified_values", &PyTimeSeries::modified_values)
        .def("values", &PyTimeSeries::values)
        .def("removed_keys", &PyTimeSeries::removed_keys)
        .def("__getitem__", &PyTimeSeries::child_at)
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
        .def("__contains__", &PyTimeSeries::contains)
        .def("__len__", &PyTimeSeries::size)
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

    nb::class_<PyEvalClock>(m, "EvaluationClock")
        .def_prop_ro("evaluation_time", [](const PyEvalClock &clock) { return clock.evaluation_time; })
        .def_prop_ro("now", [](const PyEvalClock &clock) { return clock.now; })
        .def_prop_ro("cycle_time", [](const PyEvalClock &clock) { return clock.cycle_time; })
        .def_prop_ro("next_cycle_evaluation_time",
                     [](const PyEvalClock &clock) { return clock.next_cycle_evaluation_time; });
    nb::class_<PyEvaluationEngineApi>(m, "EvaluationEngineApi")
        // One-shot cycle-boundary notifications: python sugar over the
        // C++-primary EngineControlView facility (C++-first ruling; the
        // wrapper re-acquires the GIL because drains run outside the
        // cycle hold). Lifecycle observers themselves stay C++-only.
        .def("add_before_evaluation_notification",
             [](PyEvaluationEngineApi &self, nb::object fn) {
                 self.checked().add_before_evaluation_notification(
                     py_notification_thunk(std::move(fn), self.lease));
             })
        .def("add_after_evaluation_notification",
             [](PyEvaluationEngineApi &self, nb::object fn) {
                 self.checked().add_after_evaluation_notification(
                     py_notification_thunk(std::move(fn), self.lease));
             })
        .def_prop_ro("evaluation_mode", [](const PyEvaluationEngineApi &self) {
            return self.checked().mode() == GraphExecutorMode::RealTime ? "real_time" : "simulation";
        })
        .def_prop_ro("start_time", [](const PyEvaluationEngineApi &self) { return self.checked().start_time(); })
        .def_prop_ro("end_time", [](const PyEvaluationEngineApi &self) { return self.checked().end_time(); })
        .def_prop_ro("evaluation_clock", [](const PyEvaluationEngineApi &self) {
            return PyEvalClock{self.checked().evaluation_clock()};
        })
        .def_prop_ro("is_stop_requested",
                     [](const PyEvaluationEngineApi &self) { return self.checked().stop_requested(); })
        .def("request_engine_stop", [](const PyEvaluationEngineApi &self) { self.checked().request_stop(); });
    nb::enum_<NodeKind>(m, "NodeType")
        .value("COMPUTE", NodeKind::Compute)
        .value("PUSH_SOURCE", NodeKind::PushSource)
        .value("PULL_SOURCE", NodeKind::PullSource)
        .value("SINK", NodeKind::Sink)
        .value("NESTED", NodeKind::Nested);
    nb::class_<PyGraph>(m, "Graph")
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
        })
        .def_prop_ro("label", [](const PyGraph &self) {
            const GraphView graph = self.checked();
            const auto *schema = graph.schema();
            return schema != nullptr && schema->display_name != nullptr
                       ? std::string{schema->display_name}
                       : std::string{};
        })
        .def_prop_ro("started", [](const PyGraph &self) {
            return self.checked().started();
        })
        .def_prop_ro("evaluating", [](const PyGraph &self) {
            return self.checked().evaluating();
        })
        .def_prop_ro("evaluation_clock", [](const PyGraph &self) {
            return PyEvalClock{self.checked().executor().evaluation_clock()};
        })
        .def_prop_ro("parent_node", [](const PyGraph &self) -> nb::object {
            const GraphView graph = self.checked();
            if (!graph.is_nested()) { return nb::none(); }
            const NodeView parent = graph.as_nested().parent_node();
            return nb::cast(PyNode{parent.pointer(), NodeScheduler{}, self.lease});
        })
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
        });
    nb::class_<PyNode>(m, "Node")
        .def_prop_ro("node_ndx", [](const PyNode &self) { return self.checked().node_index(); })
        .def_prop_ro("node_index", [](const PyNode &self) { return self.checked().node_index(); })
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
        })
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
        })
        .def_prop_ro("label", [](const PyNode &self) { return std::string{self.checked().label()}; })
        .def_prop_ro("graph", [](const PyNode &self) {
            const GraphView graph = self.checked().graph();
            return PyGraph{graph.pointer(), self.lease};
        })
        .def_prop_ro("node_type", [](const PyNode &self) { return self.checked().node_kind(); })
        .def_prop_ro("started", [](const PyNode &self) { return self.checked().started(); })
        .def_prop_ro("has_input", [](const PyNode &self) { return self.checked().has_input(); })
        .def_prop_ro("has_output", [](const PyNode &self) { return self.checked().has_output(); })
        .def("notify", &PyNode::notify)
        .def("notify_next_cycle", &PyNode::notify_next_cycle);
    nb::class_<PyScheduler>(m, "Scheduler")
        // hgraph's SCHEDULER.schedule(when: datetime | timedelta, tag=None,
        // on_wall_clock=False). Two overloads distinguish absolute times from
        // relative deltas; a non-empty tag replaces any prior event under it.
        .def("schedule",
             [](const PyScheduler &self, DateTime when, std::optional<std::string> tag, bool on_wall_clock) {
                 self.scheduler.schedule(when, std::move(tag), on_wall_clock);
             },
             nb::arg("when"), nb::arg("tag") = nb::none(), nb::arg("on_wall_clock") = false)
        .def("schedule",
             [](const PyScheduler &self, TimeDelta delta, std::optional<std::string> tag, bool on_wall_clock) {
                 self.scheduler.schedule(delta, std::move(tag), on_wall_clock);
             },
             nb::arg("when"), nb::arg("tag") = nb::none(), nb::arg("on_wall_clock") = false)
        .def("schedule_delta", [](const PyScheduler &self, TimeDelta delta) { self.scheduler.schedule(delta); })
        .def("reset", [](const PyScheduler &self) { self.scheduler.reset(); })
        .def("has_tag", [](const PyScheduler &self, std::string_view tag) { return self.scheduler.has_tag(tag); })
        .def_prop_ro("is_scheduled", [](const PyScheduler &self) { return self.scheduler.is_scheduled(); })
        .def_prop_ro("is_scheduled_now", [](const PyScheduler &self) { return self.scheduler.is_scheduled_now(); });
    }
}  // namespace hgraph::python_bridge
