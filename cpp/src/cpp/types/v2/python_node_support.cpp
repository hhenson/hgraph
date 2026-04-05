#include <hgraph/types/v2/python_node_support.h>

#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/traits.h>
#include <hgraph/types/v2/evaluation_clock.h>
#include <hgraph/types/v2/evaluation_engine.h>
#include <hgraph/types/v2/graph.h>
#include <hgraph/types/v2/node.h>

#include <fmt/format.h>

#include <cassert>
#include <string>
#include <vector>

namespace hgraph::v2
{
    namespace
    {
        [[nodiscard]] Graph &graph_of(Node *node) noexcept
        {
            assert(node != nullptr);
            assert(node->graph() != nullptr);
            return *node->graph();
        }

        [[nodiscard]] const TSMeta *field_schema_or_throw(const TSMeta *schema, std::string_view field_name)
        {
            if (schema == nullptr || schema->kind != TSKind::TSB) {
                throw std::logic_error("v2 Python time-series bundle access requires a TSB schema");
            }

            for (size_t i = 0; i < schema->data.tsb.field_count; ++i) {
                const auto &field = schema->data.tsb.fields[i];
                if (field_name == field.name) { return field.ts_type; }
            }

            throw std::out_of_range("v2 Python time-series bundle field is out of range");
        }

        void mark_output_modified(const TSOutputView &view, engine_time_t evaluation_time)
        {
            LinkedTSContext context = view.linked_context();
            if (context.ts_state == nullptr) {
                throw std::logic_error("v2 Python output mutation requires a linked output state");
            }
            context.ts_state->mark_modified(evaluation_time);
        }

        class PythonTraitsHandle
        {
          public:
            explicit PythonTraitsHandle(Node *node) noexcept : m_node(node) {}

            void set_traits(nb::kwargs values) const
            {
                graph_of(m_node).traits().set_traits(std::move(values));
            }

            void set_trait(const std::string &name, nb::object value) const
            {
                graph_of(m_node).traits().set_trait(name, std::move(value));
            }

            [[nodiscard]] nb::object get_trait(const std::string &name) const
            {
                return graph_of(m_node).traits().get_trait(name);
            }

            [[nodiscard]] nb::object get_trait_or(const std::string &name, nb::object default_value = nb::none()) const
            {
                return graph_of(m_node).traits().get_trait_or(name, std::move(default_value));
            }

          private:
            Node *m_node{nullptr};
        };

        class PythonGraphHandle
        {
          public:
            explicit PythonGraphHandle(Node *node) noexcept : m_node(node) {}

            [[nodiscard]] nb::tuple graph_id() const
            {
                return nb::tuple();
            }

            [[nodiscard]] nb::object parent_node() const
            {
                return nb::none();
            }

            [[nodiscard]] std::string label() const
            {
                return {};
            }

            [[nodiscard]] EvaluationClock evaluation_clock() const
            {
                return graph_of(m_node).evaluation_clock();
            }

            [[nodiscard]] EvaluationEngineApi evaluation_engine_api() const
            {
                return graph_of(m_node).evaluation_engine_api();
            }

            [[nodiscard]] PythonTraitsHandle traits() const
            {
                return PythonTraitsHandle{m_node};
            }

            void schedule_node(int64_t node_index, engine_time_t when, bool force_set = false) const
            {
                graph_of(m_node).schedule_node(node_index, when, force_set);
            }

          private:
            Node *m_node{nullptr};
        };

        class NodeSchedulerHandle
        {
          public:
            explicit NodeSchedulerHandle(NodeScheduler *scheduler) noexcept : m_scheduler(scheduler) {}

            [[nodiscard]] engine_time_t next_scheduled_time() const noexcept
            {
                return m_scheduler != nullptr ? m_scheduler->next_scheduled_time() : MIN_DT;
            }

            [[nodiscard]] bool requires_scheduling() const noexcept
            {
                return m_scheduler != nullptr && m_scheduler->requires_scheduling();
            }

            [[nodiscard]] bool is_scheduled() const noexcept
            {
                return m_scheduler != nullptr && m_scheduler->is_scheduled();
            }

            [[nodiscard]] bool is_scheduled_now() const noexcept
            {
                return m_scheduler != nullptr && m_scheduler->is_scheduled_now();
            }

            [[nodiscard]] bool has_tag(const std::string &tag) const
            {
                return m_scheduler != nullptr && m_scheduler->has_tag(tag);
            }

            [[nodiscard]] engine_time_t pop_tag(const std::string &tag) const
            {
                return pop_tag(tag, MIN_DT);
            }

            [[nodiscard]] engine_time_t pop_tag(const std::string &tag, engine_time_t default_time) const
            {
                return m_scheduler != nullptr ? m_scheduler->pop_tag(tag, default_time) : default_time;
            }

            void schedule(engine_time_t when, std::optional<std::string> tag, bool on_wall_clock = false) const
            {
                assert(m_scheduler != nullptr);
                m_scheduler->schedule(when, std::move(tag), on_wall_clock);
            }

            void schedule(engine_time_delta_t when, std::optional<std::string> tag, bool on_wall_clock = false) const
            {
                assert(m_scheduler != nullptr);
                m_scheduler->schedule(when, std::move(tag), on_wall_clock);
            }

            void un_schedule(const std::string &tag) const
            {
                assert(m_scheduler != nullptr);
                m_scheduler->un_schedule(tag);
            }

            void un_schedule() const
            {
                assert(m_scheduler != nullptr);
                m_scheduler->un_schedule();
            }

            void reset() const
            {
                assert(m_scheduler != nullptr);
                m_scheduler->reset();
            }

            void advance() const
            {
                assert(m_scheduler != nullptr);
                m_scheduler->advance();
            }

          private:
            NodeScheduler *m_scheduler{nullptr};
        };

        class PythonTimeSeriesHandle
        {
          public:
            PythonTimeSeriesHandle(Node *node,
                                   TSInput *input,
                                   TSOutput *output,
                                   const TSMeta *schema,
                                   std::vector<std::string> fields = {})
                : m_node(node), m_input(input), m_output(output), m_schema(schema), m_fields(std::move(fields))
            {
            }

            [[nodiscard]] bool truthy() const noexcept
            {
                return m_schema != nullptr;
            }

            [[nodiscard]] nb::object value() const
            {
                return current_value().to_python();
            }

            [[nodiscard]] nb::object delta_value() const
            {
                return current_delta_value().to_python();
            }

            [[nodiscard]] bool modified() const
            {
                return m_input != nullptr ? input_view().modified() : output_view().modified();
            }

            [[nodiscard]] bool valid() const
            {
                return m_input != nullptr ? input_view().valid() : output_view().valid();
            }

            [[nodiscard]] bool all_valid() const
            {
                return m_input != nullptr ? input_view().all_valid() : output_view().all_valid();
            }

            [[nodiscard]] engine_time_t last_modified_time() const
            {
                return m_input != nullptr ? input_view().last_modified_time() : output_view().last_modified_time();
            }

            [[nodiscard]] bool is_bundle() const noexcept
            {
                return m_schema != nullptr && m_schema->kind == TSKind::TSB;
            }

            [[nodiscard]] PythonTimeSeriesHandle field(const std::string &field_name) const
            {
                const TSMeta *field_type = field_schema_or_throw(m_schema, field_name);
                std::vector<std::string> fields = m_fields;
                fields.push_back(field_name);
                return PythonTimeSeriesHandle{m_node, m_input, m_output, field_type, std::move(fields)};
            }

            [[nodiscard]] nb::object get_item(const nb::handle &key) const
            {
                return nb::cast(field(nb::cast<std::string>(key)));
            }

            [[nodiscard]] nb::tuple keys() const
            {
                if (!is_bundle()) { throw std::logic_error("v2 Python time-series keys() requires a bundle schema"); }
                nb::list result;
                for (size_t i = 0; i < m_schema->data.tsb.field_count; ++i) {
                    result.append(nb::str(m_schema->data.tsb.fields[i].name));
                }
                return nb::tuple(result);
            }

            [[nodiscard]] nb::list values() const
            {
                if (!is_bundle()) { throw std::logic_error("v2 Python time-series values() requires a bundle schema"); }
                nb::list result;
                for (size_t i = 0; i < m_schema->data.tsb.field_count; ++i) {
                    result.append(nb::cast(field(m_schema->data.tsb.fields[i].name)));
                }
                return result;
            }

            [[nodiscard]] nb::list items() const
            {
                if (!is_bundle()) { throw std::logic_error("v2 Python time-series items() requires a bundle schema"); }
                nb::list result;
                for (size_t i = 0; i < m_schema->data.tsb.field_count; ++i) {
                    const auto &field_info = m_schema->data.tsb.fields[i];
                    result.append(nb::make_tuple(nb::str(field_info.name), nb::cast(field(field_info.name))));
                }
                return result;
            }

            [[nodiscard]] nb::list valid_items() const
            {
                if (!is_bundle()) { throw std::logic_error("v2 Python time-series valid_items() requires a bundle schema"); }
                nb::list result;
                for (size_t i = 0; i < m_schema->data.tsb.field_count; ++i) {
                    const auto child = field(m_schema->data.tsb.fields[i].name);
                    if (child.valid()) { result.append(nb::make_tuple(nb::str(m_schema->data.tsb.fields[i].name), nb::cast(child))); }
                }
                return result;
            }

            [[nodiscard]] nb::list modified_items() const
            {
                if (!is_bundle()) { throw std::logic_error("v2 Python time-series modified_items() requires a bundle schema"); }
                nb::list result;
                for (size_t i = 0; i < m_schema->data.tsb.field_count; ++i) {
                    const auto child = field(m_schema->data.tsb.fields[i].name);
                    if (child.modified()) { result.append(nb::make_tuple(nb::str(m_schema->data.tsb.fields[i].name), nb::cast(child))); }
                }
                return result;
            }

            void make_active() const
            {
                ensure_input();
                input_view().make_active();
            }

            void make_passive() const
            {
                ensure_input();
                input_view().make_passive();
            }

            [[nodiscard]] bool active() const
            {
                ensure_input();
                return input_view().active();
            }

            void apply_result(nb::handle value) const
            {
                set_value(value);
            }

            void set_value(nb::handle value) const
            {
                ensure_output();
                auto view = output_view();
                view.value().from_python(nb::borrow<nb::object>(value));
                mark_output_modified(view, evaluation_time());
            }

            [[nodiscard]] std::string repr() const
            {
                return fmt::format(
                    "v2._PythonTimeSeriesHandle@{:p}[schema={}]",
                    static_cast<const void *>(this),
                    m_schema != nullptr && m_schema->kind == TSKind::TSB ? "bundle" : "leaf");
            }

          private:
            [[nodiscard]] engine_time_t evaluation_time() const
            {
                return m_node != nullptr ? m_node->evaluation_time() : MIN_DT;
            }

            [[nodiscard]] TSInputView input_view() const
            {
                ensure_input();
                auto view = m_input->view(m_node, evaluation_time());
                for (const auto &field_name : m_fields) { view = view.as_bundle().field(field_name); }
                return view;
            }

            [[nodiscard]] TSOutputView output_view() const
            {
                ensure_output();
                auto view = m_output->view(evaluation_time());
                for (const auto &field_name : m_fields) { view = view.as_bundle().field(field_name); }
                return view;
            }

            [[nodiscard]] View current_value() const
            {
                return m_input != nullptr ? input_view().value() : output_view().value();
            }

            [[nodiscard]] View current_delta_value() const
            {
                return m_input != nullptr ? input_view().delta_value() : output_view().delta_value();
            }

            void ensure_input() const
            {
                if (m_input == nullptr) { throw std::logic_error("v2 Python time-series handle is not an input"); }
            }

            void ensure_output() const
            {
                if (m_output == nullptr) { throw std::logic_error("v2 Python time-series handle is not an output"); }
            }

            Node *m_node{nullptr};
            TSInput *m_input{nullptr};
            TSOutput *m_output{nullptr};
            const TSMeta *m_schema{nullptr};
            std::vector<std::string> m_fields;
        };

        class PythonNodeHandle
        {
          public:
            PythonNodeHandle(Node *node,
                             nb::object signature,
                             nb::object scalars,
                             nb::object graph,
                             nb::object input,
                             nb::object output,
                             nb::object recordable_state,
                             nb::object scheduler)
                : m_node(node),
                  m_signature(std::move(signature)),
                  m_scalars(std::move(scalars)),
                  m_graph(std::move(graph)),
                  m_input(std::move(input)),
                  m_output(std::move(output)),
                  m_recordable_state(std::move(recordable_state)),
                  m_scheduler(std::move(scheduler))
            {
            }

            [[nodiscard]] int64_t node_ndx() const noexcept
            {
                return m_node != nullptr ? m_node->node_index() : -1;
            }

            [[nodiscard]] nb::tuple owning_graph_id() const
            {
                return nb::tuple();
            }

            [[nodiscard]] nb::tuple node_id() const
            {
                return nb::make_tuple(node_ndx());
            }

            [[nodiscard]] nb::object signature() const
            {
                return nb::borrow(m_signature);
            }

            [[nodiscard]] nb::object scalars() const
            {
                return nb::borrow(m_scalars);
            }

            [[nodiscard]] nb::object graph() const
            {
                return nb::borrow(m_graph);
            }

            [[nodiscard]] nb::object input() const
            {
                return m_input.is_valid() ? nb::borrow(m_input) : nb::none();
            }

            [[nodiscard]] nb::object output() const
            {
                return m_output.is_valid() ? nb::borrow(m_output) : nb::none();
            }

            [[nodiscard]] nb::object recordable_state() const
            {
                return m_recordable_state.is_valid() ? nb::borrow(m_recordable_state) : nb::none();
            }

            [[nodiscard]] nb::object scheduler() const
            {
                return nb::borrow(m_scheduler);
            }

            [[nodiscard]] bool has_scheduler() const noexcept
            {
                return m_scheduler.is_valid() && !m_scheduler.is_none();
            }

            [[nodiscard]] bool has_input() const noexcept
            {
                return m_input.is_valid() && !m_input.is_none();
            }

            [[nodiscard]] bool has_output() const noexcept
            {
                return m_output.is_valid() && !m_output.is_none();
            }

            void notify(nb::object modified_time = nb::none()) const
            {
                assert(m_node != nullptr);
                if (m_node->started()) {
                    engine_time_t when =
                        modified_time.is_none() ? graph_of(m_node).evaluation_time() : nb::cast<engine_time_t>(modified_time);
                    if (when < graph_of(m_node).evaluation_time()) { when = graph_of(m_node).evaluation_time(); }
                    m_node->notify(when);
                } else {
                    m_node->notify(graph_of(m_node).evaluation_time());
                }
            }

            void notify_next_cycle() const
            {
                assert(m_node != nullptr);
                if (m_node->started()) {
                    graph_of(m_node).schedule_node(m_node->node_index(), graph_of(m_node).evaluation_clock().next_cycle_evaluation_time());
                } else {
                    notify();
                }
            }

            [[nodiscard]] std::string repr() const
            {
                const nb::object signature_obj = signature();
                const std::string name = nb::cast<std::string>(signature_obj.attr("name"));
                const nb::object label = signature_obj.attr("label");
                const std::string wiring_path_name = nb::cast<std::string>(signature_obj.attr("wiring_path_name"));
                if (!label.is_none()) { return fmt::format("{}.{}", wiring_path_name, nb::cast<std::string>(label)); }
                return fmt::format("{}.{}", wiring_path_name, name);
            }

          private:
            Node *m_node{nullptr};
            nb::object m_signature;
            nb::object m_scalars;
            nb::object m_graph;
            nb::object m_input;
            nb::object m_output;
            nb::object m_recordable_state;
            nb::object m_scheduler;
        };
    }  // namespace

    nb::object make_python_node_handle(nb::handle signature,
                                       nb::handle scalars,
                                       Node *node,
                                       TSInput *input,
                                       TSOutput *output,
                                       TSOutput *recordable_state,
                                       const TSMeta *input_schema,
                                       const TSMeta *output_schema,
                                       const TSMeta *recordable_state_schema,
                                       NodeScheduler *scheduler)
    {
        nb::gil_scoped_acquire guard;
        nb::object graph = nb::cast(PythonGraphHandle{node});
        nb::object input_handle =
            input != nullptr ? nb::cast(PythonTimeSeriesHandle{node, input, nullptr, input_schema}) : nb::none();
        nb::object output_handle =
            output != nullptr ? nb::cast(PythonTimeSeriesHandle{node, nullptr, output, output_schema}) : nb::none();
        nb::object recordable_state_handle = recordable_state != nullptr
                                                 ? nb::cast(PythonTimeSeriesHandle{
                                                       node, nullptr, recordable_state, recordable_state_schema})
                                                 : nb::none();
        nb::object scheduler_handle = scheduler != nullptr ? nb::cast(NodeSchedulerHandle{scheduler}) : nb::none();
        return nb::cast(PythonNodeHandle{node,
                                         nb::borrow(signature),
                                         nb::borrow(scalars),
                                         std::move(graph),
                                         std::move(input_handle),
                                         std::move(output_handle),
                                         std::move(recordable_state_handle),
                                         std::move(scheduler_handle)});
    }

    nb::dict make_python_node_kwargs(nb::handle signature, nb::handle scalars, nb::handle node_handle)
    {
        nb::gil_scoped_acquire guard;
        nb::dict values;

        nb::object input = nb::borrow(node_handle).attr("input");
        nb::object time_series_inputs = nb::borrow(signature).attr("time_series_inputs");
        if (!input.is_none() && !time_series_inputs.is_none()) {
            for (auto key : time_series_inputs.attr("keys")()) {
                nb::object key_obj = nb::borrow<nb::object>(key);
                values[key_obj] = input[key_obj];
            }
        }

        nb::object injector_type = nb::module_::import_("hgraph._types._scalar_type_meta_data").attr("Injector");
        if (!scalars.is_none()) {
            for (auto item : nb::borrow<nb::dict>(scalars).items()) {
                nb::tuple pair = nb::borrow<nb::tuple>(item);
                nb::object key = nb::borrow<nb::object>(pair[0]);
                nb::object value = nb::borrow<nb::object>(pair[1]);
                values[key] = value;
                if (nb::isinstance(value, injector_type)) { values[key] = value(nb::borrow(node_handle)); }
            }
        }

        nb::dict kwargs;
        for (auto arg : nb::borrow(signature).attr("args")) {
            nb::object key = nb::borrow<nb::object>(arg);
            if (PyMapping_HasKey(values.ptr(), key.ptr())) {
                kwargs[key] = nb::steal<nb::object>(PyObject_GetItem(values.ptr(), key.ptr()));
            }
        }
        return kwargs;
    }

    nb::tuple python_callable_parameter_names(nb::handle callable)
    {
        nb::gil_scoped_acquire guard;
        if (!callable.is_valid() || callable.is_none()) { return nb::make_tuple(); }
        nb::object inspect = nb::module_::import_("inspect");
        return nb::tuple(inspect.attr("signature")(nb::borrow(callable)).attr("parameters").attr("keys")());
    }

    nb::object call_python_callable(nb::handle callable, nb::handle kwargs)
    {
        nb::gil_scoped_acquire guard;
        if (!callable.is_valid() || callable.is_none()) { return nb::none(); }
        nb::tuple args = nb::make_tuple();
        PyObject *result = PyObject_Call(callable.ptr(), args.ptr(), kwargs.ptr());
        if (result == nullptr) { throw nb::python_error(); }
        return nb::steal<nb::object>(result);
    }

    nb::object call_python_callable_with_subset(nb::handle callable, nb::handle kwargs, nb::handle parameter_names)
    {
        nb::gil_scoped_acquire guard;
        if (!callable.is_valid() || callable.is_none()) { return nb::none(); }

        nb::dict filtered;
        for (auto key : nb::borrow(parameter_names)) {
            nb::object key_obj = nb::borrow<nb::object>(key);
            if (PyMapping_HasKey(kwargs.ptr(), key_obj.ptr())) {
                filtered[key_obj] = nb::steal<nb::object>(PyObject_GetItem(kwargs.ptr(), key_obj.ptr()));
            }
        }
        return call_python_callable(callable, filtered);
    }

    void register_python_runtime_bindings(nb::module_ &m)
    {
        nb::class_<EvaluationClock>(m, "EvaluationClock")
            .def_prop_ro("evaluation_time", &EvaluationClock::evaluation_time)
            .def_prop_ro("now", &EvaluationClock::now)
            .def_prop_ro("next_cycle_evaluation_time", &EvaluationClock::next_cycle_evaluation_time)
            .def_prop_ro("cycle_time", &EvaluationClock::cycle_time);

        nb::class_<EvaluationEngineApi>(m, "EvaluationEngineApi")
            .def_prop_ro("evaluation_mode", &EvaluationEngineApi::evaluation_mode)
            .def_prop_ro("start_time", &EvaluationEngineApi::start_time)
            .def_prop_ro("end_time", &EvaluationEngineApi::end_time)
            .def_prop_ro("evaluation_clock", &EvaluationEngineApi::evaluation_clock)
            .def("request_engine_stop", &EvaluationEngineApi::request_engine_stop)
            .def_prop_ro("is_stop_requested", &EvaluationEngineApi::is_stop_requested)
            .def("add_before_evaluation_notification",
                 [](const EvaluationEngineApi &self, nb::callable fn) {
                     self.add_before_evaluation_notification([fn = std::move(fn)]() {
                         nb::gil_scoped_acquire guard;
                         fn();
                     });
                 },
                 "fn"_a)
            .def("add_after_evaluation_notification",
                 [](const EvaluationEngineApi &self, nb::callable fn) {
                     self.add_after_evaluation_notification([fn = std::move(fn)]() {
                         nb::gil_scoped_acquire guard;
                         fn();
                     });
                 },
                 "fn"_a);

        nb::class_<PythonTraitsHandle>(m, "_PythonTraitsHandle")
            .def("set_traits", &PythonTraitsHandle::set_traits)
            .def("set_trait", &PythonTraitsHandle::set_trait, "trait_name"_a, "value"_a)
            .def("get_trait", &PythonTraitsHandle::get_trait, "trait_name"_a)
            .def("get_trait_or", &PythonTraitsHandle::get_trait_or, "trait_name"_a, "default"_a = nb::none());

        nb::class_<PythonGraphHandle>(m, "_PythonGraphHandle")
            .def_prop_ro("graph_id", &PythonGraphHandle::graph_id)
            .def_prop_ro("parent_node", &PythonGraphHandle::parent_node)
            .def_prop_ro("label", &PythonGraphHandle::label)
            .def_prop_ro("evaluation_clock", &PythonGraphHandle::evaluation_clock)
            .def_prop_ro("evaluation_engine_api", &PythonGraphHandle::evaluation_engine_api)
            .def_prop_ro("traits", &PythonGraphHandle::traits)
            .def("schedule_node", &PythonGraphHandle::schedule_node, "node_ndx"_a, "when"_a, "force_set"_a = false);

        nb::class_<NodeSchedulerHandle>(m, "_NodeSchedulerHandle")
            .def_prop_ro("next_scheduled_time", &NodeSchedulerHandle::next_scheduled_time)
            .def_prop_ro("requires_scheduling", &NodeSchedulerHandle::requires_scheduling)
            .def_prop_ro("is_scheduled", &NodeSchedulerHandle::is_scheduled)
            .def_prop_ro("is_scheduled_now", &NodeSchedulerHandle::is_scheduled_now)
            .def("has_tag", &NodeSchedulerHandle::has_tag, "tag"_a)
            .def("pop_tag", nb::overload_cast<const std::string &>(&NodeSchedulerHandle::pop_tag, nb::const_), "tag"_a)
            .def("pop_tag",
                 nb::overload_cast<const std::string &, engine_time_t>(&NodeSchedulerHandle::pop_tag, nb::const_),
                 "tag"_a,
                 "default"_a)
            .def("schedule",
                 nb::overload_cast<engine_time_t, std::optional<std::string>, bool>(&NodeSchedulerHandle::schedule, nb::const_),
                 "when"_a,
                 "tag"_a = nb::none(),
                 "on_wall_clock"_a = false)
            .def("schedule",
                 nb::overload_cast<engine_time_delta_t, std::optional<std::string>, bool>(&NodeSchedulerHandle::schedule, nb::const_),
                 "when"_a,
                 "tag"_a = nb::none(),
                 "on_wall_clock"_a = false)
            .def("un_schedule",
                 nb::overload_cast<const std::string &>(&NodeSchedulerHandle::un_schedule, nb::const_),
                 "tag"_a)
            .def("un_schedule", nb::overload_cast<>(&NodeSchedulerHandle::un_schedule, nb::const_))
            .def("reset", &NodeSchedulerHandle::reset)
            .def("advance", &NodeSchedulerHandle::advance);

        nb::class_<PythonTimeSeriesHandle>(m, "_PythonTimeSeriesHandle")
            .def("__bool__", &PythonTimeSeriesHandle::truthy)
            .def("__getitem__", &PythonTimeSeriesHandle::get_item, "key"_a)
            .def("__getattr__", &PythonTimeSeriesHandle::get_item, "key"_a)
            .def("__iter__", [](const PythonTimeSeriesHandle &self) { return nb::iter(self.keys()); })
            .def_prop_rw("value", &PythonTimeSeriesHandle::value, &PythonTimeSeriesHandle::set_value)
            .def_prop_ro("delta_value", &PythonTimeSeriesHandle::delta_value)
            .def_prop_ro("modified", &PythonTimeSeriesHandle::modified)
            .def_prop_ro("valid", &PythonTimeSeriesHandle::valid)
            .def_prop_ro("all_valid", &PythonTimeSeriesHandle::all_valid)
            .def_prop_ro("last_modified_time", &PythonTimeSeriesHandle::last_modified_time)
            .def("keys", &PythonTimeSeriesHandle::keys)
            .def("values", &PythonTimeSeriesHandle::values)
            .def("items", &PythonTimeSeriesHandle::items)
            .def("valid_items", &PythonTimeSeriesHandle::valid_items)
            .def("modified_items", &PythonTimeSeriesHandle::modified_items)
            .def("make_active", &PythonTimeSeriesHandle::make_active)
            .def("make_passive", &PythonTimeSeriesHandle::make_passive)
            .def_prop_ro("active", &PythonTimeSeriesHandle::active)
            .def("apply_result", &PythonTimeSeriesHandle::apply_result, "value"_a)
            .def("__repr__", &PythonTimeSeriesHandle::repr);

        nb::class_<PythonNodeHandle>(m, "_PythonNodeHandle")
            .def_prop_ro("node_ndx", &PythonNodeHandle::node_ndx)
            .def_prop_ro("owning_graph_id", &PythonNodeHandle::owning_graph_id)
            .def_prop_ro("node_id", &PythonNodeHandle::node_id)
            .def_prop_ro("signature", &PythonNodeHandle::signature)
            .def_prop_ro("scalars", &PythonNodeHandle::scalars)
            .def_prop_ro("graph", &PythonNodeHandle::graph)
            .def_prop_ro("input", &PythonNodeHandle::input)
            .def_prop_ro("output", &PythonNodeHandle::output)
            .def_prop_ro("recordable_state", &PythonNodeHandle::recordable_state)
            .def_prop_ro("scheduler", &PythonNodeHandle::scheduler)
            .def_prop_ro("has_scheduler", &PythonNodeHandle::has_scheduler)
            .def_prop_ro("has_input", &PythonNodeHandle::has_input)
            .def_prop_ro("has_output", &PythonNodeHandle::has_output)
            .def("notify", &PythonNodeHandle::notify, "modified_time"_a = nb::none())
            .def("notify_next_cycle", &PythonNodeHandle::notify_next_cycle)
            .def("__repr__", &PythonNodeHandle::repr)
            .def("__str__", &PythonNodeHandle::repr);
    }
}  // namespace hgraph::v2
