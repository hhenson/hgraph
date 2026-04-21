#include <hgraph/types/python_node_support.h>

#include <hgraph/types/constants.h>
#include <hgraph/types/time_series/time_series_state.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_output_builder.h>
#include <hgraph/types/time_series/ts_type_registry.h>
#include <hgraph/types/time_series/ts_value_builder.h>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/traits.h>
#include <hgraph/types/evaluation_clock.h>
#include <hgraph/types/evaluation_engine.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/value/type_meta.h>

#include <fmt/format.h>

#include <algorithm>
#include <cassert>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace hgraph
{
    namespace
    {
        struct RawViewAccess : View
        {
            using View::data_of;
        };

        struct RawMapAccess : MapView
        {
            explicit RawMapAccess(const View &view) : MapView(view) {}

            using MapView::map_dispatch;
        };

        [[nodiscard]] const TSMeta *field_schema(const TSMeta *schema, std::string_view field_name) noexcept {
            if (schema == nullptr || schema->kind != TSKind::TSB) { return nullptr; }

            for (size_t i = 0; i < schema->data.tsb.field_count; ++i) {
                const auto &field = schema->data.tsb.fields[i];
                if (field_name == field.name) { return field.ts_type; }
            }

            return nullptr;
        }

        [[nodiscard]] Graph &graph_of(Node *node) noexcept {
            assert(node != nullptr);
            assert(node->graph() != nullptr);
            return *node->graph();
        }

        [[nodiscard]] Node *owner_node_from_context(const LinkedTSContext &context) noexcept {
            const auto owner_from_output = [&](TSOutput *output) noexcept -> Node * {
                if (output == nullptr) { return nullptr; }
                Node *owner = nullptr;
                std::visit(
                    [&](auto &state) {
                        hgraph::visit(
                            state.parent,
                            [&](Node *parent) noexcept { owner = parent; },
                            [](auto *) noexcept {},
                            []() noexcept {});
                    },
                    output->root_state_variant());
                return owner;
            };

            BaseState *cursor = context.notification_state != nullptr ? context.notification_state : context.ts_state;
            while (cursor != nullptr) {
                Node *owner = nullptr;
                bool  advanced = false;
                hgraph::visit(
                    cursor->parent,
                    [&](TSLState *parent) {
                        cursor = parent;
                        advanced = true;
                    },
                    [&](TSDState *parent) {
                        cursor = parent;
                        advanced = true;
                    },
                    [&](TSBState *parent) {
                        cursor = parent;
                        advanced = true;
                    },
                    [&](SignalState *parent) {
                        cursor = parent;
                        advanced = true;
                    },
                    [&](Node *parent) {
                        owner = parent;
                        cursor = nullptr;
                        advanced = true;
                    },
                    [&](TSInput *parent) {
                        static_cast<void>(parent);
                        cursor = nullptr;
                        advanced = true;
                    },
                    [&](TSOutput *parent) {
                        owner = owner_from_output(parent);
                        cursor = nullptr;
                        advanced = true;
                    },
                    [] {});
                if (owner != nullptr) { return owner; }
                if (!advanced) { break; }
            }
            return owner_from_output(context.owning_output);
        }

        [[nodiscard]] const TSMeta *field_schema_or_throw(const TSMeta *schema, std::string_view field_name) {
            if (schema == nullptr || schema->kind != TSKind::TSB) {
                throw std::logic_error("v2 Python time-series bundle access requires a TSB schema");
            }

            if (const TSMeta *field = field_schema(schema, field_name); field != nullptr) { return field; }
            throw std::out_of_range("v2 Python time-series bundle field is out of range");
        }

        [[nodiscard]] const TSMeta *index_schema_or_throw(const TSMeta *schema, size_t) {
            if (schema == nullptr || schema->kind != TSKind::TSL) {
                throw std::logic_error("v2 Python time-series list access requires a TSL schema");
            }

            if (const TSMeta *element = schema->element_ts(); element != nullptr) { return element; }
            throw std::logic_error("v2 Python time-series list schema is missing its element type");
        }

        [[nodiscard]] const TSMeta *value_schema_or_throw(const TSMeta *schema) {
            if (schema == nullptr || schema->kind != TSKind::TSD) {
                throw std::logic_error("v2 Python time-series dict access requires a TSD schema");
            }

            if (const TSMeta *element = schema->element_ts(); element != nullptr) { return element; }
            throw std::logic_error("v2 Python time-series dict schema is missing its value type");
        }

        [[nodiscard]] TimeSeriesStateParentPtr parent_ptr(TSBState &state) noexcept { return &state; }
        [[nodiscard]] TimeSeriesStateParentPtr parent_ptr(TSLState &state) noexcept { return &state; }

        [[nodiscard]] TSOutputView output_view_from_context(const LinkedTSContext &context, engine_time_t evaluation_time) {
            TSViewContext view_context{TSContext{
                context.schema,
                context.value_dispatch,
                context.ts_dispatch,
                context.value_data,
                context.ts_state,
                context.owning_output,
                context.output_view_ops,
                context.notification_state,
                context.pending_dict_child,
            }};
            return TSOutputView{
                view_context,
                TSViewContext::none(),
                evaluation_time,
                context.owning_output,
                context.output_view_ops != nullptr ? context.output_view_ops : &::hgraph::detail::default_output_view_ops(),
            };
        }

        [[nodiscard]] const BaseState *parent_collection_state(const BaseState *state) noexcept {
            if (state == nullptr) { return nullptr; }

            const BaseState *parent_state = nullptr;
            hgraph::visit(
                state->parent,
                [&](const auto *parent) {
                    using T = std::remove_pointer_t<decltype(parent)>;
                    if constexpr (std::same_as<T, TSLState> || std::same_as<T, TSDState> || std::same_as<T, TSBState>) {
                        parent_state = parent;
                    }
                },
                [] {});
            return parent_state;
        }

        [[nodiscard]] engine_time_t effective_modified_time(const LinkedTSContext &context) noexcept {
            for (const BaseState *state = context.ts_state; state != nullptr; state = parent_collection_state(state)) {
                if (state->last_modified_time != MIN_DT) { return state->last_modified_time; }
            }
            return MIN_DT;
        }

        [[nodiscard]] bool sampled_this_tick(const TSViewContext &context, engine_time_t evaluation_time) noexcept
        {
            const auto snapshot = detail::transition_snapshot(context);
            return snapshot.active() && snapshot.modified_time == evaluation_time;
        }

        template <typename TView>
        [[nodiscard]] const Value *transition_previous_value(const TView &view) noexcept
        {
            const auto snapshot = detail::transition_snapshot(view.context_ref());
            return snapshot.active() && snapshot.modified_time == view.evaluation_time() ? snapshot.previous_value : nullptr;
        }

        template <typename TView>
        [[nodiscard]] const Value *transition_previous_map_value(const TView &view) noexcept
        {
            const Value *previous = transition_previous_value(view);
            if (previous == nullptr || !previous->has_value() || previous->view().schema() == nullptr ||
                previous->view().schema()->kind != value::TypeKind::Map) {
                return nullptr;
            }
            return previous;
        }

        [[nodiscard]] Value ts_nested_value_from_python(const value::TypeMeta &schema, const nb::handle &value) {
            Value nested_value(schema, MutationTracking::Plain);
            nested_value.reset();
            nested_value.from_python(nb::borrow<nb::object>(value));
            return nested_value;
        }

        class PythonTraitsHandle
        {
          public:
            explicit PythonTraitsHandle(Node *node) noexcept : m_node(node) {}

            void set_traits(nb::kwargs values) const { graph_of(m_node).traits().set_traits(std::move(values)); }

            void set_trait(const std::string &name, nb::object value) const {
                graph_of(m_node).traits().set_trait(name, std::move(value));
            }

            [[nodiscard]] nb::object get_trait(const std::string &name) const { return graph_of(m_node).traits().get_trait(name); }

            [[nodiscard]] nb::object get_trait_or(const std::string &name, nb::object default_value = nb::none()) const {
                return graph_of(m_node).traits().get_trait_or(name, std::move(default_value));
            }

          private:
            Node *m_node{nullptr};
        };

        class PythonGraphHandle
        {
          public:
            explicit PythonGraphHandle(Node *node) noexcept : m_node(node) {}

            [[nodiscard]] nb::tuple graph_id() const {
                if (m_node == nullptr) { return nb::tuple(); }
                const auto &graph_id = graph_of(m_node).graph_id();
                nb::tuple   out      = nb::steal<nb::tuple>(PyTuple_New(static_cast<Py_ssize_t>(graph_id.size())));
                for (size_t i = 0; i < graph_id.size(); ++i) {
                    PyTuple_SET_ITEM(out.ptr(), static_cast<Py_ssize_t>(i), nb::cast(graph_id[i]).release().ptr());
                }
                return out;
            }

            [[nodiscard]] nb::object parent_node() const { return nb::none(); }

            [[nodiscard]] std::string label() const {
                return m_node != nullptr ? std::string{graph_of(m_node).label()} : std::string{};
            }

            [[nodiscard]] EvaluationClock evaluation_clock() const { return graph_of(m_node).evaluation_clock(); }

            [[nodiscard]] EvaluationEngineApi evaluation_engine_api() const { return graph_of(m_node).evaluation_engine_api(); }

            [[nodiscard]] PythonTraitsHandle traits() const { return PythonTraitsHandle{m_node}; }

            void schedule_node(int64_t node_index, engine_time_t when, bool force_set = false) const {
                graph_of(m_node).schedule_node(node_index, when, force_set);
            }

          private:
            Node *m_node{nullptr};
        };

        class NodeSchedulerHandle
        {
          public:
            explicit NodeSchedulerHandle(NodeScheduler *scheduler) noexcept : m_scheduler(scheduler) {}

            [[nodiscard]] engine_time_t next_scheduled_time() const noexcept {
                return m_scheduler != nullptr ? m_scheduler->next_scheduled_time() : MIN_DT;
            }

            [[nodiscard]] bool requires_scheduling() const noexcept {
                return m_scheduler != nullptr && m_scheduler->requires_scheduling();
            }

            [[nodiscard]] bool is_scheduled() const noexcept { return m_scheduler != nullptr && m_scheduler->is_scheduled(); }

            [[nodiscard]] bool is_scheduled_now() const noexcept {
                return m_scheduler != nullptr && m_scheduler->is_scheduled_now();
            }

            [[nodiscard]] bool has_tag(const std::string &tag) const { return m_scheduler != nullptr && m_scheduler->has_tag(tag); }

            [[nodiscard]] engine_time_t pop_tag(const std::string &tag) const { return pop_tag(tag, MIN_DT); }

            [[nodiscard]] engine_time_t pop_tag(const std::string &tag, engine_time_t default_time) const {
                return m_scheduler != nullptr ? m_scheduler->pop_tag(tag, default_time) : default_time;
            }

            void schedule(engine_time_t when, std::optional<std::string> tag, bool on_wall_clock = false) const {
                assert(m_scheduler != nullptr);
                m_scheduler->schedule(when, std::move(tag), on_wall_clock);
            }

            void schedule(engine_time_delta_t when, std::optional<std::string> tag, bool on_wall_clock = false) const {
                assert(m_scheduler != nullptr);
                m_scheduler->schedule(when, std::move(tag), on_wall_clock);
            }

            void un_schedule(const std::string &tag) const {
                assert(m_scheduler != nullptr);
                m_scheduler->un_schedule(tag);
            }

            void un_schedule() const {
                assert(m_scheduler != nullptr);
                m_scheduler->un_schedule();
            }

            void reset() const {
                assert(m_scheduler != nullptr);
                m_scheduler->reset();
            }

            void advance() const {
                assert(m_scheduler != nullptr);
                m_scheduler->advance();
            }

          private:
            NodeScheduler *m_scheduler{nullptr};
        };

        struct PythonNodeHeapStateView
        {
            nb::object python_signature;
            nb::object python_scalars;
            nb::object eval_fn;
            nb::object start_fn;
            nb::object stop_fn;
            nb::object node_handle;
        };

        struct PythonNodeRuntimeDataView
        {
            TSInput                 *input{nullptr};
            TSOutput                *output{nullptr};
            TSOutput                *recordable_state{nullptr};
            PythonNodeHeapStateView *heap_state{nullptr};
        };

        [[nodiscard]] nb::object python_node_handle_for(Node *node) {
            if (node == nullptr) { return nb::none(); }
            auto *runtime_data = static_cast<const PythonNodeRuntimeDataView *>(node->data());
            if (runtime_data == nullptr || runtime_data->heap_state == nullptr ||
                !runtime_data->heap_state->node_handle.is_valid()) {
                return nb::none();
            }
            return nb::borrow(runtime_data->heap_state->node_handle);
        }

        class PythonTimeSeriesHandle
        {
          public:
            struct PathStep
            {
                enum class Kind : uint8_t { Field, Index, Key };

                Kind        kind{Kind::Field};
                std::string field_name;
                size_t      index{0};
                Value       key;

                [[nodiscard]] static PathStep field(std::string name) {
                    PathStep step;
                    step.kind       = Kind::Field;
                    step.field_name = std::move(name);
                    return step;
                }

                [[nodiscard]] static PathStep index_at(size_t value) {
                    PathStep step;
                    step.kind  = Kind::Index;
                    step.index = value;
                    return step;
                }

                [[nodiscard]] static PathStep key_for(const View &value) {
                    PathStep step;
                    step.kind = Kind::Key;
                    step.key  = value.clone();
                    return step;
                }

                [[nodiscard]] static PathStep key_from_python(const TSMeta *schema, const nb::handle &value) {
                    const auto *key_schema = schema != nullptr ? schema->key_type() : nullptr;
                    if (key_schema == nullptr) {
                        throw std::logic_error("v2 Python time-series dict access requires a key schema");
                    }

                    PathStep step;
                    step.kind = Kind::Key;
                    step.key  = ts_nested_value_from_python(*key_schema, value);
                    return step;
                }
            };

            PythonTimeSeriesHandle(Node *node, TSInput *input, TSOutput *output, const TSMeta *schema,
                                   std::vector<PathStep>          path_steps            = {},
                                   std::optional<LinkedTSContext> bound_output          = std::nullopt,
                                   engine_time_t                  fixed_evaluation_time = MIN_DT)
                : m_node(node), m_input(input), m_output(output), m_schema(schema), m_path_steps(std::move(path_steps)),
                  m_bound_output(std::move(bound_output)), m_fixed_evaluation_time(fixed_evaluation_time) {}

            explicit PythonTimeSeriesHandle(LinkedTSContext bound_output, engine_time_t fixed_evaluation_time = MIN_DT)
                : m_schema(bound_output.schema), m_bound_output(std::move(bound_output)),
                  m_fixed_evaluation_time(fixed_evaluation_time) {}

            [[nodiscard]] bool truthy() const noexcept { return m_schema != nullptr; }

            [[nodiscard]] nb::object value() const {
                if (m_schema != nullptr && m_schema->kind == TSKind::TSValue && !valid()) { return nb::none(); }
                return m_input != nullptr ? input_view().to_python() : output_view().to_python();
            }

            [[nodiscard]] nb::object delta_value() const {
                if (m_schema != nullptr && m_schema->kind == TSKind::TSValue && !valid()) { return nb::none(); }
                if (m_input != nullptr) {
                    TSInputView view = input_view();
                    nb::object delta = view.delta_to_python();
                    if (delta.is_none() && m_schema != nullptr && m_schema->kind == TSKind::TSD && view.modified() && view.valid()) {
                        return view.to_python();
                    }
                    return delta;
                }
                TSOutputView view = output_view();
                nb::object delta = view.delta_to_python();
                if (delta.is_none() && m_schema != nullptr && m_schema->kind == TSKind::TSD && view.modified() && view.valid()) {
                    return view.to_python();
                }
                return delta;
            }

            [[nodiscard]] bool modified() const {
                if (has_live_bound_output()) {
                    TSOutputView view = output_view();
                    return view.modified() || sampled_this_tick(view.context_ref(), evaluation_time());
                }
                if (has_detached_bound_value()) {
                    const engine_time_t when = effective_modified_time(*m_bound_output);
                    return when != MIN_DT ? when == m_fixed_evaluation_time : current_value().has_value();
                }

                const bool modified = [this]() {
                    if (m_input != nullptr) {
                        TSInputView view = input_view();
                        return view.modified() || sampled_this_tick(view.context_ref(), evaluation_time());
                    }

                    TSOutputView view = output_view();
                    return view.modified() || sampled_this_tick(view.context_ref(), evaluation_time());
                }();
                return modified;
            }

            [[nodiscard]] bool valid() const {
                if (has_live_bound_output()) { return output_view().valid(); }
                if (has_detached_bound_value()) { return current_value().has_value(); }
                return m_input != nullptr ? input_view().valid() : output_view().valid();
            }

            [[nodiscard]] bool all_valid() const {
                if (has_live_bound_output()) { return output_view().all_valid(); }
                if (has_detached_bound_value()) { return current_value().has_value(); }
                return m_input != nullptr ? input_view().all_valid() : output_view().all_valid();
            }

            [[nodiscard]] engine_time_t last_modified_time() const {
                if (has_live_bound_output()) { return output_view().last_modified_time(); }
                if (has_detached_bound_value()) { return effective_modified_time(*m_bound_output); }
                return m_input != nullptr ? input_view().last_modified_time() : output_view().last_modified_time();
            }

            [[nodiscard]] nb::object owning_graph() const {
                if (m_node == nullptr && m_bound_output.has_value()) {
                    if (Node *owner = owner_node_from_context(*m_bound_output); owner != nullptr) {
                        return nb::cast(PythonGraphHandle{owner});
                    }
                }
                nb::object node_handle = python_node_handle_for(m_node);
                if (node_handle.is_valid() && !node_handle.is_none()) { return node_handle.attr("graph"); }
                return m_node != nullptr ? nb::cast(PythonGraphHandle{m_node}) : nb::none();
            }

            [[nodiscard]] nb::object owning_node() const {
                nb::object handle = python_node_handle_for(m_node);
                if (handle.is_valid() && !handle.is_none()) { return handle; }
                if (m_bound_output.has_value()) { return python_node_handle_for(owner_node_from_context(*m_bound_output)); }
                return nb::none();
            }

            [[nodiscard]] bool has_owning_node() const noexcept {
                return m_node != nullptr || (m_bound_output.has_value() && owner_node_from_context(*m_bound_output) != nullptr);
            }

            [[nodiscard]] bool is_bundle() const noexcept { return m_schema != nullptr && m_schema->kind == TSKind::TSB; }

            [[nodiscard]] bool is_list() const noexcept { return m_schema != nullptr && m_schema->kind == TSKind::TSL; }

            [[nodiscard]] bool is_dict() const noexcept { return m_schema != nullptr && m_schema->kind == TSKind::TSD; }

            [[nodiscard]] bool is_set() const noexcept { return m_schema != nullptr && m_schema->kind == TSKind::TSS; }

            [[nodiscard]] bool is_window() const noexcept { return m_schema != nullptr && m_schema->kind == TSKind::TSW; }

            [[nodiscard]] bool is_reference() const noexcept { return m_schema != nullptr && m_schema->kind == TSKind::REF; }

            [[nodiscard]] PythonTimeSeriesHandle field(const std::string &field_name) const {
                const TSMeta         *field_type = field_schema_or_throw(m_schema, field_name);
                std::vector<PathStep> path_steps = m_path_steps;
                path_steps.push_back(PathStep::field(field_name));
                return PythonTimeSeriesHandle{
                    m_node, m_input, m_output, field_type, std::move(path_steps), m_bound_output, m_fixed_evaluation_time};
            }

            [[nodiscard]] PythonTimeSeriesHandle index(size_t item_index) const {
                const TSMeta         *element_type = index_schema_or_throw(m_schema, item_index);
                std::vector<PathStep> path_steps   = m_path_steps;
                path_steps.push_back(PathStep::index_at(item_index));
                return PythonTimeSeriesHandle{
                    m_node, m_input, m_output, element_type, std::move(path_steps), m_bound_output, m_fixed_evaluation_time};
            }

            [[nodiscard]] PythonTimeSeriesHandle key_item(const nb::handle &key) const {
                const TSMeta         *value_type = value_schema_or_throw(m_schema);
                std::vector<PathStep> path_steps = m_path_steps;
                path_steps.push_back(PathStep::key_from_python(m_schema, key));
                return PythonTimeSeriesHandle{
                    m_node, m_input, m_output, value_type, std::move(path_steps), m_bound_output, m_fixed_evaluation_time};
            }

            [[nodiscard]] PythonTimeSeriesHandle key_item(const View &key) const {
                const TSMeta         *value_type = value_schema_or_throw(m_schema);
                std::vector<PathStep> path_steps = m_path_steps;
                path_steps.push_back(PathStep::key_for(key));
                return PythonTimeSeriesHandle{
                    m_node, m_input, m_output, value_type, std::move(path_steps), m_bound_output, m_fixed_evaluation_time};
            }

            [[nodiscard]] nb::object get_item(const nb::handle &key) const {
                if (is_bundle()) { return nb::cast(field(nb::cast<std::string>(key))); }
                if (is_list()) { return nb::cast(index(nb::cast<size_t>(key))); }
                if (is_dict()) { return nb::cast(key_item(key)); }
                throw std::logic_error("v2 Python time-series __getitem__ requires a bundle, list, or dict schema");
            }

            [[nodiscard]] nb::object get(const nb::handle &key, nb::object default_value = nb::none()) const {
                if (!is_dict()) { throw std::logic_error("v2 Python time-series get() requires a TSD schema"); }
                const auto child = key_item(key);
                return child.valid() ? nb::cast(child) : std::move(default_value);
            }

            [[nodiscard]] PythonTimeSeriesHandle key_set() const {
                if (!is_dict()) { throw std::logic_error("v2 Python time-series key_set requires a TSD schema"); }

                if (m_output != nullptr || m_bound_output.has_value()) {
                    return PythonTimeSeriesHandle{output_view().as_dict().key_set().linked_context(), evaluation_time()};
                }

                if (m_input != nullptr) {
                    TSInputView view = input_view();
                    if (const LinkedTSContext *target = view.linked_target(); target != nullptr && target->is_bound()) {
                        return PythonTimeSeriesHandle{
                            output_view_from_context(*target, evaluation_time()).as_dict().key_set().linked_context(),
                            evaluation_time()};
                    }
                }

                throw std::logic_error("v2 Python time-series key_set requires a bound dict handle");
            }

            [[nodiscard]] nb::object get_attr(const std::string &name) const {
                if (is_bundle() && field_schema(m_schema, name) != nullptr) { return nb::cast(field(name)); }

                throw nb::attribute_error(fmt::format("'_PythonTimeSeriesHandle' object has no attribute '{}'", name).c_str());
            }

            [[nodiscard]] nb::object parent_input() const {
                if (m_input == nullptr || m_path_steps.empty()) { return nb::none(); }
                return nb::cast(handle_with_prefix(m_path_steps.size() - 1));
            }

            [[nodiscard]] bool has_parent_input() const noexcept { return m_input != nullptr && !m_path_steps.empty(); }

            [[nodiscard]] nb::object parent_output() const {
                if (!has_output_parent()) { return nb::none(); }
                return nb::cast(handle_with_prefix(m_path_steps.size() - 1));
            }

            [[nodiscard]] bool has_parent_output() const noexcept { return has_output_parent(); }

            [[nodiscard]] nb::object key_from_value(const PythonTimeSeriesHandle &value) const {
                if (!(is_bundle() || is_list() || is_dict())) { return nb::none(); }
                if (!same_root(value)) { return nb::none(); }
                if (value.m_path_steps.size() != m_path_steps.size() + 1) { return nb::none(); }
                for (size_t i = 0; i < m_path_steps.size(); ++i) {
                    if (!path_step_equal(m_path_steps[i], value.m_path_steps[i])) { return nb::none(); }
                }

                const PathStep &step = value.m_path_steps.back();
                switch (step.kind) {
                    case PathStep::Kind::Field: return nb::str(step.field_name.c_str());
                    case PathStep::Kind::Index: return nb::int_(step.index);
                    case PathStep::Kind::Key: return step.key.to_python();
                }
                return nb::none();
            }

            [[nodiscard]] size_t len() const {
                if (is_bundle()) { return m_schema->data.tsb.field_count; }
                if (is_list()) { return m_input != nullptr ? input_view().as_list().size() : output_view().as_list().size(); }
                if (is_dict()) { return m_input != nullptr ? input_view().as_dict().size() : output_view().as_dict().size(); }
                if (is_set()) { return m_input != nullptr ? input_view().as_set().size() : output_view().as_set().size(); }
                if (is_window()) { return m_input != nullptr ? input_view().as_window().size() : output_view().as_window().size(); }
                throw std::logic_error("v2 Python time-series __len__ requires a collection schema");
            }

            [[nodiscard]] bool contains(const nb::handle &item) const {
                if (is_bundle()) { return field_schema(m_schema, nb::cast<std::string>(item)) != nullptr; }
                if (is_dict()) {
                    const Value key_value = key_value_from_python(item);
                    return m_input != nullptr ? input_view().value().as_map().contains(key_value.view())
                                              : output_view().value().as_map().contains(key_value.view());
                }
                if (is_set()) {
                    const value::TypeMeta *element_schema = current_value().as_set().element_schema();
                    if (element_schema == nullptr) { return false; }
                    Value value = ts_nested_value_from_python(*element_schema, item);
                    return m_input != nullptr ? input_view().value().as_set().contains(value.view())
                                              : output_view().value().as_set().contains(value.view());
                }
                throw std::logic_error("v2 Python time-series __contains__ requires a bundle, dict, or set schema");
            }

            [[nodiscard]] nb::tuple keys() const {
                nb::list result;
                if (is_bundle()) {
                    for (size_t i = 0; i < m_schema->data.tsb.field_count; ++i) {
                        result.append(nb::str(m_schema->data.tsb.fields[i].name));
                    }
                    return nb::tuple(result);
                }
                if (is_list()) {
                    if (m_input != nullptr) {
                        const auto list = input_view().as_list();
                        for (size_t i = 0; i < list.size(); ++i) { result.append(nb::int_(i)); }
                    } else {
                        const auto list = output_view().as_list();
                        for (size_t i = 0; i < list.size(); ++i) { result.append(nb::int_(i)); }
                    }
                    return nb::tuple(result);
                }
                if (is_dict()) {
                    const auto append_keys = [&result](const auto &dict) {
                        for (const View &key : dict.keys()) { result.append(key.to_python()); }
                    };
                    if (m_input != nullptr) {
                        append_keys(input_view().as_dict());
                    } else {
                        append_keys(output_view().as_dict());
                    }
                    return nb::tuple(result);
                }
                throw std::logic_error("v2 Python time-series keys() requires a bundle, list, or dict schema");
            }

            [[nodiscard]] nb::list values() const {
                nb::list result;
                if (is_bundle()) {
                    for (size_t i = 0; i < m_schema->data.tsb.field_count; ++i) {
                        result.append(nb::cast(field(m_schema->data.tsb.fields[i].name)));
                    }
                    return result;
                }
                if (is_list()) {
                    if (m_input != nullptr) {
                        const auto list = input_view().as_list();
                        for (size_t i = 0; i < list.size(); ++i) { result.append(nb::cast(index(i))); }
                    } else {
                        const auto list = output_view().as_list();
                        for (size_t i = 0; i < list.size(); ++i) { result.append(nb::cast(index(i))); }
                    }
                    return result;
                }
                if (is_dict()) {
                    const auto append_values = [this, &result](const auto &dict) {
                        for (const auto &[key, value] : dict.items()) {
                            static_cast<void>(value);
                            result.append(nb::cast(key_item(key)));
                        }
                    };
                    if (m_input != nullptr) {
                        append_values(input_view().as_dict());
                    } else {
                        append_values(output_view().as_dict());
                    }
                    return result;
                }
                if (is_set()) {
                    const View value = current_value();
                    if (!value.has_value()) { return result; }

                    for (auto item : nb::iter(value.to_python())) { result.append(nb::borrow<nb::object>(item)); }
                    return result;
                }
                throw std::logic_error("v2 Python time-series values() requires a bundle, list, dict, or set schema");
            }

            [[nodiscard]] nb::set added() const {
                if (!is_set()) { throw std::logic_error("v2 Python time-series added() requires a TSS schema"); }

                nb::set result;
                if (m_input != nullptr) {
                    const TSInputView view = input_view();
                    for (const View &item : view.as_set().added_values()) { result.add(item.to_python()); }
                    if (result.empty() && sampled_this_tick(view.context_ref(), view.evaluation_time()) && view.valid()) {
                        for (const View &item : view.as_set().values()) { result.add(item.to_python()); }
                    }
                } else {
                    for (const View &item : output_view().as_set().added_values()) { result.add(item.to_python()); }
                }
                return result;
            }

            [[nodiscard]] nb::set removed() const {
                if (!is_set()) { throw std::logic_error("v2 Python time-series removed() requires a TSS schema"); }

                nb::set result;
                if (m_input != nullptr) {
                    const TSInputView view = input_view();
                    for (const View &item : view.as_set().removed_values()) { result.add(item.to_python()); }
                } else {
                    for (const View &item : output_view().as_set().removed_values()) { result.add(item.to_python()); }
                }
                return result;
            }

            [[nodiscard]] nb::list items() const {
                nb::list result;
                if (is_bundle()) {
                    for (size_t i = 0; i < m_schema->data.tsb.field_count; ++i) {
                        const auto &field_info = m_schema->data.tsb.fields[i];
                        result.append(nb::make_tuple(nb::str(field_info.name), nb::cast(field(field_info.name))));
                    }
                    return result;
                }
                if (is_list()) {
                    if (m_input != nullptr) {
                        const auto list = input_view().as_list();
                        for (size_t i = 0; i < list.size(); ++i) { result.append(nb::make_tuple(nb::int_(i), nb::cast(index(i)))); }
                    } else {
                        const auto list = output_view().as_list();
                        for (size_t i = 0; i < list.size(); ++i) { result.append(nb::make_tuple(nb::int_(i), nb::cast(index(i)))); }
                    }
                    return result;
                }
                if (is_dict()) {
                    const auto append_items = [this, &result](const auto &dict) {
                        for (const auto &[key, value] : dict.items()) {
                            static_cast<void>(value);
                            result.append(nb::make_tuple(key.to_python(), nb::cast(key_item(key))));
                        }
                    };
                    if (m_input != nullptr) {
                        append_items(input_view().as_dict());
                    } else {
                        append_items(output_view().as_dict());
                    }
                    return result;
                }
                throw std::logic_error("v2 Python time-series items() requires a bundle, list, or dict schema");
            }

            [[nodiscard]] nb::tuple added_keys() const {
                if (!is_dict()) { throw std::logic_error("v2 Python time-series added_keys() requires a TSD schema"); }

                const auto build_keys = [](const auto &view) {
                    nb::list result;

                    if (const Value *previous_value = transition_previous_map_value(view); previous_value != nullptr) {
                        const auto       current  = view.value().as_map();
                        const auto       previous = previous_value->view().as_map();
                        constexpr size_t no_slot  = static_cast<size_t>(-1);

                        for (size_t slot = current.first_live_slot(); slot != no_slot; slot = current.next_live_slot(slot)) {
                            const View key = current.delta().key_at_slot(slot);
                            if (!previous.contains(key)) { result.append(key.to_python()); }
                        }
                        return nb::tuple(result);
                    }

                    MapDeltaView delta = view.delta_value().as_map().delta();
                    for (size_t slot = delta.first_added_slot(); slot != static_cast<size_t>(-1);
                         slot        = delta.next_added_slot(slot)) {
                        result.append(delta.key_at_slot(slot).to_python());
                    }
                    return nb::tuple(result);
                };

                return m_input != nullptr ? build_keys(input_view()) : build_keys(output_view());
            }

            [[nodiscard]] nb::tuple removed_keys() const {
                if (!is_dict()) { throw std::logic_error("v2 Python time-series removed_keys() requires a TSD schema"); }

                const auto build_keys = [](const auto &view) {
                    nb::list result;

                    if (const Value *previous_value = transition_previous_map_value(view); previous_value != nullptr) {
                        const auto       current  = view.value().as_map();
                        const auto       previous = previous_value->view().as_map();
                        constexpr size_t no_slot  = static_cast<size_t>(-1);

                        for (size_t slot = previous.first_live_slot(); slot != no_slot; slot = previous.next_live_slot(slot)) {
                            const View key = previous.delta().key_at_slot(slot);
                            if (!current.contains(key)) { result.append(key.to_python()); }
                        }
                        return nb::tuple(result);
                    }

                    MapDeltaView delta = view.delta_value().as_map().delta();
                    for (size_t slot = delta.first_removed_slot(); slot != static_cast<size_t>(-1);
                         slot        = delta.next_removed_slot(slot)) {
                        result.append(delta.key_at_slot(slot).to_python());
                    }
                    return nb::tuple(result);
                };

                return m_input != nullptr ? build_keys(input_view()) : build_keys(output_view());
            }

            [[nodiscard]] nb::list added_values() const {
                if (!is_dict()) { throw std::logic_error("v2 Python time-series added_values() requires a TSD schema"); }

                const auto build_values = [this](const auto &view) {
                    nb::list result;

                    if (const Value *previous_value = transition_previous_map_value(view); previous_value != nullptr) {
                        const auto       current  = view.value().as_map();
                        const auto       previous = previous_value->view().as_map();
                        constexpr size_t no_slot  = static_cast<size_t>(-1);

                        for (size_t slot = current.first_live_slot(); slot != no_slot; slot = current.next_live_slot(slot)) {
                            const View key = current.delta().key_at_slot(slot);
                            if (!previous.contains(key)) { result.append(nb::cast(dict_slot_item(view.context_ref(), slot))); }
                        }
                        return result;
                    }

                    MapDeltaView delta = view.delta_value().as_map().delta();
                    for (size_t slot = delta.first_added_slot(); slot != static_cast<size_t>(-1);
                         slot        = delta.next_added_slot(slot)) {
                        result.append(nb::cast(dict_slot_item(view.context_ref(), slot)));
                    }
                    return result;
                };

                return m_input != nullptr ? build_values(input_view()) : build_values(output_view());
            }

            [[nodiscard]] nb::list removed_values() const {
                if (!is_dict()) { throw std::logic_error("v2 Python time-series removed_values() requires a TSD schema"); }

                const auto build_values = [this](const auto &view) {
                    nb::list result;

                    if (const Value *previous_value = transition_previous_map_value(view); previous_value != nullptr) {
                        const auto       current  = view.value().as_map();
                        const auto       previous = previous_value->view().as_map();
                        constexpr size_t no_slot  = static_cast<size_t>(-1);

                        for (size_t slot = previous.first_live_slot(); slot != no_slot; slot = previous.next_live_slot(slot)) {
                            const View key = previous.delta().key_at_slot(slot);
                            if (!current.contains(key)) {
                                result.append(nb::cast(detached_value_item(previous.at(key), value_schema_or_throw(m_schema))));
                            }
                        }
                        return result;
                    }

                    MapDeltaView delta = view.delta_value().as_map().delta();
                    for (size_t slot = delta.first_removed_slot(); slot != static_cast<size_t>(-1);
                         slot        = delta.next_removed_slot(slot)) {
                        result.append(nb::cast(dict_slot_item(view.context_ref(), slot)));
                    }
                    return result;
                };

                return m_input != nullptr ? build_values(input_view()) : build_values(output_view());
            }

            [[nodiscard]] nb::list added_items() const {
                if (!is_dict()) { throw std::logic_error("v2 Python time-series added_items() requires a TSD schema"); }

                const auto build_items = [this](const auto &view) {
                    nb::list result;

                    if (const Value *previous_value = transition_previous_map_value(view); previous_value != nullptr) {
                        const auto       current  = view.value().as_map();
                        const auto       previous = previous_value->view().as_map();
                        constexpr size_t no_slot  = static_cast<size_t>(-1);

                        for (size_t slot = current.first_live_slot(); slot != no_slot; slot = current.next_live_slot(slot)) {
                            const View key = current.delta().key_at_slot(slot);
                            if (!previous.contains(key)) {
                                result.append(nb::make_tuple(key.to_python(), nb::cast(dict_slot_item(view.context_ref(), slot))));
                            }
                        }
                        return result;
                    }

                    MapDeltaView delta = view.delta_value().as_map().delta();
                    for (size_t slot = delta.first_added_slot(); slot != static_cast<size_t>(-1);
                         slot        = delta.next_added_slot(slot)) {
                        result.append(nb::make_tuple(delta.key_at_slot(slot).to_python(),
                                                     nb::cast(dict_slot_item(view.context_ref(), slot))));
                    }
                    return result;
                };

                return m_input != nullptr ? build_items(input_view()) : build_items(output_view());
            }

            [[nodiscard]] nb::list removed_items() const {
                if (!is_dict()) { throw std::logic_error("v2 Python time-series removed_items() requires a TSD schema"); }

                const auto build_items = [this](const auto &view) {
                    nb::list result;

                    if (const Value *previous_value = transition_previous_map_value(view); previous_value != nullptr) {
                        const auto       current  = view.value().as_map();
                        const auto       previous = previous_value->view().as_map();
                        constexpr size_t no_slot  = static_cast<size_t>(-1);

                        for (size_t slot = previous.first_live_slot(); slot != no_slot; slot = previous.next_live_slot(slot)) {
                            const View key = previous.delta().key_at_slot(slot);
                            if (!current.contains(key)) {
                                result.append(nb::make_tuple(
                                    key.to_python(),
                                    nb::cast(detached_value_item(previous.at(key), value_schema_or_throw(m_schema)))));
                            }
                        }
                        return result;
                    }

                    MapDeltaView delta = view.delta_value().as_map().delta();
                    for (size_t slot = delta.first_removed_slot(); slot != static_cast<size_t>(-1);
                         slot        = delta.next_removed_slot(slot)) {
                        result.append(nb::make_tuple(delta.key_at_slot(slot).to_python(),
                                                     nb::cast(dict_slot_item(view.context_ref(), slot))));
                    }
                    return result;
                };

                return m_input != nullptr ? build_items(input_view()) : build_items(output_view());
            }

            [[nodiscard]] nb::list valid_items() const {
                nb::list result;
                if (is_bundle()) {
                    for (size_t i = 0; i < m_schema->data.tsb.field_count; ++i) {
                        const auto child = field(m_schema->data.tsb.fields[i].name);
                        if (child.valid()) {
                            result.append(nb::make_tuple(nb::str(m_schema->data.tsb.fields[i].name), nb::cast(child)));
                        }
                    }
                    return result;
                }
                if (is_list()) {
                    if (m_input != nullptr) {
                        const auto list = input_view().as_list();
                        for (size_t i = 0; i < list.size(); ++i) {
                            const auto child = index(i);
                            if (child.valid()) { result.append(nb::make_tuple(nb::int_(i), nb::cast(child))); }
                        }
                    } else {
                        const auto list = output_view().as_list();
                        for (size_t i = 0; i < list.size(); ++i) {
                            const auto child = index(i);
                            if (child.valid()) { result.append(nb::make_tuple(nb::int_(i), nb::cast(child))); }
                        }
                    }
                    return result;
                }
                if (is_dict()) {
                    const auto append_valid_items = [this, &result](const auto &dict) {
                        for (const auto &[key, value] : dict.items()) {
                            static_cast<void>(value);
                            const auto child = key_item(key);
                            if (child.valid()) { result.append(nb::make_tuple(key.to_python(), nb::cast(child))); }
                        }
                    };
                    if (m_input != nullptr) {
                        append_valid_items(input_view().as_dict());
                    } else {
                        append_valid_items(output_view().as_dict());
                    }
                    return result;
                }
                throw std::logic_error("v2 Python time-series valid_items() requires a bundle, list, or dict schema");
            }

            [[nodiscard]] nb::list modified_items() const {
                nb::list result;
                if (is_bundle()) {
                    const auto append_modified_items = [this, &result](const auto &bundle) {
                        for (const auto &[name, child] : bundle.modified_items()) {
                            result.append(
                                nb::make_tuple(nb::str(name.data(), name.size()), nb::cast(handle_from_context(child.context_ref()))));
                        }
                    };
                    if (m_input != nullptr) {
                        append_modified_items(input_view().as_bundle());
                    } else {
                        append_modified_items(output_view().as_bundle());
                    }
                    return result;
                }
                if (is_list()) {
                    const auto append_modified_items = [this, &result](const auto &list) {
                        for (const auto &[index, child] : list.modified_items()) {
                            result.append(nb::make_tuple(nb::int_(index), nb::cast(handle_from_context(child.context_ref()))));
                        }
                    };
                    if (m_input != nullptr) {
                        append_modified_items(input_view().as_list());
                    } else {
                        append_modified_items(output_view().as_list());
                    }
                    return result;
                }
                if (is_dict()) {
                    const auto append_modified_items = [this, &result](const auto &dict) {
                        for (const auto &[key, value] : dict.items()) {
                            static_cast<void>(value);
                            const auto child = key_item(key);
                            if (!child.modified()) { continue; }
                            result.append(nb::make_tuple(key.to_python(), nb::cast(key_item(key))));
                        }
                    };
                    if (m_input != nullptr) {
                        append_modified_items(input_view().as_dict());
                    } else {
                        append_modified_items(output_view().as_dict());
                    }
                    return result;
                }
                throw std::logic_error("v2 Python time-series modified_items() requires a bundle, list, or dict schema");
            }

            [[nodiscard]] nb::tuple valid_keys() const {
                nb::list result;
                for (const auto &item : valid_items()) { result.append(nb::cast<nb::tuple>(item)[0]); }
                return nb::tuple(result);
            }

            [[nodiscard]] nb::tuple modified_keys() const {
                nb::list result;
                for (const auto &item : modified_items()) { result.append(nb::cast<nb::tuple>(item)[0]); }
                return nb::tuple(result);
            }

            [[nodiscard]] nb::list valid_values() const {
                nb::list result;
                for (const auto &item : valid_items()) { result.append(nb::cast<nb::tuple>(item)[1]); }
                return result;
            }

            [[nodiscard]] nb::list modified_values() const {
                nb::list result;
                for (const auto &item : modified_items()) { result.append(nb::cast<nb::tuple>(item)[1]); }
                return result;
            }

            [[nodiscard]] PythonTimeSeriesHandle get_or_create(const nb::handle &key) const {
                if (!is_dict()) { throw std::logic_error("v2 Python time-series get_or_create() requires a TSD schema"); }
                const Value key_value = key_value_from_python(key);
                if (m_input != nullptr) {
                    ensure_input_dict_child(key_value);
                    return key_item(key);
                }
                if (m_output != nullptr || m_bound_output.has_value()) {
                    ensure_output_dict_child(key_value);
                    return handle_from_context(output_view().as_dict().at(key_value.view()).context_ref());
                }
                return key_item(key);
            }

            void create(const nb::handle &key) const { static_cast<void>(get_or_create(key)); }

            void del_item(const nb::handle &key) const {
                if (!is_dict()) { throw std::logic_error("v2 Python time-series __delitem__ requires a TSD schema"); }
                ensure_output();
                const Value key_value = key_value_from_python(key);
                output_view().as_dict().erase(key_value.view());
            }

            void on_key_removed(const nb::handle &key) const {
                if (!is_dict()) { throw std::logic_error("v2 Python time-series on_key_removed() requires a TSD schema"); }

                const Value key_value = key_value_from_python(key);

                if (m_input != nullptr) {
                    TSInputView view = input_view();
                    if (!view.value().has_value()) { return; }
                    auto mutation = view.value().as_map().begin_mutation(evaluation_time());
                    static_cast<void>(mutation.remove(key_value.view()));
                    return;
                }

                ensure_output();
                output_view().as_dict().erase(key_value.view());
            }

            [[nodiscard]] PythonTimeSeriesHandle get_ref(const nb::handle &key, nb::object requester = nb::none()) const {
                static_cast<void>(requester);
                if (!is_dict()) { throw std::logic_error("v2 Python time-series get_ref() requires a TSD schema"); }
                return key_item(key);
            }

            void release_ref(const nb::handle &key, nb::object requester = nb::none()) const {
                static_cast<void>(key);
                static_cast<void>(requester);
                if (!is_dict()) { throw std::logic_error("v2 Python time-series release_ref() requires a TSD schema"); }
            }

            void bind_output(const PythonTimeSeriesHandle &output) const {
                ensure_input();
                if (is_reference() && !output.is_reference()) {
                    bind_reference_value(output);
                    return;
                }
                input_view().bind_output(output.output_view());
            }

            void un_bind_output(bool unbind_refs = false) const {
                static_cast<void>(unbind_refs);
                ensure_input();

                TSInputView view  = input_view();
                BaseState  *state = view.context_ref().ts_state;
                if (state == nullptr) {
                    throw std::runtime_error("v2 Python time-series un_bind_output requires a live input state");
                }

                if (!is_reference()) { view.unbind_output(); }

                if (is_reference()) {
                    if (state->storage_kind == TSStorageKind::TargetLink) { static_cast<TargetLinkState *>(state)->reset_target(); }
                    set_local_reference_value(view, TimeSeriesReference::make());
                    state->last_modified_time = MIN_DT;
                }
            }

            [[nodiscard]] bool bound() const {
                if (m_input == nullptr) { return false; }
                TSInputView view = input_view();
                if (const LinkedTSContext *target = view.linked_target(); target != nullptr) { return target->is_bound(); }
                return ::hgraph::detail::has_local_reference_binding(view.context_ref());
            }

            [[nodiscard]] bool has_peer() const { return bound(); }

            [[nodiscard]] bool has_output() const { return bound(); }

            [[nodiscard]] nb::object output() const {
                if (m_input == nullptr) { return nb::none(); }
                TSInputView view = input_view();
                if (const LinkedTSContext *target = view.linked_target(); target != nullptr && target->is_bound()) {
                    return nb::cast(PythonTimeSeriesHandle{*target, evaluation_time()});
                }

                if (is_reference()) {
                    const View value = current_value();
                    if (value.has_value()) {
                        if (const auto *ref = value.as_atomic().template try_as<TimeSeriesReference>();
                            ref != nullptr && ref->is_peered()) {
                            const LinkedTSContext &target = ref->target();
                            engine_time_t          when   = effective_modified_time(target);
                            if (when == MIN_DT) { when = ref->observed_time(); }
                            return nb::cast(PythonTimeSeriesHandle{target, when});
                        }
                    }
                }

                return nb::none();
            }

            [[nodiscard]] nb::object reference_output() const { return nb::none(); }

            [[nodiscard]] PythonTimeSeriesHandle get_contains_output(const nb::handle &item, nb::object requester) const {
                if (!is_set()) { throw std::logic_error("v2 Python time-series get_contains_output() requires a TSS schema"); }
                if (requester.is_none()) {
                    throw std::logic_error("v2 Python time-series get_contains_output() requires a requester");
                }

                TSOutputView view = output_view();
                const auto  *item_schema =
                    m_schema != nullptr && m_schema->value_type != nullptr ? m_schema->value_type->element_type : nullptr;
                if (item_schema == nullptr) {
                    throw std::logic_error("v2 Python time-series get_contains_output() requires a TSS element schema");
                }

                Value key_value = ts_nested_value_from_python(*item_schema, item);
                return PythonTimeSeriesHandle{view.as_set().register_contains_output(key_value.view()).linked_context(),
                                              evaluation_time()};
            }

            void release_contains_output(const nb::handle &item, nb::object requester) const {
                if (!is_set()) { throw std::logic_error("v2 Python time-series release_contains_output() requires a TSS schema"); }
                if (requester.is_none()) { return; }

                TSOutputView view = output_view();
                const auto  *item_schema =
                    m_schema != nullptr && m_schema->value_type != nullptr ? m_schema->value_type->element_type : nullptr;
                if (item_schema == nullptr) { return; }

                Value key_value = ts_nested_value_from_python(*item_schema, item);
                view.as_set().unregister_contains_output(key_value.view());
            }

            [[nodiscard]] PythonTimeSeriesHandle is_empty_output() const {
                if (!is_set()) { throw std::logic_error("v2 Python time-series is_empty_output() requires a TSS schema"); }
                TSOutputView view = output_view();
                return PythonTimeSeriesHandle{view.as_set().register_is_empty_output().linked_context(), evaluation_time()};
            }

            void make_active() const {
                ensure_input();
                input_view().make_active();
            }

            void make_passive() const {
                ensure_input();
                input_view().make_passive();
            }

            [[nodiscard]] bool active() const {
                ensure_input();
                return input_view().active();
            }

            void apply_result(nb::handle value) const { output_view().apply_result(value); }

            [[nodiscard]] bool can_apply_result(nb::handle value) const {
                ensure_output();
                return output_view().can_apply_result(value);
            }

            void clear() const {
                ensure_output();
                output_view().clear();
            }

            [[nodiscard]] nb::object as_schema() const {
                if (!is_bundle()) { throw std::logic_error("v2 Python time-series as_schema requires a TSB schema"); }
                return nb::cast(*this);
            }

            [[nodiscard]] bool has_removed_value() const {
                if (!is_window()) { throw std::logic_error("v2 Python time-series has_removed_value requires a TSW schema"); }
                if (!current_value().has_value()) { return false; }
                return current_window_value_buffer().has_removed();
            }

            [[nodiscard]] nb::object removed_value() const {
                if (!is_window()) { throw std::logic_error("v2 Python time-series removed_value requires a TSW schema"); }
                if (!current_value().has_value()) { return nb::none(); }
                BufferView buffer = current_window_value_buffer();
                return buffer.has_removed() ? buffer.removed().to_python() : nb::none();
            }

            void set_value(nb::handle value) const { output_view().from_python(value); }

            [[nodiscard]] std::string repr() const {
                return fmt::format("v2._PythonTimeSeriesHandle@{:p}[schema={}]", static_cast<const void *>(this),
                                   schema_kind_label());
            }

          private:
            [[nodiscard]] engine_time_t evaluation_time() const {
                return m_node != nullptr ? m_node->evaluation_time() : m_fixed_evaluation_time;
            }

            [[nodiscard]] PythonTimeSeriesHandle dict_slot_item(const TSViewContext &context, size_t slot) const {
                const auto *collection = context.ts_dispatch != nullptr ? context.ts_dispatch->as_collection() : nullptr;
                const auto *dispatch   = collection != nullptr ? collection->as_keys() : nullptr;
                if (dispatch == nullptr) { return PythonTimeSeriesHandle{LinkedTSContext::none(), evaluation_time()}; }
                return handle_from_context(dispatch->child_at(context, slot));
            }

            [[nodiscard]] Value key_value_from_python(const nb::handle &key) const {
                const value::TypeMeta *key_schema = m_schema != nullptr ? m_schema->key_type() : nullptr;
                if (key_schema == nullptr) { throw std::logic_error("v2 Python dict access requires a key schema"); }

                return ts_nested_value_from_python(*key_schema, key);
            }

            [[nodiscard]] PythonTimeSeriesHandle detached_value_item(const View &value, const TSMeta *schema) const {
                if (schema == nullptr || !value.has_value()) {
                    return PythonTimeSeriesHandle{LinkedTSContext::none(), evaluation_time()};
                }

                const auto     &builder = TSValueBuilderFactory::checked_builder_for(schema);
                LinkedTSContext context{
                    schema, &builder.value_builder().dispatch(), &builder.ts_dispatch(), RawViewAccess::data_of(value), nullptr,
                };
                return PythonTimeSeriesHandle{context, evaluation_time()};
            }

            [[nodiscard]] PythonTimeSeriesHandle handle_from_context(const TSViewContext &context) const {
                if (!context.is_bound()) { return PythonTimeSeriesHandle{LinkedTSContext::none(), evaluation_time()}; }
                LinkedTSContext linked{
                    context.schema,   context.value_dispatch, context.ts_dispatch,     context.value_data,
                    context.ts_state, context.owning_output,  context.output_view_ops, context.notification_state,
                    context.pending_dict_child,
                };
                return PythonTimeSeriesHandle{linked, evaluation_time()};
            }

            static void set_local_reference_value(const TSInputView &view, const TimeSeriesReference &value) {
                const TSViewContext &context = view.context_ref();
                if (context.schema == nullptr || context.schema->kind != TSKind::REF || context.schema->value_type == nullptr ||
                    context.value_dispatch == nullptr || context.value_data == nullptr) {
                    throw std::runtime_error("v2 REF local binding requires native REF value storage");
                }

                View local_value{context.value_dispatch, context.value_data, context.schema->value_type};
                local_value.as_atomic().set(value);
            }

            void bind_reference_value(const PythonTimeSeriesHandle &output) const {
                TSInputView view  = input_view();
                BaseState  *state = view.context_ref().ts_state;

                if (state != nullptr) {
                    TSOutputView output_view = output.output_view();
                    if (output_view.context_ref().is_bound() || output_view.value().has_value()) {
                        try {
                            view.bind_output(output_view);
                            return;
                        } catch (...) {}
                    }
                }

                TimeSeriesReference reference   = TimeSeriesReference::make();
                TSOutputView        output_view = output.output_view();
                if (output_view.context_ref().is_bound() || output_view.value().has_value()) {
                    reference = TimeSeriesReference::make(output_view);
                }

                if (state == nullptr) { throw std::runtime_error("v2 REF bind_output requires a live input state"); }
                if (state->storage_kind == TSStorageKind::TargetLink) { static_cast<TargetLinkState *>(state)->reset_target(); }

                set_local_reference_value(view, reference);
                state->mark_modified(evaluation_time());
            }

          public:
            void add_set_item(const nb::handle &item) const {
                if (!is_set()) { throw std::logic_error("v2 Python time-series add() requires a TSS schema"); }
                ensure_output();

                const value::TypeMeta *element_schema = output_view().value().as_set().element_schema();
                if (element_schema == nullptr) { throw std::logic_error("v2 Python set output add() requires an element schema"); }

                Value element = ts_nested_value_from_python(*element_schema, item);
                output_view().as_mutable_set().add(element.view());
            }

            void remove_set_item(const nb::handle &item) const {
                if (!is_set()) { throw std::logic_error("v2 Python time-series remove() requires a TSS schema"); }
                ensure_output();

                const value::TypeMeta *element_schema = output_view().value().as_set().element_schema();
                if (element_schema == nullptr) {
                    throw std::logic_error("v2 Python set output remove() requires an element schema");
                }

                Value element = ts_nested_value_from_python(*element_schema, item);
                output_view().as_mutable_set().remove(element.view());
            }

            void clear_set_items() const {
                if (!is_set()) { throw std::logic_error("v2 Python time-series clear() requires a TSS schema"); }
                ensure_output();
                output_view().as_mutable_set().clear();
            }

          private:
            [[nodiscard]] bool has_live_bound_output() const noexcept {
                return m_input == nullptr && m_output == nullptr && m_bound_output.has_value() &&
                       m_bound_output->ts_state != nullptr;
            }

            [[nodiscard]] bool has_detached_bound_value() const noexcept {
                return m_input == nullptr && m_output == nullptr && m_bound_output.has_value() &&
                       m_bound_output->ts_state == nullptr;
            }

            [[nodiscard]] TSInputView input_view() const { return input_view_prefix(m_path_steps.size()); }

            [[nodiscard]] TSOutputView output_view() const { return output_view_prefix(m_path_steps.size()); }

            [[nodiscard]] PythonTimeSeriesHandle handle_with_prefix(size_t step_count) const {
                std::vector<PathStep> path_steps(m_path_steps.begin(), m_path_steps.begin() + step_count);
                return PythonTimeSeriesHandle{m_node,
                                              m_input,
                                              m_output,
                                              schema_for_prefix(step_count),
                                              std::move(path_steps),
                                              m_bound_output,
                                              m_fixed_evaluation_time};
            }

            [[nodiscard]] TSInputView input_view_prefix(size_t step_count) const {
                ensure_input();
                auto view = m_input->view(m_node, evaluation_time());
                for (size_t i = 0; i < step_count; ++i) {
                    const auto &step = m_path_steps[i];
                    switch (step.kind) {
                        case PathStep::Kind::Field: view = view.as_bundle().field(step.field_name); break;
                        case PathStep::Kind::Index: view = view.as_list().at(step.index); break;
                        case PathStep::Kind::Key:
                            {
                                const TSViewContext &context = view.context_ref();
                                const TSMeta        *schema  = context.resolved().schema;
                                BaseState           *resolved_state =
                                    context.ts_state != nullptr ? context.ts_state->resolved_state() : nullptr;
                                if (schema != nullptr && schema->kind == TSKind::TSD && resolved_state != nullptr &&
                                    resolved_state->storage_kind == TSStorageKind::Native && view.value().has_value()) {
                                    bind_input_dict_state(resolved_state, *schema, view.value());
                                    auto *dict_state = static_cast<TSDState *>(resolved_state);
                                    dict_state->sync_with_value_storage();
                                    const size_t slot = view.value().as_map().find_slot(step.key.view());
                                    if (slot != static_cast<size_t>(-1) &&
                                        (dict_state->child_states.size() <= slot || dict_state->child_states[slot] == nullptr)) {
                                        dict_state->on_insert(slot);
                                    }
                                }
                                view = view.as_dict().at(step.key.view());
                                break;
                            }
                    }
                }
                return view;
            }

            [[nodiscard]] TSOutputView output_view_prefix(size_t step_count) const {
                ensure_output();
                auto view = m_output != nullptr ? m_output->view(evaluation_time())
                                                : output_view_from_context(*m_bound_output, evaluation_time());
                for (size_t i = 0; i < step_count; ++i) {
                    const auto &step = m_path_steps[i];
                    switch (step.kind) {
                        case PathStep::Kind::Field: view = view.as_bundle().field(step.field_name); break;
                        case PathStep::Kind::Index: view = view.as_list().at(step.index); break;
                        case PathStep::Kind::Key:
                            {
                                TSOutputView child = view.as_dict().at(step.key.view());
                                if (child.context_ref().ts_state == nullptr && !child.context_ref().is_bound()) {
                                    BaseState *state =
                                        view.context_ref().ts_state != nullptr ? view.context_ref().ts_state->resolved_state()
                                                                               : nullptr;
                                    if (state != nullptr && state->storage_kind == TSStorageKind::Native) {
                                        auto *dict_state = static_cast<TSDState *>(state);
                                        const size_t slot = view.value().as_map().find_slot(step.key.view());
                                        if (slot != static_cast<size_t>(-1)) {
                                            dict_state->on_insert(slot);
                                            child = view.as_dict().at(step.key.view());
                                        }
                                    }
                                }
                                view = child;
                                break;
                            }
                    }
                }
                return view;
            }

            [[nodiscard]] View current_value() const { return m_input != nullptr ? input_view().value() : output_view().value(); }

            [[nodiscard]] BufferView current_window_value_buffer() const {
                const View value = current_value();
                if (!value.has_value()) {
                    throw std::logic_error("v2 Python time-series window value buffer requires a live value");
                }
                return BufferView{value.as_tuple()[1]};
            }

            static void bind_input_dict_state(BaseState *state, const TSMeta &schema, const View &value) {
                if (state == nullptr || schema.kind != TSKind::TSD || !value.has_value()) { return; }

                const auto *map_dispatch = RawMapAccess(value).map_dispatch();
                if (map_dispatch == nullptr) {
                    throw std::logic_error("v2 Python input dict state creation requires map-backed value storage");
                }

                auto *dict_state = static_cast<TSDState *>(state);
                dict_state->bind_value_storage(*schema.element_ts(), *map_dispatch, RawViewAccess::data_of(value), false);
            }

            static void ensure_input_child_state(TSInputView &view, const PathStep &step, const TSMeta *child_schema) {
                if (child_schema == nullptr) { return; }

                BaseState *resolved_state =
                    view.context_ref().ts_state != nullptr ? view.context_ref().ts_state->resolved_state() : nullptr;
                if (resolved_state == nullptr || resolved_state->storage_kind != TSStorageKind::Native) { return; }

                switch (step.kind) {
                    case PathStep::Kind::Field:
                        {
                            const TSMeta *schema = view.context_ref().resolved().schema;
                            if (schema == nullptr || schema->kind != TSKind::TSB) { return; }

                            auto *bundle_state = static_cast<TSBState *>(resolved_state);

                            for (size_t index = 0; index < schema->field_count(); ++index) {
                                const auto &field = schema->fields()[index];
                                if (step.field_name != field.name) { continue; }
                                if (bundle_state->child_states.size() <= index) { bundle_state->child_states.resize(index + 1); }
                                if (bundle_state->child_states[index] == nullptr) {
                                    bundle_state->child_states[index] =
                                        make_time_series_state_node(*child_schema, parent_ptr(*bundle_state), index);
                                }
                                bind_input_dict_state(std::visit([](auto &typed_state) -> BaseState * { return &typed_state; },
                                                                 *bundle_state->child_states[index]),
                                                      *child_schema, view.value().as_bundle().at(index));
                                return;
                            }
                            return;
                        }

                    case PathStep::Kind::Index:
                        {
                            const TSMeta *schema = view.context_ref().resolved().schema;
                            if (schema == nullptr || schema->kind != TSKind::TSL) { return; }

                            auto *list_state = static_cast<TSLState *>(resolved_state);
                            if (list_state->child_states.size() <= step.index) { list_state->child_states.resize(step.index + 1); }
                            if (list_state->child_states[step.index] == nullptr) {
                                list_state->child_states[step.index] =
                                    make_time_series_state_node(*child_schema, parent_ptr(*list_state), step.index);
                            }
                            bind_input_dict_state(std::visit([](auto &typed_state) -> BaseState * { return &typed_state; },
                                                             *list_state->child_states[step.index]),
                                                  *child_schema, view.value().as_list().at(step.index));
                            return;
                        }

                    case PathStep::Kind::Key:
                        {
                            const TSMeta *schema = view.context_ref().resolved().schema;
                            if (schema == nullptr || schema->kind != TSKind::TSD || !view.value().has_value()) { return; }

                            auto *dict_state = static_cast<TSDState *>(resolved_state);
                            dict_state->sync_with_value_storage();
                            const size_t slot = view.value().as_map().find_slot(step.key.view());
                            if (slot == static_cast<size_t>(-1)) { return; }

                            if (dict_state->child_states.size() <= slot || dict_state->child_states[slot] == nullptr) {
                                dict_state->on_insert(slot);
                            }
                            return;
                        }
                }
            }

            void ensure_input_dict_child(const Value &key) const {
                TSInputView view      = input_view();
                auto        dict_view = view.as_dict();
                if (dict_view.at(key.view()).context_ref().is_bound()) { return; }

                const TSMeta *child_schema = m_schema != nullptr ? m_schema->element_ts() : nullptr;
                if (child_schema == nullptr || child_schema->value_type == nullptr || !view.value().has_value()) {
                    throw std::logic_error("v2 Python time-series dict child creation requires live map-backed storage");
                }

                Value placeholder{*child_schema->value_type, MutationTracking::Plain};
                auto  mutation = view.value().as_map().begin_mutation(evaluation_time());
                mutation.set(key.view(), placeholder.view());

                if (BaseState *state =
                        view.context_ref().ts_state != nullptr ? view.context_ref().ts_state->resolved_state() : nullptr;
                    state != nullptr && state->storage_kind == TSStorageKind::Native) {
                    auto *dict_state = static_cast<TSDState *>(state);
                    dict_state->sync_with_value_storage();

                    if (!dict_view.at(key.view()).context_ref().is_bound()) {
                        const size_t slot = view.value().as_map().find_slot(key.view());
                        if (slot == static_cast<size_t>(-1)) {
                            throw std::logic_error("v2 Python time-series dict child creation failed to allocate a stable slot");
                        }

                        dict_state->on_insert(slot);
                    }
                }
            }

            void ensure_output_dict_child(const Value &key) const {
                TSOutputView view       = output_view();
                TSOutputView child_view = view.as_dict().at(key.view());
                if (child_view.context_ref().is_bound() || child_view.value().has_value()) { return; }

                const TSMeta *child_schema = m_schema != nullptr ? m_schema->element_ts() : nullptr;
                if (child_schema == nullptr || child_schema->value_type == nullptr || !view.value().has_value()) {
                    throw std::logic_error("v2 Python time-series dict child creation requires live map-backed output storage");
                }

                Value placeholder{*child_schema->value_type, MutationTracking::Plain};
                placeholder.reset();

                auto mutation = view.value().as_map().begin_mutation(evaluation_time());
                mutation.set(key.view(), placeholder.view());

                if (BaseState *state =
                        view.context_ref().ts_state != nullptr ? view.context_ref().ts_state->resolved_state() : nullptr;
                    state != nullptr && state->storage_kind == TSStorageKind::Native) {
                    auto *dict_state = static_cast<TSDState *>(state);
                    dict_state->sync_with_value_storage();

                    if (!view.as_dict().at(key.view()).context_ref().is_bound()) {
                        const size_t slot = view.value().as_map().find_slot(key.view());
                        if (slot == static_cast<size_t>(-1)) {
                            throw std::logic_error(
                                "v2 Python time-series dict child creation failed to allocate a stable output slot");
                        }

                        dict_state->on_insert(slot);
                    }
                }
            }

            [[nodiscard]] bool has_output_parent() const noexcept {
                return (m_output != nullptr || m_bound_output.has_value()) && !m_path_steps.empty();
            }

            [[nodiscard]] const TSMeta *root_schema() const {
                if (m_input != nullptr) { return input_view_prefix(0).ts_schema(); }
                if (m_output != nullptr || m_bound_output.has_value()) { return output_view_prefix(0).ts_schema(); }
                return m_schema;
            }

            [[nodiscard]] const TSMeta *schema_for_prefix(size_t step_count) const {
                const TSMeta *schema = root_schema();
                for (size_t i = 0; i < step_count && schema != nullptr; ++i) {
                    const auto &step = m_path_steps[i];
                    switch (step.kind) {
                        case PathStep::Kind::Field: schema = field_schema_or_throw(schema, step.field_name); break;
                        case PathStep::Kind::Index: schema = index_schema_or_throw(schema, step.index); break;
                        case PathStep::Kind::Key: schema = value_schema_or_throw(schema); break;
                    }
                }
                return schema;
            }

            [[nodiscard]] bool same_root(const PythonTimeSeriesHandle &other) const noexcept {
                if (m_node != other.m_node || m_input != other.m_input || m_output != other.m_output) { return false; }
                if (m_bound_output.has_value() != other.m_bound_output.has_value()) { return false; }
                if (!m_bound_output.has_value()) { return true; }
                return m_bound_output->ts_state == other.m_bound_output->ts_state &&
                       m_bound_output->schema == other.m_bound_output->schema &&
                       m_bound_output->value_data == other.m_bound_output->value_data &&
                       m_bound_output->owning_output == other.m_bound_output->owning_output &&
                       m_bound_output->output_view_ops == other.m_bound_output->output_view_ops;
            }

            [[nodiscard]] static bool path_step_equal(const PathStep &lhs, const PathStep &rhs) {
                if (lhs.kind != rhs.kind) { return false; }
                switch (lhs.kind) {
                    case PathStep::Kind::Field: return lhs.field_name == rhs.field_name;
                    case PathStep::Kind::Index: return lhs.index == rhs.index;
                    case PathStep::Kind::Key: return lhs.key.view() == rhs.key.view();
                }
                return false;
            }

            void ensure_input() const {
                if (m_input == nullptr) { throw std::logic_error("v2 Python time-series handle is not an input"); }
            }

            void ensure_output() const {
                if (m_output == nullptr && !m_bound_output.has_value()) {
                    throw std::logic_error("v2 Python time-series handle is not an output");
                }
            }

            [[nodiscard]] std::string_view schema_kind_label() const noexcept {
                if (m_schema == nullptr) { return "none"; }
                switch (m_schema->kind) {
                    case TSKind::TSValue: return "leaf";
                    case TSKind::TSB: return "bundle";
                    case TSKind::TSL: return "list";
                    case TSKind::TSD: return "dict";
                    case TSKind::TSS: return "set";
                    case TSKind::TSW: return "window";
                    case TSKind::REF: return "ref";
                    case TSKind::SIGNAL: return "signal";
                }
                return "unknown";
            }

            Node                          *m_node{nullptr};
            TSInput                       *m_input{nullptr};
            TSOutput                      *m_output{nullptr};
            const TSMeta                  *m_schema{nullptr};
            std::vector<PathStep>          m_path_steps;
            std::optional<LinkedTSContext> m_bound_output;
            engine_time_t                  m_fixed_evaluation_time{MIN_DT};

            friend struct V2PythonReferenceSupport;
        };

        struct V2PythonReferenceSupport
        {
            [[nodiscard]] static TimeSeriesReference make(nb::object ts, nb::object items) {
                if (!ts.is_none()) {
                    if (nb::isinstance<TimeSeriesReference>(ts)) { return nb::cast<TimeSeriesReference>(ts); }
                    if (nb::isinstance<PythonTimeSeriesHandle>(ts)) {
                        auto handle = nb::cast<PythonTimeSeriesHandle>(ts);
                        return handle.m_input != nullptr ? TimeSeriesReference::make(handle.input_view())
                                                         : TimeSeriesReference::make(handle.output_view());
                    }
                    throw std::runtime_error("v2 TimeSeriesReference.make only supports v2 time-series handles and refs");
                }

                if (!items.is_none()) {
                    std::vector<TimeSeriesReference> refs;
                    for (auto item : nb::iter(items)) {
                        refs.push_back(nb::cast<TimeSeriesReference>(nb::borrow<nb::object>(item)));
                    }
                    return TimeSeriesReference::make(std::move(refs));
                }

                return TimeSeriesReference::make();
            }

            static void bind_input(const TimeSeriesReference &ref, PythonTimeSeriesHandle &input_handle) {
                if (input_handle.m_input == nullptr) {
                    throw std::runtime_error("v2 TimeSeriesReference.bind_input requires an input handle");
                }

                if (ref.is_empty()) {
                    input_handle.un_bind_output();
                    return;
                }

                if (!ref.is_peered()) {
                    throw std::runtime_error("v2 TimeSeriesReference.bind_input only supports peered refs for now");
                }

                input_handle.bind_output(PythonTimeSeriesHandle{ref.target(), input_handle.evaluation_time()});
            }

            [[nodiscard]] static nb::object output(const TimeSeriesReference &ref) {
                if (!ref.is_peered()) { return nb::none(); }

                const LinkedTSContext &target = ref.target();
                engine_time_t          when   = effective_modified_time(target);
                if (when == MIN_DT) { when = ref.observed_time(); }
                return nb::cast(PythonTimeSeriesHandle{target, when});
            }
        };

        class PythonNodeHandle
        {
          public:
            PythonNodeHandle(Node *node, nb::object signature, nb::object scalars, nb::object graph, nb::object input,
                             nb::object output, nb::object error_output, nb::object recordable_state, nb::object scheduler)
                : m_node(node), m_signature(std::move(signature)), m_scalars(std::move(scalars)), m_graph(std::move(graph)),
                  m_input(std::move(input)), m_output(std::move(output)), m_error_output(std::move(error_output)),
                  m_recordable_state(std::move(recordable_state)), m_scheduler(std::move(scheduler)) {}

            [[nodiscard]] int64_t node_ndx() const noexcept { return m_node != nullptr ? m_node->public_node_index() : -1; }

            [[nodiscard]] nb::tuple owning_graph_id() const {
                if (m_graph.is_valid() && !m_graph.is_none()) { return nb::cast<nb::tuple>(m_graph.attr("graph_id")); }
                return nb::tuple();
            }

            [[nodiscard]] nb::tuple node_id() const {
                nb::list out;
                for (auto value : owning_graph_id()) { out.append(nb::borrow<nb::object>(value)); }
                if (m_node != nullptr) { out.append(node_ndx()); }
                return nb::tuple(out);
            }

            [[nodiscard]] nb::object signature() const { return nb::borrow(m_signature); }

            [[nodiscard]] nb::object scalars() const { return nb::borrow(m_scalars); }

            [[nodiscard]] nb::object graph() const { return nb::borrow(m_graph); }

            [[nodiscard]] nb::object input() const { return m_input.is_valid() ? nb::borrow(m_input) : nb::none(); }

            [[nodiscard]] nb::object output() const { return m_output.is_valid() ? nb::borrow(m_output) : nb::none(); }

            [[nodiscard]] nb::object error_output() const {
                return m_error_output.is_valid() ? nb::borrow(m_error_output) : nb::none();
            }

            [[nodiscard]] nb::object recordable_state() const {
                return m_recordable_state.is_valid() ? nb::borrow(m_recordable_state) : nb::none();
            }

            [[nodiscard]] nb::object scheduler() const { return nb::borrow(m_scheduler); }

            [[nodiscard]] bool has_scheduler() const noexcept { return m_scheduler.is_valid() && !m_scheduler.is_none(); }

            [[nodiscard]] bool has_input() const noexcept { return m_input.is_valid() && !m_input.is_none(); }

            [[nodiscard]] bool has_output() const noexcept { return m_output.is_valid() && !m_output.is_none(); }

            [[nodiscard]] bool has_error_output() const noexcept { return m_error_output.is_valid() && !m_error_output.is_none(); }

            void notify(nb::object modified_time = nb::none()) const {
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

            void notify_next_cycle() const {
                assert(m_node != nullptr);
                if (m_node->started()) {
                    graph_of(m_node).schedule_node(m_node->node_index(),
                                                   graph_of(m_node).evaluation_clock().next_cycle_evaluation_time());
                } else {
                    notify();
                }
            }

            [[nodiscard]] std::string repr() const {
                const nb::object  signature_obj    = signature();
                const std::string name             = nb::cast<std::string>(signature_obj.attr("name"));
                const nb::object  label            = signature_obj.attr("label");
                const std::string wiring_path_name = nb::cast<std::string>(signature_obj.attr("wiring_path_name"));
                if (!label.is_none()) { return fmt::format("{}.{}", wiring_path_name, nb::cast<std::string>(label)); }
                return fmt::format("{}.{}", wiring_path_name, name);
            }

          private:
            Node      *m_node{nullptr};
            nb::object m_signature;
            nb::object m_scalars;
            nb::object m_graph;
            nb::object m_input;
            nb::object m_output;
            nb::object m_error_output;
            nb::object m_recordable_state;
            nb::object m_scheduler;
        };
    }  // namespace

    nb::object make_python_node_handle(nb::handle signature, nb::handle scalars, Node *node, TSInput *input, TSOutput *output,
                                       TSOutput *error_output, TSOutput *recordable_state, const TSMeta *input_schema,
                                       const TSMeta *output_schema, const TSMeta *error_output_schema,
                                       const TSMeta *recordable_state_schema, NodeScheduler *scheduler) {
        nb::gil_scoped_acquire guard;
        nb::object             graph = nb::cast(PythonGraphHandle{node});
        nb::object             input_handle =
            input != nullptr ? nb::cast(PythonTimeSeriesHandle{node, input, nullptr, input_schema}) : nb::none();
        nb::object output_handle =
            output != nullptr ? nb::cast(PythonTimeSeriesHandle{node, nullptr, output, output_schema}) : nb::none();
        nb::object error_output_handle = error_output != nullptr
                                             ? nb::cast(PythonTimeSeriesHandle{node, nullptr, error_output, error_output_schema})
                                             : nb::none();
        nb::object recordable_state_handle =
            recordable_state != nullptr ? nb::cast(PythonTimeSeriesHandle{node, nullptr, recordable_state, recordable_state_schema})
                                        : nb::none();
        nb::object scheduler_handle = scheduler != nullptr ? nb::cast(NodeSchedulerHandle{scheduler}) : nb::none();
        return nb::cast(PythonNodeHandle{node, nb::borrow(signature), nb::borrow(scalars), std::move(graph),
                                         std::move(input_handle), std::move(output_handle), std::move(error_output_handle),
                                         std::move(recordable_state_handle), std::move(scheduler_handle)});
    }

    nb::dict make_python_node_kwargs(nb::handle signature, nb::handle scalars, nb::handle node_handle) {
        nb::gil_scoped_acquire guard;
        nb::dict               values;

        nb::object input              = nb::borrow(node_handle).attr("input");
        nb::object time_series_inputs = nb::borrow(signature).attr("time_series_inputs");
        if (!input.is_none() && !time_series_inputs.is_none()) {
            std::unordered_set<std::string> active_input_names;
            nb::object                      active_inputs = nb::borrow(signature).attr("active_inputs");
            if (!active_inputs.is_none()) {
                for (auto item : active_inputs) { active_input_names.insert(nb::cast<std::string>(item)); }
            } else {
                for (auto item : time_series_inputs.attr("keys")()) {
                    active_input_names.insert(nb::cast<std::string>(nb::borrow<nb::object>(item)));
                }
            }

            for (auto key : time_series_inputs.attr("keys")()) {
                nb::object key_obj = nb::borrow<nb::object>(key);
                nb::object handle  = nb::steal<nb::object>(PyObject_GetItem(input.ptr(), key_obj.ptr()));
                if (!handle.is_none() && active_input_names.contains(nb::cast<std::string>(key_obj))) {
                    nb::cast<PythonTimeSeriesHandle &>(handle).make_active();
                }
                values[key_obj] = std::move(handle);
            }
        }

        nb::object injector_type = nb::module_::import_("hgraph._types._scalar_type_meta_data").attr("Injector");
        if (!scalars.is_none()) {
            for (auto item : nb::borrow<nb::dict>(scalars).items()) {
                nb::tuple  pair  = nb::borrow<nb::tuple>(item);
                nb::object key   = nb::borrow<nb::object>(pair[0]);
                nb::object value = nb::borrow<nb::object>(pair[1]);
                values[key]      = value;
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

    nb::tuple python_callable_parameter_names(nb::handle callable) {
        nb::gil_scoped_acquire guard;
        if (!callable.is_valid() || callable.is_none()) { return nb::make_tuple(); }
        nb::object inspect = nb::module_::import_("inspect");
        return nb::tuple(inspect.attr("signature")(nb::borrow(callable)).attr("parameters").attr("keys")());
    }

    nb::object call_python_callable(nb::handle callable, nb::handle kwargs) {
        nb::gil_scoped_acquire guard;
        if (!callable.is_valid() || callable.is_none()) { return nb::none(); }
        nb::tuple args   = nb::make_tuple();
        PyObject *result = PyObject_Call(callable.ptr(), args.ptr(), kwargs.ptr());
        if (result == nullptr) { throw nb::python_error(); }
        return nb::steal<nb::object>(result);
    }

    nb::object call_python_node_eval(nb::handle signature, nb::handle callable, nb::handle kwargs) {
        nb::gil_scoped_acquire guard;
        if (!callable.is_valid() || callable.is_none()) { return nb::none(); }

        nb::object context_inputs = nb::borrow(signature).attr("context_inputs");
        if (context_inputs.is_none() || nb::len(context_inputs) == 0) {
            return call_python_callable(callable, kwargs);
        }

        nb::object contextlib = nb::module_::import_("contextlib");
        nb::object stack      = contextlib.attr("ExitStack")();
        nb::object enter      = stack.attr("__enter__");
        nb::object exit       = stack.attr("__exit__");
        nb::object manager_fn = stack.attr("enter_context");
        nb::tuple  no_error   = nb::make_tuple(nb::none(), nb::none(), nb::none());

        enter();
        try {
            for (auto key : nb::borrow(context_inputs)) {
                nb::object key_obj = nb::borrow<nb::object>(key);
                if (!PyMapping_HasKey(kwargs.ptr(), key_obj.ptr())) { continue; }

                nb::object handle = nb::steal<nb::object>(PyObject_GetItem(kwargs.ptr(), key_obj.ptr()));
                if (handle.is_none() || !nb::cast<bool>(handle.attr("valid"))) { continue; }
                manager_fn(handle.attr("value"));
            }

            nb::tuple args   = nb::make_tuple();
            PyObject *result = PyObject_Call(callable.ptr(), args.ptr(), kwargs.ptr());
            if (result == nullptr) { throw nb::python_error(); }

            static_cast<void>(nb::steal<nb::object>(PyObject_CallObject(exit.ptr(), no_error.ptr())));
            return nb::steal<nb::object>(result);
        } catch (...) {
            try {
                static_cast<void>(nb::steal<nb::object>(PyObject_CallObject(exit.ptr(), no_error.ptr())));
            } catch (...) {}
            throw;
        }
    }

    nb::object call_python_callable_with_subset(nb::handle callable, nb::handle kwargs, nb::handle parameter_names) {
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

    void register_python_runtime_bindings(nb::module_ &m) {
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
            .def(
                "add_before_evaluation_notification",
                [](const EvaluationEngineApi &self, nb::callable fn) {
                    self.add_before_evaluation_notification([fn = std::move(fn)]() {
                        nb::gil_scoped_acquire guard;
                        fn();
                    });
                },
                "fn"_a)
            .def(
                "add_after_evaluation_notification",
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

        auto graph_cls = nb::class_<PythonGraphHandle>(m, "Graph")
            .def_prop_ro("graph_id", &PythonGraphHandle::graph_id)
            .def_prop_ro("parent_node", &PythonGraphHandle::parent_node)
            .def_prop_ro("label", &PythonGraphHandle::label)
            .def_prop_ro("evaluation_clock", &PythonGraphHandle::evaluation_clock)
            .def_prop_ro("evaluation_engine_api", &PythonGraphHandle::evaluation_engine_api)
            .def_prop_ro("traits", &PythonGraphHandle::traits)
            .def("schedule_node", &PythonGraphHandle::schedule_node, "node_ndx"_a, "when"_a, "force_set"_a = false);
        m.attr("_PythonGraphHandle") = m.attr("Graph");

        nb::class_<NodeSchedulerHandle>(m, "_NodeSchedulerHandle")
            .def_prop_ro("next_scheduled_time", &NodeSchedulerHandle::next_scheduled_time)
            .def_prop_ro("requires_scheduling", &NodeSchedulerHandle::requires_scheduling)
            .def_prop_ro("is_scheduled", &NodeSchedulerHandle::is_scheduled)
            .def_prop_ro("is_scheduled_now", &NodeSchedulerHandle::is_scheduled_now)
            .def("has_tag", &NodeSchedulerHandle::has_tag, "tag"_a)
            .def("pop_tag", nb::overload_cast<const std::string &>(&NodeSchedulerHandle::pop_tag, nb::const_), "tag"_a)
            .def("pop_tag", nb::overload_cast<const std::string &, engine_time_t>(&NodeSchedulerHandle::pop_tag, nb::const_),
                 "tag"_a, "default"_a)
            .def("schedule",
                 nb::overload_cast<engine_time_t, std::optional<std::string>, bool>(&NodeSchedulerHandle::schedule, nb::const_),
                 "when"_a, "tag"_a = nb::none(), "on_wall_clock"_a = false)
            .def("schedule",
                 nb::overload_cast<engine_time_delta_t, std::optional<std::string>, bool>(&NodeSchedulerHandle::schedule,
                                                                                          nb::const_),
                 "when"_a, "tag"_a = nb::none(), "on_wall_clock"_a = false)
            .def("un_schedule", nb::overload_cast<const std::string &>(&NodeSchedulerHandle::un_schedule, nb::const_), "tag"_a)
            .def("un_schedule", nb::overload_cast<>(&NodeSchedulerHandle::un_schedule, nb::const_))
            .def("reset", &NodeSchedulerHandle::reset)
            .def("advance", &NodeSchedulerHandle::advance);

        nb::class_<TimeSeriesReference>(m, "TimeSeriesReference")
            .def("__str__", &TimeSeriesReference::to_string)
            .def("__repr__", &TimeSeriesReference::to_string)
            .def(
                "__eq__",
                [](const TimeSeriesReference &self, nb::object other) {
                    return other.is_none()
                               ? false
                               : nb::isinstance<TimeSeriesReference>(other) && self == nb::cast<TimeSeriesReference>(other);
                },
                "other"_a, nb::is_operator())
            .def("bind_input", &V2PythonReferenceSupport::bind_input, "input_"_a)
            .def_prop_ro("has_output", &TimeSeriesReference::is_peered)
            .def_prop_ro("has_peer", &TimeSeriesReference::is_peered)
            .def_prop_ro("is_empty", &TimeSeriesReference::is_empty)
            .def_prop_ro("is_bound", &TimeSeriesReference::is_peered)
            .def_prop_ro("is_unbound", &TimeSeriesReference::is_non_peered)
            .def_prop_ro("is_valid", &TimeSeriesReference::is_valid)
            .def_prop_ro("valid", &TimeSeriesReference::is_valid)
            .def_prop_ro("output", &V2PythonReferenceSupport::output)
            .def_prop_ro("items",
                         [](const TimeSeriesReference &self) {
                             nb::list out;
                             if (self.is_non_peered()) {
                                 for (const auto &item : self.items()) { out.append(nb::cast(item)); }
                             }
                             return nb::tuple(out);
                         })
            .def("__getitem__", &TimeSeriesReference::operator[])
            .def_static("make", &V2PythonReferenceSupport::make, "ts"_a = nb::none(), "from_items"_a = nb::none());

        auto time_series_cls = nb::class_<PythonTimeSeriesHandle>(m, "TimeSeriesHandle")
            .def("__bool__", &PythonTimeSeriesHandle::truthy)
            .def("__getitem__", &PythonTimeSeriesHandle::get_item, "key"_a)
            .def("__delitem__", &PythonTimeSeriesHandle::del_item, "key"_a)
            .def("__getattr__", &PythonTimeSeriesHandle::get_attr, "key"_a)
            .def("__contains__", &PythonTimeSeriesHandle::contains, "item"_a)
            .def("__len__", &PythonTimeSeriesHandle::len)
            .def(
                "__iter__",
                [](const PythonTimeSeriesHandle &self) { return self.is_dict() ? nb::iter(self.keys()) : nb::iter(self.values()); })
            .def_prop_ro("owning_node", &PythonTimeSeriesHandle::owning_node)
            .def_prop_ro("owning_graph", &PythonTimeSeriesHandle::owning_graph)
            .def_prop_ro("has_owning_node", &PythonTimeSeriesHandle::has_owning_node)
            .def_prop_ro("parent_input", &PythonTimeSeriesHandle::parent_input)
            .def_prop_ro("has_parent_input", &PythonTimeSeriesHandle::has_parent_input)
            .def_prop_ro("parent_output", &PythonTimeSeriesHandle::parent_output)
            .def_prop_ro("has_parent_output", &PythonTimeSeriesHandle::has_parent_output)
            .def_prop_rw("value", &PythonTimeSeriesHandle::value, &PythonTimeSeriesHandle::set_value)
            .def_prop_ro("delta_value", &PythonTimeSeriesHandle::delta_value)
            .def_prop_ro("is_reference", &PythonTimeSeriesHandle::is_reference)
            .def_prop_ro("modified", &PythonTimeSeriesHandle::modified)
            .def_prop_ro("valid", &PythonTimeSeriesHandle::valid)
            .def_prop_ro("all_valid", &PythonTimeSeriesHandle::all_valid)
            .def_prop_ro("last_modified_time", &PythonTimeSeriesHandle::last_modified_time)
            .def_prop_ro("key_set", &PythonTimeSeriesHandle::key_set)
            .def("get", &PythonTimeSeriesHandle::get, "key"_a, "default"_a = nb::none())
            .def("get_or_create", &PythonTimeSeriesHandle::get_or_create, "key"_a)
            .def("create", &PythonTimeSeriesHandle::create, "key"_a)
            .def("on_key_removed", &PythonTimeSeriesHandle::on_key_removed, "key"_a)
            .def("get_ref", &PythonTimeSeriesHandle::get_ref, "key"_a, "requester"_a = nb::none())
            .def("release_ref", &PythonTimeSeriesHandle::release_ref, "key"_a, "requester"_a = nb::none())
            .def("get_contains_output", &PythonTimeSeriesHandle::get_contains_output, "item"_a, "requester"_a)
            .def("release_contains_output", &PythonTimeSeriesHandle::release_contains_output, "item"_a, "requester"_a)
            .def("is_empty_output", &PythonTimeSeriesHandle::is_empty_output)
            .def_prop_ro("bound", &PythonTimeSeriesHandle::bound)
            .def_prop_ro("has_peer", &PythonTimeSeriesHandle::has_peer)
            .def_prop_ro("has_output", &PythonTimeSeriesHandle::has_output)
            .def_prop_ro("output", &PythonTimeSeriesHandle::output)
            .def_prop_ro("reference_output", &PythonTimeSeriesHandle::reference_output)
            .def("bind_output", &PythonTimeSeriesHandle::bind_output, "output"_a)
            .def("un_bind_output", &PythonTimeSeriesHandle::un_bind_output, "unbind_refs"_a = false)
            .def("key_from_value", &PythonTimeSeriesHandle::key_from_value, "value"_a)
            .def("keys", &PythonTimeSeriesHandle::keys)
            .def("valid_keys", &PythonTimeSeriesHandle::valid_keys)
            .def("modified_keys", &PythonTimeSeriesHandle::modified_keys)
            .def("values", &PythonTimeSeriesHandle::values)
            .def("added", &PythonTimeSeriesHandle::added)
            .def("removed", &PythonTimeSeriesHandle::removed)
            .def(
                "add", [](const PythonTimeSeriesHandle &self, nb::handle item) { self.add_set_item(item); }, "item"_a)
            .def(
                "remove", [](const PythonTimeSeriesHandle &self, nb::handle item) { self.remove_set_item(item); }, "item"_a)
            .def("clear", &PythonTimeSeriesHandle::clear)
            .def("valid_values", &PythonTimeSeriesHandle::valid_values)
            .def("modified_values", &PythonTimeSeriesHandle::modified_values)
            .def("items", &PythonTimeSeriesHandle::items)
            .def("added_keys", &PythonTimeSeriesHandle::added_keys)
            .def("added_values", &PythonTimeSeriesHandle::added_values)
            .def("added_items", &PythonTimeSeriesHandle::added_items)
            .def("removed_keys", &PythonTimeSeriesHandle::removed_keys)
            .def("removed_values", &PythonTimeSeriesHandle::removed_values)
            .def("removed_items", &PythonTimeSeriesHandle::removed_items)
            .def("valid_items", &PythonTimeSeriesHandle::valid_items)
            .def("modified_items", &PythonTimeSeriesHandle::modified_items)
            .def("make_active", &PythonTimeSeriesHandle::make_active)
            .def("make_passive", &PythonTimeSeriesHandle::make_passive)
            .def_prop_ro("active", &PythonTimeSeriesHandle::active)
            .def_prop_ro("as_schema", &PythonTimeSeriesHandle::as_schema)
            .def_prop_ro("has_removed_value", &PythonTimeSeriesHandle::has_removed_value)
            .def_prop_ro("removed_value", &PythonTimeSeriesHandle::removed_value)
            .def("can_apply_result", &PythonTimeSeriesHandle::can_apply_result, "value"_a)
            .def("apply_result", &PythonTimeSeriesHandle::apply_result, "value"_a)
            .def("__repr__", &PythonTimeSeriesHandle::repr);
        m.attr("_PythonTimeSeriesHandle") = m.attr("TimeSeriesHandle");
        m.attr("TimeSeriesInput")         = m.attr("TimeSeriesHandle");
        m.attr("TimeSeriesOutput")        = m.attr("TimeSeriesHandle");
        m.attr("TimeSeriesValueInput")    = m.attr("TimeSeriesHandle");
        m.attr("TimeSeriesValueOutput")   = m.attr("TimeSeriesHandle");
        m.attr("TimeSeriesReferenceInput")  = m.attr("TimeSeriesHandle");
        m.attr("TimeSeriesReferenceOutput") = m.attr("TimeSeriesHandle");
        m.attr("TimeSeriesListInput")     = m.attr("TimeSeriesHandle");
        m.attr("TimeSeriesListOutput")    = m.attr("TimeSeriesHandle");
        m.attr("TimeSeriesDictInput")     = m.attr("TimeSeriesHandle");
        m.attr("TimeSeriesDictOutput")    = m.attr("TimeSeriesHandle");
        m.attr("TimeSeriesBundleInput")   = m.attr("TimeSeriesHandle");
        m.attr("TimeSeriesBundleOutput")  = m.attr("TimeSeriesHandle");
        m.attr("TimeSeriesSetInput")      = m.attr("TimeSeriesHandle");
        m.attr("TimeSeriesSetOutput")     = m.attr("TimeSeriesHandle");

        auto node_cls = nb::class_<PythonNodeHandle>(m, "Node")
            .def_prop_ro("node_ndx", &PythonNodeHandle::node_ndx)
            .def_prop_ro("owning_graph_id", &PythonNodeHandle::owning_graph_id)
            .def_prop_ro("node_id", &PythonNodeHandle::node_id)
            .def_prop_ro("signature", &PythonNodeHandle::signature)
            .def_prop_ro("scalars", &PythonNodeHandle::scalars)
            .def_prop_ro("graph", &PythonNodeHandle::graph)
            .def_prop_ro("input", &PythonNodeHandle::input)
            .def_prop_ro("output", &PythonNodeHandle::output)
            .def_prop_ro("error_output", &PythonNodeHandle::error_output)
            .def_prop_ro("recordable_state", &PythonNodeHandle::recordable_state)
            .def_prop_ro("scheduler", &PythonNodeHandle::scheduler)
            .def_prop_ro("has_scheduler", &PythonNodeHandle::has_scheduler)
            .def_prop_ro("has_input", &PythonNodeHandle::has_input)
            .def_prop_ro("has_output", &PythonNodeHandle::has_output)
            .def_prop_ro("has_error_output", &PythonNodeHandle::has_error_output)
            .def("notify", &PythonNodeHandle::notify, "modified_time"_a = nb::none())
            .def("notify_next_cycle", &PythonNodeHandle::notify_next_cycle)
            .def("__repr__", &PythonNodeHandle::repr)
            .def("__str__", &PythonNodeHandle::repr);
        m.attr("_PythonNodeHandle") = m.attr("Node");
        m.attr("NestedNode")        = m.attr("Node");
        m.attr("PushQueueNode")     = m.attr("Node");
    }
}  // namespace hgraph
