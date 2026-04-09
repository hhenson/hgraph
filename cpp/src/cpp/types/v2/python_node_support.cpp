#include <hgraph/types/v2/python_node_support.h>

#include <hgraph/types/constants.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/traits.h>
#include <hgraph/types/v2/evaluation_clock.h>
#include <hgraph/types/v2/evaluation_engine.h>
#include <hgraph/types/v2/graph.h>
#include <hgraph/types/v2/node.h>
#include <hgraph/types/v2/ref.h>

#include <fmt/format.h>

#include <algorithm>
#include <cassert>
#include <optional>
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

        [[nodiscard]] const TSMeta *index_schema_or_throw(const TSMeta *schema, size_t)
        {
            if (schema == nullptr || schema->kind != TSKind::TSL) {
                throw std::logic_error("v2 Python time-series list access requires a TSL schema");
            }

            if (const TSMeta *element = schema->element_ts(); element != nullptr) { return element; }
            throw std::logic_error("v2 Python time-series list schema is missing its element type");
        }

        [[nodiscard]] const TSMeta *value_schema_or_throw(const TSMeta *schema)
        {
            if (schema == nullptr || schema->kind != TSKind::TSD) {
                throw std::logic_error("v2 Python time-series dict access requires a TSD schema");
            }

            if (const TSMeta *element = schema->element_ts(); element != nullptr) { return element; }
            throw std::logic_error("v2 Python time-series dict schema is missing its value type");
        }

        void mark_output_modified(const TSOutputView &view, engine_time_t evaluation_time)
        {
            LinkedTSContext context = view.linked_context();
            if (context.ts_state == nullptr) {
                throw std::logic_error("v2 Python output mutation requires a linked output state");
            }
            context.ts_state->mark_modified(evaluation_time);
        }

        [[nodiscard]] TSOutputView output_view_from_context(const LinkedTSContext &context, engine_time_t evaluation_time)
        {
            TSViewContext view_context{context.schema, context.value_dispatch, context.ts_dispatch, context.value_data, context.ts_state};
            return TSOutputView{view_context, TSViewContext::none(), evaluation_time, nullptr, &detail::default_output_view_ops()};
        }

        [[nodiscard]] nb::object removed_type()
        {
            static nb::object type = nb::module_::import_("hgraph").attr("Removed");
            return nb::borrow(type);
        }

        [[nodiscard]] nb::object set_delta_builder()
        {
            static nb::object fn = nb::module_::import_("hgraph").attr("set_delta");
            return nb::borrow(fn);
        }

        [[nodiscard]] const RefLinkState *switching_ref_state(const TSViewContext &context) noexcept
        {
            const BaseState *state = context.ts_state;
            while (state != nullptr && state->storage_kind == TSStorageKind::TargetLink) {
                const LinkedTSContext *target = state->linked_target();
                state = target != nullptr ? target->ts_state : nullptr;
            }
            return state != nullptr && state->storage_kind == TSStorageKind::RefLink
                       ? static_cast<const RefLinkState *>(state)
                       : nullptr;
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
            struct PathStep
            {
                enum class Kind : uint8_t { Field, Index, Key };

                Kind kind{Kind::Field};
                std::string field_name;
                size_t index{0};
                Value key;

                [[nodiscard]] static PathStep field(std::string name)
                {
                    PathStep step;
                    step.kind = Kind::Field;
                    step.field_name = std::move(name);
                    return step;
                }

                [[nodiscard]] static PathStep index_at(size_t value)
                {
                    PathStep step;
                    step.kind = Kind::Index;
                    step.index = value;
                    return step;
                }

                [[nodiscard]] static PathStep key_for(const View &value)
                {
                    PathStep step;
                    step.kind = Kind::Key;
                    step.key = value.clone();
                    return step;
                }

                [[nodiscard]] static PathStep key_from_python(const TSMeta *schema, const nb::handle &value)
                {
                    const auto *key_schema = schema != nullptr ? schema->key_type() : nullptr;
                    if (key_schema == nullptr) {
                        throw std::logic_error("v2 Python time-series dict access requires a key schema");
                    }

                    PathStep step;
                    step.kind = Kind::Key;
                    step.key = Value{key_schema};
                    step.key.from_python(nb::borrow<nb::object>(value));
                    return step;
                }
            };

            PythonTimeSeriesHandle(Node *node,
                                   TSInput *input,
                                   TSOutput *output,
                                   const TSMeta *schema,
                                   std::vector<PathStep> path_steps = {},
                                   std::optional<LinkedTSContext> bound_output = std::nullopt,
                                   engine_time_t fixed_evaluation_time = MIN_DT)
                : m_node(node),
                  m_input(input),
                  m_output(output),
                  m_schema(schema),
                  m_path_steps(std::move(path_steps)),
                  m_bound_output(std::move(bound_output)),
                  m_fixed_evaluation_time(fixed_evaluation_time)
            {
            }

            explicit PythonTimeSeriesHandle(LinkedTSContext bound_output, engine_time_t fixed_evaluation_time = MIN_DT)
                : m_schema(bound_output.schema),
                  m_bound_output(std::move(bound_output)),
                  m_fixed_evaluation_time(fixed_evaluation_time)
            {
            }

            [[nodiscard]] bool truthy() const noexcept
            {
                return m_schema != nullptr;
            }

            [[nodiscard]] nb::object value() const
            {
                if (is_bundle()) { return bundle_python_value(false); }
                if (is_list()) { return list_python_value(false); }
                if (is_dict()) { return dict_python_value(); }
                if (is_set()) { return current_value().to_python(); }
                return current_value().to_python();
            }

            [[nodiscard]] nb::object delta_value() const
            {
                if (is_bundle()) { return bundle_python_value(true); }
                if (is_list()) { return list_python_value(true); }
                if (is_dict()) { return dict_python_delta_value(); }
                if (is_set()) { return set_python_delta_value(); }
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

            [[nodiscard]] bool is_list() const noexcept
            {
                return m_schema != nullptr && m_schema->kind == TSKind::TSL;
            }

            [[nodiscard]] bool is_dict() const noexcept
            {
                return m_schema != nullptr && m_schema->kind == TSKind::TSD;
            }

            [[nodiscard]] bool is_set() const noexcept
            {
                return m_schema != nullptr && m_schema->kind == TSKind::TSS;
            }

            [[nodiscard]] bool is_reference() const noexcept
            {
                return m_schema != nullptr && m_schema->kind == TSKind::REF;
            }

            [[nodiscard]] PythonTimeSeriesHandle field(const std::string &field_name) const
            {
                const TSMeta *field_type = field_schema_or_throw(m_schema, field_name);
                std::vector<PathStep> path_steps = m_path_steps;
                path_steps.push_back(PathStep::field(field_name));
                return PythonTimeSeriesHandle{
                    m_node, m_input, m_output, field_type, std::move(path_steps), m_bound_output, m_fixed_evaluation_time};
            }

            [[nodiscard]] PythonTimeSeriesHandle index(size_t item_index) const
            {
                const TSMeta *element_type = index_schema_or_throw(m_schema, item_index);
                std::vector<PathStep> path_steps = m_path_steps;
                path_steps.push_back(PathStep::index_at(item_index));
                return PythonTimeSeriesHandle{
                    m_node, m_input, m_output, element_type, std::move(path_steps), m_bound_output, m_fixed_evaluation_time};
            }

            [[nodiscard]] PythonTimeSeriesHandle key_item(const nb::handle &key) const
            {
                const TSMeta *value_type = value_schema_or_throw(m_schema);
                std::vector<PathStep> path_steps = m_path_steps;
                path_steps.push_back(PathStep::key_from_python(m_schema, key));
                return PythonTimeSeriesHandle{
                    m_node, m_input, m_output, value_type, std::move(path_steps), m_bound_output, m_fixed_evaluation_time};
            }

            [[nodiscard]] PythonTimeSeriesHandle key_item(const View &key) const
            {
                const TSMeta *value_type = value_schema_or_throw(m_schema);
                std::vector<PathStep> path_steps = m_path_steps;
                path_steps.push_back(PathStep::key_for(key));
                return PythonTimeSeriesHandle{
                    m_node, m_input, m_output, value_type, std::move(path_steps), m_bound_output, m_fixed_evaluation_time};
            }

            [[nodiscard]] nb::object get_item(const nb::handle &key) const
            {
                if (is_bundle()) { return nb::cast(field(nb::cast<std::string>(key))); }
                if (is_list()) { return nb::cast(index(nb::cast<size_t>(key))); }
                if (is_dict()) { return nb::cast(key_item(key)); }
                throw std::logic_error("v2 Python time-series __getitem__ requires a bundle, list, or dict schema");
            }

            [[nodiscard]] nb::tuple keys() const
            {
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

            [[nodiscard]] nb::list values() const
            {
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
                    if (m_input != nullptr) {
                        for (const View &item : input_view().as_set().values()) { result.append(item.to_python()); }
                    } else {
                        for (const View &item : output_view().as_set().values()) { result.append(item.to_python()); }
                    }
                    return result;
                }
                throw std::logic_error("v2 Python time-series values() requires a bundle, list, dict, or set schema");
            }

            [[nodiscard]] nb::list added() const
            {
                if (!is_set()) { throw std::logic_error("v2 Python time-series added() requires a TSS schema"); }

                nb::list result;
                if (m_input != nullptr) {
                    for (const View &item : input_view().as_set().added_values()) { result.append(item.to_python()); }
                } else {
                    for (const View &item : output_view().as_set().added_values()) { result.append(item.to_python()); }
                }
                return result;
            }

            [[nodiscard]] nb::list removed() const
            {
                if (!is_set()) { throw std::logic_error("v2 Python time-series removed() requires a TSS schema"); }

                nb::list result;
                if (m_input != nullptr) {
                    for (const View &item : input_view().as_set().removed_values()) { result.append(item.to_python()); }
                } else {
                    for (const View &item : output_view().as_set().removed_values()) { result.append(item.to_python()); }
                }
                return result;
            }

            [[nodiscard]] nb::list items() const
            {
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
                        for (size_t i = 0; i < list.size(); ++i) {
                            result.append(nb::make_tuple(nb::int_(i), nb::cast(index(i))));
                        }
                    } else {
                        const auto list = output_view().as_list();
                        for (size_t i = 0; i < list.size(); ++i) {
                            result.append(nb::make_tuple(nb::int_(i), nb::cast(index(i))));
                        }
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

            [[nodiscard]] nb::list valid_items() const
            {
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

            [[nodiscard]] nb::list modified_items() const
            {
                nb::list result;
                if (is_bundle()) {
                    for (size_t i = 0; i < m_schema->data.tsb.field_count; ++i) {
                        const auto child = field(m_schema->data.tsb.fields[i].name);
                        if (child.modified()) {
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
                            if (child.modified()) { result.append(nb::make_tuple(nb::int_(i), nb::cast(child))); }
                        }
                    } else {
                        const auto list = output_view().as_list();
                        for (size_t i = 0; i < list.size(); ++i) {
                            const auto child = index(i);
                            if (child.modified()) { result.append(nb::make_tuple(nb::int_(i), nb::cast(child))); }
                        }
                    }
                    return result;
                }
                if (is_dict()) {
                    const auto append_modified_items = [this, &result](const auto &dict) {
                        for (const auto &[key, value] : dict.items()) {
                            static_cast<void>(value);
                            const auto child = key_item(key);
                            if (child.modified()) { result.append(nb::make_tuple(key.to_python(), nb::cast(child))); }
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

            [[nodiscard]] nb::tuple valid_keys() const
            {
                nb::list result;
                for (const auto &item : valid_items()) { result.append(nb::cast<nb::tuple>(item)[0]); }
                return nb::tuple(result);
            }

            [[nodiscard]] nb::tuple modified_keys() const
            {
                nb::list result;
                for (const auto &item : modified_items()) { result.append(nb::cast<nb::tuple>(item)[0]); }
                return nb::tuple(result);
            }

            [[nodiscard]] nb::list valid_values() const
            {
                nb::list result;
                for (const auto &item : valid_items()) { result.append(nb::cast<nb::tuple>(item)[1]); }
                return result;
            }

            [[nodiscard]] nb::list modified_values() const
            {
                nb::list result;
                for (const auto &item : modified_items()) { result.append(nb::cast<nb::tuple>(item)[1]); }
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
                if (value.is_none()) { return; }
                set_value(value);
            }

            void set_value(nb::handle value) const
            {
                ensure_output();
                auto view = output_view();
                if (is_bundle() && !value.is_none()) {
                    set_bundle_value(value);
                    return;
                }
                if (is_list() && !value.is_none()) {
                    set_list_value(value);
                    return;
                }
                if (is_dict() && !value.is_none()) {
                    set_dict_value(view, value);
                    return;
                }
                if (is_set() && !value.is_none()) {
                    set_set_value(view, value);
                    return;
                }
                view.value().from_python(nb::borrow<nb::object>(value));
                mark_output_modified(view, evaluation_time());
            }

            [[nodiscard]] std::string repr() const
            {
                return fmt::format(
                    "v2._PythonTimeSeriesHandle@{:p}[schema={}]",
                    static_cast<const void *>(this),
                    schema_kind_label());
            }

          private:
            [[nodiscard]] engine_time_t evaluation_time() const
            {
                return m_node != nullptr ? m_node->evaluation_time() : m_fixed_evaluation_time;
            }

            [[nodiscard]] static nb::object remove_sentinel()
            {
                return get_remove();
            }

            [[nodiscard]] static nb::object remove_if_exists_sentinel()
            {
                return get_remove_if_exists();
            }

            void set_bundle_value(nb::handle value) const
            {
                if (m_schema == nullptr || m_schema->kind != TSKind::TSB) {
                    throw std::logic_error("v2 Python bundle output mutation requires a TSB schema");
                }

                const nb::object python_type = m_schema->python_type();
                if (python_type.is_valid() && !python_type.is_none() && nb::isinstance(value, python_type)) {
                    for (size_t i = 0; i < m_schema->data.tsb.field_count; ++i) {
                        const std::string_view field_name = m_schema->data.tsb.fields[i].name;
                        nb::object attr = nb::getattr(value, field_name.data(), nb::none());
                        if (!attr.is_none()) { field(std::string(field_name)).set_value(attr); }
                    }
                    return;
                }

                nb::object items = nb::hasattr(value, "items") ? nb::getattr(value, "items")() : nb::borrow<nb::object>(value);
                for (auto pair : nb::iter(items)) {
                    nb::object field_value = nb::borrow<nb::object>(pair[1]);
                    if (!field_value.is_none()) { field(nb::cast<std::string>(pair[0])).set_value(field_value); }
                }
            }

            void set_list_value(nb::handle value) const
            {
                if (m_schema == nullptr || m_schema->kind != TSKind::TSL) {
                    throw std::logic_error("v2 Python list output mutation requires a TSL schema");
                }

                if (nb::isinstance<nb::tuple>(value) || nb::isinstance<nb::list>(value)) {
                    const size_t size = nb::len(value);
                    for (size_t i = 0; i < size; ++i) {
                        nb::object item = nb::borrow<nb::object>(value[i]);
                        if (!item.is_none()) { index(i).set_value(item); }
                    }
                    return;
                }

                if (nb::isinstance<nb::dict>(value)) {
                    for (auto [key, item] : nb::cast<nb::dict>(value)) {
                        nb::object item_object = nb::borrow<nb::object>(item);
                        if (!item_object.is_none()) { index(nb::cast<size_t>(key)).set_value(item_object); }
                    }
                    return;
                }

                throw std::runtime_error("Invalid value type for v2 Python list output");
            }

            void set_dict_value(const TSOutputView &view, nb::handle value) const
            {
                if (m_schema == nullptr || m_schema->kind != TSKind::TSD) {
                    throw std::logic_error("v2 Python dict output mutation requires a TSD schema");
                }

                const value::TypeMeta *key_schema = m_schema->key_type();
                const TSMeta *value_ts_schema = m_schema->element_ts();
                const value::TypeMeta *mapped_schema = value_ts_schema != nullptr ? value_ts_schema->value_type : nullptr;
                if (key_schema == nullptr || mapped_schema == nullptr) {
                    throw std::logic_error("v2 Python dict output mutation requires key and value schemas");
                }

                if (!view.valid() && !nb::cast<bool>(nb::bool_(value))) {
                    mark_output_modified(view, evaluation_time());
                    return;
                }

                auto item_attr = nb::getattr(value, "items", nb::none());
                nb::iterator items = item_attr.is_none() ? nb::iter(value) : nb::iter(item_attr());
                auto mutation = view.value().as_map().begin_mutation();
                bool changed = false;

                for (const auto &kv : items) {
                    nb::object entry_value = nb::borrow<nb::object>(kv[1]);
                    if (entry_value.is_none()) { continue; }

                    Value key_value(*key_schema);
                    key_value.reset();
                    key_value.from_python(nb::borrow<nb::object>(kv[0]));

                    if (entry_value.is(remove_sentinel()) || entry_value.is(remove_if_exists_sentinel())) {
                        const bool removed = mutation.remove(key_value.view());
                        if (!removed && entry_value.is(remove_sentinel())) {
                            throw nb::key_error("TSD key not found for REMOVE");
                        }
                        changed = changed || removed;
                        continue;
                    }

                    Value mapped_value(*mapped_schema);
                    mapped_value.reset();
                    mapped_value.from_python(entry_value);
                    mutation.set(key_value.view(), mapped_value.view());
                    changed = true;
                }

                if (changed) { mark_output_modified(view, evaluation_time()); }
            }

            void set_set_value(const TSOutputView &view, nb::handle value) const
            {
                if (m_schema == nullptr || m_schema->kind != TSKind::TSS) {
                    throw std::logic_error("v2 Python set output mutation requires a TSS schema");
                }

                auto set_view = view.value().as_set();
                const value::TypeMeta *element_schema = set_view.element_schema();
                if (element_schema == nullptr) {
                    throw std::logic_error("v2 Python set output mutation requires an element schema");
                }

                auto mutation = set_view.begin_mutation();
                bool changed = false;

                const auto convert_value = [element_schema](const nb::handle &item) {
                    Value element(*element_schema);
                    element.reset();
                    element.from_python(nb::borrow<nb::object>(item));
                    return element;
                };

                const auto apply_added = [&](const nb::handle &item) {
                    Value element = convert_value(item);
                    changed = mutation.add(element.view()) || changed;
                };

                const auto apply_removed = [&](const nb::handle &item) {
                    Value element = convert_value(item);
                    changed = mutation.remove(element.view()) || changed;
                };

                if (nb::hasattr(value, "added") && nb::hasattr(value, "removed")) {
                    for (auto item : nb::iter(nb::getattr(value, "added"))) { apply_added(nb::borrow<nb::object>(item)); }
                    for (auto item : nb::iter(nb::getattr(value, "removed"))) { apply_removed(nb::borrow<nb::object>(item)); }
                } else if (nb::isinstance<nb::frozenset>(value)) {
                    std::vector<Value> replacement;
                    replacement.reserve(nb::len(value));
                    for (auto item : nb::iter(value)) { replacement.push_back(convert_value(nb::borrow<nb::object>(item))); }

                    std::vector<Value> existing_items;
                    for (const View &existing : set_view.values()) { existing_items.push_back(existing.clone()); }

                    for (const Value &existing : existing_items) {
                        const bool keep =
                            std::any_of(replacement.begin(),
                                        replacement.end(),
                                        [&](const Value &candidate) { return existing.view() == candidate.view(); });
                        if (!keep) { changed = mutation.remove(existing.view()) || changed; }
                    }

                    for (const Value &item : replacement) {
                        if (!set_view.contains(item.view())) { changed = mutation.add(item.view()) || changed; }
                    }
                } else if (nb::isinstance<nb::set>(value) || nb::isinstance<nb::list>(value) || nb::isinstance<nb::tuple>(value)) {
                    for (auto item : nb::iter(value)) {
                        nb::object item_object = nb::borrow<nb::object>(item);
                        if (nb::isinstance(item_object, removed_type())) {
                            apply_removed(nb::getattr(item_object, "item"));
                        } else {
                            apply_added(item_object);
                        }
                    }
                } else {
                    throw std::runtime_error("Invalid value type for v2 Python set output");
                }

                if (changed || !view.valid()) { mark_output_modified(view, evaluation_time()); }
            }

            [[nodiscard]] nb::object bundle_python_value(bool delta) const
            {
                if (delta) {
                    nb::dict out;
                    for (size_t i = 0; i < m_schema->data.tsb.field_count; ++i) {
                        const std::string_view field_name = m_schema->data.tsb.fields[i].name;
                        const auto child = field(std::string(field_name));
                        if (!child.modified() || !child.valid()) { continue; }

                        nb::object value = child.delta_value();
                        if (!value.is_none()) { out[nb::str(field_name.data(), field_name.size())] = value; }
                    }
                    return out;
                }

                nb::dict out;
                for (size_t i = 0; i < m_schema->data.tsb.field_count; ++i) {
                    const std::string_view field_name = m_schema->data.tsb.fields[i].name;
                    const auto child = field(std::string(field_name));
                    if (!child.valid()) { continue; }

                    nb::object value = child.value();
                    if (!value.is_none()) { out[nb::str(field_name.data(), field_name.size())] = value; }
                }
                return out;
            }

            [[nodiscard]] nb::object list_python_value(bool delta) const
            {
                if (delta) {
                    if (m_input != nullptr) {
                        const auto view = input_view();
                        const BaseState *state = view.context_ref().ts_state;
                        if (state != nullptr && state->storage_kind != TSStorageKind::Native) {
                            if (const LinkedTSContext *target = state->linked_target();
                                target != nullptr && target->ts_state != nullptr &&
                                target->ts_state->resolved_state() != nullptr &&
                                target->ts_state->resolved_state()->storage_kind == TSStorageKind::Native) {
                                nb::dict out;
                                const auto list = current_value().as_list();
                                const auto *target_state = static_cast<const TSLState *>(target->ts_state->resolved_state());
                                for (const size_t slot : target_state->modified_children) {
                                    if (slot >= list.size()) { continue; }
                                    const View value = list.at(slot);
                                    if (value.has_value()) { out[nb::int_(slot)] = value.to_python(); }
                                }
                                return out;
                            }
                        }
                    }

                    nb::dict out;
                    const size_t size = m_input != nullptr ? input_view().as_list().size() : output_view().as_list().size();
                    for (size_t i = 0; i < size; ++i) {
                        const auto child = index(i);
                        if (!child.modified()) { continue; }
                        out[nb::int_(i)] = child.delta_value();
                    }
                    return out;
                }

                nb::list out;
                const size_t size = m_input != nullptr ? input_view().as_list().size() : output_view().as_list().size();
                for (size_t i = 0; i < size; ++i) {
                    const auto child = index(i);
                    out.append(child.valid() ? child.value() : nb::none());
                }
                return nb::tuple(out);
            }

            [[nodiscard]] nb::object dict_python_value() const
            {
                nb::dict out;
                auto map = current_value().as_map();
                constexpr size_t no_slot = static_cast<size_t>(-1);
                for (size_t slot = map.first_live_slot(); slot != no_slot; slot = map.next_live_slot(slot)) {
                    const View key = map.delta().key_at_slot(slot);
                    const View value = map.at(key);
                    if (value.has_value()) { out[key.to_python()] = value.to_python(); }
                }
                return out;
            }

            [[nodiscard]] nb::object dict_python_delta_value() const
            {
                const auto build_delta = [](const auto &view) {
                    if (const auto *ref_state = switching_ref_state(view.context_ref());
                        ref_state != nullptr && ref_state->switch_modified_time == view.last_modified_time() &&
                        ref_state->previous_target_value.has_value()) {
                        nb::dict out;
                        const auto current = view.value().as_map();
                        const auto previous = ref_state->previous_target_value.view().as_map();
                        constexpr size_t no_slot = static_cast<size_t>(-1);

                        for (size_t slot = current.first_live_slot(); slot != no_slot; slot = current.next_live_slot(slot)) {
                            const View key = current.delta().key_at_slot(slot);
                            const View current_value = current.at(key);
                            if (!previous.contains(key) || previous.at(key) != current_value) {
                                out[key.to_python()] = current_value.to_python();
                            }
                        }

                        for (size_t slot = previous.first_live_slot(); slot != no_slot; slot = previous.next_live_slot(slot)) {
                            const View key = previous.delta().key_at_slot(slot);
                            if (!current.contains(key)) { out[key.to_python()] = remove_sentinel(); }
                        }

                        return out;
                    }

                    nb::dict out;
                    MapDeltaView delta = view.delta_value().as_map().delta();

                    for (size_t slot = delta.first_added_slot(); slot != static_cast<size_t>(-1); slot = delta.next_added_slot(slot)) {
                        const View key = delta.key_at_slot(slot);
                        const View value = delta.value_at_slot(slot);
                        if (value.has_value()) { out[key.to_python()] = value.to_python(); }
                    }

                    for (size_t slot = delta.first_updated_slot(); slot != static_cast<size_t>(-1); slot = delta.next_updated_slot(slot)) {
                        if (delta.slot_added(slot)) { continue; }
                        const View key = delta.key_at_slot(slot);
                        const View value = delta.value_at_slot(slot);
                        if (value.has_value()) { out[key.to_python()] = value.to_python(); }
                    }

                    for (size_t slot = delta.first_removed_slot(); slot != static_cast<size_t>(-1); slot = delta.next_removed_slot(slot)) {
                        out[delta.key_at_slot(slot).to_python()] = remove_sentinel();
                    }

                    return out;
                };

                return m_input != nullptr ? build_delta(input_view()) : build_delta(output_view());
            }

            [[nodiscard]] nb::object set_python_delta_value() const
            {
                const auto build_delta = [](const auto &view) {
                    if (const auto *ref_state = switching_ref_state(view.context_ref());
                        ref_state != nullptr && ref_state->switch_modified_time == view.last_modified_time() &&
                        ref_state->previous_target_value.has_value()) {
                        nb::list added_values;
                        nb::list removed_values;
                        const auto current = view.value().as_set();
                        const auto previous = ref_state->previous_target_value.view().as_set();

                        for (const View &item : current.values()) {
                            if (!previous.contains(item)) { added_values.append(item.to_python()); }
                        }
                        for (const View &item : previous.values()) {
                            if (!current.contains(item)) { removed_values.append(item.to_python()); }
                        }
                        return set_delta_builder()(added_values, removed_values);
                    }

                    nb::list added_values;
                    nb::list removed_values;
                    for (const View &item : view.as_set().added_values()) { added_values.append(item.to_python()); }
                    for (const View &item : view.as_set().removed_values()) { removed_values.append(item.to_python()); }
                    return set_delta_builder()(added_values, removed_values);
                };

                return m_input != nullptr ? build_delta(input_view()) : build_delta(output_view());
            }

            [[nodiscard]] TSInputView input_view() const
            {
                ensure_input();
                auto view = m_input->view(m_node, evaluation_time());
                for (const auto &step : m_path_steps) {
                    switch (step.kind) {
                        case PathStep::Kind::Field: view = view.as_bundle().field(step.field_name); break;
                        case PathStep::Kind::Index: view = view.as_list().at(step.index); break;
                        case PathStep::Kind::Key: view = view.as_dict().at(step.key.view()); break;
                    }
                }
                return view;
            }

            [[nodiscard]] TSOutputView output_view() const
            {
                ensure_output();
                auto view =
                    m_output != nullptr ? m_output->view(evaluation_time()) : output_view_from_context(*m_bound_output, evaluation_time());
                for (const auto &step : m_path_steps) {
                    switch (step.kind) {
                        case PathStep::Kind::Field: view = view.as_bundle().field(step.field_name); break;
                        case PathStep::Kind::Index: view = view.as_list().at(step.index); break;
                        case PathStep::Kind::Key: view = view.as_dict().at(step.key.view()); break;
                    }
                }
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
                if (m_output == nullptr && !m_bound_output.has_value()) {
                    throw std::logic_error("v2 Python time-series handle is not an output");
                }
            }

            [[nodiscard]] std::string_view schema_kind_label() const noexcept
            {
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

            Node *m_node{nullptr};
            TSInput *m_input{nullptr};
            TSOutput *m_output{nullptr};
            const TSMeta *m_schema{nullptr};
            std::vector<PathStep> m_path_steps;
            std::optional<LinkedTSContext> m_bound_output;
            engine_time_t m_fixed_evaluation_time{MIN_DT};

            friend struct V2PythonReferenceSupport;
        };

        struct V2PythonReferenceSupport
        {
            [[nodiscard]] static TimeSeriesReference make(nb::object ts, nb::object items)
            {
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

            static void bind_input(const TimeSeriesReference &ref, PythonTimeSeriesHandle &input_handle)
            {
                if (input_handle.m_input == nullptr) {
                    throw std::runtime_error("v2 TimeSeriesReference.bind_input requires an input handle");
                }

                TSInputView view = input_handle.input_view();
                BaseState *state = view.context_ref().ts_state;
                if (state == nullptr || state->storage_kind != TSStorageKind::TargetLink) {
                    if (ref.is_empty()) { return; }
                    throw std::runtime_error(
                        "v2 TimeSeriesReference.bind_input only supports target-link inputs for now");
                }

                auto &link_state = *static_cast<TargetLinkState *>(state);
                if (ref.is_empty()) {
                    link_state.reset_target();
                    return;
                }

                if (!ref.is_peered()) {
                    throw std::runtime_error("v2 TimeSeriesReference.bind_input only supports peered refs for now");
                }

                TSOutputView output = ref.target_view(input_handle.evaluation_time());
                LinkedTSContext target = output.linked_context();
                const TSMeta *target_schema = view.context_ref().schema;

                if (target_schema == nullptr) {
                    throw std::runtime_error("v2 TimeSeriesReference.bind_input requires a typed input schema");
                }

                if (target.schema != target_schema) {
                    TSOutput *owning_output = output.owning_output();
                    if (owning_output == nullptr) {
                        throw std::runtime_error(
                            "v2 TimeSeriesReference.bind_input requires a bindable owning output for schema casts");
                    }
                    output = owning_output->bindable_view(output, target_schema);
                    target = output.linked_context();
                }

                if (target.schema != target_schema) {
                    throw std::runtime_error("v2 TimeSeriesReference.bind_input could not match the input schema");
                }

                link_state.set_target(target);
            }

            [[nodiscard]] static nb::object output(const TimeSeriesReference &ref)
            {
                if (!ref.is_peered()) { return nb::none(); }

                const LinkedTSContext &target = ref.target();
                const engine_time_t when = target.ts_state != nullptr ? target.ts_state->last_modified_time : MIN_DT;
                return nb::cast(PythonTimeSeriesHandle{target, when});
            }
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

        nb::class_<TimeSeriesReference>(m, "TimeSeriesReference")
            .def("__str__", &TimeSeriesReference::to_string)
            .def("__repr__", &TimeSeriesReference::to_string)
            .def(
                "__eq__",
                [](const TimeSeriesReference &self, nb::object other) {
                    return other.is_none() ? false
                                           : nb::isinstance<TimeSeriesReference>(other) &&
                                                 self == nb::cast<TimeSeriesReference>(other);
                },
                "other"_a,
                nb::is_operator())
            .def("bind_input", &V2PythonReferenceSupport::bind_input, "input_"_a)
            .def_prop_ro("has_output", &TimeSeriesReference::is_peered)
            .def_prop_ro("has_peer", &TimeSeriesReference::is_peered)
            .def_prop_ro("is_empty", &TimeSeriesReference::is_empty)
            .def_prop_ro("is_bound", &TimeSeriesReference::is_peered)
            .def_prop_ro("is_unbound", &TimeSeriesReference::is_non_peered)
            .def_prop_ro("is_valid", &TimeSeriesReference::is_valid)
            .def_prop_ro("valid", &TimeSeriesReference::is_valid)
            .def_prop_ro("output", &V2PythonReferenceSupport::output)
            .def_prop_ro(
                "items",
                [](const TimeSeriesReference &self) {
                    nb::list out;
                    if (self.is_non_peered()) {
                        for (const auto &item : self.items()) { out.append(nb::cast(item)); }
                    }
                    return nb::tuple(out);
                })
            .def("__getitem__", &TimeSeriesReference::operator[])
            .def_static("make",
                        &V2PythonReferenceSupport::make,
                        "ts"_a = nb::none(),
                        "from_items"_a = nb::none());

        nb::class_<PythonTimeSeriesHandle>(m, "_PythonTimeSeriesHandle")
            .def("__bool__", &PythonTimeSeriesHandle::truthy)
            .def("__getitem__", &PythonTimeSeriesHandle::get_item, "key"_a)
            .def("__getattr__", &PythonTimeSeriesHandle::get_item, "key"_a)
            .def("__iter__",
                 [](const PythonTimeSeriesHandle &self) {
                     return self.is_list() || self.is_set() ? nb::iter(self.values()) : nb::iter(self.keys());
                 })
            .def_prop_rw("value", &PythonTimeSeriesHandle::value, &PythonTimeSeriesHandle::set_value)
            .def_prop_ro("delta_value", &PythonTimeSeriesHandle::delta_value)
            .def_prop_ro("is_reference", &PythonTimeSeriesHandle::is_reference)
            .def_prop_ro("modified", &PythonTimeSeriesHandle::modified)
            .def_prop_ro("valid", &PythonTimeSeriesHandle::valid)
            .def_prop_ro("all_valid", &PythonTimeSeriesHandle::all_valid)
            .def_prop_ro("last_modified_time", &PythonTimeSeriesHandle::last_modified_time)
            .def("keys", &PythonTimeSeriesHandle::keys)
            .def("valid_keys", &PythonTimeSeriesHandle::valid_keys)
            .def("modified_keys", &PythonTimeSeriesHandle::modified_keys)
            .def("values", &PythonTimeSeriesHandle::values)
            .def("added", &PythonTimeSeriesHandle::added)
            .def("removed", &PythonTimeSeriesHandle::removed)
            .def("valid_values", &PythonTimeSeriesHandle::valid_values)
            .def("modified_values", &PythonTimeSeriesHandle::modified_values)
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
