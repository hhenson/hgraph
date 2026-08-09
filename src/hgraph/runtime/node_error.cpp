#include <hgraph/runtime/node_error.h>

#include <hgraph/runtime/node.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/time_series/visitor.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/util/scope.h>

#include <algorithm>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace hgraph
{
    namespace
    {
        constexpr std::size_t max_trace_depth = 64;
        constexpr std::size_t max_value_chars = 256;

        [[nodiscard]] std::string node_name(const NodeView &node)
        {
            if (!node.valid()) { return "<unknown>"; }
            if (!node.label().empty()) { return std::string{node.label()}; }
            const NodeTypeMetaData *schema = node.schema();
            return schema != nullptr && schema->display_name != nullptr
                       ? std::string{schema->display_name}
                       : std::string{"<unnamed>"};
        }

        [[nodiscard]] std::string bounded_value(const TSInputView &input, bool delta)
        {
            return fallback_on_exception(std::string{"<unavailable>"}, [&] {
                if (!input.valid()) { return std::string{"<unset>"}; }
                std::string value = (delta ? input.delta_value() : input.value()).to_string();
                if (value.size() > max_value_chars)
                {
                    value.resize(max_value_chars);
                    value += "...";
                }
                return value;
            });
        }

        void append_node_trace(std::string &out, const NodeView &node, DateTime evaluation_time,
                               std::size_t depth, bool capture_values, std::size_t indent,
                               std::vector<const void *> &visited);

        void append_input_sources(std::string &out, const TSInputView &input,
                                  DateTime evaluation_time, std::size_t depth,
                                  bool capture_values, std::size_t indent,
                                  std::vector<const void *> &visited)
        {
            if (input.is_bindable() && input.bound())
            {
                NodeView source = input.bound_output().owner_node();
                if (source.valid())
                {
                    append_node_trace(out, source, evaluation_time, depth, capture_values,
                                      indent, visited);
                }
                return;
            }

            if (input.schema() == nullptr) { return; }
            visit(
                input,
                [&](TSBInputView bundle) {
                    for (auto &&[name, child] : bundle.modified_items())
                    {
                        static_cast<void>(name);
                        append_input_sources(out, child, evaluation_time, depth,
                                             capture_values, indent, visited);
                    }
                },
                [&](TSLInputView list) {
                    for (auto &&[index, child] : list.modified_items())
                    {
                        static_cast<void>(index);
                        append_input_sources(out, child, evaluation_time, depth,
                                             capture_values, indent, visited);
                    }
                },
                [&](TSDInputView dict) {
                    for (auto &&[key, child] : dict.modified_items())
                    {
                        static_cast<void>(key);
                        append_input_sources(out, child, evaluation_time, depth,
                                             capture_values, indent, visited);
                    }
                },
                [](TSInputView) {});
        }

        void append_input(std::string &out, std::string_view name, const TSInputView &input,
                          DateTime evaluation_time, std::size_t depth, bool capture_values,
                          std::size_t indent, std::vector<const void *> &visited)
        {
            const bool modified = input.modified();
            if (!capture_values && !modified) { return; }

            out.append(indent, ' ');
            if (modified) { out += '*'; }
            out.append(name);
            if (modified) { out += '*'; }
            if (capture_values)
            {
                out += ": value=";
                out += bounded_value(input, false);
                if (modified)
                {
                    out += ", delta=";
                    out += bounded_value(input, true);
                }
                out += ", last_modified=";
                out += std::to_string(input.last_modified_time().time_since_epoch().count());
            }
            out += '\n';

            if (modified && depth > 0)
            {
                append_input_sources(out, input, evaluation_time, depth - 1,
                                     capture_values, indent + 2, visited);
            }
        }

        void append_node_trace(std::string &out, const NodeView &node, DateTime evaluation_time,
                               std::size_t depth, bool capture_values, std::size_t indent,
                               std::vector<const void *> &visited)
        {
            if (!node.valid()) { return; }
            out.append(indent, ' ');
            out += node_name(node);
            out += '[';
            out += std::to_string(node.node_index());
            out += ']';

            if (std::find(visited.begin(), visited.end(), node.data()) != visited.end())
            {
                out += " (cycle)\n";
                return;
            }
            out += '\n';
            visited.push_back(node.data());
            auto pop_visited = make_scope_exit([&] noexcept { visited.pop_back(); });

            if (depth == 0 || !node.has_input()) { return; }
            TSInputView input = node.input(evaluation_time);
            if (input.schema() == nullptr)
            {
                append_input(out, "input", input, evaluation_time, depth,
                             capture_values, indent + 2, visited);
                return;
            }
            visit(
                input,
                [&](TSBInputView bundle) {
                    for (auto &&[name, child] : bundle.items())
                    {
                        append_input(out, name, child, evaluation_time, depth,
                                     capture_values, indent + 2, visited);
                    }
                },
                [&](TSInputView leaf) {
                    append_input(out, "input", leaf, evaluation_time, depth,
                                 capture_values, indent + 2, visited);
                });
        }
    }  // namespace

    const ValueTypeMetaData *node_error_value_meta()
    {
        auto       &registry = TypeRegistry::instance();
        const auto *str_meta = scalar_descriptor<std::string>::value_meta();

        // Rebuild per call: the registry is reset between tests, so a cached
        // ``str_meta`` would dangle. ``bundle`` interns, so this is cheap.
        const std::vector<std::pair<std::string, const ValueTypeMetaData *>> fields{
            {"signature_name", str_meta},
            {"label", str_meta},
            {"wiring_path", str_meta},
            {"error_msg", str_meta},
            {"stack_trace", str_meta},
            {"activation_back_trace", str_meta},
            {"additional_context", str_meta},
        };
        return registry.bundle("NodeError", fields);
    }

    const TSValueTypeMetaData *node_error_ts_meta()
    {
        return TypeRegistry::instance().ts(node_error_value_meta());
    }

    Value make_node_error_value(const NodeErrorFields &fields)
    {
        const auto *meta    = node_error_value_meta();
        const auto binding = ValuePlanFactory::instance().type_for(meta);
        if (!binding) { throw std::logic_error("NodeError: bundle schema has no canonical binding"); }

        BundleBuilder builder{binding};
        const auto    set_field = [&](std::string_view name, const std::string &text) {
            builder.set(name, Value{text});
        };
        set_field("signature_name", fields.signature_name);
        set_field("label", fields.label);
        set_field("wiring_path", fields.wiring_path);
        set_field("error_msg", fields.error_msg);
        set_field("stack_trace", fields.stack_trace);
        set_field("activation_back_trace", fields.activation_back_trace);
        if (fields.additional_context.has_value())
        {
            set_field("additional_context", *fields.additional_context);
        }
        return builder.build();
    }

    NodeErrorFields capture_node_error(const NodeView &node, DateTime evaluation_time,
                                       std::string error_msg, ErrorCaptureOptions options,
                                       std::optional<std::string> additional_context)
    {
        NodeErrorFields fields;
        const NodeTypeMetaData *schema = node.schema();
        fields.signature_name =
            schema != nullptr && schema->display_name != nullptr ? std::string{schema->display_name} : std::string{};
        fields.label       = std::string{node.label()};
        fields.wiring_path = fields.label.empty() ? fields.signature_name : fields.label;
        fields.error_msg   = std::move(error_msg);
        fields.additional_context = std::move(additional_context);

        fields.activation_back_trace = fallback_on_exception(std::string{}, [&] {
            std::string               trace;
            std::vector<const void *> visited;
            append_node_trace(trace, node, evaluation_time,
                              std::min(options.trace_back_depth, max_trace_depth),
                              options.capture_values, 0, visited);
            return trace;
        });
        return fields;
    }
}  // namespace hgraph
