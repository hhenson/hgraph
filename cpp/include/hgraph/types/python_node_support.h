#pragma once

#include <hgraph/hgraph_base.h>

#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace hgraph
{
    struct TSMeta;
    struct Graph;
    struct TSInput;
    struct TSInputView;
    struct TSOutput;
}

namespace hgraph
{
    struct Node;
    struct NodeScheduler;
    struct View;

    struct NestedGraphEntry
    {
        nb::object key;
        Graph     *graph{nullptr};
    };

    [[nodiscard]] nb::object make_python_node_handle(nb::handle signature,
                                                     nb::handle scalars,
                                                     Node *node,
                                                     TSInput *input,
                                                     TSOutput *output,
                                                     TSOutput *error_output,
                                                     TSOutput *recordable_state,
                                                     const ::hgraph::TSMeta *input_schema,
                                                     const ::hgraph::TSMeta *output_schema,
                                                     const ::hgraph::TSMeta *error_output_schema,
                                                     const ::hgraph::TSMeta *recordable_state_schema,
                                                     NodeScheduler *scheduler);

    [[nodiscard]] nb::dict make_python_node_kwargs(nb::handle signature, nb::handle scalars, nb::handle node_handle);
    [[nodiscard]] nb::tuple python_callable_parameter_names(nb::handle callable);
    [[nodiscard]] nb::object call_python_callable(nb::handle callable, nb::handle kwargs);
    [[nodiscard]] nb::object call_python_node_eval(nb::handle signature, nb::handle callable, nb::handle kwargs);
    [[nodiscard]] nb::object call_python_callable_with_subset(nb::handle callable,
                                                              nb::handle kwargs,
                                                              nb::handle parameter_names);
    [[nodiscard]] bool node_has_python_handle_layout(const Node &node) noexcept;
    [[nodiscard]] bool node_is_nested_runtime(const Node &node) noexcept;
    [[nodiscard]] TSInput *node_input_ptr(Node &node) noexcept;
    [[nodiscard]] TSOutput *node_output_ptr(Node &node) noexcept;
    [[nodiscard]] TSOutput *node_error_output_ptr(Node &node) noexcept;
    [[nodiscard]] TSOutput *node_recordable_state_ptr(Node &node) noexcept;
    [[nodiscard]] engine_time_t node_last_evaluation_time(Node &node) noexcept;
    [[nodiscard]] std::vector<NestedGraphEntry> node_nested_graph_entries(Node &node);
    [[nodiscard]] bool last_value_node_copy_from_input(Node &node, const TSInputView &source);
    [[nodiscard]] bool last_value_node_apply_tsd_item(Node &node, const View &key, const View &value);
    [[nodiscard]] bool last_value_node_remove_tsd_item(Node &node, const View &key);
    [[nodiscard]] bool last_value_node_apply_tss_item(Node &node, const View &item, bool remove);
    [[nodiscard]] bool last_value_node_apply_value(Node &node, nb::handle value);

    void register_python_runtime_bindings(nb::module_ &m);
}  // namespace hgraph
