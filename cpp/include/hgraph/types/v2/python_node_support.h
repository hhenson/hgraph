#pragma once

#include <hgraph/hgraph_base.h>

#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <unordered_map>

namespace hgraph
{
    struct TSMeta;
    struct TSInput;
    struct TSOutput;
}

namespace hgraph::v2
{
    struct Node;
    struct NodeScheduler;

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

    void register_python_runtime_bindings(nb::module_ &m);
}  // namespace hgraph::v2
