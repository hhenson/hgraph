#ifndef HGRAPH_CPP_ROOT_STATIC_NODE_H
#define HGRAPH_CPP_ROOT_STATIC_NODE_H

#include <hgraph/types/call_args.h>
#include <hgraph/types/type_resolution.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/runtime/node_scheduler.h>
#include <hgraph/types/static_node_core.h>
#include <hgraph/types/frame_selectors.h>
#include <hgraph/types/node_delta_builders.h>

namespace hgraph
{
    namespace call_args_detail
    {
        // Context-bound inputs are auto-bound call parameters: no positional
        // slot, never "missing", keyword-overridable (see call_args.h).
        template <fixed_string N, typename S, auto... P>
        inline constexpr bool auto_context_param_v<In<N, S, P...>> =
            static_node_detail::resolved_input_binding<P...>() == InputBinding::Context;
    }  // namespace call_args_detail
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_STATIC_NODE_H
