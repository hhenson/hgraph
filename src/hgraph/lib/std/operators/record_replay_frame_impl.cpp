#include <hgraph/lib/std/operators/impl/record_replay_frame_impl.h>

namespace hgraph::stdlib
{
    void register_record_replay_frame_operators()
    {
        register_overload<record, record_frame_impl>();
        register_overload<replay, replay_frame_impl>();
        register_overload<compare, compare_impl>();
        register_overload<replay_const, replay_const_impl>();
    }
}  // namespace hgraph::stdlib
