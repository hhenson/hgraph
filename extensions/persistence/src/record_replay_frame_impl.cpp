#include "record_replay_frame_impl.h"

namespace hgraph::persistence
{
    void register_record_replay_frame_operators()
    {
        register_overload<stdlib::record, record_frame_impl>();
        register_overload<stdlib::replay, replay_frame_impl>();
        register_overload<stdlib::compare, compare_impl>();
        register_overload<stdlib::replay_const, replay_const_impl>();
    }
}  // namespace hgraph::persistence
