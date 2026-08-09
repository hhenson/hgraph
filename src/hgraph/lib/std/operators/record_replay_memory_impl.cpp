#include <hgraph/lib/std/operators/impl/record_replay_memory_impl.h>

namespace hgraph::stdlib
{
    void register_record_replay_memory_operators()
    {
        // record has two model-selected backends: the DENSE cycle-aligned
        // harness recorder (IN_MEMORY_DENSE) and the SPARSE absolute-time
        // :memory: recorder (IN_MEMORY). replay is a SINGLE backend serving
        // both models: a bare replay(key) reads the dense plain-key buffer; an
        // explicit recordable_id reads the sparse :memory: recording (component
        // recover / cross-run reads).
        register_overload<record, dense_record_impl>();
        register_overload<record, sparse_record_impl>();
        register_overload<replay, replay_impl>();
        register_overload<compare, memory_compare_impl>();
    }
}  // namespace hgraph::stdlib
