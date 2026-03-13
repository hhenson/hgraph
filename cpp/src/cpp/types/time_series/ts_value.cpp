#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_value.h>

namespace hgraph
{
    namespace
    {
        [[nodiscard]] const value::TypeMeta &require_value_schema(const TSMeta *schema)
        {
            if (schema == nullptr) { throw std::invalid_argument("TSValue requires a non-null schema"); }
            if (schema->value_type == nullptr) { throw std::invalid_argument("TSValue requires a schema with a value_type"); }
            return *schema->value_type;
        }

        template <typename TState>
        TState make_initialized_root_state()
        {
            TState state{};
            state.parent = static_cast<TSOutput *>(nullptr);
            state.index = 0;
            state.last_modified_time = MIN_DT;
            return state;
        }
    }  // namespace

    TSValue::TSValue(const TSMeta *schema)
        : m_value(require_value_schema(schema), MutationTracking::Delta), m_state(make_root_state(schema)), m_schema(schema)
    {
    }

    AtomicView TSValue::atomic_value() const
    {
        return value().as_atomic();
    }

    ListView TSValue::list_value() const
    {
        return value().as_list();
    }

    BundleView TSValue::bundle_value() const
    {
        return value().as_bundle();
    }

    SetView TSValue::set_value() const
    {
        return value().as_set();
    }

    MapView TSValue::dict_value() const
    {
        return value().as_map();
    }

    CyclicBufferView TSValue::window_value() const
    {
        return value().as_cyclic_buffer();
    }

    ListDeltaView TSValue::list_delta_value() const
    {
        return list_value().delta();
    }

    BundleDeltaView TSValue::bundle_delta_value() const
    {
        return bundle_value().delta();
    }

    SetDeltaView TSValue::set_delta_value() const
    {
        return set_value().delta();
    }

    MapDeltaView TSValue::dict_delta_value() const
    {
        return dict_value().delta();
    }

    TimeSeriesStateV TSValue::make_root_state(const TSMeta *schema)
    {
        if (schema == nullptr) { throw std::invalid_argument("TSValue root state requires a non-null schema"); }

        switch (schema->kind) {
            case TSKind::TSValue: return make_initialized_root_state<TSState>();
            case TSKind::TSS: return make_initialized_root_state<TSSState>();
            case TSKind::TSD: return make_initialized_root_state<TSDState>();
            case TSKind::TSL: return make_initialized_root_state<TSLState>();
            case TSKind::TSW: return make_initialized_root_state<TSWState>();
            case TSKind::TSB: return make_initialized_root_state<TSBState>();
            case TSKind::REF: return make_initialized_root_state<RefLinkState>();
            case TSKind::SIGNAL: return make_initialized_root_state<SignalState>();
        }

        throw std::invalid_argument("TSValue root state requires a supported TS schema kind");
    }
}  // namespace hgraph
