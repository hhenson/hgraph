#ifndef HGRAPH_TYPES_DETAIL_REFERENCE_CHILD_PROJECTION_H
#define HGRAPH_TYPES_DETAIL_REFERENCE_CHILD_PROJECTION_H

#include <hgraph/types/graph_wiring.h>

namespace hgraph::stdlib::container_impl_detail {
/** Selected input route and native REF publication slots, prepared once at
    start; only designation/presence/lifetime observations run during eval. */
struct PreparedRefProjectionState {
  PreparedInputReferenceRoute child{};
  ValueTypeRef binding{};
};
}

namespace hgraph::static_schema_detail {
template <>
struct scalar_name<stdlib::container_impl_detail::PreparedRefProjectionState> {
  static constexpr std::string_view value{"stdlib.prepared_ref_projection_state"};
};
}

namespace hgraph::stdlib {
/** Internal REF projection: activation observes only the designation; the
    passive declared observation delegates shape validation and retargeting to
    TargetLink. The selected reference route and native output binding are cold. */
struct prepared_ref_child_projection {
  static constexpr auto name = "prepared_ref_child_projection";
  static constexpr bool schedule_on_start = true;
  using signature_args = std::tuple<
      In<"designation", REF<TsVar<"S">>, InputValidity::Unchecked>,
      In<"observed", TsVar<"S">, InputActivity::Passive, InputValidity::Unchecked>,
      Scalar<"index", Int>, TypeArg<"child", TsVar<"E">>,
      State<container_impl_detail::PreparedRefProjectionState>, Out<REF<TsVar<"E">>>>;

  static void start(In<"observed", TsVar<"S">, InputActivity::Passive, InputValidity::Unchecked> observed,
                    Scalar<"index", Int> index, State<container_impl_detail::PreparedRefProjectionState> state,
                    Out<REF<TsVar<"E">>> out, NodeView node) {
    auto child = observed.base().indexed_child_at(static_cast<std::size_t>(index.value()));
    // The output owner prepares native and forwarding terminal routes alike.
    // Static-node start reacquires this same route after the user hook.
    acquire_prepared_output_route(node, out.evaluation_time());
    const auto *route = out.prepared_output();
    const auto binding = route != nullptr && route->native() ? route->value_binding : out.value().binding();
    if (binding == nullptr || binding.ops() != &ops_for<TimeSeriesReference>()) {
      throw std::logic_error("REF projection requires its prepared native reference output binding");
    }
    state.set(container_impl_detail::PreparedRefProjectionState{
        child.prepare_reference_route(), binding});
  }

  static void eval(In<"designation", REF<TsVar<"S">>, InputValidity::Unchecked> designation,
                   State<container_impl_detail::PreparedRefProjectionState> state, Out<REF<TsVar<"E">>> out) {
    static_cast<void>(designation);
    const auto &prepared = *static_cast<const container_impl_detail::PreparedRefProjectionState *>(state.view().data());
    auto reference = prepared.child.read(out.evaluation_time());
    const auto *route = out.prepared_output();
    if (route != nullptr && route->native()) {
      auto *destination = static_cast<TimeSeriesReference *>(route->native_value);
      if (out.valid() && *destination == reference) { return; }
      *destination = std::move(reference);
      route->commit(out.evaluation_time());
    } else {
      // A terminal retargeted after start uses its existing owner mutation.
      // The source binding was proved cold; no schema or type discovery is
      // needed to publish through the link's current target.
      if (out.valid() && *static_cast<const TimeSeriesReference *>(out.value().data()) == reference) { return; }
      auto mutation = out.begin_mutation(out.evaluation_time());
      static_cast<void>(mutation.copy_value_from(ValueView{prepared.binding, &reference}));
    }
  }
};
} // namespace hgraph::stdlib

namespace hgraph::graph_wiring_detail {
inline WiringPortRef project_reference_child(Wiring &w, const WiringPortRef &parent, std::size_t index,
                                            const TSValueTypeMetaData *child) {
  const Port<void> source{w, parent};
  return wire<stdlib::prepared_ref_child_projection>(w, arg<"designation">(source), arg<"observed">(source),
      arg<"index">(static_cast<Int>(index)), arg<"child">(TypeCarrier::of_ts(child))).erased();
}
}
#endif
