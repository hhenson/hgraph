#ifndef HGL_GLOBAL_KEY_PREFLIGHT_H
#define HGL_GLOBAL_KEY_PREFLIGHT_H

#include <hgraph/types/operator_dispatch.h>

namespace hgl::ordinary
{
    template <typename Impl, typename... Arguments>
    auto preflight_wire(hgraph::Wiring &wiring, const Arguments &...arguments) {
        using Signature = hgraph::StaticNodeSignature<Impl>;
        std::vector<hgraph::WiringArg> erased;
        erased.reserve(sizeof...(Arguments));
        (erased.push_back(hgraph::operator_dispatch_detail::make_wiring_arg(arguments)), ...);
        [[maybe_unused]] auto result = hgraph::wire_operator(wiring, Impl::name, erased, Signature::has_output());
        if constexpr (Signature::has_output()) {
            if constexpr (Signature::is_generic()) { return result.output; }
            else { return result.output.template as<typename Signature::output_schema_type>(); }
        }
    }

    /// Evaluate a source node's const global-entry contract after overload
    /// matching, before adding its native node. The ordinary registry still
    /// owns candidate matching, provider lifetime and actual node wiring.
    template <typename Op, typename Impl,
              hgraph::OperatorNodePack Pack = hgraph::OperatorNodePack::Infer,
              hgraph::OperatorPackCardinality Cardinality = {}>
    void register_preflight_overload() {
        if constexpr (!requires(hgraph::Wiring &wiring, const hgraph::ResolutionMap &map, const hgraph::ValueView &scalars) {
            Impl::preflight_keys(wiring, map, scalars);
        }) {
            hgraph::register_overload<Op, Impl, Pack, Cardinality>();
        } else {
            auto candidate = hgraph::make_operator_impl<Impl, Pack, Cardinality>(std::string{Op::name});
            hgraph::operator_dispatch_detail::require_candidate_shape<Op>(candidate);
            auto wire = std::move(candidate.wire);
            candidate.wire = [wire = std::move(wire)](
                hgraph::Wiring &wiring, const hgraph::ResolutionMap &map,
                std::span<const hgraph::WiringArg> arguments,
                std::span<const std::pair<std::string, hgraph::WiringPortRef>> keywords) {
                auto scalars = hgraph::operator_dispatch_detail::assemble_scalars<Impl>(map, arguments, wiring.realization_options());
                Impl::preflight_keys(wiring, map, scalars.view());
                return wire(wiring, map, arguments, keywords);
            };
            hgraph::OperatorRegistry::instance().register_overload(std::move(candidate));
        }
    }
}
#endif
