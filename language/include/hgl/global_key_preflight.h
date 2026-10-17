#ifndef HGL_GLOBAL_KEY_PREFLIGHT_H
#define HGL_GLOBAL_KEY_PREFLIGHT_H

#include <hgraph/types/operator_dispatch.h>

#include <stdexcept>
#include <vector>

namespace hgl::ordinary
{
    namespace detail
    {
        /// Erase one argument of a generated operator call. A forwarded
        /// positional pack (VarIn) contributes one argument per entry, as
        /// entries spelled at the call site do, so an empty pack contributes
        /// none; hgraph's VarIn-to-TSL conversion would reject it.
        template <typename Argument>
        void append_operator_argument(std::vector<hgraph::WiringArg> &erased, const Argument &argument) {
            if constexpr (hgraph::operator_dispatch_detail::is_var_in<Argument>::value) {
                for (const hgraph::WiringPortRef &port : argument) {
                    hgraph::WiringArg entry;
                    entry.kind = hgraph::WiringArg::Kind::TimeSeries;
                    entry.port = port;
                    erased.push_back(std::move(entry));
                }
            } else {
                erased.push_back(hgraph::operator_dispatch_detail::make_wiring_arg(argument));
            }
        }

        template <typename... Arguments>
        std::vector<hgraph::WiringArg> erase_operator_arguments(const Arguments &...arguments) {
            std::vector<hgraph::WiringArg> erased;
            erased.reserve(sizeof...(Arguments));
            (append_operator_argument(erased, arguments), ...);
            return erased;
        }
    }

    /// Wire a runtime function with a parameter pack through its operator
    /// contract, as hgraph::wire<Contract> does, spreading a forwarded pack
    /// into its entries.
    template <typename Contract, typename... Arguments>
    auto wire_contract(hgraph::Wiring &wiring, const Arguments &...arguments) {
        const auto erased = detail::erase_operator_arguments(arguments...);
        auto result = hgraph::wire_operator(wiring, Contract::name, erased, Contract::has_output);
        if constexpr (Contract::has_output) {
            if (!result.has_output) { throw std::logic_error("wire<Operator>: selected overload has no output"); }
            return result.output;
        } else if (result.has_output) {
            throw std::logic_error("wire<Operator>: selected overload unexpectedly produced an output");
        }
    }

    template <typename Impl, typename... Arguments>
    auto preflight_wire(hgraph::Wiring &wiring, const Arguments &...arguments) {
        using Signature = hgraph::StaticNodeSignature<Impl>;
        const auto erased = detail::erase_operator_arguments(arguments...);
        [[maybe_unused]] auto result = hgraph::wire_operator(wiring, Impl::name, erased, Signature::has_output());
        if constexpr (Signature::has_output()) {
            using Output = typename Signature::output_schema_type;
            // Generic inputs do not erase an independently concrete result.
            if constexpr (hgraph::schema_descriptor<Output>::is_concrete()) {
                return result.output.template as<Output>();
            } else { return result.output; }
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
