#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_REDUCE_LAYOUT_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_REDUCE_LAYOUT_H

#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/wired_fn.h>

#include <array>
#include <optional>
#include <span>
#include <vector>

namespace hgraph::stdlib::higher_order_impl_detail
{
    /*
     * Shared wiring-time binary reduction layout, mirroring Python _reduce_tsl:
     * linear for non-associative functions and small associative inputs,
     * otherwise balanced binary pairing with an odd-element carry. The caller
     * owns any identity/default leaves.
     */
    [[nodiscard]] inline WiringPortRef reduce_layout(Wiring &w, const WiredFn &func,
                                                     std::vector<WiringPortRef> elements,
                                                     bool associative = true)
    {
        auto combine = [&](const WiringPortRef &lhs, const WiringPortRef &rhs) {
            const std::array<WiringPortRef, 2> args{lhs, rhs};
            return func.wire(w, std::span<const WiringPortRef>{args.data(), args.size()});
        };

        if (!associative || elements.size() < 4)
        {
            WiringPortRef out = elements.front();
            for (std::size_t i = 1; i < elements.size(); ++i) { out = combine(out, elements[i]); }
            return out;
        }

        std::vector<WiringPortRef> outs;
        outs.reserve(elements.size() / 2);
        for (std::size_t i = 0; i + 1 < elements.size(); i += 2)
        {
            outs.push_back(combine(elements[i], elements[i + 1]));
        }
        std::optional<WiringPortRef> over_run;
        if (elements.size() % 2 == 1) { over_run = elements.back(); }

        while (outs.size() > 1)
        {
            std::size_t count = outs.size();
            if (count % 2 == 1)
            {
                if (over_run.has_value())
                {
                    outs.push_back(*over_run);
                    over_run.reset();
                    ++count;
                }
                else
                {
                    over_run = outs.back();
                    outs.pop_back();
                    --count;
                }
            }
            std::vector<WiringPortRef> next;
            next.reserve(count / 2);
            for (std::size_t i = 0; i + 1 < count; i += 2) { next.push_back(combine(outs[i], outs[i + 1])); }
            outs = std::move(next);
        }
        return over_run.has_value() ? combine(outs.front(), *over_run) : outs.front();
    }
}  // namespace hgraph::stdlib::higher_order_impl_detail

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_REDUCE_LAYOUT_H
