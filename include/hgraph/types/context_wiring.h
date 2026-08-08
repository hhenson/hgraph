#ifndef HGRAPH_TYPES_CONTEXT_WIRING_H
#define HGRAPH_TYPES_CONTEXT_WIRING_H

#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/static_schema.h>

#include <stdexcept>
#include <string>
#include <string_view>

namespace hgraph::context
{
    /**
     * User-facing context wiring (design record: *Contexts* in services.rst).
     *
     * A context is a wiring-scoped named port: ``context::scope<"price">
     * ctx{w, port};`` publishes ``port`` for the lexical scope of ``ctx``;
     * anything wired inside consumes it either through a
     * ``Context<"price", TS<Float>>`` signature input (static_node.h) or the
     * function forms below. Scopes nest; the nearest publication wins.
     */
    template <fixed_string Name>
    class scope
    {
      public:
        template <typename Schema>
        scope(Wiring &w, const Port<Schema> &port)
        {
            static_assert(!Name.sv().empty(), "context::scope requires a non-empty name");
            graph_wiring_detail::push_context_source(w, Name.sv(), port.erased());
        }

        scope(const scope &)            = delete;
        scope &operator=(const scope &) = delete;
        scope(scope &&)                 = delete;
        scope &operator=(scope &&)      = delete;

        ~scope() { graph_wiring_detail::pop_context_source(); }
    };

    /** The nearest published context named ``name``, typed. Throws when missing or mismatched. */
    template <typename Schema>
    [[nodiscard]] Port<Schema> get(Wiring &w, std::string_view name)
    {
        WiringPortRef ref = graph_wiring_detail::resolve_context_source(w, name);
        if constexpr (!std::is_void_v<Schema>)
        {
            const auto *expected = schema_descriptor<Schema>::ts_meta();
            if (!graph_wiring_detail::input_accepts_output_schema(expected, ref.schema))
            {
                throw std::logic_error("context::get: context '" + std::string{name} +
                                       "' schema does not match the requested schema");
            }
        }
        return Port<Schema>{w, std::move(ref)};
    }

    /** ``true`` when a context named ``name`` is published in this wiring's scope stack. */
    [[nodiscard]] inline bool has(const Wiring &w, std::string_view name) noexcept
    {
        return graph_wiring_detail::has_context_source(w, name);
    }
}  // namespace hgraph::context

#endif  // HGRAPH_TYPES_CONTEXT_WIRING_H
