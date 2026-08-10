#ifndef HGRAPH_LIB_STD_OPERATORS_STRING_H
#define HGRAPH_LIB_STD_OPERATORS_STRING_H

#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>

namespace hgraph::stdlib
{
    /**
     * String operator **definitions** (markers only). Mirrors the Python ``hgraph`` string
     * operators (``_string.py``). (``str_`` lives in *conversion*.)
     */

    /** Match each string against a regular expression and expose both success and captures.
        @param pattern Regular-expression pattern; changing it recompiles the active match.
        @param s String to test.
        @return A bundle containing ``is_match`` and the captured ``groups``.
        @par Python example
        @code{.py}
        match = hg.match_(r"([A-Z]+)-(\d+)", code)
        @endcode */
    struct match_ : Operator<"match_", In<"pattern", TS<Str>>, In<"s", TS<Str>>, Out<TsVar<"O">>>
    {
    };

    /** Replace regular-expression matches in each input string.
        @param pattern Pattern whose matches are replaced.
        @param repl Replacement string, including supported capture references.
        @param s Source string.
        @return The transformed string.
        @par Python example
        @code{.py}
        normalized = hg.replace(r"\s+", "_", label)
        @endcode */
    struct replace : Operator<"replace", In<"pattern", TS<Str>>, In<"repl", TS<Str>>, In<"s", TS<Str>>, Out<TS<Str>>>
    {
    };

    /** Extract the slice ``s[start:end]`` using live start and end positions.
        @param s Source string.
        @param start Inclusive starting index.
        @param end Exclusive ending index.
        @return The selected substring.
        @par Python example
        @code{.py}
        prefix = hg.substr(code, 0, 3)
        @endcode */
    struct substr : Operator<"substr", In<"s", TS<Str>>, In<"start", TS<Int>>, In<"end", TS<Int>>, Out<TS<Str>>>
    {
    };

    /**
     * ``split`` — split ``s`` over ``separator`` into the requested output shape.
     *
     * Fixed TSL output size is an output type decision, not an input-derived fact.
     * Callers must supply the output schema explicitly, for example:
     *
     * ``wire<stdlib::split, TSL<TS<Str>, 2>>(w, s, Str{","})``.
     * @param s String to split.
     * @param separator Wiring-time separator; it is fixed for the graph's lifetime.
     * @return The explicitly selected tuple, list, set, or other supported string collection.
     * @par Python example
     * @code{.py}
     * fields = hg.split[TS[tuple[str, ...]]](line, separator=",")
     * @endcode
     */
    struct split : Operator<"split", In<"s", TS<Str>>, Scalar<"separator", Str>, Out<TsVar<"O">>>
    {
    };

    /** Join current string inputs with a fixed separator.
        @param strings Collection or variadic string inputs, kept in argument order.
        @param separator Wiring-time text inserted between adjacent values.
        @return The joined string, updated when an input changes.
        @par Python example
        @code{.py}
        full_name = hg.join(first_name, last_name, separator=" ")
        @endcode */
    struct join : Operator<"join", In<"strings", TsVar<"S">>, Scalar<"separator", Str>, Out<TS<Str>>>
    {
    };

    /** Format positional and named time-series values with a Python-style format string.
        ``__sample__`` can reduce output frequency, while ``__strict__`` controls whether
        every referenced input must be valid before formatting.
        @param fmt Format string; positional fields consume ``args`` and named fields consume ``kwargs``.
        @param args Positional values used by the format string.
        @param kwargs Named values used by the format string.
        @param __sample__ Emit every nth formatted tick; one emits every tick.
        @param __strict__ When true, wait for every supplied value to be valid.
        @return The formatted string.
        @par Python example
        @code{.py}
        message = hg.format_("{symbol}: {price:.2f}", symbol=symbol, price=price)
        @endcode */
    struct format_
        : Operator<"format_", In<"fmt", TS<Str>>, VarIn<"args", TsVar<"A">>, Scalar<"__sample__", Int>,
                   Scalar<"__strict__", Bool>, VarKwIn<"kwargs">, Out<TS<Str>>>
    {
    };
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_STRING_H
