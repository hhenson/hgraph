#ifndef HGRAPH_LIB_STD_OPERATORS_NUMPY_H
#define HGRAPH_LIB_STD_OPERATORS_NUMPY_H

#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>

namespace hgraph::stdlib::numpy
{
    /** Convert a fixed tick window into a shaped array, optionally padding with ``zero``. */
    struct as_array : Operator<"as_array", In<"tsw", TsVar<"W">>, Out<TsVar<"__out__">>>
    {
    };

    /** Apply a wiring-time integer or integer-tuple index to a shaped array. */
    struct get_item : Operator<"get_item", In<"ts", TsVar<"A">>,
                               Scalar<"idx", ScalarVar<"I">>, Out<TsVar<"__out__">>>
    {
    };

    /** Native cumulative sum. The optional scalar axis is supplied as a second argument. */
    struct cumsum : Operator<"cumsum", In<"a", TsVar<"A">>, Out<TsVar<"__out__">>>
    {
    };

    /** Native correlation coefficients, with an optional second array. */
    struct corrcoef : Operator<"corrcoef", In<"x", TsVar<"X">>, Scalar<"rowvar", Bool>,
                              Out<TsVar<"__out__">>>
    {
    };

    /** Native scalar quantile over an array or time-series window. */
    struct quantile : Operator<"quantile", In<"a", TsVar<"A">>, In<"q", TS<Float>>,
                              Scalar<"method", Str>, Scalar<"keepdims", Bool>, Out<TS<Float>>>
    {
    };

    /** Materialize a window's values and evaluation timestamps as shaped arrays. */
    struct rolling_window_arrays
        : Operator<"rolling_window_arrays", In<"window", TsVar<"W">>,
                   Out<TsVar<"__out__">>>
    {
    };

    /** Population/sample standard deviation over a numeric shaped array. */
    struct standard_deviation
        : Operator<"np_std", In<"ts", TsVar<"A">>, Scalar<"ddof", Int>,
                   Out<TS<Float>>>
    {
    };
}  // namespace hgraph::stdlib::numpy

#endif  // HGRAPH_LIB_STD_OPERATORS_NUMPY_H
