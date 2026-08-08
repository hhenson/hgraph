#ifndef HGRAPH_LIB_STD_OPERATORS_OPERATORS_H
#define HGRAPH_LIB_STD_OPERATORS_OPERATORS_H

/**
 * The standard operator **definitions** — the abstract ``Operator<>`` markers for the
 * ``hgraph`` standard library, grouped by family. These declare the operator names and
 * documentary signatures only; the implementations are registered separately (see
 * ``register_standard_operators`` in ``<hgraph/lib/std/std_operators.h>``).
 *
 * The catalogue mirrors the Python ``hgraph`` operators. JSON, table, and
 * data-frame operators use the native value-layer scalar types.
 *
 * See ``docs/source/developer_guide/operators.rst``.
 */

#include <hgraph/lib/std/operators/arithmetic.h>
#include <hgraph/lib/std/operators/collection.h>
#include <hgraph/lib/std/operators/comparison.h>
#include <hgraph/lib/std/operators/container.h>
#include <hgraph/lib/std/operators/control.h>
#include <hgraph/lib/std/operators/conversion.h>
#include <hgraph/lib/std/operators/data_frame.h>
#include <hgraph/lib/std/operators/higher_order.h>
#include <hgraph/lib/std/operators/io.h>
#include <hgraph/lib/std/operators/json.h>
#include <hgraph/lib/std/operators/table.h>
#include <hgraph/lib/std/operators/logical.h>
#include <hgraph/lib/std/operators/numpy.h>
#include <hgraph/lib/std/operators/stream.h>
#include <hgraph/lib/std/operators/string.h>
#include <hgraph/lib/std/operators/temporal.h>

#endif  // HGRAPH_LIB_STD_OPERATORS_OPERATORS_H
