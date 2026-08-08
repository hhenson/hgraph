#ifndef HGRAPH_LIB_STD_STD_OPERATORS_H
#define HGRAPH_LIB_STD_STD_OPERATORS_H

/**
 * Convenience umbrella for the standard library operators.
 *
 * - The operator **definitions** (the abstract ``Operator<>`` markers) live under
 *   ``include/hgraph/lib/std/operators/``, grouped by family, and are aggregated by
 *   ``operators/operators.h``.
 * - The operator **implementations** live under
 *   ``include/hgraph/lib/std/operators/impl/`` and are compiled into the
 *   ``hgraph_stdlib`` target. Include those headers directly only when tests
 *   or extensions need implementation details.
 *
 * Including this header pulls in definitions, registration, and the opt-in wiring expression syntax under
 * ``hgraph::stdlib::syntax``. See ``docs/source/developer_guide/operators.rst``.
 */

#include <hgraph/lib/std/lifted_kernels.h>
#include <hgraph/lib/std/lower.h>
#include <hgraph/lib/std/operators/operators.h>
#include <hgraph/lib/std/operators/registration.h>
#include <hgraph/lib/std/operators/syntax.h>
#include <hgraph/types/lift.h>

#endif  // HGRAPH_LIB_STD_STD_OPERATORS_H
