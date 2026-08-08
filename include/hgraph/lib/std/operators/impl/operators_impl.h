#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_OPERATORS_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_OPERATORS_IMPL_H

/**
 * The standard operator **implementations**, grouped by family to mirror
 * the operator *definitions* under ``include/hgraph/lib/std/operators/``. Each definition
 * file ``<family>.h`` has a matching implementation file ``impl/<family>_impl.h`` that
 * provides the concrete overloads and a ``register_<family>_operators()`` function.
 * This header aggregates the implementation families for internal registration code and
 * implementation-level tests.
 *
 * Only a subset is implemented so far (scalar arithmetic / comparison / logical,
 * string, collection, stream basics, date components, ``const_`` / ``zero_`` /
 * ``debug_print`` / ``null_sink``); further families gain an ``impl/<family>_impl.h``
 * and a registration call as their implementations land. See
 * ``docs/source/developer_guide/operators.rst``.
 */

#include <hgraph/lib/std/operators/impl/arithmetic_impl.h>
#include <hgraph/lib/std/operators/impl/comparison_impl.h>
#include <hgraph/lib/std/operators/impl/container_impl.h>
#include <hgraph/lib/std/operators/impl/collection_impl.h>
#include <hgraph/lib/std/operators/impl/control_impl.h>
#include <hgraph/lib/std/operators/impl/conversion_impl.h>
#include <hgraph/lib/std/operators/impl/higher_order_impl.h>
#include <hgraph/lib/std/operators/impl/io_impl.h>
#include <hgraph/lib/std/operators/impl/json_impl.h>
#include <hgraph/lib/std/operators/impl/data_frame_impl.h>
#include <hgraph/lib/std/operators/impl/record_replay_frame_impl.h>
#include <hgraph/lib/std/operators/impl/record_replay_memory_impl.h>
#include <hgraph/lib/std/operators/impl/table_impl.h>
#include <hgraph/lib/std/operators/impl/logical_impl.h>
#include <hgraph/lib/std/operators/impl/numpy_impl.h>
#include <hgraph/lib/std/operators/impl/stream_impl.h>
#include <hgraph/lib/std/operators/impl/string_impl.h>
#include <hgraph/lib/std/operators/impl/temporal_impl.h>

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_OPERATORS_IMPL_H
