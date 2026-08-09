#ifndef HGRAPH_LIB_STD_OPERATORS_REGISTRATION_H
#define HGRAPH_LIB_STD_OPERATORS_REGISTRATION_H

#include <hgraph/hgraph_export.h>

namespace hgraph::stdlib
{
    /**
     * Register the standard operator overloads. Call once per registry lifetime
     * before wiring graphs that use the standard library operators.
     */
    HGRAPH_EXPORT void register_standard_operators();
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_REGISTRATION_H
