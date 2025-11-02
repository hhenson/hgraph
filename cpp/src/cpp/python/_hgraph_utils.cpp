#include <hgraph/util/lifecycle.h>
#include <hgraph/util/date_time.h>
#include <hgraph/python/global_keys.h>

void export_utils(nb::module_ &m) {
    using namespace hgraph;

    ComponentLifeCycle::register_with_nanobind(m);
    OutputKeyBuilder::register_with_nanobind(m);

    // Expose date/time constants
    m.attr("MIN_DT") = MIN_DT;
    m.attr("MAX_DT") = MAX_DT;
    m.attr("MIN_ST") = MIN_ST;
    m.attr("MAX_ET") = MAX_ET;
    m.attr("MIN_TD") = MIN_TD;
}