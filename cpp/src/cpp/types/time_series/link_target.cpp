#include <hgraph/types/time_series/link_target.h>
#include <hgraph/types/time_series/ts_input.h>

namespace hgraph {

void LinkTarget::ActiveNotifier::notify(engine_time_t et) {
    if (owning_input) {
        owning_input->notify(et);
    }
}

LinkTarget::~LinkTarget() {
    cleanup_ref_binding();
}

} // namespace hgraph
