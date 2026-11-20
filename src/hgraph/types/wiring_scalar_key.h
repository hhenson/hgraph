#pragma once

#include <hgraph/hgraph_export.h>

#include <atomic>
#include <optional>

namespace hgraph {
class ValueView;

namespace detail {
// A storage provider can compare an opaque, retained configuration without
// projecting fields. An empty result leaves the ordinary native view in charge.
using StoredZeroSignsMatch = std::optional<bool> (*)(const ValueView &, const ValueView &);
HGRAPH_EXPORT std::atomic<StoredZeroSignsMatch> &stored_zero_signs_comparison() noexcept;
}
}
