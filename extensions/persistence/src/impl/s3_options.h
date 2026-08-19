#ifndef HGRAPH_PERSISTENCE_IMPL_S3_OPTIONS_H
#define HGRAPH_PERSISTENCE_IMPL_S3_OPTIONS_H

#include <hgraph/persistence/store_location.h>

#include <string>
#include <string_view>

#if defined(HGRAPH_PERSISTENCE_WITH_S3)
#include <arrow/filesystem/s3fs.h>
#endif

namespace hgraph::persistence::store::impl
{
    [[nodiscard]] std::string normalize_s3_prefix(std::string_view prefix);

#if defined(HGRAPH_PERSISTENCE_WITH_S3)
    /** Build the one Arrow S3 configuration used by every persistence facade. */
    [[nodiscard]] arrow::fs::S3Options make_s3_options(const S3Location &location);
#endif
}  // namespace hgraph::persistence::store::impl

#endif  // HGRAPH_PERSISTENCE_IMPL_S3_OPTIONS_H
