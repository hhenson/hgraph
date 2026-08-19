#ifndef HGRAPH_PERSISTENCE_IMPL_OBJECT_STORE_IMPL_H
#define HGRAPH_PERSISTENCE_IMPL_OBJECT_STORE_IMPL_H

#include <hgraph/persistence/object_store.h>

namespace hgraph::persistence::store::impl
{
    [[nodiscard]] ObjectStore make_memory_object_store();
    [[nodiscard]] ObjectStore make_local_object_store(const LocalLocation &location);
    [[nodiscard]] ObjectStore make_s3_object_store(const S3Location &location);
}  // namespace hgraph::persistence::store::impl

#endif  // HGRAPH_PERSISTENCE_IMPL_OBJECT_STORE_IMPL_H
