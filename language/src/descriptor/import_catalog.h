#ifndef HGL_DESCRIPTOR_IMPORT_CATALOG_H
#define HGL_DESCRIPTOR_IMPORT_CATALOG_H

#include "descriptor/module_descriptor.h"
#include "descriptor/module_descriptor_reader.h"
#include "semantics/module_catalog.h"

#include <optional>

namespace hgl::descriptor
{
    /// Copy one already validated descriptor into the data-only semantic
    /// module catalog. Descriptor-local schema IDs never cross this boundary.
    [[nodiscard]] std::optional<ReadError> add_to_catalog(const ModuleDescriptor &descriptor, semantics::ModuleCatalog &catalog);
}  // namespace hgl::descriptor

#endif  // HGL_DESCRIPTOR_IMPORT_CATALOG_H
