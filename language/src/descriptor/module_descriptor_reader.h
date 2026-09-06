#ifndef HGL_DESCRIPTOR_MODULE_DESCRIPTOR_READER_H
#define HGL_DESCRIPTOR_MODULE_DESCRIPTOR_READER_H

#include "descriptor/module_descriptor.h"

#include <optional>
#include <string>
#include <string_view>

namespace hgl::descriptor
{
    struct ReadError
    {
        std::string path{};
        std::string message{};

        friend bool operator==(const ReadError &, const ReadError &) = default;
    };

    struct ReadResult
    {
        std::optional<ModuleDescriptor> value{};
        std::optional<ReadError>        error{};

        [[nodiscard]] explicit operator bool() const noexcept { return value.has_value(); }
    };

    /// Parse and validate one versioned HGL module descriptor without loading
    /// native code or consulting a registry. Unknown object members are
    /// ignored within a supported format version; duplicate members, invalid
    /// enums and dangling schema references are rejected.
    [[nodiscard]] ReadResult read_json(std::string_view json);

    /// Validate a descriptor assembled by another producer. This applies the
    /// same semantic integrity rules as read_json after JSON decoding.
    [[nodiscard]] std::optional<ReadError> validate(const ModuleDescriptor &descriptor);
}  // namespace hgl::descriptor

#endif  // HGL_DESCRIPTOR_MODULE_DESCRIPTOR_READER_H
