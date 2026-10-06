#ifndef HGL_DRIVER_REJECTION_SOURCE_H
#define HGL_DRIVER_REJECTION_SOURCE_H

#include "syntax/diagnostic.h"

#include <iosfwd>
#include <string>
#include <vector>

namespace hgl::driver
{
    struct RejectionExpectation
    {
        std::uint32_t    line{};
        syntax::Category category{};
        std::string      code{};
    };

    struct RejectionCase
    {
        syntax::SourceRange               range{};
        std::string                       name{};
        bool                              named_test{false};
        std::vector<RejectionExpectation> expectations{};
    };

    struct RejectionSource
    {
        bool                       valid{true};
        std::vector<RejectionCase> cases{};
    };

    // Discover metadata and reliable owner ranges before any frontend lowering.
    // Ranges refer to the original source and exclude unnamed context wrappers.
    [[nodiscard]] RejectionSource inspect_rejection_source(const syntax::SourceFile &file, std::ostream &errors);
}  // namespace hgl::driver

#endif
