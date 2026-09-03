#ifndef HGL_SYNTAX_DIAGNOSTIC_H
#define HGL_SYNTAX_DIAGNOSTIC_H

#include "syntax/source.h"

#include <string>
#include <string_view>
#include <vector>

namespace hgl::syntax
{
    /// The diagnostic categories of the developer guide ("Diagnostics").
    enum class Category
    {
        Parse,
        Name,
        Type,
        Shape,
        FunctionKind,
        Phase,
        Injectable,
        Operator,
        Module,
        Build,
        Backend,
        Test,
    };

    [[nodiscard]] std::string_view category_name(Category category) noexcept;

    struct Note
    {
        std::string message;
        SourceRange range{};
    };

    struct Diagnostic
    {
        Category          category{Category::Parse};
        std::string       message;
        SourceRange       range{};
        std::vector<Note> notes{};
    };

    /// Collects diagnostics in source order of emission. Every category is an
    /// error in the first slice; there are no warnings yet.
    class DiagnosticSink
    {
      public:
        Diagnostic &report(Category category, SourceRange range, std::string message)
        {
            diagnostics_.push_back(Diagnostic{category, std::move(message), range, {}});
            return diagnostics_.back();
        }

        [[nodiscard]] bool has_errors() const noexcept { return !diagnostics_.empty(); }
        [[nodiscard]] std::size_t size() const noexcept { return diagnostics_.size(); }
        [[nodiscard]] const std::vector<Diagnostic> &diagnostics() const noexcept { return diagnostics_; }

        /// `path:line:col: category: message`, one line per diagnostic and
        /// note, followed by the source line and a caret marker.
        [[nodiscard]] std::string render(const SourceFile &file) const;

      private:
        std::vector<Diagnostic> diagnostics_;
    };

    [[nodiscard]] std::string render_diagnostic(const SourceFile &file, const Diagnostic &diagnostic);
}  // namespace hgl::syntax

#endif  // HGL_SYNTAX_DIAGNOSTIC_H
