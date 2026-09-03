#include "syntax/diagnostic.h"

#include <algorithm>
#include <sstream>

namespace hgl::syntax
{
    std::string_view category_name(Category category) noexcept
    {
        switch (category)
        {
            case Category::Parse: return "parse";
            case Category::Name: return "name";
            case Category::Type: return "type";
            case Category::Shape: return "shape";
            case Category::FunctionKind: return "function-kind";
            case Category::Phase: return "phase";
            case Category::Injectable: return "injectable";
            case Category::Operator: return "operator";
            case Category::Module: return "module";
            case Category::Build: return "build";
            case Category::Backend: return "backend";
            case Category::Test: return "test";
        }
        return "diagnostic";
    }

    namespace
    {
        void render_line(std::ostringstream &out, const SourceFile &file, SourceRange range, std::string_view label,
                         std::string_view message)
        {
            const Location at = file.location(range.begin);
            out << file.path() << ':' << at.line << ':' << at.column << ": " << label << ": " << message << '\n';
            const std::string_view line = file.line_text(at.line);
            if (line.empty()) { return; }
            out << "    " << line << '\n';
            out << "    ";
            for (std::uint32_t i = 1; i < at.column; ++i) { out << (line[i - 1] == '\t' ? '\t' : ' '); }
            // Mark the range on this line only; a multi-line range shows its first line.
            const Location end   = file.location(range.end);
            std::uint32_t  width = 1;
            if (end.line == at.line && end.column > at.column) { width = end.column - at.column; }
            for (std::uint32_t i = 0; i < width; ++i) { out << '^'; }
            out << '\n';
        }
    }  // namespace

    std::string render_diagnostic(const SourceFile &file, const Diagnostic &diagnostic)
    {
        std::ostringstream out;
        render_line(out, file, diagnostic.range, category_name(diagnostic.category), diagnostic.message);
        for (const Note &note : diagnostic.notes) { render_line(out, file, note.range, "note", note.message); }
        return out.str();
    }

    std::string DiagnosticSink::render(const SourceFile &file) const
    {
        // Emission order interleaves lexer and parser diagnostics; readers
        // want source order.
        std::vector<const Diagnostic *> ordered;
        ordered.reserve(diagnostics_.size());
        for (const Diagnostic &diagnostic : diagnostics_) { ordered.push_back(&diagnostic); }
        std::stable_sort(ordered.begin(), ordered.end(),
                         [](const Diagnostic *a, const Diagnostic *b) { return a->range.begin < b->range.begin; });
        std::string out;
        for (const Diagnostic *diagnostic : ordered) { out += render_diagnostic(file, *diagnostic); }
        return out;
    }
}  // namespace hgl::syntax
