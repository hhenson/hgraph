#include "syntax/documentation.h"
#include "syntax/ast.h"
#include <algorithm>
#include <cctype>
#include <map>
#include <set>
#include <sstream>

namespace hgl::syntax
{
    namespace
    {
        std::string trim(std::string_view s) {
            auto first = s.find_first_not_of(" \t\r\n");
            return first == s.npos ? "" : std::string{s.substr(first, s.find_last_not_of(" \t\r\n") - first + 1)};
        }
        std::string compact(std::string_view s) {
            std::string out;
            char        quote   = 0;
            bool        escaped = false;
            for (unsigned char c : s) {
                if (quote) {
                    out += static_cast<char>(c);
                    if (escaped) escaped = false;
                    else if (c == '\\')
                        escaped = true;
                    else if (c == quote)
                        quote = 0;
                } else if (c == '\"' || c == '\'') {
                    quote = static_cast<char>(c);
                    out += quote;
                } else if (!std::isspace(c))
                    out += static_cast<char>(c);
            }
            return out;
        }
        std::vector<std::string> lines(std::string_view text) {
            std::istringstream       input{std::string{text}};
            std::vector<std::string> out;
            for (std::string line; std::getline(input, line);) {
                if (!line.empty() && line.back() == '\r') line.pop_back();
                out.push_back(std::move(line));
            }
            return out;
        }
        std::string normalize(std::string_view raw) {
            auto body = lines(raw.substr(3, raw.size() - 5));
            if (body.empty()) return {};
            body[0]            = trim(body[0]);
            std::size_t indent = std::string::npos;
            for (std::size_t i = 1; i < body.size(); ++i) {
                if (!trim(body[i]).empty()) indent = std::min(indent, body[i].find_first_not_of(" \t"));
            }
            if (indent != std::string::npos) {
                for (std::size_t i = 1; i < body.size(); ++i) body[i].erase(0, std::min(indent, body[i].size()));
            }
            while (!body.empty() && trim(body.front()).empty()) body.erase(body.begin());
            while (!body.empty() && trim(body.back()).empty()) body.pop_back();
            std::string out;
            for (const auto &line : body) {
                if (!out.empty()) out += '\n';
                out += line;
            }
            return out;
        }
        struct Target
        {
            SourceRange                                  range;
            std::string                                  name;
            std::set<std::string>                        args, types, requirements;
            std::map<std::string, std::set<std::string>> properties;
            std::uint32_t                                signature_end;
        };
        bool heading(std::string_view s) {
            static const std::set<std::string_view> names{"Args:",     "Type Args:",  "Returns:",  "Raises:", "Ticks:",
                                                          "Validity:", "Properties:", "Requires:", "Notes:",  "Examples:"};
            return names.contains(s);
        }
        void validate(const Documentation &doc, const Target &target, DiagnosticSink &diagnostics) {
            std::string section, domain;
            for (const auto &line : lines(doc.text)) {
                if (heading(line)) {
                    section = line;
                    domain.clear();
                    continue;
                }
                const auto indent = line.find_first_not_of(' ');
                if (indent == 0) {
                    section.clear();
                    continue;
                }
                if (indent != 4 && !(section == "Properties:" && indent == 8)) continue;
                const auto content = trim(line);
                auto       colon   = content.find(':');
                if (section == "Requires:" || (section == "Properties:" && indent == 4)) colon = content.rfind(':');
                if (colon == content.npos) continue;
                const auto key   = compact(content.substr(0, colon));
                bool       valid = true;
                if (section == "Args:") valid = target.args.contains(key);
                else if (section == "Type Args:")
                    valid = target.types.contains(key);
                else if (section == "Requires:")
                    valid = target.requirements.contains(key);
                else if (section == "Properties:") {
                    if (indent == 4) {
                        domain = key;
                        valid  = target.properties.contains(domain);
                    } else {
                        auto found = target.properties.find(domain);
                        valid      = found != target.properties.end() && found->second.contains(key);
                    }
                }
                if (!valid)
                    diagnostics.report(Category::Name, doc.comment, "documentation " + section + " unknown key '" + key + "'");
            }
        }
    }  // namespace
    void capture_documentation(const SourceFile &file, ast::Module &module, DiagnosticSink &diagnostics) {
        std::vector<Target> targets;
        std::string         prefix, part;
        for (const auto &decl : module.decls) {
            Target target{decl.range, {}, {}, {}, {}, {}, decl.range.end};
            bool   admitted = true;
            std::visit(
                [&](const auto &node) {
                    using T = std::decay_t<decltype(node)>;
                    if constexpr (std::is_same_v<T, ast::ModuleDecl>) {
                        for (const auto &name : node.path) {
                            if (!prefix.empty()) prefix += '.';
                            prefix += name.text;
                        }
                        part        = node.part.text;
                        target.name = prefix;
                    } else if constexpr (std::is_same_v<T, ast::StructDecl> || std::is_same_v<T, ast::EnumDecl> ||
                                         std::is_same_v<T, ast::OperatorDecl> || std::is_same_v<T, ast::FunctionDecl> ||
                                         std::is_same_v<T, ast::NativeFunctionDecl> || std::is_same_v<T, ast::NativeTypeDecl> ||
                                         std::is_same_v<T, ast::TestDecl>) {
                        target.name = prefix + "." + std::string{node.name.text};
                        if constexpr (requires { node.generics; })
                            for (const auto &g : node.generics) target.types.emplace(g.name.text);
                        if constexpr (requires { node.signature; })
                            for (const auto &p : node.signature.parameters) target.args.emplace(p.name.text);
                        if constexpr (requires { node.requirements; }) {
                            if (node.requirements != ast::no_node) {
                                const auto collect = [&](const auto &self, ast::ConstraintId id) -> void {
                                    const auto &constraint = module.constraint(id);
                                    target.requirements.insert(compact(file.slice(constraint.range)));
                                    if (const auto *logic = std::get_if<ast::ConstraintLogic>(&constraint.node)) {
                                        self(self, logic->lhs);
                                        self(self, logic->rhs);
                                    }
                                };
                                collect(collect, node.requirements);
                            }
                        }
                        if constexpr (std::is_same_v<T, ast::OperatorDecl>) {
                            for (const auto &p : node.properties) {
                                std::string domain = "<";
                                for (auto id : p.domain) {
                                    if (domain.size() > 1) domain += ',';
                                    domain += compact(file.slice(module.type(id).range));
                                }
                                domain += '>';
                                for (const auto &e : p.entries) target.properties[domain].emplace(e.name.text);
                            }
                        }
                        if constexpr (std::is_same_v<T, ast::StructDecl>) {
                            for (const auto &member : node.members) {
                                if (const auto *field = std::get_if<ast::StructField>(&member)) {
                                    SourceRange range{field->name.range.begin, field->default_value == ast::no_node
                                                                                   ? module.type(field->type).range.end
                                                                                   : module.expr(field->default_value).range.end};
                                    targets.push_back(Target{
                                        range, target.name + "." + std::string{field->name.text}, {}, {}, {}, {}, range.end});
                                }
                            }
                        }
                        // Body delimiters are excluded; operator property blocks are part of the declaration.
                        if constexpr (!std::is_same_v<T, ast::OperatorDecl>) {
                            auto after = node.name.range.end;
                            if constexpr (requires { node.generics; }) {
                                for (const auto &g : node.generics) {
                                    after = std::max(after, g.name.range.end);
                                    if (g.type != ast::no_node) after = std::max(after, module.type(g.type).range.end);
                                }
                            }
                            if constexpr (requires { node.parents; }) {
                                for (auto parent : node.parents) after = std::max(after, module.type(parent).range.end);
                            }
                            if constexpr (requires { node.signature; }) {
                                for (const auto &p : node.signature.parameters) {
                                    after = std::max(after, module.type(p.type).range.end);
                                    if (p.default_value != ast::no_node)
                                        after = std::max(after, module.expr(p.default_value).range.end);
                                }
                                if (node.signature.result != ast::no_node)
                                    after = std::max(after, module.type(node.signature.result).range.end);
                            }
                            if constexpr (requires { node.requirements; }) {
                                if (node.requirements != ast::no_node)
                                    after = std::max(after, module.constraint(node.requirements).range.end);
                            }
                            const auto text    = file.slice({after, decl.range.end});
                            auto       end     = text.find('{');
                            auto       concise = text.find("=>");
                            if (concise < end) end = concise;
                            if (end != text.npos) target.signature_end = after + static_cast<std::uint32_t>(end);
                        }
                    } else
                        admitted = false;
                },
                decl.node);
            if (admitted) targets.push_back(std::move(target));
        }
        std::set<std::uint32_t> attached;
        for (const auto &comment : module.comments) {
            const auto raw = file.slice(comment.range);
            if (!raw.starts_with("/**") || raw.size() < 5) continue;
            const Target *found = nullptr;
            for (const auto &target : targets) {
                if (target.range.begin >= comment.range.end && trim(file.slice({comment.range.end, target.range.begin})).empty() &&
                    (!found || target.range.begin < found->range.begin))
                    found = &target;
            }
            if (!found || !attached.insert(found->range.begin).second) {
                diagnostics.report(Category::Parse, comment.range, "documentation must immediately precede a declaration");
                continue;
            }
            Documentation doc{found->name,    trim(file.slice({found->range.begin, found->signature_end})),
                              normalize(raw), part,
                              found->range,   comment.range};
            validate(doc, *found, diagnostics);
            module.documentation.push_back(std::move(doc));
        }
    }
    std::string documentation_rst(const std::vector<Documentation> &docs) {
        std::string out;
        for (const auto &doc : docs) {
            auto title = doc.name + (doc.part.empty() ? "" : " (" + doc.part + ")");
            out += title + "\n" + std::string(title.size(), '-') + "\n\n.. code-block:: text\n\n";
            for (const auto &line : lines(doc.declaration)) out += "    " + line + "\n";
            out += '\n';
            std::string section;
            for (auto line : lines(doc.text)) {
                if (heading(line)) {
                    out += "\n.. rubric:: " + line.substr(0, line.size() - 1) + "\n\n";
                    section = line;
                } else {
                    if (!line.empty() && line.front() != ' ') section.clear();
                    if (!section.empty() && line.starts_with("    ")) line.erase(0, 4);
                    if ((section == "Args:" || section == "Type Args:") && !line.starts_with(' ')) {
                        const auto colon = line.find(':');
                        if (colon != line.npos) {
                            out += "``" + line.substr(0, colon) + "``\n    " + trim(line.substr(colon + 1)) + "\n";
                            continue;
                        }
                    }
                    if ((section == "Properties:" || section == "Requires:") && line.ends_with(':')) line.pop_back();
                    out += line + '\n';
                }
            }
            out += '\n';
        }
        return out;
    }
    std::string documentation_comments(const std::vector<Documentation> &docs) {
        std::string out;
        if (!docs.empty()) out += "// clang-format off\n";
        for (const auto &doc : docs) {
            out += "// HGL documentation: " + doc.name + "\n";
            for (const auto &line : lines(doc.declaration + "\n" + doc.text)) {
                // A fixed suffix prevents C++ backslash-newline splicing.
                out += "// " + line + " |\n";
            }
        }
        if (!docs.empty()) out += "// clang-format on\n";
        return out;
    }
}  // namespace hgl::syntax
