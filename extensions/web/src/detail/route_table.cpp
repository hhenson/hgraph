#include "route_table.h"

#include <algorithm>
#include <cstdint>
#include <stdexcept>
#include <string>

namespace hgraph::web::detail {
namespace {

// The underlying HttpMethod values index the root table directly.  A value
// outside this window is not a method this build knows about, and sizing an
// array from it would be worse than rejecting it.
constexpr std::int64_t max_method_value = 64;

[[nodiscard]] std::string quoted(std::string_view value) {
  std::string text;
  text.reserve(value.size() + 2);
  text.push_back('\'');
  text.append(value);
  text.push_back('\'');
  return text;
}

[[nodiscard]] std::invalid_argument pattern_error(std::string_view pattern,
                                                  std::string_view reason) {
  return std::invalid_argument("Web route pattern " + quoted(pattern) + ' ' +
                               std::string{reason});
}

/** Split a '/'-separated path or pattern into its segments.
 *
 * The leading '/' introduces the first segment rather than producing one, so
 * "/" yields a single empty segment and "/a/" yields {"a", ""}: a trailing
 * '/' is a segment of its own and stays significant when matching.
 */
[[nodiscard]] std::vector<std::string_view>
split_segments(std::string_view path) {
  std::vector<std::string_view> segments;
  std::size_t start = 1;
  while (true) {
    const auto slash = path.find('/', start);
    if (slash == std::string_view::npos) {
      segments.push_back(path.substr(start));
      return segments;
    }
    segments.push_back(path.substr(start, slash - start));
    start = slash + 1;
  }
}

[[nodiscard]] int hex_value(char character) noexcept {
  if (character >= '0' && character <= '9') {
    return character - '0';
  }
  if (character >= 'a' && character <= 'f') {
    return character - 'a' + 10;
  }
  if (character >= 'A' && character <= 'F') {
    return character - 'A' + 10;
  }
  return -1;
}

/** Percent-decode one path segment.  '+' is left alone: it means a space in
 * a query string, not in a path.  A truncated or non-hex escape returns
 * false, which fails the match rather than guessing at the intent. */
[[nodiscard]] bool percent_decode(std::string_view segment, std::string &out) {
  out.clear();
  out.reserve(segment.size());
  for (std::size_t index = 0; index != segment.size(); ++index) {
    if (segment[index] != '%') {
      out.push_back(segment[index]);
      continue;
    }
    if (index + 2 >= segment.size()) {
      return false;
    }
    const int high = hex_value(segment[index + 1]);
    const int low = hex_value(segment[index + 2]);
    if (high < 0 || low < 0) {
      return false;
    }
    out.push_back(static_cast<char>((high << 4) | low));
    index += 2;
  }
  return true;
}

[[nodiscard]] bool is_capture(std::string_view segment) noexcept {
  return !segment.empty() && segment.front() == '{';
}

[[nodiscard]] bool is_rest(std::string_view segment) noexcept {
  return !segment.empty() && segment.front() == '*';
}

[[nodiscard]] std::string_view capture_name(std::string_view segment) noexcept {
  return segment.substr(1, segment.size() - 2);
}

} // namespace

void RouteTable::validate_pattern(std::string_view pattern) {
  if (!pattern.starts_with('/')) {
    throw pattern_error(pattern, "must start with '/'");
  }
  const auto segments = split_segments(pattern);
  std::vector<std::string_view> names;
  for (std::size_t index = 0; index != segments.size(); ++index) {
    const auto segment = segments[index];
    const bool last = index + 1 == segments.size();
    std::string_view name;
    if (segment.empty()) {
      // A trailing '/' is a legitimate (and distinct) route, and "/" is the
      // root route; an empty segment anywhere else came from "//".
      if (!last) {
        throw pattern_error(pattern, "has an empty segment");
      }
      continue;
    }
    if (is_rest(segment)) {
      if (!last) {
        throw pattern_error(pattern, "may only use a rest segment last");
      }
      name = segment.substr(1);
      if (name.empty()) {
        throw pattern_error(pattern, "has a rest segment without a name");
      }
    } else if (is_capture(segment)) {
      if (segment.back() != '}' || segment.size() < 2) {
        throw pattern_error(
            pattern, "must spell a capture '{name}' as a whole segment");
      }
      name = capture_name(segment);
      if (name.empty()) {
        throw pattern_error(pattern, "has a capture without a name");
      }
      if (name.find('{') != std::string_view::npos ||
          name.find('}') != std::string_view::npos) {
        throw pattern_error(pattern, "has a capture name containing braces");
      }
    } else {
      if (segment.find('{') != std::string_view::npos ||
          segment.find('}') != std::string_view::npos) {
        throw pattern_error(
            pattern, "must spell a capture '{name}' as a whole segment");
      }
      continue;
    }
    if (std::find(names.begin(), names.end(), name) != names.end()) {
      throw pattern_error(pattern, "repeats the capture name " + quoted(name));
    }
    names.push_back(name);
  }
}

RouteTable RouteTable::build(std::vector<Entry> entries) {
  RouteTable table;
  table.entries_ = std::move(entries);

  std::size_t method_limit = 0;
  for (const auto &entry : table.entries_) {
    const auto method = static_cast<std::int64_t>(entry.method);
    if (method < 0 || method >= max_method_value) {
      throw pattern_error(entry.pattern, "names an unknown HTTP method");
    }
    method_limit = std::max(method_limit, static_cast<std::size_t>(method) + 1);
  }
  table.roots_.assign(method_limit, npos);

  for (std::size_t index = 0; index != table.entries_.size(); ++index) {
    const auto &entry = table.entries_[index];
    validate_pattern(entry.pattern);

    const auto method = static_cast<std::size_t>(entry.method);
    if (table.roots_[method] == npos) {
      table.roots_[method] = table.nodes_.size();
      table.nodes_.emplace_back();
    }

    std::size_t current = table.roots_[method];
    std::vector<std::string> names;
    bool rest = false;
    for (const auto segment : split_segments(entry.pattern)) {
      if (is_rest(segment)) {
        names.emplace_back(segment.substr(1));
        rest = true;
        break;
      }
      if (is_capture(segment)) {
        names.emplace_back(capture_name(segment));
        if (table.nodes_[current].param_child == npos) {
          const auto child = table.nodes_.size();
          table.nodes_.emplace_back();
          table.nodes_[current].param_child = child;
        }
        current = table.nodes_[current].param_child;
        continue;
      }
      std::size_t child = npos;
      for (const auto &literal : table.nodes_[current].literals) {
        if (literal.first == segment) {
          child = literal.second;
          break;
        }
      }
      if (child == npos) {
        child = table.nodes_.size();
        table.nodes_.emplace_back();
        table.nodes_[current].literals.emplace_back(std::string{segment},
                                                    child);
      }
      current = child;
    }

    auto &node = table.nodes_[current];
    auto &terminal = rest ? node.rest_terminal : node.terminal;
    if (terminal != npos) {
      // Two patterns reaching one terminal are the same route even when they
      // spell their captures differently, so this also catches an exactly
      // repeated (method, pattern) pair.
      throw std::invalid_argument(
          "Web route " + quoted(entry.pattern) + " duplicates " +
          quoted(table.entries_[terminal].pattern) + " for method " +
          std::string{enum_name(entry.method)});
    }
    terminal = index;
    (rest ? node.rest_names : node.names) = std::move(names);
  }

  for (auto &node : table.nodes_) {
    std::sort(node.literals.begin(), node.literals.end(),
              [](const auto &lhs, const auto &rhs) {
                return lhs.first < rhs.first;
              });
  }
  return table;
}

RouteTable::Match RouteTable::match(HttpMethod method,
                                    std::string_view path) const {
  Match result;
  const auto value = static_cast<std::int64_t>(method);
  if (value < 0 || static_cast<std::size_t>(value) >= roots_.size() ||
      roots_[static_cast<std::size_t>(value)] == npos) {
    return result;
  }
  if (!path.starts_with('/')) {
    return result;
  }

  const auto raw = split_segments(path);
  std::vector<std::string> segments(raw.size());
  for (std::size_t index = 0; index != raw.size(); ++index) {
    if (!percent_decode(raw[index], segments[index])) {
      return result;
    }
  }

  std::vector<std::string> captured;
  static_cast<void>(match_from(roots_[static_cast<std::size_t>(value)],
                               segments, 0, captured, result));
  return result;
}

bool RouteTable::match_from(std::size_t node_index,
                            const std::vector<std::string> &segments,
                            std::size_t position,
                            std::vector<std::string> &captured,
                            Match &result) const {
  const auto fill = [&result, &captured](std::size_t entry_index,
                                         const std::vector<std::string> &names) {
    result.matched = true;
    result.entry_index = entry_index;
    result.params.clear();
    const auto count = std::min(names.size(), captured.size());
    result.params.reserve(count);
    for (std::size_t index = 0; index != count; ++index) {
      result.params.emplace_back(names[index], captured[index]);
    }
  };

  const Node &node = nodes_[node_index];
  if (position == segments.size()) {
    if (node.terminal == npos) {
      return false;
    }
    fill(node.terminal, node.names);
    return true;
  }

  const std::string &segment = segments[position];
  const auto literal =
      std::lower_bound(node.literals.begin(), node.literals.end(), segment,
                       [](const auto &child, const std::string &value) {
                         return child.first < value;
                       });
  if (literal != node.literals.end() && literal->first == segment &&
      match_from(literal->second, segments, position + 1, captured, result)) {
    return true;
  }

  // A path's trailing '/' only matches a pattern's trailing '/': neither a
  // capture nor a rest segment absorbs the empty final segment it leaves.
  if (segment.empty() && position + 1 == segments.size()) {
    return false;
  }

  if (node.param_child != npos) {
    captured.push_back(segment);
    if (match_from(node.param_child, segments, position + 1, captured,
                   result)) {
      return true;
    }
    captured.pop_back();
  }

  if (node.rest_terminal != npos) {
    std::string remainder = segment;
    for (std::size_t index = position + 1; index != segments.size(); ++index) {
      remainder.push_back('/');
      remainder.append(segments[index]);
    }
    captured.push_back(std::move(remainder));
    fill(node.rest_terminal, node.rest_names);
    captured.pop_back();
    return true;
  }
  return false;
}

} // namespace hgraph::web::detail
