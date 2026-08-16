#ifndef HGRAPH_WEB_DETAIL_ROUTE_TABLE_H
#define HGRAPH_WEB_DETAIL_ROUTE_TABLE_H

#include <hgraph/web/types.h>

#include <cstddef>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace hgraph::web::detail {

/** The compiled per-method segment trie behind RFC 0024 routing.
 *
 * A pattern is '/'-separated and holds literal segments, `{name}` captures,
 * and an optional final rest segment spelled `*name`, which matches one or
 * more remaining segments.  Match precedence at every level is literal, then
 * `{name}`, then rest, with backtracking: a literal branch that dead-ends
 * deeper does not prevent the param branch from matching.
 *
 * A table is immutable once built.  The server compiles a fresh table from
 * the current route set and swaps a `std::shared_ptr<const RouteTable>`, so
 * matching holds no lock and mutates no shared state.
 */
class RouteTable {
public:
  struct Entry {
    HttpMethod method{};
    std::string pattern{};
  };

  /** `entry_index` indexes `entries()`; `params` are (name, decoded value)
   * pairs in pattern order. */
  struct Match {
    bool matched{};
    std::size_t entry_index{};
    std::vector<std::pair<std::string, std::string>> params{};
  };

  /** Compile `entries`, preserving their order as the match index space.
   * Throws std::invalid_argument for a malformed pattern or for two entries
   * of one method that would occupy the same route. */
  [[nodiscard]] static RouteTable build(std::vector<Entry> entries);

  /** The pattern grammar, reusable at wiring time.  Throws
   * std::invalid_argument naming the offending pattern. */
  static void validate_pattern(std::string_view pattern);

  /** Percent-decode a request path segment-wise ('/' separators are
   * preserved, '+' is not decoded).  Empty on a malformed escape — the
   * transport answers 400 for those (RFC 0024, routing). */
  [[nodiscard]] static std::optional<std::string>
  decode_path(std::string_view path);

  /** Match a raw request path (no query string).  Every incoming segment is
   * percent-decoded before comparison and capture; an invalid escape fails
   * the whole match, leaving the transport to answer 400. */
  [[nodiscard]] Match match(HttpMethod method, std::string_view path) const;

  [[nodiscard]] const std::vector<Entry> &entries() const noexcept {
    return entries_;
  }

private:
  static constexpr std::size_t npos = static_cast<std::size_t>(-1);

  struct Node {
    // Sorted by segment at build so matching binary-searches the literal
    // branch it must try first.
    std::vector<std::pair<std::string, std::size_t>> literals{};
    // Capture names for this node's own terminals, in pattern order.  Names
    // live on the terminal rather than on the edges, so two patterns sharing
    // a param edge may name that capture differently.
    std::vector<std::string> names{};
    std::vector<std::string> rest_names{};
    std::size_t param_child{npos};
    std::size_t terminal{npos};
    std::size_t rest_terminal{npos};
  };

  RouteTable() = default;

  [[nodiscard]] bool match_from(std::size_t node_index,
                                const std::vector<std::string> &segments,
                                std::size_t position,
                                std::vector<std::string> &captured,
                                Match &result) const;

  std::vector<Entry> entries_{};
  std::vector<Node> nodes_{};
  // Root node per HttpMethod value; npos where that method has no routes.
  std::vector<std::size_t> roots_{};
};

} // namespace hgraph::web::detail

#endif // HGRAPH_WEB_DETAIL_ROUTE_TABLE_H
