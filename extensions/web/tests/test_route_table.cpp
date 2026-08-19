// Route-table tests: the pattern grammar, percent-decoded captures, and the
// literal > {param} > rest precedence with backtracking (RFC 0024, routing).

#include "detail/route_table.h"

#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {
using namespace hgraph::web;
using hgraph::web::detail::RouteTable;

void require(bool condition, std::string message) {
  if (!condition) {
    throw std::runtime_error(std::move(message));
  }
}

template <typename Fn> void require_invalid(Fn &&fn, std::string message) {
  bool rejected = false;
  try {
    std::forward<Fn>(fn)();
  } catch (const std::invalid_argument &) {
    rejected = true;
  }
  require(rejected, std::move(message));
}

[[nodiscard]] RouteTable table_of(std::vector<RouteTable::Entry> entries) {
  return RouteTable::build(std::move(entries));
}

[[nodiscard]] std::string param(const RouteTable::Match &match,
                                std::string_view name) {
  for (const auto &captured : match.params) {
    if (captured.first == name) {
      return captured.second;
    }
  }
  return "<absent>";
}

void test_literal_routes() {
  const auto table = table_of({
      {HttpMethod::Get, "/"},
      {HttpMethod::Get, "/health"},
      {HttpMethod::Get, "/api/orders"},
  });
  require(table.entries().size() == 3, "entries are retained in build order");
  require(table.entries()[2].pattern == "/api/orders",
          "entry patterns round-trip");

  const auto health = table.match(HttpMethod::Get, "/health");
  require(health.matched && health.entry_index == 1, "a literal route matches");
  require(health.params.empty(), "a literal route captures nothing");
  require(table.match(HttpMethod::Get, "/").entry_index == 0,
          "the root route matches '/'");
  require(table.match(HttpMethod::Get, "/api/orders").entry_index == 2,
          "a multi-segment literal route matches");
}

void test_no_match_cases() {
  const auto table = table_of({
      {HttpMethod::Get, "/api/orders"},
  });
  require(!table.match(HttpMethod::Post, "/api/orders").matched,
          "a route is not reachable through another method");
  require(!table.match(HttpMethod::Get, "/api").matched,
          "a path shorter than the pattern does not match");
  require(!table.match(HttpMethod::Get, "/api/orders/7").matched,
          "a path deeper than the pattern does not match");
  require(!table.match(HttpMethod::Get, "/api/other").matched,
          "an unknown path does not match");
  require(!table.match(HttpMethod::Get, "api/orders").matched,
          "a path without a leading '/' does not match");
}

void test_capture_decoding() {
  const auto table = table_of({
      {HttpMethod::Get, "/orders/{id}"},
  });

  const auto upper = table.match(HttpMethod::Get, "/orders/%41%2F");
  require(upper.matched && param(upper, "id") == "A/",
          "captures are delivered percent-decoded");
  const auto lower = table.match(HttpMethod::Get, "/orders/%41%2f");
  require(lower.matched && param(lower, "id") == "A/",
          "percent escapes accept lower-case hex");
  const auto plus = table.match(HttpMethod::Get, "/orders/a+b");
  require(plus.matched && param(plus, "id") == "a+b",
          "'+' is not a space in a path");

  require(!table.match(HttpMethod::Get, "/orders/%zz").matched,
          "a non-hex escape fails the match");
  require(!table.match(HttpMethod::Get, "/orders/%4").matched,
          "a truncated escape fails the match");
  require(!table.match(HttpMethod::Get, "/orders/").matched,
          "a capture does not absorb an empty final segment");
}

void test_match_decoded_does_not_decode_again() {
  // The transport percent-decodes once at the header boundary. Feeding that
  // result back through match() re-reads a literal '%' as a broken escape, so
  // a static mount named "/100%.txt" (requested as "/100%25.txt", decoded to
  // "/100%.txt") could never be served.
  const auto table = table_of({
      {HttpMethod::Get, "/100%.txt"},
      {HttpMethod::Get, "/plain.txt"},
  });

  require(!table.match(HttpMethod::Get, "/100%.txt").matched,
          "match() rejects an already-decoded literal '%' as a bad escape");
  require(table.match_decoded(HttpMethod::Get, "/100%.txt").matched,
          "match_decoded() serves a path whose decoded name contains '%'");

  // Ordinary paths behave identically through both entry points.
  require(table.match_decoded(HttpMethod::Get, "/plain.txt").matched,
          "match_decoded() matches ordinary literals");
  require(!table.match_decoded(HttpMethod::Get, "/absent.txt").matched,
          "match_decoded() still reports a genuine miss");
  require(!table.match_decoded(HttpMethod::Post, "/plain.txt").matched,
          "match_decoded() honours the method");
  require(!table.match_decoded(HttpMethod::Get, "relative").matched,
          "match_decoded() requires an absolute path");

  // And it must NOT decode: an escape stays literal rather than collapsing.
  require(!table.match_decoded(HttpMethod::Get, "/100%25.txt").matched,
          "match_decoded() leaves escapes alone instead of decoding them");
}

void test_multiple_captures() {
  const auto table = table_of({
      {HttpMethod::Get, "/api/{version}/orders/{id}/items"},
  });
  const auto match = table.match(HttpMethod::Get, "/api/v2/orders/%37/items");
  require(match.matched, "a multi-capture route matches");
  require(match.params.size() == 2, "every capture is delivered");
  require(match.params[0].first == "version" && match.params[0].second == "v2",
          "captures arrive in pattern order");
  require(match.params[1].first == "id" && match.params[1].second == "7",
          "the second capture is decoded");
}

void test_rest_capture() {
  const auto table = table_of({
      {HttpMethod::Get, "/static/*path"},
  });
  const auto deep = table.match(HttpMethod::Get, "/static/css/site.css");
  require(deep.matched && param(deep, "path") == "css/site.css",
          "a rest segment spans the remaining path");
  const auto decoded = table.match(HttpMethod::Get, "/static/%61/b%2Fc");
  require(decoded.matched && param(decoded, "path") == "a/b/c",
          "rest segments join their decoded segments with '/'");
  const auto single = table.match(HttpMethod::Get, "/static/one");
  require(single.matched && param(single, "path") == "one",
          "a rest segment matches a single remaining segment");

  require(!table.match(HttpMethod::Get, "/static").matched,
          "a rest segment needs at least one remaining segment");
  require(!table.match(HttpMethod::Get, "/static/").matched,
          "a rest segment does not absorb an empty final segment");
}

void test_precedence() {
  const auto table = table_of({
      {HttpMethod::Get, "/a/exact"},
      {HttpMethod::Get, "/a/{name}"},
      {HttpMethod::Get, "/a/*rest"},
  });
  require(table.match(HttpMethod::Get, "/a/exact").entry_index == 0,
          "a literal segment wins over a capture and a rest segment");
  const auto captured = table.match(HttpMethod::Get, "/a/other");
  require(captured.entry_index == 1 && param(captured, "name") == "other",
          "a capture wins over a rest segment");
  const auto rest = table.match(HttpMethod::Get, "/a/x/y");
  require(rest.entry_index == 2 && param(rest, "rest") == "x/y",
          "a rest segment matches what the other branches cannot");
  // The literal branch is entered first and dead-ends: 'exact' has no deeper
  // route, so matching falls back through the capture to the rest segment.
  const auto fallback = table.match(HttpMethod::Get, "/a/exact/more");
  require(fallback.entry_index == 2 && param(fallback, "rest") == "exact/more",
          "a dead-ended literal branch still reaches the rest segment");
}

void test_backtracking() {
  const auto table = table_of({
      {HttpMethod::Get, "/a/b/c"},
      {HttpMethod::Get, "/a/{x}/d"},
  });
  const auto backtracked = table.match(HttpMethod::Get, "/a/b/d");
  require(backtracked.matched && backtracked.entry_index == 1,
          "a literal branch that dead-ends deeper falls back to the capture");
  require(param(backtracked, "x") == "b",
          "the backtracked capture holds the segment the literal claimed");
  require(table.match(HttpMethod::Get, "/a/b/c").entry_index == 0,
          "the literal branch still wins where it reaches its terminal");
  const auto other = table.match(HttpMethod::Get, "/a/z/d");
  require(other.entry_index == 1 && param(other, "x") == "z",
          "a non-literal segment takes the capture directly");
  require(!table.match(HttpMethod::Get, "/a/b/e").matched,
          "exhausting both branches fails the match");
}

void test_method_separation() {
  const auto table = table_of({
      {HttpMethod::Get, "/orders"},
      {HttpMethod::Post, "/orders"},
  });
  require(table.match(HttpMethod::Get, "/orders").entry_index == 0,
          "one pattern per method is a distinct entry (Get)");
  require(table.match(HttpMethod::Post, "/orders").entry_index == 1,
          "one pattern per method is a distinct entry (Post)");
  require(!table.match(HttpMethod::Put, "/orders").matched,
          "an unregistered method matches nothing");
}

void test_trailing_slash_is_significant() {
  const auto both = table_of({
      {HttpMethod::Get, "/a"},
      {HttpMethod::Get, "/a/"},
  });
  require(both.match(HttpMethod::Get, "/a").entry_index == 0,
          "'/a' selects the pattern without a trailing slash");
  require(both.match(HttpMethod::Get, "/a/").entry_index == 1,
          "'/a/' selects the pattern with a trailing slash");

  const auto bare = table_of({{HttpMethod::Get, "/a"}});
  require(!bare.match(HttpMethod::Get, "/a/").matched,
          "a trailing slash is not stripped from the request path");
  const auto slashed = table_of({{HttpMethod::Get, "/a/"}});
  require(!slashed.match(HttpMethod::Get, "/a").matched,
          "a trailing slash is not added to the request path");
}

void test_root_routes() {
  const auto root = table_of({{HttpMethod::Get, "/"}});
  require(root.match(HttpMethod::Get, "/").matched, "'/' is a valid route");
  require(!root.match(HttpMethod::Get, "/x").matched,
          "the root route does not swallow deeper paths");
  require(!root.match(HttpMethod::Get, "").matched,
          "an empty path matches nothing");

  const auto catch_all = table_of({{HttpMethod::Get, "/*rest"}});
  const auto match = catch_all.match(HttpMethod::Get, "/x/y");
  require(match.matched && param(match, "rest") == "x/y",
          "a root rest segment spans the whole path");
  require(!catch_all.match(HttpMethod::Get, "/").matched,
          "a root rest segment does not match the empty root segment");
}

void test_pattern_validation() {
  RouteTable::validate_pattern("/");
  RouteTable::validate_pattern("/a/");
  RouteTable::validate_pattern("/health");
  RouteTable::validate_pattern("/api/orders/{id}");
  RouteTable::validate_pattern("/{a}/{b}");
  RouteTable::validate_pattern("/static/*path");

  require_invalid([] { RouteTable::validate_pattern("health"); },
                  "patterns must start with '/'");
  require_invalid([] { RouteTable::validate_pattern(""); },
                  "an empty pattern is rejected");
  require_invalid([] { RouteTable::validate_pattern("//"); },
                  "'//' is rejected");
  require_invalid([] { RouteTable::validate_pattern("/a//b"); },
                  "an empty intermediate segment is rejected");
  require_invalid([] { RouteTable::validate_pattern("/{}"); },
                  "an empty capture name is rejected");
  require_invalid([] { RouteTable::validate_pattern("/{name"); },
                  "an unterminated capture is rejected");
  require_invalid([] { RouteTable::validate_pattern("/a{b}"); },
                  "a capture must be a whole segment (prefix)");
  require_invalid([] { RouteTable::validate_pattern("/{a}b"); },
                  "a capture must be a whole segment (suffix)");
  require_invalid([] { RouteTable::validate_pattern("/a}b"); },
                  "a stray closing brace is rejected");
  require_invalid([] { RouteTable::validate_pattern("/*"); },
                  "a rest segment needs a name");
  require_invalid([] { RouteTable::validate_pattern("/*rest/more"); },
                  "a rest segment must be last");
  require_invalid([] { RouteTable::validate_pattern("/{id}/x/{id}"); },
                  "a capture name cannot repeat within a pattern");
  require_invalid([] { RouteTable::validate_pattern("/{id}/*id"); },
                  "a rest name cannot repeat a capture name");
}

void test_build_rejects_duplicates() {
  require_invalid(
      [] {
        static_cast<void>(table_of({
            {HttpMethod::Get, "/a/b"},
            {HttpMethod::Get, "/a/b"},
        }));
      },
      "a repeated (method, pattern) pair is rejected");
  // Two patterns that reach one terminal are one route however they spell
  // their captures; letting the second win silently would drop a route.
  require_invalid(
      [] {
        static_cast<void>(table_of({
            {HttpMethod::Get, "/a/{x}"},
            {HttpMethod::Get, "/a/{y}"},
        }));
      },
      "captures with different names cannot occupy the same route");
  require_invalid(
      [] {
        static_cast<void>(table_of({
            {HttpMethod::Get, "/*a"},
            {HttpMethod::Get, "/*b"},
        }));
      },
      "two rest segments cannot occupy the same route");
  require_invalid(
      [] {
        static_cast<void>(table_of({{HttpMethod::Get, "no-slash"}}));
      },
      "build validates every pattern");

  const auto table = table_of({
      {HttpMethod::Get, "/a/b"},
      {HttpMethod::Post, "/a/b"},
  });
  require(table.entries().size() == 2,
          "one pattern under two methods is not a duplicate");
}
} // namespace

int main() {
  try {
    test_literal_routes();
    test_no_match_cases();
    test_capture_decoding();
    test_match_decoded_does_not_decode_again();
    test_multiple_captures();
    test_rest_capture();
    test_precedence();
    test_backtracking();
    test_method_separation();
    test_trailing_slash_is_significant();
    test_root_routes();
    test_pattern_validation();
    test_build_rejects_duplicates();
  } catch (const std::exception &error) {
    std::cerr << "hgraph_web_route_table_tests failed: " << error.what() << "\n";
    return 1;
  }
  std::cout << "hgraph_web_route_table_tests passed\n";
  return 0;
}
