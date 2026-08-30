#ifndef HGRAPH_FABRIC_RESOLUTION_H
#define HGRAPH_FABRIC_RESOLUTION_H

#include <hgraph/fabric/config.h>
#include <hgraph/fabric/export.h>
#include <hgraph/fabric/types.h>

#include <hgraph/types/frame.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <ostream>
#include <span>
#include <string_view>
#include <vector>

namespace hgraph::fabric {
/** Outcome of resolving one independent RFC 0026 consistency forest. */
enum class ResolutionStatus : std::uint8_t {
  Ready,
  Unchanged,
  Pending,
  Ambiguous,
  Cyclic,
  Corrupt,
};

[[nodiscard]] constexpr std::string_view
enum_name(ResolutionStatus value) noexcept {
  switch (value) {
  case ResolutionStatus::Ready:
    return "Ready";
  case ResolutionStatus::Unchanged:
    return "Unchanged";
  case ResolutionStatus::Pending:
    return "Pending";
  case ResolutionStatus::Ambiguous:
    return "Ambiguous";
  case ResolutionStatus::Cyclic:
    return "Cyclic";
  case ResolutionStatus::Corrupt:
    return "Corrupt";
  }
  return "Unknown";
}

inline std::ostream &operator<<(std::ostream &stream, ResolutionStatus value) {
  return stream << enum_name(value);
}

/** Previously exposed version of one direct root. It is a lower bound:
    resolution may change hidden lineage but never return an older root
    version to a running graph. */
struct ExposedRootVersion {
  Str data_id{};
  DataVersion output_version{};

  friend bool operator==(const ExposedRootVersion &,
                         const ExposedRootVersion &) = default;
};

/** One immutable revision selected into a resolved transitive closure. */
struct ResolvedRevision {
  Str data_id{};
  RevisionId revision{};
  DataVersion output_version{};
  std::vector<DataDependencyInput> dependencies{};
  DateTime as_of{MIN_DT};

  friend bool operator==(const ResolvedRevision &,
                         const ResolvedRevision &) = default;
};

struct ResolvedCut {
  std::vector<Str> roots{};
  std::vector<ResolvedRevision> revisions{};

  friend bool operator==(const ResolvedCut &, const ResolvedCut &) = default;
};

/** A direct root Frame which must be delivered in the next single graph
    evaluation cycle. No entry is emitted for a root whose selected
    revision changed while its output version remained unchanged. */
struct RootUpdate {
  Str data_id{};
  RevisionId revision{};
  DataVersion output_version{};
  Frame frame{};
};

/** Per-call resolver work counters. Cache counters describe immutable
    revision/Frame reuse; depth_sum / candidate_selections is the average
    explored backtracking depth. */
struct ResolverMetrics {
  std::uint64_t accepted_heads_observed{};
  std::uint64_t revision_cache_hits{};
  std::uint64_t revision_cache_misses{};
  std::uint64_t frame_cache_hits{};
  std::uint64_t frame_cache_misses{};
  std::uint64_t output_index_hits{};
  std::uint64_t output_index_misses{};
  std::uint64_t revisions_examined{};
  std::uint64_t edges_examined{};
  std::uint64_t candidate_selections{};
  std::uint64_t backtracking_depth_sum{};
  std::uint64_t maximum_backtracking_depth{};
  /** Present on a coordinator result when both a notice timestamp and a
      successful-cut timestamp were supplied. */
  std::optional<TimeDelta> notice_to_ready{};

  [[nodiscard]] double average_backtracking_depth() const noexcept {
    return candidate_selections == 0
               ? 0.0
               : static_cast<double>(backtracking_depth_sum) /
                     static_cast<double>(candidate_selections);
  }
};

struct ForestResolution {
  ResolutionStatus status{ResolutionStatus::Pending};
  std::vector<Str> roots{};
  /** Union of newest-compatible ancestry considered when deciding
      dynamic forest membership. */
  std::vector<Str> observed_data_ids{};
  std::optional<ResolvedCut> cut{};
  std::vector<RootUpdate> changed_roots{};
  ResolverMetrics metrics{};
  Str diagnostic{};
};

/** Stateful cold-path consistency resolver.

    Immutable revisions and Frames are cached for the resolver lifetime;
    each new accepted slot is validated exactly once and indexed by output
    version. Search is newest-first depth-first backtracking. Its worst-case
    time is exponential in an ambiguous forest (the contract requires all
    maximal closures to be distinguished), O(R + E) for the usual unique
    closure, and O(new revisions + retained indexes) storage. The class is
    intended to be owned by one ingress coordinator and is not thread-safe. */
class HGRAPH_FABRIC_CLASS_EXPORT ConsistencyResolver final {
public:
  explicit ConsistencyResolver(FabricConfig config);
  ~ConsistencyResolver();

  ConsistencyResolver(const ConsistencyResolver &) = delete;
  ConsistencyResolver &operator=(const ConsistencyResolver &) = delete;
  ConsistencyResolver(ConsistencyResolver &&) noexcept;
  ConsistencyResolver &operator=(ConsistencyResolver &&) noexcept;

  [[nodiscard]] ForestResolution
  resolve_forest(std::vector<Str> roots,
                 std::span<const ExposedRootVersion> exposed_roots = {});

  /** Seed the immutable revision/output-version/dependency indexes from one
      complete accepted notice. Missing earlier slots are loaded directly by
      revision id; no latest/as-of discovery read is performed. */
  void observe_accepted_revision(DataRevisionInput revision);

  /** Resolve from the already observed cache. A previously unseen ancestry
      id is recovered durably once, but cached heads are not polled again. */
  [[nodiscard]] ForestResolution
  resolve_forest_cached(std::vector<Str> roots,
                        std::span<const ExposedRootVersion> exposed_roots = {});

  /** Resolve only from revisions knowable at ``maximum_as_of``. The bound
      applies recursively to roots and ordinary ancestry. */
  [[nodiscard]] ForestResolution
  resolve_forest_at(std::vector<Str> roots, DateTime maximum_as_of,
                    std::span<const ExposedRootVersion> exposed_roots = {});

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

/** One atomic ingress decision. Successful forests may advance while an
    unrelated forest remains pending or fails. changed_roots is the single
    visible delivery batch; committed_lineage is the hidden selection after
    applying every successful forest in the same decision. */
struct CoordinationResult {
  std::vector<ForestResolution> forests{};
  std::vector<RootUpdate> changed_roots{};
  std::vector<ResolvedRevision> committed_lineage{};
};

/** Repartitions roots on each resolve, merging newest ancestry which
    intersects and allowing dependency-set changes to split it again.
    Successful cuts and their hidden lineage commit atomically with the
    returned root batch; failed forests retain their previously exposed
    cut and do not block independent forests. */
class HGRAPH_FABRIC_CLASS_EXPORT ConsistencyCoordinator final {
public:
  ConsistencyCoordinator(FabricConfig config, std::vector<Str> roots);
  ~ConsistencyCoordinator();

  ConsistencyCoordinator(const ConsistencyCoordinator &) = delete;
  ConsistencyCoordinator &operator=(const ConsistencyCoordinator &) = delete;
  ConsistencyCoordinator(ConsistencyCoordinator &&) noexcept;
  ConsistencyCoordinator &operator=(ConsistencyCoordinator &&) noexcept;

  /** Conflate a notice by data id while retaining its earliest latency origin.
   */
  void observe_notice(Str data_id, DateTime noticed_at);

  /** Populate the live resolver indexes from a complete accepted revision
      carried on the graph notification edge. */
  void observe_accepted_revision(DataRevisionInput revision);

  /** Resolve current accepted heads. MIN_DT omits notice latency for startup
      image calls which did not originate from a notice. */
  [[nodiscard]] CoordinationResult resolve(DateTime ready_at = MIN_DT);

  /** Resolve a notice-driven update without polling durable heads already in
      the cache. New ancestry and explicit revision gaps are still recovered. */
  [[nodiscard]] CoordinationResult resolve_cached(DateTime ready_at = MIN_DT);

  /** Resolve and atomically commit a historical cut using no revision later
      than ``maximum_as_of``. */
  [[nodiscard]] CoordinationResult resolve_at(DateTime maximum_as_of,
                                              DateTime ready_at = MIN_DT);

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};
} // namespace hgraph::fabric

#endif // HGRAPH_FABRIC_RESOLUTION_H
