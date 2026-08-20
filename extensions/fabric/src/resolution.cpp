#include <hgraph/fabric/resolution.h>

#include <hgraph/fabric/keys.h>
#include <hgraph/fabric/metadata_codec.h>
#include <hgraph/fabric/value_builders.h>

#include <arrow/table.h>

#include <algorithm>
#include <limits>
#include <map>
#include <set>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>

namespace hgraph::fabric {
namespace {
using persistence::store::ImmutableWriteStatus;
using persistence::store::ObjectBytes;
using persistence::store::StoredObject;

struct IdLess {
  using is_transparent = void;

  [[nodiscard]] bool operator()(std::string_view lhs,
                                std::string_view rhs) const noexcept {
    return canonical_data_id_less(lhs, rhs);
  }
};

class CorruptHistory final : public std::runtime_error {
public:
  using std::runtime_error::runtime_error;
};

[[nodiscard]] Int checked_increment(Int value, std::string_view field) {
  if (value == std::numeric_limits<Int>::max()) {
    throw CorruptHistory("fabric " + std::string{field} + " is exhausted");
  }
  return value + 1;
}

void canonicalise_ids(std::vector<Str> &ids, std::string_view subject) {
  if (ids.empty()) {
    throw std::invalid_argument("fabric " + std::string{subject} +
                                " must not be empty");
  }
  for (const auto &id : ids) {
    require_data_id(id);
  }
  std::ranges::sort(ids, canonical_data_id_less);
  if (std::ranges::adjacent_find(ids) != ids.end()) {
    throw std::invalid_argument("fabric " + std::string{subject} +
                                " data ids must be unique");
  }
}

[[nodiscard]] bool same_tuple(const DataRevisionInput &lhs,
                              const DataRevisionInput &rhs) {
  return lhs.output_version == rhs.output_version &&
         lhs.dependencies == rhs.dependencies &&
         lhs.self_predecessor == rhs.self_predecessor;
}

[[nodiscard]] ResolvedRevision resolved(const DataRevisionInput &value) {
  return ResolvedRevision{
      .data_id = value.data_id,
      .revision = value.revision,
      .output_version = value.output_version,
      .dependencies = value.dependencies,
      .as_of = value.as_of,
  };
}

[[nodiscard]] bool successful(ResolutionStatus status) noexcept {
  return status == ResolutionStatus::Ready ||
         status == ResolutionStatus::Unchanged;
}

[[nodiscard]] bool intersects(const std::vector<Str> &lhs,
                              const std::vector<Str> &rhs) {
  auto left = lhs.begin();
  auto right = rhs.begin();
  while (left != lhs.end() && right != rhs.end()) {
    if (*left == *right) {
      return true;
    }
    if (canonical_data_id_less(*left, *right)) {
      ++left;
    } else {
      ++right;
    }
  }
  return false;
}
} // namespace

struct ConsistencyResolver::Impl {
  struct CachedData {
    std::vector<DataRevisionInput> revisions{};
    std::map<DataVersion, std::vector<std::size_t>> output_index{};
    std::map<DataVersion, Frame> frames{};
    std::shared_ptr<arrow::Schema> fixed_schema{};
  };

  struct Requirement {
    bool root{};
    std::optional<DataVersion> version{};
  };

  struct SearchState {
    std::map<Str, Requirement, IdLess> requirements{};
    std::map<Str, DataRevisionInput, IdLess> selected{};
  };

  FabricConfig config;
  std::map<Str, CachedData, IdLess> cache{};
  std::set<Str, IdLess> refreshed_this_call{};
  ResolverMetrics metrics{};
  std::map<Str, DataVersion, IdLess> lower_bounds{};
  std::vector<ResolvedCut> maxima{};
  bool saw_cycle{};

  explicit Impl(FabricConfig configured) : config(std::move(configured)) {
    require_valid_config(config);
  }

  [[nodiscard]] DataRevisionInput
  decode_slot(std::string_view data_id, RevisionId revision,
              const StoredObject &object) const {
    try {
      DataRevisionInput decoded =
          data_revision_input(decode_revision(object.data).view());
      if (decoded.data_id != data_id || decoded.revision != revision) {
        throw CorruptHistory(
            "fabric revision payload does not match its durable key");
      }
      return decoded;
    } catch (const CorruptHistory &) {
      throw;
    } catch (const std::exception &error) {
      throw CorruptHistory("fabric revision slot is malformed for '" +
                           std::string{data_id} + "': " + error.what());
    }
  }

  [[nodiscard]] std::optional<RevisionId>
  indexed_latest(std::string_view data_id) const {
    const auto object = config.objects.get(latest_key(config.prefix, data_id));
    if (!object.has_value()) {
      return std::nullopt;
    }
    try {
      return decode_revision_reference(MetadataObjectKind::Latest,
                                       object->data);
    } catch (const std::exception &error) {
      throw CorruptHistory("fabric latest index is malformed for '" +
                           std::string{data_id} + "': " + error.what());
    }
  }

  void repair_as_of(const DataRevisionInput &revision) const {
    const ObjectBytes desired =
        encode_revision_reference(MetadataObjectKind::AsOf, revision.revision);
    const auto result = config.objects.put_immutable(
        as_of_key(config.prefix, revision.data_id, revision.as_of), desired);
    if (result.status == ImmutableWriteStatus::Conflict) {
      throw CorruptHistory(
          "fabric as-of entry conflicts with accepted revision '" +
          revision.data_id + "':" + std::to_string(revision.revision));
    }
  }

  void advance_latest(std::string_view data_id, RevisionId target) const {
    const std::string key = latest_key(config.prefix, data_id);
    const ObjectBytes desired =
        encode_revision_reference(MetadataObjectKind::Latest, target);
    for (;;) {
      const auto current = config.objects.get(key);
      if (current.has_value()) {
        RevisionId current_revision{};
        try {
          current_revision = decode_revision_reference(
              MetadataObjectKind::Latest, current->data);
        } catch (const std::exception &error) {
          throw CorruptHistory("fabric latest index is malformed for '" +
                               std::string{data_id} + "': " + error.what());
        }
        if (current_revision >= target) {
          return;
        }
      }
      const auto result = config.objects.compare_exchange_ref(
          key,
          current.has_value()
              ? std::optional<std::string_view>{current->version_token}
              : std::nullopt,
          desired);
      if (result.exchanged) {
        return;
      }
      if (!result.current.has_value()) {
        continue;
      }
      try {
        if (decode_revision_reference(MetadataObjectKind::Latest,
                                      result.current->data) >= target) {
          return;
        }
      } catch (const std::exception &error) {
        throw CorruptHistory("fabric latest index is malformed for '" +
                             std::string{data_id} + "': " + error.what());
      }
    }
  }

  void validate_frame(CachedData &data, const DataRevisionInput &revision) {
    if (data.frames.contains(revision.output_version)) {
      ++metrics.frame_cache_hits;
      return;
    }

    ++metrics.frame_cache_misses;
    Frame frame = config.frames.read(data_version_key(
        config.prefix, revision.data_id, revision.output_version));
    if (!frame.has_value()) {
      throw CorruptHistory(
          "fabric accepted revision references a missing durable Frame: " +
          revision.data_id + ":" + std::to_string(revision.output_version));
    }
    if (data.fixed_schema == nullptr) {
      data.fixed_schema = frame.table->schema();
    } else if (!frame.table->schema()->Equals(*data.fixed_schema, true)) {
      throw CorruptHistory(
          "fabric accepted Frame violates the fixed schema for data id '" +
          revision.data_id + "'");
    }
    data.frames.emplace(revision.output_version, std::move(frame));
  }

  void append(CachedData &data, DataRevisionInput revision) {
    if (data.revisions.empty()) {
      if (revision.revision != 1) {
        throw CorruptHistory("fabric revision history does not start at one");
      }
    } else {
      const auto &previous = data.revisions.back();
      if (revision.revision !=
          checked_increment(previous.revision, "revision id")) {
        throw CorruptHistory("fabric revision history is not contiguous");
      }
      if (revision.as_of <= previous.as_of) {
        throw CorruptHistory("fabric revision as-of history is not monotonic");
      }
      if (revision.output_version < previous.output_version) {
        throw CorruptHistory("fabric output version history regressed");
      }
      if (same_tuple(revision, previous)) {
        throw CorruptHistory(
            "fabric revision history contains a duplicate accepted tuple");
      }
    }

    validate_frame(data, revision);
    repair_as_of(revision);
    data.output_index[revision.output_version].push_back(data.revisions.size());
    data.revisions.push_back(std::move(revision));
    ++metrics.revision_cache_misses;
  }

  void require_no_gap(std::string_view data_id, RevisionId expected) const {
    const std::string prefix = revision_key_prefix(config.prefix, data_id);
    const std::optional<std::string> start_after =
        expected == 1 ? std::nullopt
                      : std::optional<std::string>{
                            revision_key(config.prefix, data_id, expected - 1)};
    const auto page = config.objects.list(
        prefix,
        start_after.has_value() ? std::optional<std::string_view>{*start_after}
                                : std::nullopt,
        1);
    if (page.objects.empty()) {
      return;
    }
    if (page.objects.front().key !=
        revision_key(config.prefix, data_id, expected)) {
      throw CorruptHistory("fabric revision history contains a non-contiguous "
                           "or malformed slot");
    }
    throw CorruptHistory(
        "fabric revision listing disagrees with immutable slot lookup");
  }

  CachedData &refresh(std::string_view data_id) {
    require_data_id(data_id);
    if (!refreshed_this_call.emplace(data_id).second) {
      ++metrics.revision_cache_hits;
      return cache.find(data_id)->second;
    }
    auto [entry, inserted] = cache.try_emplace(Str{data_id});
    auto &data = entry->second;
    const std::size_t before = data.revisions.size();
    const auto latest = indexed_latest(data_id);
    if (data.revisions.size() >=
        static_cast<std::size_t>(std::numeric_limits<RevisionId>::max())) {
      throw CorruptHistory("fabric revision id is exhausted");
    }
    RevisionId next = static_cast<RevisionId>(data.revisions.size() + 1);

    if (latest.has_value() && *latest < 1) {
      throw CorruptHistory("fabric latest index must be positive");
    }
    while (latest.has_value() && next <= *latest) {
      const auto object =
          config.objects.get(revision_key(config.prefix, data_id, next));
      if (!object.has_value()) {
        throw CorruptHistory("fabric accepted revision slot is missing: " +
                             std::string{data_id} + ":" + std::to_string(next));
      }
      append(data, decode_slot(data_id, next, *object));
      next = checked_increment(next, "revision id");
    }

    for (;;) {
      const auto object =
          config.objects.get(revision_key(config.prefix, data_id, next));
      if (!object.has_value()) {
        break;
      }
      append(data, decode_slot(data_id, next, *object));
      next = checked_increment(next, "revision id");
    }
    require_no_gap(data_id, next);

    if (data.revisions.empty()) {
      if (latest.has_value()) {
        throw CorruptHistory(
            "fabric latest refers to an empty revision history");
      }
    } else {
      advance_latest(data_id, data.revisions.back().revision);
    }

    if (!inserted && data.revisions.size() == before && before != 0) {
      ++metrics.revision_cache_hits;
    }
    ++metrics.accepted_heads_observed;
    return data;
  }

  [[nodiscard]] std::vector<const DataRevisionInput *>
  candidates(const Str &data_id, const Requirement &requirement) {
    auto &data = refresh(data_id);
    std::vector<const DataRevisionInput *> result;
    if (requirement.version.has_value()) {
      const auto found = data.output_index.find(*requirement.version);
      if (found == data.output_index.end()) {
        ++metrics.output_index_misses;
        throw CorruptHistory(
            "fabric accepted revision references missing data version '" +
            data_id + "':" + std::to_string(*requirement.version));
      }
      ++metrics.output_index_hits;
      result.reserve(found->second.size());
      for (auto index = found->second.rbegin(); index != found->second.rend();
           ++index) {
        result.push_back(&data.revisions[*index]);
      }
      return result;
    }

    result.reserve(data.revisions.size());
    const auto lower = lower_bounds.find(data_id);
    for (auto revision = data.revisions.rbegin();
         revision != data.revisions.rend(); ++revision) {
      if (lower != lower_bounds.end() &&
          revision->output_version < lower->second) {
        continue;
      }
      result.push_back(&*revision);
    }
    return result;
  }

  void discover_requirement(
      const Str &data_id, std::optional<DataVersion> version, bool root,
      std::set<std::pair<Str, std::optional<DataVersion>>> &visited,
      std::set<Str, IdLess> &observed) {
    if (!visited.emplace(data_id, version).second) {
      return;
    }
    observed.insert(data_id);
    Requirement requirement{.root = root, .version = version};
    auto values = candidates(data_id, requirement);
    if (values.empty()) {
      return;
    }

    if (root) {
      // Forest membership follows the newest accepted acknowledgement, not
      // every historical acknowledgement which reused its output version.
      // Older same-output ancestry remains available to compatible-cut search,
      // but must not keep roots coupled after their latest lineages split.
      values.resize(1);
    }
    for (const auto *candidate : values) {
      for (const auto &dependency : candidate->dependencies) {
        discover_requirement(dependency.data_id, dependency.version, false,
                             visited, observed);
      }
    }
  }

  [[nodiscard]] std::vector<Str>
  discover_frontier(const std::vector<Str> &roots) {
    std::set<std::pair<Str, std::optional<DataVersion>>> visited;
    std::set<Str, IdLess> observed;
    for (const auto &root : roots) {
      discover_requirement(root, std::nullopt, true, visited, observed);
    }
    return {observed.begin(), observed.end()};
  }

  [[nodiscard]] static bool
  add_requirement(SearchState &state, const Str &data_id, DataVersion version) {
    auto [found, inserted] = state.requirements.try_emplace(
        data_id, Requirement{.version = version});
    if (!inserted) {
      if (found->second.version.has_value() &&
          *found->second.version != version) {
        return false;
      }
      found->second.version = version;
    }
    const auto selected = state.selected.find(data_id);
    return selected == state.selected.end() ||
           selected->second.output_version == version;
  }

  [[nodiscard]] static bool cyclic(const SearchState &state) {
    enum class Mark : std::uint8_t {
      Visiting,
      Complete,
    };
    std::map<Str, Mark, IdLess> marks;
    const auto visit = [&](const auto &self, const Str &id) -> bool {
      const auto mark = marks.find(id);
      if (mark != marks.end()) {
        return mark->second == Mark::Visiting;
      }
      marks.emplace(id, Mark::Visiting);
      const auto selected = state.selected.find(id);
      if (selected != state.selected.end()) {
        for (const auto &dependency : selected->second.dependencies) {
          if (state.selected.contains(dependency.data_id) &&
              self(self, dependency.data_id)) {
            return true;
          }
        }
      }
      marks[id] = Mark::Complete;
      return false;
    };
    for (const auto &[id, revision] : state.selected) {
      static_cast<void>(revision);
      if (visit(visit, id)) {
        return true;
      }
    }
    return false;
  }

  [[nodiscard]] static ResolvedCut make_cut(const std::vector<Str> &roots,
                                            const SearchState &state) {
    ResolvedCut cut{.roots = roots};
    cut.revisions.reserve(state.selected.size());
    for (const auto &[id, revision] : state.selected) {
      static_cast<void>(id);
      cut.revisions.push_back(resolved(revision));
    }
    return cut;
  }

  [[nodiscard]] static RevisionId selected_revision(const ResolvedCut &cut,
                                                    std::string_view data_id) {
    const auto found =
        std::ranges::lower_bound(cut.revisions, data_id, canonical_data_id_less,
                                 &ResolvedRevision::data_id);
    return found != cut.revisions.end() && found->data_id == data_id
               ? found->revision
               : RevisionId{0};
  }

  [[nodiscard]] static bool dominates(const ResolvedCut &lhs,
                                      const ResolvedCut &rhs) {
    for (const auto &revision : rhs.revisions) {
      const RevisionId left = selected_revision(lhs, revision.data_id);
      // Dependency sets may legitimately change. Components absent
      // from the other closure do not order the two cuts; roots and
      // shared ancestry do. This lets a newer root replace an old
      // dependency rather than making every dependency-set change
      // ambiguous.
      if (left != 0 && left < revision.revision) {
        return false;
      }
    }
    return true;
  }

  void consider(ResolvedCut cut) {
    for (const auto &current : maxima) {
      if (dominates(current, cut)) {
        return;
      }
    }
    std::erase_if(maxima, [&cut](const ResolvedCut &current) {
      return dominates(cut, current);
    });
    maxima.push_back(std::move(cut));
  }

  void search(const std::vector<Str> &roots, const SearchState &state) {
    const auto unresolved =
        std::ranges::find_if(state.requirements, [&state](const auto &entry) {
          return !state.selected.contains(entry.first);
        });
    if (unresolved == state.requirements.end()) {
      if (cyclic(state)) {
        saw_cycle = true;
        return;
      }
      consider(make_cut(roots, state));
      return;
    }

    const Str data_id = unresolved->first;
    const Requirement requirement = unresolved->second;
    const auto available = candidates(data_id, requirement);
    for (const auto *candidate : available) {
      ++metrics.revisions_examined;
      ++metrics.candidate_selections;
      const std::uint64_t depth = state.selected.size() + 1;
      metrics.backtracking_depth_sum += depth;
      metrics.maximum_backtracking_depth =
          std::max(metrics.maximum_backtracking_depth, depth);

      SearchState next = state;
      next.selected.emplace(data_id, *candidate);
      bool compatible = true;
      for (const auto &dependency : candidate->dependencies) {
        ++metrics.edges_examined;
        if (!add_requirement(next, dependency.data_id, dependency.version)) {
          compatible = false;
          break;
        }
      }
      if (compatible) {
        search(roots, next);
      }
    }
  }

  [[nodiscard]] const DataRevisionInput &
  selection(const ResolvedCut &cut, std::string_view data_id) const {
    const auto found =
        std::ranges::lower_bound(cut.revisions, data_id, canonical_data_id_less,
                                 &ResolvedRevision::data_id);
    if (found == cut.revisions.end() || found->data_id != data_id) {
      throw std::logic_error("resolved cut omitted a direct fabric root");
    }
    const auto data = cache.find(found->data_id);
    if (data == cache.end() || found->revision < 1 ||
        static_cast<std::size_t>(found->revision) >
            data->second.revisions.size()) {
      throw std::logic_error("resolved cut references an uncached revision");
    }
    return data->second
        .revisions[static_cast<std::size_t>(found->revision - 1)];
  }

  [[nodiscard]] Frame selected_frame(const DataRevisionInput &revision) {
    auto data = cache.find(revision.data_id);
    if (data == cache.end()) {
      throw std::logic_error("resolved root has no cached data");
    }
    const auto frame = data->second.frames.find(revision.output_version);
    if (frame == data->second.frames.end()) {
      throw std::logic_error("resolved root has no cached Frame");
    }
    ++metrics.frame_cache_hits;
    return frame->second;
  }

  ForestResolution resolve(std::vector<Str> roots,
                           std::span<const ExposedRootVersion> exposed) {
    canonicalise_ids(roots, "resolver roots");
    metrics = {};
    lower_bounds.clear();
    refreshed_this_call.clear();
    maxima.clear();
    saw_cycle = false;

    for (const auto &item : exposed) {
      require_data_id(item.data_id);
      if (item.output_version <= 0) {
        throw std::invalid_argument(
            "fabric exposed root version must be positive");
      }
      if (!std::ranges::binary_search(roots, item.data_id,
                                      canonical_data_id_less)) {
        throw std::invalid_argument(
            "fabric exposed version is not a resolver root");
      }
      if (!lower_bounds.emplace(item.data_id, item.output_version).second) {
        throw std::invalid_argument(
            "fabric exposed root versions must be unique");
      }
    }

    ForestResolution result{.roots = roots};
    try {
      result.observed_data_ids = discover_frontier(roots);
      SearchState initial;
      for (const auto &root : roots) {
        initial.requirements.emplace(root, Requirement{.root = true});
      }
      search(roots, initial);
    } catch (const CorruptHistory &error) {
      result.status = ResolutionStatus::Corrupt;
      result.diagnostic = error.what();
      result.metrics = metrics;
      if (result.observed_data_ids.empty()) {
        result.observed_data_ids = roots;
      }
      return result;
    }

    result.metrics = metrics;
    if (maxima.empty()) {
      result.status =
          saw_cycle ? ResolutionStatus::Cyclic : ResolutionStatus::Pending;
      result.diagnostic = saw_cycle
                              ? "ordinary dependency closure is cyclic"
                              : "no compatible acknowledgement is available";
      return result;
    }
    if (maxima.size() != 1) {
      result.status = ResolutionStatus::Ambiguous;
      result.diagnostic =
          "consistent closures have no unique component-wise greatest cut";
      return result;
    }

    result.cut = std::move(maxima.front());
    bool changed = false;
    for (const auto &root : roots) {
      const auto &chosen = selection(*result.cut, root);
      const auto prior = lower_bounds.find(root);
      if (prior == lower_bounds.end() ||
          chosen.output_version > prior->second) {
        changed = true;
        result.changed_roots.push_back(RootUpdate{
            .data_id = root,
            .revision = chosen.revision,
            .output_version = chosen.output_version,
            .frame = selected_frame(chosen),
        });
      }
    }
    result.status =
        changed ? ResolutionStatus::Ready : ResolutionStatus::Unchanged;
    result.metrics = metrics;
    return result;
  }
};

ConsistencyResolver::ConsistencyResolver(FabricConfig config)
    : impl_(std::make_unique<Impl>(std::move(config))) {}

ConsistencyResolver::~ConsistencyResolver() = default;
ConsistencyResolver::ConsistencyResolver(ConsistencyResolver &&) noexcept =
    default;
ConsistencyResolver &
ConsistencyResolver::operator=(ConsistencyResolver &&) noexcept = default;

ForestResolution ConsistencyResolver::resolve_forest(
    std::vector<Str> roots, std::span<const ExposedRootVersion> exposed_roots) {
  return impl_->resolve(std::move(roots), exposed_roots);
}

struct ConsistencyCoordinator::Impl {
  ConsistencyResolver resolver;
  std::vector<Str> roots;
  std::map<Str, DataVersion, IdLess> exposed{};
  std::map<Str, ResolvedCut, IdLess> root_cuts{};
  std::map<Str, DateTime, IdLess> pending_notices{};

  Impl(FabricConfig config, std::vector<Str> configured_roots)
      : resolver(std::move(config)), roots(std::move(configured_roots)) {
    canonicalise_ids(roots, "coordinator roots");
  }

  [[nodiscard]] std::vector<ExposedRootVersion>
  bounds(const std::vector<Str> &group) const {
    std::vector<ExposedRootVersion> result;
    for (const auto &root : group) {
      const auto found = exposed.find(root);
      if (found != exposed.end()) {
        result.push_back({root, found->second});
      }
    }
    return result;
  }

  void observe(Str data_id, DateTime noticed_at) {
    require_data_id(data_id);
    if (noticed_at <= MIN_DT) {
      throw std::invalid_argument("fabric notice time must be a real instant");
    }
    const auto found = pending_notices.find(data_id);
    if (found == pending_notices.end()) {
      pending_notices.emplace(std::move(data_id), noticed_at);
    } else {
      found->second = std::min(found->second, noticed_at);
    }
  }

  [[nodiscard]] static std::size_t find(std::vector<std::size_t> &parent,
                                        std::size_t value) {
    while (parent[value] != value) {
      parent[value] = parent[parent[value]];
      value = parent[value];
    }
    return value;
  }

  [[nodiscard]] static bool
  merge_overlaps(std::vector<std::vector<Str>> &groups,
                 const std::vector<ForestResolution> &results) {
    std::vector<std::size_t> parent(groups.size());
    for (std::size_t index = 0; index < parent.size(); ++index) {
      parent[index] = index;
    }
    bool merged = false;
    for (std::size_t left = 0; left < results.size(); ++left) {
      for (std::size_t right = left + 1; right < results.size(); ++right) {
        if (!intersects(results[left].observed_data_ids,
                        results[right].observed_data_ids)) {
          continue;
        }
        auto left_root = find(parent, left);
        auto right_root = find(parent, right);
        if (left_root != right_root) {
          parent[right_root] = left_root;
          merged = true;
        }
      }
    }
    if (!merged) {
      return false;
    }

    std::map<std::size_t, std::vector<Str>> combined;
    for (std::size_t index = 0; index < groups.size(); ++index) {
      auto &destination = combined[find(parent, index)];
      destination.insert(destination.end(), groups[index].begin(),
                         groups[index].end());
    }
    groups.clear();
    for (auto &[root, group] : combined) {
      static_cast<void>(root);
      std::ranges::sort(group, canonical_data_id_less);
      groups.push_back(std::move(group));
    }
    std::ranges::sort(groups, [](const auto &lhs, const auto &rhs) {
      return canonical_data_id_less(lhs.front(), rhs.front());
    });
    return true;
  }

  [[nodiscard]] CoordinationResult resolve_all(DateTime ready_at) {
    std::vector<std::vector<Str>> groups;
    groups.reserve(roots.size());
    for (const auto &root : roots) {
      groups.push_back({root});
    }

    std::vector<ForestResolution> results;
    for (;;) {
      results.clear();
      results.reserve(groups.size());
      for (const auto &group : groups) {
        const auto exposed_bounds = bounds(group);
        results.push_back(resolver.resolve_forest(group, exposed_bounds));
      }
      if (!merge_overlaps(groups, results)) {
        break;
      }
    }

    auto next_exposed = exposed;
    auto next_root_cuts = root_cuts;
    auto next_notices = pending_notices;
    std::vector<RootUpdate> updates;
    for (auto &result : results) {
      if (!successful(result.status) || !result.cut.has_value()) {
        continue;
      }
      std::optional<DateTime> earliest_notice;
      for (const auto &data_id : result.observed_data_ids) {
        const auto notice = next_notices.find(data_id);
        if (notice == next_notices.end()) {
          continue;
        }
        if (!earliest_notice.has_value() || notice->second < *earliest_notice) {
          earliest_notice = notice->second;
        }
        next_notices.erase(notice);
      }
      if (earliest_notice.has_value() && ready_at > MIN_DT) {
        if (ready_at < *earliest_notice) {
          throw std::invalid_argument(
              "fabric ready time precedes its observed notice");
        }
        result.metrics.notice_to_ready = ready_at - *earliest_notice;
      }
      for (const auto &root : result.roots) {
        const auto found = std::ranges::lower_bound(result.cut->revisions, root,
                                                    canonical_data_id_less,
                                                    &ResolvedRevision::data_id);
        if (found == result.cut->revisions.end() || found->data_id != root) {
          throw std::logic_error("successful fabric cut omitted a root");
        }
        next_exposed[root] = found->output_version;
        next_root_cuts[root] = *result.cut;
      }
      updates.insert(updates.end(), result.changed_roots.begin(),
                     result.changed_roots.end());
    }

    std::map<Str, ResolvedRevision, IdLess> lineage;
    for (const auto &[root, cut] : next_root_cuts) {
      static_cast<void>(root);
      for (const auto &revision : cut.revisions) {
        const auto found = lineage.find(revision.data_id);
        if (found == lineage.end() ||
            found->second.revision < revision.revision) {
          lineage[revision.data_id] = revision;
        }
      }
    }
    std::vector<ResolvedRevision> committed;
    committed.reserve(lineage.size());
    for (auto &[id, revision] : lineage) {
      static_cast<void>(id);
      committed.push_back(std::move(revision));
    }

    std::ranges::sort(updates,
                      [](const RootUpdate &lhs, const RootUpdate &rhs) {
                        return canonical_data_id_less(lhs.data_id, rhs.data_id);
                      });
    exposed.swap(next_exposed);
    root_cuts.swap(next_root_cuts);
    pending_notices.swap(next_notices);
    return CoordinationResult{
        .forests = std::move(results),
        .changed_roots = std::move(updates),
        .committed_lineage = std::move(committed),
    };
  }
};

ConsistencyCoordinator::ConsistencyCoordinator(FabricConfig config,
                                               std::vector<Str> roots)
    : impl_(std::make_unique<Impl>(std::move(config), std::move(roots))) {}

ConsistencyCoordinator::~ConsistencyCoordinator() = default;
ConsistencyCoordinator::ConsistencyCoordinator(
    ConsistencyCoordinator &&) noexcept = default;
ConsistencyCoordinator &
ConsistencyCoordinator::operator=(ConsistencyCoordinator &&) noexcept = default;

void ConsistencyCoordinator::observe_notice(Str data_id, DateTime noticed_at) {
  impl_->observe(std::move(data_id), noticed_at);
}

CoordinationResult ConsistencyCoordinator::resolve(DateTime ready_at) {
  return impl_->resolve_all(ready_at);
}
} // namespace hgraph::fabric
