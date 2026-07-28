#include <hgraph/runtime/mesh_node.h>

#include <hgraph/runtime/nested_bindings.h>
#include <hgraph/runtime/nested_graph_storage.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/utils/key_slot_store.h>
#include <hgraph/types/utils/slot_bitmap.h>
#include <hgraph/util/date_time.h>
#include <hgraph/util/scope.h>

#include "mapped_child_bindings.h"
#include "mapped_key_source.h"

#include <ankerl/unordered_dense.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace hgraph {
namespace {
constexpr std::string_view mesh_storage_field_name{"mesh"};
constexpr std::string_view mesh_subscribe_storage_field_name{"mesh_subscribe"};
constexpr std::string_view mesh_key_set_storage_field_name{"mesh_key_set"};

struct ValueKeyHash {
  using is_transparent = void;
  [[nodiscard]] std::size_t operator()(const Value &v) const noexcept {
    return v.hash();
  }
  [[nodiscard]] std::size_t operator()(const ValueView &v) const noexcept {
    return v.hash();
  }
};

struct ValueKeyEqual {
  using is_transparent = void;
  [[nodiscard]] bool operator()(const Value &a, const Value &b) const noexcept {
    return a.equals(b);
  }
  [[nodiscard]] bool operator()(const Value &a,
                                const ValueView &b) const noexcept {
    return a.equals(b);
  }
  [[nodiscard]] bool operator()(const ValueView &a,
                                const Value &b) const noexcept {
    return b.equals(a);
  }
};

using ValueSet =
    ankerl::unordered_dense::set<Value, ValueKeyHash, ValueKeyEqual>;

struct MeshNodeStorage;

/** Stable per-instance context for the out-of-band child schedule observer. */
struct MeshChildScheduleContext {
  MeshNodeStorage *storage{nullptr};
  std::size_t slot{0};
  NodePtr parent_node{};
  // Coalesce repeated pull observations of the same retained child deadline.
  // Push observations remain distinct because one child graph may schedule
  // multiple internal nodes.
  DateTime pulled_when{MAX_DT};
};

struct MeshChildSchedule {
  DateTime when{MAX_DT};
  std::size_t slot{0};
  bool pulled{false};

  [[nodiscard]] bool operator>(const MeshChildSchedule &other) const noexcept {
    if (when != other.when) {
      return when > other.when;
    }
    if (slot != other.slot) {
      return slot > other.slot;
    }
    return pulled > other.pulled;
  }
};

// One mesh instance. Declaration order is load-bearing (reverse
// destruction): the child graph (a subscriber to key_source) tears down
// before the key source it observes and the schedule-observer context it
// references.
struct MeshEntry {
  explicit MeshEntry(Value key_) : key(std::move(key_)) {}

  Value key{};
  runtime_detail::MappedKeySource key_source{};
  MeshChildScheduleContext schedule_context{};
  GraphValue graph{};
  int rank{0};
  // Pause/resume settle state, per cycle:
  bool paused{false};            // paused this cycle, awaiting a dependency
  DateTime settled_time{MIN_DT}; // completed (no pause) at this evaluation time
};

struct MeshNodeStorage final : SlotObserver {
  struct RequestedKeysObserver final : SlotObserver {
    explicit RequestedKeysObserver(MeshNodeStorage &owner_) noexcept
        : owner(owner_) {}

    void on_capacity(std::size_t, std::size_t new_capacity) override {
      if (owner.instance_keys.has_value()) {
        owner.instance_keys->reserve_to(new_capacity);
      }
    }

    void on_insert(std::size_t) override {}
    void on_remove(std::size_t) override {}
    void on_erase(std::size_t) override {}
    void on_clear() override { owner.requested_keys_source_cleared = true; }

    MeshNodeStorage &owner;
  };

  MeshNodeStorage() : requested_keys_observer(*this) {}
  MeshNodeStorage(const MeshNodeStorage &) = delete;
  MeshNodeStorage &operator=(const MeshNodeStorage &) = delete;
  MeshNodeStorage(MeshNodeStorage &&) = delete;
  MeshNodeStorage &operator=(MeshNodeStorage &&) = delete;

  ~MeshNodeStorage() override {
    unsubscribe_requested_keys_noexcept();
    if (instance_keys.has_value()) {
      instance_keys->remove_slot_observer(this);
    }
    stop_all_noexcept();
  }

  // Authoritative instance keys are a superset of __keys__: on-demand
  // dependency keys use the same delayed-remove slot protocol.
  std::optional<KeySlotStore> instance_keys{};
  InPlaceGraphSlotStore<MeshEntry> entries{};
  RequestedKeysObserver requested_keys_observer;
  TSOutputHandle observed_requested_keys_source{};
  bool observing_requested_keys{false};
  bool requested_keys_source_cleared{false};
  // Cached effective outer sources detect forwarding/repoint changes that
  // require every surviving child boundary to be rebound.
  std::vector<TSOutputHandle> outer_sources{};
  bool refresh_all_bindings{false};
  // depends_on -> set of keys that depend on it (reverse edges).
  ankerl::unordered_dense::map<Value, ValueSet, ValueKeyHash, ValueKeyEqual>
      dependents{};

  std::vector<Value>
      graphs_to_remove{}; // lost a dependent; remove if unreferenced
  // Ordinary input notifications and internal child schedules identify a
  // sparse worklist. Only those slots need dependency-rank ordering.
  SlotBitmap evaluation_candidates{};
  std::vector<std::pair<int, std::size_t>> evaluation_order{};
  // Min-heap of child wake-ups, fed by the nested out-of-band observer and by
  // the pull after mesh-driven child evaluation.
  std::vector<MeshChildSchedule> child_schedule_queue{};
  int max_rank{0};
  bool primed{false};
  // The instance whose child graph is currently being evaluated; a
  // mesh_subscribe inside it reads this as its "my_key" (the requester).
  ValuePtr current_eval_key{};
  DateTime retirement_time{MIN_DT};

  void push_child_schedule(MeshChildSchedule schedule) {
    child_schedule_queue.push_back(schedule);
    std::push_heap(child_schedule_queue.begin(), child_schedule_queue.end(),
                   std::greater<>{});
  }

  void push_observed_child_schedule(DateTime when,
                                    const MeshChildScheduleContext &schedule) {
    if (schedule.storage != this) {
      return;
    }
    const NodeView parent{schedule.parent_node};
    if (parent.valid() && when <= parent.graph().evaluation_time()) {
      // Current-cycle notifications already identify their slot. Recording
      // them directly avoids two heap operations per child on broadcast
      // ticks while future deadlines still use the priority queue.
      evaluation_candidates.set(schedule.slot);
      return;
    }
    push_child_schedule(MeshChildSchedule{when, schedule.slot, false});
  }

  void push_pulled_child_schedule(DateTime when,
                                  MeshChildScheduleContext &schedule) {
    if (schedule.storage != this || schedule.pulled_when == when) {
      return;
    }
    schedule.pulled_when = when;
    push_child_schedule(MeshChildSchedule{when, schedule.slot, true});
  }

  void initialise(const ValueTypeRef &key_binding,
                  MemoryUtils::StorageLayout graph_layout) {
    entries.bind_graph_layout(graph_layout);
    if (instance_keys.has_value()) {
      return;
    }
    instance_keys.emplace(key_binding);
    instance_keys->add_slot_observer(this);
  }

  [[nodiscard]] bool observe_requested_keys_source(TSOutputHandle source) {
    if (source.same_as(observed_requested_keys_source) &&
        !requested_keys_source_cleared) {
      return false;
    }

    unsubscribe_requested_keys_noexcept();
    observed_requested_keys_source = source;
    requested_keys_source_cleared = false;
    if (source.bound()) {
      auto data = source.data_view();
      auto set = data.as_set();
      instance_keys->reserve_to(set.slot_capacity());
      set.subscribe_slot_observer(&requested_keys_observer);
      observing_requested_keys = true;
    }
    return true;
  }

  void unsubscribe_requested_keys_noexcept() noexcept {
    if (observing_requested_keys) {
      static_cast<void>(fallback_on_exception(false, [&] {
        auto data = observed_requested_keys_source.data_view();
        if (data.valid()) {
          data.as_set().unsubscribe_slot_observer(&requested_keys_observer);
        }
        return true;
      }));
    }
    observed_requested_keys_source.reset();
    observing_requested_keys = false;
  }

  void stop_all_noexcept() noexcept {
    for (std::size_t slot = 0; slot < entries.slot_capacity(); ++slot) {
      MeshEntry *entry = entries.entry_at(slot);
      if (entry != nullptr && entry->graph.has_value()) {
        static_cast<void>(fallback_on_exception(false, [&] {
          if (entry->graph.view().started()) {
            entry->graph.view().stop();
          }
          return true;
        }));
      }
    }
    entries.destroy_all();
    dependents.clear();
    graphs_to_remove.clear();
    outer_sources.clear();
    refresh_all_bindings = false;
    evaluation_candidates.reset();
    evaluation_order.clear();
    child_schedule_queue.clear();
    max_rank = 0;
    primed = false;
    current_eval_key = {};
    retirement_time = MIN_DT;
    requested_keys_source_cleared = false;
  }

  [[nodiscard]] std::size_t active_count() const noexcept {
    return instance_keys.has_value() ? instance_keys->size() : 0;
  }

  [[nodiscard]] std::size_t find_slot(const ValueView &key) const {
    if (!instance_keys.has_value() || !key.has_value()) {
      return KeySlotStore::npos;
    }
    return instance_keys->find_slot(key);
  }

  [[nodiscard]] MeshEntry *find(const ValueView &key) noexcept {
    const std::size_t slot = find_slot(key);
    return slot == KeySlotStore::npos ? nullptr : entries.entry_at(slot);
  }

  void retire_slot(std::size_t slot, DateTime evaluation_time) {
    if (!instance_keys.has_value() || !instance_keys->slot_live(slot)) {
      return;
    }
    if (MeshEntry *entry = entries.entry_at(slot); entry != nullptr) {
      entry->schedule_context.pulled_when = MAX_DT;
    }
    evaluation_candidates.reset(slot);
    retirement_time = evaluation_time;
    static_cast<void>(instance_keys->remove_slot(slot));
  }

  void erase_retired_before(DateTime evaluation_time) noexcept {
    if (!instance_keys.has_value() || !instance_keys->has_pending_erase() ||
        retirement_time == MIN_DT || retirement_time >= evaluation_time) {
      return;
    }
    instance_keys->erase_pending();
    retirement_time = MIN_DT;
  }

  void on_capacity(std::size_t, std::size_t new_capacity) override {
    entries.reserve_to(new_capacity);
    evaluation_candidates.resize(new_capacity);
  }

  void on_insert(std::size_t) override {}

  void on_remove(std::size_t slot) override {
    MeshEntry *entry = entries.entry_at(slot);
    evaluation_candidates.reset(slot);
    if (entry != nullptr) {
      entry->schedule_context.pulled_when = MAX_DT;
    }
    if (entry != nullptr && entry->graph.has_value() &&
        entry->graph.view().started()) {
      static_cast<void>(fallback_on_exception(false, [&] {
        entry->graph.view().stop();
        return true;
      }));
    }
  }

  void on_erase(std::size_t slot) override { entries.destroy_at(slot); }
  void on_clear() override {
    evaluation_candidates.reset();
    entries.destroy_all();
  }
};

struct MeshNodeContext {
  MeshNodeSpec spec{};
  std::size_t storage_offset{0};
  ValueTypeRef key_binding{nullptr};
  MemoryUtils::StorageLayout graph_layout{};
};

void initialise_mesh_storage(MeshNodeStorage &storage,
                             const MeshNodeContext &context) {
  if (context.key_binding == nullptr) {
    throw std::logic_error("mesh_ has no resolved key binding");
  }
  storage.initialise(context.key_binding, context.graph_layout);
}

struct MeshSubscribeStorage {
  Value requester{};
  Value dependency{};
  bool has_dependency{false};
  bool owns_output_binding{false};
};

struct MeshSubscribeContext {
  std::size_t storage_offset{0};
};

struct MeshKeySetStorage {
  bool owns_output_binding{false};
};

struct MeshKeySetContext {
  std::size_t storage_offset{0};
};

// Program-lifetime, intentionally-leaked context storage.
[[nodiscard]] std::vector<std::unique_ptr<MeshNodeContext>> &
mesh_node_contexts() noexcept {
  static auto *contexts = new std::vector<std::unique_ptr<MeshNodeContext>>;
  return *contexts;
}

[[nodiscard]] const MeshNodeContext &
register_mesh_node_context(MeshNodeSpec spec, std::size_t storage_offset,
                           const ValueTypeRef &key_binding,
                           MemoryUtils::StorageLayout graph_layout) {
  auto context = std::make_unique<MeshNodeContext>(MeshNodeContext{
      .spec = std::move(spec),
      .storage_offset = storage_offset,
      .key_binding = key_binding,
      .graph_layout = graph_layout,
  });
  const auto *result = context.get();
  mesh_node_contexts().push_back(std::move(context));
  return *result;
}

[[nodiscard]] NodeStorageMetrics mesh_storage_metrics(
    const void *raw_context, const void *memory) noexcept {
  const auto &context = *static_cast<const MeshNodeContext *>(raw_context);
  const auto &storage = *MemoryUtils::cast<const MeshNodeStorage>(
      MemoryUtils::advance(memory, context.storage_offset));
  return NodeStorageMetrics{
      .nested_graph_count = storage.entries.entry_count(),
      .nested_graph_capacity = storage.entries.slot_capacity(),
      .nested_graph_blocks = storage.entries.block_count(),
      .dynamic_live_bytes = storage.entries.live_bytes(),
      .dynamic_reserved_bytes = storage.entries.reserved_bytes(),
  };
}

[[nodiscard]] std::vector<std::unique_ptr<MeshSubscribeContext>> &
mesh_subscribe_contexts() noexcept {
  static auto *contexts =
      new std::vector<std::unique_ptr<MeshSubscribeContext>>;
  return *contexts;
}

[[nodiscard]] const MeshSubscribeContext &
register_mesh_subscribe_context(std::size_t storage_offset) {
  auto context = std::make_unique<MeshSubscribeContext>(MeshSubscribeContext{
      .storage_offset = storage_offset,
  });
  const auto *result = context.get();
  mesh_subscribe_contexts().push_back(std::move(context));
  return *result;
}

[[nodiscard]] std::vector<std::unique_ptr<MeshKeySetContext>> &
mesh_key_set_contexts() noexcept {
  static auto *contexts = new std::vector<std::unique_ptr<MeshKeySetContext>>;
  return *contexts;
}

[[nodiscard]] const MeshKeySetContext &
register_mesh_key_set_context(std::size_t storage_offset) {
  auto context = std::make_unique<MeshKeySetContext>(MeshKeySetContext{
      .storage_offset = storage_offset,
  });
  const auto *result = context.get();
  mesh_key_set_contexts().push_back(std::move(context));
  return *result;
}

MeshNodeStorage &storage_of(const NodeView &view,
                            const MeshNodeContext &context) {
  return *MemoryUtils::cast<MeshNodeStorage>(
      MemoryUtils::advance(view.data(), context.storage_offset));
}

[[nodiscard]] TSOutputHandle effective_output_handle(TSOutputView source) {
  if (!source.bound()) {
    return {};
  }

  TSOutputHandle current = source.handle();
  while (source.forwarding()) {
    TSOutputHandle target = source.forwarding_target();
    if (!target.bound() || target.same_as(current)) {
      break;
    }
    current = target;
    source = target.view(source.evaluation_time());
  }
  return current;
}

[[nodiscard]] bool update_mesh_source_handles(const TSInputView &root_input,
                                              MeshNodeStorage &storage,
                                              std::size_t keys_input_index) {
  const std::size_t outer_count = root_input.as_bundle().size();
  const bool initialized = storage.outer_sources.size() == outer_count;
  storage.outer_sources.resize(outer_count);

  bool changed = false;
  for (std::size_t index = 0; index < outer_count; ++index) {
    TSOutputHandle current = effective_output_handle(
        root_input.indexed_child_at(index).bound_output());
    if (!current.same_as(storage.outer_sources[index])) {
      storage.outer_sources[index] = current;
      if (initialized && index != keys_input_index) {
        changed = true;
      }
    }
  }
  return changed;
}

void add_mesh_evaluation_slot(MeshNodeStorage &storage, std::size_t slot) {
  if (!storage.instance_keys.has_value() || slot == KeySlotStore::npos ||
      !storage.instance_keys->slot_live(slot) ||
      storage.entries.entry_at(slot) == nullptr) {
    return;
  }
  storage.evaluation_candidates.set(slot);
}

void collect_all_mesh_evaluation_slots(MeshNodeStorage &storage) {
  if (!storage.instance_keys.has_value()) {
    return;
  }
  for (std::size_t slot = 0; slot < storage.instance_keys->slot_capacity();
       ++slot) {
    add_mesh_evaluation_slot(storage, slot);
  }
}

[[nodiscard]] bool drain_due_mesh_schedules(MeshNodeStorage &storage,
                                            DateTime evaluation_time) {
  bool added = false;
  while (!storage.child_schedule_queue.empty() &&
         storage.child_schedule_queue.front().when <= evaluation_time) {
    std::pop_heap(storage.child_schedule_queue.begin(),
                  storage.child_schedule_queue.end(), std::greater<>{});
    const MeshChildSchedule schedule = storage.child_schedule_queue.back();
    storage.child_schedule_queue.pop_back();

    MeshEntry *entry = storage.entries.entry_at(schedule.slot);
    if (entry == nullptr || !storage.instance_keys.has_value() ||
        !storage.instance_keys->slot_live(schedule.slot)) {
      continue;
    }
    if (schedule.pulled) {
      if (entry->schedule_context.pulled_when != schedule.when) {
        continue;
      }
      entry->schedule_context.pulled_when = MAX_DT;
    }
    add_mesh_evaluation_slot(storage, schedule.slot);
    added = true;
  }
  return added;
}

void materialize_mesh_evaluation_order(MeshNodeStorage &storage) {
  auto &order = storage.evaluation_order;
  order.clear();
  order.reserve(storage.evaluation_candidates.count());
  for (std::size_t word_index = 0;
       word_index < storage.evaluation_candidates.word_count(); ++word_index) {
    std::uint64_t word = storage.evaluation_candidates.words[word_index];
    while (word != 0) {
      const auto bit = static_cast<std::size_t>(std::countr_zero(word));
      const std::size_t slot = word_index * SlotBitmap::bits_per_word + bit;
      if (storage.instance_keys.has_value() &&
          storage.instance_keys->slot_live(slot)) {
        if (MeshEntry *entry = storage.entries.entry_at(slot);
            entry != nullptr) {
          order.emplace_back(entry->rank, slot);
        }
      }
      word &= word - 1;
    }
  }
  std::sort(order.begin(), order.end(), [](const auto &a, const auto &b) {
    if (a.first != b.first) {
      return a.first < b.first;
    }
    return a.second < b.second;
  });
}

void prepare_mesh_evaluation_candidates(const NodeView &view,
                                        const MeshNodeContext &context,
                                        MeshNodeStorage &storage,
                                        DateTime evaluation_time) {
  auto root_input = view.input(evaluation_time);
  bool input_event = false;
  storage.refresh_all_bindings = update_mesh_source_handles(
      root_input.borrowed_ref(), storage, context.spec.keys_input_index);

  const auto root_bundle = root_input.as_bundle();
  for (std::size_t index = 0; index < root_bundle.size(); ++index) {
    auto input = root_input.indexed_child_at(index);
    if (!input.modified()) {
      continue;
    }
    input_event = true;
  }

  for (const MapArgSource &arg : context.spec.args) {
    if (arg.kind != MapArgSourceKind::OuterInput) {
      continue;
    }
    auto input = root_input.indexed_child_at(arg.outer_index);
    if (!input.modified()) {
      continue;
    }
    const auto *schema = input.schema();
    if (schema != nullptr &&
        (schema->kind == TSTypeKind::TSB || schema->kind == TSTypeKind::TSL)) {
      // A structured forwarding boundary may preserve its root handle while
      // projected leaf endpoints move.
      storage.refresh_all_bindings = true;
    }
  }

  // Multiplexed membership changes can replace or remove an element endpoint.
  // Explicitly select those keys so their child boundaries are rebound even
  // if the old element produced no notification.
  for (const std::size_t mux_index : context.spec.multiplexed_inputs) {
    if (mux_index >= storage.outer_sources.size()) {
      continue;
    }
    const TSOutputHandle &source = storage.outer_sources[mux_index];
    if (!source.bound()) {
      continue;
    }
    auto source_data = source.data_view();
    if (!source_data.valid()) {
      if (root_input.indexed_child_at(mux_index).modified()) {
        storage.refresh_all_bindings = true;
      }
      continue;
    }
    auto dict = source_data.as_dict();
    if (!dict.modified(evaluation_time)) {
      continue;
    }
    for (const ValueView &key : dict.modified_keys(evaluation_time)) {
      add_mesh_evaluation_slot(storage, storage.find_slot(key));
    }
    for (const ValueView &key : dict.added_keys()) {
      add_mesh_evaluation_slot(storage, storage.find_slot(key));
    }
    for (const ValueView &key : dict.removed_keys()) {
      add_mesh_evaluation_slot(storage, storage.find_slot(key));
    }
  }

  static_cast<void>(drain_due_mesh_schedules(storage, evaluation_time));

  if (storage.refresh_all_bindings ||
      (!input_event && !storage.evaluation_candidates.any())) {
    collect_all_mesh_evaluation_slots(storage);
  }
}

const MeshSubscribeContext &mesh_subscribe_context_of(const NodeView &view) {
  const NodeTypeRef type = view.type();
  if (!type) {
    throw std::logic_error("mesh_subscribe: node has no type");
  }
  const void *context = type.ops_ref().extended_view_context;
  if (context == nullptr) {
    throw std::logic_error("mesh_subscribe: missing typed storage context");
  }
  return *static_cast<const MeshSubscribeContext *>(context);
}

MeshSubscribeStorage &mesh_subscribe_storage_of(const NodeView &view) {
  const auto &context = mesh_subscribe_context_of(view);
  return *MemoryUtils::cast<MeshSubscribeStorage>(
      MemoryUtils::advance(view.data(), context.storage_offset));
}

const MeshKeySetContext &mesh_key_set_context_of(const NodeView &view) {
  const NodeTypeRef type = view.type();
  if (!type) {
    throw std::logic_error("mesh_key_set: node has no type");
  }
  const void *context = type.ops_ref().extended_view_context;
  if (context == nullptr) {
    throw std::logic_error("mesh_key_set: missing typed storage context");
  }
  return *static_cast<const MeshKeySetContext *>(context);
}

MeshKeySetStorage &mesh_key_set_storage_of(const NodeView &view) {
  const auto &context = mesh_key_set_context_of(view);
  return *MemoryUtils::cast<MeshKeySetStorage>(
      MemoryUtils::advance(view.data(), context.storage_offset));
}

void queue_graph_removal(MeshNodeStorage &storage, const ValueView &key) {
  storage.graphs_to_remove.emplace_back(key);
}

void remove_requester_edges(MeshNodeStorage &storage,
                            const ValueView &requester) {
  for (auto it = storage.dependents.begin(); it != storage.dependents.end();) {
    it->second.erase(Value{requester});
    if (it->second.empty()) {
      queue_graph_removal(storage, it->first.view());
      it = storage.dependents.erase(it);
    } else {
      ++it;
    }
  }
}

// ---- instance lifecycle ----

void bind_instance_inputs(const NodeView &view, const MeshNodeContext &context,
                          MeshEntry &entry, DateTime evaluation_time,
                          bool sampled = false) {
  const MeshNodeSpec &spec = context.spec;
  const TSOutputView key_source = entry.key_source.bound()
                                      ? entry.key_source.view(evaluation_time)
                                      : TSOutputView{};
  runtime_detail::bind_mapped_child_inputs(
      view, entry.graph.view(), evaluation_time, spec.child, spec.args,
      entry.key.view(), key_source, std::nullopt, false, sampled);
}

void bind_instance_output(const NodeView &view, const MeshNodeContext &context,
                          MeshEntry &entry, DateTime evaluation_time) {
  const MeshNodeSpec &spec = context.spec;
  const TSOutputView key_source = entry.key_source.bound()
                                      ? entry.key_source.view(evaluation_time)
                                      : TSOutputView{};
  runtime_detail::bind_mapped_child_output(
      view, entry.graph.view(), evaluation_time, spec.child.output_binding,
      spec.args, entry.key.view(), key_source, spec.output_binding_mode);
}

void clear_instance_output_binding(const NodeView &view,
                                   const MeshNodeContext &context,
                                   const MeshEntry &entry,
                                   DateTime evaluation_time) {
  runtime_detail::clear_mapped_output_element_binding(
      view, evaluation_time, entry.key.view(),
      context.spec.output_binding_mode);
}

void stop_and_clear_all_instances(const NodeView &view,
                                  const MeshNodeContext &context,
                                  MeshNodeStorage &storage,
                                  DateTime evaluation_time) noexcept {
  if (storage.instance_keys.has_value()) {
    for (std::size_t slot = 0; slot < storage.instance_keys->slot_capacity();
         ++slot) {
      if (!storage.instance_keys->slot_live(slot)) {
        continue;
      }
      MeshEntry *entry = storage.entries.entry_at(slot);
      if (entry != nullptr) {
        static_cast<void>(fallback_on_exception(false, [&] {
          clear_instance_output_binding(view, context, *entry, evaluation_time);
          return true;
        }));
      }
      static_cast<void>(fallback_on_exception(false, [&] {
        storage.retire_slot(slot, evaluation_time);
        return true;
      }));
    }
  }
  storage.dependents.clear();
  storage.graphs_to_remove.clear();
  storage.evaluation_candidates.reset();
  storage.evaluation_order.clear();
  storage.child_schedule_queue.clear();
  storage.refresh_all_bindings = false;
  storage.max_rank = 0;
  storage.primed = false;
  storage.current_eval_key = {};
}

MeshEntry &create_instance(const NodeView &view, const MeshNodeContext &context,
                           MeshNodeStorage &storage, const ValueView &key_view,
                           int rank, DateTime evaluation_time) {
  const MeshNodeSpec &spec = context.spec;
  initialise_mesh_storage(storage, context);

  const auto inserted = storage.instance_keys->insert(key_view);
  const std::size_t slot = inserted.slot;
  MeshEntry *existing = storage.entries.entry_at(slot);
  if (!inserted.inserted) {
    if (existing == nullptr) {
      throw std::logic_error("mesh_ live key has no instance entry");
    }
    return *existing;
  }

  auto key_rollback =
      UnwindCleanupGuard([&] { storage.retire_slot(slot, evaluation_time); });
  auto &entry = existing != nullptr
                    ? *existing
                    : storage.entries.construct_at(
                          slot, Value{ValueView{context.key_binding,
                                               (*storage.instance_keys)[slot]}});
  entry.rank = rank;
  entry.paused = true;
  entry.settled_time = MIN_DT;

  auto output = view.output(evaluation_time);
  auto output_dict = output.as_dict();
  auto output_mutation = output_dict.begin_mutation(evaluation_time);

  auto rollback = UnwindCleanupGuard([&] {
    clear_instance_output_binding(view, context, entry, evaluation_time);
    if (entry.graph.has_value() && entry.graph.view().started()) {
      entry.graph.view().stop();
    }
    (void)output_mutation.erase(entry.key.view());
    storage.retire_slot(slot, evaluation_time);
  });
  key_rollback.release();

  if (!entry.graph.has_value()) {
    entry.graph = spec.child.graph_builder.make_nested_graph(
        view.pointer(), storage.entries.graph_memory(slot),
        context.graph_layout);
  }
  if (spec.key_output_schema != nullptr) {
    entry.key_source.bind(*spec.key_output_schema, entry.key, evaluation_time);
  }

  (void)output_mutation[key_view];

  bind_instance_inputs(view, context, entry, evaluation_time, true);
  bind_instance_output(view, context, entry, evaluation_time);
  entry.schedule_context =
      MeshChildScheduleContext{&storage, slot, view.pointer()};
  entry.graph.view().set_child_schedule_observer(
      [](void *raw_context, DateTime when) {
        auto *schedule = static_cast<MeshChildScheduleContext *>(raw_context);
        if (schedule->storage != nullptr) {
          schedule->storage->push_observed_child_schedule(when, *schedule);
        }
      },
      &entry.schedule_context);
  entry.graph.view().start(evaluation_time);
  schedule_sampled_input_consumers(entry.graph.view(), evaluation_time,
                                   spec.child.input_bindings);
  add_mesh_evaluation_slot(storage, slot);
  rollback.release();

  storage.max_rank = std::max(storage.max_rank, rank);
  return entry;
}

void remove_instance(const NodeView &view, const MeshNodeContext &context,
                     MeshNodeStorage &storage, const ValueView &key_view,
                     DateTime evaluation_time) {
  const std::size_t slot = storage.find_slot(key_view);
  if (slot == KeySlotStore::npos) {
    return;
  }
  MeshEntry *entry = storage.entries.entry_at(slot);
  if (entry == nullptr) {
    throw std::logic_error("mesh_ live key has no instance entry");
  }

  clear_instance_output_binding(view, context, *entry, evaluation_time);

  auto output = view.output(evaluation_time);
  auto output_dict = output.as_dict();
  auto output_mutation = output_dict.begin_mutation(evaluation_time);
  (void)output_mutation.erase(key_view);

  remove_requester_edges(storage, key_view);
  if (auto dep_it = storage.dependents.find(key_view);
      dep_it != storage.dependents.end() && dep_it->second.empty()) {
    storage.dependents.erase(dep_it);
  }
  storage.retire_slot(slot, evaluation_time);
}

// ---- ranking ----

[[nodiscard]] bool keys_contains(const NodeView &view,
                                 const MeshNodeContext &context,
                                 const ValueView &key,
                                 DateTime evaluation_time) {
  auto keys_input = view.input(evaluation_time)
                        .indexed_child_at(context.spec.keys_input_index);
  if (!keys_input.valid()) {
    return false;
  }
  return keys_input.as_set().contains(key);
}

void process_graphs_to_remove(const NodeView &view,
                              const MeshNodeContext &context,
                              MeshNodeStorage &storage,
                              DateTime evaluation_time) {
  if (storage.graphs_to_remove.empty()) {
    return;
  }

  std::vector<Value> to_remove;
  to_remove.swap(storage.graphs_to_remove);
  for (const Value &k : to_remove) {
    auto it = storage.dependents.find(k);
    const bool has_dependents =
        it != storage.dependents.end() && !it->second.empty();
    if (!has_dependents &&
        !keys_contains(view, context, k.view(), evaluation_time)) {
      remove_instance(view, context, storage, k.view(), evaluation_time);
    }
  }
}

void reconcile_requested_keys(const NodeView &view,
                              const MeshNodeContext &context,
                              MeshNodeStorage &storage,
                              const TSSInputView &key_set,
                              DateTime evaluation_time) {
  std::vector<Value> removed;
  removed.reserve(storage.active_count());
  for (std::size_t slot = 0; slot < storage.instance_keys->slot_capacity();
       ++slot) {
    if (!storage.instance_keys->slot_live(slot)) {
      continue;
    }
    MeshEntry *entry = storage.entries.entry_at(slot);
    if (entry == nullptr) {
      throw std::logic_error("mesh_ live key has no instance entry");
    }
    if (key_set.contains(entry->key.view())) {
      continue;
    }

    auto it = storage.dependents.find(entry->key);
    const bool has_dependents =
        it != storage.dependents.end() && !it->second.empty();
    if (!has_dependents) {
      removed.push_back(entry->key);
    }
  }
  for (const Value &key : removed) {
    remove_instance(view, context, storage, key.view(), evaluation_time);
  }

  for (const ValueView &key : key_set.values()) {
    if (storage.find(key) == nullptr) {
      create_instance(view, context, storage, key, storage.max_rank,
                      evaluation_time);
    }
  }
  storage.primed = true;
}

void re_rank(MeshNodeStorage &storage, const ValueView &key,
             const ValueView &depends_on, std::vector<Value> &stack) {
  MeshEntry *key_entry = storage.find(key);
  MeshEntry *dep_entry = storage.find(depends_on);
  if (key_entry == nullptr || dep_entry == nullptr) {
    return;
  }
  if (key_entry->rank > dep_entry->rank) {
    return;
  }

  key_entry->rank = dep_entry->rank + 1;
  storage.max_rank = std::max(storage.max_rank, key_entry->rank);

  stack.push_back(Value{key});
  // Re-rank everything that depends on ``key``.
  if (auto it = storage.dependents.find(key); it != storage.dependents.end()) {
    for (const Value &dependent : it->second) {
      const bool on_stack =
          std::any_of(stack.begin(), stack.end(),
                      [&](const Value &s) { return s.equals(dependent); });
      if (on_stack) {
        throw std::runtime_error("mesh_ has a dependency cycle");
      }
      re_rank(storage, dependent.view(), key, stack);
    }
  }
  stack.pop_back();
}

// ---- evaluation ----

// The mesh node is the pause BOUNDARY: it resolves its instances' pauses
// internally (the settle loop) and always completes, so it returns true.
bool mesh_evaluate_impl(const void *, const NodeView &view,
                        DateTime evaluation_time) {
  if (!view.started()) {
    return true;
  }

  auto mesh_view = view.as<MeshNodeView>();
  const auto &context =
      *static_cast<const MeshNodeContext *>(mesh_view.internal_context());
  auto &storage = storage_of(view, context);
  const auto &spec = context.spec;
  initialise_mesh_storage(storage, context);
  storage.erase_retired_before(evaluation_time);

  auto root_input = view.input(evaluation_time);
  auto keys_input = root_input.indexed_child_at(spec.keys_input_index);

  // 1. __keys__ key-set membership drives instance create/remove.
  if (!keys_input.valid()) {
    storage.unsubscribe_requested_keys_noexcept();
    auto output = view.output(evaluation_time);
    // Only tear down what was ever published: clear() on a never-valid
    // dict would touch-VALIDATE it (the empty-tick rule), making a mesh
    // whose __keys__ never validated tick a valid empty dict at start
    // where released hgraph stays silent (issues #128/#132/#151).
    if (output.valid()) {
      auto output_dict = output.as_dict();
      auto output_mutation = output_dict.begin_mutation(evaluation_time);
      stop_and_clear_all_instances(view, context, storage, evaluation_time);
      output_mutation.clear();
    } else {
      stop_and_clear_all_instances(view, context, storage, evaluation_time);
    }
    return true;
  }

  {
    auto key_set = keys_input.as_set();
    const TSOutputHandle requested_keys_source =
        effective_output_handle(keys_input.bound_output());
    const bool source_changed =
        storage.observe_requested_keys_source(requested_keys_source);
    if (!storage.primed || source_changed) {
      reconcile_requested_keys(view, context, storage, key_set,
                               evaluation_time);
    } else if (keys_input.modified()) {
      for (const ValueView &k : key_set.added()) {
        if (storage.find(k) == nullptr) {
          create_instance(view, context, storage, k, storage.max_rank,
                          evaluation_time);
        }
      }
      for (const ValueView &k : key_set.removed()) {
        auto it = storage.dependents.find(k);
        const bool has_dependents =
            it != storage.dependents.end() && !it->second.empty();
        if (!has_dependents) {
          remove_instance(view, context, storage, k, evaluation_time);
        }
      }
    }
  }

  // 2. Removals queued because a dependent went away (remove_dependency).
  process_graphs_to_remove(view, context, storage, evaluation_time);

  // 3. Settle loop (pause/resume). Only children selected by an input
  //    notification, creation, pause, or internal schedule enter the worklist.
  //    Those candidates still evaluate in dependency-rank order, and a pause
  //    can add/rank a missing dependency for the next pass.
  prepare_mesh_evaluation_candidates(view, context, storage, evaluation_time);

  std::size_t guard = 0;
  while (true) {
    static_cast<void>(drain_due_mesh_schedules(storage, evaluation_time));
    // Snapshot candidate slots by rank. add_dependency can create or re-rank
    // instances mid-pass, so the next pass rematerializes this order.
    materialize_mesh_evaluation_order(storage);
    bool evaluated = false;

    for (const auto &ranked : storage.evaluation_order) {
      MeshEntry *entry = storage.entries.entry_at(ranked.second);
      if (!storage.instance_keys->slot_live(ranked.second)) {
        continue;
      }
      if (entry == nullptr || !entry->graph.has_value()) {
        continue;
      }
      if (entry->settled_time == evaluation_time) {
        continue;
      } // already done this cycle

      bind_instance_inputs(view, context, *entry, evaluation_time);
      bind_instance_output(view, context, *entry, evaluation_time);

      auto child = entry->graph.view();
      const DateTime child_next = child.next_scheduled_time();
      const bool due = child_next <= evaluation_time;
      if (!due && !entry->paused) {
        if (child_next != MAX_DT) {
          storage.push_pulled_child_schedule(child_next,
                                             entry->schedule_context);
        } else {
          entry->schedule_context.pulled_when = MAX_DT;
        }
        continue;
      }

      const ValueView entry_key = entry->key.view();
      storage.current_eval_key = entry_key.type().read_only(entry_key.data());
      auto clear_current_key = make_scope_exit(
          [&storage]() noexcept { storage.current_eval_key = {}; });
      entry->paused = false;
      if (child.evaluate(evaluation_time)) {
        entry->settled_time = evaluation_time;
        runtime_detail::finalize_mapped_child_output(
            view, evaluation_time, spec.child.output_binding,
            entry->key.view());
      } else {
        entry->paused = true;
      } // a dependency was created / ranked; re-scan
      evaluated = true;

      if (const DateTime next = child.next_scheduled_time();
          next != MAX_DT && next > evaluation_time) {
        storage.push_pulled_child_schedule(next, entry->schedule_context);
      } else {
        entry->schedule_context.pulled_when = MAX_DT;
      }
    }

    if (++guard > storage.active_count() + 64) {
      std::string detail;
      for (std::size_t slot = 0; slot < storage.instance_keys->slot_capacity();
           ++slot) {
        if (!storage.instance_keys->slot_live(slot)) {
          continue;
        }
        const MeshEntry *entry = storage.entries.entry_at(slot);
        if (entry == nullptr || !entry->graph.has_value()) {
          continue;
        }
        const auto child = entry->graph.view();
        if (entry->settled_time == evaluation_time ||
            (!entry->paused && child.next_scheduled_time() > evaluation_time)) {
          continue;
        }
        if (!detail.empty()) {
          detail.append(", ");
        }
        detail.append(entry->key.view().to_string());
        detail.append(fmt::format("[rank={}, paused={}, settled={}, next={}]",
                                  entry->rank, entry->paused,
                                  entry->settled_time == evaluation_time,
                                  child.next_scheduled_time()
                                      .time_since_epoch()
                                      .count()));
      }
      throw std::runtime_error(
          fmt::format("mesh_ failed to settle within the cycle: {}", detail));
    }

    // Output propagation or an input rebind may have scheduled another child
    // while this rank snapshot was being processed. Pull those callbacks into
    // the next pass even when no prior candidate evaluated.
    const bool added_during_pass =
        drain_due_mesh_schedules(storage, evaluation_time);
    if (!evaluated && !added_during_pass) {
      break;
    }
  }
  storage.current_eval_key = {};
  process_graphs_to_remove(view, context, storage, evaluation_time);
  storage.evaluation_candidates.reset();
  storage.evaluation_order.clear();
  storage.refresh_all_bindings = false;

  // Current-cycle observer callbacks can arrive after the queue was drained at
  // the start of a pass (for example while a new child samples valid config).
  // Purge them before the minimum future deadline re-arms the mesh parent.
  static_cast<void>(drain_due_mesh_schedules(storage, evaluation_time));
  storage.evaluation_candidates.reset();
  if (!storage.child_schedule_queue.empty()) {
    view.graph().schedule_node(view.node_index(),
                               storage.child_schedule_queue.front().when);
  }
  return true;
}

// ---- mesh_subscribe node (inside an instance: mesh_(func)[item]) ----
//
// Simplified shape: the only wired input is ``item`` (the requested key). The
// mesh's own TSD output (``self``) and the requester's key (``my_key``) come
// from the enclosing mesh node via the ``parent_node()`` walk at runtime, not
// from boundary inputs. The node "takes a key and returns the element type":
// it forwards ``self[item]`` to its output, pausing (returning false) until the
// dependency is created, ranked below the requester, and evaluated this cycle.
constexpr std::size_t subscribe_item_field =
    0; // TS<K> (active) — the requested key
constexpr std::size_t subscribe_value_field =
    1; // OUT — seeded with nothing<OUT>, rebound at
       // runtime to self[item] (reads it + makes us
       // reactive to the sibling's ticks)
constexpr std::size_t key_set_value_field =
    0; // TSS<K> seeded with nothing<TSS<K>>, rebound to
       // self.key_set() for scheduling.

[[nodiscard]] std::optional<MeshNodeView>
resolve_mesh_node(const NodeView &view) {
  GraphView graph = view.graph();
  while (graph.is_nested()) {
    NodeView parent = graph.as_nested().parent_node();
    if (parent.is<MeshNodeView>()) {
      return parent.as<MeshNodeView>();
    }
    if (!parent.valid()) {
      break;
    }
    graph = parent.graph();
  }
  return std::nullopt;
}

void forget_subscribe_dependency(MeshSubscribeStorage &storage) noexcept {
  storage.requester = Value{};
  storage.dependency = Value{};
  storage.has_dependency = false;
}

void remove_subscribe_dependency(const NodeView &view,
                                 MeshSubscribeStorage &storage) {
  if (!storage.has_dependency) {
    return;
  }
  if (std::optional<MeshNodeView> mesh = resolve_mesh_node(view);
      mesh.has_value()) {
    mesh->remove_dependency(storage.requester.view(),
                            storage.dependency.view());
  }
  forget_subscribe_dependency(storage);
}

void unbind_subscribe_value_input(const NodeView &view,
                                  DateTime evaluation_time) {
  if (!view.has_input()) {
    return;
  }
  auto input = view.input(evaluation_time);
  auto bundle = input.as_bundle();
  auto value = bundle.at(subscribe_value_field);
  if (value.is_bindable() && value.bound()) {
    value.unbind_output();
  }
}

void passivate_subscribe_value_input(const NodeView &view,
                                     DateTime evaluation_time) {
  if (!view.has_input()) {
    return;
  }
  auto input = view.input(evaluation_time);
  auto bundle = input.as_bundle();
  auto value = bundle.at(subscribe_value_field);
  if (value.active()) {
    value.make_passive();
  }
}

void clear_subscribe_output(const NodeView &view, MeshSubscribeStorage &storage,
                            DateTime evaluation_time) {
  if (!view.has_output()) {
    return;
  }
  auto output = view.output(evaluation_time);
  if (storage.owns_output_binding) {
    static_cast<void>(clear_forwarding_output_tree(std::move(output)));
  }
  storage.owns_output_binding = false;
}

void clear_subscribe_runtime_links(const NodeView &view,
                                   MeshSubscribeStorage &storage,
                                   DateTime evaluation_time) {
  unbind_subscribe_value_input(view, evaluation_time);
  clear_subscribe_output(view, storage, evaluation_time);
}

[[nodiscard]] bool
same_subscribe_dependency(const MeshSubscribeStorage &storage,
                          const ValueView &requester,
                          const ValueView &dependency) {
  return storage.has_dependency && storage.requester.has_value() &&
         storage.dependency.has_value() &&
         storage.requester.equals(requester) &&
         storage.dependency.equals(dependency);
}

void publish_subscribe_source(const NodeView &view,
                              MeshSubscribeStorage &storage,
                              const TSOutputView &source,
                              DateTime evaluation_time) {
  auto output = view.output(evaluation_time);
  bool changed = false;
  if (source.bound()) {
    changed =
        bind_forwarding_output_tree_to_source(output.borrowed_ref(), source);
    storage.owns_output_binding = true;
  } else {
    changed = clear_forwarding_output_tree(output.borrowed_ref());
    storage.owns_output_binding = false;
  }

  if (source.bound() && source.valid() && changed) {
    output.begin_mutation(evaluation_time).mark_modified();
  }
}

void passivate_key_set_value_input(const NodeView &view,
                                   DateTime evaluation_time) {
  if (!view.has_input()) {
    return;
  }
  auto input = view.input(evaluation_time);
  auto bundle = input.as_bundle();
  auto value = bundle.at(key_set_value_field);
  if (value.active()) {
    value.make_passive();
  }
}

void publish_key_set_source(const NodeView &view, MeshKeySetStorage &storage,
                            const TSOutputView &source,
                            DateTime evaluation_time) {
  auto output = view.output(evaluation_time);
  if (!output.forwarding()) {
    throw std::logic_error("mesh_key_set output must be a forwarding endpoint");
  }

  const TSOutputHandle before = output.forwarding_target();
  if (source.bound()) {
    bind_forwarding_output_to_source(output, source);
    storage.owns_output_binding = output.forwarding_bound();
  } else if (output.forwarding_bound()) {
    output.clear_forwarding_target();
    storage.owns_output_binding = false;
  }

  if (source.bound() && source.valid() &&
      !output.forwarding_target().same_as(before)) {
    output.begin_mutation(evaluation_time).mark_modified();
  }
}

bool mesh_key_set_evaluate_impl(const void *, const NodeView &view,
                                DateTime evaluation_time) {
  if (!view.started()) {
    return true;
  }

  auto &storage = mesh_key_set_storage_of(view);
  std::optional<MeshNodeView> mesh = resolve_mesh_node(view);
  if (!mesh.has_value()) {
    throw std::logic_error(
        "mesh_key_set: not evaluated inside a mesh instance");
  }

  auto input = view.input(evaluation_time);
  auto bundle = input.as_bundle();
  auto self = mesh->node().output(evaluation_time);
  auto source = self.as_dict().key_set();
  bind_input_to_source(bundle.at(key_set_value_field), source);
  publish_key_set_source(view, storage, source, evaluation_time);
  return true;
}

bool mesh_subscribe_evaluate_impl(const void *, const NodeView &view,
                                  DateTime evaluation_time) {
  if (!view.started()) {
    return true;
  }

  auto input = view.input(evaluation_time);
  auto bundle = input.as_bundle();
  auto item_in = bundle.at(subscribe_item_field);
  auto &storage = mesh_subscribe_storage_of(view);
  if (!item_in.valid()) {
    remove_subscribe_dependency(view, storage);
    clear_subscribe_runtime_links(view, storage, evaluation_time);
    return true;
  }

  std::optional<MeshNodeView> mesh = resolve_mesh_node(view);
  if (!mesh.has_value()) {
    throw std::logic_error(
        "mesh_subscribe: not evaluated inside a mesh instance");
  }

  const ValueView my_key = mesh->current_key();
  const Value item{item_in.value()};
  if (!my_key.has_value()) {
    remove_subscribe_dependency(view, storage);
    clear_subscribe_runtime_links(view, storage, evaluation_time);
    return true;
  }

  if (!same_subscribe_dependency(storage, my_key, item.view())) {
    remove_subscribe_dependency(view, storage);
    clear_subscribe_runtime_links(view, storage, evaluation_time);
    storage.requester = Value{my_key};
    storage.dependency = item;
    storage.has_dependency = true;
  }

  // Register the dependency (creating / ranking the target on demand). If the
  // target is not yet available this cycle, PAUSE: the mesh resolves it in rank
  // order and re-evaluates this instance to resume from here.
  if (!mesh->add_dependency(my_key, item.view())) {
    return false;
  }

  // Available — (re)bind our dynamic ``value`` input to self[item] so we
  // forward it AND become reactive to its future ticks (a sibling tick
  // reschedules us via the input notification). Binding is idempotent when
  // already pointed at it.
  auto self = mesh->node().output(evaluation_time);
  auto dict = self.as_dict();
  if (dict.contains(item.view())) {
    auto source = dict.at(item.view());
    bind_input_to_source(bundle.at(subscribe_value_field), source);
    publish_subscribe_source(view, storage, source, evaluation_time);
  } else {
    clear_subscribe_runtime_links(view, storage, evaluation_time);
  }
  return true;
}

void mesh_subscribe_stop(const NodeView &view, DateTime evaluation_time) {
  auto &storage = mesh_subscribe_storage_of(view);
  remove_subscribe_dependency(view, storage);
  passivate_subscribe_value_input(view, evaluation_time);
  storage.owns_output_binding = false;
}

void mesh_key_set_stop(const NodeView &view, DateTime evaluation_time) {
  auto &storage = mesh_key_set_storage_of(view);
  passivate_key_set_value_input(view, evaluation_time);
  storage.owns_output_binding = false;
}

void mesh_node_stop(const NodeView &view, DateTime evaluation_time) {
  auto mesh_view = view.as<MeshNodeView>();
  auto &storage = storage_of(view, *static_cast<const MeshNodeContext *>(
                                       mesh_view.internal_context()));

  auto output = view.output(evaluation_time);
  auto output_dict = output.as_dict();
  auto output_mutation = output_dict.begin_mutation(evaluation_time);

  stop_and_clear_all_instances(
      view, *static_cast<const MeshNodeContext *>(mesh_view.internal_context()),
      storage, evaluation_time);
  output_mutation.clear();
}
} // namespace

// ---- MeshNodeView ----

const void *MeshNodeView::node_view_type_id() noexcept {
  static const char token{};
  return &token;
}

MeshNodeView MeshNodeView::from_node(NodeView view, const void *context) {
  if (context == nullptr) {
    throw std::logic_error("MeshNodeView requires a typed view context");
  }
  const auto &typed_context = *static_cast<const MeshNodeContext *>(context);
  void *storage =
      MemoryUtils::advance(view.data(), typed_context.storage_offset);
  return MeshNodeView{std::move(view), context, storage};
}

const NodeView &MeshNodeView::node() const noexcept { return view_; }

std::size_t MeshNodeView::active_count() const noexcept {
  return MemoryUtils::cast<MeshNodeStorage>(storage_)->active_count();
}

std::size_t MeshNodeView::child_graph_count() const noexcept {
  const auto &storage = *MemoryUtils::cast<MeshNodeStorage>(storage_);
  std::size_t count = 0;
  for (std::size_t slot = 0; slot < storage.entries.slot_capacity(); ++slot) {
    const auto *entry = storage.entries.entry_at(slot);
    if (entry != nullptr && entry->graph.has_value()) {
      ++count;
    }
  }
  return count;
}

bool MeshNodeView::child_graphs_use_in_place_storage() const noexcept {
  const auto &storage = *MemoryUtils::cast<MeshNodeStorage>(storage_);
  for (std::size_t slot = 0; slot < storage.entries.slot_capacity(); ++slot) {
    const auto *entry = storage.entries.entry_at(slot);
    if (entry != nullptr && entry->graph.has_value() &&
        !entry->graph.uses_external_storage()) {
      return false;
    }
  }
  return true;
}

ValueView MeshNodeView::current_key() const {
  const ValuePtr pointer =
      MemoryUtils::cast<MeshNodeStorage>(storage_)->current_eval_key;
  return pointer.is_unbound()
             ? ValueView{}
             : ValueView{ValueTypeRef::checked(pointer), pointer.data()};
}

bool MeshNodeView::add_dependency(const ValueView &key,
                                  const ValueView &depends_on) const {
  if (key.equals(depends_on)) {
    throw std::runtime_error("mesh_ has a dependency cycle");
  }

  auto &storage = *MemoryUtils::cast<MeshNodeStorage>(storage_);
  const auto &context = *static_cast<const MeshNodeContext *>(context_);
  const DateTime t = view_.graph().evaluation_time();

  storage.dependents[Value{depends_on}].insert(Value{key});

  MeshEntry *key_entry = storage.find(key);
  if (key_entry == nullptr) {
    return false;
  } // defensive: we should be evaluating it

  MeshEntry *dep_entry = storage.find(depends_on);
  if (dep_entry == nullptr) {
    // Create the dependency on demand, same cycle, ranked below the requester;
    // the resolver evaluates it first (lower rank) and then resumes us.
    create_instance(view_, context, storage, depends_on, 0, t);
    std::vector<Value> stack;
    re_rank(storage, key, depends_on, stack);
    return false;
  }

  if (key_entry->rank <= dep_entry->rank) {
    // The requester must outrank its dependency; re-rank and re-evaluate in
    // order.
    std::vector<Value> stack;
    re_rank(storage, key, depends_on, stack);
    return false;
  }

  // The dependency exists and is ranked below us: available if it produced its
  // result this cycle, OR if it has nothing to do this cycle — a quiescent
  // instance's current output IS its settled result, and the settle loop only
  // runs due-or-paused instances, so pausing on it would never be resolved
  // (the requester would re-pause every pass until the guard threw). Rank
  // order makes the quiescence test sound: a due dependency ran earlier in
  // the same pass and is already settled or paused by the time we ask.
  if (dep_entry->settled_time == t) {
    return true;
  }
  if (dep_entry->paused || !dep_entry->graph.has_value()) {
    return false;
  }
  return dep_entry->graph.view().next_scheduled_time() > t;
}

void MeshNodeView::remove_dependency(const ValueView &key,
                                     const ValueView &depends_on) const {
  auto &storage = *MemoryUtils::cast<MeshNodeStorage>(storage_);
  auto it = storage.dependents.find(depends_on);
  if (it == storage.dependents.end()) {
    return;
  }
  it->second.erase(Value{key});
  if (it->second.empty()) {
    queue_graph_removal(storage, depends_on);
    storage.dependents.erase(it);
  }
}

MeshNodeView::MeshNodeView(NodeView view, const void *context,
                           void *storage) noexcept
    : view_(std::move(view)), context_(context), storage_(storage) {}

NodeBuilder mesh_node(NodeTypeMetaData meta, MeshNodeSpec spec) {
  meta.node_kind = NodeKind::Nested;
  meta.valid_inputs = std::vector<std::size_t>{};
  if (meta.output_schema == nullptr ||
      meta.output_schema->kind != TSTypeKind::TSD) {
    throw std::invalid_argument("mesh_node requires a TSD output schema");
  }
  if (spec.output_binding_mode !=
      MapOutputBindingMode::ChildTerminalWritesElement) {
    if (meta.output_schema == nullptr ||
        meta.output_schema->kind != TSTypeKind::TSD ||
        meta.output_schema->element_ts() == nullptr) {
      throw std::invalid_argument(
          "mesh_node forwarding-element output requires a TSD output schema");
    }
    meta.output_endpoint_schema = TSEndpointSchema::non_peered_dict(
        meta.output_schema,
        TSEndpointSchema::peered(meta.output_schema->element_ts()));
  }

  const auto key_binding =
      ValuePlanFactory::instance().type_for(meta.output_schema->key_type());
  if (!key_binding) {
    throw std::logic_error("mesh_node could not resolve its key binding");
  }
  const MemoryUtils::StorageLayout graph_layout =
      spec.child.graph_builder.nested_storage_layout();
  const GraphTypeRef child_graph_type = spec.child.graph_builder.nested_type();
  if (!child_graph_type) {
    throw std::logic_error("mesh_node could not resolve its child graph type");
  }

  NodeTypeDescriptor descriptor;
  descriptor.schema = std::move(meta);

  const std::array fields{NodeStorageField{
      .name = mesh_storage_field_name,
      .plan = &MemoryUtils::plan_for<MeshNodeStorage>(),
  }};
  // The mesh field destroys BEFORE the owned TSD output: instance children
  // forward into it (and read it as their self-context).
  descriptor.storage_plan =
      &node_storage_plan_for(descriptor.schema, {}, fields);

  descriptor.callbacks.stop = &mesh_node_stop;
  descriptor.ops.evaluate_impl = &mesh_evaluate_impl;
  descriptor.ops.storage_metrics_impl = &mesh_storage_metrics;
  descriptor.ops.extended_view_type_id = MeshNodeView::node_view_type_id();
  MeshNodeStorage debug_exemplar;
  debug_exemplar.entries.bind_graph_layout(graph_layout);
  const std::size_t entries_offset = static_cast<std::size_t>(
      reinterpret_cast<const std::byte *>(&debug_exemplar.entries) -
      reinterpret_cast<const std::byte *>(&debug_exemplar));
  MeshEntry debug_entry{Value{key_binding}};
  const std::size_t graph_pointer_offset =
      static_cast<std::size_t>(
          reinterpret_cast<const std::byte *>(&debug_entry.graph) -
          reinterpret_cast<const std::byte *>(&debug_entry)) +
      GraphValue::debug_pointer_offset();
  descriptor.dynamic_debug = NodeTypeDescriptor::DynamicDebug{
      .key_type = key_binding.record(),
      .element_type = child_graph_type.record(),
      .layout = debug_exemplar.entries.debug_layout(
          descriptor.storage_plan->component(mesh_storage_field_name).offset +
              entries_offset,
          graph_pointer_offset, true),
  };
  descriptor.ops.extended_view_context = &register_mesh_node_context(
      std::move(spec),
      descriptor.storage_plan->component(mesh_storage_field_name).offset,
      key_binding, graph_layout);

  return NodeBuilder::from_descriptor(std::move(descriptor));
}

NodeBuilder mesh_subscribe_node(NodeTypeMetaData meta) {
  meta.node_kind = NodeKind::Compute;
  meta.output_endpoint_schema =
      forwarding_output_endpoint_schema(meta.output_schema);

  NodeTypeDescriptor descriptor;
  descriptor.schema = std::move(meta);

  const std::array fields{NodeStorageField{
      .name = mesh_subscribe_storage_field_name,
      .plan = &MemoryUtils::plan_for<MeshSubscribeStorage>(),
  }};
  descriptor.storage_plan = &node_storage_plan_for(descriptor.schema, fields);

  const auto &context = register_mesh_subscribe_context(
      descriptor.storage_plan->component(mesh_subscribe_storage_field_name)
          .offset);

  descriptor.callbacks.stop = &mesh_subscribe_stop;
  descriptor.ops.evaluate_impl = &mesh_subscribe_evaluate_impl;
  descriptor.ops.extended_view_context = &context;
  return NodeBuilder::from_descriptor(std::move(descriptor));
}

NodeBuilder mesh_key_set_node(NodeTypeMetaData meta) {
  meta.node_kind = NodeKind::Compute;
  meta.schedule_on_start = true;
  meta.output_endpoint_schema = TSEndpointSchema::peered(meta.output_schema);

  NodeTypeDescriptor descriptor;
  descriptor.schema = std::move(meta);

  const std::array fields{NodeStorageField{
      .name = mesh_key_set_storage_field_name,
      .plan = &MemoryUtils::plan_for<MeshKeySetStorage>(),
  }};
  descriptor.storage_plan = &node_storage_plan_for(descriptor.schema, fields);

  const auto &context = register_mesh_key_set_context(
      descriptor.storage_plan->component(mesh_key_set_storage_field_name)
          .offset);

  descriptor.callbacks.stop = &mesh_key_set_stop;
  descriptor.ops.evaluate_impl = &mesh_key_set_evaluate_impl;
  descriptor.ops.extended_view_context = &context;
  return NodeBuilder::from_descriptor(std::move(descriptor));
}
} // namespace hgraph
