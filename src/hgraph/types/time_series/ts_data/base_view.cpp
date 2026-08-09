#include <hgraph/types/time_series/ts_data.h>

#include <hgraph/runtime/graph.h>
#include <hgraph/runtime/node.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/value/value.h>

#include "ownership.h"

#include <stdexcept>
#include <string>
#include <utility>

namespace hgraph {
namespace {
DynamicStorageMetrics observer_storage_metrics(const TSDataView &view) noexcept {
  if (!view.valid()) {
    return {};
  }
  try {
    DynamicStorageMetrics result =
        view.tracking().observers.dynamic_storage_metrics();
    const auto &table = view.ops();
    const auto *ownership = table.ownership_ops;
    if (ownership == nullptr) {
      return result;
    }

    const std::size_t child_count =
        ownership->child_count(table.context, view.data());
    for (std::size_t index = 0; index < child_count; ++index) {
      const auto child = ownership->child_at(
          table.context, const_cast<void *>(view.data()), index);
      if (!child.type || child.data == nullptr) {
        continue;
      }
      result += observer_storage_metrics(TSDataView{child.type, child.data});
    }
    return result;
  } catch (...) {
    return {};
  }
}
} // namespace

const TSParentLink &TSDataView::parent_link() const {
  return tracking().parent;
}

std::size_t TSDataView::child_id() const {
  const auto &link = tracking().parent;
  return link.has_parent() ? link.child_id : TS_DATA_NO_CHILD_ID;
}

bool TSDataView::has_parent() const { return tracking().parent.has_parent(); }

std::vector<std::size_t> TSDataView::path_from_root() const {
  return tracking().parent.path_from_root();
}

TSDataView TSDataView::root_view() const {
  const auto &link = tracking().parent;
  return link.has_ts_data_parent() ? link.root_view() : borrowed_ref();
}

TSParentLink TSDataView::root_endpoint_owner() const noexcept {
  if (!valid()) {
    return {};
  }

  auto current = borrowed_ref();
  for (std::size_t depth = 0; current.valid() && depth < 256; ++depth) {
    const auto &link = current.parent_link();
    if (link.has_endpoint_parent()) {
      return link;
    }
    if (!link.has_ts_data_parent()) {
      return {};
    }
    current = TSDataView{link.parent_storage_type(), link.parent_data()};
  }
  return {};
}

NodeView TSDataView::owner_node() const {
  return root_endpoint_owner().parent_node();
}

GraphView TSDataView::owner_graph() const {
  return root_endpoint_owner().parent_graph();
}

void *TSDataView::mutable_data() const {
  const auto type = type_ref();
  if (type && !has_capability(type.capabilities(), TypeCapabilities::Mutable))
    throw std::logic_error(
        "TSDataView::mutable_data requires a mutable time-series role");
  if (!ops().allows_mutation) {
    throw std::logic_error(
        "TSDataView::mutable_data requires mutable TSData ops");
  }
  return storage_.data();
}

const TSDataOps &TSDataView::ops() const { return storage_.ops(); }

const TSDataLayout &TSDataView::layout() const {
  const auto &table = ops();
  return *table.layout_impl(table.context);
}

const TSDataTracking &TSDataView::tracking() const {
  const auto &table = ops();
  return *table.tracking_impl(table.context, data());
}

ValueView TSDataView::value() const {
  const auto &table = ops();
  if (table.value_view_impl != nullptr) {
    return table.value_view_impl(table.context, data());
  }
  const auto *data_layout = table.layout_impl(table.context);
  return ValueView{data_layout->value_binding,
                   table.value_memory_impl(table.context, data())}
      .concrete();
}

ValueView TSDataView::delta_value(DateTime evaluation_time) const {
  const auto &table = ops();
  if (table.delta_view_impl != nullptr) {
    return table.delta_view_impl(table.context, data(), evaluation_time);
  }
  const auto *data_layout = table.layout_impl(table.context);
  const auto *data_tracking = table.tracking_impl(table.context, data());
  if (evaluation_time == MIN_DT ||
      data_tracking->last_modified_time != evaluation_time) {
    return ValueView{data_layout->delta_binding, nullptr};
  }
  return ValueView{data_layout->delta_binding,
                   table.delta_memory_impl(table.context, data())}
      .concrete();
}

#if HGRAPH_ENABLE_PYTHON_USER_NODES
nb::object TSDataView::value_to_python() const {
  const auto &table = ops();
  if (!table.has_current_value_impl(table.context, data())) {
    return nb::none();
  }
  return table.to_python_impl(table.context, data());
}

nb::object TSDataView::delta_value_to_python(DateTime evaluation_time) const {
  const auto &table = ops();
  if (evaluation_time == MIN_DT ||
      table.tracking_impl(table.context, data())->last_modified_time !=
          evaluation_time) {
    return nb::none();
  }
  return table.delta_to_python_impl(table.context, data(), evaluation_time);
}
#endif

DateTime TSDataView::last_modified_time() const {
  return tracking().last_modified_time;
}

bool TSDataView::modified(DateTime evaluation_time) const {
  return evaluation_time != MIN_DT &&
         tracking().last_modified_time == evaluation_time;
}

void TSDataView::subscribe(Notifiable *observer) const {
  require_live("TSDataView::subscribe");
  mutable_tracking().observers.subscribe(observer);
}

void TSDataView::unsubscribe(Notifiable *observer) const {
  require_live("TSDataView::unsubscribe");
  mutable_tracking().observers.unsubscribe(observer);
}

void TSDataView::replace_observer(Notifiable *observer,
                                  Notifiable *replacement) const {
  require_live("TSDataView::replace_observer");
  mutable_tracking().observers.replace(observer, replacement);
}

bool TSDataView::has_observers() const { return !tracking().observers.empty(); }

std::size_t TSDataView::observer_count() const {
  return tracking().observers.size();
}

bool TSDataView::has_current_value() const {
  const auto &table = ops();
  return table.has_current_value_impl(table.context, data());
}

bool TSDataView::all_valid() const {
  const auto &table = ops();
  if (!table.has_current_value_impl(table.context, data())) {
    return false;
  }
  return table.all_valid_impl(table.context, data());
}

DynamicStorageMetrics TSDataView::dynamic_storage_metrics() const noexcept {
  if (!valid()) {
    return {};
  }
  try {
    const auto &table = ops();
    return table.dynamic_storage_metrics_impl(table.context, data()) +
           observer_storage_metrics(*this);
  } catch (...) {
    return {};
  }
}

std::size_t TSDataView::indexed_child_count() const {
  const auto &table = ops();
  return table.indexed_child_count_impl(table.context, data());
}

TSDataView TSDataView::indexed_child_at(std::size_t index) const {
  const auto &table = ops();
  const auto child_count =
      table.indexed_child_count_impl(table.context, data());
  if (index >= child_count) {
    throw std::out_of_range(
        "TSDataView::indexed_child_at index " + std::to_string(index) +
        " is out of range for schema '" +
        (schema() != nullptr ? std::string{schema()->name()}
                             : std::string{"<untyped>"}) +
        "' with " + std::to_string(child_count) + " children");
  }

  const auto element_type =
      table.indexed_child_binding_impl(table.context, data(), index);
  if (!element_type) {
    throw std::logic_error(
        "TSDataView::indexed_child_at element binding is not resolved");
  }

  const auto *element_memory =
      table.indexed_child_memory_impl(table.context, data(), index);
  if (element_memory == nullptr) {
    return TSDataView{element_type, element_memory};
  }

  auto parent = borrowed_ref();
  if (!table.allows_mutation) {
    return TSDataView{element_type, element_memory};
  }
  return TSDataView{element_type, element_memory, parent, index};
}

TSDataView TSDataView::ensure_indexed_child_at(std::size_t index) const {
  const auto &table = ops();
  if (index >= table.indexed_child_count_impl(table.context, data())) {
    if (!table.indexed_child_growth) {
      throw std::out_of_range(
          "TSDataView::ensure_indexed_child_at index out of range");
    }
    static_cast<void>(table.mutable_indexed_child_memory_impl(
        table.context, mutable_data(), index));
  }
  return indexed_child_at(index);
}

bool TSDataView::clear_collection(DateTime modified_time) const {
  const auto &table = ops();
  return table.clear_collection_impl(borrowed_ref(), modified_time);
}

TSSDataView TSDataView::as_set() & { return TSSDataView{borrowed_ref()}; }

TSSDataView TSDataView::as_set() const & { return TSSDataView{borrowed_ref()}; }

TSDDataView TSDataView::as_dict() & { return TSDDataView{borrowed_ref()}; }

TSDDataView TSDataView::as_dict() const & {
  return TSDDataView{borrowed_ref()};
}

TSBDataView TSDataView::as_bundle() & { return TSBDataView{borrowed_ref()}; }

TSBDataView TSDataView::as_bundle() const & {
  return TSBDataView{borrowed_ref()};
}

TSLDataView TSDataView::as_list() & { return TSLDataView{borrowed_ref()}; }

TSLDataView TSDataView::as_list() const & {
  return TSLDataView{borrowed_ref()};
}

TSWDataView TSDataView::as_window() & { return TSWDataView{borrowed_ref()}; }

TSWDataView TSDataView::as_window() const & {
  return TSWDataView{borrowed_ref()};
}

TSDataMutationView TSDataView::begin_mutation(DateTime evaluation_time) const {
  return TSDataMutationView{borrowed_ref(), evaluation_time};
}

TSDataView::TSDataView(TSRoleTypeRef type, void *data, const TSDataView &parent,
                       std::size_t child_id)
    : storage_(type, data) {
  bind_parent(parent, child_id);
}

TSDataView::TSDataView(TSRoleTypeRef type, const void *data,
                       const TSDataView &parent, std::size_t child_id)
    : storage_(type, data) {
  bind_parent(parent, child_id);
}

void TSDataView::require_live(const char *what) const {
  if (!valid()) {
    throw std::logic_error(std::string{what} + " requires a live view");
  }
}

TSDataTracking &TSDataView::mutable_tracking() const {
  if (!valid()) {
    throw std::logic_error("TSDataView::mutable_tracking requires a live view");
  }
  const auto &table = ops();
  return *table.mutable_tracking_impl(table.context, storage_.data());
}

void TSDataView::bind_parent(const TSDataView &parent,
                             std::size_t child_id) const {
  if (!valid()) {
    throw std::logic_error("TSDataView child requires a live view");
  }
  if (!parent.valid()) {
    throw std::logic_error("TSDataView child requires a live parent view");
  }
  if (!parent.ops().allows_mutation) {
    throw std::logic_error(
        "TSDataView child requires mutable parent TSData ops");
  }
  mutable_tracking().parent =
      TSParentLink{parent.storage_type(), parent.data(), child_id};
}

void TSDataView::bind_parent(const NodeView &parent,
                             TSEndpointOwnerPort port) const {
  if (!valid()) {
    throw std::logic_error("TSDataView child requires a live view");
  }
  if (!parent.valid()) {
    throw std::logic_error(
        "TSDataView node endpoint requires a live node view");
  }
  mutable_tracking().parent = TSParentLink{parent.pointer(), port};
}

void TSDataView::bind_parent(TSInput &parent, std::size_t child_id) const {
  if (!valid()) {
    throw std::logic_error("TSDataView child requires a live view");
  }
  mutable_tracking().parent = TSParentLink{parent, child_id};
}

void TSDataView::bind_parent(TSOutput &parent, std::size_t child_id) const {
  if (!valid()) {
    throw std::logic_error("TSDataView child requires a live view");
  }
  mutable_tracking().parent = TSParentLink{parent, child_id};
}

TSDataMutationView::TSDataMutationView(TSDataView view,
                                       DateTime evaluation_time)
    : storage_(view.storage_ref()), mutation_time_(evaluation_time) {
  validate_mutation_view();
}

TSDataMutationView::TSDataMutationView(TSDataMutationView &&other) noexcept
    : storage_(std::exchange(other.storage_, TSDataStorageRef<>{})),
      mutation_time_(std::exchange(other.mutation_time_, MIN_DT)) {}

TSDataMutationView::~TSDataMutationView() noexcept = default;

TSDataView TSDataMutationView::view() const noexcept {
  return TSDataView{storage_};
}

const TSDataOps &TSDataMutationView::ops() const { return view().ops(); }

void *TSDataMutationView::mutable_data() const {
  require_active_mutation();
  return storage_.data();
}

ValueView TSDataMutationView::value() const { return view().value(); }

ValueView TSDataMutationView::delta_value(DateTime evaluation_time) const {
  return view().delta_value(evaluation_time);
}

bool TSDataMutationView::modified(DateTime evaluation_time) const {
  return modified(ops(), evaluation_time);
}

void TSDataMutationView::mark_modified() { mark_modified(ops()); }

bool TSDataMutationView::copy_value_from(const ValueView &source) {
  require_active_mutation();

  auto current = view();
  const auto &table = current.ops();
  const bool newly_modified = table.copy_value_from_impl(
      table.context, current.mutable_data(), source, mutation_time_);
  if (newly_modified) {
    // The modification may already be recorded for this cycle — e.g.
    // the storage was structurally created earlier in the same dict
    // mutation (which marks it), or a write-through forwarding link
    // landed on pre-marked storage. Recording is idempotent; parents
    // are notified only by the first recording.
    if (record_modified_local()) {
      notify_parent_modified();
    }
  }
  return newly_modified;
}

bool TSDataMutationView::move_value_from(Value &&source) {
  return move_value_from(source.view());
}

bool TSDataMutationView::move_value_from(ValueView source) {
  require_active_mutation();
  if (!source.writable_payload()) {
    throw std::invalid_argument(
        "TSDataMutationView::move_value_from requires writable source storage");
  }

  auto current = view();
  const auto &table = current.ops();
  const bool newly_modified = table.move_value_from_impl(
      table.context, current.mutable_data(), std::move(source), mutation_time_);
  if (newly_modified) {
    // The modification may already be recorded for this cycle — e.g.
    // the storage was structurally created earlier in the same dict
    // mutation (which marks it), or a write-through forwarding link
    // landed on pre-marked storage. Recording is idempotent; parents
    // are notified only by the first recording.
    if (record_modified_local()) {
      notify_parent_modified();
    }
  }
  return newly_modified;
}

bool TSDataMutationView::invalidate() {
  require_active_mutation();

  TSDataView current = view();
  if (!current.has_current_value()) {
    return false;
  }

  const auto &table = current.ops();
  if (const auto *ownership = table.ownership_ops; ownership != nullptr) {
    const std::size_t children =
        ownership->child_count(table.context, current.data());
    for (std::size_t index = 0; index < children; ++index) {
      const auto child_ref =
          ownership->child_at(table.context, current.mutable_data(), index);
      if (!child_ref.type || child_ref.data == nullptr) {
        continue;
      }
      // A structural alternative can expose borrowed input-role
      // children beneath a mutable root. Invalidating the root must
      // not mutate those upstream sources.
      if (!has_capability(child_ref.type.capabilities(),
                          TypeCapabilities::Mutable)) {
        continue;
      }
      TSDataMutationView child{TSDataView{child_ref.type, child_ref.data},
                               mutation_time_};
      static_cast<void>(child.invalidate());
    }
  }

  auto &state =
      *table.mutable_tracking_impl(table.context, current.mutable_data());
  state.observers.notify(mutation_time_);
  state.parent.notify_child_modified(mutation_time_);
  state.last_modified_time = MIN_DT;
  return true;
}

#if HGRAPH_ENABLE_PYTHON_USER_NODES
bool TSDataMutationView::from_python(nb::handle source) {
  require_active_mutation();
  if (source.is_none()) {
    return false;
  }

  auto current = view();
  const auto &table = current.ops();
  const bool newly_modified = table.from_python_impl(
      table.context, current.mutable_data(), source, mutation_time_);
  if (newly_modified && !record_modified_local()) {
    throw std::logic_error("TSDataMutationView::from_python reported a new "
                           "modification that was already recorded");
  }
  if (newly_modified) {
    notify_parent_modified();
  }
  return newly_modified;
}

#endif

void TSDataMutationView::validate_mutation_view() const {
  if (mutation_time_ == MIN_DT) {
    throw std::invalid_argument(
        "TSDataMutationView requires a concrete evaluation time");
  }
  (void)view().mutable_data();
}

bool TSDataMutationView::record_modified_local() const {
  return record_modified_local(ops());
}

void TSDataMutationView::notify_parent_modified() const {
  notify_parent_modified(ops());
}

void apply_slot_mutation_result(TSDataMutationView &mutation,
                                const SlotTSDataMutationResult &result) {
  if (!result.changed) {
    return;
  }
  mutation.mark_modified();
}
} // namespace hgraph
