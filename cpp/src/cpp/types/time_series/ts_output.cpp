#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_output_view.h>
#include <hgraph/types/time_series/ts_reference.h>
#include <hgraph/types/time_series/ref_link.h>
#include <hgraph/types/time_series/observer_list.h>
#include <hgraph/types/time_series/set_delta.h>
#include <hgraph/types/time_series/map_delta.h>
#include <hgraph/types/time_series/ts_dict_view.h>
#include <hgraph/types/time_series/ts_list_view.h>
#include <hgraph/types/time_series/ts_set_view.h>
#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/time_series/ts_meta_schema.h>
#include <hgraph/types/time_series/ts_type_registry.h>
#include <hgraph/types/value/map_storage.h>
#include <hgraph/types/value/key_set.h>
#include <hgraph/types/value/value.h>

#include <unordered_map>
#include <unordered_set>

namespace hgraph {

// ============================================================================
// ContainsTracking - tracks TSS key membership as TSD[K, TS[bool]]
// ============================================================================

struct ContainsTracking : Notifiable {
    struct Tracker {
        std::unordered_set<void*> requesters;
        value::Value<> key_value;
    };

    TSOutput* owner_;
    std::unordered_map<size_t, Tracker> trackers_;
    std::unordered_set<size_t> pending_removal_;
    engine_time_t last_cleanup_time_{MIN_DT};
    ObserverList* registered_obs_{nullptr};
    std::unique_ptr<TSValue> tsd_;
    const TSMeta* meta_{nullptr};

    explicit ContainsTracking(TSOutput* owner) : owner_(owner) {}

    ~ContainsTracking() override {
        if (registered_obs_) {
            registered_obs_->remove_observer(this);
        }
    }

    static const value::TypeMeta* get_element_type(const TSMeta* meta) {
        if (meta->value_type->kind == value::TypeKind::Tuple) {
            return meta->value_type->fields[0].type->element_type;
        }
        return meta->value_type->element_type;
    }

    void init(engine_time_t current_time) {
        if (meta_) return;

        auto output_view = owner_->view(current_time);
        const auto* tss_meta = output_view.ts_meta();

        const auto* elem_type = get_element_type(tss_meta);
        auto& registry = TSTypeRegistry::instance();
        auto& cache = TSMetaSchemaCache::instance();

        meta_ = registry.tsd(elem_type, registry.ts(cache.bool_meta()));
        tsd_ = std::make_unique<TSValue>(meta_);

        auto observer_schema = cache.get_observer_schema(tss_meta);
        auto observer_tuple = value::View(
            output_view.ts_view().view_data().observer_data, observer_schema);
        registered_obs_ = static_cast<ObserverList*>(
            observer_tuple.as_tuple().at(0).data());
        registered_obs_->add_observer(this);
    }

    void process_pending_removals(engine_time_t current_time) {
        if (pending_removal_.empty()) return;
        if (current_time <= last_cleanup_time_) return;

        auto dict_view = tsd_->ts_view(current_time).as_dict();
        for (auto it = pending_removal_.begin(); it != pending_removal_.end(); ) {
            auto tracker_it = trackers_.find(*it);
            if (tracker_it != trackers_.end() && tracker_it->second.requesters.empty()) {
                dict_view.remove(tracker_it->second.key_value.const_view());
                trackers_.erase(tracker_it);
            }
            it = pending_removal_.erase(it);
        }
        last_cleanup_time_ = current_time;
    }

    TSView get_view(const value::View& key, void* requester, engine_time_t current_time) {
        init(current_time);
        process_pending_removals(current_time);

        const auto* tss_meta = owner_->ts_meta();
        const auto* elem_type = get_element_type(tss_meta);
        size_t key_hash = elem_type->ops->hash(key.data(), elem_type);

        pending_removal_.erase(key_hash);

        auto dict_view = tsd_->ts_view(current_time).as_dict();
        auto tracker_it = trackers_.find(key_hash);
        if (tracker_it == trackers_.end()) {
            TSView elem_ts = dict_view.create(key);

            auto output_view = owner_->view(current_time);
            bool present = output_view.ts_view().as_set().contains(key);
            auto* bool_meta = TSMetaSchemaCache::instance().bool_meta();
            value::View bool_view(&present, bool_meta);
            elem_ts.set_value(bool_view);

            Tracker tracker;
            tracker.key_value = value::Value<>(key);
            auto [ins_it, _] = trackers_.emplace(key_hash, std::move(tracker));
            tracker_it = ins_it;
        }

        tracker_it->second.requesters.insert(requester);

        return dict_view.at(tracker_it->second.key_value.const_view());
    }

    void release(const value::View& key, void* requester) {
        if (trackers_.empty()) return;

        const auto* tss_meta = owner_->ts_meta();
        const auto* elem_type = get_element_type(tss_meta);
        size_t key_hash = elem_type->ops->hash(key.data(), elem_type);

        auto it = trackers_.find(key_hash);
        if (it == trackers_.end()) return;

        it->second.requesters.erase(requester);
        if (it->second.requesters.empty()) {
            pending_removal_.insert(key_hash);
        }
    }

    // Notifiable callback — fired when TSS content changes
    void notify(engine_time_t time) override {
        if (trackers_.empty() || !tsd_) return;

        auto output_view = owner_->view(time);
        auto set_view = output_view.ts_view().as_set();
        auto dict_view = tsd_->ts_view(time).as_dict();
        auto* bool_meta = TSMetaSchemaCache::instance().bool_meta();

        for (auto& [hash, tracker] : trackers_) {
            bool present = set_view.contains(tracker.key_value.const_view());
            auto elem_view = dict_view.at(tracker.key_value.const_view());
            if (!elem_view) continue;

            auto val_view = elem_view.value();
            bool current = val_view.valid() ? val_view.as<bool>() : false;

            if (current != present) {
                bool new_val = present;
                value::View new_view(&new_val, bool_meta);
                elem_view.set_value(new_view);
            }
        }
    }
};

// ============================================================================
// AlternativeTimePropagator - copies native container time to alternative
// ============================================================================

/// Subscribes to the native TSD's ObserverList. When the native container
/// fires notify_modified, this copies the native container time to the
/// alternative's container time so that modified/valid checks work correctly.
struct AlternativeTimePropagator : Notifiable {
    engine_time_t* native_container_time{nullptr};
    engine_time_t* alt_container_time{nullptr};

    void notify(engine_time_t et) override {
        if (et == MIN_DT) return;
        if (native_container_time && alt_container_time) {
            *alt_container_time = *native_container_time;
        }
    }
};

// ============================================================================
// AlternativeStructuralObserver Implementation
// ============================================================================

AlternativeStructuralObserver::AlternativeStructuralObserver(
    TSOutput* output,
    TSValue* alt,
    const TSMeta* native_meta,
    const TSMeta* target_meta
)
    : output_(output)
    , alt_(alt)
    , native_meta_(native_meta)
    , target_meta_(target_meta)
    , registered_key_set_(nullptr)
{}

AlternativeStructuralObserver::~AlternativeStructuralObserver() {
    // Unregister from KeySet if registered
    if (registered_key_set_) {
        registered_key_set_->remove_observer(this);
        registered_key_set_ = nullptr;
    }
}

void AlternativeStructuralObserver::register_with(value::KeySet* key_set) {
    // Unregister from previous KeySet if any
    if (registered_key_set_) {
        registered_key_set_->remove_observer(this);
    }

    registered_key_set_ = key_set;
    if (key_set) {
        key_set->add_observer(this);
    }
}

void AlternativeStructuralObserver::on_capacity(size_t /*old_cap*/, size_t /*new_cap*/) {
    // Alternative capacity is managed separately - no action needed
}

void AlternativeStructuralObserver::on_insert(size_t slot) {
    // A new element was added to the native at this slot
    // We need to create the corresponding element in the alternative
    // and establish the appropriate link

    if (!output_ || !alt_) return;

    // Get native and alternative views at setup time
    engine_time_t setup_time = MIN_DT;
    TSView native_view = output_->native_value().ts_view(setup_time);
    // Set valid path so Case 3 (TS→REF) can create resolvable TSReferences
    native_view.view_data().path = output_->root_path();
    TSView alt_view = alt_->ts_view(setup_time);

    // Extract key from native MapStorage at the given storage slot
    const ViewData& native_vd = native_view.view_data();
    auto* native_storage = static_cast<value::MapStorage*>(native_vd.value_data);
    const void* key_data = native_storage->key_at_slot(slot);
    value::View key_view(key_data, native_meta_->key_type);

    // Get native element by key
    TSView native_elem = native_view.as_dict().at(key_view);

    // Create corresponding element in alternative
    TSView alt_elem = alt_view.as_dict().create(key_view);

    if (!native_elem || !alt_elem) return;

    // Get element schemas
    const TSMeta* native_elem_meta = native_meta_->element_ts;
    const TSMeta* target_elem_meta = target_meta_->element_ts;

    // Establish the link for this new element
    output_->establish_links_recursive(
        *alt_,
        alt_elem,
        native_elem,
        target_elem_meta,
        native_elem_meta
    );
}

void AlternativeStructuralObserver::on_erase(size_t slot) {
    // An element was removed from the native at this slot
    // Clean up the corresponding element in the alternative

    if (!output_ || !alt_) return;

    // Get native and alternative views at setup time
    engine_time_t setup_time = MIN_DT;
    TSView native_view = output_->native_value().ts_view(setup_time);
    TSView alt_view = alt_->ts_view(setup_time);

    // Extract key from native MapStorage (key data still valid during erase callback)
    const ViewData& native_vd = native_view.view_data();
    auto* native_storage = static_cast<value::MapStorage*>(native_vd.value_data);
    const void* key_data = native_storage->key_at_slot(slot);
    value::View key_view(key_data, native_meta_->key_type);

    // Remove from alternative by key
    alt_view.as_dict().remove(key_view);
}


void AlternativeStructuralObserver::on_update(size_t /*slot*/) {
    // Value updates are handled by AlternativeTimePropagator (subscribes to native ObserverList).
    // No structural changes needed here.
}

void AlternativeStructuralObserver::on_clear() {
    // All elements were cleared from native
    // Clear all links in the alternative

    if (!alt_) return;

    engine_time_t setup_time = MIN_DT;
    TSView alt_view = alt_->ts_view(setup_time);

    // For TSD/TSL, iterate and unbind all elements
    // This is a simplified approach - more sophisticated would track each element
    alt_view.unbind();
}

void TSOutput::subscribe_structural_observer(TSView native_view, AlternativeStructuralObserver* observer) {
    // For TSD/TSL types, we need to subscribe to the KeySet
    // to receive insert/erase notifications

    if (!observer) return;

    const TSMeta* meta = native_view.ts_meta();
    if (!meta) return;

    if (meta->kind == TSKind::TSD) {
        // For TSD, register with the KeySet's observer dispatcher.
        // The native's value_data points to a MapStorage which owns the KeySet.
        const ViewData& vd = native_view.view_data();
        if (vd.value_data) {
            auto* map_storage = static_cast<value::MapStorage*>(vd.value_data);
            // Use register_with for proper lifecycle management
            observer->register_with(&map_storage->key_set());
        }
    } else if (meta->kind == TSKind::TSL) {
        // For TSL, structural changes (size changes) are less common and typically
        // handled differently. Fixed-size TSL doesn't have structural changes.
        // Dynamic TSL would need similar observer registration when implemented.
        // For now, TSL alternatives sync on access.
    }
}

// ============================================================================
// TSOutput Implementation
// ============================================================================

TSOutput::TSOutput(const TSMeta* ts_meta, node_ptr owner, size_t port_index)
    : native_value_(ts_meta)
    , owning_node_(owner)
    , port_index_(port_index) {
}

TSOutput::TSOutput(TSOutput&& other) noexcept
    : native_value_(std::move(other.native_value_))
    , alternatives_(std::move(other.alternatives_))
    , structural_observers_(std::move(other.structural_observers_))
    , time_propagators_(std::move(other.time_propagators_))
    , forwarded_target_(std::move(other.forwarded_target_))
    , owning_node_(other.owning_node_)
    , port_index_(other.port_index_)
    , contains_tracking_(std::exchange(other.contains_tracking_, nullptr))
{
    other.owning_node_ = nullptr;
    other.port_index_ = 0;
}

TSOutput& TSOutput::operator=(TSOutput&& other) noexcept {
    if (this != &other) {
        delete static_cast<ContainsTracking*>(contains_tracking_);
        native_value_ = std::move(other.native_value_);
        alternatives_ = std::move(other.alternatives_);
        structural_observers_ = std::move(other.structural_observers_);
        time_propagators_ = std::move(other.time_propagators_);
        forwarded_target_ = std::move(other.forwarded_target_);
        owning_node_ = other.owning_node_;
        port_index_ = other.port_index_;
        contains_tracking_ = std::exchange(other.contains_tracking_, nullptr);
        other.owning_node_ = nullptr;
        other.port_index_ = 0;
    }
    return *this;
}

TSOutput::~TSOutput() {
    delete static_cast<ContainsTracking*>(contains_tracking_);
}

TSView TSOutput::get_contains_view(const value::View& key, void* requester, engine_time_t current_time) {
    if (!contains_tracking_) {
        contains_tracking_ = new ContainsTracking(this);
    }
    return static_cast<ContainsTracking*>(contains_tracking_)->get_view(key, requester, current_time);
}

void TSOutput::release_contains(const value::View& key, void* requester) {
    if (!contains_tracking_) return;
    static_cast<ContainsTracking*>(contains_tracking_)->release(key, requester);
}

TSOutputView TSOutput::view(engine_time_t current_time) {
    if (is_forwarded()) {
        // Cross-graph forwarding: all 7 fields from outer's storage
        ViewData vd;
        vd.value_data = forwarded_target_->value_data;
        vd.time_data = forwarded_target_->time_data;
        vd.observer_data = forwarded_target_->observer_data;
        vd.delta_data = forwarded_target_->delta_data;
        vd.link_data = forwarded_target_->link_data;
        vd.ops = forwarded_target_->ops;
        vd.meta = forwarded_target_->meta;
        vd.path = root_path();
        return TSOutputView(TSView(vd, current_time), this);
    }
    TSView ts_view = native_value_.ts_view(current_time);
    // Set the path on the view
    ts_view.view_data().path = root_path();
    return TSOutputView(std::move(ts_view), this);
}

TSOutputView TSOutput::view(engine_time_t current_time, const TSMeta* schema) {
    // Same schema as native - return native view
    if (schema == native_value_.meta()) {
        return view(current_time);
    }

    // Get or create alternative for this schema
    TSValue& alt = get_or_create_alternative(schema);
    TSView ts_view = alt.ts_view(current_time);
    // Set the path on the view (same root, different schema)
    ts_view.view_data().path = root_path();
    return TSOutputView(std::move(ts_view), this);
}

TSValue& TSOutput::get_or_create_alternative(const TSMeta* schema) {
    // Check for existing alternative
    auto it = alternatives_.find(schema);
    if (it != alternatives_.end()) {
        return it->second;
    }

    // Create new alternative with target schema
    auto [insert_it, inserted] = alternatives_.emplace(schema, TSValue(schema));
    TSValue& alt = insert_it->second;

    // Establish appropriate links from alternative to native
    // Use a dummy time since we're just setting up structure
    engine_time_t setup_time = MIN_DT;
    TSView alt_view = alt.ts_view(setup_time);
    TSView native_view = native_value_.ts_view(setup_time);
    // Set valid path on native view so Case 3 (TS→REF) can create resolvable TSReferences
    native_view.view_data().path = root_path();

    establish_links_recursive(alt, alt_view, native_view, schema, native_value_.meta());

    return alt;
}

void TSOutput::establish_links_recursive(
    TSValue& alt,
    TSView alt_view,
    TSView native_view,
    const TSMeta* target_meta,
    const TSMeta* native_meta
) {
    // Case 1: Same type - create direct Link
    if (target_meta == native_meta) {
        alt_view.bind(native_view);
        return;
    }

    // Case 2: REF on native side (dereferencing needed)
    if (native_meta->kind == TSKind::REF) {
        const TSMeta* deref_meta = native_meta->element_ts;  // REF's element_ts is the referenced type

        // Get the REFLink from the alternative's link storage (inline storage)
        // The link schema uses REFLink at each position, providing stable addresses
        // for proper lifecycle management with two-phase removal
        void* link_data = alt_view.view_data().link_data;
        if (!link_data) {
            // No link storage available - fall back to direct bind
            alt_view.bind(native_view);
            return;
        }

        REFLink* ref_link = static_cast<REFLink*>(link_data);

        // Bind the REFLink to the REF source - this sets up:
        // 1. Subscription to REF source for rebind notifications
        // 2. Initial dereference to get current target
        ref_link->bind_to_ref(native_view, MIN_DT);

        if (target_meta == deref_meta) {
            // REF[X] → X: Simple REFLink dereference
            // The REFLink's target is now the dereferenced value
            if (ref_link->valid()) {
                TSView target = ref_link->target_view(MIN_DT);
                if (target) {
                    // For simple dereference, the alternative's value access
                    // goes through the REFLink's target
                    alt_view.bind(target);
                }
            }
        } else {
            // REF[X] → Y where X != Y: REFLink + nested conversion
            // Example: REF[TSD[str, TS[int]]] → TSD[str, REF[TS[int]]]
            //
            // The REFLink dereferences the outer REF to get the actual X
            // Then we recursively establish links for the X → Y conversion
            if (ref_link->valid()) {
                TSView linked_target = ref_link->target_view(MIN_DT);
                if (linked_target) {
                    // Now recursively establish links between alt_view and linked_target
                    // This handles the nested conversion (e.g., TS[int] → REF[TS[int]] for each element)
                    establish_links_recursive(alt, alt_view, linked_target, target_meta, deref_meta);
                }
            }
        }
        return;
    }

    // Case 3: TSValue → REF (wrapping)
    if (native_meta->kind == TSKind::TSValue && target_meta->kind == TSKind::REF) {
        // Store TSReference value constructed from native's path
        // The TSReference points to the native position
        ShortPath native_path = native_view.short_path();

        // Create a peered TSReference using factory method
        TSReference ref = TSReference::peered(native_path);

        // Get the value view from the alternative and set the TSReference
        // The alternative's value storage should be a TSReference type
        value::View alt_value = alt_view.value();
        if (alt_value) {
            // Copy the TSReference into the alternative's value storage
            // The value schema for REF types stores TSReference
            auto* ref_ptr = static_cast<TSReference*>(alt_value.data());
            if (ref_ptr) {
                *ref_ptr = std::move(ref);
            }
        }
        return;
    }

    // Case 4: Both are bundles - recurse into each field
    if (target_meta->kind == TSKind::TSB && native_meta->kind == TSKind::TSB) {
        size_t num_fields = target_meta->field_count;
        for (size_t i = 0; i < num_fields; ++i) {
            establish_links_recursive(
                alt,
                alt_view[i],
                native_view[i],
                target_meta->fields[i].ts_type,
                native_meta->fields[i].ts_type
            );
        }
        return;
    }

    // Case 5: Both are lists/dicts - recurse into elements
    if ((target_meta->kind == TSKind::TSL && native_meta->kind == TSKind::TSL) ||
        (target_meta->kind == TSKind::TSD && native_meta->kind == TSKind::TSD)) {
        // For collections, we need to:
        // 1. Subscribe to native structural changes
        // 2. For existing elements, recurse with element schemas

        const TSMeta* target_elem = target_meta->element_ts;
        const TSMeta* native_elem = native_meta->element_ts;

        // If element types are the same, bind the whole collection
        if (target_elem == native_elem) {
            alt_view.bind(native_view);
            return;
        }

        // Element types differ - need per-element conversion
        // Create a structural observer to keep alternative in sync with native

        auto observer = std::make_unique<AlternativeStructuralObserver>(
            this, &alt, native_meta, target_meta
        );

        // Subscribe the observer to native's structural changes
        subscribe_structural_observer(native_view, observer.get());

        // Store the observer to keep it alive
        structural_observers_.push_back(std::move(observer));

        // Subscribe a time propagator: when native container is modified,
        // copy its container time to the alternative so modified/valid work.
        {
            auto& cache = TSMetaSchemaCache::instance();
            auto* native_time_schema = cache.get_time_schema(native_meta);
            auto* alt_time_schema = cache.get_time_schema(target_meta);
            auto* native_obs_schema = cache.get_observer_schema(native_meta);

            if (native_time_schema && alt_time_schema && native_obs_schema) {
                const auto& native_vd = native_view.view_data();
                const auto& alt_vd = alt_view.view_data();

                if (native_vd.time_data && alt_vd.time_data && native_vd.observer_data) {
                    value::View native_time(native_vd.time_data, native_time_schema);
                    value::View alt_time(alt_vd.time_data, alt_time_schema);
                    value::View native_obs(native_vd.observer_data, native_obs_schema);

                    auto propagator = std::make_unique<AlternativeTimePropagator>();
                    propagator->native_container_time = &native_time.as_tuple().at(0).as<engine_time_t>();
                    propagator->alt_container_time = &alt_time.as_tuple().at(0).as<engine_time_t>();

                    // Subscribe to native container's ObserverList
                    auto* native_container_obs = static_cast<ObserverList*>(
                        native_obs.as_tuple().at(0).data());
                    if (native_container_obs) {
                        native_container_obs->add_observer(propagator.get());
                    }

                    time_propagators_.push_back(std::move(propagator));
                }
            }
        }

        // Process existing elements
        if (target_meta->kind == TSKind::TSD) {
            // For TSD, iterate over all slots using the iterator
            TSDView native_dict = native_view.as_dict();
            TSDView alt_dict = alt_view.as_dict();

            auto native_items = native_dict.items();
            for (auto it = native_items.begin(); it != native_items.end(); ++it) {
                value::View key_view = it.key();
                TSView native_elem_view = *it;

                // Create the corresponding element in alternative (alternative starts empty)
                TSView alt_elem_view = alt_dict.create(key_view);

                // Establish links for this element
                establish_links_recursive(
                    alt,
                    alt_elem_view,
                    native_elem_view,
                    target_elem,
                    native_elem
                );
            }
        } else {
            // For TSL, iterate by index
            TSLView native_list = native_view.as_list();
            TSLView alt_list = alt_view.as_list();

            for (size_t i = 0; i < native_list.size(); ++i) {
                TSView native_elem_view = native_list.at(i);
                TSView alt_elem_view = alt_list.at(i);

                establish_links_recursive(
                    alt,
                    alt_elem_view,
                    native_elem_view,
                    target_elem,
                    native_elem
                );
            }
        }
        return;
    }

    // Default: Direct bind (may not be type-correct, but handles unknown cases)
    alt_view.bind(native_view);
}

// ============================================================================
// TSOutputView Implementation
// ============================================================================

void TSOutputView::subscribe(Notifiable* observer) {
    if (!observer) return;

    // Get the observer list for this position
    value::View obs_view = ts_view_.observer();
    if (!obs_view) return;

    // For composite types (TSB, TSL, TSD), the observer schema is tuple[ObserverList, ...]
    // For scalar types, the observer schema is just ObserverList
    const TSMeta* meta = ts_view_.view_data().meta;
    ObserverList* obs_list = nullptr;

    if (meta && (meta->kind == TSKind::TSB || meta->kind == TSKind::TSL || meta->kind == TSKind::TSD)) {
        // Composite type: navigate to element 0 of the tuple
        obs_list = static_cast<ObserverList*>(obs_view.as_tuple().at(0).data());
    } else {
        // Scalar type: observer is directly ObserverList
        obs_list = static_cast<ObserverList*>(obs_view.data());
    }

    if (obs_list) {
        obs_list->add_observer(observer);
    }
}

void TSOutputView::unsubscribe(Notifiable* observer) {
    if (!observer) return;

    // Get the observer list for this position
    value::View obs_view = ts_view_.observer();
    if (!obs_view) return;

    // For composite types (TSB, TSL, TSD), the observer schema is tuple[ObserverList, ...]
    // For scalar types, the observer schema is just ObserverList
    const TSMeta* meta = ts_view_.view_data().meta;
    ObserverList* obs_list = nullptr;

    if (meta && (meta->kind == TSKind::TSB || meta->kind == TSKind::TSL || meta->kind == TSKind::TSD)) {
        // Composite type: navigate to element 0 of the tuple
        obs_list = static_cast<ObserverList*>(obs_view.as_tuple().at(0).data());
    } else {
        // Scalar type: observer is directly ObserverList
        obs_list = static_cast<ObserverList*>(obs_view.data());
    }

    if (obs_list) {
        obs_list->remove_observer(observer);
    }
}

TSOutputView TSOutputView::field(const std::string& name) const {
    TSView child = ts_view_.field(name);
    return TSOutputView(std::move(child), output_);
}

TSOutputView TSOutputView::operator[](size_t index) const {
    TSView child = ts_view_[index];
    return TSOutputView(std::move(child), output_);
}

TSOutputView TSOutputView::operator[](const value::View& key) const {
    TSView child = ts_view_.as_dict().at(key);
    return TSOutputView(std::move(child), output_);
}

FQPath TSOutputView::fq_path() const {
    if (!output_) {
        // No owner context - return empty FQPath
        return FQPath();
    }
    return output_->to_fq_path(ts_view_);
}

} // namespace hgraph
