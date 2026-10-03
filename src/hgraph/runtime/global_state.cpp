#include <hgraph/runtime/global_state.h>

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/value/any_ops.h>
#include <hgraph/types/value/mutable_container_ops.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <string>

namespace hgraph
{
    namespace
    {
        // The active C++ authoring context: one process-wide slot, never a
        // thread-local (the build is single-threaded; bridges hand the state
        // to the Wiring directly and never touch this).
        GlobalContext *active_global_context = nullptr;

        // Canonical binding for the GlobalState backing: a mutable Map<string,
        // Any>. Generation-checked cache shared by every thread (no
        // thread-local: ruling 2026-09-05): readers take one acquire load and
        // stay off the registry mutexes; a stale generation (a test-only
        // registry reset) resolves again under the registry and publishes a
        // fresh immutable record. The record a reset retires is left to the
        // process rather than freed under a concurrent reader -- a few bytes
        // per test-only reset.
        struct CachedBinding
        {
            std::uint64_t generation{0};
            ValueTypeRef  binding{};
        };

        ValueTypeRef global_state_binding()
        {
            static std::atomic<const CachedBinding *> cache{nullptr};

            auto               &registry   = TypeRegistry::instance();
            const std::uint64_t generation = registry.reset_generation();
            if (const CachedBinding *cached = cache.load(std::memory_order_acquire);
                cached != nullptr && cached->generation == generation && cached->binding.bound())
            {
                return cached->binding;
            }

            const auto *str_meta = registry.register_scalar<std::string>("str");
            const auto *any_meta = registry.any();
            const auto *schema   = registry.mutable_map(str_meta, any_meta);
            auto binding = ValuePlanFactory::instance().type_for(schema);
            if (!binding) { throw std::logic_error("GlobalState: no binding for Map<string, Any>"); }
            auto *fresh = new CachedBinding{generation, binding};
            const CachedBinding *expected = cache.load(std::memory_order_acquire);
            while (!cache.compare_exchange_weak(expected, fresh, std::memory_order_acq_rel, std::memory_order_acquire))
            {
                if (expected != nullptr && expected->generation == generation)
                {
                    delete fresh;   // a concurrent refresh published the same interned binding
                    return expected->binding;
                }
            }
            return binding;
        }
    }  // namespace

    GlobalState::GlobalState() : map_{global_state_binding()} {}

    PreparedGlobalEntry GlobalStateView::prepare(std::string_view key, ValueTypeRef binding) const
    {
        if (prepared_ == nullptr || absent_ == nullptr || !binding) {
            throw std::logic_error("global-state preparation requires an owning store and a concrete binding");
        }
        const std::string name{key};
        const auto found = prepared_->find(name);
        if (found != prepared_->end() && found->second.schema() != binding.schema()) {
            throw std::invalid_argument("global-state type conflict for '" + name + "'");
        }
        if (found != prepared_->end() && found->second != binding) {
            throw std::invalid_argument("global-state storage binding conflict for '" + name + "'");
        }
        const auto seed = get(key);
        if (seed.has_value() && seed.schema() != binding.schema()) {
            throw std::invalid_argument("global-state seed type conflict for '" + name + "'");
        }
        // The map's ValueSlotStore gives Any cells non-moving storage. The Any
        // representation is one Value owner; retaining its cell avoids both
        // keyed lookup and type dispatch during node hooks.
        const Value key_value{name};
        auto box = map_->as_map().begin_mutation().value(key_value.view()).as_mutable_any();
        auto *cell = static_cast<Value *>(box.mutable_data());
        if (found == prepared_->end()) {
            Value retained = seed.has_value() ? Value{binding, seed} : Value::typed_null(binding);
            prepared_->emplace(name, binding);
            *cell = std::move(retained);
            if (!cell->has_value()) { ++*absent_; }
        }
        return PreparedGlobalEntry{*cell, binding, name, *absent_};
    }

    bool GlobalStateView::is_prepared(std::string_view key) const
    {
        return prepared_ != nullptr && prepared_->contains(std::string{key});
    }

    ValueView PreparedGlobalEntry::get() const
    {
        if (value_ == nullptr || !value_->has_value()) {
            throw std::runtime_error("missing global-state value for '" + key_ + "'");
        }
        return value_->view();
    }

    void PreparedGlobalEntry::set(const ValueView &value) const
    {
        if (value_ == nullptr) { throw std::logic_error("unprepared global-state entry"); }
        Value retained{binding_, value};
        const bool was_absent = !value_->has_value();
        *value_ = std::move(retained);
        if (was_absent) { --*absent_; }
    }

    std::size_t GlobalStateView::size() const
    {
        return map_->as_map().size() - (absent_ != nullptr ? *absent_ : 0);
    }

    bool GlobalStateView::contains(std::string_view key) const
    {
        const Value key_value{std::string{key}};
        if (prepared_ != nullptr && prepared_->contains(std::string{key})) { return get(key).has_value(); }
        return map_->as_map().contains(key_value.view());
    }

    ValueView GlobalStateView::get(std::string_view key) const
    {
        const Value key_value{std::string{key}};
        if (!map_->as_map().contains(key_value.view())) { return ValueView{}; }
        // The GlobalState is by definition a mutable store, so a read honours the
        // stored value's own mutability: a value boxed as mutable (e.g. a mutable
        // List/Map) comes back as a writable view that can be mutated in place; an
        // immutable value comes back read-only (its ops refuse begin_mutation).
        // Routed through the mutable map accessor; the key is present, so no entry
        // is created.
        return map_->as_map().begin_mutation().value(key_value.view()).as_any().get();
    }

    void GlobalStateView::set(std::string_view key, const ValueView &value) const
    {
        if (prepared_ != nullptr) {
            const auto bound = prepared_->find(std::string{key});
            if (bound != prepared_->end()) {
                if (bound->second.schema() != value.schema()) {
                    throw std::invalid_argument("global-state type conflict for '" + std::string{key} + "'");
                }
                prepare(key, bound->second).set(value);
                return;
            }
        }
        const Value key_value{std::string{key}};
        // Get (creating an empty Any if needed) the value slot and assign the
        // boxed value in place — a single copy of ``value``, no temporary Any.
        map_->as_map().begin_mutation().value(key_value.view()).as_mutable_any().set(value);
    }

    void GlobalStateView::set(std::string_view key, const Value &value) const { set(key, value.view()); }

    void GlobalStateView::set(std::string_view key, Value &&value) const
    {
        if (prepared_ != nullptr && prepared_->contains(std::string{key})) {
            set(key, value.view());
            return;
        }
        const Value key_value{std::string{key}};
        map_->as_map().begin_mutation().value(key_value.view()).as_mutable_any().set(std::move(value));
    }

    bool GlobalStateView::erase(std::string_view key) const
    {
        if (prepared_ != nullptr && prepared_->contains(std::string{key})) {
            throw std::logic_error("cannot erase prepared global-state entry '" + std::string{key} + "'");
        }
        const Value key_value{std::string{key}};
        return map_->as_map().begin_mutation().remove(key_value.view());
    }

    void GlobalStateView::copy_from(const GlobalStateView &other) const
    {
        if (prepared_ != nullptr && !prepared_->empty()) {
            throw std::logic_error("cannot replace prepared global-state storage");
        }
        Value copied{other.as_value()};
        if (other.prepared_ != nullptr) {
            for (const auto &[key, binding] : *other.prepared_) {
                static_cast<void>(binding);
                if (!other.get(key).has_value()) {
                    const Value key_value{key};
                    copied.as_map().begin_mutation().remove(key_value.view());
                }
            }
        }
        *map_ = std::move(copied);
        if (absent_ != nullptr) { *absent_ = 0; }
    }

    GlobalContext::GlobalContext()
        : state_(&owned_state_)
    {
        activate();
    }

    GlobalContext::GlobalContext(GlobalState &state)
        : state_(&state)
    {
        activate();
    }

    GlobalContext::~GlobalContext()
    {
        // Detach the binding handed to every wiring, child and prepared
        // execution seeded from this context: none may outlive the state.
        if (seed_) { seed_->detach(); }
        if (active_global_context == this) { active_global_context = nullptr; }
    }

    void GlobalContext::activate()
    {
        if (active_global_context != nullptr)
        {
            throw std::logic_error("GlobalContext does not support nested activation");
        }
        seed_                 = std::make_shared<GlobalSeedBinding>(state_);
        active_global_context = this;
    }

    GlobalContext *GlobalContext::active() noexcept { return active_global_context; }

    GlobalState *GlobalContext::active_state() noexcept
    {
        return active_global_context != nullptr ? &active_global_context->state() : nullptr;
    }


}  // namespace hgraph
