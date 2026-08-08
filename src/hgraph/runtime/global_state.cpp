#include <hgraph/runtime/global_state.h>

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/value/any_ops.h>
#include <hgraph/types/value/mutable_container_ops.h>

#include <string>

namespace hgraph
{
    namespace
    {
        thread_local GlobalContext *active_global_context = nullptr;

        // Canonical binding for the GlobalState backing: a mutable Map<string, Any>.
        ValueTypeRef global_state_binding()
        {
            auto       &registry = TypeRegistry::instance();
            const auto *str_meta = registry.register_scalar<std::string>("str");
            const auto *any_meta = registry.any();
            const auto *schema   = registry.mutable_map(str_meta, any_meta);
            const auto binding  = ValuePlanFactory::instance().type_for(schema);
            if (!binding) { throw std::logic_error("GlobalState: no binding for Map<string, Any>"); }
            return binding;
        }
    }  // namespace

    GlobalState::GlobalState() : map_{global_state_binding()} {}

    std::size_t GlobalStateView::size() const { return map_->as_map().size(); }

    bool GlobalStateView::contains(std::string_view key) const
    {
        const Value key_value{std::string{key}};
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
        const Value key_value{std::string{key}};
        // Get (creating an empty Any if needed) the value slot and assign the
        // boxed value in place — a single copy of ``value``, no temporary Any.
        map_->as_map().begin_mutation().value(key_value.view()).as_mutable_any().set(value);
    }

    void GlobalStateView::set(std::string_view key, const Value &value) const { set(key, value.view()); }

    void GlobalStateView::set(std::string_view key, Value &&value) const
    {
        const Value key_value{std::string{key}};
        map_->as_map().begin_mutation().value(key_value.view()).as_mutable_any().set(std::move(value));
    }

    bool GlobalStateView::erase(std::string_view key) const
    {
        const Value key_value{std::string{key}};
        return map_->as_map().begin_mutation().remove(key_value.view());
    }

    void GlobalStateView::copy_from(const GlobalStateView &other) const
    {
        *map_ = other.as_value();
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
        if (active_global_context == this) { active_global_context = nullptr; }
    }

    void GlobalContext::activate()
    {
        if (active_global_context != nullptr)
        {
            throw std::logic_error("GlobalContext does not support nested activation");
        }
        active_global_context = this;
    }

    GlobalState *GlobalContext::active_state() noexcept
    {
        return active_global_context != nullptr ? &active_global_context->state() : nullptr;
    }
}  // namespace hgraph
