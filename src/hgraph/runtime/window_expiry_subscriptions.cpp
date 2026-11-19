#include "window_expiry_subscriptions.h"

#include "../types/time_series/ts_data/ownership.h"

#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/util/scope.h>

#include <hgraph/types/utils/stable_slot_store.h>

#include <algorithm>

namespace hgraph
{
    namespace
    {
        bool plan_branch(WindowExpiryBranch &branch, const TSValueTypeMetaData &schema)
        {
            if (schema.is_duration_based()) { return true; }
            if (schema.kind == TSTypeKind::TSB)
            {
                for (std::size_t field = 0; field < schema.field_count(); ++field)
                {
                    WindowExpiryBranch child{field, {}};
                    if (plan_branch(child, *schema.fields()[field].type))
                        branch.children.push_back(std::move(child));
                }
            }
            else if (schema.kind == TSTypeKind::TSD || (schema.kind == TSTypeKind::TSL && schema.fixed_size() != 0))
            {
                WindowExpiryBranch element;
                if (plan_branch(element, *schema.element_ts())) { branch.children.push_back(std::move(element)); }
            }
            return !branch.children.empty();
        }

        struct WindowSlotSubscriptions
        {
            WindowSlotSubscriptions() = default;
            WindowSlotSubscriptions(const WindowSlotSubscriptions &) = delete;
            WindowSlotSubscriptions &operator=(const WindowSlotSubscriptions &) = delete;

            void add_window(const GraphView &root, const TSOutputView &window, bool newly_inserted)
            {
                windows.emplace_front(root, window, newly_inserted);
            }

            std::forward_list<WindowExpirySubscription> windows;
            std::forward_list<WindowCollectionSubscription> collections;
        };

        WindowExpirySubscriptions &group_for(std::unique_ptr<WindowExpirySubscriptions> &group)
        {
            if (!group) { group = std::make_unique<WindowExpirySubscriptions>(); }
            return *group;
        }

        WindowSlotSubscriptions &group_for(WindowSlotSubscriptions &group) { return group; }

        template<typename Group>
        void attach_branch(const GraphView &root, const TSOutputView &output, const WindowExpiryBranch &branch,
                           Group &group, bool newly_inserted);

        struct WindowSlotOps
        {
            const MemoryUtils::StoragePlan &plan;
            void (*construct)(void *, const GraphView &, const TSOutputView &, const WindowExpiryBranch &, bool);
        };

        const WindowSlotOps &window_slot_ops(const WindowExpiryBranch &element)
        {
            static const WindowSlotOps leaf{
                MemoryUtils::plan_for<WindowExpirySubscription>(),
                [](void *memory, const GraphView &root, const TSOutputView &child, const WindowExpiryBranch &, bool inserted) {
                    std::construct_at(static_cast<WindowExpirySubscription *>(memory), root, child, inserted);
                }};
            static const WindowSlotOps subtree{
                MemoryUtils::plan_for<WindowSlotSubscriptions>(),
                [](void *memory, const GraphView &root, const TSOutputView &child, const WindowExpiryBranch &branch, bool inserted) {
                    auto *group = std::construct_at(static_cast<WindowSlotSubscriptions *>(memory));
                    auto rollback = UnwindCleanupGuard([&] { std::destroy_at(group); });
                    attach_branch(root, child, branch, *group, inserted);
                    rollback.release();
                }};
            return element.children.empty() ? leaf : subtree;
        }

        /** Parallel state for the parent's slot IDs. Registrations stay put
            across growth. The last logical removal releases the bulk storage. */
        class WindowSlots
        {
          public:
            explicit WindowSlots(const WindowExpiryBranch &element) : ops_(window_slot_ops(element)) {}
            ~WindowSlots() { clear(); }
            WindowSlots(const WindowSlots &) = delete;
            WindowSlots &operator=(const WindowSlots &) = delete;

            void attach(std::size_t slot, const GraphView &root, const TSOutputView &child,
                        const WindowExpiryBranch &branch, bool inserted)
            {
                if (!slots_.bound()) { slots_.bind_layout(ops_.plan.layout); }
                if (slot >= slots_.slot_capacity())
                    slots_.reserve_to(std::max(slot + 1, 2 * slots_.slot_capacity()));
                if (slots_.constructed(slot)) { return; }
                ops_.construct(slots_.slot_memory(slot), root, child, branch, inserted);
                slots_.mark_constructed(slot);
            }

            void remove(std::size_t slot) noexcept
            {
                if (auto *entry = slots_.live_slot_memory(slot))
                {
                    ops_.plan.destroy(entry);
                    slots_.mark_free(slot);
                    if (slots_.constructed_count() == 0) { slots_ = {}; }
                }
            }

            void clear() noexcept
            {
                for (std::size_t slot = 0; slot < slots_.slot_capacity(); ++slot)
                    if (void *entry = slots_.live_slot_memory(slot)) { ops_.plan.destroy(entry); }
                slots_ = {};
            }

          private:
            const WindowSlotOps &ops_;
            StableSlotStore<StableSlotStateModel::ConstructedOnly> slots_;
        };
    }

    class WindowCollectionSubscription final : public SlotObserver
    {
      public:
        WindowCollectionSubscription(const GraphView &root, const TSOutputView &output,
                                     const WindowExpiryBranch &element, bool newly_inserted)
            : root_(root.pointer()), output_(output.handle()), element_(element),
              dictionary_(output.schema()->kind == TSTypeKind::TSD), slots_(element)
        {
            if (dictionary_)
            {
                auto dict = output.as_dict();
                // The initial walk is only for children already present at
                // attachment (for example in a recovered graph).
                if (!dict.empty())
                    for (std::size_t slot = 0; slot < dict.slot_capacity(); ++slot)
                        if (dict.slot_live(slot)) { attach_slot(slot, newly_inserted); }
                dict.data_view().key_set().subscribe_slot_observer(this);
            }
            else
            {
                const auto size = output.data_view().indexed_child_count();
                for (std::size_t index = 0; index < size; ++index)
                    attach_slot(index, newly_inserted);
                detail::subscribe_dynamic_list_slots(output.data_view(), this);
            }
        }

        ~WindowCollectionSubscription() override
        {
            auto data = output_.data_view();
            if (dictionary_) { data.as_dict().key_set().unsubscribe_slot_observer(this); }
            else { detail::unsubscribe_dynamic_list_slots(data, this); }
        }

        void on_insert(std::size_t slot) override { attach_slot(slot, true); }
        void on_remove(std::size_t slot) override { slots_.remove(slot); }
        void on_erase(std::size_t) override {}  // detached on logical removal
        void on_capacity(std::size_t, std::size_t) override {}  // child storage is address-stable
        void on_clear() override { slots_.clear(); }

      private:
        void attach_slot(std::size_t slot, bool newly_inserted)
        {
            const auto view = output_.view(MIN_DT);
            const auto child = dictionary_ ? view.as_dict().at_slot(slot) : view.indexed_child_at(slot);
            if (child.forwarding() || !child.data_view().ops().allows_mutation) { return; }
            slots_.attach(slot, GraphView{root_}, child, element_, newly_inserted);
        }

        GraphPtr root_;
        TSOutputHandle output_;
        const WindowExpiryBranch &element_;
        bool dictionary_;
        WindowSlots slots_;
    };

    WindowExpirySubscriptions::WindowExpirySubscriptions() = default;
    WindowExpirySubscriptions::~WindowExpirySubscriptions() = default;

    void WindowExpirySubscriptions::add_window(const GraphView &root, const TSOutputView &window, bool newly_inserted)
    {
        if (!windows) { windows.emplace(); }
        windows->emplace_back(root, window, newly_inserted);
    }

    namespace
    {
        template<typename Group>
        void attach_branch(const GraphView &root, const TSOutputView &output, const WindowExpiryBranch &branch,
                           Group &group, bool newly_inserted)
        {
            // REF and proxy projections borrow their owner's event timing.
            if (output.forwarding() || !output.data_view().ops().allows_mutation) { return; }
            const auto &schema = *output.schema();
            if (schema.kind == TSTypeKind::TSW)
            {
                group_for(group).add_window(root, output, newly_inserted);
            }
            else if (schema.kind == TSTypeKind::TSD || schema.is_unbounded_tsl())
            {
                group_for(group).collections.emplace_front(root, output, branch.children.front(), newly_inserted);
            }
            else if (schema.kind == TSTypeKind::TSL)
            {
                for (std::size_t i = 0; i < schema.fixed_size(); ++i)
                    attach_branch(root, output.indexed_child_at(i), branch.children.front(), group, newly_inserted);
            }
            else
            {
                for (const auto &field : branch.children)
                    attach_branch(root, output.indexed_child_at(field.field), field, group, newly_inserted);
            }
        }
    }

    void plan_window_expiry(std::vector<WindowExpiryPath> &paths, const NodeBuilder &node, std::size_t index)
    {
        const auto &schema = *node.type().schema();
        auto plan = [&](GraphEdgeSourceKind source, const TSValueTypeMetaData *endpoint) {
            WindowExpiryPath path{index, source, {}};
            if (endpoint && plan_branch(path.branch, *endpoint)) { paths.push_back(std::move(path)); }
        };
        plan(GraphEdgeSourceKind::Output, schema.output_schema);
        plan(GraphEdgeSourceKind::ErrorOutput, schema.error_output_schema);
        plan(GraphEdgeSourceKind::RecordableState, schema.recordable_state_schema);
    }

    std::unique_ptr<WindowExpirySubscriptions> attach_window_expiry(
        const GraphView &graph, const std::vector<WindowExpiryPath> &paths, DateTime now)
    {
        std::unique_ptr<WindowExpirySubscriptions> subscriptions;
        if (paths.empty()) { return subscriptions; }
        const auto root = graph.root();
        for (const auto &path : paths)
        {
            auto node = graph.node_at(path.node);
            switch (path.source)
            {
                case GraphEdgeSourceKind::Output:
                    if (!node_replays_output_events(node))
                        attach_branch(root, node.output(now), path.branch, subscriptions, false);
                    break;
                case GraphEdgeSourceKind::ErrorOutput:
                    attach_branch(root, node.error_output(now), path.branch, subscriptions, false);
                    break;
                case GraphEdgeSourceKind::RecordableState:
                    attach_branch(root, node.recordable_state(now), path.branch, subscriptions, false);
                    break;
            }
        }
        return subscriptions;
    }

    WindowExpirySubscription::WindowExpirySubscription(const GraphView &graph, const TSOutputView &window,
                                                       bool newly_inserted)
        : output_(window.handle()),
          alarm_(std::in_place, graph, this,
                 [](void *self, DateTime now) { static_cast<WindowExpirySubscription *>(self)->expire(now); },
                 [](void *self) { static_cast<WindowExpirySubscription *>(self)->refresh(); })
    {
        refresh();
        // A cold restore may populate this child silently after insertion.
        if (newly_inserted) { alarm_->request_refresh(); }
        window.subscribe(this);
    }

    WindowExpirySubscription::~WindowExpirySubscription()
    {
        if (output_.bound()) { output_.data_view().unsubscribe(this); }
    }

    void WindowExpirySubscription::source_invalidated(const TSDataTracking *) noexcept
    {
        output_.reset();
        alarm_.reset();
    }

    void WindowExpirySubscription::refresh()
    {
        auto data = output_.data_view();
        alarm_->set_deadline(data.as_window().next_expiry_time());
    }

    void WindowExpirySubscription::notify(DateTime)
    {
        // The observer fires only on the first modification at a given time.
        // A later clear or append in this same cycle may change the deadline.
        refresh();
        alarm_->request_refresh();
    }

    void WindowExpirySubscription::expire(DateTime now)
    {
        auto data = output_.data_view();
        auto window = data.as_window();
        window.begin_mutation(now).expire();
    }
}
