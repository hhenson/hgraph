#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/time_series_state.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_input_builder.h>
#include <hgraph/types/value/type_registry.h>

#include <cassert>
#include <mutex>
#include <new>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>

namespace hgraph
{
    namespace
    {
        [[nodiscard]] size_t align_up(size_t value, size_t alignment) noexcept
        {
            const size_t mask = alignment - 1;
            return (value + mask) & ~mask;
        }

        template <typename T>
        void append_key_bytes(std::string &key, const T &value)
        {
            key.append(reinterpret_cast<const char *>(&value), sizeof(T));
        }

        void append_slot_key(std::string &key, const TSInputConstructionSlot &slot)
        {
            append_key_bytes(key, static_cast<uint8_t>(slot.kind()));
            const auto schema_bits = reinterpret_cast<std::uintptr_t>(slot.schema());
            append_key_bytes(key, schema_bits);

            switch (slot.kind()) {
                case TSInputSlotKind::Empty:
                    return;

                case TSInputSlotKind::NonPeeredCollection:
                    {
                        const auto &children = slot.children();
                        append_key_bytes(key, children.size());
                        for (const auto &child : children) {
                            append_slot_key(key, child);
                        }
                        return;
                    }

                case TSInputSlotKind::LinkTerminal:
                    {
                        const auto &binding = slot.binding();
                        append_key_bytes(key, binding.src_node);
                        append_key_bytes(key, binding.output_path.size());
                        for (const int64_t path_element : binding.output_path) {
                            append_key_bytes(key, path_element);
                        }
                        return;
                    }
            }
        }

        [[nodiscard]] std::string plan_cache_key(const TSInputConstructionPlan &construction_plan)
        {
            std::string key;
            key.reserve(128);
            append_slot_key(key, construction_plan.root());
            return key;
        }

        template <typename TState>
        void initialize_base_state(TState &state,
                                   TimeSeriesStateParentPtr parent,
                                   size_t index,
                                   engine_time_t modified_time = MIN_DT,
                                   TSStorageKind storage_kind = TSStorageKind::Native) noexcept
        {
            state.parent = parent;
            state.index = index;
            state.last_modified_time = modified_time;
            state.storage_kind = storage_kind;
            state.subscribers.clear();
        }

        template <typename TCollectionState>
        void initialize_collection_state(TCollectionState &state,
                                         TimeSeriesStateParentPtr parent,
                                         size_t index,
                                         engine_time_t modified_time = MIN_DT) noexcept
        {
            initialize_base_state(state, parent, index, modified_time);
            state.reset_child_states();
            state.modified_children.clear();
        }

        [[nodiscard]] TimeSeriesStateParentPtr root_parent() noexcept
        {
            return {};
        }

        [[nodiscard]] TimeSeriesStateParentPtr parent_ptr(TSBState &state) noexcept { return &state; }
        [[nodiscard]] TimeSeriesStateParentPtr parent_ptr(TSLState &state) noexcept { return &state; }

        [[nodiscard]] size_t child_slot_capacity(const TSMeta &schema)
        {
            switch (schema.kind) {
                case TSKind::TSB:
                    return schema.field_count();

                case TSKind::TSL:
                    if (schema.fixed_size() == 0) {
                        throw std::invalid_argument("TSInput construction plans currently require fixed-size TSL prefixes");
                    }
                    return schema.fixed_size();

                default:
                    throw std::invalid_argument("TSInput construction slots only support TSB and fixed-size TSL collections");
            }
        }

        [[nodiscard]] const TSMeta *child_schema_at(const TSMeta &schema, int64_t slot)
        {
            if (slot < 0) { throw std::out_of_range("TSInput construction plan paths must use non-negative slots"); }

            switch (schema.kind) {
                case TSKind::TSB:
                    if (static_cast<size_t>(slot) >= schema.field_count()) {
                        throw std::out_of_range("TSInput construction plan path is out of bounds for TSB");
                    }
                    {
                        const TSMeta *child_schema = schema.fields()[slot].ts_type;
                        assert(child_schema != nullptr);
                        return child_schema;
                    }

                case TSKind::TSL:
                    if (schema.fixed_size() == 0) {
                        throw std::invalid_argument("TSInput construction plan paths do not support dynamic TSL prefixes");
                    }
                    if (static_cast<size_t>(slot) >= schema.fixed_size()) {
                        throw std::out_of_range("TSInput construction plan path is out of bounds for TSL");
                    }
                    {
                        const TSMeta *child_schema = schema.element_ts();
                        assert(child_schema != nullptr);
                        return child_schema;
                    }

                default:
                    throw std::invalid_argument("TSInput construction plan paths only support TSB and fixed-size TSL prefixes");
            }
        }

        void ensure_non_peered_collection_slot(TSInputConstructionSlot &slot, const TSMeta *schema)
        {
            if (schema == nullptr) { throw std::invalid_argument("TSInput construction plan collection slots require a schema"); }

            if (schema->kind != TSKind::TSB && schema->kind != TSKind::TSL) {
                throw std::invalid_argument("TSInput construction plan prefixes must be TSB or fixed-size TSL");
            }

            if (slot.kind() == TSInputSlotKind::Empty) {
                slot = TSInputConstructionSlot::create_non_peered_collection(schema);
                return;
            }

            if (slot.kind() != TSInputSlotKind::NonPeeredCollection || slot.schema() != schema) {
                throw std::invalid_argument("TSInput construction plan contains conflicting collection requirements");
            }
        }

        void install_terminal_slot(TSInputConstructionSlot &slot, const TSMeta *schema, const TSInputBindingRef &binding)
        {
            if (schema == nullptr) { throw std::invalid_argument("TSInput link terminals require a schema"); }

            if (slot.kind() == TSInputSlotKind::Empty) {
                slot = TSInputConstructionSlot::create_link_terminal(schema, binding);
                return;
            }

            if (slot.kind() != TSInputSlotKind::LinkTerminal || slot.schema() != schema || !(slot.binding() == binding)) {
                throw std::invalid_argument("TSInput construction plan contains conflicting terminal requirements");
            }
        }

        [[nodiscard]] const value::TypeMeta *active_schema_from(const TSMeta *schema)
        {
            auto &registry = value::TypeRegistry::instance();
            const value::TypeMeta *bool_meta = value::scalar_type_meta<bool>();
            static std::unordered_map<const TSMeta *, const value::TypeMeta *> cache;
            static std::recursive_mutex cache_mutex;

            if (schema == nullptr) { return bool_meta; }

            std::lock_guard<std::recursive_mutex> lock(cache_mutex);
            if (const auto it = cache.find(schema); it != cache.end()) { return it->second; }

            const value::TypeMeta *active_schema = bool_meta;
            switch (schema->kind) {
                case TSKind::TSValue:
                case TSKind::TSS:
                case TSKind::TSW:
                case TSKind::REF:
                case TSKind::SIGNAL:
                    active_schema = bool_meta;
                    break;

                case TSKind::TSB:
                    {
                        auto builder = registry.tuple();
                        builder.add_element(bool_meta);
                        for (size_t i = 0; i < schema->field_count(); ++i) {
                            builder.add_element(active_schema_from(schema->fields()[i].ts_type));
                        }
                        active_schema = builder.build();
                        break;
                    }

                case TSKind::TSL:
                    {
                        const value::TypeMeta *child_active = active_schema_from(schema->element_ts());
                        const value::TypeMeta *child_collection = schema->fixed_size() > 0
                                                                      ? registry.fixed_list(child_active, schema->fixed_size()).build()
                                                                      : registry.list(child_active).build();
                        auto builder = registry.tuple();
                        builder.add_element(bool_meta);
                        builder.add_element(child_collection);
                        active_schema = builder.build();
                        break;
                    }

                case TSKind::TSD:
                    {
                        const value::TypeMeta *child_active = active_schema_from(schema->element_ts());
                        const value::TypeMeta *child_collection = registry.map(schema->key_type(), child_active).build();
                        auto builder = registry.tuple();
                        builder.add_element(bool_meta);
                        builder.add_element(child_collection);
                        active_schema = builder.build();
                        break;
                    }
            }

            cache.emplace(schema, active_schema);
            return active_schema;
        }

        struct InputStateNodeOps
        {
            virtual ~InputStateNodeOps() = default;

            virtual void construct(TimeSeriesStateV &state, TimeSeriesStateParentPtr parent, size_t index) const = 0;
            virtual void copy_construct(TimeSeriesStateV &dst,
                                        const TimeSeriesStateV &src,
                                        TimeSeriesStateParentPtr parent,
                                        size_t index) const = 0;
        };

        template <typename TCollectionState>
        void construct_collection_children(const std::vector<std::shared_ptr<const InputStateNodeOps>> &children, TCollectionState &state)
        {
            state.child_states.resize(children.size());
            for (size_t child_index = 0; child_index < children.size(); ++child_index) {
                const auto &child = children[child_index];
                if (!child) { continue; }

                auto child_state = std::make_unique<TimeSeriesStateV>();
                child->construct(*child_state, parent_ptr(state), child_index);
                state.child_states[child_index] = std::move(child_state);
            }
        }

        template <typename TCollectionState>
        void copy_collection_children(const std::vector<std::shared_ptr<const InputStateNodeOps>> &children,
                                      TCollectionState &dst_state,
                                      const TCollectionState &src_state)
        {
            dst_state.child_states.resize(children.size());
            for (size_t child_index = 0; child_index < children.size(); ++child_index) {
                const auto &child = children[child_index];
                if (!child) { continue; }

                auto child_state = std::make_unique<TimeSeriesStateV>();
                if (child_index < src_state.child_states.size() && src_state.child_states[child_index] != nullptr) {
                    child->copy_construct(*child_state, *src_state.child_states[child_index], parent_ptr(dst_state), child_index);
                } else {
                    child->construct(*child_state, parent_ptr(dst_state), child_index);
                }
                dst_state.child_states[child_index] = std::move(child_state);
            }
        }

        struct LinkTerminalNodeOps final : InputStateNodeOps
        {
            void construct(TimeSeriesStateV &state, TimeSeriesStateParentPtr parent, size_t index) const override
            {
                auto &link_state = state.emplace<TargetLinkState>();
                initialize_base_state(link_state, parent, index, MIN_DT, TSStorageKind::TargetLink);
                link_state.target.clear();
                link_state.scheduling_notifier.set_target(nullptr);
            }

            void copy_construct(TimeSeriesStateV &dst,
                                const TimeSeriesStateV &src,
                                TimeSeriesStateParentPtr parent,
                                size_t index) const override
            {
                const auto &src_state = std::get<TargetLinkState>(src);
                auto &dst_state = dst.emplace<TargetLinkState>();
                initialize_base_state(dst_state, parent, index, src_state.last_modified_time, TSStorageKind::TargetLink);
                dst_state.scheduling_notifier.set_target(src_state.scheduling_notifier.get_target());
                if (src_state.target.is_bound()) {
                    dst_state.set_target(src_state.target);
                } else {
                    dst_state.target.clear();
                }
            }
        };

        struct BundleNodeOps final : InputStateNodeOps
        {
            explicit BundleNodeOps(std::vector<std::shared_ptr<const InputStateNodeOps>> children)
                : m_children(std::move(children))
            {
            }

            void construct(TimeSeriesStateV &state, TimeSeriesStateParentPtr parent, size_t index) const override
            {
                auto &bundle_state = state.emplace<TSBState>();
                initialize_collection_state(bundle_state, parent, index);
                construct_collection_children(m_children, bundle_state);
            }

            void copy_construct(TimeSeriesStateV &dst,
                                const TimeSeriesStateV &src,
                                TimeSeriesStateParentPtr parent,
                                size_t index) const override
            {
                const auto &src_state = std::get<TSBState>(src);
                auto &dst_state = dst.emplace<TSBState>();
                initialize_collection_state(dst_state, parent, index, src_state.last_modified_time);
                dst_state.modified_children = src_state.modified_children;
                copy_collection_children(m_children, dst_state, src_state);
            }

          private:
            std::vector<std::shared_ptr<const InputStateNodeOps>> m_children;
        };

        struct ListNodeOps final : InputStateNodeOps
        {
            explicit ListNodeOps(std::vector<std::shared_ptr<const InputStateNodeOps>> children)
                : m_children(std::move(children))
            {
            }

            void construct(TimeSeriesStateV &state, TimeSeriesStateParentPtr parent, size_t index) const override
            {
                auto &list_state = state.emplace<TSLState>();
                initialize_collection_state(list_state, parent, index);
                construct_collection_children(m_children, list_state);
            }

            void copy_construct(TimeSeriesStateV &dst,
                                const TimeSeriesStateV &src,
                                TimeSeriesStateParentPtr parent,
                                size_t index) const override
            {
                const auto &src_state = std::get<TSLState>(src);
                auto &dst_state = dst.emplace<TSLState>();
                initialize_collection_state(dst_state, parent, index, src_state.last_modified_time);
                dst_state.modified_children = src_state.modified_children;
                copy_collection_children(m_children, dst_state, src_state);
            }

          private:
            std::vector<std::shared_ptr<const InputStateNodeOps>> m_children;
        };

        [[nodiscard]] std::shared_ptr<const InputStateNodeOps> compile_state_node(const TSInputConstructionSlot &slot)
        {
            switch (slot.kind()) {
                case TSInputSlotKind::Empty:
                    return {};

                case TSInputSlotKind::LinkTerminal:
                    return std::make_shared<LinkTerminalNodeOps>();

                case TSInputSlotKind::NonPeeredCollection:
                    {
                        const auto &slot_children = slot.children();
                        std::vector<std::shared_ptr<const InputStateNodeOps>> children(slot_children.size());
                        for (size_t index = 0; index < slot_children.size(); ++index) {
                            children[index] = compile_state_node(slot_children[index]);
                        }

                        switch (slot.schema() != nullptr ? slot.schema()->kind : TSKind::TSValue) {
                            case TSKind::TSB:
                                return std::make_shared<BundleNodeOps>(std::move(children));

                            case TSKind::TSL:
                                return std::make_shared<ListNodeOps>(std::move(children));

                            default:
                                throw std::invalid_argument("TSInput non-peered collection slots require TSB or TSL schemas");
                        }
                    }
            }

            throw std::invalid_argument("TSInput construction plan encountered an unsupported slot kind");
        }

        struct PlannedInputTSBuilderOps final : detail::TSBuilderOps
        {
            explicit PlannedInputTSBuilderOps(std::shared_ptr<const InputStateNodeOps> root_ops)
                : m_root_ops(std::move(root_ops))
            {
            }

            [[nodiscard]] detail::TSBuilderLayout layout(const TSMeta &schema,
                                                         const ValueBuilder &value_builder) const noexcept override
            {
                static_cast<void>(schema);

                const size_t value_offset = 0;
                const size_t value_size = value_builder.size();
                const size_t value_alignment = value_builder.alignment();
                const size_t ts_alignment = alignof(TimeSeriesStateV);
                const size_t ts_offset = align_up(value_size, ts_alignment);
                const size_t total_size = ts_offset + sizeof(TimeSeriesStateV);
                const size_t total_alignment = std::max(value_alignment, ts_alignment);
                return detail::TSBuilderLayout{
                    .value_offset = value_offset,
                    .ts_offset = ts_offset,
                    .size = total_size,
                    .alignment = total_alignment,
                };
            }

            void construct(void *memory, const TSMeta &schema) const override
            {
                static_cast<void>(schema);
                auto *state = new (memory) TimeSeriesStateV();
                m_root_ops->construct(*state, root_parent(), 0);
            }

            void destruct(void *memory) const noexcept override
            {
                std::destroy_at(static_cast<TimeSeriesStateV *>(memory));
            }

            void copy_construct(void *dst, const void *src, const TSMeta &schema) const override
            {
                static_cast<void>(schema);
                auto *dst_state = new (dst) TimeSeriesStateV();
                m_root_ops->copy_construct(*dst_state, *static_cast<const TimeSeriesStateV *>(src), root_parent(), 0);
            }

            void move_construct(void *dst, void *src, const TSMeta &schema) const override
            {
                copy_construct(dst, src, schema);
            }

          private:
            std::shared_ptr<const InputStateNodeOps> m_root_ops;
        };

        struct CompositeTSInputBuilderOps final : detail::TSInputBuilderOps
        {
            [[nodiscard]] detail::TSInputBuilderLayout layout(const TSValueBuilder &ts_value_builder,
                                                              const ValueBuilder &active_builder) const noexcept override
            {
                const size_t ts_value_offset = 0;
                const size_t active_offset = align_up(ts_value_builder.size(), active_builder.alignment());
                const size_t total_size = active_offset + active_builder.size();
                const size_t total_alignment = std::max(ts_value_builder.alignment(), active_builder.alignment());
                return detail::TSInputBuilderLayout{
                    .ts_value_offset = ts_value_offset,
                    .active_offset = active_offset,
                    .size = total_size,
                    .alignment = total_alignment,
                };
            }

            void construct(void *ts_value_memory,
                           void *active_memory,
                           const TSValueBuilder &ts_value_builder,
                           const ValueBuilder &active_builder) const override
            {
                ts_value_builder.construct(ts_value_memory);
                try {
                    active_builder.construct(active_memory);
                } catch (...) {
                    ts_value_builder.destruct(ts_value_memory);
                    throw;
                }
            }

            void destruct(void *ts_value_memory,
                          void *active_memory,
                          const TSValueBuilder &ts_value_builder,
                          const ValueBuilder &active_builder) const noexcept override
            {
                active_builder.destruct(active_memory);
                ts_value_builder.destruct(ts_value_memory);
            }

            void copy_construct(void *dst_ts_value_memory,
                                void *dst_active_memory,
                                const void *src_ts_value_memory,
                                const void *src_active_memory,
                                const TSValueBuilder &ts_value_builder,
                                const ValueBuilder &active_builder) const override
            {
                ts_value_builder.copy_construct(dst_ts_value_memory, src_ts_value_memory, ts_value_builder);
                try {
                    active_builder.copy_construct(dst_active_memory, src_active_memory, active_builder);
                } catch (...) {
                    ts_value_builder.destruct(dst_ts_value_memory);
                    throw;
                }
            }

            void move_construct(void *dst_ts_value_memory,
                                void *dst_active_memory,
                                void *src_ts_value_memory,
                                void *src_active_memory,
                                const TSValueBuilder &ts_value_builder,
                                const ValueBuilder &active_builder) const override
            {
                ts_value_builder.move_construct(dst_ts_value_memory, src_ts_value_memory, ts_value_builder);
                try {
                    active_builder.move_construct(dst_active_memory, src_active_memory, active_builder);
                } catch (...) {
                    ts_value_builder.destruct(dst_ts_value_memory);
                    throw;
                }
            }
        };

        [[nodiscard]] const detail::TSInputBuilderOps &composite_builder_ops() noexcept
        {
            static CompositeTSInputBuilderOps ops;
            return ops;
        }
    }  // namespace

    TSInputConstructionSlot TSInputConstructionSlot::create_non_peered_collection(const TSMeta *schema)
    {
        if (schema == nullptr || (schema->kind != TSKind::TSB && schema->kind != TSKind::TSL)) {
            throw std::invalid_argument("TSInputConstructionSlot::create_non_peered_collection requires a TSB or TSL schema");
        }

        TSInputConstructionSlot slot;
        slot.set_schema_and_kind(schema, TSInputSlotKind::NonPeeredCollection);
        slot.children().resize(child_slot_capacity(*schema));
        return slot;
    }

    TSInputConstructionSlot TSInputConstructionSlot::create_link_terminal(const TSMeta *schema, TSInputBindingRef binding) noexcept
    {
        TSInputConstructionSlot slot;
        slot.set_schema_and_kind(schema, TSInputSlotKind::LinkTerminal);
        slot.m_payload = std::move(binding);
        return slot;
    }

    const std::vector<TSInputConstructionSlot> &TSInputConstructionSlot::children() const
    {
        return std::get<std::vector<TSInputConstructionSlot>>(m_payload);
    }

    std::vector<TSInputConstructionSlot> &TSInputConstructionSlot::children()
    {
        if (!std::holds_alternative<std::vector<TSInputConstructionSlot>>(m_payload)) {
            m_payload.emplace<std::vector<TSInputConstructionSlot>>();
        }
        return std::get<std::vector<TSInputConstructionSlot>>(m_payload);
    }

    const TSInputBindingRef &TSInputConstructionSlot::binding() const
    {
        return std::get<TSInputBindingRef>(m_payload);
    }

    TSInputBindingRef &TSInputConstructionSlot::binding()
    {
        if (!std::holds_alternative<TSInputBindingRef>(m_payload)) {
            m_payload.emplace<TSInputBindingRef>();
        }
        return std::get<TSInputBindingRef>(m_payload);
    }

    TSInputConstructionPlan::TSInputConstructionPlan(const TSMeta *root_schema)
        : m_root(TSInputConstructionSlot::create_non_peered_collection(root_schema))
    {
    }

    TSInputConstructionPlan TSInputConstructionPlanCompiler::compile(const TSMeta &root_schema,
                                                                     const std::vector<TSInputConstructionEdge> &edges)
    {
        TSInputConstructionPlan plan{&root_schema};

        for (const auto &edge : edges) {
            if (edge.input_path.empty()) {
                throw std::invalid_argument("TSInput construction plan edges require a non-empty input path");
            }

            TSInputConstructionSlot *slot = &plan.root();
            const TSMeta *schema = &root_schema;

            for (size_t depth = 0; depth < edge.input_path.size(); ++depth) {
                const int64_t slot_index = edge.input_path[depth];
                const TSMeta *child_schema = child_schema_at(*schema, slot_index);

                if (static_cast<size_t>(slot_index) >= slot->children().size()) {
                    throw std::out_of_range("TSInput construction plan edge extends beyond the parent slot capacity");
                }

                TSInputConstructionSlot &child_slot = slot->children()[slot_index];
                const bool is_terminal = depth + 1 == edge.input_path.size();

                if (is_terminal) {
                    install_terminal_slot(child_slot, child_schema, edge.binding);
                } else {
                    ensure_non_peered_collection_slot(child_slot, child_schema);
                    slot = &child_slot;
                    schema = child_schema;
                }
            }
        }

        return plan;
    }

    TSInputBuilder::TSInputBuilder(const TSMeta &schema,
                                   const value::TypeMeta &active_schema,
                                   std::shared_ptr<const detail::TSBuilderOps> ts_state_builder_ops,
                                   const detail::TSInputBuilderOps &builder_ops) noexcept
        : m_schema(&schema),
          m_active_schema(&active_schema),
          m_ts_state_builder_ops(std::move(ts_state_builder_ops)),
          m_ts_value_builder(schema,
                            ValueBuilderFactory::checked_builder_for(schema.value_type, MutationTracking::Delta),
                            *m_ts_state_builder_ops,
                            TSValueBuilderFactory::checked_builder_for(schema).ts_dispatch()),
          m_active_builder(&ValueBuilderFactory::checked_builder_for(&active_schema)),
          m_builder_ops(builder_ops)
    {
        const auto layout = m_builder_ops.get().layout(m_ts_value_builder, active_builder());
        m_ts_value_offset = layout.ts_value_offset;
        m_active_offset = layout.active_offset;
        m_size = layout.size;
        m_alignment = layout.alignment;
    }

    void *TSInputBuilder::allocate() const
    {
        return ::operator new(m_size, std::align_val_t{m_alignment});
    }

    void TSInputBuilder::construct(void *memory) const
    {
        m_builder_ops.get().construct(ts_value_memory(memory), active_memory(memory), ts_value_builder(), active_builder());
    }

    void TSInputBuilder::destruct(void *memory) const noexcept
    {
        m_builder_ops.get().destruct(ts_value_memory(memory), active_memory(memory), ts_value_builder(), active_builder());
    }

    void TSInputBuilder::deallocate(void *memory) const noexcept
    {
        ::operator delete(memory, std::align_val_t{m_alignment});
    }

    void TSInputBuilder::copy_construct(void *dst, const void *src, const TSInputBuilder &src_builder) const
    {
        if (!compatible_with(src_builder)) {
            throw std::invalid_argument("TSInput copy construction requires matching builder");
        }

        m_builder_ops.get().copy_construct(ts_value_memory(dst), active_memory(dst), src_builder.ts_value_memory(src),
                                           src_builder.active_memory(src), ts_value_builder(), active_builder());
    }

    void TSInputBuilder::move_construct(void *dst, void *src, const TSInputBuilder &src_builder) const
    {
        if (!compatible_with(src_builder)) {
            throw std::invalid_argument("TSInput move construction requires matching builder");
        }

        m_builder_ops.get().move_construct(ts_value_memory(dst), active_memory(dst), src_builder.ts_value_memory(src),
                                           src_builder.active_memory(src), ts_value_builder(), active_builder());
    }

    void *TSInputBuilder::ts_value_memory(void *memory) const noexcept
    {
        return static_cast<std::byte *>(memory) + m_ts_value_offset;
    }

    const void *TSInputBuilder::ts_value_memory(const void *memory) const noexcept
    {
        return static_cast<const std::byte *>(memory) + m_ts_value_offset;
    }

    void *TSInputBuilder::active_memory(void *memory) const noexcept
    {
        return static_cast<std::byte *>(memory) + m_active_offset;
    }

    const void *TSInputBuilder::active_memory(const void *memory) const noexcept
    {
        return static_cast<const std::byte *>(memory) + m_active_offset;
    }

    bool TSInputBuilder::compatible_with(const TSInputBuilder &other) const noexcept
    {
        return m_schema == other.m_schema && m_active_schema == other.m_active_schema &&
               m_ts_state_builder_ops.get() == other.m_ts_state_builder_ops.get() && m_active_builder == other.m_active_builder;
    }

    TSInput TSInputBuilder::make_input() const
    {
        return TSInput{*this};
    }

    void TSInputBuilder::construct_input(TSInput &input, void *memory, MemoryOwnership ownership) const
    {
        assert(memory != nullptr);
        assert(input.m_builder == nullptr);
        assert(input.storage_memory() == nullptr);

        input.m_builder = this;
        input.rebind_builder(ts_value_builder(), TSValue::StorageOwnership::External);
        try {
            construct(memory);
            input.set_storage(memory, ownership);
            input.attach_storage(ts_value_memory(memory));
        } catch (...) {
            input.clear_storage_handle();
            input.m_builder = nullptr;
            input.detach_storage();
            input.reset_binding();
            throw;
        }
    }

    void TSInputBuilder::construct_input(TSInput &input) const
    {
        input.clear_storage();
        input.reset_binding();
        input.m_builder = nullptr;
        input.clear_storage_handle();

        void *memory = allocate();
        try {
            construct_input(input, memory, MemoryOwnership::Owned);
        } catch (...) {
            deallocate(memory);
            throw;
        }
    }

    void TSInputBuilder::copy_construct_input(TSInput &input, const TSInput &other, void *memory, MemoryOwnership ownership) const
    {
        if (other.m_builder != this || !compatible_with(other.builder())) {
            throw std::invalid_argument("TSInputBuilder::copy_construct_input requires matching builder");
        }

        assert(memory != nullptr);
        assert(input.m_builder == nullptr);
        assert(input.storage_memory() == nullptr);

        input.m_builder = this;
        input.rebind_builder(ts_value_builder(), TSValue::StorageOwnership::External);
        try {
            copy_construct(memory, other.storage_memory(), other.builder());
            input.set_storage(memory, ownership);
            input.attach_storage(ts_value_memory(memory));
        } catch (...) {
            input.clear_storage_handle();
            input.m_builder = nullptr;
            input.detach_storage();
            input.reset_binding();
            throw;
        }
    }

    void TSInputBuilder::copy_construct_input(TSInput &input, const TSInput &other) const
    {
        input.clear_storage();
        input.reset_binding();
        input.m_builder = nullptr;
        input.clear_storage_handle();

        void *memory = allocate();
        try {
            copy_construct_input(input, other, memory, MemoryOwnership::Owned);
        } catch (...) {
            deallocate(memory);
            throw;
        }
    }

    void TSInputBuilder::move_construct_input(TSInput &input, TSInput &other) const
    {
        if (other.m_builder != this || !compatible_with(other.builder())) {
            throw std::invalid_argument("TSInputBuilder::move_construct_input requires matching builder");
        }

        input.clear_storage();
        input.reset_binding();
        input.m_builder = this;
        input.rebind_builder(ts_value_builder(), TSValue::StorageOwnership::External);
        input.m_storage = other.m_storage;
        if (input.storage_memory() != nullptr) { input.attach_storage(ts_value_memory(input.storage_memory())); }

        other.m_builder = nullptr;
        other.clear_storage_handle();
        other.detach_storage();
        other.reset_binding();
    }

    void TSInputBuilder::destruct_input(TSInput &input) const noexcept
    {
        if (input.m_builder != this || input.storage_memory() == nullptr) { return; }
        void *memory = input.storage_memory();
        const bool owns_storage = input.owns_storage();
        destruct(memory);
        if (owns_storage) { deallocate(memory); }
        input.clear_storage_handle();
        input.m_builder = nullptr;
        input.detach_storage();
        input.reset_binding();
    }

    const TSInputBuilder *TSInputBuilderFactory::builder_for(const TSInputConstructionPlan &construction_plan)
    {
        const TSInputConstructionSlot &root = construction_plan.root();
        if (root.kind() != TSInputSlotKind::NonPeeredCollection || root.schema() == nullptr || root.schema()->kind != TSKind::TSB) {
            return nullptr;
        }

        static std::unordered_map<std::string, std::unique_ptr<TSInputBuilder>> cache;
        static std::mutex mutex;

        const std::string key = plan_cache_key(construction_plan);

        std::lock_guard lock(mutex);
        if (const auto it = cache.find(key); it != cache.end()) { return it->second.get(); }

        const auto root_ops = compile_state_node(root);
        const auto active_schema = active_schema_from(root.schema());
        auto builder = std::unique_ptr<TSInputBuilder>(new TSInputBuilder(
            construction_plan.schema(),
            *active_schema,
            std::make_shared<PlannedInputTSBuilderOps>(root_ops),
            composite_builder_ops()));

        const TSInputBuilder *builder_ptr = builder.get();
        cache.emplace(key, std::move(builder));
        return builder_ptr;
    }

    const TSInputBuilder &TSInputBuilderFactory::checked_builder_for(const TSInputConstructionPlan &construction_plan)
    {
        if (const auto *builder = builder_for(construction_plan); builder != nullptr) { return *builder; }
        throw std::invalid_argument("TSInputBuilderFactory requires a TSB-root construction plan");
    }
}  // namespace hgraph
