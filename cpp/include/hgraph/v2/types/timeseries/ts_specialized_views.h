#ifndef HGRAPH_CPP_ROOT_TS_SPECIALIZED_VIEWS_H
#define HGRAPH_CPP_ROOT_TS_SPECIALIZED_VIEWS_H

#ifndef HGRAPH_CPP_ROOT_TS_VIEW_H
    #error "ts_specialized_views.h requires ts_view.h to be included first"
#endif

#include <string_view>

namespace hgraph::v2
{
    namespace detail
    {
        [[nodiscard]] inline const TsValueTypeBinding &checked_ts_child_binding(const TSValueTypeMetaData *type) {
            return TsValueBuilder::checked(type).checked_binding();
        }

        [[nodiscard]] inline const TsBundleOps &checked_bundle_ops(const TsValueOps *ops) {
            if (ops != nullptr && ops->is_tsb()) { return ops->checked_bundle_ops(); }
            throw std::logic_error("TsbView requires bundle operations");
        }

        [[nodiscard]] inline const TsListOps &checked_list_ops(const TsValueOps *ops) {
            if (ops != nullptr && ops->is_tsl()) { return ops->checked_list_ops(); }
            throw std::logic_error("TslView requires list operations");
        }

        [[nodiscard]] inline const TsSetOps &checked_set_ops(const TsValueOps *ops) {
            if (ops != nullptr && ops->is_tss()) { return ops->checked_set_ops(); }
            throw std::logic_error("TssView requires set operations");
        }

        [[nodiscard]] inline const TsDictOps &checked_dict_ops(const TsValueOps *ops) {
            if (ops != nullptr && ops->is_tsd()) { return ops->checked_dict_ops(); }
            throw std::logic_error("TsdView requires dict operations");
        }

        [[nodiscard]] inline const TsWindowOps &checked_window_ops(const TsValueOps *ops) {
            if (ops != nullptr && ops->is_tsw()) { return ops->checked_window_ops(); }
            throw std::logic_error("TswView requires window operations");
        }

        [[nodiscard]] inline size_t checked_kind_state_offset(const MemoryUtils::StoragePlan &plan) {
            return ts_state_layout(plan).kind_offset;
        }

        [[nodiscard]] inline TsView child_ts_view(const TsValueTypeBinding &binding, void *state_data, void *value_data,
                                                  engine_time_t evaluation_time) {
            return TsView{TsViewContext{ts_state_view(&binding, state_data), value_data, evaluation_time}};
        }
    }  // namespace detail

    template <typename Binding>
        requires requires(const Binding &binding) {
            { binding.checked_ops().checked_value_binding() } -> std::same_as<const ValueTypeBinding &>;
        }
    struct TsbView : BasicTsView<Binding>
    {
        using base_type = BasicTsView<Binding>;
        using base_type::base_type;

        explicit TsbView(base_type view)
            : base_type(std::move(view)), m_bundle_ops(&detail::checked_bundle_ops(this->ops())),
              m_kind_state_offset(this->state_plan() != nullptr ? detail::checked_kind_state_offset(*this->state_plan())
                                                                : TsStatePlanLayout::npos) {}

        [[nodiscard]] size_t field_count() const {
            if (!this->has_value()) { throw std::logic_error("TsbView::field_count() on an empty view"); }
            return m_bundle_ops->field_count();
        }

        [[nodiscard]] bool has_field(std::string_view name) const noexcept { return m_bundle_ops->field(name) != nullptr; }

        [[nodiscard]] TsView at(size_t index) const {
            if (!this->has_value()) { throw std::logic_error("TsbView::at() on an empty view"); }
            const TsBundleFieldOps &field = m_bundle_ops->checked_field(index);

            BundleView      bundle      = this->value().as_bundle();
            const ValueView child_value = bundle.IndexedValueView::at(index);
            void           *child_state = nullptr;
            if (this->state_data() != nullptr && m_kind_state_offset != TsStatePlanLayout::npos) {
                child_state = MemoryUtils::advance(this->state_data(), m_kind_state_offset + field.state_offset);
            }
            return detail::child_ts_view(field.checked_binding(), child_state, const_cast<void *>(child_value.data()),
                                         this->evaluation_time());
        }

        [[nodiscard]] TsView at(std::string_view name) const {
            if (!this->has_value()) { throw std::logic_error("TsbView::at() on an empty view"); }

            if (const auto *field = m_bundle_ops->field(name); field != nullptr) { return at(field->index); }
            throw std::out_of_range("TsbView field not found");
        }

        [[nodiscard]] TsView operator[](size_t index) const { return at(index); }
        [[nodiscard]] TsView operator[](std::string_view name) const { return at(name); }

        void set(size_t index, const ValueView &value_view) {
            BundleView bundle = this->value().as_bundle();
            bundle.IndexedValueView::set(index, value_view);
        }
        void set(std::string_view name, const ValueView &value_view) { this->value().as_bundle().set(name, value_view); }

      private:
        const TsBundleOps *m_bundle_ops{nullptr};
        size_t             m_kind_state_offset{TsStatePlanLayout::npos};
    };

    template <typename Binding>
        requires requires(const Binding &binding) {
            { binding.checked_ops().checked_value_binding() } -> std::same_as<const ValueTypeBinding &>;
        }
    struct TslView : BasicTsView<Binding>
    {
        using base_type = BasicTsView<Binding>;
        using base_type::base_type;

        explicit TslView(base_type view)
            : base_type(std::move(view)), m_list_ops(&detail::checked_list_ops(this->ops())),
              m_kind_state_offset(this->state_plan() != nullptr ? detail::checked_kind_state_offset(*this->state_plan())
                                                                : TsStatePlanLayout::npos) {}

        [[nodiscard]] const TsValueTypeBinding *element_binding() const noexcept { return m_list_ops->element_binding; }
        [[nodiscard]] bool                      is_fixed() const noexcept { return m_list_ops->is_fixed(); }
        [[nodiscard]] size_t                    fixed_size() const noexcept { return m_list_ops->fixed_size; }
        [[nodiscard]] size_t                    size() const { return this->value().as_list().size(); }

        [[nodiscard]] TsView at(size_t index) const {
            const ValueView child_value = this->value().as_list().at(index);
            void           *child_state = nullptr;
            if (this->state_data() != nullptr && m_kind_state_offset != TsStatePlanLayout::npos &&
                m_list_ops->has_inline_child_states()) {
                child_state =
                    MemoryUtils::advance(this->state_data(), m_kind_state_offset + index * m_list_ops->element_state_stride);
            } else {
                ListView            list  = this->value().as_list();
                TsDynamicListState *state = ensure_bound_child_states(list);
                child_state               = state != nullptr ? state->child_state_memory(index) : nullptr;
            }
            return detail::child_ts_view(m_list_ops->checked_element_binding(), child_state, const_cast<void *>(child_value.data()),
                                         this->evaluation_time());
        }

        [[nodiscard]] TsView operator[](size_t index) const { return at(index); }
        [[nodiscard]] TsView front() const { return at(0); }
        [[nodiscard]] TsView back() const { return at(size() - 1); }

        void set(size_t index, const ValueView &value_view) { this->value().as_list().set(index, value_view); }
        void push_back(const ValueView &value_view) { this->value().as_list().push_back(value_view); }
        void pop_back() { this->value().as_list().pop_back(); }
        void resize(size_t new_size) { this->value().as_list().resize(new_size); }
        void clear() { this->value().as_list().clear(); }

        class iterator
        {
          public:
            iterator() = default;

            iterator(const TslView *view, size_t index) noexcept : m_view(view), m_index(index) {}

            [[nodiscard]] TsView operator*() const { return m_view->at(m_index); }

            iterator &operator++() {
                ++m_index;
                return *this;
            }

            [[nodiscard]] bool operator==(const iterator &other) const noexcept {
                return m_view == other.m_view && m_index == other.m_index;
            }

            [[nodiscard]] bool operator!=(const iterator &other) const noexcept { return !(*this == other); }

          private:
            const TslView *m_view{nullptr};
            size_t         m_index{0};
        };

        [[nodiscard]] iterator begin() const noexcept { return iterator(this, 0); }
        [[nodiscard]] iterator end() const noexcept { return iterator(this, size()); }

      private:
        const TsListOps *m_list_ops{nullptr};
        size_t           m_kind_state_offset{TsStatePlanLayout::npos};

        [[nodiscard]] TsDynamicListState *dynamic_list_state() const noexcept {
            if (this->state_data() == nullptr || m_kind_state_offset == TsStatePlanLayout::npos ||
                m_list_ops->has_inline_child_states()) {
                return nullptr;
            }
            return MemoryUtils::cast<TsDynamicListState>(MemoryUtils::advance(this->state_data(), m_kind_state_offset));
        }

        [[nodiscard]] TsDynamicListState *ensure_bound_child_states(ListView list) const {
            TsDynamicListState *state = dynamic_list_state();
            if (state == nullptr) { return nullptr; }
            state->ensure_bound(*static_cast<detail::DynamicListStorage *>(list.data()), m_list_ops->checked_element_binding(),
                                this->state_allocator());
            return state;
        }
    };

    template <typename Binding>
        requires requires(const Binding &binding) {
            { binding.checked_ops().checked_value_binding() } -> std::same_as<const ValueTypeBinding &>;
        }
    struct TssView : BasicTsView<Binding>
    {
        using base_type = BasicTsView<Binding>;
        using base_type::base_type;

        explicit TssView(base_type view) : base_type(std::move(view)), m_set_ops(&detail::checked_set_ops(this->ops())) {}

        [[nodiscard]] size_t                   size() const { return this->value().as_set().size(); }
        [[nodiscard]] bool                     empty() const { return this->value().as_set().empty(); }
        [[nodiscard]] const ValueTypeMetaData *element_type() const noexcept { return m_set_ops->element_type; }
        [[nodiscard]] bool              contains(const ValueView &entry) const { return this->value().as_set().contains(entry); }
        [[nodiscard]] bool              add(const ValueView &entry) { return this->value().as_set().add(entry); }
        [[nodiscard]] bool              remove(const ValueView &entry) { return this->value().as_set().remove(entry); }
        void                            clear() { this->value().as_set().clear(); }
        void                            begin_mutation() { this->value().as_set().begin_mutation(); }
        void                            end_mutation() { this->value().as_set().end_mutation(); }
        void                            erase_pending() { this->value().as_set().erase_pending(); }
        [[nodiscard]] bool              has_pending_erase() const { return this->value().as_set().has_pending_erase(); }
        [[nodiscard]] SetView::iterator begin() const { return this->value().as_set().begin(); }
        [[nodiscard]] SetView::iterator end() const { return this->value().as_set().end(); }

      private:
        const TsSetOps *m_set_ops{nullptr};
    };

    template <typename Binding>
        requires requires(const Binding &binding) {
            { binding.checked_ops().checked_value_binding() } -> std::same_as<const ValueTypeBinding &>;
        }
    struct TsdView : BasicTsView<Binding>
    {
        using base_type = BasicTsView<Binding>;
        using base_type::base_type;

        explicit TsdView(base_type view)
            : base_type(std::move(view)), m_dict_ops(&detail::checked_dict_ops(this->ops())),
              m_kind_state_offset(this->state_plan() != nullptr ? detail::checked_kind_state_offset(*this->state_plan())
                                                                : TsStatePlanLayout::npos) {}

        [[nodiscard]] size_t                    size() const { return this->value().as_map().size(); }
        [[nodiscard]] bool                      empty() const { return this->value().as_map().empty(); }
        [[nodiscard]] const ValueTypeMetaData  *key_type() const noexcept { return m_dict_ops->key_type; }
        [[nodiscard]] const TsValueTypeBinding *value_binding() const noexcept { return m_dict_ops->value_binding; }

        [[nodiscard]] bool contains(const ValueView &key) const { return this->value().as_map().contains(key); }

        [[nodiscard]] TsView at(const ValueView &key) const {
            MapView      map  = this->value().as_map();
            const size_t slot = map.find_slot(key);
            if (slot == KeySlotStore::npos) { throw std::out_of_range("TsdView key not found"); }

            TsDictState    *state       = ensure_bound_child_states(map);
            const void     *child_state = state != nullptr ? state->child_state_memory(slot) : nullptr;
            const ValueView child_value = map.value_at_slot(slot);
            return detail::child_ts_view(m_dict_ops->checked_value_binding(), const_cast<void *>(child_state),
                                         const_cast<void *>(child_value.data()), this->evaluation_time());
        }

        void               set(const ValueView &key, const ValueView &value_view) { this->value().as_map().set(key, value_view); }
        [[nodiscard]] bool add(const ValueView &key, const ValueView &value_view) {
            return this->value().as_map().add(key, value_view);
        }
        [[nodiscard]] bool remove(const ValueView &key) { return this->value().as_map().remove(key); }
        void               clear() { this->value().as_map().clear(); }
        void               begin_mutation() { this->value().as_map().begin_mutation(); }
        void               end_mutation() { this->value().as_map().end_mutation(); }
        void               erase_pending() { this->value().as_map().erase_pending(); }
        [[nodiscard]] bool has_pending_erase() const { return this->value().as_map().has_pending_erase(); }

        struct entry
        {
            ValueView key{};
            TsView    value{};
        };

        class iterator
        {
          public:
            iterator() = default;

            iterator(const TsdView *view, size_t slot) noexcept : m_view(view), m_slot(slot) { advance_to_live(); }

            [[nodiscard]] entry operator*() const {
                MapView         map   = view_ref().value().as_map();
                TsDictState    *state = view_ref().ensure_bound_child_states(map);
                const ValueView key   = map.key_at_slot(m_slot);
                const ValueView value = map.value_at_slot(m_slot);
                return entry{
                    .key   = key,
                    .value = detail::child_ts_view(*view_ref().value_binding(),
                                                   state != nullptr ? state->child_state_memory(m_slot) : nullptr,
                                                   const_cast<void *>(value.data()), view_ref().evaluation_time()),
                };
            }

            iterator &operator++() {
                ++m_slot;
                advance_to_live();
                return *this;
            }

            [[nodiscard]] bool operator==(const iterator &other) const noexcept {
                return m_view == other.m_view && m_slot == other.m_slot;
            }
            [[nodiscard]] bool operator!=(const iterator &other) const noexcept { return !(*this == other); }

          private:
            [[nodiscard]] const TsdView &view_ref() const noexcept { return *m_view; }

            void advance_to_live() noexcept {
                if (m_view == nullptr || !view_ref().has_value()) { return; }
                MapView map = view_ref().value().as_map();
                while (m_slot < map.slot_capacity() && !map.slot_live(m_slot)) { ++m_slot; }
            }

            const TsdView *m_view{nullptr};
            size_t         m_slot{0};
        };

        [[nodiscard]] iterator begin() const { return iterator(this, 0); }
        [[nodiscard]] iterator end() const { return iterator(this, this->value().as_map().slot_capacity()); }

      private:
        const TsDictOps *m_dict_ops{nullptr};
        size_t           m_kind_state_offset{TsStatePlanLayout::npos};

        [[nodiscard]] TsDictState *dict_state() const noexcept {
            if (this->state_data() == nullptr || m_kind_state_offset == TsStatePlanLayout::npos) { return nullptr; }
            return MemoryUtils::cast<TsDictState>(MemoryUtils::advance(this->state_data(), m_kind_state_offset));
        }

        [[nodiscard]] TsDictState *ensure_bound_child_states(MapView map) const {
            TsDictState *state = dict_state();
            if (state == nullptr) { return nullptr; }
            state->ensure_bound(*static_cast<detail::MapStorage *>(map.data()), m_dict_ops->checked_value_binding(),
                                this->state_allocator());
            return state;
        }
    };

    template <typename Binding>
        requires requires(const Binding &binding) {
            { binding.checked_ops().checked_value_binding() } -> std::same_as<const ValueTypeBinding &>;
        }
    struct TswView : BasicTsView<Binding>
    {
        using base_type = BasicTsView<Binding>;
        using base_type::base_type;

        explicit TswView(base_type view) : base_type(std::move(view)), m_window_ops(&detail::checked_window_ops(this->ops())) {}

        [[nodiscard]] bool                     is_duration_based() const noexcept { return m_window_ops->is_duration_based; }
        [[nodiscard]] size_t                   period() const noexcept { return m_window_ops->period; }
        [[nodiscard]] size_t                   min_period() const noexcept { return m_window_ops->min_period; }
        [[nodiscard]] engine_time_delta_t      time_range() const noexcept { return m_window_ops->time_range; }
        [[nodiscard]] engine_time_delta_t      min_time_range() const noexcept { return m_window_ops->min_time_range; }
        [[nodiscard]] const ValueTypeMetaData *element_type() const noexcept { return m_window_ops->element_type; }

      private:
        const TsWindowOps *m_window_ops{nullptr};
    };

    template <typename Binding>
        requires requires(const Binding &binding) {
            { binding.checked_ops().checked_value_binding() } -> std::same_as<const ValueTypeBinding &>;
        }
    inline TsbView<Binding> BasicTsView<Binding>::as_tsb() const {
        if (!is_tsb()) { throw std::logic_error("BasicTsView::as_tsb() requires a TSB value"); }
        return TsbView<Binding>{BasicTsView<Binding>{this->cloned_context()}};
    }

    template <typename Binding>
        requires requires(const Binding &binding) {
            { binding.checked_ops().checked_value_binding() } -> std::same_as<const ValueTypeBinding &>;
        }
    inline TslView<Binding> BasicTsView<Binding>::as_tsl() const {
        if (!is_tsl()) { throw std::logic_error("BasicTsView::as_tsl() requires a TSL value"); }
        return TslView<Binding>{BasicTsView<Binding>{this->cloned_context()}};
    }

    template <typename Binding>
        requires requires(const Binding &binding) {
            { binding.checked_ops().checked_value_binding() } -> std::same_as<const ValueTypeBinding &>;
        }
    inline TssView<Binding> BasicTsView<Binding>::as_tss() const {
        if (!is_tss()) { throw std::logic_error("BasicTsView::as_tss() requires a TSS value"); }
        return TssView<Binding>{BasicTsView<Binding>{this->cloned_context()}};
    }

    template <typename Binding>
        requires requires(const Binding &binding) {
            { binding.checked_ops().checked_value_binding() } -> std::same_as<const ValueTypeBinding &>;
        }
    inline TsdView<Binding> BasicTsView<Binding>::as_tsd() const {
        if (!is_tsd()) { throw std::logic_error("BasicTsView::as_tsd() requires a TSD value"); }
        return TsdView<Binding>{BasicTsView<Binding>{this->cloned_context()}};
    }

    template <typename Binding>
        requires requires(const Binding &binding) {
            { binding.checked_ops().checked_value_binding() } -> std::same_as<const ValueTypeBinding &>;
        }
    inline TswView<Binding> BasicTsView<Binding>::as_tsw() const {
        if (!is_tsw()) { throw std::logic_error("BasicTsView::as_tsw() requires a TSW value"); }
        return TswView<Binding>{BasicTsView<Binding>{this->cloned_context()}};
    }
}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_TS_SPECIALIZED_VIEWS_H
