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

        [[nodiscard]] inline TsView child_ts_view(const TSValueTypeMetaData *type, void *data, engine_time_t evaluation_time) {
            const TsValueTypeBinding &binding = checked_ts_child_binding(type);
            return TsView{TsViewContext{ts_storage_view(&binding, data), evaluation_time}};
        }
    }  // namespace detail

    struct TsbView : TsView
    {
        using TsView::TsView;

        explicit TsbView(TsView view) noexcept : TsView(std::move(view)) {}

        [[nodiscard]] size_t field_count() const {
            if (!has_value()) { throw std::logic_error("TsbView::field_count() on an empty view"); }
            return type()->field_count();
        }

        [[nodiscard]] bool has_field(std::string_view name) const noexcept {
            const auto *field = type()->fields();
            for (size_t index = 0; index < type()->field_count(); ++index) {
                if (field[index].name != nullptr && name == field[index].name) { return true; }
            }
            return false;
        }

        [[nodiscard]] TsView at(size_t index) const {
            if (!has_value()) { throw std::logic_error("TsbView::at() on an empty view"); }
            if (index >= type()->field_count()) { throw std::out_of_range("TsbView index out of range"); }

            BundleView      bundle = value().as_bundle();
            const ValueView child  = bundle.IndexedValueView::at(index);
            return detail::child_ts_view(type()->fields()[index].type, const_cast<void *>(child.data()), evaluation_time());
        }

        [[nodiscard]] TsView at(std::string_view name) const {
            if (!has_value()) { throw std::logic_error("TsbView::at() on an empty view"); }

            const auto *field = type()->fields();
            for (size_t index = 0; index < type()->field_count(); ++index) {
                if (field[index].name != nullptr && name == field[index].name) { return at(index); }
            }
            throw std::out_of_range("TsbView field not found");
        }

        [[nodiscard]] TsView operator[](size_t index) const { return at(index); }
        [[nodiscard]] TsView operator[](std::string_view name) const { return at(name); }

        void set(size_t index, const ValueView &value_view) {
            BundleView bundle = value().as_bundle();
            bundle.IndexedValueView::set(index, value_view);
        }
        void set(std::string_view name, const ValueView &value_view) { value().as_bundle().set(name, value_view); }
    };

    struct TslView : TsView
    {
        using TsView::TsView;

        explicit TslView(TsView view) noexcept : TsView(std::move(view)) {}

        [[nodiscard]] const TSValueTypeMetaData *element_ts() const noexcept {
            return type() != nullptr ? type()->element_ts() : nullptr;
        }
        [[nodiscard]] bool   is_fixed() const noexcept { return type() != nullptr && type()->fixed_size() != 0; }
        [[nodiscard]] size_t fixed_size() const noexcept { return type() != nullptr ? type()->fixed_size() : 0; }
        [[nodiscard]] size_t size() const { return value().as_list().size(); }

        [[nodiscard]] TsView at(size_t index) const {
            const ValueView child = value().as_list().at(index);
            return detail::child_ts_view(element_ts(), const_cast<void *>(child.data()), evaluation_time());
        }

        [[nodiscard]] TsView operator[](size_t index) const { return at(index); }
        [[nodiscard]] TsView front() const { return at(0); }
        [[nodiscard]] TsView back() const { return at(size() - 1); }

        void set(size_t index, const ValueView &value_view) { value().as_list().set(index, value_view); }
        void push_back(const ValueView &value_view) { value().as_list().push_back(value_view); }
        void pop_back() { value().as_list().pop_back(); }
        void resize(size_t new_size) { value().as_list().resize(new_size); }
        void clear() { value().as_list().clear(); }

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
    };

    struct TssView : TsView
    {
        using TsView::TsView;

        explicit TssView(TsView view) noexcept : TsView(std::move(view)) {}

        [[nodiscard]] size_t                   size() const { return value().as_set().size(); }
        [[nodiscard]] bool                     empty() const { return value().as_set().empty(); }
        [[nodiscard]] const ValueTypeMetaData *element_type() const noexcept {
            return value().type() != nullptr ? value().type()->element_type : nullptr;
        }
        [[nodiscard]] bool              contains(const ValueView &entry) const { return value().as_set().contains(entry); }
        [[nodiscard]] bool              add(const ValueView &entry) { return value().as_set().add(entry); }
        [[nodiscard]] bool              remove(const ValueView &entry) { return value().as_set().remove(entry); }
        void                            clear() { value().as_set().clear(); }
        void                            begin_mutation() { value().as_set().begin_mutation(); }
        void                            end_mutation() { value().as_set().end_mutation(); }
        void                            erase_pending() { value().as_set().erase_pending(); }
        [[nodiscard]] bool              has_pending_erase() const { return value().as_set().has_pending_erase(); }
        [[nodiscard]] SetView::iterator begin() const { return value().as_set().begin(); }
        [[nodiscard]] SetView::iterator end() const { return value().as_set().end(); }
    };

    struct TsdView : TsView
    {
        using TsView::TsView;

        explicit TsdView(TsView view) noexcept : TsView(std::move(view)) {}

        [[nodiscard]] size_t                   size() const { return value().as_map().size(); }
        [[nodiscard]] bool                     empty() const { return value().as_map().empty(); }
        [[nodiscard]] const ValueTypeMetaData *key_type() const noexcept {
            return type() != nullptr ? type()->key_type() : nullptr;
        }
        [[nodiscard]] const TSValueTypeMetaData *value_ts() const noexcept {
            return type() != nullptr ? type()->element_ts() : nullptr;
        }

        [[nodiscard]] bool contains(const ValueView &key) const { return value().as_map().contains(key); }

        [[nodiscard]] TsView at(const ValueView &key) const {
            const ValueView child = value().as_map().at(key);
            return detail::child_ts_view(value_ts(), const_cast<void *>(child.data()), evaluation_time());
        }

        void               set(const ValueView &key, const ValueView &value_view) { value().as_map().set(key, value_view); }
        [[nodiscard]] bool add(const ValueView &key, const ValueView &value_view) { return value().as_map().add(key, value_view); }
        [[nodiscard]] bool remove(const ValueView &key) { return value().as_map().remove(key); }
        void               clear() { value().as_map().clear(); }
        void               begin_mutation() { value().as_map().begin_mutation(); }
        void               end_mutation() { value().as_map().end_mutation(); }
        void               erase_pending() { value().as_map().erase_pending(); }
        [[nodiscard]] bool has_pending_erase() const { return value().as_map().has_pending_erase(); }

        struct entry
        {
            ValueView key{};
            TsView    value{};
        };

        class iterator
        {
          public:
            iterator() = default;

            iterator(MapView::iterator iterator, engine_time_t evaluation_time, const TSValueTypeMetaData *value_ts) noexcept
                : m_iterator(iterator), m_evaluation_time(evaluation_time), m_value_ts(value_ts) {}

            [[nodiscard]] entry operator*() const {
                const auto current = *m_iterator;
                return entry{
                    .key   = current.key,
                    .value = detail::child_ts_view(m_value_ts, const_cast<void *>(current.value.data()), m_evaluation_time),
                };
            }

            iterator &operator++() {
                ++m_iterator;
                return *this;
            }

            [[nodiscard]] bool operator==(const iterator &other) const noexcept { return m_iterator == other.m_iterator; }
            [[nodiscard]] bool operator!=(const iterator &other) const noexcept { return !(*this == other); }

          private:
            MapView::iterator          m_iterator{};
            engine_time_t              m_evaluation_time{MIN_DT};
            const TSValueTypeMetaData *m_value_ts{nullptr};
        };

        [[nodiscard]] iterator begin() const { return iterator(value().as_map().begin(), evaluation_time(), value_ts()); }
        [[nodiscard]] iterator end() const { return iterator(value().as_map().end(), evaluation_time(), value_ts()); }
    };

    struct TswView : TsView
    {
        using TsView::TsView;

        explicit TswView(TsView view) noexcept : TsView(std::move(view)) {}

        [[nodiscard]] bool   is_duration_based() const noexcept { return type() != nullptr && type()->is_duration_based(); }
        [[nodiscard]] size_t period() const noexcept { return type() != nullptr ? type()->period() : 0; }
        [[nodiscard]] size_t min_period() const noexcept { return type() != nullptr ? type()->min_period() : 0; }
        [[nodiscard]] engine_time_delta_t time_range() const noexcept {
            return type() != nullptr ? type()->time_range() : engine_time_delta_t{0};
        }
        [[nodiscard]] engine_time_delta_t min_time_range() const noexcept {
            return type() != nullptr ? type()->min_time_range() : engine_time_delta_t{0};
        }
        [[nodiscard]] const ValueTypeMetaData *element_type() const noexcept { return value_type(); }
    };

    template <typename Binding>
        requires requires(const Binding &binding) {
            { binding.checked_ops().checked_value_binding() } -> std::same_as<const ValueTypeBinding &>;
        }
    inline TsbView BasicTsView<Binding>::as_tsb() const {
        if (!is_tsb()) { throw std::logic_error("BasicTsView::as_tsb() requires a TSB value"); }
        const TsValueTypeBinding &binding = detail::checked_ts_child_binding(type());
        return TsbView{
            TsView{TsViewContext{detail::ts_storage_view(&binding, const_cast<void *>(storage().data())), evaluation_time()}}};
    }

    template <typename Binding>
        requires requires(const Binding &binding) {
            { binding.checked_ops().checked_value_binding() } -> std::same_as<const ValueTypeBinding &>;
        }
    inline TslView BasicTsView<Binding>::as_tsl() const {
        if (!is_tsl()) { throw std::logic_error("BasicTsView::as_tsl() requires a TSL value"); }
        const TsValueTypeBinding &binding = detail::checked_ts_child_binding(type());
        return TslView{
            TsView{TsViewContext{detail::ts_storage_view(&binding, const_cast<void *>(storage().data())), evaluation_time()}}};
    }

    template <typename Binding>
        requires requires(const Binding &binding) {
            { binding.checked_ops().checked_value_binding() } -> std::same_as<const ValueTypeBinding &>;
        }
    inline TssView BasicTsView<Binding>::as_tss() const {
        if (!is_tss()) { throw std::logic_error("BasicTsView::as_tss() requires a TSS value"); }
        const TsValueTypeBinding &binding = detail::checked_ts_child_binding(type());
        return TssView{
            TsView{TsViewContext{detail::ts_storage_view(&binding, const_cast<void *>(storage().data())), evaluation_time()}}};
    }

    template <typename Binding>
        requires requires(const Binding &binding) {
            { binding.checked_ops().checked_value_binding() } -> std::same_as<const ValueTypeBinding &>;
        }
    inline TsdView BasicTsView<Binding>::as_tsd() const {
        if (!is_tsd()) { throw std::logic_error("BasicTsView::as_tsd() requires a TSD value"); }
        const TsValueTypeBinding &binding = detail::checked_ts_child_binding(type());
        return TsdView{
            TsView{TsViewContext{detail::ts_storage_view(&binding, const_cast<void *>(storage().data())), evaluation_time()}}};
    }

    template <typename Binding>
        requires requires(const Binding &binding) {
            { binding.checked_ops().checked_value_binding() } -> std::same_as<const ValueTypeBinding &>;
        }
    inline TswView BasicTsView<Binding>::as_tsw() const {
        if (!is_tsw()) { throw std::logic_error("BasicTsView::as_tsw() requires a TSW value"); }
        const TsValueTypeBinding &binding = detail::checked_ts_child_binding(type());
        return TswView{
            TsView{TsViewContext{detail::ts_storage_view(&binding, const_cast<void *>(storage().data())), evaluation_time()}}};
    }
}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_TS_SPECIALIZED_VIEWS_H
