#ifndef HGRAPH_CPP_ROOT_SPECIALIZED_VIEWS_H
#define HGRAPH_CPP_ROOT_SPECIALIZED_VIEWS_H

#ifndef HGRAPH_CPP_ROOT_VIEW_H
    #error "specialized_views.h requires view.h to be included first"
#endif

#include <string_view>

namespace hgraph::v2
{
    namespace detail
    {
        [[nodiscard]] inline ValueView child_view(const void *memory, const ValueTypeMetaData *type) {
            const ValueTypeBinding &binding = checked_child_binding(type);
            return ValueView{&binding, const_cast<void *>(memory)};
        }

        inline void require_child_value(const ValueView &value, const ValueTypeMetaData *type, const char *message) {
            const ValueTypeBinding &binding = checked_child_binding(type);
            if (!value.has_value()) { throw std::logic_error(std::string(message) + " requires a source value"); }
            if (value.binding() != &binding) {
                throw std::logic_error(std::string(message) + " requires a matching child binding");
            }
        }
    }  // namespace detail

    struct IndexedValueView : ValueView
    {
        using ValueView::ValueView;

        explicit IndexedValueView(ValueView view) noexcept : ValueView(view) {}

        [[nodiscard]] size_t size() const {
            if (!has_value()) { throw std::logic_error("IndexedValueView::size() on an empty view"); }

            switch (type()->kind) {
                case ValueTypeKind::Tuple:
                case ValueTypeKind::Bundle: return type()->field_count;
                case ValueTypeKind::List:
                    return type()->fixed_size != 0 ? type()->fixed_size
                                                   : static_cast<const detail::DynamicListStorage *>(data())->size;
                case ValueTypeKind::CyclicBuffer: return static_cast<const detail::CyclicBufferStorage *>(data())->size;
                case ValueTypeKind::Queue: return static_cast<const detail::QueueStorage *>(data())->size();
                default: throw std::logic_error("IndexedValueView kind does not support positional access");
            }
        }

        [[nodiscard]] ValueView at(size_t index) const { return detail::child_view(element_memory(index), element_type(index)); }

        [[nodiscard]] ValueView operator[](size_t index) const { return at(index); }

        void set(size_t index, const ValueView &value) {
            const auto *child_type = element_type(index);
            detail::require_child_value(value, child_type, "IndexedValueView::set()");
            replace_element(index, value.data());
        }

        class iterator
        {
          public:
            iterator() = default;

            iterator(const IndexedValueView *view, size_t index) noexcept : m_view(view), m_index(index) {}

            [[nodiscard]] ValueView operator*() const { return m_view->at(m_index); }

            iterator &operator++() {
                ++m_index;
                return *this;
            }

            [[nodiscard]] bool operator==(const iterator &other) const noexcept {
                return m_view == other.m_view && m_index == other.m_index;
            }

            [[nodiscard]] bool operator!=(const iterator &other) const noexcept { return !(*this == other); }

          private:
            const IndexedValueView *m_view{nullptr};
            size_t                  m_index{0};
        };

        [[nodiscard]] iterator begin() const noexcept { return iterator(this, 0); }
        [[nodiscard]] iterator end() const noexcept { return iterator(this, size()); }

      protected:
        [[nodiscard]] const ValueTypeMetaData *element_type(size_t index) const {
            switch (type()->kind) {
                case ValueTypeKind::Tuple:
                case ValueTypeKind::Bundle:
                    if (index >= type()->field_count) { throw std::out_of_range("IndexedValueView index out of range"); }
                    return type()->fields[index].type;
                case ValueTypeKind::List:
                case ValueTypeKind::CyclicBuffer:
                case ValueTypeKind::Queue:
                    if (index >= size()) { throw std::out_of_range("IndexedValueView index out of range"); }
                    return type()->element_type;
                default: throw std::logic_error("IndexedValueView kind does not expose homogeneous element types");
            }
        }

        [[nodiscard]] void *element_memory(size_t index) const {
            if (!has_value()) { throw std::logic_error("IndexedValueView::element_memory() on an empty view"); }

            switch (type()->kind) {
                case ValueTypeKind::Tuple:
                case ValueTypeKind::Bundle:
                    {
                        if (index >= type()->field_count) { throw std::out_of_range("IndexedValueView index out of range"); }
                        return const_cast<void *>(MemoryUtils::advance(data(), type()->fields[index].offset));
                    }
                case ValueTypeKind::List:
                    {
                        if (type()->fixed_size != 0) {
                            return const_cast<void *>(MemoryUtils::advance(data(), plan()->element_offset(index)));
                        }
                        auto *storage = static_cast<detail::DynamicListStorage *>(const_cast<void *>(data()));
                        if (index >= storage->size) { throw std::out_of_range("IndexedValueView index out of range"); }
                        return storage->values.value_memory(index);
                    }
                case ValueTypeKind::CyclicBuffer:
                    {
                        auto *storage = static_cast<detail::CyclicBufferStorage *>(const_cast<void *>(data()));
                        return storage->values.value_memory(storage->slot_for_index(index));
                    }
                case ValueTypeKind::Queue:
                    {
                        auto *storage = static_cast<detail::QueueStorage *>(const_cast<void *>(data()));
                        if (index >= storage->order.size()) { throw std::out_of_range("IndexedValueView index out of range"); }
                        return storage->values.value_memory(storage->order[index]);
                    }
                default: throw std::logic_error("IndexedValueView kind does not support positional access");
            }
        }

        void replace_element(size_t index, const void *src) {
            switch (type()->kind) {
                case ValueTypeKind::Tuple:
                case ValueTypeKind::Bundle:
                    {
                        const auto &field = type()->fields[index];
                        detail::replace_value_memory(MemoryUtils::advance(data(), field.offset),
                                                     detail::checked_child_binding(field.type).checked_plan(), src);
                        return;
                    }
                case ValueTypeKind::List:
                    {
                        if (type()->fixed_size != 0) {
                            detail::replace_value_memory(MemoryUtils::advance(data(), plan()->element_offset(index)),
                                                         detail::checked_child_binding(type()->element_type).checked_plan(), src);
                            return;
                        }
                        auto *storage = static_cast<detail::DynamicListStorage *>(data());
                        storage->replace_at(index, src);
                        return;
                    }
                case ValueTypeKind::CyclicBuffer:
                    {
                        auto        *storage = static_cast<detail::CyclicBufferStorage *>(data());
                        const size_t slot    = storage->slot_for_index(index);
                        detail::replace_slot_value(storage->values, slot, src);
                        return;
                    }
                case ValueTypeKind::Queue:
                    {
                        auto *storage = static_cast<detail::QueueStorage *>(data());
                        if (index >= storage->order.size()) { throw std::out_of_range("IndexedValueView index out of range"); }
                        detail::replace_slot_value(storage->values, storage->order[index], src);
                        return;
                    }
                default: throw std::logic_error("IndexedValueView kind does not support mutation by index");
            }
        }
    };

    struct TupleView : IndexedValueView
    {
        using IndexedValueView::IndexedValueView;

        explicit TupleView(ValueView view) noexcept : IndexedValueView(view) {}
    };

    struct BundleView : IndexedValueView
    {
        using IndexedValueView::IndexedValueView;

        // Bundles use the same field-addressable layout as MemoryUtils named tuples,
        // but remain a distinct public value kind in the type system.
        explicit BundleView(ValueView view) noexcept : IndexedValueView(view) {}

        [[nodiscard]] size_t field_count() const { return size(); }

        [[nodiscard]] bool has_field(std::string_view name) const noexcept {
            const auto *field = type()->fields;
            for (size_t index = 0; index < type()->field_count; ++index) {
                if (field[index].name != nullptr && name == field[index].name) { return true; }
            }
            return false;
        }

        [[nodiscard]] ValueView at(std::string_view name) const {
            const auto *field = type()->fields;
            for (size_t index = 0; index < type()->field_count; ++index) {
                if (field[index].name != nullptr && name == field[index].name) { return IndexedValueView::at(index); }
            }
            throw std::out_of_range("BundleView field not found");
        }

        [[nodiscard]] ValueView operator[](std::string_view name) const { return at(name); }

        void set(std::string_view name, const ValueView &value) {
            const auto *field = type()->fields;
            for (size_t index = 0; index < type()->field_count; ++index) {
                if (field[index].name != nullptr && name == field[index].name) {
                    IndexedValueView::set(index, value);
                    return;
                }
            }
            throw std::out_of_range("BundleView field not found");
        }
    };

    struct ListView : IndexedValueView
    {
        using IndexedValueView::IndexedValueView;

        explicit ListView(ValueView view) noexcept : IndexedValueView(view) {}

        [[nodiscard]] const ValueTypeMetaData *element_type() const noexcept { return type()->element_type; }
        [[nodiscard]] bool                     is_fixed() const noexcept { return type()->fixed_size != 0; }
        [[nodiscard]] ValueView                front() const { return at(0); }
        [[nodiscard]] ValueView                back() const { return at(size() - 1); }

        void push_back(const ValueView &value) {
            if (is_fixed()) { throw std::logic_error("ListView::push_back() is only valid for dynamic lists"); }
            detail::require_child_value(value, type()->element_type, "ListView::push_back()");
            static_cast<detail::DynamicListStorage *>(data())->push_back_copy(value.data());
        }

        void pop_back() {
            if (is_fixed()) { throw std::logic_error("ListView::pop_back() is only valid for dynamic lists"); }
            auto *storage = static_cast<detail::DynamicListStorage *>(data());
            if (storage->size == 0) { throw std::out_of_range("ListView::pop_back() on an empty list"); }
            storage->values.destroy_at(storage->size - 1);
            --storage->size;
        }

        void resize(size_t new_size) {
            if (is_fixed()) { throw std::logic_error("ListView::resize() is only valid for dynamic lists"); }
            static_cast<detail::DynamicListStorage *>(data())->resize(new_size);
        }

        void clear() {
            if (is_fixed()) { throw std::logic_error("ListView::clear() is only valid for dynamic lists"); }
            static_cast<detail::DynamicListStorage *>(data())->clear();
        }
    };

    struct CyclicBufferView : IndexedValueView
    {
        using IndexedValueView::IndexedValueView;

        explicit CyclicBufferView(ValueView view) noexcept : IndexedValueView(view) {}

        [[nodiscard]] size_t    capacity() const noexcept { return type()->fixed_size; }
        [[nodiscard]] bool      full() const { return size() == capacity(); }
        [[nodiscard]] ValueView front() const { return at(0); }
        [[nodiscard]] ValueView back() const { return at(size() - 1); }

        void push(const ValueView &value) {
            detail::require_child_value(value, type()->element_type, "CyclicBufferView::push()");
            static_cast<detail::CyclicBufferStorage *>(data())->push_back_copy(value.data());
        }

        void clear() { static_cast<detail::CyclicBufferStorage *>(data())->clear(); }
    };

    struct QueueView : IndexedValueView
    {
        using IndexedValueView::IndexedValueView;

        explicit QueueView(ValueView view) noexcept : IndexedValueView(view) {}

        [[nodiscard]] size_t    max_capacity() const noexcept { return type()->fixed_size; }
        [[nodiscard]] bool      has_max_capacity() const noexcept { return max_capacity() != 0; }
        [[nodiscard]] ValueView front() const { return at(0); }
        [[nodiscard]] ValueView back() const { return at(size() - 1); }

        void push(const ValueView &value) {
            detail::require_child_value(value, type()->element_type, "QueueView::push()");
            static_cast<detail::QueueStorage *>(data())->push_back_copy(value.data());
        }

        void pop() { static_cast<detail::QueueStorage *>(data())->pop_front(); }

        void clear() { static_cast<detail::QueueStorage *>(data())->clear(); }
    };

    struct SetView : ValueView
    {
        using ValueView::ValueView;

        explicit SetView(ValueView view) noexcept : ValueView(view) {}

        [[nodiscard]] size_t size() const { return static_cast<const detail::SetStorage *>(data())->keys.size(); }

        [[nodiscard]] bool empty() const { return size() == 0; }

        [[nodiscard]] bool contains(const ValueView &value) const {
            detail::require_child_value(value, type()->element_type, "SetView::contains()");
            return static_cast<const detail::SetStorage *>(data())->keys.contains(value.data());
        }

        [[nodiscard]] bool add(const ValueView &value) {
            detail::require_child_value(value, type()->element_type, "SetView::add()");
            return static_cast<detail::SetStorage *>(data())->keys.insert(value.data()).inserted;
        }

        [[nodiscard]] bool remove(const ValueView &value) {
            detail::require_child_value(value, type()->element_type, "SetView::remove()");
            return static_cast<detail::SetStorage *>(data())->keys.remove(value.data());
        }

        void               clear() { static_cast<detail::SetStorage *>(data())->clear(); }
        void               begin_mutation() { static_cast<detail::SetStorage *>(data())->keys.begin_mutation(); }
        void               end_mutation() { static_cast<detail::SetStorage *>(data())->keys.end_mutation(); }
        void               erase_pending() { static_cast<detail::SetStorage *>(data())->keys.erase_pending(); }
        [[nodiscard]] bool has_pending_erase() const {
            return static_cast<const detail::SetStorage *>(data())->keys.has_pending_erase();
        }

        class iterator
        {
          public:
            iterator() = default;

            iterator(const detail::SetStorage *storage, const ValueTypeMetaData *element_type, size_t slot) noexcept
                : m_storage(storage), m_element_type(element_type), m_slot(slot) {
                advance_to_live();
            }

            [[nodiscard]] ValueView operator*() const {
                return detail::child_view(const_cast<void *>(m_storage->keys[m_slot]), m_element_type);
            }

            iterator &operator++() {
                ++m_slot;
                advance_to_live();
                return *this;
            }

            [[nodiscard]] bool operator==(const iterator &other) const noexcept {
                return m_storage == other.m_storage && m_slot == other.m_slot;
            }

            [[nodiscard]] bool operator!=(const iterator &other) const noexcept { return !(*this == other); }

          private:
            void advance_to_live() noexcept {
                if (m_storage == nullptr) { return; }
                while (m_slot < m_storage->keys.slot_capacity() && !m_storage->keys.slot_live(m_slot)) { ++m_slot; }
            }

            const detail::SetStorage *m_storage{nullptr};
            const ValueTypeMetaData  *m_element_type{nullptr};
            size_t                    m_slot{0};
        };

        [[nodiscard]] iterator begin() const noexcept {
            return iterator(static_cast<const detail::SetStorage *>(data()), type()->element_type, 0);
        }

        [[nodiscard]] iterator end() const noexcept {
            const auto *storage = static_cast<const detail::SetStorage *>(data());
            return iterator(storage, type()->element_type, storage->keys.slot_capacity());
        }
    };

    struct MapView : ValueView
    {
        using ValueView::ValueView;

        explicit MapView(ValueView view) noexcept : ValueView(view) {}

        [[nodiscard]] size_t                   size() const { return static_cast<const detail::MapStorage *>(data())->keys.size(); }
        [[nodiscard]] bool                     empty() const { return size() == 0; }
        [[nodiscard]] const ValueTypeMetaData *key_type() const noexcept { return type()->key_type; }
        [[nodiscard]] const ValueTypeMetaData *value_type() const noexcept { return type()->element_type; }

        [[nodiscard]] bool contains(const ValueView &key) const {
            detail::require_child_value(key, key_type(), "MapView::contains()");
            return static_cast<const detail::MapStorage *>(data())->contains(key.data());
        }

        [[nodiscard]] ValueView at(const ValueView &key) const {
            detail::require_child_value(key, key_type(), "MapView::at()");
            auto *value_memory = static_cast<detail::MapStorage *>(const_cast<void *>(data()))->value_at(key.data());
            if (value_memory == nullptr) { throw std::out_of_range("MapView key not found"); }
            return detail::child_view(value_memory, value_type());
        }

        void set(const ValueView &key, const ValueView &value) {
            detail::require_child_value(key, key_type(), "MapView::set()");
            detail::require_child_value(value, value_type(), "MapView::set()");
            static_cast<detail::MapStorage *>(data())->set_item(key.data(), value.data());
        }

        [[nodiscard]] bool add(const ValueView &key, const ValueView &value) {
            if (contains(key)) { return false; }
            set(key, value);
            return true;
        }

        [[nodiscard]] bool remove(const ValueView &key) {
            detail::require_child_value(key, key_type(), "MapView::remove()");
            return static_cast<detail::MapStorage *>(data())->remove(key.data());
        }

        void               clear() { static_cast<detail::MapStorage *>(data())->clear(); }
        void               begin_mutation() { static_cast<detail::MapStorage *>(data())->keys.begin_mutation(); }
        void               end_mutation() { static_cast<detail::MapStorage *>(data())->keys.end_mutation(); }
        void               erase_pending() { static_cast<detail::MapStorage *>(data())->keys.erase_pending(); }
        [[nodiscard]] bool has_pending_erase() const {
            return static_cast<const detail::MapStorage *>(data())->keys.has_pending_erase();
        }

        struct entry
        {
            ValueView key{};
            ValueView value{};
        };

        class iterator
        {
          public:
            iterator() = default;

            iterator(const detail::MapStorage *storage, size_t slot) noexcept : m_storage(storage), m_slot(slot) {
                advance_to_live();
            }

            [[nodiscard]] entry operator*() const {
                return entry{
                    .key   = detail::child_view(const_cast<void *>(m_storage->keys[m_slot]), m_key_type),
                    .value = detail::child_view(m_storage->values.value_memory(m_slot), m_value_type),
                };
            }

            iterator &operator++() {
                ++m_slot;
                advance_to_live();
                return *this;
            }

            [[nodiscard]] bool operator==(const iterator &other) const noexcept {
                return m_storage == other.m_storage && m_slot == other.m_slot;
            }

            [[nodiscard]] bool operator!=(const iterator &other) const noexcept { return !(*this == other); }

            const ValueTypeMetaData *m_key_type{nullptr};
            const ValueTypeMetaData *m_value_type{nullptr};

          private:
            void advance_to_live() noexcept {
                if (m_storage == nullptr) { return; }
                while (m_slot < m_storage->keys.slot_capacity() && !m_storage->keys.slot_live(m_slot)) { ++m_slot; }
            }

            const detail::MapStorage *m_storage{nullptr};
            size_t                    m_slot{0};
        };

        [[nodiscard]] iterator begin() const noexcept {
            iterator it(static_cast<const detail::MapStorage *>(data()), 0);
            it.m_key_type   = key_type();
            it.m_value_type = value_type();
            return it;
        }

        [[nodiscard]] iterator end() const noexcept {
            iterator it(static_cast<const detail::MapStorage *>(data()),
                        static_cast<const detail::MapStorage *>(data())->keys.slot_capacity());
            it.m_key_type   = key_type();
            it.m_value_type = value_type();
            return it;
        }
    };

    inline std::optional<TupleView> ValueView::try_as_tuple() const {
        if (!has_value() || !is_tuple()) { return std::nullopt; }
        return TupleView{*this};
    }

    inline std::optional<BundleView> ValueView::try_as_bundle() const {
        if (!has_value() || !is_bundle()) { return std::nullopt; }
        return BundleView{*this};
    }

    inline std::optional<ListView> ValueView::try_as_list() const {
        if (!has_value() || !is_list()) { return std::nullopt; }
        return ListView{*this};
    }

    inline std::optional<SetView> ValueView::try_as_set() const {
        if (!has_value() || !is_set()) { return std::nullopt; }
        return SetView{*this};
    }

    inline std::optional<MapView> ValueView::try_as_map() const {
        if (!has_value() || !is_map()) { return std::nullopt; }
        return MapView{*this};
    }

    inline std::optional<CyclicBufferView> ValueView::try_as_cyclic_buffer() const {
        if (!has_value() || !is_cyclic_buffer()) { return std::nullopt; }
        return CyclicBufferView{*this};
    }

    inline std::optional<QueueView> ValueView::try_as_queue() const {
        if (!has_value() || !is_queue()) { return std::nullopt; }
        return QueueView{*this};
    }

    inline TupleView ValueView::as_tuple() const {
        if (auto view = try_as_tuple(); view) { return *view; }
        throw std::logic_error("ValueView::as_tuple() requires a tuple value");
    }

    inline BundleView ValueView::as_bundle() const {
        if (auto view = try_as_bundle(); view) { return *view; }
        throw std::logic_error("ValueView::as_bundle() requires a bundle value");
    }

    inline ListView ValueView::as_list() const {
        if (auto view = try_as_list(); view) { return *view; }
        throw std::logic_error("ValueView::as_list() requires a list value");
    }

    inline SetView ValueView::as_set() const {
        if (auto view = try_as_set(); view) { return *view; }
        throw std::logic_error("ValueView::as_set() requires a set value");
    }

    inline MapView ValueView::as_map() const {
        if (auto view = try_as_map(); view) { return *view; }
        throw std::logic_error("ValueView::as_map() requires a map value");
    }

    inline CyclicBufferView ValueView::as_cyclic_buffer() const {
        if (auto view = try_as_cyclic_buffer(); view) { return *view; }
        throw std::logic_error("ValueView::as_cyclic_buffer() requires a cyclic buffer value");
    }

    inline QueueView ValueView::as_queue() const {
        if (auto view = try_as_queue(); view) { return *view; }
        throw std::logic_error("ValueView::as_queue() requires a queue value");
    }
}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_SPECIALIZED_VIEWS_H
