#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/value/tracking.h>
#include <hgraph/types/time_series/value/view.h>

#include <cstddef>
#include <type_traits>
#include <utility>

namespace hgraph
{

    struct BufferMutationView;
    struct CyclicBufferMutationView;
    struct QueueMutationView;

    struct ValueBuilder;

    namespace detail
    {

        /**
         * Behavior-only dispatch shared by sequence storage that exposes
         * buffer-style indexed access and push/pop mutation.
         */
        struct BufferViewDispatch : ViewDispatch
        {
            virtual void begin_mutation(void *data) const = 0;
            virtual void end_mutation(void *data) const = 0;
            [[nodiscard]] virtual size_t size(const void *data) const noexcept = 0;
            [[nodiscard]] virtual const value::TypeMeta &element_schema() const noexcept = 0;
            [[nodiscard]] virtual const ViewDispatch &element_dispatch() const noexcept = 0;
            [[nodiscard]] virtual void *element_data(void *data, size_t index) const = 0;
            [[nodiscard]] virtual const void *element_data(const void *data, size_t index) const = 0;
            [[nodiscard]] virtual bool has_removed(const void *data) const noexcept = 0;
            [[nodiscard]] virtual void *removed_data(void *data) const = 0;
            [[nodiscard]] virtual const void *removed_data(const void *data) const = 0;
            virtual void push(void *data, const void *value) const = 0;
            virtual void pop(void *data) const = 0;
            virtual void clear(void *data) const = 0;
        };

        /**
         * Behavior-only dispatch for cyclic buffer storage.
         */
        struct CyclicBufferViewDispatch : BufferViewDispatch
        {
            [[nodiscard]] virtual size_t capacity() const noexcept = 0;
            virtual void set_at(void *data, size_t index, const void *value) const = 0;
        };

        /**
         * Behavior-only dispatch for queue storage.
         */
        struct QueueViewDispatch : BufferViewDispatch
        {
            [[nodiscard]] virtual size_t max_capacity() const noexcept = 0;
        };

        [[nodiscard]] HGRAPH_EXPORT const ValueBuilder *sequence_builder_for(
            const value::TypeMeta *schema, MutationTracking tracking);

    }  // namespace detail

    /**
     * Non-owning typed wrapper over buffer-like sequence values.
     *
     * This captures the API shared by cyclic buffers and queues so common
     * navigation and mutation semantics remain defined in one place.
     */
    struct HGRAPH_EXPORT BufferView : View
    {
        explicit BufferView(const View &view);

        /**
         * Return the mutable surface for this buffer view.
         *
         * Buffer mutation scopes manage retained removed payloads so the last
         * delta element remains inspectable until the next outermost mutation
         * scope begins.
         */
        BufferMutationView begin_mutation();
        [[nodiscard]] size_t size() const;
        [[nodiscard]] bool empty() const;
        [[nodiscard]] const value::TypeMeta *element_schema() const;
        [[nodiscard]] bool has_removed() const;
        [[nodiscard]] View removed();
        [[nodiscard]] View removed() const;
        [[nodiscard]] View front();
        [[nodiscard]] View front() const;
        [[nodiscard]] View back();
        [[nodiscard]] View back() const;

      protected:
        void begin_mutation_scope();
        void end_mutation_scope() noexcept;
        /**
         * Return the shared sequence dispatch surface for this buffer view.
         */
        [[nodiscard]] const detail::BufferViewDispatch *buffer_dispatch() const noexcept;

        /**
         * Return the element view at the supplied logical buffer index.
         *
         * Indexed access is shared internally by the concrete buffer views, but
         * is not part of the common public buffer contract.
         */
        [[nodiscard]] View element_at(size_t index);
        [[nodiscard]] View element_at(size_t index) const;
    };

    /**
     * Non-owning typed wrapper over a cyclic buffer value.
     */
    struct HGRAPH_EXPORT CyclicBufferView : BufferView
    {
        explicit CyclicBufferView(const View &view);

        CyclicBufferMutationView begin_mutation();
        [[nodiscard]] size_t capacity() const;
        [[nodiscard]] bool full() const;
        [[nodiscard]] View at(size_t index);
        [[nodiscard]] View at(size_t index) const;
        [[nodiscard]] View operator[](size_t index);
        [[nodiscard]] View operator[](size_t index) const;

      protected:
        [[nodiscard]] const detail::CyclicBufferViewDispatch *cyclic_dispatch() const noexcept;
    };

    /**
     * Non-owning typed wrapper over a queue value.
     */
    struct HGRAPH_EXPORT QueueView : BufferView
    {
        explicit QueueView(const View &view);

        QueueMutationView begin_mutation();
        [[nodiscard]] size_t max_capacity() const;
        [[nodiscard]] bool has_max_capacity() const noexcept;

      protected:
        [[nodiscard]] const detail::QueueViewDispatch *queue_dispatch() const noexcept;
    };

    /**
     * Mutable buffer surface shared by cyclic buffers and queues.
     *
     * The mutation scope is RAII-managed so retained removed payloads are
     * released when the outermost scope begins again, matching the
     * single-delta semantics expected by the time-series layer. Delta-tracking
     * buffer implementations keep one extra internal element slot beyond the
     * user-visible logical capacity so the most recently removed payload can
     * remain inspectable for the current delta without disturbing the live
     * contents. Plain builders omit that retained-removed behavior.
     */
    struct HGRAPH_EXPORT BufferMutationView : BufferView
    {
        explicit BufferMutationView(BufferView &view);
        BufferMutationView(const BufferMutationView &) = delete;
        BufferMutationView &operator=(const BufferMutationView &) = delete;
        BufferMutationView(BufferMutationView &&other) noexcept;
        BufferMutationView &operator=(BufferMutationView &&other) = delete;
        ~BufferMutationView();

        /**
         * Push a value onto the logical end of the buffer.
         */
        void push(const View &value);

        template <typename T>
            requires(!std::derived_from<std::remove_cvref_t<T>, View>)
        void push(T &&value)
        {
            auto *dispatch = buffer_dispatch();
            if (dispatch == nullptr) { throw std::runtime_error("BufferMutationView::push on invalid view"); }
            using TValue = std::remove_cvref_t<T>;
            if (&dispatch->element_schema() != value::scalar_type_meta<TValue>()) {
                throw std::invalid_argument("BufferMutationView::push requires a matching atomic element schema");
            }
            if constexpr (std::is_lvalue_reference_v<T &&>) {
                dispatch->push(data(), std::addressof(value));
            } else {
                TValue moved_value = std::forward<T>(value);
                dispatch->push(data(), std::addressof(moved_value));
            }
        }

        /**
         * Push a value and return this mutation view for fluent chains.
         */
        BufferMutationView &pushing(const View &value)
        {
            push(value);
            return *this;
        }

        template <typename T>
            requires(!std::derived_from<std::remove_cvref_t<T>, View>)
        BufferMutationView &pushing(T &&value)
        {
            push(std::forward<T>(value));
            return *this;
        }

        /**
         * Pop the logical front element from the buffer.
         */
        void pop();

        /**
         * Pop the logical front element and return this mutation view for
         * fluent chains.
         */
        BufferMutationView &popping()
        {
            pop();
            return *this;
        }

        /**
         * Clear the buffer contents.
         */
        void clear();

        /**
         * Clear the buffer and return this mutation view for fluent chains.
         */
        BufferMutationView &clearing()
        {
            clear();
            return *this;
        }

      private:
        bool m_owns_scope{true};
    };

    /**
     * Mutable cyclic-buffer surface.
     */
    struct HGRAPH_EXPORT CyclicBufferMutationView : CyclicBufferView
    {
        explicit CyclicBufferMutationView(CyclicBufferView &view);
        CyclicBufferMutationView(const CyclicBufferMutationView &) = delete;
        CyclicBufferMutationView &operator=(const CyclicBufferMutationView &) = delete;
        CyclicBufferMutationView(CyclicBufferMutationView &&other) noexcept;
        CyclicBufferMutationView &operator=(CyclicBufferMutationView &&other) = delete;
        ~CyclicBufferMutationView();

        /**
         * Push a value onto the cyclic buffer.
         */
        void push(const View &value);

        template <typename T>
            requires(!std::derived_from<std::remove_cvref_t<T>, View>)
        void push(T &&value)
        {
            auto *dispatch = buffer_dispatch();
            if (dispatch == nullptr) { throw std::runtime_error("CyclicBufferMutationView::push on invalid view"); }
            using TValue = std::remove_cvref_t<T>;
            if (&dispatch->element_schema() != value::scalar_type_meta<TValue>()) {
                throw std::invalid_argument("CyclicBufferMutationView::push requires a matching atomic element schema");
            }
            if constexpr (std::is_lvalue_reference_v<T &&>) {
                dispatch->push(data(), std::addressof(value));
            } else {
                TValue moved_value = std::forward<T>(value);
                dispatch->push(data(), std::addressof(moved_value));
            }
        }

        /**
         * Push a value and return this mutation view for fluent chains.
         */
        CyclicBufferMutationView &pushing(const View &value)
        {
            push(value);
            return *this;
        }

        template <typename T>
            requires(!std::derived_from<std::remove_cvref_t<T>, View>)
        CyclicBufferMutationView &pushing(T &&value)
        {
            push(std::forward<T>(value));
            return *this;
        }

        /**
         * Pop the oldest value from the cyclic buffer.
         */
        void pop();

        /**
         * Pop the oldest value and return this mutation view for fluent
         * chains.
         */
        CyclicBufferMutationView &popping()
        {
            pop();
            return *this;
        }

        /**
         * Clear the cyclic buffer contents.
         */
        void clear();

        /**
         * Clear the cyclic buffer and return this mutation view for fluent
         * chains.
         */
        CyclicBufferMutationView &clearing()
        {
            clear();
            return *this;
        }

        /**
         * Replace the value at the supplied logical index.
         */
        void set(size_t index, const View &value);

        template <typename T>
            requires(!std::derived_from<std::remove_cvref_t<T>, View>)
        void set(size_t index, T &&value)
        {
            auto *dispatch = cyclic_dispatch();
            if (dispatch == nullptr) { throw std::runtime_error("CyclicBufferMutationView::set on invalid view"); }
            using TValue = std::remove_cvref_t<T>;
            if (&dispatch->element_schema() != value::scalar_type_meta<TValue>()) {
                throw std::invalid_argument("CyclicBufferMutationView::set requires a matching atomic element schema");
            }
            if constexpr (std::is_lvalue_reference_v<T &&>) {
                dispatch->set_at(data(), index, std::addressof(value));
            } else {
                TValue moved_value = std::forward<T>(value);
                dispatch->set_at(data(), index, std::addressof(moved_value));
            }
        }

        /**
         * Replace the value at the supplied logical index and return this
         * mutation view for fluent chains.
         */
        CyclicBufferMutationView &setting(size_t index, const View &value)
        {
            set(index, value);
            return *this;
        }

        template <typename T>
            requires(!std::derived_from<std::remove_cvref_t<T>, View>)
        CyclicBufferMutationView &setting(size_t index, T &&value)
        {
            set(index, std::forward<T>(value));
            return *this;
        }

      private:
        bool m_owns_scope{true};
    };

    /**
     * Mutable queue surface.
     */
    struct HGRAPH_EXPORT QueueMutationView : QueueView
    {
        explicit QueueMutationView(QueueView &view);
        QueueMutationView(const QueueMutationView &) = delete;
        QueueMutationView &operator=(const QueueMutationView &) = delete;
        QueueMutationView(QueueMutationView &&other) noexcept;
        QueueMutationView &operator=(QueueMutationView &&other) = delete;
        ~QueueMutationView();

        /**
         * Push a value onto the logical end of the queue.
         */
        void push(const View &value);

        template <typename T>
            requires(!std::derived_from<std::remove_cvref_t<T>, View>)
        void push(T &&value)
        {
            auto *dispatch = buffer_dispatch();
            if (dispatch == nullptr) { throw std::runtime_error("QueueMutationView::push on invalid view"); }
            using TValue = std::remove_cvref_t<T>;
            if (&dispatch->element_schema() != value::scalar_type_meta<TValue>()) {
                throw std::invalid_argument("QueueMutationView::push requires a matching atomic element schema");
            }
            if constexpr (std::is_lvalue_reference_v<T &&>) {
                dispatch->push(data(), std::addressof(value));
            } else {
                TValue moved_value = std::forward<T>(value);
                dispatch->push(data(), std::addressof(moved_value));
            }
        }

        /**
         * Push a value and return this mutation view for fluent chains.
         */
        QueueMutationView &pushing(const View &value)
        {
            push(value);
            return *this;
        }

        template <typename T>
            requires(!std::derived_from<std::remove_cvref_t<T>, View>)
        QueueMutationView &pushing(T &&value)
        {
            push(std::forward<T>(value));
            return *this;
        }

        /**
         * Pop the logical front element from the queue.
         */
        void pop();

        /**
         * Pop the logical front element and return this mutation view for
         * fluent chains.
         */
        QueueMutationView &popping()
        {
            pop();
            return *this;
        }

        /**
         * Clear the queue contents.
         */
        void clear();

        /**
         * Clear the queue and return this mutation view for fluent chains.
         */
        QueueMutationView &clearing()
        {
            clear();
            return *this;
        }

      private:
        bool m_owns_scope{true};
    };

    inline CyclicBufferView View::as_cyclic_buffer()
    {
        return CyclicBufferView{*this};
    }

    inline CyclicBufferView View::as_cyclic_buffer() const
    {
        return CyclicBufferView{*this};
    }

    inline QueueView View::as_queue()
    {
        return QueueView{*this};
    }

    inline QueueView View::as_queue() const
    {
        return QueueView{*this};
    }

}  // namespace hgraph
