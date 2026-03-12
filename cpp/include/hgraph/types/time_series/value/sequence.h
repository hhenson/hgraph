#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/value/view.h>

#include <cstddef>
#include <type_traits>
#include <utility>

namespace hgraph
{

    struct ValueBuilder;

    namespace detail
    {

        /**
         * Behavior-only dispatch shared by sequence storage that exposes
         * buffer-style indexed access and push/pop mutation.
         */
        struct BufferViewDispatch : ViewDispatch
        {
            [[nodiscard]] virtual size_t size(const void *data) const noexcept = 0;
            [[nodiscard]] virtual const value::TypeMeta &element_schema() const noexcept = 0;
            [[nodiscard]] virtual const ViewDispatch &element_dispatch() const noexcept = 0;
            [[nodiscard]] virtual void *element_data(void *data, size_t index) const = 0;
            [[nodiscard]] virtual const void *element_data(const void *data, size_t index) const = 0;
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

        [[nodiscard]] HGRAPH_EXPORT const ValueBuilder *sequence_builder_for(const value::TypeMeta *schema);

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

        [[nodiscard]] size_t size() const;
        [[nodiscard]] bool empty() const;
        [[nodiscard]] const value::TypeMeta *element_schema() const;
        [[nodiscard]] View front();
        [[nodiscard]] View front() const;
        [[nodiscard]] View back();
        [[nodiscard]] View back() const;
        void push(const View &value);
        void pop();
        void clear();

      protected:
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

        [[nodiscard]] size_t capacity() const;
        [[nodiscard]] bool full() const;
        [[nodiscard]] View at(size_t index);
        [[nodiscard]] View at(size_t index) const;
        [[nodiscard]] View operator[](size_t index);
        [[nodiscard]] View operator[](size_t index) const;
        void set(size_t index, const View &value);

      private:
        [[nodiscard]] const detail::CyclicBufferViewDispatch *cyclic_dispatch() const noexcept;
    };

    /**
     * Non-owning typed wrapper over a queue value.
     */
    struct HGRAPH_EXPORT QueueView : BufferView
    {
        explicit QueueView(const View &view);

        [[nodiscard]] size_t max_capacity() const;
        [[nodiscard]] bool has_max_capacity() const noexcept;

      private:
        [[nodiscard]] const detail::QueueViewDispatch *queue_dispatch() const noexcept;
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
