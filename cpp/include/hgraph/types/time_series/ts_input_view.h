#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/notifiable.h>
#include <hgraph/types/time_series/active_trie.h>
#include <hgraph/types/time_series/value/atomic.h>
#include <hgraph/types/time_series/ts_view.h>

namespace hgraph {

struct TSOutputView;

/**
 * Input-specialized instantiation of the generic time-series view surface.
 *
 * `TSInputView` is intended to expose input-only behavior, especially binding
 * and activation control, while reusing the shared time-series navigation
 * contract.
 *
 * Activation is path-local by design. A parent input may remain passive while
 * one of its descendants is active. The concrete activation and binding
 * behavior lives behind runtime ops carried on the view context, so this type
 * remains a thin endpoint-specific facade over the shared TS navigation model.
 */
struct HGRAPH_EXPORT TSInputView : TSView<TSInputView> {
    TSInputView() = default;
    TSInputView(TSViewContext context,
                TSViewContext parent,
                engine_time_t evaluation_time,
                TSInput *owning_input = nullptr,
                ActiveTriePosition active_pos = {},
                Notifiable *scheduling_notifier = nullptr,
                const detail::TSInputViewOps *input_view_ops = nullptr) noexcept
        : TSView<TSInputView>(context, parent, evaluation_time),
          m_owning_input(owning_input),
          m_active_pos(std::move(active_pos)),
          m_scheduling_notifier(scheduling_notifier),
          m_input_view_ops(input_view_ops)
    {
    }

    /**
     * Construct a navigable input view over TS storage.
     *
     * This is the wiring-time/runtime navigation surface used for collection
     * access and binding.
     */
    ~TSInputView() = default;

    /**
     * Bind this input position to an output.
     *
     * Native input state is upgraded to a target-link state on demand when
     * dynamic rebinding requires it.
     */
    void bind_output(const TSOutputView &output);

    /**
     * Remove any current output binding from this input position.
     */
    void unbind_output();

    /**
     * Mark the input view as active.
     *
     * This is intended to enable active observation for the represented input
     * position so upstream notifications reach the owning input endpoint.
     * Activity is local to this path and does not imply parent activation.
     */
    void make_active();

    /**
     * Mark the input view as passive.
     *
     * This is intended to disable active observation for the represented input
     * position so upstream notifications are no longer requested.
     */
    void make_passive();

    /**
     * Return whether the input view is currently active.
     *
     * This is intended to report whether the represented input position is
     * currently participating in active observation.
     */
    [[nodiscard]] bool active() const;

    /**
     * Internal helper used by collection wrappers to preserve input-specific
     * runtime state when navigating to a child TS position.
     */
    [[nodiscard]] TSInputView make_child_view_impl(TSViewContext context,
                                                   TSViewContext parent,
                                                   engine_time_t evaluation_time) const;

    [[nodiscard]] TSViewContext &context_mutable() noexcept { return this->m_context; }
    [[nodiscard]] const TSViewContext &context_ref() const noexcept { return this->m_context; }
    [[nodiscard]] const TSViewContext &parent_context_ref() const noexcept { return this->m_parent; }
    [[nodiscard]] TSInput *owning_input() const noexcept { return m_owning_input; }
    [[nodiscard]] ActiveTriePosition &active_position_mutable() noexcept { return m_active_pos; }
    [[nodiscard]] const ActiveTriePosition &active_position() const noexcept { return m_active_pos; }
    [[nodiscard]] Notifiable *scheduling_notifier() const noexcept { return m_scheduling_notifier; }
    [[nodiscard]] const detail::TSInputViewOps *input_view_ops() const noexcept { return m_input_view_ops; }

  private:
    TSInput *m_owning_input{nullptr};
    ActiveTriePosition m_active_pos;
    Notifiable *m_scheduling_notifier{nullptr};
    const detail::TSInputViewOps *m_input_view_ops{nullptr};
};

struct TSSInputView : TSSReadView<TSInputView>
{
    using TSSReadView<TSInputView>::TSSReadView;
    TSSInputView() = default;
};

}  // namespace hgraph
