// Since OSX does not fully support c++23, this is a temporary solution for scope implementations.
#ifndef HGRAPH_CPP_ENGINE_SCOPE_H
#define HGRAPH_CPP_ENGINE_SCOPE_H

#include <exception>
#include <type_traits>
#include <utility>

namespace hgraph {
    /**
     * RAII helper that runs a cleanup function on scope exit unless explicitly
     * released. Equivalent to ``std::experimental::scope_exit`` from the C++23
     * Library Fundamentals TS, provided here so the runtime does not depend on
     * a TS implementation.
     *
     * Construct via ``make_scope_exit`` so the function type is deduced.
     * The optional ``HideExceptions`` template parameter makes destruction
     * suppress cleanup failures. Use that only for best-effort rollback during
     * an already-failing path where a cleanup exception must not replace the
     * primary failure.
     */
    template<class F, bool HideExceptions = false>
    class scope_exit {
    public:
        /** Capture ``f`` and arm the guard. */
        explicit scope_exit(F f) noexcept(std::is_nothrow_move_constructible_v<F>) : fn_(std::move(f)), active_(true) {
        }

        /** Move construction transfers ownership of the cleanup; the source is released. */
        scope_exit(scope_exit &&other) noexcept(std::is_nothrow_move_constructible_v<F>)
            : fn_(std::move(other.fn_)), active_(other.active_) { other.release(); }

        scope_exit(const scope_exit &) = delete;

        scope_exit &operator=(const scope_exit &) = delete;

        scope_exit &operator=(scope_exit &&) = delete;

        /** Invoke the cleanup if still armed. */
        ~scope_exit() noexcept(HideExceptions || std::is_nothrow_invocable_v<F &>) {
            if (!active_) { return; }
            if constexpr (HideExceptions) {
                try {
                    fn_();
                } catch (...) {
                }
            } else {
                fn_();
            }
        }

        /** Disarm the guard so the destructor does not run the cleanup. */
        void release() noexcept { active_ = false; }

    private:
        F fn_;
        bool active_;
    };

    /** Deduction helper that constructs a ``scope_exit`` from a callable. */
    template<bool HideExceptions = false, class F>
    scope_exit<std::decay_t<F>, HideExceptions> make_scope_exit(F &&f)
    {
        return scope_exit<std::decay_t<F>, HideExceptions>(std::forward<F>(f));
    }

    /**
     * Run ``f`` and return its result. If ``f`` throws, suppress the
     * exception and return ``fallback`` instead.
     *
     * Use this only at deliberate exception boundaries where the fallback is a
     * valid domain result, such as a comparison operation returning
     * ``std::partial_ordering::unordered`` when a structural fallback cannot be
     * evaluated.
     */
    /**
     * Run ``f`` and return its result. If ``f`` throws, invoke ``annotate``
     * from inside the catch — typically a ``[[noreturn]]`` helper that
     * rethrows the in-flight exception enriched with context (e.g. a node
     * identity prefix). Should ``annotate`` return, the original exception
     * propagates unchanged. The annotate-and-propagate counterpart of
     * ``fallback_on_exception`` — call sites stay expression-shaped instead
     * of hand-rolled try/catch blocks.
     */
    template <typename F, typename Annotate>
    decltype(auto) annotate_on_exception(F &&f, Annotate &&annotate)
    {
        try {
            return std::forward<F>(f)();
        } catch (...) {
            std::forward<Annotate>(annotate)();
            throw;
        }
    }

    /** Typed annotation boundary. Only ``Exception`` is intercepted, and the
     *  caught value is provided to ``annotate``. If annotation returns, the
     *  original exception continues to propagate. */
    template <typename Exception, typename F, typename Annotate>
    decltype(auto) annotate_on_exception(F &&f, Annotate &&annotate)
    {
        try {
            return std::forward<F>(f)();
        } catch (const Exception &error) {
            std::forward<Annotate>(annotate)(error);
            throw;
        }
    }

    template <typename Result, typename F>
    [[nodiscard]] Result fallback_on_exception(Result fallback, F &&f) noexcept
    {
        try {
            return std::forward<F>(f)();
        } catch (...) {
            return fallback;
        }
    }

    /**
     * Run ``f`` and return its result. If ``f`` throws, call ``on_error`` with
     * either the ``std::exception::what()`` message or ``"unknown error"``,
     * suppress the exception, and return ``fallback``.
     */
    template <typename Result, typename F, typename OnError>
    [[nodiscard]] Result fallback_on_exception(Result fallback, F &&f, OnError &&on_error)
    {
        try {
            return std::forward<F>(f)();
        } catch (const std::exception &error) {
            std::forward<OnError>(on_error)(error.what());
            return fallback;
        } catch (...) {
            std::forward<OnError>(on_error)("unknown error");
            return fallback;
        }
    }

    /**
     * Single-use cleanup guard that distinguishes normal completion from
     * exception unwinding.
     *
     * Call ``complete()`` to finalise the protected operation: this runs the
     * cleanup once and lets exceptions propagate. If ``complete()`` is not
     * called and the scope exits while a new exception is in flight, the
     * destructor invokes the cleanup and swallows any failure so it does not
     * mask the original error. ``release()`` cancels the cleanup entirely.
     */
    template <class F>
    class UnwindCleanupGuard
    {
      public:
        /** Capture ``f`` and snapshot the current uncaught-exception count. */
        explicit UnwindCleanupGuard(F f) noexcept
            : fn_(std::move(f)), uncaught_exceptions_(std::uncaught_exceptions())
        {
        }

        UnwindCleanupGuard(const UnwindCleanupGuard &) = delete;
        UnwindCleanupGuard &operator=(const UnwindCleanupGuard &) = delete;

        /** Run the cleanup now, allowing any failure to propagate. */
        void complete()
        {
            if (!active_) { return; }
            active_ = false;
            fn_();
        }

        /** Disarm the guard; the cleanup will not run on destruction. */
        void release() noexcept
        {
            active_ = false;
        }

        ~UnwindCleanupGuard() noexcept
        {
            if (!active_ || std::uncaught_exceptions() <= uncaught_exceptions_) { return; }

            try {
                fn_();
            } catch (...) {
            }
        }

      private:
        F fn_;
        int uncaught_exceptions_{0};
        bool active_{true};
    };

    template <class F>
    UnwindCleanupGuard(F) -> UnwindCleanupGuard<F>;

    /**
     * Records the first exception thrown across a sequence of best-effort
     * operations while still letting later operations run. Call
     * ``rethrow_if_any()`` once all cleanup steps have been attempted to
     * surface any captured failure.
     */
    class FirstExceptionRecorder
    {
      public:
        /** Run ``f`` and remember the first exception it throws (if any). */
        template <class F>
        void capture(F &&f) noexcept
        {
            try {
                std::forward<F>(f)();
            } catch (...) {
                if (first_exception_ == nullptr) { first_exception_ = std::current_exception(); }
            }
        }

        /** Rethrow the first captured exception, if there is one. */
        void rethrow_if_any() const
        {
            if (first_exception_ != nullptr) { std::rethrow_exception(first_exception_); }
        }

        [[nodiscard]] bool has_exception() const noexcept
        {
            return first_exception_ != nullptr;
        }

      private:
        std::exception_ptr first_exception_;
    };
} // namespace hgraph
#endif  // HGRAPH_CPP_ENGINE_SCOPE_H
