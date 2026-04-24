// Since OSX does not fully support c++23, this is a temporary solution for scope implementations.
#ifndef HGRAPH_CPP_ENGINE_SCOPE_H
#define HGRAPH_CPP_ENGINE_SCOPE_H

#include <exception>
#include <utility>

namespace hgraph {
    template<class F>
    class scope_exit {
    public:
        explicit scope_exit(F &&f) noexcept : fn_(std::move(f)), active_(true) {
        }

        scope_exit(scope_exit &&other) noexcept : fn_(std::move(other.fn_)), active_(other.active_) { other.release(); }

        scope_exit(const scope_exit &) = delete;

        scope_exit &operator=(const scope_exit &) = delete;

        scope_exit &operator=(scope_exit &&) = delete;

        ~scope_exit() {
            if (active_) { fn_(); }
        }

        void release() noexcept { active_ = false; }

    private:
        F fn_;
        bool active_;
    };

    // helper deduction:
    template<class F>
    scope_exit<F> make_scope_exit(F &&f) { return scope_exit<F>(std::forward<F>(f)); }

    // Runs a cleanup action exactly once. If complete() is not called and the
    // scope exits due to exception unwinding, the cleanup is invoked from the
    // destructor and any cleanup failure is suppressed. If complete() is
    // called explicitly, cleanup failures are allowed to propagate normally.
    template <class F>
    class UnwindCleanupGuard
    {
      public:
        explicit UnwindCleanupGuard(F f) noexcept
            : fn_(std::move(f)), uncaught_exceptions_(std::uncaught_exceptions())
        {
        }

        UnwindCleanupGuard(const UnwindCleanupGuard &) = delete;
        UnwindCleanupGuard &operator=(const UnwindCleanupGuard &) = delete;

        void complete()
        {
            if (!active_) { return; }
            active_ = false;
            fn_();
        }

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

    // Captures the first exception thrown across a sequence of best-effort
    // operations while still allowing later operations to run. Call
    // rethrow_if_any() after all cleanup steps have been attempted.
    class FirstExceptionRecorder
    {
      public:
        template <class F>
        void capture(F &&f) noexcept
        {
            try {
                std::forward<F>(f)();
            } catch (...) {
                if (first_exception_ == nullptr) { first_exception_ = std::current_exception(); }
            }
        }

        void rethrow_if_any() const
        {
            if (first_exception_ != nullptr) { std::rethrow_exception(first_exception_); }
        }

      private:
        std::exception_ptr first_exception_;
    };
} // namespace hgraph
#endif  // HGRAPH_CPP_ENGINE_SCOPE_H
