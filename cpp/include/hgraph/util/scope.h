// Since OSX does not fully support c++23, this is a temporary solution for scope implementations.
#ifndef HGRAPH_CPP_ENGINE_SCOPE_H
#define HGRAPH_CPP_ENGINE_SCOPE_H

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
} // namespace hgraph
#endif  // HGRAPH_CPP_ENGINE_SCOPE_H