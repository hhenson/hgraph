#pragma once

#include <hgraph/types/node.h>

#include <optional>
#include <type_traits>
#include <variant>

namespace hgraph {

    /**
     * Shared visitor-visible base for template-based C++ compute nodes.
     * NodeImpl<Spec> derives from this so the visitor table only needs one extra concrete family type.
     */
    struct HGRAPH_EXPORT CppSpecNode : Node {
        using Node::Node;
        VISITOR_SUPPORT()
    };

    namespace cpp_node_detail {
        template<typename Spec, typename = void>
        struct spec_state {
            using type = std::monostate;
        };

        template<typename Spec>
        struct spec_state<Spec, std::void_t<typename Spec::state>> {
            using type = typename Spec::state;
        };

        template<typename Spec>
        using spec_state_t = typename spec_state<Spec>::type;

        template<typename>
        inline constexpr bool always_false_v = false;

        template<typename Spec>
        inline constexpr bool has_make_state_v =
            requires(Node& node) {
                { Spec::make_state(node) } -> std::convertible_to<spec_state_t<Spec>>;
            };

        template<typename Spec>
        inline void call_init(Node& node, spec_state_t<Spec>& state) {
            if constexpr (requires(Node& n, spec_state_t<Spec>& s) { Spec::init(n, s); }) {
                Spec::init(node, state);
            } else if constexpr (requires(Node& n) { Spec::init(n); }) {
                Spec::init(node);
            }
        }

        template<typename Spec>
        inline void call_start(Node& node, spec_state_t<Spec>& state) {
            if constexpr (requires(Node& n, spec_state_t<Spec>& s) { Spec::start(n, s); }) {
                Spec::start(node, state);
            } else if constexpr (requires(Node& n) { Spec::start(n); }) {
                Spec::start(node);
            }
        }

        template<typename Spec>
        inline void call_stop(Node& node, spec_state_t<Spec>& state) {
            if constexpr (requires(Node& n, spec_state_t<Spec>& s) { Spec::stop(n, s); }) {
                Spec::stop(node, state);
            } else if constexpr (requires(Node& n) { Spec::stop(n); }) {
                Spec::stop(node);
            }
        }

        template<typename Spec>
        inline void call_dispose(Node& node, spec_state_t<Spec>& state) {
            if constexpr (requires(Node& n, spec_state_t<Spec>& s) { Spec::dispose(n, s); }) {
                Spec::dispose(node, state);
            } else if constexpr (requires(Node& n) { Spec::dispose(n); }) {
                Spec::dispose(node);
            }
        }

        template<typename Spec>
        inline void call_eval(Node& node, spec_state_t<Spec>& state) {
            if constexpr (requires(Node& n, spec_state_t<Spec>& s) { Spec::eval(n, s); }) {
                Spec::eval(node, state);
            } else if constexpr (requires(Node& n) { Spec::eval(n); }) {
                Spec::eval(node);
            } else {
                static_assert(always_false_v<Spec>,
                              "Spec must define static eval(Node&) or static eval(Node&, state&)");
            }
        }
    }  // namespace cpp_node_detail

    /**
     * Generic compute-node implementation backed by a lightweight Spec.
     * Spec provides required eval hook and optional lifecycle/state hooks.
     */
    template<typename Spec>
    struct NodeImpl final : CppSpecNode {
        using state_type = cpp_node_detail::spec_state_t<Spec>;
        using CppSpecNode::CppSpecNode;

      protected:
        void initialise() override {
            static_assert(cpp_node_detail::has_make_state_v<Spec> || std::is_default_constructible_v<state_type>,
                          "Spec::state must be default constructible or Spec::make_state(Node&) must be provided");
            auto& state = ensure_state();
            cpp_node_detail::call_init<Spec>(*this, state);
        }

        void start() override {
            _initialise_inputs();
            Node::start();
        }

        void do_start() override {
            cpp_node_detail::call_start<Spec>(*this, ensure_state());
        }

        void do_stop() override {
            if (!state_.has_value()) {
                return;
            }
            cpp_node_detail::call_stop<Spec>(*this, *state_);
        }

        void do_eval() override {
            cpp_node_detail::call_eval<Spec>(*this, ensure_state());
        }

        void dispose() override {
            if (!state_.has_value()) {
                return;
            }
            cpp_node_detail::call_dispose<Spec>(*this, *state_);
            state_.reset();
        }

      private:
        state_type& ensure_state() {
            if (!state_.has_value()) {
                if constexpr (cpp_node_detail::has_make_state_v<Spec>) {
                    state_.emplace(Spec::make_state(*this));
                } else {
                    state_.emplace();
                }
            }
            return *state_;
        }

        std::optional<state_type> state_{};
    };
}  // namespace hgraph
