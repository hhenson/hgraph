#pragma once

#include <concepts>
#include <cstddef>
#include <iterator>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

namespace hgraph {

/**
 * Type-erased iterable for collection APIs.
 *
 * State implementations must provide:
 * - bool at_end() const
 * - void next()
 * - T value() const
 *
 * Optional:
 * - size_t size() const
 */
template <typename T>
class TSIterable {
public:
    class iterator;

    using value_type = T;
    using sentinel = std::default_sentinel_t;

    TSIterable() = default;

    TSIterable(std::vector<T> values) {
        struct VectorState {
            std::shared_ptr<std::vector<T>> values;
            size_t index{0};

            [[nodiscard]] bool at_end() const {
                return !values || index >= values->size();
            }

            void next() { ++index; }

            [[nodiscard]] T value() const { return TSIterable::clone_element((*values)[index]); }

            [[nodiscard]] size_t size() const {
                return values ? values->size() : 0;
            }
        };

        auto shared = std::make_shared<std::vector<T>>(std::move(values));
        *this = from_state(VectorState{std::move(shared), 0});
    }

    TSIterable(const TSIterable& other) : vtable_(other.vtable_) {
        if (vtable_ != nullptr && other.state_ != nullptr) {
            state_ = vtable_->clone(other.state_);
        }
    }

    TSIterable(TSIterable&& other) noexcept
        : vtable_(other.vtable_), state_(other.state_) {
        other.vtable_ = nullptr;
        other.state_ = nullptr;
    }

    TSIterable& operator=(const TSIterable& other) {
        if (this == &other) {
            return *this;
        }
        reset();
        vtable_ = other.vtable_;
        if (vtable_ != nullptr && other.state_ != nullptr) {
            state_ = vtable_->clone(other.state_);
        }
        return *this;
    }

    TSIterable& operator=(TSIterable&& other) noexcept {
        if (this == &other) {
            return *this;
        }
        reset();
        vtable_ = other.vtable_;
        state_ = other.state_;
        other.vtable_ = nullptr;
        other.state_ = nullptr;
        return *this;
    }

    ~TSIterable() { reset(); }

    [[nodiscard]] iterator begin() const {
        if (vtable_ == nullptr || state_ == nullptr) {
            return iterator{};
        }
        return iterator(vtable_, vtable_->clone(state_));
    }

    [[nodiscard]] sentinel end() const noexcept { return {}; }

    [[nodiscard]] size_t size() const {
        if (vtable_ == nullptr || state_ == nullptr) {
            return 0;
        }
        if (vtable_->size != nullptr) {
            return vtable_->size(state_);
        }

        size_t out = 0;
        void* cursor = vtable_->clone(state_);
        while (!vtable_->at_end(cursor)) {
            ++out;
            vtable_->next(cursor);
        }
        vtable_->destroy(cursor);
        return out;
    }

    [[nodiscard]] bool empty() const {
        if (vtable_ == nullptr || state_ == nullptr) {
            return true;
        }
        return vtable_->at_end(state_);
    }

    template <typename State>
    [[nodiscard]] static TSIterable from_state(State state) {
        TSIterable out;
        out.vtable_ = &vtable_for<State>();
        out.state_ = new State(std::move(state));
        return out;
    }

private:
    template <typename U>
    struct is_std_pair : std::false_type {};

    template <typename A, typename B>
    struct is_std_pair<std::pair<A, B>> : std::true_type {};

    template <typename U>
    static U clone_element(const U& value) {
        if constexpr (std::is_copy_constructible_v<U>) {
            return value;
        } else if constexpr (requires(const U& v) { v.view(); }) {
            return U(value.view().clone());
        } else if constexpr (is_std_pair<U>::value) {
            return U{
                clone_element<typename U::first_type>(value.first),
                clone_element<typename U::second_type>(value.second)};
        } else {
            static_assert(std::is_copy_constructible_v<U>,
                          "TSIterable vector fallback requires copyable element or clone support");
        }
    }

    template <typename State>
    static consteval bool has_size_method() {
        return requires(const State& s) {
            { s.size() } -> std::convertible_to<size_t>;
        };
    }

    struct VTable {
        void (*destroy)(void*);
        void* (*clone)(const void*);
        bool (*at_end)(const void*);
        void (*next)(void*);
        T (*value)(void*);
        size_t (*size)(const void*);
    };

    template <typename State>
    static size_t size_dispatch(const void* p) {
        return static_cast<const State*>(p)->size();
    }

    template <typename State>
    static consteval size_t (*size_fn_for())(const void*) {
        if constexpr (has_size_method<State>()) {
            return &size_dispatch<State>;
        } else {
            return nullptr;
        }
    }

    template <typename State>
    [[nodiscard]] static const VTable& vtable_for() {
        static const VTable table{
            [](void* p) { delete static_cast<State*>(p); },
            [](const void* p) -> void* { return new State(*static_cast<const State*>(p)); },
            [](const void* p) { return static_cast<const State*>(p)->at_end(); },
            [](void* p) { static_cast<State*>(p)->next(); },
            [](void* p) { return static_cast<State*>(p)->value(); },
            size_fn_for<State>()};
        return table;
    }

    void reset() {
        if (vtable_ != nullptr && state_ != nullptr) {
            vtable_->destroy(state_);
        }
        vtable_ = nullptr;
        state_ = nullptr;
    }

    const VTable* vtable_{nullptr};
    void* state_{nullptr};
};

template <typename T>
class TSIterable<T>::iterator {
public:
    using iterator_category = std::input_iterator_tag;
    using value_type = T;
    using difference_type = std::ptrdiff_t;
    using pointer = void;
    using reference = T;

    iterator() = default;

    iterator(const VTable* vtable, void* state) : vtable_(vtable), state_(state) {}

    iterator(const iterator& other) : vtable_(other.vtable_) {
        if (vtable_ != nullptr && other.state_ != nullptr) {
            state_ = vtable_->clone(other.state_);
        }
    }

    iterator(iterator&& other) noexcept
        : vtable_(other.vtable_), state_(other.state_) {
        other.vtable_ = nullptr;
        other.state_ = nullptr;
    }

    iterator& operator=(const iterator& other) {
        if (this == &other) {
            return *this;
        }
        reset();
        vtable_ = other.vtable_;
        if (vtable_ != nullptr && other.state_ != nullptr) {
            state_ = vtable_->clone(other.state_);
        }
        return *this;
    }

    iterator& operator=(iterator&& other) noexcept {
        if (this == &other) {
            return *this;
        }
        reset();
        vtable_ = other.vtable_;
        state_ = other.state_;
        other.vtable_ = nullptr;
        other.state_ = nullptr;
        return *this;
    }

    ~iterator() { reset(); }

    [[nodiscard]] T operator*() const { return vtable_->value(state_); }

    iterator& operator++() {
        vtable_->next(state_);
        return *this;
    }

    void operator++(int) { ++(*this); }

    [[nodiscard]] bool operator==(sentinel) const {
        return vtable_ == nullptr || state_ == nullptr || vtable_->at_end(state_);
    }

private:
    void reset() {
        if (vtable_ != nullptr && state_ != nullptr) {
            vtable_->destroy(state_);
        }
        vtable_ = nullptr;
        state_ = nullptr;
    }

    const VTable* vtable_{nullptr};
    void* state_{nullptr};
};

}  // namespace hgraph
