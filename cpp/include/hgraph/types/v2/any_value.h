#ifndef HGRAPH_CPP_ROOT_ANY_VALUE_H
#define HGRAPH_CPP_ROOT_ANY_VALUE_H

#include <cstddef>
#include <cstdint>
#include <typeinfo>
#include <utility>
#include <new>
#include <string>
#include <cstring>
#include <type_traits>
#include <functional>
#include <stdexcept>

#include "hgraph/hgraph_export.h"
#include <nanobind/nanobind.h>

namespace hgraph
{
    namespace nb = nanobind;

    // Small buffer size defaults: match nb::object size to satisfy current largest payload requirement
    inline constexpr std::size_t HGRAPH_TS_VALUE_SBO = sizeof(nb::object);
    inline constexpr std::size_t HGRAPH_TS_ALIGN     = alignof(std::max_align_t);

    // Type-id wrapper (can be replaced with a stable hashed id later)
    struct HGRAPH_EXPORT TypeId
    {
        const std::type_info *info{};
    };

    HGRAPH_EXPORT bool operator==(TypeId a, TypeId b);

    template <std::size_t SBO = HGRAPH_TS_VALUE_SBO, std::size_t Align = HGRAPH_TS_ALIGN>
    class AnyValue
    {
    public:
        AnyValue() noexcept : vtable_(nullptr), using_heap_(false) {}

        AnyValue(const AnyValue &other) : vtable_(nullptr), using_heap_(false) {
            if (other.vtable_) other.vtable_->copy(*this, other);
        }

        AnyValue(AnyValue &&other) noexcept : vtable_(nullptr), using_heap_(false) {
            if (other.vtable_) other.vtable_->move(*this, other);
        }

        AnyValue &operator=(const AnyValue &other) {
            if (this != &other) {
                reset();
                if (other.vtable_) other.vtable_->copy(*this, other);
            }
            return *this;
        }

        AnyValue &operator=(AnyValue &&other) noexcept {
            if (this != &other) {
                reset();
                if (other.vtable_) other.vtable_->move(*this, other);
            }
            return *this;
        }

        ~AnyValue() { reset(); }

        void reset() noexcept {
            if (vtable_) vtable_->destroy(*this);
            vtable_     = nullptr;
            using_heap_ = false;
        }

        [[nodiscard]] bool   has_value() const noexcept { return vtable_ != nullptr; }
        [[nodiscard]] TypeId type() const noexcept { return has_value() ? vtable_->type : TypeId{}; }

        template <class T, class... Args>
        T &emplace(Args &&... args) {
            reset();
            constexpr std::size_t t_align = alignof(T);
            constexpr std::size_t t_size  = sizeof(T);
            // SBO strategy: inline if size fits AND alignment requirement is satisfied
            // Note: storage_ is aligned to Align (typically alignof(std::max_align_t))
            // If t_align < Align, storage may be over-aligned, but this is safe and preferred for simplicity
            if (t_size <= SBO && t_align <= Align) {
                // Inline storage (Small Buffer Optimization)
                new(storage_ptr()) T(std::forward<Args>(args)...);
                using_heap_ = false;
            } else {
                // Heap allocation (type too large or requires stricter alignment)
                T *p = new T(std::forward<Args>(args)...);
                std::memcpy(storage_, &p, sizeof(T *));
                using_heap_ = true;
            }
            vtable_ = &vtable_for<T>();
            return *reinterpret_cast<T *>(get_ptr());
        }

        template <class T>
        T *get_if() noexcept {
            if (!vtable_ || vtable_->type.info != &typeid(T)) return nullptr;
            return reinterpret_cast<T *>(get_ptr());
        }

        template <class T>
        const T *get_if() const noexcept {
            if (!vtable_ || vtable_->type.info != &typeid(T)) return nullptr;
            return reinterpret_cast<const T *>(get_ptr());
        }

        // Place a borrowed reference to an external object.
        // Copying/moving this AnyValue materializes an owned copy in the destination.
        template <class T>
            requires(std::is_copy_constructible_v<std::remove_reference_t<T>>)
        T &emplace_ref(T &ref) {
            reset();
            using U = std::remove_reference_t<T>;
            U *p    = std::addressof(ref);
            std::memcpy(storage_, &p, sizeof(U *));
            // Mark as heap-allocated so get_ptr() dereferences the stored pointer rather than returning storage_ address
            using_heap_ = true;
            vtable_     = &ref_vtable_for<U>();
            return ref;
        }

        // Is the currently held object a borrowed reference?
        [[nodiscard]] bool is_reference() const noexcept { return vtable_ && vtable_->is_reference; }

        // Convert a borrowed reference into an owned value in-place.
        void ensure_owned() {
            if (!is_reference()) return;
            AnyValue tmp;
            vtable_->copy(tmp, *this); // ref copy => materialize owned into tmp
            swap(tmp);
        }

        // Swap helper (safe byte-wise swap of the inline buffer and metadata)
        void swap(AnyValue &other) noexcept {
            unsigned char tmp[SBO];
            std::memcpy(tmp, storage_, SBO);
            std::memcpy(storage_, other.storage_, SBO);
            std::memcpy(other.storage_, tmp, SBO);
            std::swap(vtable_, other.vtable_);
            std::swap(using_heap_, other.using_heap_);
        }

        // Hash of the contained value (type-aware). Returns 0 if empty.
        [[nodiscard]] std::size_t hash_code() const noexcept { return vtable_ ? vtable_->hash(*this) : 0; }

        /// Returns the actual storage size used by the contained value.
        /// For heap-allocated values, returns sizeof(void*) (the pointer size).
        /// For inline values, returns the SBO buffer size.
        /// Returns 0 if empty.
        [[nodiscard]] std::size_t storage_size() const noexcept {
            if (!vtable_) return 0;
            return using_heap_ ? sizeof(void *) : SBO;
        }

        /// Returns true if the value is stored inline (using SBO), false if heap-allocated or empty.
        [[nodiscard]] bool is_inline() const noexcept { return vtable_ && !using_heap_; }

        /// Returns true if the value is heap-allocated, false if inline or empty.
        [[nodiscard]] bool is_heap_allocated() const noexcept { return using_heap_; }

        /// Visit the contained value with a type-erased callback.
        /// The callback receives the value as const void* and its type_info.
        /// Does nothing if the container is empty.
        /// Useful for introspection, debugging, and generic serialization.
        void visit_untyped(const std::function<void(const void *, const std::type_info &)> &visitor) const {
            if (!vtable_) return;
            visitor(get_ptr(), *vtable_->type.info);
        }

        /// Visit the value if it contains type T, otherwise do nothing.
        /// Returns true if the visitor was invoked, false if empty or type mismatch.
        /// This is the type-safe way to handle values when the type is known at the call site.
        template <typename T, typename Visitor>
        bool visit_as(Visitor &&visitor) const {
            if (const T *p = get_if<T>()) {
                std::forward<Visitor>(visitor)(*p);
                return true;
            }
            return false;
        }

        /// Visit the value if it contains type T, otherwise do nothing (mutable version).
        /// Returns true if the visitor was invoked, false if empty or type mismatch.
        /// Allows modification of the contained value.
        template <typename T, typename Visitor>
        bool visit_as(Visitor &&visitor) {
            if (T *p = get_if<T>()) {
                std::forward<Visitor>(visitor)(*p);
                return true;
            }
            return false;
        }

    private:
        struct VTable
        {
            TypeId        type;
            void (*       copy)(AnyValue &, const AnyValue &);
            void (*       move)(AnyValue &, AnyValue &) noexcept;
            void (*       destroy)(AnyValue &) noexcept;
            std::size_t (*hash)(const AnyValue &) noexcept;
            bool (*       equals)(const AnyValue &, const AnyValue &) noexcept;
            bool (*       less)(const AnyValue &, const AnyValue &); // may throw when < not supported
            bool          is_reference;                              // indicates borrowed reference storage vs owned
        };

    public:
        // Equality: type + value. Empty equals empty. Different types -> false.
        friend bool operator==(const AnyValue &a, const AnyValue &b) noexcept {
            if (!a.vtable_ && !b.vtable_) return true;        // both empty
            if (!a.vtable_ || !b.vtable_) return false;       // one empty
            if (a.vtable_->type.info != b.vtable_->type.info) // different types
                return false;
            return a.vtable_->equals(a, b);
        }

        friend bool operator!=(const AnyValue &a, const AnyValue &b) noexcept { return !(a == b); }

        // Optional less-than: only defined when underlying type supports operator<.
        // Behavior:
        // - both empty -> false
        // - one empty -> throws std::runtime_error
        // - different types -> throws std::runtime_error
        // - same type -> delegate to per-type vtable less (may throw if < unsupported)
        friend bool operator<(const AnyValue &a, const AnyValue &b) {
            if (!a.vtable_ && !b.vtable_) return false; // empty vs empty
            if (!a.vtable_ || !b.vtable_) { throw std::runtime_error("AnyValue: operator< comparison with empty value"); }
            if (a.vtable_->type.info != b.vtable_->type.info) { throw std::runtime_error("AnyValue: operator< type mismatch"); }
            return a.vtable_->less(a, b);
        }

        template <class T>
        static const VTable &vtable_for() {
            static const VTable vt{
                TypeId{&typeid(T)},
                // copy
                [](AnyValue &dst, const AnyValue &src) {
                    if (src.using_heap_) {
                        auto *sp = *reinterpret_cast<T * const*>(src.storage_);
                        T *   np = new T(*sp);
                        std::memcpy(dst.storage_, &np, sizeof(T *));
                        dst.using_heap_ = true;
                    } else {
                        new(dst.storage_ptr()) T(*reinterpret_cast<const T *>(src.storage_ptr()));
                        dst.using_heap_ = false;
                    }
                    dst.vtable_ = &vtable_for<T>();
                },
                // move
                [](AnyValue &dst, AnyValue &src) noexcept {
                    if (src.using_heap_) {
                        auto *sp = *reinterpret_cast<T **>(src.storage_);
                        std::memcpy(dst.storage_, &sp, sizeof(T *));
                        dst.using_heap_ = true;
                        // release src
                        *reinterpret_cast<T **>(src.storage_) = nullptr;
                    } else {
                        new(dst.storage_ptr()) T(std::move(*reinterpret_cast<T *>(src.storage_ptr())));
                        dst.using_heap_ = false;
                        reinterpret_cast<T *>(src.storage_ptr())->~T();
                    }
                    dst.vtable_     = &vtable_for<T>();
                    src.vtable_     = nullptr;
                    src.using_heap_ = false;
                },
                // destroy
                [](AnyValue &self) noexcept {
                    if (!self.vtable_) return;
                    if (self.using_heap_) {
                        auto *p = *reinterpret_cast<T **>(self.storage_);
                        delete p;
                        *reinterpret_cast<T **>(self.storage_) = nullptr;
                    } else { reinterpret_cast<T *>(self.storage_ptr())->~T(); }
                },
                // hash
                [](const AnyValue &self) noexcept -> std::size_t {
                    if (!self.vtable_) return 0u;
                    const T *p = nullptr;
                    if (self.using_heap_) { p = *reinterpret_cast<T * const*>(self.storage_); } else {
                        p = reinterpret_cast<const T *>(self.storage_ptr());
                    }
                    // Prefer std::hash<T> if available; otherwise hash the pointer and type id
                    if constexpr (requires(const T &x) { { std::hash<T>{}(x) } -> std::convertible_to<std::size_t>; }) {
                        return std::hash<T>{}(*p);
                    } else {
                        const std::size_t th = std::hash<const void *>{}(self.vtable_->type.info);
                        const std::size_t ph = std::hash<const void *>{}(static_cast<const void *>(p));
                        // boost::hash_combine style using golden ratio constant (2^64 / phi)
                        return th ^ (ph + 0x9e3779b97f4a7c15ull + (th << 6) + (th >> 2));
                    }
                },
                // equals
                [](const AnyValue &a, const AnyValue &b) noexcept -> bool {
                    const T *ap = a.using_heap_
                                      ? *reinterpret_cast<T * const*>(a.storage_)
                                      : reinterpret_cast<const T *>(a.storage_ptr());
                    const T *bp = b.using_heap_
                                      ? *reinterpret_cast<T * const*>(b.storage_)
                                      : reinterpret_cast<const T *>(b.storage_ptr());
                    if constexpr (requires(const T &x, const T &y) { { x == y } -> std::convertible_to<bool>; }) {
                        return *ap == *bp;
                    } else {
                        return ap == bp; // fallback to pointer identity
                    }
                },
                // less-than (may throw if operator< is not available)
                [](const AnyValue &a, const AnyValue &b) -> bool {
                    if (!a.vtable_ || !b.vtable_) {
                        if (!a.vtable_ && !b.vtable_) return false; // empty vs empty
                        throw std::runtime_error("AnyValue: operator< comparison with empty value");
                    }
                    // types must match (enforced by caller, but double-check defensively)
                    if (a.vtable_->type.info != b.vtable_->type.info) {
                        throw std::runtime_error("AnyValue: operator< type mismatch");
                    }
                    const T *ap = a.using_heap_
                                      ? *reinterpret_cast<T * const*>(a.storage_)
                                      : reinterpret_cast<const T *>(a.storage_ptr());
                    const T *bp = b.using_heap_
                                      ? *reinterpret_cast<T * const*>(b.storage_)
                                      : reinterpret_cast<const T *>(b.storage_ptr());
                    if constexpr (requires(const T &x, const T &y) { { x < y } -> std::convertible_to<bool>; }) {
                        return *ap < *bp;
                    } else { throw std::runtime_error("AnyValue: operator< not supported for contained type"); }
                },
                /* is_reference */ false
            };
            return vt;
        }

        // VTable for a borrowed reference to T
        template <class T>
        static const VTable &ref_vtable_for() {
            static const VTable vt{
                TypeId{&typeid(T)},
                // copy => materialize an owned copy in the destination
                [](AnyValue &dst, const AnyValue &src) {
                    const T *p = *reinterpret_cast<T * const*>(src.storage_);
                    dst.template emplace<T>(*p);
                },
                // move => materialize owned copy in destination; source ref remains valid (refs are non-owning)
                [](AnyValue &dst, AnyValue &src) noexcept {
                    const T *p = *reinterpret_cast<T * const*>(src.storage_);
                    // Note: src is intentionally not modified - references remain valid after "move"
                    dst.template emplace<T>(*p);
                },
                // destroy => no-op for borrowed references
                [](AnyValue &) noexcept {},
                // hash => hash of the referenced value when available, else pointer hash
                [](const AnyValue &self) noexcept -> std::size_t {
                    const T *p = *reinterpret_cast<T * const*>(self.storage_);
                    if constexpr (requires(const T &x) { { std::hash<T>{}(x) } -> std::convertible_to<std::size_t>; }) {
                        return std::hash<T>{}(*p);
                    } else { return std::hash<const void *>{}(static_cast<const void *>(p)); }
                },
                // equals => compare by value if possible; else pointer identity
                [](const AnyValue &a, const AnyValue &b) noexcept -> bool {
                    const T *ap = *reinterpret_cast<T * const*>(a.storage_);
                    const T *bp = b.using_heap_
                                      ? *reinterpret_cast<T * const*>(b.storage_)
                                      : reinterpret_cast<const T *>(b.storage_ptr());
                    if constexpr (requires(const T &x, const T &y) { { x == y } -> std::convertible_to<bool>; }) {
                        return *ap == *bp;
                    } else { return ap == bp; }
                },
                // less-than => compare by value when operator< exists; else throw
                [](const AnyValue &a, const AnyValue &b) -> bool {
                    if (!a.vtable_ || !b.vtable_) {
                        if (!a.vtable_ && !b.vtable_) return false;
                        throw std::runtime_error("AnyValue: operator< comparison with empty value");
                    }
                    if (a.vtable_->type.info != b.vtable_->type.info) {
                        throw std::runtime_error("AnyValue: operator< type mismatch");
                    }
                    const T *ap = *reinterpret_cast<T * const*>(a.storage_);
                    const T *bp = b.using_heap_
                                      ? *reinterpret_cast<T * const*>(b.storage_)
                                      : reinterpret_cast<const T *>(b.storage_ptr());
                    if constexpr (requires(const T &x, const T &y) { { x < y } -> std::convertible_to<bool>; }) {
                        return *ap < *bp;
                    } else { throw std::runtime_error("AnyValue: operator< not supported for contained type"); }
                },
                /* is_reference */ true
            };
            return vt;
        }

        void *                    storage_ptr() noexcept { return static_cast<void *>(storage_); }
        [[nodiscard]] const void *storage_ptr() const noexcept { return static_cast<const void *>(storage_); }

        void *get_ptr() noexcept {
            if (using_heap_) return static_cast<void *>(*reinterpret_cast<void **>(storage_));
            return storage_ptr();
        }

        [[nodiscard]] const void *get_ptr() const noexcept {
            if (using_heap_) return static_cast<const void *>(*reinterpret_cast<void * const*>(storage_));
            return storage_ptr();
        }

        const VTable *               vtable_;
        bool                         using_heap_;
        alignas(Align) unsigned char storage_[SBO];
    };

    // String formatting helper for AnyValue (implemented in ts_event.cpp)
    HGRAPH_EXPORT std::string to_string(const AnyValue<> &v);
} // namespace hgraph

namespace std
{
    template <>
    struct hash<hgraph::TypeId>
    {
        size_t operator()(const hgraph::TypeId &id) const noexcept { return std::hash<const void *>{}(id.info); }
    };

    template <std::size_t SBO, std::size_t Align>
    struct hash<hgraph::AnyValue<SBO, Align>>
    {
        size_t operator()(const hgraph::AnyValue<SBO, Align> &v) const noexcept { return v.hash_code(); }
    };
}

// Safety check to ensure the configured SBO matches nb::object size as agreed.
static_assert(hgraph::HGRAPH_TS_VALUE_SBO == sizeof(nanobind::object),
              "HGRAPH_TS_VALUE_SBO must equal sizeof(nanobind::object)");

#endif // HGRAPH_CPP_ROOT_ANY_VALUE_H