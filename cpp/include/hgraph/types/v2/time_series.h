#ifndef HGRAPH_CPP_ROOT_TIME_SERIES_H
#define HGRAPH_CPP_ROOT_TIME_SERIES_H

#include <cstddef>
#include <cstdint>
#include <typeinfo>
#include <utility>
#include <new>
#include <vector>
#include <string>
#include <cstring>

#include "hgraph/util/date_time.h"
#include <nanobind/nanobind.h>

namespace hgraph {

    namespace nb = nanobind;

    // Small buffer size defaults: match nb::object size to satisfy current largest payload requirement
    inline constexpr std::size_t HGRAPH_TS_VALUE_SBO = sizeof(nb::object);
    inline constexpr std::size_t HGRAPH_TS_ALIGN = alignof(std::max_align_t);

    // Type-id wrapper (can be replaced with a stable hashed id later)
    struct TypeId {
        const std::type_info* info{};
        friend bool operator==(TypeId a, TypeId b) { return a.info == b.info; }
    };

    template <std::size_t SBO = HGRAPH_TS_VALUE_SBO, std::size_t Align = HGRAPH_TS_ALIGN>
    class AnyValue {
    public:
        AnyValue() noexcept : vtable_(nullptr), using_heap_(false) {}

        AnyValue(const AnyValue& other) : vtable_(nullptr), using_heap_(false) {
            if (other.vtable_) other.vtable_->copy(*this, other);
        }
        AnyValue(AnyValue&& other) noexcept : vtable_(nullptr), using_heap_(false) {
            if (other.vtable_) other.vtable_->move(*this, other);
        }
        AnyValue& operator=(const AnyValue& other) {
            if (this != &other) {
                reset();
                if (other.vtable_) other.vtable_->copy(*this, other);
            }
            return *this;
        }
        AnyValue& operator=(AnyValue&& other) noexcept {
            if (this != &other) {
                reset();
                if (other.vtable_) other.vtable_->move(*this, other);
            }
            return *this;
        }
        ~AnyValue() { reset(); }

        void reset() noexcept {
            if (vtable_) vtable_->destroy(*this);
            vtable_ = nullptr;
            using_heap_ = false;
        }

        bool has_value() const noexcept { return vtable_ != nullptr; }
        TypeId type() const noexcept { return has_value() ? vtable_->type : TypeId{}; }

        template <class T, class... Args>
        T& emplace(Args&&... args) {
            reset();
            constexpr std::size_t t_align = alignof(T);
            constexpr std::size_t t_size = sizeof(T);
            if (t_size <= SBO && t_align <= Align) {
                // Inline
                new (storage_ptr()) T(std::forward<Args>(args)...);
                using_heap_ = false;
            } else {
                // Heap
                T* p = new T(std::forward<Args>(args)...);
                std::memcpy(storage_, &p, sizeof(T*));
                using_heap_ = true;
            }
            vtable_ = &vtable_for<T>();
            return *reinterpret_cast<T*>(get_ptr());
        }

        template <class T>
        T* get_if() noexcept {
            if (!vtable_ || vtable_->type.info != &typeid(T)) return nullptr;
            return reinterpret_cast<T*>(get_ptr());
        }
        template <class T>
        const T* get_if() const noexcept {
            if (!vtable_ || vtable_->type.info != &typeid(T)) return nullptr;
            return reinterpret_cast<const T*>(get_ptr());
        }

    private:
        struct VTable {
            TypeId type;
            void (*copy)(AnyValue&, const AnyValue&);
            void (*move)(AnyValue&, AnyValue&) noexcept;
            void (*destroy)(AnyValue&) noexcept;
        };

        template <class T>
        static const VTable& vtable_for() {
            static const VTable vt{
                TypeId{&typeid(T)},
                // copy
                [](AnyValue& dst, const AnyValue& src) {
                    if (src.using_heap_) {
                        auto* sp = *reinterpret_cast<T* const*>(src.storage_);
                        T* np = new T(*sp);
                        std::memcpy(dst.storage_, &np, sizeof(T*));
                        dst.using_heap_ = true;
                    } else {
                        new (dst.storage_ptr()) T(*reinterpret_cast<const T*>(src.storage_ptr()));
                        dst.using_heap_ = false;
                    }
                    dst.vtable_ = &vtable_for<T>();
                },
                // move
                [](AnyValue& dst, AnyValue& src) noexcept {
                    if (src.using_heap_) {
                        auto* sp = *reinterpret_cast<T**>(src.storage_);
                        std::memcpy(dst.storage_, &sp, sizeof(T*));
                        dst.using_heap_ = true;
                        // release src
                        *reinterpret_cast<T**>(src.storage_) = nullptr;
                    } else {
                        new (dst.storage_ptr()) T(std::move(*reinterpret_cast<T*>(src.storage_ptr())));
                        dst.using_heap_ = false;
                        reinterpret_cast<T*>(src.storage_ptr())->~T();
                    }
                    dst.vtable_ = &vtable_for<T>();
                    src.vtable_ = nullptr;
                    src.using_heap_ = false;
                },
                // destroy
                [](AnyValue& self) noexcept {
                    if (!self.vtable_) return;
                    if (self.using_heap_) {
                        auto* p = *reinterpret_cast<T**>(self.storage_);
                        delete p;
                        *reinterpret_cast<T**>(self.storage_) = nullptr;
                    } else {
                        reinterpret_cast<T*>(self.storage_ptr())->~T();
                    }
                }
            };
            return vt;
        }

        void* storage_ptr() noexcept { return static_cast<void*>(storage_); }
        const void* storage_ptr() const noexcept { return static_cast<const void*>(storage_); }

        void* get_ptr() noexcept {
            if (using_heap_) return static_cast<void*>(*reinterpret_cast<void**>(storage_));
            return storage_ptr();
        }
        const void* get_ptr() const noexcept {
            if (using_heap_) return static_cast<const void*>(*reinterpret_cast<void* const*>(storage_));
            return storage_ptr();
        }

        alignas(Align) unsigned char storage_[SBO];
        const VTable* vtable_;
        bool using_heap_;
    };

    enum class TsEventKind : std::uint8_t { None = 0, Invalidate = 1, Modify = 2 };

    struct TsEventAny {
        engine_time_t time{};
        TsEventKind kind{TsEventKind::None};
        AnyValue<> value; // engaged when kind==Modify

        static TsEventAny none(engine_time_t t) { return {t, TsEventKind::None, {}}; }
        static TsEventAny invalidate(engine_time_t t) { return {t, TsEventKind::Invalidate, {}}; }
        template <class T>
        static TsEventAny modify(engine_time_t t, T&& v) {
            TsEventAny e{t, TsEventKind::Modify, {}};
            e.value.template emplace<std::decay_t<T>>(std::forward<T>(v));
            return e;
        }
    };

    struct TsValueAny {
        bool has_value{false};
        AnyValue<> value;

        static TsValueAny none() { return {}; }
        template <class T>
        static TsValueAny of(T&& v) {
            TsValueAny sv; sv.has_value = true; sv.value.template emplace<std::decay_t<T>>(std::forward<T>(v)); return sv;
        }
    };

    // Legacy typed event helpers retained for interop
    enum class TsState { MODIFY = 0, INVALID = 1, NONE = 2 };
    struct TsEvent { engine_time_t event_time{}; };
    template <class T> struct TsModifyEvent : TsEvent { T value; };
    struct TsInvalidateEvent : TsEvent { };
    struct TsNoneEvent : TsEvent { };

    template <class T>
    inline TsEventAny erase_event(const TsModifyEvent<T>& e) { return TsEventAny::modify(e.event_time, e.value); }
    inline TsEventAny erase_event(const TsInvalidateEvent& e) { return TsEventAny::invalidate(e.event_time); }
    inline TsEventAny erase_event(const TsNoneEvent& e) { return TsEventAny::none(e.event_time); }

} // namespace hgraph

#endif // HGRAPH_CPP_ROOT_TIME_SERIES_H
