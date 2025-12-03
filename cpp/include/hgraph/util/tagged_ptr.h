#pragma once
#include <cstdint>
#include <type_traits>

template<typename T, typename TagEnum>
class tagged_ptr {
    static_assert(alignof(T) >= 8, "Type must have 8-byte alignment");
    static_assert(std::is_enum_v<TagEnum>, "FlagEnum must be an enum type");
    
    using PtrType = std::uintptr_t;
    static constexpr PtrType MASK_FLAGS = 0x7;  // Lower 3 bits for flags
    static constexpr PtrType MASK_PTR = ~MASK_FLAGS;

    PtrType value_;

public:
    tagged_ptr() : value_(0) {}
    explicit tagged_ptr(T* ptr) : value_(reinterpret_cast<PtrType>(ptr)) {}
    tagged_ptr(T* ptr, TagEnum flags) : value_(reinterpret_cast<PtrType>(ptr) | flags) {}

    // Pointer operations
    tagged_ptr& operator=(T* ptr) {
        value_ = (value_ & MASK_FLAGS) | (reinterpret_cast<PtrType>(ptr) & MASK_PTR);
        return *this;
    }

    T* operator->() const { return get(); }
    T& operator*() const { return *get(); }
    T* get() const { return reinterpret_cast<T*>(value_ & MASK_PTR); }
    operator bool() const { return get() != nullptr; }

    // Flag operations

    TagEnum get_flags() const {
        return static_cast<TagEnum>(value_ & MASK_FLAGS);
    }

    tagged_ptr& operator|=(TagEnum flag) {
        value_ |= (static_cast<PtrType>(1) << static_cast<PtrType>(flag));
        return *this;
    }

    tagged_ptr& operator&=(TagEnum flag) {
        value_ &= ~(static_cast<PtrType>(1) << static_cast<PtrType>(flag));
        return *this;
    }

    bool operator&(TagEnum flag) const {
        return (value_ & (static_cast<PtrType>(1) << static_cast<PtrType>(flag))) != 0;
    }
};
