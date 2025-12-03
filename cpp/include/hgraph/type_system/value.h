#ifndef HGRAPH_VALUE_H
#define HGRAPH_VALUE_H
#include <hgraph/type_system/value_placeholder.h>
#include <hgraph/type_system/value_vtbl.h>

namespace hgraph {
    /**
     * Value - Represents a value in the type system
     *
     * This struct encapsulates a value along with its associated
     * virtual table for operations. It provides methods for copying,
     * destroying, comparing, and hashing the value.
     */
    struct Value {
        Value(){
            vtbl = type_t<void>::contruct(nullptr, &placeholder);
        }

        ~Value(){
            if (vtbl & value_flags::has_destructor)
                vtbl->destroy(&placeholder);
        }

        Value(const Value& other){
            vtbl = other.vtbl;
            vtbl->copy(&other.placeholder, &placeholder);
        }

        Value(Value&& other){
            std::swap(vtbl, other.vtbl);
            std::swap(placeholder, other.placeholder);
        }

        template<typename T>
        explicit Value(const T& val){
            vtbl = type_t<T>::contruct(val, &placeholder);
        }

        template<typename T>
        explicit Value(T&& val){
            vtbl = type_t<T>::contruct(std::move(val), &placeholder);
        }

        template<typename T>
        const type_t<T>::typename const_placement_type get() const {
            return type_t<T>::get(vtbl, placeholder);
        }

        template<typename T>
        type_t<T>::typename const_placement_type get() {
            return type_t<T>::get(vtbl, placeholder);
        }

        bool is_empty() const {
            return vtbl & value_flags::empty;
        }

        Value& operator=(const Value& other){
            if (this != &other) {
                if (vtbl & value_flags::has_destructor)
                    vtbl->destroy(&placeholder);
                vtbl = other.vtbl;
                vtbl->copy(&other.placeholder, &placeholder);
            }
            return *this;
        }

        Value& operator=(Value&& other){
            if (this != &other) {
                if (vtbl & value_flags::has_destructor)
                    vtbl->destroy(&placeholder);
                std::swap(vtbl, other.vtbl);
                std::swap(placeholder, other.placeholder);
            }
            return *this;
        }

        bool operator==(const Value& other) const {
            return vtbl->equal(&placeholder, &other.placeholder);
        }

        bool operator<(const Value& other) const {
            return vtbl->less(&placeholder, &other.placeholder);
        }

        size_t hash() const {
            return vtbl->hash(&placeholder);
        }

    private:
        enum class value_flags {
            empty = 1,
            has_destructor = 2,
        };

        typedef tagged_ptr<ValueVTable, value_flags> vtbl_ptr;

        template<typename T>
        friend struct type_t<T>;

        ValuePlaceholder placeholder;
        vtbl_ptr vtbl;
    };
} // namespace hgraph

#endif // HGRAPH_VALUE_H