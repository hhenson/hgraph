#ifndef HGRAPH_TYPE_H
#define HGRAPH_TYPE_H

#include <hgraph/hgraph_base.h>

namespace hgraph
{
    struct Type {
        explicit Type(const std::string &name);

        virtual ~Type() = default;

        [[nodiscard]] std::string get_name() const;

        virtual bool is_scalar() const;
        virtual bool is_time_series() const;

    private:
        std::string _name;
    };

    template<typename T>
    struct type_t {
        // this template has to be specialized for each type T that can be used in the type system as a scalar
        // the API for the specialization is defined here

        // typedef typename placement_type;
        // defines the placement_type for the type T - what actually gets stored and is the API type for T
        // for example for int, placement_type would be int, for std::string it might be somethings different than std::string, 
        // for example a custom string view type or a reference counted string type

        // typedef typename const_interface_type;
        // interface type for const access to T, does not have to be T itself, could be a reference or a wrapper

        // Value::vtbl_ptr construct(const T& val, Value::placeholder* placeholder);
        // constructs a ValueVTable pointer for the given type T and initializes the placeholder with val
        // not that vtbl_ptr also encodes flags about the type (like if it has a destructor, etc)
        
        // const typename const_interface_type get(Value::vtbl_ptr vtbl, const Value::placeholder& placeholder);
        // retrieves the value of type T from the given ValueVTable pointer and placeholder

        static_assert(False, "type_t<T> must be specialized for each type T used in the type system");
    };
} // namespace hgraph


#endif // HGRAPH_TYPE_H