#ifndef HGRAPH_TYPES_VALUE_NATIVE_BUNDLE_TRAITS_H
#define HGRAPH_TYPES_VALUE_NATIVE_BUNDLE_TRAITS_H

namespace hgraph
{
    /** Specialize with a concrete NominalBundle schema and a tuple of data
        member pointers in that schema's field order. */
    template <typename T> struct native_bundle;

    template <typename T>
    concept NativeBundleValue = requires {
        typename native_bundle<T>::schema;
        native_bundle<T>::members;
    };
}

#endif
