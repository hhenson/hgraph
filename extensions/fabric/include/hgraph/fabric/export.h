#ifndef HGRAPH_FABRIC_EXPORT_H
#define HGRAPH_FABRIC_EXPORT_H

/* Match core's Windows boundary: export class methods from the producer, but
   do not import private STL representation as part of the consumer contract. */

#if defined(HGRAPH_FABRIC_STATIC_DEFINE)
    #define HGRAPH_FABRIC_EXPORT
    #define HGRAPH_FABRIC_CLASS_EXPORT
#elif defined(_WIN32)
    #if defined(hgraph_fabric_EXPORTS)
        #define HGRAPH_FABRIC_EXPORT __declspec(dllexport)
        #define HGRAPH_FABRIC_CLASS_EXPORT __declspec(dllexport)
    #else
        #define HGRAPH_FABRIC_EXPORT __declspec(dllimport)
        #define HGRAPH_FABRIC_CLASS_EXPORT
    #endif
#else
    #define HGRAPH_FABRIC_EXPORT __attribute__((visibility("default")))
    #define HGRAPH_FABRIC_CLASS_EXPORT __attribute__((visibility("default")))
#endif

#endif  // HGRAPH_FABRIC_EXPORT_H
