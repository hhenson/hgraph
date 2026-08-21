#ifndef HGRAPH_FABRIC_EXPORT_H
#define HGRAPH_FABRIC_EXPORT_H

#if defined(HGRAPH_FABRIC_STATIC_DEFINE)
    #define HGRAPH_FABRIC_EXPORT
#elif defined(_WIN32)
    #if defined(hgraph_fabric_EXPORTS)
        #define HGRAPH_FABRIC_EXPORT __declspec(dllexport)
    #else
        #define HGRAPH_FABRIC_EXPORT __declspec(dllimport)
    #endif
#else
    #define HGRAPH_FABRIC_EXPORT __attribute__((visibility("default")))
#endif

#endif  // HGRAPH_FABRIC_EXPORT_H
