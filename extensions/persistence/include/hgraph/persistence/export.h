#ifndef HGRAPH_PERSISTENCE_EXPORT_H
#define HGRAPH_PERSISTENCE_EXPORT_H

#if defined(HGRAPH_PERSISTENCE_STATIC_DEFINE)
    #define HGRAPH_PERSISTENCE_EXPORT
#elif defined(_WIN32)
    #if defined(hgraph_persistence_EXPORTS)
        #define HGRAPH_PERSISTENCE_EXPORT __declspec(dllexport)
    #else
        #define HGRAPH_PERSISTENCE_EXPORT __declspec(dllimport)
    #endif
#else
    #define HGRAPH_PERSISTENCE_EXPORT __attribute__((visibility("default")))
#endif

#endif  // HGRAPH_PERSISTENCE_EXPORT_H
