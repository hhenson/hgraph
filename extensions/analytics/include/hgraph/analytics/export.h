#ifndef HGRAPH_ANALYTICS_EXPORT_H
#define HGRAPH_ANALYTICS_EXPORT_H

#if defined(HGRAPH_ANALYTICS_STATIC_DEFINE)
    #define HGRAPH_ANALYTICS_EXPORT
#elif defined(_WIN32)
    #if defined(hgraph_analytics_EXPORTS)
        #define HGRAPH_ANALYTICS_EXPORT __declspec(dllexport)
    #else
        #define HGRAPH_ANALYTICS_EXPORT __declspec(dllimport)
    #endif
#else
    #define HGRAPH_ANALYTICS_EXPORT __attribute__((visibility("default")))
#endif

#endif  // HGRAPH_ANALYTICS_EXPORT_H
