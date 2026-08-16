#ifndef HGRAPH_WEB_EXPORT_H
#define HGRAPH_WEB_EXPORT_H

#if defined(HGRAPH_WEB_STATIC_DEFINE)
    #define HGRAPH_WEB_EXPORT
#elif defined(_WIN32)
    #if defined(hgraph_web_EXPORTS)
        #define HGRAPH_WEB_EXPORT __declspec(dllexport)
    #else
        #define HGRAPH_WEB_EXPORT __declspec(dllimport)
    #endif
#else
    #define HGRAPH_WEB_EXPORT __attribute__((visibility("default")))
#endif

#endif  // HGRAPH_WEB_EXPORT_H
