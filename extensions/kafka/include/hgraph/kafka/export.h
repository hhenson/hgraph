#ifndef HGRAPH_KAFKA_EXPORT_H
#define HGRAPH_KAFKA_EXPORT_H

#if defined(HGRAPH_KAFKA_STATIC_DEFINE)
    #define HGRAPH_KAFKA_EXPORT
#elif defined(_WIN32)
    #if defined(hgraph_kafka_EXPORTS)
        #define HGRAPH_KAFKA_EXPORT __declspec(dllexport)
    #else
        #define HGRAPH_KAFKA_EXPORT __declspec(dllimport)
    #endif
#else
    #define HGRAPH_KAFKA_EXPORT __attribute__((visibility("default")))
#endif

#endif  // HGRAPH_KAFKA_EXPORT_H
