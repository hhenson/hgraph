#pragma once

#if defined _WIN32 || defined __CYGWIN__
#ifdef hgraph_EXPORTS
#ifdef __GNUCC__
#define HGRAPH_EXPORT __attribute__ ((dllexport))
#else
#define HGRAPH_EXPORT __declspec(dllexport)
#define DLL_WARNING_DISABLE_4251
#endif
#else
#ifdef __GNUC__
#define HGRAPH_EXPORT __attribute__ ((dllexport))
#else
#define HGRAPH_EXPORT __declspec(dllimport)
#endif
#endif
#else
#if __GNUC__ >= 4
#define HGRAPH_EXPORT __attribute__ ((visibility ("default")))
#else
#define HGRAPH_EXPORT
#endif
#endif

#ifdef DLL_WARNING_DISABLE_4251
#pragma warning( disable : 4251 )
#pragma warning( disable : 4275 )
#endif