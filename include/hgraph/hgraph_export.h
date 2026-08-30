#pragma once

/* Public classes are exported by the producing DLL, while consumers link
 * their out-of-line methods through the import library without marking the
 * complete class dllimport. This keeps private STL representation details out
 * of the consumer DLL contract. Producer targets scope MSVC's corresponding
 * representation diagnostics in CMake; exported methods remain the supported
 * boundary. */

#if defined HGRAPH_STATIC_DEFINE
#define HGRAPH_EXPORT
#define HGRAPH_CLASS_EXPORT
#define HGRAPH_LOCAL
#elif defined _WIN32 || defined __CYGWIN__
#define HGRAPH_LOCAL
#ifdef hgraph_EXPORTS
#ifdef __GNUCC__
#define HGRAPH_EXPORT __attribute__ ((dllexport))
#define HGRAPH_CLASS_EXPORT __attribute__ ((dllexport))
#else
#define HGRAPH_EXPORT __declspec(dllexport)
#define HGRAPH_CLASS_EXPORT __declspec(dllexport)
#endif
#else
#ifdef __GNUC__
#define HGRAPH_EXPORT __attribute__ ((dllexport))
#define HGRAPH_CLASS_EXPORT
#else
#define HGRAPH_EXPORT __declspec(dllimport)
#define HGRAPH_CLASS_EXPORT
#endif
#endif

#else
#if __GNUC__ >= 4
#define HGRAPH_EXPORT __attribute__ ((visibility ("default")))
#define HGRAPH_CLASS_EXPORT __attribute__ ((visibility ("default")))
#define HGRAPH_LOCAL __attribute__ ((visibility ("hidden")))
#else
#define HGRAPH_EXPORT
#define HGRAPH_CLASS_EXPORT
#define HGRAPH_LOCAL
#endif
#endif
