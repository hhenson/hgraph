#ifndef HGRAPH_CPP_ROOT_CONFIG_H
#define HGRAPH_CPP_ROOT_CONFIG_H

/*
 * Python user nodes are a CMake option; the type layer compiles the same
 * headers either way (RFC 0035), so only the bridge units under
 * src/hgraph/python and include/hgraph/python read this flag.
 */
#if !defined(HGRAPH_ENABLE_PYTHON_USER_NODES)
#define HGRAPH_ENABLE_PYTHON_USER_NODES 0
#endif

#if defined(_MSC_VER)
#define HGRAPH_NOINLINE __declspec(noinline)
#elif defined(__GNUC__) || defined(__clang__)
#define HGRAPH_NOINLINE __attribute__((noinline))
#else
#define HGRAPH_NOINLINE
#endif

#endif  // HGRAPH_CPP_ROOT_CONFIG_H
