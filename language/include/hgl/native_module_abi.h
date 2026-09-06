#ifndef HGL_NATIVE_MODULE_ABI_H
#define HGL_NATIVE_MODULE_ABI_H

#include <stddef.h>
#include <stdint.h>

#if defined(_WIN32)
    #define HGL_NATIVE_MODULE_EXPORT __declspec(dllexport)
#elif defined(__GNUC__) || defined(__clang__)
    #define HGL_NATIVE_MODULE_EXPORT __attribute__((visibility("default")))
#else
    #define HGL_NATIVE_MODULE_EXPORT
#endif

#ifdef __cplusplus
extern "C" {
#endif

    enum {
        HGL_NATIVE_MODULE_ABI_V1         = 1,
        HGL_NATIVE_MODULE_OK             = 0,
        HGL_NATIVE_MODULE_ERROR          = 1,
        HGL_NATIVE_MODULE_ERROR_CAPACITY = 1024
    };

#define HGL_NATIVE_MODULE_QUERY_SYMBOL_V1 "hgl_query_native_module_v1"

    typedef struct hgl_native_module_error_v1
    {
        uint32_t struct_size;
        char     message[HGL_NATIVE_MODULE_ERROR_CAPACITY];
    } hgl_native_module_error_v1;

    typedef int32_t (*hgl_native_module_hook_v1)(void *context, hgl_native_module_error_v1 *error);
    typedef int32_t (*hgl_native_module_active_v1)(const void *context);

    /**
     * Version-one HGL native module lifecycle table.
     *
     * The table and every string/context it references must remain valid for the
     * lifetime of the loaded image. Hooks must not allow exceptions to cross the
     * ABI boundary. A successful init is idempotent; deinit removes logical
     * registrations but does not imply that the image can be unloaded.
     */
    typedef struct hgl_native_module_v1
    {
        uint32_t                    abi_version;
        uint32_t                    struct_size;
        const char                 *module_identity;
        const char                 *descriptor_fingerprint;
        void                       *context;
        hgl_native_module_hook_v1   init;
        hgl_native_module_hook_v1   deinit;
        hgl_native_module_active_v1 is_active;
    } hgl_native_module_v1;

    /** Return the immutable module table, or NULL for an unsupported ABI. */
    typedef const hgl_native_module_v1 *(*hgl_query_native_module_fn_v1)(uint32_t requested_abi);

#ifdef __cplusplus
}
#endif

#endif  // HGL_NATIVE_MODULE_ABI_H
