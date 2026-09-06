#include <hgl/native_module_abi.h>

#include <stddef.h>
#include <stdint.h>

static int32_t init_module(void *context, hgl_native_module_error_v1 *error)
{
    (void)error;
    *(int *)context = 1;
    return HGL_NATIVE_MODULE_OK;
}

static int32_t deinit_module(void *context, hgl_native_module_error_v1 *error)
{
    (void)error;
    *(int *)context = 0;
    return HGL_NATIVE_MODULE_OK;
}

static int32_t is_active(const void *context) { return *(const int *)context; }

int main(void)
{
    int active = 0;
    const hgl_native_module_v1 module = {HGL_NATIVE_MODULE_ABI_V1,
                                         sizeof(hgl_native_module_v1),
                                         "test.module",
                                         "fingerprint",
                                         &active,
                                         init_module,
                                         deinit_module,
                                         is_active};
    hgl_native_module_error_v1 error = {sizeof(hgl_native_module_error_v1), {0}};

    if (module.init(module.context, &error) != HGL_NATIVE_MODULE_OK || !module.is_active(module.context)) { return 1; }
    if (module.deinit(module.context, &error) != HGL_NATIVE_MODULE_OK || module.is_active(module.context)) { return 2; }
    return HGL_NATIVE_MODULE_QUERY_SYMBOL_V1[0] == '\0';
}
