#include <hgl/ordinary_values.h>

#include <atomic>
#include <cstdlib>
#include <iostream>
#include <new>
#if defined(_MSC_VER)
#include <malloc.h>
#endif

namespace {
    std::atomic<bool> tracking{false};
    std::atomic<std::size_t> allocations{0};
    void count() noexcept { if (tracking.load(std::memory_order_relaxed)) { allocations.fetch_add(1, std::memory_order_relaxed); } }
    void *allocate(std::size_t size) {
        count();
        if (void *memory = std::malloc(size == 0 ? 1 : size)) { return memory; }
        throw std::bad_alloc{};
    }
    void *allocate_aligned(std::size_t size, std::size_t alignment) {
        count();
#if defined(_MSC_VER)
        if (void *memory = _aligned_malloc(size == 0 ? 1 : size, alignment)) { return memory; }
#else
        void *memory = nullptr;
        if (posix_memalign(&memory, alignment, size == 0 ? 1 : size) == 0) { return memory; }
#endif
        throw std::bad_alloc{};
    }
    void free_aligned(void *memory) noexcept {
#if defined(_MSC_VER)
        _aligned_free(memory);
#else
        std::free(memory);
#endif
    }
}

void *operator new(std::size_t size) { return allocate(size); }
void *operator new[](std::size_t size) { return allocate(size); }
void *operator new(std::size_t size, std::align_val_t alignment) { return allocate_aligned(size, static_cast<std::size_t>(alignment)); }
void *operator new[](std::size_t size, std::align_val_t alignment) { return allocate_aligned(size, static_cast<std::size_t>(alignment)); }
void *operator new(std::size_t size, const std::nothrow_t &) noexcept { try { return ::operator new(size); } catch (...) { return nullptr; } }
void *operator new[](std::size_t size, const std::nothrow_t &) noexcept { try { return ::operator new[](size); } catch (...) { return nullptr; } }
void *operator new(std::size_t size, std::align_val_t alignment, const std::nothrow_t &) noexcept {
    try { return ::operator new(size, alignment); } catch (...) { return nullptr; }
}
void *operator new[](std::size_t size, std::align_val_t alignment, const std::nothrow_t &) noexcept {
    try { return ::operator new[](size, alignment); } catch (...) { return nullptr; }
}
void operator delete(void *memory) noexcept { std::free(memory); }
void operator delete[](void *memory) noexcept { std::free(memory); }
void operator delete(void *memory, std::size_t) noexcept { std::free(memory); }
void operator delete[](void *memory, std::size_t) noexcept { std::free(memory); }
void operator delete(void *memory, const std::nothrow_t &) noexcept { std::free(memory); }
void operator delete[](void *memory, const std::nothrow_t &) noexcept { std::free(memory); }
void operator delete(void *memory, std::align_val_t) noexcept { free_aligned(memory); }
void operator delete[](void *memory, std::align_val_t) noexcept { free_aligned(memory); }
void operator delete(void *memory, std::size_t, std::align_val_t) noexcept { free_aligned(memory); }
void operator delete[](void *memory, std::size_t, std::align_val_t) noexcept { free_aligned(memory); }
void operator delete(void *memory, std::align_val_t, const std::nothrow_t &) noexcept { free_aligned(memory); }
void operator delete[](void *memory, std::align_val_t, const std::nothrow_t &) noexcept { free_aligned(memory); }

int main() {
    using namespace hgraph;
    using namespace hgl::ordinary;
    const Value zero{Int{0}}, falsity{Bool{false}};
    const auto absent = Value::typed_null(zero.binding());
    // Warm the established scalar read paths before measuring evaluation.
    (void)required_scalar<Int>(zero.view());
    (void)required_scalar<Bool>(falsity.view());
    allocations.store(0);
    tracking.store(true);
    Int sum = 0;
    bool flag = false;
    for (std::size_t index = 0; index < 10000; ++index) {
        sum += required_scalar<Int>(zero.view());
        flag = flag || required_scalar<Bool>(falsity.view());
    }
    tracking.store(false);
    const auto successful_allocations = allocations.load();
    allocations.store(0);
    tracking.store(true);
    bool matched = false;
    try { (void)required_scalar<Int>(absent.view()); }
    catch (const hgl::ExecutionError &error) { matched = error.code() == "value.unset_read"; }
    tracking.store(false);
    const auto error_allocations = allocations.load();
    std::cout << "successful scalar payload reads: allocations=" << successful_allocations
              << "; unset error diagnostics: allocations=" << error_allocations << '\n';
    return successful_allocations == 0 && sum == 0 && !flag && matched ? 0 : 1;
}
