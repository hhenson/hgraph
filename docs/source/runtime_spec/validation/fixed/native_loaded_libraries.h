// Enumerate modules in this probe process, including loader path overrides.
#pragma once
#include <filesystem>
#include <stdexcept>
#include <string>
#include <vector>
#if defined(__APPLE__)
#include <mach-o/dyld.h>
#elif defined(__linux__)
#include <link.h>
#elif defined(_WIN32)
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#include <tlhelp32.h>
#endif

inline std::vector<std::string> loaded_libraries() {
    std::vector<std::string> paths;
#if defined(__APPLE__)
    for (std::uint32_t i = 0; i < _dyld_image_count(); ++i) {
        if (const char *path = _dyld_get_image_name(i)) paths.emplace_back(path);
    }
#elif defined(__linux__)
    dl_iterate_phdr([](dl_phdr_info *info, std::size_t, void *context) {
        if (info->dlpi_name && *info->dlpi_name)
            static_cast<std::vector<std::string> *>(context)->emplace_back(info->dlpi_name);
        return 0;
    }, &paths);
#elif defined(_WIN32)
    HANDLE snapshot = CreateToolhelp32Snapshot(TH32CS_SNAPMODULE | TH32CS_SNAPMODULE32, GetCurrentProcessId());
    if (snapshot == INVALID_HANDLE_VALUE) throw std::runtime_error("Cannot enumerate loaded libraries");
    MODULEENTRY32W entry{};
    entry.dwSize = sizeof(entry);
    if (!Module32FirstW(snapshot, &entry)) {
        CloseHandle(snapshot);
        throw std::runtime_error("Cannot read loaded libraries");
    }
    do { paths.push_back(std::filesystem::path(entry.szExePath).string()); }
    while (Module32NextW(snapshot, &entry));
    CloseHandle(snapshot);
#else
    throw std::runtime_error("Native library identity is unsupported on this platform");
#endif
    return paths;
}
