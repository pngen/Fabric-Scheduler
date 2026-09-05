#pragma once
// Thin CUDA adapter. Fabric Scheduler gates placement; the CUDA adapter only
// runs real device work AFTER a valid, current execution handoff.
#include <cstddef>
#include <cstdint>
#ifdef __cplusplus
extern "C" {
#endif

typedef struct FabricCudaDeviceInfo {
    char name[256];
    int major;
    int minor;
    unsigned long long totalBytes;
    unsigned long long freeBytes;
    int present;
} FabricCudaDeviceInfo;

int fs_cuda_count(void);
int fs_cuda_probe(int device, FabricCudaDeviceInfo* out);
int fs_cuda_memfree(int device, unsigned long long* freeBytes);
int fs_cuda_exec(int device, unsigned long long bytes, double* parity);

#ifdef __cplusplus
}
#endif