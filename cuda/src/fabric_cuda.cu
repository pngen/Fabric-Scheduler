#include "fabric_cuda.h"
#include <cuda_runtime.h>
#include <cstdio>
#include <cstring>
#include <cmath>
#include <vector>

int fs_cuda_count(void) {
    int n = 0;
    cudaError_t e = cudaGetDeviceCount(&n);
    if (e != cudaSuccess) return -1;
    return n;
}

int fs_cuda_probe(int device, FabricCudaDeviceInfo* out) {
    if (!out) return 1;
    std::memset(out, 0, sizeof(*out));
    cudaError_t e = cudaSetDevice(device);
    if (e != cudaSuccess) return 2;
    cudaDeviceProp p;
    e = cudaGetDeviceProperties(&p, device);
    if (e != cudaSuccess) return 3;
    std::snprintf(out->name, sizeof(out->name), "%s", p.name);
    out->major = p.major;
    out->minor = p.minor;
    size_t freeB = 0, totalB = 0;
    cudaMemGetInfo(&freeB, &totalB);
    out->totalBytes = totalB;
    out->freeBytes = freeB;
    out->present = 1;
    return 0;
}

int fs_cuda_memfree(int device, unsigned long long* freeBytes) {
    cudaError_t e = cudaSetDevice(device);
    if (e != cudaSuccess) return 1;
    size_t freeB = 0, totalB = 0;
    e = cudaMemGetInfo(&freeB, &totalB);
    if (e != cudaSuccess) return 2;
    *freeBytes = freeB;
    return 0;
}

__global__ void fs_saxpy_kernel(const float* a, const float* b, float* out, float alpha, int n) {
    int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i < n) out[i] = alpha * a[i] + b[i];
}

int fs_cuda_exec(int device, unsigned long long bytes, double* parity) {
    cudaError_t e = cudaSetDevice(device);
    if (e != cudaSuccess) return 1;
    const std::size_t n = static_cast<std::size_t>(bytes / sizeof(float));
    if (n == 0) return 2;
    float* da = nullptr;
    float* db = nullptr;
    float* dout = nullptr;
    e = cudaMalloc(&da, n * sizeof(float));
    if (e != cudaSuccess) return 3;
    e = cudaMalloc(&db, n * sizeof(float));
    if (e != cudaSuccess) { cudaFree(da); return 4; }
    e = cudaMalloc(&dout, n * sizeof(float));
    if (e != cudaSuccess) { cudaFree(da); cudaFree(db); return 5; }

    std::vector<float> a(n), b(n), out(n);
    for (std::size_t i = 0; i < n; ++i) { a[i] = static_cast<float>(i % 251); b[i] = 1.0f; }
    e = cudaMemcpy(da, a.data(), n * sizeof(float), cudaMemcpyHostToDevice);
    if (e == cudaSuccess) e = cudaMemcpy(db, b.data(), n * sizeof(float), cudaMemcpyHostToDevice);
    if (e == cudaSuccess) {
        const int threads = 256;
        const int blocks = static_cast<int>((n + threads - 1) / threads);
        const float alpha = 3.0f;
        fs_saxpy_kernel<<<blocks, threads>>>(da, db, dout, alpha, static_cast<int>(n));
        e = cudaDeviceSynchronize();
    }
    if (e == cudaSuccess) e = cudaMemcpy(out.data(), dout, n * sizeof(float), cudaMemcpyDeviceToHost);

    double ok = 1.0;
    if (e == cudaSuccess) {
        for (std::size_t i = 0; i < n; ++i) {
            const float want = 3.0f * a[i] + b[i];
            if (std::abs(out[i] - want) > 1e-3f) { ok = 0.0; break; }
        }
    }
    if (parity) *parity = ok;
    cudaFree(da); cudaFree(db); cudaFree(dout);
    return (e == cudaSuccess) ? 0 : 6;
}
