# CUDA proof binaries. Built only when FABRIC_BUILD_CUDA=ON and nvcc is present.
# The .cu is compiled with nvcc via a custom command (NMake/VS CUDA toolset is
# not registered for the BuildTools install), then linked into the host target.
if(NOT CUDAToolkit_ROOT)
  set(CUDAToolkit_ROOT "C:/Program Files/NVIDIA GPU Computing Toolkit/CUDA/v12.9" CACHE PATH "CUDA toolkit root" FORCE)
endif()
find_package(CUDAToolkit REQUIRED)

if(NOT CUDAToolkit_NVCC_EXECUTABLE)
  message(FATAL_ERROR "nvcc not found; cannot build CUDA proof binaries")
endif()

set(CU_OBJ "${CMAKE_CURRENT_BINARY_DIR}/fabric_cuda.obj")
add_custom_command(
  OUTPUT "${CU_OBJ}"
  COMMAND "${CUDAToolkit_NVCC_EXECUTABLE}"
          -x cu -c "${CMAKE_CURRENT_SOURCE_DIR}/src/fabric_cuda.cu" -o "${CU_OBJ}"
          -std=c++17 -gencode arch=compute_120,code=sm_120 -DWIN32 -D_WINDOWS
          -Xcompiler=/EHsc -Xcompiler=/W4 -Xcompiler=/MD -I"${CMAKE_CURRENT_SOURCE_DIR}/include"
  DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/src/fabric_cuda.cu"
  COMMENT "nvcc fabric_cuda.cu")

add_executable(cuda_proof src/cuda_proof.cpp "${CU_OBJ}")
target_link_libraries(cuda_proof PRIVATE FabricScheduler::FabricScheduler CUDA::cudart)
target_include_directories(cuda_proof PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/include")
if(MSVC)
  target_compile_options(cuda_proof PRIVATE /W4)
endif()
add_test(NAME cuda_proof COMMAND cuda_proof)