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

# The object is built per configuration with the C runtime of that
# configuration: a Debug executable cannot link an object compiled against the
# release runtime (LNK2038).
set(CU_OBJ_DIR "${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_CFG_INTDIR}")
set(CU_OBJ "${CU_OBJ_DIR}/fabric_cuda.obj")
add_custom_command(
  OUTPUT "${CU_OBJ}"
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${CU_OBJ_DIR}"
  COMMAND "${CUDAToolkit_NVCC_EXECUTABLE}"
          -x cu -c "${CMAKE_CURRENT_SOURCE_DIR}/src/fabric_cuda.cu" -o "${CU_OBJ}"
          -std=c++17 -gencode arch=compute_120,code=sm_120 -DWIN32 -D_WINDOWS
          -Xcompiler=/EHsc -Xcompiler=/W4 -Xcompiler=$<IF:$<CONFIG:Debug>,/MDd,/MD>
          -I"${CMAKE_CURRENT_SOURCE_DIR}/include"
  DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/src/fabric_cuda.cu"
  COMMENT "nvcc fabric_cuda.cu")

# One target owns the generated object. Listing the object as a source of both
# executables attached the nvcc rule to each of them, and a parallel build ran
# it twice at once on the same output file.
add_library(fabric_cuda_object STATIC "${CU_OBJ}")
set_target_properties(fabric_cuda_object PROPERTIES LINKER_LANGUAGE CXX)

add_executable(cuda_proof src/cuda_proof.cpp)
target_link_libraries(cuda_proof PRIVATE FabricScheduler::FabricScheduler fabric_cuda_object CUDA::cudart)
target_include_directories(cuda_proof PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/include")
if(MSVC)
  target_compile_options(cuda_proof PRIVATE /W4)
endif()
add_test(NAME cuda_proof COMMAND cuda_proof)

add_executable(cuda_worker src/cuda_worker_main.cpp)
target_link_libraries(cuda_worker PRIVATE FabricScheduler::FabricScheduler fabric_cuda_object CUDA::cudart)
target_include_directories(cuda_worker PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/include")
if(MSVC)
  target_compile_options(cuda_worker PRIVATE /W4)
endif()

# Integrated real CUDA worker-death scheduling proof driver. Host-only (it spawns
# the coordinator and CUDA worker OS processes), so it links the library but not
# the CUDA runtime directly.
add_executable(cuda_worker_death_proof src/cuda_worker_death_proof.cpp)
target_link_libraries(cuda_worker_death_proof PRIVATE FabricScheduler::FabricScheduler)
add_dependencies(cuda_worker_death_proof cuda_worker)
if(MSVC)
  target_compile_options(cuda_worker_death_proof PRIVATE /W4)
endif()
add_test(NAME cuda_worker_death_proof COMMAND cuda_worker_death_proof)
