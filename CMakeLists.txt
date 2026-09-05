cmake_minimum_required(VERSION 3.20)
project(FabricScheduler VERSION 1.0.0 LANGUAGES CXX)

# ---------------------------------------------------------------------------
# Global configuration
# ---------------------------------------------------------------------------
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Build type" FORCE)
endif()

option(FABRIC_BUILD_TESTS "Build the test suite" ON)
option(FABRIC_BUILD_EXAMPLES "Build runnable examples" ON)
option(FABRIC_BUILD_BENCHMARKS "Build benchmarks" ON)
option(FABRIC_BUILD_TOOLS "Build CLI inspection tooling" ON)
option(FABRIC_BUILD_CUDA "Build CUDA proof binaries (requires CUDA toolkit)" ON)
option(FABRIC_ENABLE_ASAN "Enable AddressSanitizer for first-party builds" OFF)

if(MSVC)
  add_compile_options(/W4 /WX /utf-8 /EHsc)
  add_compile_options(/permissive-)
  if(FABRIC_ENABLE_ASAN)
    add_compile_options(/fsanitize=address)
    add_link_options(/fsanitize=address)
  endif()
else()
  add_compile_options(-Wall -Wextra -Wpedantic -Werror)
endif()

# ---------------------------------------------------------------------------
# FabricScheduler library
# ---------------------------------------------------------------------------
set(FABRIC_LIBRARY_SOURCES
  src/core.cpp
  src/request.cpp
  src/scheduler.cpp
  src/candidate.cpp
  src/eligibility.cpp
  src/ranking.cpp
  src/placement.cpp
  src/revalidation.cpp
  src/policy.cpp
  src/persistence.cpp
  src/protocol.cpp
  src/net.cpp
  src/coordinator.cpp
  src/worker.cpp
  src/adapters/resource_broker.cpp
  src/adapters/communication.cpp
  src/adapters/congestion.cpp
  src/adapters/residency.cpp
  src/adapters/execution.cpp
)

add_library(FabricScheduler STATIC ${FABRIC_LIBRARY_SOURCES})
add_library(FabricScheduler::FabricScheduler ALIAS FabricScheduler)

target_include_directories(FabricScheduler PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>
)
target_compile_features(FabricScheduler PUBLIC cxx_std_20)
if(WIN32)
  target_link_libraries(FabricScheduler PRIVATE ws2_32)
endif()

set_target_properties(FabricScheduler PROPERTIES
  VERSION ${PROJECT_VERSION}
  SOVERSION ${PROJECT_VERSION_MAJOR}
  EXPORT_NAME FabricScheduler
)

if(FABRIC_BUILD_CUDA)
  add_subdirectory(cuda)
endif()

include(CTest)
if(BUILD_TESTING)
  add_subdirectory(tests)
endif()
if(FABRIC_BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()
if(FABRIC_BUILD_BENCHMARKS)
  add_subdirectory(benchmarks)
endif()
if(FABRIC_BUILD_TOOLS)
  add_subdirectory(tools)
endif()

# ---------------------------------------------------------------------------
# Install / packaging
# ---------------------------------------------------------------------------
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

install(TARGETS FabricScheduler
  EXPORT FabricSchedulerTargets
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
  INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

install(EXPORT FabricSchedulerTargets
  FILE FabricSchedulerTargets.cmake
  NAMESPACE FabricScheduler::
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/FabricScheduler
)

configure_package_config_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/cmake/FabricSchedulerConfig.cmake.in
  ${CMAKE_CURRENT_BINARY_DIR}/FabricSchedulerConfig.cmake
  INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/FabricScheduler
)
write_basic_package_version_file(
  ${CMAKE_CURRENT_BINARY_DIR}/FabricSchedulerConfigVersion.cmake
  VERSION ${PROJECT_VERSION}
  COMPATIBILITY SameMajorVersion
)
install(FILES
  ${CMAKE_CURRENT_BINARY_DIR}/FabricSchedulerConfig.cmake
  ${CMAKE_CURRENT_BINARY_DIR}/FabricSchedulerConfigVersion.cmake
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/FabricScheduler
)