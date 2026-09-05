#pragma once
#include "fabric_scheduler/core/generations.hpp"
#include "fabric_scheduler/core/types.hpp"
#include "fabric_scheduler/core/units.hpp"
#include "fabric_scheduler/core/error.hpp"
#include <cstdint>
#include <string>
#include <vector>

namespace fabric {

// ---------------------------------------------------------------------------
// Capability class of an accelerator. The scheduler gates candidates by the
// capability class the workload requires.
// ---------------------------------------------------------------------------
enum class AcceleratorClass : std::uint8_t {
    None,
    Cuda,            // NVIDIA CUDA (compute capability >= a floor)
    Rocm,            // AMD ROCm
    Metal,           // Apple Metal
    Vulkan,          // Cross-vendor Vulkan
    NoneRequired     // CPU-only workload
};

inline constexpr const char* to_string(AcceleratorClass c) noexcept {
    switch (c) {
        case AcceleratorClass::None:            return "NONE";
        case AcceleratorClass::Cuda:            return "CUDA";
        case AcceleratorClass::Rocm:            return "ROCM";
        case AcceleratorClass::Metal:           return "METAL";
        case AcceleratorClass::Vulkan:          return "VULKAN";
        case AcceleratorClass::NoneRequired:    return "NONE_REQUIRED";
    }
    return "UNKNOWN";
}

// ---------------------------------------------------------------------------
// Communication pattern. Influences the communication-cost factor but is not
// a substitute for a communication plan.
// ---------------------------------------------------------------------------
enum class CommunicationPattern : std::uint8_t {
    None,
    PointToPoint,
    AllReduce,
    AllGather,
    ReduceScatter,
    AllToAll,
    Broadcast,
    Gather,
    Scatter
};

inline constexpr const char* to_string(CommunicationPattern p) noexcept {
    switch (p) {
        case CommunicationPattern::None:          return "NONE";
        case CommunicationPattern::PointToPoint:  return "POINT_TO_POINT";
        case CommunicationPattern::AllReduce:     return "ALL_REDUCE";
        case CommunicationPattern::AllGather:     return "ALL_GATHER";
        case CommunicationPattern::ReduceScatter: return "REDUCE_SCATTER";
        case CommunicationPattern::AllToAll:      return "ALL_TO_ALL";
        case CommunicationPattern::Broadcast:     return "BROADCAST";
        case CommunicationPattern::Gather:        return "GATHER";
        case CommunicationPattern::Scatter:       return "SCATTER";
    }
    return "UNKNOWN";
}

// ---------------------------------------------------------------------------
// Latency sensitivity / throughput objective.
// ---------------------------------------------------------------------------
enum class LatencySensitivity : std::uint8_t {
    Low, Medium, High, Critical
};

inline constexpr const char* to_string(LatencySensitivity s) noexcept {
    switch (s) {
        case LatencySensitivity::Low:      return "LOW";
        case LatencySensitivity::Medium:   return "MEDIUM";
        case LatencySensitivity::High:     return "HIGH";
        case LatencySensitivity::Critical: return "CRITICAL";
    }
    return "UNKNOWN";
}

// ---------------------------------------------------------------------------
// Failure-domain constraint. Expresses how many distinct failure domains the
// placement must span and whether colocating replicas is forbidden.
// ---------------------------------------------------------------------------
struct FailureDomainConstraint {
    CoreCount minimumDistinctDomains{0};   // 0 = no requirement
    bool avoidColocation{false};           // replicas must not share a domain
    bool avoidSingleRoot{false};           // must not share a constrained root (PCIe switch etc.)
};

// ---------------------------------------------------------------------------
// Accelerator requirement.
// ---------------------------------------------------------------------------
struct AcceleratorRequirement {
    AcceleratorClass capability{AcceleratorClass::None};
    CoreCount minimumCount{0};
    CoreCount targetCount{0};
    CoreCount maximumCount{0};
    ByteCount vramPerDevice{0};
    ByteCount aggregateVram{0};
    bool requiresContiguousMemory{false};
    // A required capability generation to compare against candidate capability.
    CapabilityGeneration capabilityGeneration;
};

// ---------------------------------------------------------------------------
// Memory requirement (host + pinned).
// ---------------------------------------------------------------------------
struct MemoryRequirement {
    ByteCount hostMemory{0};
    ByteCount pinnedMemory{0};
};

// ---------------------------------------------------------------------------
// Storage requirement.
// ---------------------------------------------------------------------------
struct StorageRequirement {
    ByteCount capacity{0};
    bool requireLocal{false};
};

// ---------------------------------------------------------------------------
// Network requirement.
// ---------------------------------------------------------------------------
struct NetworkRequirement {
    ByteCount bandwidth{0};
    bool requireNic{false};
};

// ---------------------------------------------------------------------------
// Residency / state-locality requirement.
// ---------------------------------------------------------------------------
struct ResidencyRequirement {
    bool requireModelResident{false};
    bool requireAdapterResident{false};
    bool requireStateLocal{false};
    std::string modelPath;
    std::string adapterPath;
    std::string statePath;
};

// ---------------------------------------------------------------------------
// Locality preference (soft/ranked, but a required-ancestor is hard).
// ---------------------------------------------------------------------------
enum class LocalityKind : std::uint8_t {
    Numa, Pcie, Nic, Storage, Topology
};
inline constexpr const char* to_string(LocalityKind k) noexcept {
    switch (k) {
        case LocalityKind::Numa:    return "NUMA";
        case LocalityKind::Pcie:    return "PCIE";
        case LocalityKind::Nic:     return "NIC";
        case LocalityKind::Storage: return "STORAGE";
        case LocalityKind::Topology:return "TOPOLOGY";
    }
    return "UNKNOWN";
}

// Number of locality kinds (5).
inline constexpr std::size_t kLocalityKindCount = 5;

// ---------------------------------------------------------------------------
// Demand profile. Carries everything a placement request can express.
// ---------------------------------------------------------------------------
struct DemandProfile {
    WorkloadDemandId demandId;
    WorkloadDemandGeneration demandGeneration;
    WorkloadId workloadId;
    WorkloadGeneration workloadGeneration;

    AcceleratorRequirement accelerator;
    CoreCount cpuRequired{0};
    MemoryRequirement memory;
    StorageRequirement storage;
    NetworkRequirement network;
    TopologyGeneration requiredTopologyGeneration;   // 0 = no hard topology requirement

    CommunicationPattern communication{CommunicationPattern::None};
    ByteCount communicationBytes{0};

    ResidencyRequirement residency;
    FailureDomainConstraint failureDomains;

    Duration expectedExecutionDuration{0};
    LatencySensitivity latencySensitivity{LatencySensitivity::Low};
    // throughput objective in units-per-second, 0 = unspecified
    double throughputObjective{0.0};

    int externalPriority{0};
    std::uint64_t externalPriorityOrdinal{0};
    SloGeneration sloGeneration;

    PolicyGeneration policyGeneration;
    PriorityGeneration priorityGeneration;

    // Maximum acceptable state/checkpoint movement cost (bytes). 0 = no cap.
    ByteCount maximumMovementCost{0};

    // Allowed fallback classes: which candidate classes may substitute.
    std::vector<LocalityKind> fallbackLocalities;

    Provenance provenance{Provenance::Reported};

    // Validate the demand. Throws FabricError for malformed/invalid demand.
    void validate() const;
};

// ---------------------------------------------------------------------------
// A submitted scheduling request.
// ---------------------------------------------------------------------------
struct SchedulingRequest {
    SchedulingRequestId requestId;
    SchedulingRequestGeneration generation;
    DemandProfile demand;
    CoordinatorEpoch coordinatorEpoch;
    SourceId sourceId;
    SourceBootId sourceBootId;

    // Submission time ordinal used for stable ordering of concurrent requests.
    std::uint64_t submitOrdinal{0};
};

}  // namespace fabric