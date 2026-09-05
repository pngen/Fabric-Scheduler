#pragma once
#include "fabric_scheduler/core/generations.hpp"
#include "fabric_scheduler/core/types.hpp"
#include "fabric_scheduler/core/units.hpp"
#include "fabric_scheduler/request.hpp"
#include <cstdint>
#include <string>
#include <vector>

namespace fabric {

// ---------------------------------------------------------------------------
// A single device binding inside a candidate. Binds resource identities and
// their generations plus the physical locality the scheduler may rely on.
// ---------------------------------------------------------------------------
struct DeviceBinding {
    DeviceId deviceId;
    DeviceGeneration deviceGeneration;
    ResourceId resourceId;
    ResourceGeneration resourceGeneration;
    NodeId nodeId;
    NodeGeneration nodeGeneration;
    MemoryDomainId memoryDomainId;
    MemoryDomainGeneration memoryDomainGeneration;
    NicId nicId;
    NicGeneration nicGeneration;
    StorageEndpointId storageId;
    StorageEndpointGeneration storageGeneration;
    CpuDomainId cpuDomainId;
    CpuDomainGeneration cpuDomainGeneration;

    std::string pcieRoot;       // PCIe root complex identifier (not inferred from names).
    bool sharesPcieRoot{false}; // true if another binding shares this root.
    AcceleratorClass capability{AcceleratorClass::None};
    std::int32_t cudaCompatibility{0};  // CUDA compute capability major*10+minor, 0 if N/A
    bool capabilityCurrent{false};
    CapabilityGeneration capabilityGeneration;
};

// ---------------------------------------------------------------------------
// Capacity evidence snapshot.
// ---------------------------------------------------------------------------
struct CapacityEvidence {
    CapacityGeneration generation;
    ByteCount vramTotal{0};
    ByteCount vramUsed{0};
    ByteCount vramAvailable{0};
    ByteCount hostMemoryAvailable{0};
    ByteCount pinnedMemoryAvailable{0};
    CoreCount cpuAvailable{0};
    ByteCount storageAvailable{0};
    ByteCount nicBandwidthAvailable{0};
    bool contiguousMemory{false};
    Provenance provenance{Provenance::Unknown};
};

// ---------------------------------------------------------------------------
// Health evidence snapshot.
// ---------------------------------------------------------------------------
struct HealthEvidence {
    HealthGeneration generation;
    bool ready{false};
    double healthScore{1.0};   // 0.0 .. 1.0, lower means degraded
    std::string degradationReason;
    Provenance provenance{Provenance::Unknown};
};

// ---------------------------------------------------------------------------
// Reservation evidence snapshot.
// ---------------------------------------------------------------------------
struct ReservationEvidence {
    ReservationGeneration generation;
    bool protectedResource{false};   // a live reservation protects this candidate resource
    ByteCount reservedBytes{0};
    std::string reservationId;
    Provenance provenance{Provenance::Unknown};
};

// ---------------------------------------------------------------------------
// Congestion evidence.
// ---------------------------------------------------------------------------
struct CongestionEvidence {
    CongestionGeneration generation;
    double congestion{0.0};          // 0.0 (clear) .. 1.0 (fully congested)
    double residualBandwidth{1.0};   // fraction of nominal remaining
    ByteCount bandwidthNominal{0};
    std::string bottleneck;
    Provenance provenance{Provenance::Unknown};
};

// ---------------------------------------------------------------------------
// Locality evidence derived from supplied topology (never from naming).
// ---------------------------------------------------------------------------
struct LocalityEvidence {
    TopologyGeneration topologyGeneration;
    std::uint32_t numaDistance{0};   // 0 = same NUMA node
    std::uint32_t pcieHops{0};
    bool sharesPcieRoot{false};
    std::uint32_t nicDistance{0};
    std::uint32_t storageDistance{0};
    std::string topologyGroup;       // named topology group identifier
    Provenance provenance{Provenance::Unknown};
};

// ---------------------------------------------------------------------------
// Communication-cost evidence supplied by the Communication Planner boundary.
// ---------------------------------------------------------------------------
struct CommunicationEvidence {
    CommunicationPlanGeneration planGeneration;
    Cost estimatedCost{0.0};
    Cost derivedPathCost{0.0};
    Cost collectiveCost{0.0};
    bool pathFeasible{true};
    std::string planId;
    Provenance provenance{Provenance::Unknown};
};

// ---------------------------------------------------------------------------
// Residency / state-locality evidence.
// ---------------------------------------------------------------------------
struct ResidencyEvidence {
    ResidencyGeneration residencyGeneration;
    bool modelResident{false};
    bool adapterResident{false};
    bool stateLocal{false};
    ByteCount moveBytes{0};          // bytes that must move to use this candidate
    std::string moveSource;
    Provenance provenance{Provenance::Unknown};
};

// ---------------------------------------------------------------------------
// A placement candidate. A candidate is a first-class, generation-bound bundle
// of resource identities plus the evidence used to decide its eligibility and
// rank. It never holds a hidden mutable reference to external evidence.
// ---------------------------------------------------------------------------
struct Candidate {
    CandidateId candidateId;
    CandidateGeneration candidateGeneration;

    SourceId sourceId;
    SourceBootId sourceBootId;
    WorkerId workerId;
    WorkerBootId workerBootId;
    Provenance provenance{Provenance::Synthetic};

    std::vector<DeviceBinding> bindings;

    CapacityEvidence capacity;
    HealthEvidence health;
    ReservationEvidence reservation;
    CongestionEvidence congestion;
    LocalityEvidence locality;
    CommunicationEvidence communication;
    ResidencyEvidence residency;
    PolicyGeneration policyGeneration;

    // Raw headroom fields surfaced for ranking.
    Cost memoryMovementCost{0.0};

    // Deterministic stable key: the strongly-typed candidate id is the final
    // tie-break, but a numeric fingerprint is used for fast indexing.
    CandidateId stableKey() const noexcept { return candidateId; }
};

}  // namespace fabric