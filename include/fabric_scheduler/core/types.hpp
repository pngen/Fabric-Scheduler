#pragma once
#include <cstdint>

namespace fabric {

// ---------------------------------------------------------------------------
// Evidence provenance. Every cost/evidence component must declare how it was
// obtained so callers never mistake an estimate for a measurement.
// ---------------------------------------------------------------------------
enum class Provenance : std::uint8_t {
    Measured,     // obtained from a direct observation
    Reported,     // reported by an autonomous source of truth
    Derived,      // computed deterministically from other evidence
    Estimated,    // extrapolated/interpolated from available evidence
    Forecast,     // predicts a future state, not guaranteed
    Synthetic,    // constructed for a controlled/scenario test
    Unknown       // no known provenance; never treated as favorable
};

inline constexpr const char* to_string(Provenance p) noexcept {
    switch (p) {
        case Provenance::Measured:   return "MEASURED";
        case Provenance::Reported:   return "REPORTED";
        case Provenance::Derived:    return "DERIVED";
        case Provenance::Estimated:  return "ESTIMATED";
        case Provenance::Forecast:   return "FORECAST";
        case Provenance::Synthetic:  return "SYNTHETIC";
        case Provenance::Unknown:    return "UNKNOWN";
    }
    return "UNKNOWN";
}

// ---------------------------------------------------------------------------
// Resource classes the scheduler reasons about. These are the *kinds* of
// scarce resource a candidate consumes; the scheduler does not arbitrate them.
// ---------------------------------------------------------------------------
enum class ResourceKind : std::uint8_t {
    Accelerator,
    CpuCore,
    HostMemory,
    PinnedMemory,
    Nic,
    Storage,
    LinkBandwidth,
    ModelResidency
};

inline constexpr const char* to_string(ResourceKind k) noexcept {
    switch (k) {
        case ResourceKind::Accelerator:    return "ACCELERATOR";
        case ResourceKind::CpuCore:        return "CPU_CORE";
        case ResourceKind::HostMemory:     return "HOST_MEMORY";
        case ResourceKind::PinnedMemory:   return "PINNED_MEMORY";
        case ResourceKind::Nic:            return "NIC";
        case ResourceKind::Storage:        return "STORAGE";
        case ResourceKind::LinkBandwidth:  return "LINK_BANDWIDTH";
        case ResourceKind::ModelResidency: return "MODEL_RESIDENCY";
    }
    return "UNKNOWN";
}

// ---------------------------------------------------------------------------
// Rank factor identities. Ranking is decomposed into named, inspectable
// factors rather than a single opaque score.
// ---------------------------------------------------------------------------
enum class RankFactorKind : std::uint16_t {
    AcceleratorFit,
    ResourceHeadroom,
    VramHeadroom,
    CpuHeadroom,
    PinnedMemoryHeadroom,
    TopologyLocality,
    NumaLocality,
    PcieLocality,
    NicLocality,
    StorageLocality,
    CommunicationCost,
    CongestionPenalty,
    ResidualBandwidth,
    ModelResidency,
    AdapterResidency,
    ReusableStateLocality,
    CheckpointMovementCost,
    StartupCost,
    ReservationFit,
    FailureDomainDiversification,
    HealthMargin,
    EnergyPower,
    FragmentationImpact,
    ExternalPriority,
    MigrationRecomputeCost,
    EstimatedQueueing,
    PolicyPreference
};

inline constexpr const char* to_string(RankFactorKind k) noexcept {
    switch (k) {
        case RankFactorKind::AcceleratorFit:              return "ACCELERATOR_FIT";
        case RankFactorKind::ResourceHeadroom:            return "RESOURCE_HEADROOM";
        case RankFactorKind::VramHeadroom:                return "VRAM_HEADROOM";
        case RankFactorKind::CpuHeadroom:                 return "CPU_HEADROOM";
        case RankFactorKind::PinnedMemoryHeadroom:        return "PINNED_MEMORY_HEADROOM";
        case RankFactorKind::TopologyLocality:            return "TOPOLOGY_LOCALITY";
        case RankFactorKind::NumaLocality:                return "NUMA_LOCALITY";
        case RankFactorKind::PcieLocality:                return "PCIE_LOCALITY";
        case RankFactorKind::NicLocality:                 return "NIC_LOCALITY";
        case RankFactorKind::StorageLocality:             return "STORAGE_LOCALITY";
        case RankFactorKind::CommunicationCost:           return "COMMUNICATION_COST";
        case RankFactorKind::CongestionPenalty:           return "CONGESTION_PENALTY";
        case RankFactorKind::ResidualBandwidth:           return "RESIDUAL_BANDWIDTH";
        case RankFactorKind::ModelResidency:              return "MODEL_RESIDENCY";
        case RankFactorKind::AdapterResidency:            return "ADAPTER_RESIDENCY";
        case RankFactorKind::ReusableStateLocality:       return "REUSABLE_STATE_LOCALITY";
        case RankFactorKind::CheckpointMovementCost:      return "CHECKPOINT_MOVEMENT_COST";
        case RankFactorKind::StartupCost:                 return "STARTUP_COST";
        case RankFactorKind::ReservationFit:              return "RESERVATION_FIT";
        case RankFactorKind::FailureDomainDiversification:return "FAILURE_DOMAIN_DIVERSIFICATION";
        case RankFactorKind::HealthMargin:                return "HEALTH_MARGIN";
        case RankFactorKind::EnergyPower:                 return "ENERGY_POWER";
        case RankFactorKind::FragmentationImpact:         return "FRAGMENTATION_IMPACT";
        case RankFactorKind::ExternalPriority:            return "EXTERNAL_PRIORITY";
        case RankFactorKind::MigrationRecomputeCost:      return "MIGRATION_RECOMPUTE_COST";
        case RankFactorKind::EstimatedQueueing:           return "ESTIMATED_QUEUEING";
        case RankFactorKind::PolicyPreference:            return "POLICY_PREFERENCE";
    }
    return "UNKNOWN";
}

}  // namespace fabric
