#pragma once
#include "fabric_scheduler/core/generations.hpp"
#include "fabric_scheduler/core/types.hpp"
#include "fabric_scheduler/candidate.hpp"
#include "fabric_scheduler/core/error.hpp"
#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>

namespace fabric {

// Number of ranking factor identities.
inline constexpr std::size_t kRankFactorCount = 27;

// Canonical, deterministic iteration order of ranking factors. This is the
// single source of truth for how factors are enumerated for explanation and
// persistence, so ordering is never left to unordered-container iteration.
inline constexpr std::array<RankFactorKind, kRankFactorCount> kRankFactorOrder = {
    RankFactorKind::AcceleratorFit,
    RankFactorKind::ResourceHeadroom,
    RankFactorKind::VramHeadroom,
    RankFactorKind::CpuHeadroom,
    RankFactorKind::PinnedMemoryHeadroom,
    RankFactorKind::TopologyLocality,
    RankFactorKind::NumaLocality,
    RankFactorKind::PcieLocality,
    RankFactorKind::NicLocality,
    RankFactorKind::StorageLocality,
    RankFactorKind::CommunicationCost,
    RankFactorKind::CongestionPenalty,
    RankFactorKind::ResidualBandwidth,
    RankFactorKind::ModelResidency,
    RankFactorKind::AdapterResidency,
    RankFactorKind::ReusableStateLocality,
    RankFactorKind::CheckpointMovementCost,
    RankFactorKind::StartupCost,
    RankFactorKind::ReservationFit,
    RankFactorKind::FailureDomainDiversification,
    RankFactorKind::HealthMargin,
    RankFactorKind::EnergyPower,
    RankFactorKind::FragmentationImpact,
    RankFactorKind::ExternalPriority,
    RankFactorKind::MigrationRecomputeCost,
    RankFactorKind::EstimatedQueueing,
    RankFactorKind::PolicyPreference
};

inline constexpr std::size_t rank_factor_index(RankFactorKind k) noexcept {
    for (std::size_t i = 0; i < kRankFactorCount; ++i) {
        if (kRankFactorOrder[i] == k) return i;
    }
    return kRankFactorCount;  // not found
}

inline constexpr bool is_valid_factor(RankFactorKind k) noexcept {
    return rank_factor_index(k) < kRankFactorCount;
}

// Explicit, inspectable factor weights. Higher weight magnifies a factor's
// contribution to the total (lower is better) cost. Weights are per-factor,
// never hidden inside a single opaque score.
struct RankingWeights {
    std::array<double, kRankFactorCount> weight{};

    constexpr RankingWeights() noexcept {
        weight.fill(0.0);
        weight[rank_factor_index(RankFactorKind::AcceleratorFit)]            = 0.0;
        weight[rank_factor_index(RankFactorKind::ResourceHeadroom)]          = 1.0;
        weight[rank_factor_index(RankFactorKind::VramHeadroom)]              = 1.0;
        weight[rank_factor_index(RankFactorKind::CpuHeadroom)]               = 1.0;
        weight[rank_factor_index(RankFactorKind::PinnedMemoryHeadroom)]      = 1.0;
        weight[rank_factor_index(RankFactorKind::TopologyLocality)]          = 2.0;
        weight[rank_factor_index(RankFactorKind::NumaLocality)]              = 2.0;
        weight[rank_factor_index(RankFactorKind::PcieLocality)]              = 2.0;
        weight[rank_factor_index(RankFactorKind::NicLocality)]               = 1.5;
        weight[rank_factor_index(RankFactorKind::StorageLocality)]           = 1.5;
        weight[rank_factor_index(RankFactorKind::CommunicationCost)]         = 3.0;
        weight[rank_factor_index(RankFactorKind::CongestionPenalty)]         = 2.5;
        weight[rank_factor_index(RankFactorKind::ResidualBandwidth)]         = 1.0;
        weight[rank_factor_index(RankFactorKind::ModelResidency)]            = 3.0;
        weight[rank_factor_index(RankFactorKind::AdapterResidency)]          = 2.0;
        weight[rank_factor_index(RankFactorKind::ReusableStateLocality)]     = 2.0;
        weight[rank_factor_index(RankFactorKind::CheckpointMovementCost)]    = 2.0;
        weight[rank_factor_index(RankFactorKind::StartupCost)]               = 1.0;
        weight[rank_factor_index(RankFactorKind::ReservationFit)]            = 2.0;
        weight[rank_factor_index(RankFactorKind::FailureDomainDiversification)] = 1.5;
        weight[rank_factor_index(RankFactorKind::HealthMargin)]              = 2.0;
        weight[rank_factor_index(RankFactorKind::EnergyPower)]               = 0.5;
        weight[rank_factor_index(RankFactorKind::FragmentationImpact)]       = 1.0;
        weight[rank_factor_index(RankFactorKind::ExternalPriority)]          = 1.0;
        weight[rank_factor_index(RankFactorKind::MigrationRecomputeCost)]    = 1.0;
        weight[rank_factor_index(RankFactorKind::EstimatedQueueing)]         = 1.0;
        weight[rank_factor_index(RankFactorKind::PolicyPreference)]          = 1.0;
    }

    double get(RankFactorKind k) const noexcept {
        const auto i = rank_factor_index(k);
        return i < kRankFactorCount ? weight[i] : 0.0;
    }
};

// Policy configuration that bounds and shapes scheduling behaviour.
struct PolicyConfig {
    PolicyGeneration generation;
    CoreCount maxFallbackCandidates{0};
    CoreCount maxCandidateCount{0};          // per request
    std::uint64_t maxHistoryEntries{0};      // 0 = unbounded-ish
    bool allowEnergyRanking{false};
    bool requireFaultDiversity{false};
};

// Narrow policy veto interface used during hard filtering (REJECT_POLICY).
// The scheduler never absorbs the policy runtime; it only consults this gate.
class PolicyVeto {
public:
    virtual ~PolicyVeto() = default;
    virtual bool allows(const Candidate& candidate) const = 0;
    virtual std::string reason(const Candidate& candidate) const = 0;
};

// A permissive default gate that allows every candidate.
class AllowAllPolicy final : public PolicyVeto {
public:
    bool allows(const Candidate&) const override { return true; }
    std::string reason(const Candidate&) const override { return {}; }
};

}  // namespace fabric
