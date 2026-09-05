#pragma once
#include "fabric_scheduler/core/generations.hpp"
#include "fabric_scheduler/core/types.hpp"
#include "fabric_scheduler/core/units.hpp"
#include "fabric_scheduler/request.hpp"
#include "fabric_scheduler/candidate.hpp"
#include "fabric_scheduler/policy.hpp"
#include <vector>
#include <cstdint>
#include <string>

namespace fabric {

// A single named, inspectable ranking factor and its weight/provenance.
struct RankFactor {
    RankFactorKind kind{RankFactorKind::PolicyPreference};
    Cost value{0.0};
    double weight{0.0};
    Provenance provenance{Provenance::Unknown};
    std::string description;
};

// Options that parameterize ranking (weights + policy preference hooks).
struct RankOptions {
    RankingWeights weights;
    PolicyGeneration policyGeneration;
    bool preferFaultDiversity{false};
    std::string note;
};

// The ranked output for one candidate. Every field is surfaced for explanation
// and for deterministic tie-breaking.
struct RankedCandidate {
    CandidateId candidateId;
    CandidateGeneration candidateGeneration;
    Cost score{0.0};
    std::vector<RankFactor> factors;   // in canonical kRankFactorOrder
    std::uint32_t unknownCount{0};     // fewer is better
    std::uint8_t evidenceStrength{0};  // higher is better
    Cost movementCost{0.0};
    Cost headroomCost{0.0};
    Cost congestionCost{0.0};
    std::uint32_t localResources{0};
    bool deterministic{false};         // true when no UNKNOWN evidence influenced the rank
};

// Deterministic comparator implementing the documented tie-break chain. For
// equal weighted score we order by: fewer unknowns, stronger evidence, lower
// movement cost, more headroom, lower congestion, more local resources, then
// stable strong-ID ordering. No unordered-container iteration is involved.
class RankComparator {
public:
    bool operator()(const RankedCandidate& a, const RankedCandidate& b) const noexcept;
};

// Evidence-strength rank (higher is better). MEASURED/REPORTED > DERIVED >
// ESTIMATED/FORECAST > SYNTHETIC > UNKNOWN. This is used for tie-breaking and
// for deciding whether a run is "deterministic" w.r.t. evidence.
inline constexpr std::uint8_t provenance_strength(Provenance p) noexcept {
    switch (p) {
        case Provenance::Measured:   return 6;
        case Provenance::Reported:   return 5;
        case Provenance::Derived:    return 4;
        case Provenance::Estimated:  return 3;
        case Provenance::Forecast:   return 2;
        case Provenance::Synthetic:  return 1;
        case Provenance::Unknown:    return 0;
    }
    return 0;
}

// Computes a deterministic total score and per-factor breakdown for an
// eligible candidate. Pure: reads request + candidate + options.
class Ranker {
public:
    RankedCandidate evaluate(const SchedulingRequest& request,
                             const Candidate& candidate,
                             const RankOptions& options) const;

private:
    Cost compute_factor(RankFactorKind kind, const SchedulingRequest& request,
                        const Candidate& candidate, Provenance& outProvenance,
                        std::string& outDescription) const;
};

}  // namespace fabric