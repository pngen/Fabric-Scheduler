#pragma once
#include "fabric_scheduler/core/generations.hpp"
#include "fabric_scheduler/core/types.hpp"
#include "fabric_scheduler/core/units.hpp"
#include "fabric_scheduler/request.hpp"
#include "fabric_scheduler/candidate.hpp"
#include "fabric_scheduler/adapters/dependency.hpp"
#include "fabric_scheduler/policy.hpp"
#include <cstdint>
#include <string>

namespace fabric {

enum class EligibilityStatus : std::uint8_t {
    ELIGIBLE, REJECT_CAPACITY, REJECT_MEMORY, REJECT_CONTIGUITY, REJECT_CAPABILITY,
    REJECT_HEALTH, REJECT_TOPOLOGY, REJECT_NUMA, REJECT_PCIE, REJECT_NETWORK,
    REJECT_STORAGE, REJECT_RESERVATION, REJECT_DEPENDENCY, REJECT_COMPATIBILITY,
    REJECT_FAILURE_DOMAIN, REJECT_POLICY, REVALIDATION_REQUIRED,
    INSUFFICIENT_EVIDENCE, UNKNOWN
};

inline constexpr const char* to_string(EligibilityStatus s) noexcept {
    switch (s) {
        case EligibilityStatus::ELIGIBLE:              return "ELIGIBLE";
        case EligibilityStatus::REJECT_CAPACITY:       return "REJECT_CAPACITY";
        case EligibilityStatus::REJECT_MEMORY:         return "REJECT_MEMORY";
        case EligibilityStatus::REJECT_CONTIGUITY:     return "REJECT_CONTIGUITY";
        case EligibilityStatus::REJECT_CAPABILITY:     return "REJECT_CAPABILITY";
        case EligibilityStatus::REJECT_HEALTH:         return "REJECT_HEALTH";
        case EligibilityStatus::REJECT_TOPOLOGY:       return "REJECT_TOPOLOGY";
        case EligibilityStatus::REJECT_NUMA:           return "REJECT_NUMA";
        case EligibilityStatus::REJECT_PCIE:           return "REJECT_PCIE";
        case EligibilityStatus::REJECT_NETWORK:        return "REJECT_NETWORK";
        case EligibilityStatus::REJECT_STORAGE:        return "REJECT_STORAGE";
        case EligibilityStatus::REJECT_RESERVATION:    return "REJECT_RESERVATION";
        case EligibilityStatus::REJECT_DEPENDENCY:     return "REJECT_DEPENDENCY";
        case EligibilityStatus::REJECT_COMPATIBILITY:  return "REJECT_COMPATIBILITY";
        case EligibilityStatus::REJECT_FAILURE_DOMAIN: return "REJECT_FAILURE_DOMAIN";
        case EligibilityStatus::REJECT_POLICY:         return "REJECT_POLICY";
        case EligibilityStatus::REVALIDATION_REQUIRED: return "REVALIDATION_REQUIRED";
        case EligibilityStatus::INSUFFICIENT_EVIDENCE: return "INSUFFICIENT_EVIDENCE";
        case EligibilityStatus::UNKNOWN:               return "UNKNOWN";
    }
    return "UNKNOWN";
}

inline constexpr bool is_rejected(EligibilityStatus s) noexcept {
    return s != EligibilityStatus::ELIGIBLE && s != EligibilityStatus::REVALIDATION_REQUIRED;
}

struct EligibilityResult {
    EligibilityStatus status{EligibilityStatus::UNKNOWN};
    std::string reason;
    RankFactorKind rejectedFactor{RankFactorKind::PolicyPreference};
    ResourceKind bottleneckResource{ResourceKind::CpuCore};
    DependencyGeneration dependencyGeneration;

    bool eligible() const noexcept { return status == EligibilityStatus::ELIGIBLE; }
};

class HardFilter {
public:
    EligibilityResult evaluate(const SchedulingRequest& request,
                               const Candidate& candidate,
                               const DependencyView* dependency,
                               const PolicyVeto* policy) const;
};

}  // namespace fabric
