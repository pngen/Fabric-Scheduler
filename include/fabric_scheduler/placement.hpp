#pragma once
#include "fabric_scheduler/core/generations.hpp"
#include "fabric_scheduler/core/types.hpp"
#include "fabric_scheduler/core/units.hpp"
#include "fabric_scheduler/request.hpp"
#include "fabric_scheduler/candidate.hpp"
#include "fabric_scheduler/ranking.hpp"
#include "fabric_scheduler/eligibility.hpp"
#include <vector>
#include <string>
#include <cstdint>

namespace fabric {

// ---------------------------------------------------------------------------
// Guarded placement lifecycle.
// ---------------------------------------------------------------------------
enum class PlacementLifecycleState : std::uint8_t {
    REQUESTED,
    DISCOVERING,
    FILTERING,
    RANKING,
    PLAN_READY,
    AWAITING_RESOURCE_COMMIT,
    COMMITTED,
    AWAITING_EXECUTION,
    ACTIVE,
    REVALIDATION_REQUIRED,
    SUPERSEDED,
    CANCELLED,
    FAILED,
    RETIRED
};

inline constexpr const char* to_string(PlacementLifecycleState s) noexcept {
    switch (s) {
        case PlacementLifecycleState::REQUESTED:              return "REQUESTED";
        case PlacementLifecycleState::DISCOVERING:            return "DISCOVERING";
        case PlacementLifecycleState::FILTERING:              return "FILTERING";
        case PlacementLifecycleState::RANKING:                return "RANKING";
        case PlacementLifecycleState::PLAN_READY:             return "PLAN_READY";
        case PlacementLifecycleState::AWAITING_RESOURCE_COMMIT:return "AWAITING_RESOURCE_COMMIT";
        case PlacementLifecycleState::COMMITTED:              return "COMMITTED";
        case PlacementLifecycleState::AWAITING_EXECUTION:     return "AWAITING_EXECUTION";
        case PlacementLifecycleState::ACTIVE:                 return "ACTIVE";
        case PlacementLifecycleState::REVALIDATION_REQUIRED:  return "REVALIDATION_REQUIRED";
        case PlacementLifecycleState::SUPERSEDED:             return "SUPERSEDED";
        case PlacementLifecycleState::CANCELLED:              return "CANCELLED";
        case PlacementLifecycleState::FAILED:                 return "FAILED";
        case PlacementLifecycleState::RETIRED:                return "RETIRED";
    }
    return "UNKNOWN";
}

inline constexpr bool is_terminal(PlacementLifecycleState s) noexcept {
    return s == PlacementLifecycleState::SUPERSEDED ||
           s == PlacementLifecycleState::CANCELLED ||
           s == PlacementLifecycleState::FAILED ||
           s == PlacementLifecycleState::RETIRED;
}

// A resource-claim intent submitted to the Resource Broker (which owns actual
// arbitration). The scheduler never treats a desired bundle as committed.
struct ResourceClaim {
    ResourceId resourceId;
    ResourceGeneration resourceGeneration;
    ResourceKind kind;
    ByteCount amount{0};
    bool provisional{true};
};

// One rejected alternative and its primary reason, recorded for explanation.
struct RejectedAlternative {
    CandidateId candidateId;
    CandidateGeneration candidateGeneration;
    EligibilityStatus status{EligibilityStatus::UNKNOWN};
    std::string reason;
};

// The set of generations a plan depends on. On revalidation every entry must
// still be current; any divergence makes the plan stale.
struct GenerationFingerprint {
    TopologyGeneration topology;
    CapacityGeneration capacity;
    ReservationGeneration reservation;
    CongestionGeneration congestion;
    CapabilityGeneration capability;
    HealthGeneration health;
    ResidencyGeneration residency;
    CommunicationPlanGeneration communicationPlan;
    PolicyGeneration policy;
    ResourceGeneration resource;
    DependencyGeneration dependency;
    CoordinatorEpoch coordinatorEpoch;

    bool equal(const GenerationFingerprint& o) const noexcept {
        return topology == o.topology && capacity == o.capacity &&
               reservation == o.reservation && congestion == o.congestion &&
               capability == o.capability && health == o.health &&
               residency == o.residency && communicationPlan == o.communicationPlan &&
               policy == o.policy && resource == o.resource &&
               dependency == o.dependency && coordinatorEpoch == o.coordinatorEpoch;
    }
};

// Execution handoff requirements. Holding these does not mean execution will
// start; Execution Fabric must grant fresh authority.
struct ExecutionHandoff {
    ExecutionId executionId;
    ExecutionGeneration executionGeneration;
    AttemptId attemptId;
    AttemptGeneration attemptGeneration;
    AuthorityGeneration authorityGeneration;
    bool authorized{false};
};

// ---------------------------------------------------------------------------
// Placement plan.
// ---------------------------------------------------------------------------
struct PlacementPlan {
    PlacementPlanId planId;
    PlacementPlanGeneration planGeneration;

    SchedulingRequestId requestId;
    SchedulingRequestGeneration requestGeneration;

    CandidateId selectedCandidate;
    CandidateGeneration selectedCandidateGeneration;
    std::vector<CandidateId> fallbackCandidates;   // deterministic order

    std::vector<DeviceBinding> bindings;
    std::vector<ResourceClaim> claims;

    CommunicationPlanGeneration communicationPlan;
    std::string communicationPlanId;

    // movement / residency prerequisites
    ByteCount movementBytes{0};
    std::string movementSource;
    bool residencyPrerequisite{false};

    std::vector<RankFactor> selectedFactors;       // winning candidate factors
    std::vector<RejectedAlternative> rejectedAlternatives;

    GenerationFingerprint generations;
    PolicyGeneration policyGeneration;
    AuthorityGeneration authorityGeneration;

    PlacementLifecycleState state{PlacementLifecycleState::REQUESTED};
    std::vector<std::pair<PlacementLifecycleState, std::uint64_t>> stateHistory;

    // authority for committing this plan (set by the scheduler's authority)
    CoordinatorEpoch coordinatorEpoch;

    // why the plan was superseded/cancelled (when applicable)
    PlacementPlanId supersededBy;
    std::string supersessionReason;

    // Whether resource commitment evidence is recorded (for COMMITTED state).
    bool hasCommitmentEvidence{false};
};

// ---------------------------------------------------------------------------
// Revalidation outcome. Reports exactly which generation moved.
// ---------------------------------------------------------------------------
struct RevalidationResult {
    bool current{false};
    std::vector<std::string> changes;   // e.g. "capacity generation advanced"
    PlacementLifecycleState resultingState{PlacementLifecycleState::REQUESTED};
};


// Free functions that implement the guarded lifecycle transition table.
// placement_advance throws FabricError(IllegalLifecycleTransition) on an
// illegal transition and records the transition into the plan's history.
bool placement_can_transition(PlacementLifecycleState from, PlacementLifecycleState to);
void placement_advance(PlacementPlan& plan, PlacementLifecycleState to,
                       std::uint64_t eventOrdinal, std::uint64_t& nextOrdinal);

}  // namespace fabric
