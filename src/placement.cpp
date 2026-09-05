#include "fabric_scheduler/placement.hpp"
#include "fabric_scheduler/core/error.hpp"
#include <string>

namespace fabric {

namespace {

bool is_terminal_state(PlacementLifecycleState s) {
    return s == PlacementLifecycleState::SUPERSEDED ||
           s == PlacementLifecycleState::CANCELLED ||
           s == PlacementLifecycleState::FAILED ||
           s == PlacementLifecycleState::RETIRED;
}

// Explicit transition table. Forward transitions are linear through the
// pipeline; terminal absorbs and revalidation is reachable from every
// non-terminal state. Anything not listed is illegal.
bool step(PlacementLifecycleState from, PlacementLifecycleState to) {
    // A state may always keep itself (idempotent no-op). placement_advance and
    // the scheduler both short-circuit identical-state transitions, so this only
    // affects the public placement_can_transition predicate's self-transition.
    if (from == to) return true;
    switch (from) {
        case PlacementLifecycleState::REQUESTED:
            return to == PlacementLifecycleState::DISCOVERING ||
                   to == PlacementLifecycleState::CANCELLED ||
                   to == PlacementLifecycleState::SUPERSEDED ||
                   to == PlacementLifecycleState::FAILED;
        case PlacementLifecycleState::DISCOVERING:
            return to == PlacementLifecycleState::FILTERING ||
                   to == PlacementLifecycleState::REVALIDATION_REQUIRED ||
                   to == PlacementLifecycleState::CANCELLED ||
                   to == PlacementLifecycleState::SUPERSEDED ||
                   to == PlacementLifecycleState::FAILED;
        case PlacementLifecycleState::FILTERING:
            return to == PlacementLifecycleState::RANKING ||
                   to == PlacementLifecycleState::REVALIDATION_REQUIRED ||
                   to == PlacementLifecycleState::CANCELLED ||
                   to == PlacementLifecycleState::SUPERSEDED ||
                   to == PlacementLifecycleState::FAILED;
        case PlacementLifecycleState::RANKING:
            return to == PlacementLifecycleState::PLAN_READY ||
                   to == PlacementLifecycleState::REVALIDATION_REQUIRED ||
                   to == PlacementLifecycleState::CANCELLED ||
                   to == PlacementLifecycleState::SUPERSEDED ||
                   to == PlacementLifecycleState::FAILED;
        case PlacementLifecycleState::PLAN_READY:
            return to == PlacementLifecycleState::AWAITING_RESOURCE_COMMIT ||
                   to == PlacementLifecycleState::REVALIDATION_REQUIRED ||
                   to == PlacementLifecycleState::CANCELLED ||
                   to == PlacementLifecycleState::SUPERSEDED ||
                   to == PlacementLifecycleState::FAILED;
        case PlacementLifecycleState::AWAITING_RESOURCE_COMMIT:
            return to == PlacementLifecycleState::COMMITTED ||
                   to == PlacementLifecycleState::REVALIDATION_REQUIRED ||
                   to == PlacementLifecycleState::CANCELLED ||
                   to == PlacementLifecycleState::SUPERSEDED ||
                   to == PlacementLifecycleState::FAILED;
        case PlacementLifecycleState::COMMITTED:
            return to == PlacementLifecycleState::AWAITING_EXECUTION ||
                   to == PlacementLifecycleState::REVALIDATION_REQUIRED ||
                   to == PlacementLifecycleState::SUPERSEDED ||
                   to == PlacementLifecycleState::FAILED ||
                   to == PlacementLifecycleState::CANCELLED;
        case PlacementLifecycleState::AWAITING_EXECUTION:
            return to == PlacementLifecycleState::ACTIVE ||
                   to == PlacementLifecycleState::REVALIDATION_REQUIRED ||
                   to == PlacementLifecycleState::SUPERSEDED ||
                   to == PlacementLifecycleState::CANCELLED ||
                   to == PlacementLifecycleState::FAILED;
        case PlacementLifecycleState::ACTIVE:
            return to == PlacementLifecycleState::ACTIVE ||
                   to == PlacementLifecycleState::REVALIDATION_REQUIRED ||
                   to == PlacementLifecycleState::SUPERSEDED ||
                   to == PlacementLifecycleState::CANCELLED ||
                   to == PlacementLifecycleState::FAILED ||
                   to == PlacementLifecycleState::RETIRED;
        case PlacementLifecycleState::REVALIDATION_REQUIRED:
            return to == PlacementLifecycleState::PLAN_READY ||
                   to == PlacementLifecycleState::FILTERING ||
                   to == PlacementLifecycleState::RANKING ||
                   to == PlacementLifecycleState::AWAITING_RESOURCE_COMMIT ||
                   to == PlacementLifecycleState::SUPERSEDED ||
                   to == PlacementLifecycleState::CANCELLED ||
                   to == PlacementLifecycleState::FAILED ||
                   to == PlacementLifecycleState::RETIRED;
        case PlacementLifecycleState::SUPERSEDED:
        case PlacementLifecycleState::CANCELLED:
        case PlacementLifecycleState::FAILED:
            return to == PlacementLifecycleState::RETIRED;
        case PlacementLifecycleState::RETIRED:
            return false;
    }
    return false;
}

}  // namespace

bool placement_can_transition(PlacementLifecycleState from, PlacementLifecycleState to) {
    return step(from, to);
}

void placement_advance(PlacementPlan& plan, PlacementLifecycleState to,
                       std::uint64_t eventOrdinal,
                       std::uint64_t& nextOrdinal) {
    if (plan.state == to) return;
    if (!step(plan.state, to)) {
        throw FabricError(ErrorCode::IllegalLifecycleTransition,
                          "illegal placement lifecycle transition " +
                              std::string(to_string(plan.state)) + " -> " +
                              std::string(to_string(to)));
    }
    const std::uint64_t ordinal = (eventOrdinal != 0) ? eventOrdinal : nextOrdinal++;
    plan.state = to;
    plan.stateHistory.emplace_back(to, ordinal);
}

}  // namespace fabric
