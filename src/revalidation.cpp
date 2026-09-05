#include "fabric_scheduler/revalidation.hpp"
#include "fabric_scheduler/placement.hpp"
#include "fabric_scheduler/core/error.hpp"

namespace fabric {

RevalidationResult revalidate_plan(const PlacementPlan& plan,
                                   const GenerationFingerprint& current) {
    RevalidationResult r;
    r.current = plan.generations.equal(current);
    r.resultingState = plan.state;

    if (plan.generations.topology != current.topology)
        r.changes.emplace_back("topology generation advanced");
    if (plan.generations.capacity != current.capacity)
        r.changes.emplace_back("capacity generation advanced");
    if (plan.generations.reservation != current.reservation)
        r.changes.emplace_back("reservation generation advanced");
    if (plan.generations.congestion != current.congestion)
        r.changes.emplace_back("congestion generation advanced");
    if (plan.generations.capability != current.capability)
        r.changes.emplace_back("capability generation advanced");
    if (plan.generations.health != current.health)
        r.changes.emplace_back("health generation advanced");
    if (plan.generations.residency != current.residency)
        r.changes.emplace_back("residency generation advanced");
    if (plan.generations.communicationPlan != current.communicationPlan)
        r.changes.emplace_back("communication-plan generation advanced");
    if (plan.generations.policy != current.policy)
        r.changes.emplace_back("policy generation advanced");
    if (plan.generations.resource != current.resource)
        r.changes.emplace_back("resource generation advanced");
    if (plan.generations.dependency != current.dependency)
        r.changes.emplace_back("dependency generation advanced");
    if (plan.generations.coordinatorEpoch != current.coordinatorEpoch)
        r.changes.emplace_back("coordinator epoch advanced");

    if (!r.current) {
        r.resultingState = PlacementLifecycleState::REVALIDATION_REQUIRED;
    }
    return r;
}

}  // namespace fabric
