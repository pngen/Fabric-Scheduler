#include "fabric_scheduler/adapters/communication.hpp"
namespace fabric {
CommunicationEvidence ReferenceCommunicationPlanner::plan(const Candidate&, const DemandProfile&) {
    CommunicationEvidence e;
    e.planGeneration = CommunicationPlanGeneration(1);
    e.estimatedCost = cost_;
    e.derivedPathCost = Cost(cost_.value());
    e.pathFeasible = true;
    e.planId = "reference-derived";
    e.provenance = Provenance::Derived;
    return e;
}
}  // namespace fabric
