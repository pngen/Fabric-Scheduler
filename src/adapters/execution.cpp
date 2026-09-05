#include "fabric_scheduler/adapters/execution.hpp"
namespace fabric {
ExecutionHandoff ReferenceExecutionAdapter::authorize(const PlacementPlan& plan) {
    ExecutionHandoff h;
    h.authorityGeneration = plan.authorityGeneration;
    h.authorized = (plan.authorityGeneration == accepted_);
    return h;
}
}  // namespace fabric
