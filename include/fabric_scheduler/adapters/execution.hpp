#pragma once
// Narrow Execution Fabric interface. Execution Fabric owns execution-attempt
// authority; the scheduler hands off a plan and only Execution Fabric may start
// real work. A plan holding a handoff is not an execution attempt.
#include "fabric_scheduler/placement.hpp"
#include "fabric_scheduler/candidate.hpp"

namespace fabric {

class ExecutionFabric {
public:
    virtual ~ExecutionFabric() = default;
    // Request fresh execution authority for a plan. Returns whether the current
    // authority generation allows launch.
    virtual ExecutionHandoff authorize(const PlacementPlan& plan) = 0;
};

// Deterministic reference execution adapter that honours the plan's authority
// generation and rejects stale plans before any real work would begin.
class ReferenceExecutionAdapter final : public ExecutionFabric {
public:
    explicit ReferenceExecutionAdapter(AuthorityGeneration acceptedAuthority)
        : accepted_(acceptedAuthority) {}
    ExecutionHandoff authorize(const PlacementPlan& plan) override;
private:
    AuthorityGeneration accepted_;
};

}  // namespace fabric
