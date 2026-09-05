#pragma once
// Narrow Communication Planner interface. The planner owns path construction;
// Fabric Scheduler supplies candidate placements and consumes cost/feasibility
// evidence. It never computes routes.
#include "fabric_scheduler/candidate.hpp"
#include "fabric_scheduler/request.hpp"
#include <string>

namespace fabric {

class CommunicationPlanner {
public:
    virtual ~CommunicationPlanner() = default;
    virtual CommunicationEvidence plan(const Candidate& candidate,
                                       const DemandProfile& demand) = 0;
};

// Deterministic reference adapter used when no real planner is attached. Values
// are DERIVED from explicit, supplied inputs - never fabricated measurements.
class ReferenceCommunicationPlanner final : public CommunicationPlanner {
public:
    // cost: deterministic per-candidate communication cost that the caller sets.
    explicit ReferenceCommunicationPlanner(Cost costPerCandidate) : cost_(costPerCandidate) {}
    CommunicationEvidence plan(const Candidate& candidate, const DemandProfile& demand) override;
private:
    Cost cost_;
};

}  // namespace fabric
