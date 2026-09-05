#pragma once
// Narrow Congestion Fabric interface. Congestion Fabric owns live congestion and
// backpressure evidence; the scheduler consumes it to penalize/prioritize cost.
#include "fabric_scheduler/candidate.hpp"

namespace fabric {

class CongestionFabric {
public:
    virtual ~CongestionFabric() = default;
    virtual CongestionEvidence current(const Candidate& candidate) = 0;
};

}  // namespace fabric
