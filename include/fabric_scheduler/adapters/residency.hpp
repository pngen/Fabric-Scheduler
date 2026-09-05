#pragma once
// Narrow residency/state-locality evidence source. The scheduler consumes
// movement cost and residency state; it never absorbs the residency runtime.
#include "fabric_scheduler/candidate.hpp"
#include "fabric_scheduler/request.hpp"

namespace fabric {

class ResidencySource {
public:
    virtual ~ResidencySource() = default;
    virtual ResidencyEvidence get(const Candidate& candidate, const DemandProfile& demand) = 0;
};

// Deterministic reference residency source fed by explicit evidence.
class ReferenceResidencySource final : public ResidencySource {
public:
    explicit ReferenceResidencySource(ResidencyEvidence evidence) : evidence_(evidence) {}
    ResidencyEvidence get(const Candidate&, const DemandProfile&) override { return evidence_; }
private:
    ResidencyEvidence evidence_;
};

}  // namespace fabric
