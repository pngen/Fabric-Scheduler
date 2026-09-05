#pragma once
// Narrow Resource Broker interface. The broker owns actual scarce-resource
// arbitration; Fabric Scheduler only requests/commits bundles and never assumes
// that a score implies an allocation.
#include "fabric_scheduler/core/generations.hpp"
#include "fabric_scheduler/candidate.hpp"
#include "fabric_scheduler/placement.hpp"
#include <string>
#include <vector>

namespace fabric {

struct ResourceCommitResult {
    bool succeeded{false};
    std::string commitmentToken;        // opaque handle used to release
    ResourceGeneration resourceGeneration;
    std::string failureReason;
    bool provisional{false};
};

class ResourceBroker {
public:
    virtual ~ResourceBroker() = default;
    // Attempt an atomic multi-resource commitment. On failure nothing is
    // reserved; the caller must not proceed to execute the placement.
    virtual ResourceCommitResult commit(const std::vector<ResourceClaim>& claims) = 0;
    // Release prior claim(s) by token, per the owning runtime's contract.
    virtual void release(const std::string& commitmentToken) = 0;
};

}  // namespace fabric
