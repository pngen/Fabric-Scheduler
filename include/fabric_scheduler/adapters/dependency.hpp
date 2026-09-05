#pragma once
// Narrow Dependency Fabric interface. Fabric Scheduler never absorbs dependency
// readiness; it consults this view and rejects candidates whose dependencies are
// not ready.
#include "fabric_scheduler/core/generations.hpp"
#include <string>

namespace fabric {

class DependencyView {
public:
    virtual ~DependencyView() = default;

    // Returns true when all dependencies for the given workload/demand are ready
    // and the dependency generation is current.
    virtual bool ready(WorkloadDemandId demand) const = 0;
    virtual DependencyGeneration generation() const = 0;
    virtual std::string unsatisfied(WorkloadDemandId demand) const = 0;
};

// A permissive default view: all dependencies are ready.
class AlwaysReadyDependency final : public DependencyView {
public:
    bool ready(WorkloadDemandId) const override { return true; }
    DependencyGeneration generation() const override { return DependencyGeneration(1); }
    std::string unsatisfied(WorkloadDemandId) const override { return {}; }
};

}  // namespace fabric
