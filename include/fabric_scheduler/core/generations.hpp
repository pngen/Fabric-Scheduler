#pragma once
// All strongly typed identities and generations the scheduler reasons about.
// These types are distinct at compile time: a TopologyGeneration can never be
// passed where a CapacityGeneration is required, and vice versa.
#include "fabric_scheduler/core/id.hpp"

// The FABRIC_STRONG_ID macro opens namespace fabric itself, so these are
// declared at global scope.
FABRIC_STRONG_ID(SchedulingRequestId)
FABRIC_STRONG_ID(SchedulingRequestGeneration)
FABRIC_STRONG_ID(SchedulingDecisionId)
FABRIC_STRONG_ID(SchedulingDecisionGeneration)
FABRIC_STRONG_ID(PlacementPlanId)
FABRIC_STRONG_ID(PlacementPlanGeneration)
FABRIC_STRONG_ID(CandidateId)
FABRIC_STRONG_ID(CandidateGeneration)
FABRIC_STRONG_ID(PlacementId)
FABRIC_STRONG_ID(PlacementGeneration)
FABRIC_STRONG_ID(WorkloadId)
FABRIC_STRONG_ID(WorkloadGeneration)
FABRIC_STRONG_ID(WorkloadDemandId)
FABRIC_STRONG_ID(WorkloadDemandGeneration)
FABRIC_STRONG_ID(ExecutionId)
FABRIC_STRONG_ID(ExecutionGeneration)
FABRIC_STRONG_ID(AttemptId)
FABRIC_STRONG_ID(AttemptGeneration)
FABRIC_STRONG_ID(ResourceId)
FABRIC_STRONG_ID(ResourceGeneration)
FABRIC_STRONG_ID(ResourcePoolId)
FABRIC_STRONG_ID(ResourcePoolGeneration)
FABRIC_STRONG_ID(DeviceId)
FABRIC_STRONG_ID(DeviceGeneration)
FABRIC_STRONG_ID(NodeId)
FABRIC_STRONG_ID(NodeGeneration)
FABRIC_STRONG_ID(CpuDomainId)
FABRIC_STRONG_ID(CpuDomainGeneration)
FABRIC_STRONG_ID(MemoryDomainId)
FABRIC_STRONG_ID(MemoryDomainGeneration)
FABRIC_STRONG_ID(NicId)
FABRIC_STRONG_ID(NicGeneration)
FABRIC_STRONG_ID(StorageEndpointId)
FABRIC_STRONG_ID(StorageEndpointGeneration)
FABRIC_STRONG_ID(LinkId)
FABRIC_STRONG_ID(LinkGeneration)
FABRIC_STRONG_ID(PathId)
FABRIC_STRONG_ID(PathGeneration)
FABRIC_STRONG_ID(TopologyGeneration)
FABRIC_STRONG_ID(CapabilityGeneration)
FABRIC_STRONG_ID(HealthGeneration)
FABRIC_STRONG_ID(CapacityGeneration)
FABRIC_STRONG_ID(ReservationGeneration)
FABRIC_STRONG_ID(CongestionGeneration)
FABRIC_STRONG_ID(CommunicationPlanGeneration)
FABRIC_STRONG_ID(CollectiveGeneration)
FABRIC_STRONG_ID(ResidencyGeneration)
FABRIC_STRONG_ID(DependencyGeneration)
FABRIC_STRONG_ID(PolicyGeneration)
FABRIC_STRONG_ID(PriorityGeneration)
FABRIC_STRONG_ID(SloGeneration)
FABRIC_STRONG_ID(CoordinatorEpoch)
FABRIC_STRONG_ID(WorkerId)
FABRIC_STRONG_ID(WorkerBootId)
FABRIC_STRONG_ID(SourceId)
FABRIC_STRONG_ID(SourceBootId)
FABRIC_STRONG_ID(AuthorityGeneration)
FABRIC_STRONG_ID(RevalidationGeneration)

namespace fabric {

using GenerationValue = std::uint64_t;

}  // namespace fabric
