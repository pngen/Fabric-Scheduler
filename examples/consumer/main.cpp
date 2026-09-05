// Independent downstream consumer of the installed FabricScheduler package.
// It configures via find_package(FabricScheduler CONFIG REQUIRED), links the
// exported target, and produces a deterministic placement result.
#include <fabric_scheduler/scheduler.hpp>
#include <fabric_scheduler/adapters/dependency.hpp>
#include <fabric_scheduler/adapters/resource_broker.hpp>
#include <fabric_scheduler/adapters/execution.hpp>
#include <cstdio>
#include <string>
#include <vector>

using namespace fabric;

namespace {
class OkBroker final : public ResourceBroker {
public:
    ResourceCommitResult commit(const std::vector<ResourceClaim>&) override {
        ResourceCommitResult r; r.succeeded = true; r.commitmentToken = "consumer"; return r;
    }
    void release(const std::string&) override {}
};

Candidate make_candidate(CandidateId id, std::uint32_t numa, ByteCount vram) {
    Candidate c;
    c.candidateId = id;
    c.candidateGeneration = CandidateGeneration(1);
    c.workerId = WorkerId(id.value() + 1000);
    c.workerBootId = WorkerBootId(id.value() + 500);
    c.sourceId = SourceId(id.value() + 7000);
    c.sourceBootId = SourceBootId(id.value() + 9000);
    c.provenance = Provenance::Reported;
    DeviceBinding b;
    b.deviceId = DeviceId(id.value()); b.deviceGeneration = DeviceGeneration(1);
    b.resourceId = ResourceId(id.value()); b.resourceGeneration = ResourceGeneration(1);
    b.nodeId = NodeId(1); b.nodeGeneration = NodeGeneration(1);
    b.memoryDomainId = MemoryDomainId(id.value()); b.memoryDomainGeneration = MemoryDomainGeneration(1);
    b.nicId = NicId(1); b.nicGeneration = NicGeneration(1);
    b.storageId = StorageEndpointId(1); b.storageGeneration = StorageEndpointGeneration(1);
    b.cpuDomainId = CpuDomainId(1); b.cpuDomainGeneration = CpuDomainGeneration(1);
    b.capability = AcceleratorClass::Cuda; b.cudaCompatibility = 120;
    b.capabilityCurrent = true; b.capabilityGeneration = CapabilityGeneration(1);
    c.bindings.push_back(b);
    c.capacity.generation = CapacityGeneration(1);
    c.capacity.vramTotal = 32_GiB; c.capacity.vramAvailable = vram;
    c.capacity.hostMemoryAvailable = 32_GiB; c.capacity.pinnedMemoryAvailable = 1_GiB;
    c.capacity.cpuAvailable = CoreCount(16); c.capacity.storageAvailable = 64_GiB;
    c.capacity.contiguousMemory = true; c.capacity.provenance = Provenance::Measured;
    c.health.generation = HealthGeneration(1); c.health.ready = true; c.health.healthScore = 1.0;
    c.health.provenance = Provenance::Reported;
    c.congestion.generation = CongestionGeneration(1); c.congestion.provenance = Provenance::Measured;
    c.locality.topologyGeneration = TopologyGeneration(1); c.locality.numaDistance = numa;
    c.locality.pcieHops = numa == 0 ? 1 : 3; c.locality.provenance = Provenance::Reported;
    c.communication.planGeneration = CommunicationPlanGeneration(1); c.communication.provenance = Provenance::Derived;
    c.residency.residencyGeneration = ResidencyGeneration(1); c.residency.stateLocal = (numa == 0);
    c.residency.modelResident = true; c.residency.provenance = Provenance::Reported;
    c.policyGeneration = PolicyGeneration(1);
    return c;
}

SchedulingRequest make_request() {
    SchedulingRequest r;
    r.requestId = SchedulingRequestId(1); r.generation = SchedulingRequestGeneration(1);
    r.coordinatorEpoch = CoordinatorEpoch(1);
    r.sourceId = SourceId(3); r.sourceBootId = SourceBootId(30);
    r.demand.demandId = WorkloadDemandId(1); r.demand.demandGeneration = WorkloadDemandGeneration(1);
    r.demand.workloadId = WorkloadId(4); r.demand.workloadGeneration = WorkloadGeneration(1);
    r.demand.accelerator.capability = AcceleratorClass::Cuda;
    r.demand.accelerator.minimumCount = CoreCount(1); r.demand.accelerator.targetCount = CoreCount(1);
    r.demand.accelerator.maximumCount = CoreCount(1); r.demand.accelerator.vramPerDevice = 4_GiB;
    r.demand.accelerator.capabilityGeneration = CapabilityGeneration(1);
    r.demand.memory.hostMemory = 1_GiB; r.demand.memory.pinnedMemory = 64_MiB;
    r.demand.cpuRequired = CoreCount(2); r.demand.storage.capacity = 1_GiB;
    r.demand.policyGeneration = PolicyGeneration(1); r.demand.requiredTopologyGeneration = TopologyGeneration(1);
    return r;
}
}  // namespace

int main() {
    Scheduler::Config cfg; cfg.policy.generation = PolicyGeneration(1);
    Scheduler sched(cfg);
    sched.ingest_candidate(make_candidate(CandidateId(1), 0, 16_GiB));  // local A
    sched.ingest_candidate(make_candidate(CandidateId(2), 3, 8_GiB));   // remote B
    AlwaysReadyDependency dep;
    AllowAllPolicy pol;
    const PlacementDecision d = sched.schedule(make_request(), &dep, &pol);
    if (!d.producedPlan) { std::printf("consumer FAILED: no plan\n"); return 2; }
    const PlacementExplanation ex = sched.explain(d.plan.planId);
    std::printf("consumer selected candidate %llu (deterministic)\n", (unsigned long long)d.plan.selectedCandidate.value());
    std::printf("%s\n", ex.why.c_str());
    OkBroker broker;
    const ResourceCommitResult cr = sched.commit(d.plan.planId, broker);
    if (!cr.succeeded) { std::printf("consumer FAILED: commit\n"); return 3; }
    std::printf("consumer commit ok; state=%s\n", to_string(sched.plan_state(d.plan.planId)));
    std::printf("consumer PASS\n");
    return d.plan.selectedCandidate == CandidateId(1) ? 0 : 4;
}
