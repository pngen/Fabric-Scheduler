#include "fabric_scheduler/scheduler.hpp"
#include "fabric_scheduler/adapters/resource_broker.hpp"
#include "fabric_scheduler/adapters/execution.hpp"
#include "fabric_scheduler/adapters/dependency.hpp"
#include "fabric_scheduler/revalidation.hpp"
#include <cassert>
#include <iostream>
#include <string>
#include <vector>

using namespace fabric;

namespace {

class TestBroker final : public ResourceBroker {
public:
    ResourceCommitResult commit(const std::vector<ResourceClaim>& claims) override {
        ResourceCommitResult r;
        if (failNext) {
            r.succeeded = false;
            r.failureReason = "capacity changed at commit time";
            failNext = false;
            return r;
        }
        r.succeeded = true;
        r.commitmentToken = "tok-" + std::to_string(++seq);
        r.resourceGeneration = claims.empty() ? ResourceGeneration(1) : claims.front().resourceGeneration;
        commits++;
        return r;
    }
    void release(const std::string&) override { releases++; }
    bool failNext = false;
    int commits = 0;
    int releases = 0;
    int seq = 0;
};

DeviceBinding make_binding(DeviceId id, MemoryDomainId numa, std::uint32_t pcieHops) {
    (void)pcieHops;
    DeviceBinding b;
    b.deviceId = id;
    b.deviceGeneration = DeviceGeneration(1);
    b.resourceId = ResourceId(id.value());
    b.resourceGeneration = ResourceGeneration(1);
    b.nodeId = NodeId(1);
    b.nodeGeneration = NodeGeneration(1);
    b.memoryDomainId = numa;
    b.memoryDomainGeneration = MemoryDomainGeneration(1);
    b.nicId = NicId(id.value() + 100);
    b.nicGeneration = NicGeneration(1);
    b.storageId = StorageEndpointId(1);
    b.storageGeneration = StorageEndpointGeneration(1);
    b.cpuDomainId = CpuDomainId(1);
    b.cpuDomainGeneration = CpuDomainGeneration(1);
    b.capability = AcceleratorClass::Cuda;
    b.cudaCompatibility = 120;
    b.capabilityCurrent = true;
    b.capabilityGeneration = CapabilityGeneration(1);
    b.sharesPcieRoot = false;
    return b;
}

Candidate make_candidate(CandidateId id, WorkerBootId wb, SourceBootId sb,
                         MemoryDomainId numa, std::uint32_t pcieHops,
                         ByteCount vram, ByteCount cpuHost, double congestion,
                         CongestionGeneration congGen, bool stateLocal) {
    Candidate c;
    c.candidateId = id;
    c.candidateGeneration = CandidateGeneration(1);
    c.workerId = WorkerId(id.value() + 1000);
    c.workerBootId = wb;
    c.sourceId = SourceId(id.value() + 5000);
    c.sourceBootId = sb;
    c.provenance = Provenance::Reported;
    c.bindings.push_back(make_binding(DeviceId(id.value()), numa, pcieHops));
    c.capacity.generation = CapacityGeneration(1);
    c.capacity.vramTotal = 8_GiB;
    c.capacity.vramAvailable = vram;
    c.capacity.hostMemoryAvailable = cpuHost;
    c.capacity.pinnedMemoryAvailable = 4_GiB;
    c.capacity.cpuAvailable = CoreCount(8);
    c.capacity.storageAvailable = 128_GiB;
    c.capacity.nicBandwidthAvailable = 400_GiB;
    c.capacity.contiguousMemory = true;
    c.capacity.provenance = Provenance::Measured;
    c.health.generation = HealthGeneration(1);
    c.health.ready = true;
    c.health.healthScore = 1.0;
    c.health.provenance = Provenance::Reported;
    c.reservation.generation = ReservationGeneration(1);
    c.reservation.provenance = Provenance::Reported;
    c.congestion.generation = congGen;
    c.congestion.congestion = congestion;
    c.congestion.residualBandwidth = congestion > 0.9 ? 0.1 : 1.0;
    c.congestion.provenance = Provenance::Measured;
    c.locality.topologyGeneration = TopologyGeneration(1);
    c.locality.numaDistance = (numa.value() == 1) ? 0 : 2;
    c.locality.pcieHops = pcieHops;
    c.locality.topologyGroup = "group-1";
    c.locality.provenance = Provenance::Reported;
    c.communication.planGeneration = CommunicationPlanGeneration(1);
    c.communication.estimatedCost = Cost(congestion > 0.9 ? 0.9 : 0.0);
    c.communication.derivedPathCost = Cost(congestion > 0.9 ? 0.6 : 0.0);
    c.communication.provenance = Provenance::Derived;
    c.residency.residencyGeneration = ResidencyGeneration(1);
    c.residency.stateLocal = stateLocal;
    c.residency.modelResident = true;
    c.residency.moveBytes = stateLocal ? ByteCount(0) : 512_MiB;
    c.residency.provenance = Provenance::Reported;
    c.policyGeneration = PolicyGeneration(1);
    c.memoryMovementCost = Cost(stateLocal ? 0.0 : 0.4);
    return c;
}

SchedulingRequest make_request(SchedulingRequestId id, WorkloadDemandId demandId) {
    SchedulingRequest r;
    r.requestId = id;
    r.generation = SchedulingRequestGeneration(1);
    r.coordinatorEpoch = CoordinatorEpoch(1);
    r.sourceId = SourceId(3);
    r.sourceBootId = SourceBootId(30);
    r.demand.demandId = demandId;
    r.demand.demandGeneration = WorkloadDemandGeneration(1);
    r.demand.workloadId = WorkloadId(4);
    r.demand.workloadGeneration = WorkloadGeneration(1);
    r.demand.accelerator.capability = AcceleratorClass::Cuda;
    r.demand.accelerator.minimumCount = CoreCount(1);
    r.demand.accelerator.targetCount = CoreCount(1);
    r.demand.accelerator.maximumCount = CoreCount(1);
    r.demand.accelerator.vramPerDevice = 4_GiB;
    r.demand.accelerator.capabilityGeneration = CapabilityGeneration(1);
    r.demand.memory.hostMemory = 1_GiB;
    r.demand.memory.pinnedMemory = 256_MiB;
    r.demand.cpuRequired = CoreCount(2);
    r.demand.storage.capacity = 4_GiB;
    r.demand.storage.requireLocal = true;
    r.demand.policyGeneration = PolicyGeneration(1);
    r.demand.requiredTopologyGeneration = TopologyGeneration(1);
    r.demand.latencySensitivity = LatencySensitivity::High;
    return r;
}

}  // namespace

int main() {
    Scheduler::Config cfg;
    cfg.policy.generation = PolicyGeneration(1);
    cfg.maxCandidatesPerRequest = CoreCount(64);
    Scheduler sched(cfg);

    Candidate a = make_candidate(CandidateId(1), WorkerBootId(10), SourceBootId(20), MemoryDomainId(1), 1, 16_GiB, 16_GiB, 0.0, CongestionGeneration(1), true);
    Candidate b = make_candidate(CandidateId(2), WorkerBootId(11), SourceBootId(21), MemoryDomainId(2), 3, 8_GiB, 8_GiB, 0.0, CongestionGeneration(1), true);
    sched.ingest_candidate(a);
    sched.ingest_candidate(b);
    assert(sched.candidate_count() == 2);

    SchedulingRequest req = make_request(SchedulingRequestId(1), WorkloadDemandId(1));
    AlwaysReadyDependency dep;
    AllowAllPolicy pol;
    PlacementDecision d = sched.schedule(req, &dep, &pol);
    assert(d.producedPlan);
    assert(d.eligible.size() == 2);
    assert(d.plan.selectedCandidate == CandidateId(1));
    std::cout << "schedule selected: " << d.plan.selectedCandidate.value() << "\n";

    TestBroker broker;
    ResourceCommitResult cr = sched.commit(d.plan.planId, broker);
    assert(cr.succeeded);
    assert(sched.plan_state(d.plan.planId) == PlacementLifecycleState::COMMITTED);

    ReferenceExecutionAdapter exec(AuthorityGeneration(1));
    ExecutionHandoff h = sched.handoff(d.plan.planId, exec);
    assert(h.authorized);
    assert(sched.plan_state(d.plan.planId) == PlacementLifecycleState::ACTIVE);

    // congestion race
    auto planA = sched.plan(d.plan.planId);
    assert(planA.has_value());
    GenerationFingerprint current = planA->generations;
    current.congestion = CongestionGeneration(2);
    current.capacity = CapacityGeneration(2);
    RevalidationResult rr = sched.revalidate(d.plan.planId, current);
    assert(!rr.current);
    assert(sched.plan_state(d.plan.planId) == PlacementLifecycleState::REVALIDATION_REQUIRED);

    Candidate a2 = make_candidate(CandidateId(1), WorkerBootId(10), SourceBootId(20), MemoryDomainId(1), 1, 16_GiB, 16_GiB, 0.95, CongestionGeneration(2), true);
    a2.candidateGeneration = CandidateGeneration(2);
    sched.ingest_candidate(a2);
    PlacementDecision d2 = sched.schedule(make_request(SchedulingRequestId(2), WorkloadDemandId(2)), &dep, &pol);
    assert(d2.producedPlan);
    assert(d2.plan.selectedCandidate == CandidateId(2));

    ExecutionHandoff h2 = sched.handoff(d.plan.planId, exec);
    assert(!h2.authorized);
    assert(broker.commits >= 1);

    std::cout << "scheduler pipeline test PASS\n";
    return 0;
}