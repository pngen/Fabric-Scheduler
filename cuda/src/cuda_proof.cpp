#include "fabric_scheduler/scheduler.hpp"
#include "fabric_scheduler/adapters/resource_broker.hpp"
#include "fabric_scheduler/adapters/execution.hpp"
#include "fabric_scheduler/adapters/dependency.hpp"
#include "fabric_scheduler/revalidation.hpp"
#include "fabric_cuda.h"
#include <cassert>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <sstream>

using namespace fabric;

namespace {

class OkBroker final : public ResourceBroker {
public:
    ResourceCommitResult commit(const std::vector<ResourceClaim>& claims) override {
        ResourceCommitResult r; r.succeeded = true; r.commitmentToken = "prove";
        r.resourceGeneration = claims.empty() ? ResourceGeneration(1) : claims.front().resourceGeneration;
        return r;
    }
    void release(const std::string&) override {}
};

// Accepts the scheduler's own authority gate; the scheduler lifecycle rejects
// a stale handoff before the adapter is reached.
class AcceptExec final : public ExecutionFabric {
public:
    ExecutionHandoff authorize(const PlacementPlan& plan) override {
        ExecutionHandoff h;
        h.authorityGeneration = plan.authorityGeneration;
        h.executionId = ExecutionId(1);
        h.executionGeneration = ExecutionGeneration(1);
        h.attemptId = AttemptId(1);
        h.attemptGeneration = AttemptGeneration(1);
        h.authorized = true;
        return h;
    }
};

Candidate make_real_candidate(DeviceId id, const FabricCudaDeviceInfo& info,
                              WorkerBootId wb, SourceBootId sb,
                              CapacityGeneration capGen, DeviceGeneration devGen,
                              CongestionGeneration congGen, CandidateGeneration candGen) {
    Candidate c;
    c.candidateId = CandidateId(100);
    c.candidateGeneration = candGen;
    c.workerId = WorkerId(id.value());
    c.workerBootId = wb;
    c.sourceId = SourceId(id.value() + 9000);
    c.sourceBootId = sb;
    c.provenance = Provenance::Measured;
    DeviceBinding b;
    b.deviceId = id; b.deviceGeneration = devGen;
    b.resourceId = ResourceId(id.value()); b.resourceGeneration = ResourceGeneration(1);
    b.nodeId = NodeId(1); b.nodeGeneration = NodeGeneration(1);
    b.memoryDomainId = MemoryDomainId(1); b.memoryDomainGeneration = MemoryDomainGeneration(1);
    b.nicId = NicId(1); b.nicGeneration = NicGeneration(1);
    b.storageId = StorageEndpointId(1); b.storageGeneration = StorageEndpointGeneration(1);
    b.cpuDomainId = CpuDomainId(1); b.cpuDomainGeneration = CpuDomainGeneration(1);
    b.capability = AcceleratorClass::Cuda;
    b.cudaCompatibility = info.major * 10 + info.minor;   // 120 for sm_120
    b.capabilityCurrent = true;
    b.capabilityGeneration = CapabilityGeneration(1);
    c.bindings.push_back(b);
    c.capacity.generation = capGen;
    c.capacity.vramTotal = ByteCount(info.totalBytes);
    c.capacity.vramAvailable = ByteCount(info.freeBytes);
    c.capacity.hostMemoryAvailable = 32_GiB;
    c.capacity.pinnedMemoryAvailable = 1_GiB;
    c.capacity.cpuAvailable = CoreCount(16);
    c.capacity.storageAvailable = 64_GiB;
    c.capacity.nicBandwidthAvailable = 4_GiB;
    c.capacity.contiguousMemory = true;
    c.capacity.provenance = Provenance::Measured;
    c.health.generation = HealthGeneration(1); c.health.ready = true; c.health.healthScore = 1.0;
    c.health.provenance = Provenance::Measured;
    c.reservation.generation = ReservationGeneration(1); c.reservation.provenance = Provenance::Reported;
    c.congestion.generation = congGen; c.congestion.congestion = 0.0; c.congestion.residualBandwidth = 1.0;
    c.congestion.provenance = Provenance::Measured;
    c.locality.topologyGeneration = TopologyGeneration(1);
    c.locality.numaDistance = 0; c.locality.pcieHops = 1; c.locality.provenance = Provenance::Derived;
    c.communication.planGeneration = CommunicationPlanGeneration(1); c.communication.provenance = Provenance::Derived;
    c.residency.residencyGeneration = ResidencyGeneration(1); c.residency.stateLocal = true;
    c.residency.modelResident = true; c.residency.provenance = Provenance::Reported;
    c.policyGeneration = PolicyGeneration(1);
    return c;
}

SchedulingRequest make_request(SchedulingRequestId rid, WorkloadDemandId did) {
    SchedulingRequest r;
    r.requestId = rid; r.generation = SchedulingRequestGeneration(1);
    r.coordinatorEpoch = CoordinatorEpoch(1);
    r.sourceId = SourceId(3); r.sourceBootId = SourceBootId(30);
    r.demand.demandId = did; r.demand.demandGeneration = WorkloadDemandGeneration(1);
    r.demand.workloadId = WorkloadId(4); r.demand.workloadGeneration = WorkloadGeneration(1);
    r.demand.accelerator.capability = AcceleratorClass::Cuda;
    r.demand.accelerator.minimumCount = CoreCount(1);
    r.demand.accelerator.targetCount = CoreCount(1);
    r.demand.accelerator.maximumCount = CoreCount(1);
    r.demand.accelerator.vramPerDevice = 512_MiB;
    r.demand.accelerator.capabilityGeneration = CapabilityGeneration(1);
    r.demand.memory.hostMemory = 1_GiB; r.demand.memory.pinnedMemory = 64_MiB;
    r.demand.cpuRequired = CoreCount(2);
    r.demand.storage.capacity = 1_GiB;
    r.demand.policyGeneration = PolicyGeneration(1);
    r.demand.requiredTopologyGeneration = TopologyGeneration(1);
    r.demand.latencySensitivity = LatencySensitivity::High;
    return r;
}

}  // namespace

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);
    try {
    int device = 0;
    const int count = fs_cuda_count();
    if (count < 1) { std::printf("no cuda device\n"); return 2; }
    FabricCudaDeviceInfo info;
    const int pr = fs_cuda_probe(device, &info);
    if (pr != 0 || !info.present) { std::printf("cuda probe failed (%d)\n", pr); return 3; }
    std::printf("discover: device=%s cc=%d.%d total=%llu free=%llu\n",
                info.name, info.major, info.minor, info.totalBytes, info.freeBytes);

    unsigned long long baselineBefore = 0;
    fs_cuda_memfree(device, &baselineBefore);

    Scheduler::Config cfg; cfg.policy.generation = PolicyGeneration(1);
    cfg.maxCandidatesPerRequest = CoreCount(16);
    Scheduler sched(cfg);
    sched.ingest_candidate(make_real_candidate(DeviceId(1), info, WorkerBootId(10), SourceBootId(20),
                                               CapacityGeneration(1), DeviceGeneration(1), CongestionGeneration(1), CandidateGeneration(6)));

    AlwaysReadyDependency dep;
    AllowAllPolicy pol;
    SchedulingRequest req = make_request(SchedulingRequestId(1), WorkloadDemandId(1));
    PlacementDecision d = sched.schedule(req, &dep, &pol);
    if (!d.producedPlan) { std::printf("no plan produced\n"); return 4; }
    if (d.plan.selectedCandidate != CandidateId(100)) { std::printf("wrong candidate\n"); return 5; }
    std::printf("schedule selected candidate 100 (physical RTX 5090)\n");

    OkBroker broker;
    ResourceCommitResult cr = sched.commit(d.plan.planId, broker);
    if (!cr.succeeded) { std::printf("commit failed: %s\n", cr.failureReason.c_str()); return 6; }
    std::printf("resource commit succeeded\n");

    AcceptExec exec;
    ExecutionHandoff h = sched.handoff(d.plan.planId, exec);
    if (!h.authorized) { std::printf("handoff not authorized\n"); return 7; }
    std::printf("execution handoff authorized\n");

    // Real CUDA work, gated by the placement.
    const unsigned long long allocBytes = 256ull * 1024 * 1024;  // 256 MiB
    double parity = 0.0;
    const int er = fs_cuda_exec(device, allocBytes, &parity);
    if (er != 0) { std::printf("cuda exec failed (%d)\n", er); return 8; }
    std::printf("cuda kernel parity: %.1f\n", parity);
    if (parity != 1.0) { std::printf("parity mismatch\n"); return 9; }

    unsigned long long baselineAfter = 0;
    fs_cuda_memfree(device, &baselineAfter);
    if (baselineAfter + (4ull << 20) < baselineBefore) {
        std::printf("device memory not restored: before=%llu after=%llu\n", baselineBefore, baselineAfter);
        return 10;
    }
    std::printf("device memory restored to baseline: before=%llu after=%llu\n", baselineBefore, baselineAfter);

    // --- stale CUDA placement proof -------------------------------------
    auto plan = sched.plan(d.plan.planId);
    GenerationFingerprint current = plan->generations;
    current.capacity = CapacityGeneration(2);
    sched.revalidate(d.plan.planId, current);
    if (sched.plan_state(d.plan.planId) != PlacementLifecycleState::REVALIDATION_REQUIRED) {
        std::printf("plan did not become REVALIDATION_REQUIRED\n"); return 11;
    }
    ExecutionHandoff staleHand = sched.handoff(d.plan.planId, exec);
    if (staleHand.authorized) { std::printf("stale plan handoff MUST be rejected\n"); return 12; }
    std::printf("stale placement handoff rejected before CUDA launch\n");

    // Fresh scheduling under a new generation succeeds and runs real CUDA.
    sched.ingest_candidate(make_real_candidate(DeviceId(1), info, WorkerBootId(11), SourceBootId(21),
                                               CapacityGeneration(2), DeviceGeneration(2), CongestionGeneration(2), CandidateGeneration(7)));
    PlacementDecision d2 = sched.schedule(make_request(SchedulingRequestId(2), WorkloadDemandId(2)), &dep, &pol);
    if (!d2.producedPlan) { std::printf("fresh plan missing\n"); return 13; }
    OkBroker broker2; sched.commit(d2.plan.planId, broker2);
    ExecutionHandoff h2 = sched.handoff(d2.plan.planId, exec);
    if (!h2.authorized) { std::printf("fresh handoff not authorized\n"); return 14; }
    double parity2 = 0.0;
    if (fs_cuda_exec(device, allocBytes, &parity2) != 0 || parity2 != 1.0) {
        std::printf("fresh cuda exec failed\n"); return 15;
    }
    std::printf("fresh CUDA work passed after revalidation\n");
    std::printf("CUDA physical proof PASS\n");
    return 0;
    } catch (const std::exception& ex) { std::printf("exception: %s\n", ex.what()); return 99; }
}