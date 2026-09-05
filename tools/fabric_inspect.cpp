#include <fabric_scheduler/scheduler.hpp>
#include <fabric_scheduler/adapters/dependency.hpp>
#include <fabric_scheduler/adapters/resource_broker.hpp>
#include <fabric_scheduler/adapters/execution.hpp>
#include <cstdio>
#include <string>
#include <vector>

using namespace fabric;

namespace {
Candidate mk(CandidateId id, std::uint32_t numa, ByteCount vram, ByteCount host,
             bool unready, bool protectedResource, double cong) {
    Candidate c;
    c.candidateId = id; c.candidateGeneration = CandidateGeneration(1);
    c.workerId = WorkerId(id.value()); c.workerBootId = WorkerBootId(id.value() + 100);
    c.sourceId = SourceId(id.value() + 200); c.sourceBootId = SourceBootId(id.value() + 300);
    c.provenance = Provenance::Reported;
    DeviceBinding b;
    b.deviceId = DeviceId(id.value()); b.resourceGeneration = ResourceGeneration(1);
    b.memoryDomainId = MemoryDomainId(id.value());
    b.capability = AcceleratorClass::Cuda; b.cudaCompatibility = 120;
    b.capabilityCurrent = true; b.capabilityGeneration = CapabilityGeneration(1);
    c.bindings.push_back(b);
    c.capacity.generation = CapacityGeneration(1);
    c.capacity.vramAvailable = vram; c.capacity.hostMemoryAvailable = host;
    c.capacity.pinnedMemoryAvailable = 1_GiB; c.capacity.cpuAvailable = CoreCount(16);
    c.capacity.storageAvailable = 64_GiB; c.capacity.contiguousMemory = true;
    c.capacity.provenance = Provenance::Measured;
    c.health.generation = HealthGeneration(1); c.health.ready = !unready;
    c.health.healthScore = unready ? 0.0 : 1.0;
    c.health.degradationReason = unready ? "ECC uncorrectable" : "";
    c.health.provenance = Provenance::Reported;
    c.reservation.generation = ReservationGeneration(1); c.reservation.protectedResource = protectedResource;
    c.reservation.provenance = Provenance::Reported;
    c.congestion.generation = CongestionGeneration(1); c.congestion.congestion = cong;
    c.congestion.residualBandwidth = cong > 0.9 ? 0.1 : 1.0 - cong;
    c.congestion.provenance = Provenance::Measured;
    c.locality.topologyGeneration = TopologyGeneration(1); c.locality.numaDistance = numa;
    c.locality.pcieHops = numa == 0 ? 1 : numa + 1; c.locality.provenance = Provenance::Reported;
    c.communication.planGeneration = CommunicationPlanGeneration(1);
    c.communication.estimatedCost = Cost(cong > 0.9 ? 0.8 : 0.0);
    c.communication.provenance = Provenance::Derived;
    c.residency.residencyGeneration = ResidencyGeneration(1); c.residency.stateLocal = (numa == 0);
    c.residency.modelResident = true; c.residency.moveBytes = (numa == 0) ? ByteCount(0) : 512_MiB;
    c.residency.provenance = Provenance::Reported;
    c.policyGeneration = PolicyGeneration(1);
    return c;
}
SchedulingRequest req() {
    SchedulingRequest r;
    r.requestId = SchedulingRequestId(1); r.generation = SchedulingRequestGeneration(1);
    r.demand.demandId = WorkloadDemandId(1); r.demand.demandGeneration = WorkloadDemandGeneration(1);
    r.demand.workloadId = WorkloadId(4); r.demand.workloadGeneration = WorkloadGeneration(1);
    r.demand.accelerator.capability = AcceleratorClass::Cuda;
    r.demand.accelerator.minimumCount = CoreCount(1); r.demand.accelerator.targetCount = CoreCount(1);
    r.demand.accelerator.maximumCount = CoreCount(1); r.demand.accelerator.vramPerDevice = 4_GiB;
    r.demand.accelerator.capabilityGeneration = CapabilityGeneration(1);
    r.demand.memory.hostMemory = 2_GiB; r.demand.memory.pinnedMemory = 64_MiB;
    r.demand.cpuRequired = CoreCount(2); r.demand.storage.capacity = 1_GiB;
    r.demand.policyGeneration = PolicyGeneration(1); r.demand.requiredTopologyGeneration = TopologyGeneration(1);
    return r;
}
}  // namespace

int main() {
    Scheduler::Config cfg; cfg.policy.generation = PolicyGeneration(1); cfg.maxCandidatesPerRequest = CoreCount(16);
    Scheduler sched(cfg);
    sched.ingest_candidate(mk(CandidateId(10), 0, 16_GiB, 16_GiB, false, false, 0.0));
    sched.ingest_candidate(mk(CandidateId(20), 3, 8_GiB, 8_GiB, false, false, 0.0));
    sched.ingest_candidate(mk(CandidateId(30), 0, 16_GiB, 16_GiB, true, false, 0.0));
    sched.ingest_candidate(mk(CandidateId(40), 0, 16_GiB, 16_GiB, false, true, 0.0));
    sched.ingest_candidate(mk(CandidateId(50), 4, 8_GiB, 8_GiB, false, false, 0.95));
    AlwaysReadyDependency dep; AllowAllPolicy pol;
    const PlacementDecision d = sched.schedule(req(), &dep, &pol);
    std::printf("=== Fabric Scheduler inspection ===\n");
    std::printf("produced plan: %s\n", d.producedPlan ? "yes" : "no");
    std::printf("eligible (ranked): ");
    for (const auto c : d.eligible) std::printf("%llu ", (unsigned long long)c.value());
    std::printf("\n\n--- hard rejections ---\n");
    for (const auto& r : d.rejections)
        std::printf("candidate %llu: %s - %s\n", (unsigned long long)r.candidateId.value(), to_string(r.status), r.reason.c_str());
    std::printf("\n");
    if (d.producedPlan) {
        const PlacementPlan& p = d.plan;
        std::printf("selected candidate: %llu (generation %llu), plan %llu/state %s\n",
                    (unsigned long long)p.selectedCandidate.value(),
                    (unsigned long long)p.selectedCandidateGeneration.value(),
                    (unsigned long long)p.planId.value(), to_string(p.state));
        std::printf("fallback order: ");
        for (const auto c : p.fallbackCandidates) std::printf("%llu ", (unsigned long long)c.value());
        std::printf("\nauthority %llu, epoch %llu\n\n--- winning candidate ranking factors ---\n",
                    (unsigned long long)p.authorityGeneration.value(), (unsigned long long)p.coordinatorEpoch.value());
        for (const auto& f : p.selectedFactors)
            std::printf("%-34s = %.4f w=%.2f %s\n", to_string(f.kind), f.value.value(), f.weight, to_string(f.provenance));
        std::printf("\n--- placement explanation ---\n");
        const PlacementExplanation ex = sched.explain(p.planId);
        std::printf("%s\n", ex.why.c_str());
    }
    std::printf("inspection done\n");
    return 0;
}
