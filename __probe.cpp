
#include "fabric_scheduler/scheduler.hpp"
#include "fabric_scheduler/adapters/dependency.hpp"
#include "fabric_scheduler/policy.hpp"
#include <iostream>
#include <unordered_set>
using namespace fabric;

// LiveVeto: reject candidates published by a fenced (dead) worker boot.
struct LiveVeto final : public PolicyVeto {
    explicit LiveVeto(const std::unordered_set<WorkerBootId>* f) : fenced(f) {}
    const std::unordered_set<WorkerBootId>* fenced;
    bool allows(const Candidate& c) const override { return fenced->count(c.workerBootId) == 0; }
    std::string reason(const Candidate& c) const override {
        return fenced->count(c.workerBootId) ? "worker fenced" : "";
    }
};

Candidate mk(CandidateId id, WorkerBootId wb, SourceBootId sb,
             std::uint32_t numa, std::uint32_t pcie, ByteCount vram, bool stateLocal) {
    Candidate c; c.candidateId=id; c.candidateGeneration=CandidateGeneration(1);
    c.workerId=WorkerId(id.value()); c.workerBootId=wb;
    c.sourceId=SourceId(id.value()+5000); c.sourceBootId=sb; c.provenance=Provenance::Reported;
    DeviceBinding b; b.deviceId=DeviceId(id.value()); b.deviceGeneration=DeviceGeneration(1);
    b.resourceId=ResourceId(id.value()); b.resourceGeneration=ResourceGeneration(1);
    b.nodeId=NodeId(1); b.nodeGeneration=NodeGeneration(1); b.memoryDomainId=MemoryDomainId(1);
    b.memoryDomainGeneration=MemoryDomainGeneration(1); b.nicId=NicId(id.value()+100); b.nicGeneration=NicGeneration(1);
    b.storageId=StorageEndpointId(1); b.storageGeneration=StorageEndpointGeneration(1);
    b.cpuDomainId=CpuDomainId(1); b.cpuDomainGeneration=CpuDomainGeneration(1);
    b.capability=AcceleratorClass::Cuda; b.cudaCompatibility=120; b.capabilityCurrent=true;
    b.capabilityGeneration=CapabilityGeneration(1); b.sharesPcieRoot=false;
    c.bindings.push_back(b);
    c.capacity.generation=CapacityGeneration(1); c.capacity.vramTotal=16_GiB; c.capacity.vramAvailable=vram;
    c.capacity.hostMemoryAvailable=16_GiB; c.capacity.pinnedMemoryAvailable=4_GiB; c.capacity.cpuAvailable=CoreCount(8);
    c.capacity.storageAvailable=128_GiB; c.capacity.nicBandwidthAvailable=400_GiB; c.capacity.contiguousMemory=true;
    c.capacity.provenance=Provenance::Measured;
    c.health.generation=HealthGeneration(1); c.health.ready=true; c.health.healthScore=1.0; c.health.provenance=Provenance::Reported;
    c.reservation.generation=ReservationGeneration(1); c.reservation.provenance=Provenance::Reported;
    c.congestion.generation=CongestionGeneration(1); c.congestion.congestion=0.0; c.congestion.residualBandwidth=1.0; c.congestion.provenance=Provenance::Measured;
    c.locality.topologyGeneration=TopologyGeneration(1); c.locality.numaDistance=numa; c.locality.pcieHops=pcie;
    c.locality.topologyGroup="g-1"; c.locality.provenance=Provenance::Reported;
    c.communication.planGeneration=CommunicationPlanGeneration(1); c.communication.provenance=Provenance::Derived;
    c.residency.residencyGeneration=ResidencyGeneration(1); c.residency.stateLocal=stateLocal;
    c.residency.modelResident=true; c.residency.moveBytes=stateLocal?ByteCount(0):512_MiB; c.residency.provenance=Provenance::Reported;
    c.policyGeneration=PolicyGeneration(1); c.memoryMovementCost=Cost(stateLocal?0.0:0.4); return c;
}
SchedulingRequest mkreq(SchedulingRequestId id){
    SchedulingRequest r; r.requestId=id; r.generation=SchedulingRequestGeneration(1);
    r.coordinatorEpoch=CoordinatorEpoch(1); r.sourceId=SourceId(3); r.sourceBootId=SourceBootId(30);
    r.demand.demandId=WorkloadDemandId(1); r.demand.demandGeneration=WorkloadDemandGeneration(1);
    r.demand.workloadId=WorkloadId(4); r.demand.workloadGeneration=WorkloadGeneration(1);
    r.demand.accelerator.capability=AcceleratorClass::Cuda; r.demand.accelerator.minimumCount=CoreCount(1);
    r.demand.accelerator.targetCount=CoreCount(1); r.demand.accelerator.maximumCount=CoreCount(1);
    r.demand.accelerator.vramPerDevice=4_GiB; r.demand.accelerator.capabilityGeneration=CapabilityGeneration(1);
    r.demand.memory.hostMemory=1_GiB; r.demand.memory.pinnedMemory=256_MiB; r.demand.cpuRequired=CoreCount(2);
    r.demand.storage.capacity=4_GiB; r.demand.storage.requireLocal=false;
    r.demand.policyGeneration=PolicyGeneration(1); r.demand.requiredTopologyGeneration=TopologyGeneration(1);
    r.demand.latencySensitivity=LatencySensitivity::High; return r;
}
int main(){
    Scheduler::Config cfg; cfg.policy.generation=PolicyGeneration(1); cfg.maxCandidatesPerRequest=CoreCount(64);
    Scheduler sched(cfg);
    Candidate a = mk(CandidateId(100), WorkerBootId(100), SourceBootId(100100), 0, 1, 16_GiB, true);
    Candidate b = mk(CandidateId(200), WorkerBootId(200), SourceBootId(200200), 2, 3, 8_GiB, false);
    sched.ingest_candidate(a); sched.ingest_candidate(b);
    AlwaysReadyDependency dep;
    std::unordered_set<WorkerBootId> fenced;
    LiveVeto veto(&fenced);
    PlacementDecision d1 = sched.schedule(mkreq(SchedulingRequestId(1)), &dep, &veto);
    std::cout << "d1 produced=" << d1.producedPlan << " eligible=" << d1.eligible.size() << " selected=" << d1.plan.selectedCandidate.value() << "\n";
    for(auto &x: d1.rejections) std::cout << "  rej " << x.candidateId.value() << " " << to_string(x.status) << " - " << x.reason << "\n";
    // Simulate worker A death: fence its boot.
    fenced.insert(WorkerBootId(100));
    PlacementDecision d2 = sched.schedule(mkreq(SchedulingRequestId(2)), &dep, &veto);
    std::cout << "d2 produced=" << d2.producedPlan << " eligible=" << d2.eligible.size() << " selected=" << d2.plan.selectedCandidate.value() << "\n";
    for(auto &x: d2.rejections) std::cout << "  rej " << x.candidateId.value() << " " << to_string(x.status) << " - " << x.reason << "\n";
    return 0;
}