#include "fabric_scheduler/scheduler.hpp"
#include "fabric_scheduler/persistence.hpp"
#include "fabric_scheduler/adapters/resource_broker.hpp"
#include "fabric_scheduler/adapters/execution.hpp"
#include "fabric_scheduler/adapters/dependency.hpp"
#include "fabric_scheduler/core/error.hpp"
#include <cassert>
#include <iostream>
#include <sstream>
#include <vector>
#include <string>
#include <cstddef>

using namespace fabric;

namespace {
class OkBroker final : public ResourceBroker {
public:
    ResourceCommitResult commit(const std::vector<ResourceClaim>&) override {
        ResourceCommitResult r; r.succeeded = true; r.commitmentToken = "tok"; return r;
    }
    void release(const std::string&) override {}
};

Candidate make_candidate() {
    Candidate c;
    c.candidateId = CandidateId(7);
    c.candidateGeneration = CandidateGeneration(1);
    c.workerId = WorkerId(5);
    c.workerBootId = WorkerBootId(6);
    c.sourceId = SourceId(8);
    c.sourceBootId = SourceBootId(9);
    c.provenance = Provenance::Reported;
    DeviceBinding b;
    b.deviceId = DeviceId(7); b.deviceGeneration = DeviceGeneration(1);
    b.resourceId = ResourceId(7); b.resourceGeneration = ResourceGeneration(1);
    b.nodeId = NodeId(1); b.nodeGeneration = NodeGeneration(1);
    b.memoryDomainId = MemoryDomainId(1); b.memoryDomainGeneration = MemoryDomainGeneration(1);
    b.nicId = NicId(1); b.nicGeneration = NicGeneration(1);
    b.storageId = StorageEndpointId(1); b.storageGeneration = StorageEndpointGeneration(1);
    b.cpuDomainId = CpuDomainId(1); b.cpuDomainGeneration = CpuDomainGeneration(1);
    b.capability = AcceleratorClass::Cuda; b.cudaCompatibility = 120;
    b.capabilityCurrent = true; b.capabilityGeneration = CapabilityGeneration(1);
    c.bindings.push_back(b);
    c.capacity.generation = CapacityGeneration(1);
    c.capacity.vramTotal = 16_GiB; c.capacity.vramAvailable = 16_GiB;
    c.capacity.hostMemoryAvailable = 16_GiB; c.capacity.pinnedMemoryAvailable = 4_GiB;
    c.capacity.cpuAvailable = CoreCount(8); c.capacity.storageAvailable = 128_GiB;
    c.capacity.contiguousMemory = true; c.capacity.provenance = Provenance::Measured;
    c.health.generation = HealthGeneration(1); c.health.ready = true; c.health.healthScore = 1.0;
    c.health.provenance = Provenance::Reported;
    c.congestion.generation = CongestionGeneration(1); c.congestion.provenance = Provenance::Measured;
    c.locality.topologyGeneration = TopologyGeneration(1); c.locality.numaDistance = 0;
    c.locality.provenance = Provenance::Reported;
    c.communication.planGeneration = CommunicationPlanGeneration(1); c.communication.provenance = Provenance::Derived;
    c.residency.residencyGeneration = ResidencyGeneration(1); c.residency.stateLocal = true;
    c.residency.modelResident = true; c.residency.provenance = Provenance::Reported;
    c.policyGeneration = PolicyGeneration(1);
    return c;
}

SchedulingRequest make_request() {
    SchedulingRequest r;
    r.requestId = SchedulingRequestId(1);
    r.generation = SchedulingRequestGeneration(1);
    r.coordinatorEpoch = CoordinatorEpoch(1);
    r.sourceId = SourceId(3); r.sourceBootId = SourceBootId(30);
    r.demand.demandId = WorkloadDemandId(1);
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
    r.demand.policyGeneration = PolicyGeneration(1);
    r.demand.requiredTopologyGeneration = TopologyGeneration(1);
    return r;
}
}  // namespace

int main() {
    Scheduler::Config cfg;
    cfg.policy.generation = PolicyGeneration(1);
    Scheduler sched(cfg);
    sched.ingest_candidate(make_candidate());
    AlwaysReadyDependency dep;
    AllowAllPolicy pol;
    SchedulingRequest req = make_request();
    PlacementDecision d = sched.schedule(req, &dep, &pol);
    assert(d.producedPlan);
    OkBroker broker;
    ResourceCommitResult cr = sched.commit(d.plan.planId, broker);
    assert(cr.succeeded);

    // round-trip
    Persisted snap = sched.snapshot();
    std::ostringstream out(std::ios::binary);
    persist_save(snap, out);
    std::string bytes = out.str();
    assert(!bytes.empty());

    std::istringstream in(bytes, std::ios::binary);
    Persisted loaded = persist_load(in);
    assert(loaded.epoch == snap.epoch);
    assert(loaded.authority == snap.authority);
    assert(loaded.nextPlanId == snap.nextPlanId);
    assert(loaded.requests.size() == snap.requests.size());
    assert(loaded.plans.size() == snap.plans.size());
    assert(loaded.candidateIdentities.size() == snap.candidateIdentities.size());
    assert(loaded.plans[0].planId == snap.plans[0].planId);
    assert(loaded.plans[0].selectedCandidate == snap.plans[0].selectedCandidate);

    // --- corruption rejection ------------------------------------------
    std::vector<std::byte> buf(bytes.size());
    for (std::size_t i = 0; i < bytes.size(); ++i) buf[i] = static_cast<std::byte>(static_cast<unsigned char>(bytes[i]));

    // bad magic: mutate header magic bytes (offset 0..3)
    {
        auto bad = buf; bad[0] = std::byte(static_cast<std::uint8_t>(0x99));
        std::istringstream is(std::string(reinterpret_cast<const char*>(bad.data()), bad.size()), std::ios::binary);
        bool threw = false; try { (void)persist_load(is); } catch (const fabric::FabricError& e) { threw = (e.code() == ErrorCode::Corruption); }
        assert(threw);
    }
    // unsupported version: offset 4..7
    {
        auto bad = buf; bad[4] = std::byte(0xFF);
        std::istringstream is(std::string(reinterpret_cast<const char*>(bad.data()), bad.size()), std::ios::binary);
        bool threw = false; try { (void)persist_load(is); } catch (const fabric::FabricError& e) { threw = (e.code() == ErrorCode::UnsupportedVersion); }
        assert(threw);
    }
    // checksum mismatch: flip a payload byte (offset >= 17)
    {
        auto bad = buf; bad[17] = static_cast<std::byte>(static_cast<std::uint8_t>(std::to_integer<uint8_t>(bad[17]) ^ 0x40));
        std::istringstream is(std::string(reinterpret_cast<const char*>(bad.data()), bad.size()), std::ios::binary);
        bool threw = false; try { (void)persist_load(is); } catch (const fabric::FabricError& e) { threw = (e.code() == ErrorCode::ChecksumMismatch); }
        assert(threw);
    }
    // truncation
    {
        auto bad = std::vector<std::byte>(buf.begin(), buf.begin() + (buf.size() / 2));
        std::istringstream is(std::string(reinterpret_cast<const char*>(bad.data()), bad.size()), std::ios::binary);
        bool threw = false; try { (void)persist_load(is); } catch (const fabric::FabricError& e) { threw = (e.code() == ErrorCode::Truncation); }
        assert(threw);
    }

    std::cout << "persistence test PASS\n";
    return 0;
}