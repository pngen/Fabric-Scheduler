#pragma once
// Shared helpers + minimal harness for the Fabric Scheduler core test suite.
#include "fabric_scheduler/scheduler.hpp"
#include "fabric_scheduler/adapters/resource_broker.hpp"
#include "fabric_scheduler/adapters/execution.hpp"
#include "fabric_scheduler/adapters/dependency.hpp"
#include "fabric_scheduler/adapters/communication.hpp"
#include "fabric_scheduler/adapters/residency.hpp"
#include "fabric_scheduler/revalidation.hpp"
#include "fabric_scheduler/eligibility.hpp"
#include "fabric_scheduler/ranking.hpp"
#include "fabric_scheduler/placement.hpp"
#include "fabric_scheduler/policy.hpp"
#include "fabric_scheduler/request.hpp"
#include "fabric_scheduler/candidate.hpp"
#include "fabric_scheduler/core/error.hpp"
#include <atomic>
#include <cstdio>
#include <cstdint>
#include <string>
#include <vector>
#include <stdexcept>
#include <cmath>
#include <limits>
#include <thread>
#include <mutex>

using namespace fabric;

// --- minimal test harness -------------------------------------------------
namespace testharness {
struct TestCase { const char* name; void (*fn)(); };
inline std::vector<TestCase>& registry() { static std::vector<TestCase> r; return r; }
struct Register {
    Register(const char* n, void (*f)()) { registry().push_back({ n, f }); }
};
inline int run_all(const char* suite) {
    int failed = 0;
    std::printf("== %s ==\n", suite);
    for (const auto& t : registry()) {
        std::printf("[RUN ] %s\n", t.name);
        try {
            t.fn();
            std::printf("[PASS] %s\n", t.name);
        } catch (const std::exception& e) {
            std::printf("[FAIL] %s : %s\n", t.name, e.what());
            ++failed;
        } catch (...) {
            std::printf("[FAIL] %s : unknown exception\n", t.name);
            ++failed;
        }
    }
    std::printf("== %s: %d FAILED ==\n", suite, failed);
    return failed;
}
}  // namespace testharness

#define TESTCASE(name)                                                     \
    static void test_##name();                                              \
    static ::testharness::Register test_reg_##name(#name, &test_##name);    \
    static void test_##name()

#define CHECK(cond)                                                        \
    do {                                                                    \
        if (!(cond)) {                                                      \
            throw std::runtime_error(                                       \
                std::string("CHECK failed at ") + __FILE__ + ":" +          \
                std::to_string(__LINE__) + " : " + #cond);                  \
        }                                                                   \
    } while (0)

// Robust throw-checking helper. A function template + lambda avoids the
// MSVC macro re-expansion quirk that breaks function-style casts in a
// parenthesised macro argument.
template<typename F>
inline void expect_fabric_error(F&& fn, fabric::ErrorCode code, const char* what) {
    bool caught = false;
    try { fn(); }
    catch (const fabric::FabricError& e) {
        caught = true;
        if (e.code() != code) {
            throw std::runtime_error(std::string(what) + " : unexpected error code");
        }
    }
    if (!caught) throw std::runtime_error(std::string(what) + " : did not throw FabricError");
}

// --- reusable adapters ----------------------------------------------------
class NotReadyDependency final : public DependencyView {
public:
    bool ready(WorkloadDemandId) const override { return false; }
    DependencyGeneration generation() const override { return DependencyGeneration(1); }
    std::string unsatisfied(WorkloadDemandId) const override { return "dependency not satisfied"; }
};

class RejectAllPolicy final : public PolicyVeto {
public:
    bool allows(const Candidate&) const override { return false; }
    std::string reason(const Candidate&) const override { return "test policy veto"; }
};

// Thread-safe reference broker for commit/handoff bursts.
class TestBroker final : public ResourceBroker {
public:
    std::atomic<bool> failNext{false};
    std::atomic<int> commits{0};
    std::atomic<int> releases{0};
    std::atomic<int> seq{0};

    ResourceCommitResult commit(const std::vector<ResourceClaim>& claims) override {
        ResourceCommitResult r;
        if (failNext.exchange(false)) {
            r.succeeded = false;
            r.failureReason = "capacity changed at commit time";
            return r;
        }
        r.succeeded = true;
        r.commitmentToken = "tok-" + std::to_string(seq.fetch_add(1));
        r.resourceGeneration = claims.empty() ? ResourceGeneration(1) : claims.front().resourceGeneration;
        commits.fetch_add(1);
        return r;
    }
    void release(const std::string&) override { releases.fetch_add(1); }
};

// --- candidate builder ----------------------------------------------------
struct CandidateSpec {
    CandidateId candidateId{1};
    CandidateGeneration candidateGeneration{1};
    WorkerId workerId{1000};
    WorkerBootId workerBootId{10};
    SourceId sourceId{5000};
    SourceBootId sourceBootId{20};
    Provenance provenance{Provenance::Reported};

    bool hasBinding{true};
    AcceleratorClass capability{AcceleratorClass::Cuda};
    bool capabilityCurrent{true};
    std::int32_t cudaCompatibility{120};
    CapabilityGeneration capabilityGeneration{1};

    MemoryDomainId numa{1};
    std::uint32_t pcieHops{1};
    bool hasNic{true};
    bool sharesPcieRoot{false};

    ByteCount vramTotal{16_GiB};
    ByteCount vramAvailable{16_GiB};
    ByteCount hostMemoryAvailable{16_GiB};
    ByteCount pinnedMemoryAvailable{4_GiB};
    CoreCount cpuAvailable{8};
    ByteCount storageAvailable{128_GiB};
    ByteCount nicBandwidthAvailable{400_GiB};
    bool contiguousMemory{true};
    Provenance capacityProvenance{Provenance::Measured};

    bool healthReady{true};
    double healthScore{1.0};
    std::string degradationReason;
    Provenance healthProvenance{Provenance::Reported};

    ReservationGeneration reservationGeneration{1};
    bool protectedResource{false};
    ByteCount reservedBytes{0};
    Provenance reservationProvenance{Provenance::Reported};

    TopologyGeneration topologyGeneration{1};
    std::uint32_t numaDistance{0};
    std::uint32_t nicDistance{0};
    std::uint32_t storageDistance{0};
    Provenance localityProvenance{Provenance::Reported};

    Cost communicationCost{0.0};
    Provenance communicationProvenance{Provenance::Derived};

    bool stateLocal{true};
    bool modelResident{true};
    bool adapterResident{false};
    ByteCount moveBytes{0};
    Provenance residencyProvenance{Provenance::Reported};

    double congestion{0.0};
    double residualBandwidth{1.0};
    Provenance congestionProvenance{Provenance::Measured};

    PolicyGeneration policyGeneration{1};
};

inline Candidate build_candidate(const CandidateSpec& s) {
    Candidate c;
    c.candidateId = s.candidateId;
    c.candidateGeneration = s.candidateGeneration;
    c.workerId = s.workerId;
    c.workerBootId = s.workerBootId;
    c.sourceId = s.sourceId;
    c.sourceBootId = s.sourceBootId;
    c.provenance = s.provenance;

    if (s.hasBinding) {
        DeviceBinding b;
        b.deviceId = DeviceId(s.candidateId.value());
        b.deviceGeneration = DeviceGeneration(1);
        b.resourceId = ResourceId(s.candidateId.value());
        b.resourceGeneration = ResourceGeneration(1);
        b.nodeId = NodeId(1);
        b.nodeGeneration = NodeGeneration(1);
        b.memoryDomainId = s.numa;
        b.memoryDomainGeneration = MemoryDomainGeneration(1);
        b.nicId = s.hasNic ? NicId(s.candidateId.value() + 100) : NicId(0);
        b.nicGeneration = s.hasNic ? NicGeneration(1) : NicGeneration(0);
        b.storageId = StorageEndpointId(1);
        b.storageGeneration = StorageEndpointGeneration(1);
        b.cpuDomainId = CpuDomainId(1);
        b.cpuDomainGeneration = CpuDomainGeneration(1);
        b.pcieRoot = "root-" + std::to_string(s.candidateId.value());
        b.sharesPcieRoot = s.sharesPcieRoot;
        b.capability = s.capability;
        b.cudaCompatibility = s.cudaCompatibility;
        b.capabilityCurrent = s.capabilityCurrent;
        b.capabilityGeneration = s.capabilityGeneration;
        c.bindings.push_back(std::move(b));
    }

    c.capacity.generation = CapacityGeneration(1);
    c.capacity.vramTotal = s.vramTotal;
    c.capacity.vramUsed = ByteCount(0);
    c.capacity.vramAvailable = s.vramAvailable;
    c.capacity.hostMemoryAvailable = s.hostMemoryAvailable;
    c.capacity.pinnedMemoryAvailable = s.pinnedMemoryAvailable;
    c.capacity.cpuAvailable = s.cpuAvailable;
    c.capacity.storageAvailable = s.storageAvailable;
    c.capacity.nicBandwidthAvailable = s.nicBandwidthAvailable;
    c.capacity.contiguousMemory = s.contiguousMemory;
    c.capacity.provenance = s.capacityProvenance;

    c.health.generation = HealthGeneration(1);
    c.health.ready = s.healthReady;
    c.health.healthScore = s.healthScore;
    c.health.degradationReason = s.degradationReason;
    c.health.provenance = s.healthProvenance;

    c.reservation.generation = s.reservationGeneration;
    c.reservation.protectedResource = s.protectedResource;
    c.reservation.reservedBytes = s.reservedBytes;
    c.reservation.provenance = s.reservationProvenance;

    c.congestion.generation = CongestionGeneration(1);
    c.congestion.congestion = s.congestion;
    c.congestion.residualBandwidth = s.residualBandwidth;
    c.congestion.bandwidthNominal = ByteCount(0);
    c.congestion.provenance = s.congestionProvenance;

    c.locality.topologyGeneration = s.topologyGeneration;
    c.locality.numaDistance = s.numaDistance;
    c.locality.pcieHops = s.pcieHops;
    c.locality.nicDistance = s.nicDistance;
    c.locality.storageDistance = s.storageDistance;
    c.locality.topologyGroup = "group-1";
    c.locality.provenance = s.localityProvenance;

    c.communication.planGeneration = CommunicationPlanGeneration(1);
    c.communication.estimatedCost = s.communicationCost;
    c.communication.derivedPathCost = Cost(s.communicationCost.value());
    c.communication.collectiveCost = Cost(0.0);
    c.communication.pathFeasible = true;
    c.communication.planId = "ref";
    c.communication.provenance = s.communicationProvenance;

    c.residency.residencyGeneration = ResidencyGeneration(1);
    c.residency.modelResident = s.modelResident;
    c.residency.adapterResident = s.adapterResident;
    c.residency.stateLocal = s.stateLocal;
    c.residency.moveBytes = s.moveBytes;
    c.residency.moveSource = s.moveBytes.value() ? "src" : "";
    c.residency.provenance = s.residencyProvenance;

    c.policyGeneration = s.policyGeneration;
    c.memoryMovementCost = Cost(s.stateLocal ? 0.0 : 0.4);
    return c;
}

// --- request builder ------------------------------------------------------
struct DemandSpec {
    WorkloadDemandId demandId{1};
    WorkloadDemandGeneration demandGeneration{1};
    WorkloadId workloadId{4};
    WorkloadGeneration workloadGeneration{1};
    AcceleratorClass capability{AcceleratorClass::Cuda};
    CoreCount minCount{1};
    CoreCount targetCount{1};
    CoreCount maxCount{1};
    ByteCount vramPerDevice{4_GiB};
    ByteCount aggregateVram{0};
    CapabilityGeneration capabilityGeneration{1};
    CoreCount cpuRequired{2};
    ByteCount hostMemory{1_GiB};
    ByteCount pinnedMemory{256_MiB};
    ByteCount storageCapacity{4_GiB};
    bool storageRequireLocal{true};
    ByteCount networkBandwidth{0};
    bool networkRequireNic{false};
    bool requiresContiguousMemory{false};
    TopologyGeneration requiredTopologyGeneration{1};
    CommunicationPattern communication{CommunicationPattern::None};
    ByteCount communicationBytes{0};
    Duration expectedExecutionDuration{100_ms};
    LatencySensitivity latencySensitivity{LatencySensitivity::High};
    double throughputObjective{0.0};
    PolicyGeneration policyGeneration{1};
    PriorityGeneration priorityGeneration{1};
};

inline SchedulingRequest build_request(const DemandSpec& s, SchedulingRequestId id) {
    SchedulingRequest r;
    r.requestId = id;
    r.generation = SchedulingRequestGeneration(1);
    r.coordinatorEpoch = CoordinatorEpoch(1);
    r.sourceId = SourceId(3);
    r.sourceBootId = SourceBootId(30);
    r.submitOrdinal = id.value();
    r.demand.demandId = s.demandId;
    r.demand.demandGeneration = s.demandGeneration;
    r.demand.workloadId = s.workloadId;
    r.demand.workloadGeneration = s.workloadGeneration;
    r.demand.accelerator.capability = s.capability;
    r.demand.accelerator.minimumCount = s.minCount;
    r.demand.accelerator.targetCount = s.targetCount;
    r.demand.accelerator.maximumCount = s.maxCount;
    r.demand.accelerator.vramPerDevice = s.vramPerDevice;
    r.demand.accelerator.aggregateVram = s.aggregateVram;
    r.demand.accelerator.capabilityGeneration = s.capabilityGeneration;
    r.demand.accelerator.requiresContiguousMemory = s.requiresContiguousMemory;
    r.demand.memory.hostMemory = s.hostMemory;
    r.demand.memory.pinnedMemory = s.pinnedMemory;
    r.demand.cpuRequired = s.cpuRequired;
    r.demand.storage.capacity = s.storageCapacity;
    r.demand.storage.requireLocal = s.storageRequireLocal;
    r.demand.network.bandwidth = s.networkBandwidth;
    r.demand.network.requireNic = s.networkRequireNic;
    r.demand.requiredTopologyGeneration = s.requiredTopologyGeneration;
    r.demand.communication = s.communication;
    r.demand.communicationBytes = s.communicationBytes;
    r.demand.expectedExecutionDuration = s.expectedExecutionDuration;
    r.demand.latencySensitivity = s.latencySensitivity;
    r.demand.throughputObjective = s.throughputObjective;
    r.demand.policyGeneration = s.policyGeneration;
    r.demand.priorityGeneration = s.priorityGeneration;
    r.demand.provenance = Provenance::Reported;
    return r;
}

// Convenient default schedulers + dependency/policy.
inline Scheduler::Config default_config() {
    Scheduler::Config cfg;
    cfg.policy.generation = PolicyGeneration(1);
    cfg.rankOptions.policyGeneration = PolicyGeneration(1);
    cfg.maxCandidatesPerRequest = CoreCount(256);
    return cfg;
}
