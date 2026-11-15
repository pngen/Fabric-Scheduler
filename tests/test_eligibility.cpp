#include "test_common.hpp"

namespace {
EligibilityResult eval_status(const DemandSpec& ds, const CandidateSpec& cs) {
    HardFilter f;
    SchedulingRequest req = build_request(ds, SchedulingRequestId(1));
    Candidate c = build_candidate(cs);
    return f.evaluate(req, c, nullptr, nullptr);
}
EligibilityResult eval_filtered(const DemandSpec& ds, const CandidateSpec& cs,
                                const DependencyView* dep, const PolicyVeto* pol) {
    HardFilter f;
    SchedulingRequest req = build_request(ds, SchedulingRequestId(1));
    Candidate c = build_candidate(cs);
    return f.evaluate(req, c, dep, pol);
}
}  // namespace

TESTCASE(default_candidate_is_eligible) {
    HardFilter f;
    SchedulingRequest req = build_request(DemandSpec{}, SchedulingRequestId(1));
    Candidate c = build_candidate(CandidateSpec{});
    auto r = f.evaluate(req, c, nullptr, nullptr);
    CHECK(r.eligible());
    CHECK(r.status == EligibilityStatus::ELIGIBLE);
}

TESTCASE(insufficient_evidence_no_binding) {
    CandidateSpec cs; cs.hasBinding = false;
    auto r = eval_status(DemandSpec{}, cs);
    CHECK(r.status == EligibilityStatus::INSUFFICIENT_EVIDENCE);
    CHECK(!r.eligible());
}

TESTCASE(insufficient_evidence_capacity_unknown) {
    CandidateSpec cs; cs.capacityProvenance = Provenance::Unknown;
    auto r = eval_status(DemandSpec{}, cs);
    CHECK(r.status == EligibilityStatus::INSUFFICIENT_EVIDENCE);
    CHECK(!r.eligible());
}

TESTCASE(insufficient_evidence_health_unknown) {
    CandidateSpec cs; cs.healthProvenance = Provenance::Unknown;
    auto r = eval_status(DemandSpec{}, cs);
    CHECK(r.status == EligibilityStatus::INSUFFICIENT_EVIDENCE);
    CHECK(!r.eligible());
}

TESTCASE(reject_capability) {
    CandidateSpec cs; cs.capabilityCurrent = false;
    auto r = eval_status(DemandSpec{}, cs);
    CHECK(r.status == EligibilityStatus::REJECT_CAPABILITY);
    CHECK(!r.eligible());
}

TESTCASE(reject_capacity_vram) {
    CandidateSpec cs; cs.vramAvailable = 2_GiB;
    auto r = eval_status(DemandSpec{}, cs);
    CHECK(r.status == EligibilityStatus::REJECT_CAPACITY);
    CHECK(!r.eligible());
}

TESTCASE(reject_capacity_cpu) {
    DemandSpec ds; ds.cpuRequired = CoreCount(16);
    auto r = eval_status(ds, CandidateSpec{});
    CHECK(r.status == EligibilityStatus::REJECT_CAPACITY);
    CHECK(!r.eligible());
}

TESTCASE(reject_capacity_host_memory) {
    DemandSpec ds; ds.hostMemory = 32_GiB;
    auto r = eval_status(ds, CandidateSpec{});
    CHECK(r.status == EligibilityStatus::REJECT_CAPACITY);
    CHECK(!r.eligible());
}

TESTCASE(reject_memory_pinned) {
    CandidateSpec cs; cs.pinnedMemoryAvailable = 64_MiB;
    auto r = eval_status(DemandSpec{}, cs);
    CHECK(r.status == EligibilityStatus::REJECT_MEMORY);
    CHECK(!r.eligible());
}

TESTCASE(reject_contiguity) {
    DemandSpec ds; ds.requiresContiguousMemory = true;
    CandidateSpec cs; cs.contiguousMemory = false;
    auto r = eval_status(ds, cs);
    CHECK(r.status == EligibilityStatus::REJECT_CONTIGUITY);
    CHECK(!r.eligible());
}

TESTCASE(reject_health) {
    CandidateSpec cs; cs.healthReady = false;
    auto r = eval_status(DemandSpec{}, cs);
    CHECK(r.status == EligibilityStatus::REJECT_HEALTH);
    CHECK(!r.eligible());
}

TESTCASE(reject_network_no_nic) {
    DemandSpec ds; ds.networkRequireNic = true;
    CandidateSpec cs; cs.hasNic = false;
    auto r = eval_status(ds, cs);
    CHECK(r.status == EligibilityStatus::REJECT_NETWORK);
    CHECK(!r.eligible());
}

TESTCASE(reject_network_bandwidth) {
    DemandSpec ds; ds.networkRequireNic = true; ds.networkBandwidth = 800_GiB;
    auto r = eval_status(ds, CandidateSpec{});
    CHECK(r.status == EligibilityStatus::REJECT_NETWORK);
    CHECK(!r.eligible());
}

TESTCASE(reject_storage_capacity) {
    DemandSpec ds; ds.storageCapacity = 256_GiB;
    auto r = eval_status(ds, CandidateSpec{});
    CHECK(r.status == EligibilityStatus::REJECT_STORAGE);
    CHECK(!r.eligible());
}

TESTCASE(reject_storage_local) {
    CandidateSpec cs; cs.stateLocal = false;
    auto r = eval_status(DemandSpec{}, cs);
    CHECK(r.status == EligibilityStatus::REJECT_STORAGE);
    CHECK(!r.eligible());
}

TESTCASE(reject_reservation) {
    CandidateSpec cs; cs.protectedResource = true;
    auto r = eval_status(DemandSpec{}, cs);
    CHECK(r.status == EligibilityStatus::REJECT_RESERVATION);
    CHECK(!r.eligible());
}

TESTCASE(reject_topology) {
    DemandSpec ds; ds.requiredTopologyGeneration = TopologyGeneration(2);
    auto r = eval_status(ds, CandidateSpec{});
    CHECK(r.status == EligibilityStatus::REJECT_TOPOLOGY);
    CHECK(!r.eligible());
}

TESTCASE(reject_dependency) {
    NotReadyDependency dep;
    auto r = eval_filtered(DemandSpec{}, CandidateSpec{}, &dep, nullptr);
    CHECK(r.status == EligibilityStatus::REJECT_DEPENDENCY);
    CHECK(!r.eligible());
}

TESTCASE(reject_policy) {
    RejectAllPolicy pol;
    auto r = eval_filtered(DemandSpec{}, CandidateSpec{}, nullptr, &pol);
    CHECK(r.status == EligibilityStatus::REJECT_POLICY);
    CHECK(!r.eligible());
}

// ---- regression: stated hard requirements are enforced ----------------------

TESTCASE(reject_capacity_accelerator_count) {
    DemandSpec ds; ds.minCount = CoreCount(2); ds.targetCount = CoreCount(2); ds.maxCount = CoreCount(2);
    // One matching device, with enough aggregate VRAM for two.
    auto r = eval_status(ds, CandidateSpec{});
    CHECK(r.status == EligibilityStatus::REJECT_CAPACITY);
    CHECK(!r.eligible());
    SchedulingRequest req = build_request(ds, SchedulingRequestId(1));
    Candidate two = build_candidate(CandidateSpec{});
    two.bindings.push_back(two.bindings.front());
    CHECK(HardFilter().evaluate(req, two, nullptr, nullptr).eligible());
}

TESTCASE(reject_required_model_residency) {
    SchedulingRequest req = build_request(DemandSpec{}, SchedulingRequestId(1));
    req.demand.residency.requireModelResident = true;
    Candidate c = build_candidate(CandidateSpec{});
    CHECK(HardFilter().evaluate(req, c, nullptr, nullptr).eligible());
    c.residency.modelResident = false;
    CHECK(HardFilter().evaluate(req, c, nullptr, nullptr).status == EligibilityStatus::REJECT_STORAGE);
}

TESTCASE(reject_required_adapter_residency) {
    SchedulingRequest req = build_request(DemandSpec{}, SchedulingRequestId(1));
    req.demand.residency.requireAdapterResident = true;
    Candidate c = build_candidate(CandidateSpec{});          // adapter not resident
    CHECK(HardFilter().evaluate(req, c, nullptr, nullptr).status == EligibilityStatus::REJECT_STORAGE);
    c.residency.adapterResident = true;
    CHECK(HardFilter().evaluate(req, c, nullptr, nullptr).eligible());
}

TESTCASE(reject_required_state_locality) {
    DemandSpec ds; ds.storageRequireLocal = false;
    SchedulingRequest req = build_request(ds, SchedulingRequestId(1));
    req.demand.residency.requireStateLocal = true;
    CandidateSpec cs; cs.stateLocal = false;
    CHECK(HardFilter().evaluate(req, build_candidate(cs), nullptr, nullptr).status == EligibilityStatus::REJECT_STORAGE);
    CHECK(HardFilter().evaluate(req, build_candidate(CandidateSpec{}), nullptr, nullptr).eligible());
}

TESTCASE(reject_movement_above_maximum) {
    DemandSpec ds; ds.storageRequireLocal = false;
    SchedulingRequest req = build_request(ds, SchedulingRequestId(1));
    CandidateSpec cs; cs.stateLocal = false; cs.moveBytes = 512_MiB;
    req.demand.maximumMovementCost = 256_MiB;
    CHECK(HardFilter().evaluate(req, build_candidate(cs), nullptr, nullptr).status == EligibilityStatus::REJECT_STORAGE);
    req.demand.maximumMovementCost = 1_GiB;
    CHECK(HardFilter().evaluate(req, build_candidate(cs), nullptr, nullptr).eligible());
}

TESTCASE(reject_infeasible_communication_path) {
    SchedulingRequest req = build_request(DemandSpec{}, SchedulingRequestId(1));
    Candidate c = build_candidate(CandidateSpec{});
    c.communication.pathFeasible = false;
    CHECK(HardFilter().evaluate(req, c, nullptr, nullptr).eligible());    // the workload does not communicate
    req.demand.communication = CommunicationPattern::AllReduce;
    CHECK(HardFilter().evaluate(req, c, nullptr, nullptr).status == EligibilityStatus::REJECT_NETWORK);
}

TESTCASE(reject_shared_root_failure_domain) {
    SchedulingRequest req = build_request(DemandSpec{}, SchedulingRequestId(1));
    req.demand.failureDomains.avoidSingleRoot = true;
    CandidateSpec cs; cs.sharesPcieRoot = true;
    CHECK(HardFilter().evaluate(req, build_candidate(cs), nullptr, nullptr).status == EligibilityStatus::REJECT_FAILURE_DOMAIN);
    CHECK(HardFilter().evaluate(req, build_candidate(CandidateSpec{}), nullptr, nullptr).eligible());
}

TESTCASE(hard_rejected_candidate_never_wins) {
    Scheduler sched(default_config());
    CandidateSpec bad; bad.candidateId = CandidateId(1); bad.vramAvailable = 2_GiB;
    CandidateSpec good; good.candidateId = CandidateId(2); good.vramAvailable = 16_GiB;
    sched.ingest_candidate(build_candidate(bad));
    sched.ingest_candidate(build_candidate(good));
    AlwaysReadyDependency dep;
    AllowAllPolicy pol;
    auto d = sched.schedule(build_request(DemandSpec{}, SchedulingRequestId(100)), &dep, &pol);
    CHECK(d.producedPlan);
    CHECK(d.plan.selectedCandidate == CandidateId(2));
    CHECK(d.eligible.size() == 1);
    CHECK(d.eligible[0] == CandidateId(2));
    CHECK(d.disqualified.size() == 1);
    CHECK(d.disqualified[0] == CandidateId(1));
}

int main() { return testharness::run_all("test_eligibility"); }
