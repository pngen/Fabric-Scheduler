#include "test_common.hpp"
#include "fabric_scheduler/scheduler.hpp"
#include "fabric_scheduler/revalidation.hpp"
#include "fabric_scheduler/persistence.hpp"
#include "fabric_scheduler/adapters/execution.hpp"
#include <functional>
#include <sstream>

using namespace fabric;

namespace {

Scheduler::Config cfg() { Scheduler::Config c; c.policy.generation = PolicyGeneration(1); c.maxCandidatesPerRequest = CoreCount(64); return c; }
SchedulingRequest req(DemandSpec d, SchedulingRequestId id) { d.storageRequireLocal = false; return build_request(d, id); }

// A permissive exec; the scheduler lifecycle governs staleness, not this adapter.
class OkExec final : public ExecutionFabric {
public:
    ExecutionHandoff authorize(const PlacementPlan& p) override {
        ExecutionHandoff h; h.authorized = true; h.authorityGeneration = p.authorityGeneration;
        h.executionId = ExecutionId(1); h.executionGeneration = ExecutionGeneration(1);
        h.attemptId = AttemptId(1); h.attemptGeneration = AttemptGeneration(1);
        return h;
    }
};

CandidateSpec localA(CandidateId id, CandidateGeneration g) {
    CandidateSpec s; s.candidateId = id; s.candidateGeneration = g; s.numaDistance = 0;
    s.stateLocal = true; s.moveBytes = ByteCount(0);
    s.workerBootId = WorkerBootId(id.value() + 10); s.sourceBootId = SourceBootId(id.value() + 20);
    return s;
}
CandidateSpec remoteB(CandidateId id, CandidateGeneration g) {
    CandidateSpec s; s.candidateId = id; s.candidateGeneration = g; s.numaDistance = 3;
    s.stateLocal = false; s.moveBytes = 512_MiB; s.vramAvailable = 8_GiB;
    s.workerBootId = WorkerBootId(id.value() + 10); s.sourceBootId = SourceBootId(id.value() + 20);
    return s;
}

DemandSpec baseDemand() { DemandSpec d; return d; }

// Test doubles that change scheduler state at the point where a concurrent
// caller would run: the scheduler consults them outside its lock.
class HookPolicy final : public PolicyVeto {
public:
    std::function<void()> hook;
    mutable bool fired{false};
    bool allows(const Candidate&) const override { if (!fired) { fired = true; hook(); } return true; }
    std::string reason(const Candidate&) const override { return {}; }
};

class HookBroker final : public ResourceBroker {
public:
    std::function<void()> hook;
    bool throwOnCommit{false};
    int releases{0};
    ResourceCommitResult commit(const std::vector<ResourceClaim>&) override {
        if (throwOnCommit) throw std::runtime_error("broker unreachable");
        if (hook) hook();
        ResourceCommitResult r; r.succeeded = true; r.commitmentToken = "tok"; return r;
    }
    void release(const std::string&) override { ++releases; }
};

class HookExec final : public ExecutionFabric {
public:
    std::function<void()> hook;
    bool grant{true};
    bool throwOnAuthorize{false};
    ExecutionHandoff authorize(const PlacementPlan& p) override {
        if (throwOnAuthorize) throw std::runtime_error("execution fabric unreachable");
        if (hook) hook();
        ExecutionHandoff h; h.authorized = grant; h.authorityGeneration = p.authorityGeneration;
        return h;
    }
};

Candidate newer_capacity(CandidateId id) {
    Candidate c = build_candidate(localA(id, CandidateGeneration(2)));
    c.capacity.generation = CapacityGeneration(2);
    return c;
}

}  // namespace

TESTCASE(coordinator_restart) {
    // Build a sharded coordinator, schedule, and persist its durable state.
    Scheduler s1(cfg());
    s1.ingest_candidate(build_candidate(localA(CandidateId(1), CandidateGeneration(1))));
    s1.ingest_candidate(build_candidate(remoteB(CandidateId(2), CandidateGeneration(1))));
    AlwaysReadyDependency dep; AllowAllPolicy pol; OkExec exec; TestBroker broker;
    PlacementDecision d1 = s1.schedule(req(baseDemand(), SchedulingRequestId(1)), &dep, &pol);
    CHECK(d1.producedPlan);
    PlacementPlanId pid = d1.plan.planId;
    Persisted snap = s1.snapshot();
    // Fresh coordinator recomputes durable state (restore) and enforces a new epoch.
    Scheduler s2(cfg());
    s2.restore(snap);
    s2.set_epoch(CoordinatorEpoch(snap.epoch.value() + 1));
    // Old epoch must be rejected going backward.
    expect_fabric_error([&]{ s2.set_epoch(CoordinatorEpoch(snap.epoch.value())); }, ErrorCode::StaleAuthority, "epoch regression");
    // The recovered plan is never blindly launchable.
    CHECK(s2.plan_state(pid) == PlacementLifecycleState::REVALIDATION_REQUIRED);
    ExecutionHandoff hOld = s2.handoff(pid, exec);
    CHECK(!hOld.authorized);
    // Dynamic candidate evidence was cleared (not restored as current); republish + replan.
    CHECK(s2.candidate_count() == 0);
    s2.ingest_candidate(build_candidate(localA(CandidateId(3), CandidateGeneration(1))));
    s2.ingest_candidate(build_candidate(remoteB(CandidateId(4), CandidateGeneration(1))));
    PlacementDecision d2 = s2.schedule(req(baseDemand(), SchedulingRequestId(2)), &dep, &pol);
    CHECK(d2.producedPlan);
    TestBroker broker2;
    CHECK(s2.commit(d2.plan.planId, broker2).succeeded);
    CHECK(s2.handoff(d2.plan.planId, exec).authorized);
}

TESTCASE(resource_commit_race) {
    Scheduler s(cfg());
    s.ingest_candidate(build_candidate(localA(CandidateId(1), CandidateGeneration(1))));
    s.ingest_candidate(build_candidate(remoteB(CandidateId(2), CandidateGeneration(1))));
    AlwaysReadyDependency dep; AllowAllPolicy pol; OkExec exec; TestBroker broker;
    PlacementDecision d = s.schedule(req(baseDemand(), SchedulingRequestId(1)), &dep, &pol);
    CHECK(d.producedPlan);
    broker.failNext = true;   // capacity changed between ranking and commit
    ResourceCommitResult cr = s.commit(d.plan.planId, broker);
    CHECK(!cr.succeeded);
    CHECK(s.plan_state(d.plan.planId) != PlacementLifecycleState::COMMITTED);
    CHECK(s.plan_state(d.plan.planId) != PlacementLifecycleState::ACTIVE);
    CHECK(!s.handoff(d.plan.planId, exec).authorized);   // no execution handoff
    // A fresh scheduling pass may commit a valid candidate.
    TestBroker broker2;
    PlacementDecision d2 = s.schedule(req(baseDemand(), SchedulingRequestId(2)), &dep, &pol);
    CHECK(d2.producedPlan);
    CHECK(s.commit(d2.plan.planId, broker2).succeeded);
}

TESTCASE(reservation_race) {
    Scheduler s(cfg());
    s.ingest_candidate(build_candidate(localA(CandidateId(1), CandidateGeneration(1))));
    s.ingest_candidate(build_candidate(remoteB(CandidateId(2), CandidateGeneration(1))));
    AlwaysReadyDependency dep; AllowAllPolicy pol; OkExec exec; TestBroker broker;
    PlacementDecision d = s.schedule(req(baseDemand(), SchedulingRequestId(1)), &dep, &pol);
    CHECK(d.producedPlan);
    CHECK(d.plan.selectedCandidate == CandidateId(1));
    auto plan = s.plan(d.plan.planId);
    GenerationFingerprint cur = plan->generations;
    cur.reservation = ReservationGeneration(2);
    RevalidationResult rr = s.revalidate(d.plan.planId, cur);
    CHECK(!rr.current);
    CHECK(s.plan_state(d.plan.planId) == PlacementLifecycleState::REVALIDATION_REQUIRED);
    CandidateSpec ap = localA(CandidateId(1), CandidateGeneration(2));
    ap.protectedResource = true;
    s.ingest_candidate(build_candidate(ap));
    PlacementDecision d2 = s.schedule(req(baseDemand(), SchedulingRequestId(2)), &dep, &pol);
    CHECK(d2.producedPlan);
    CHECK(d2.plan.selectedCandidate == CandidateId(2));
    CHECK(!s.commit(d.plan.planId, broker).succeeded);
    CHECK(!s.handoff(d.plan.planId, exec).authorized);
}

TESTCASE(congestion_race) {
    Scheduler s(cfg());
    s.ingest_candidate(build_candidate(localA(CandidateId(1), CandidateGeneration(1))));
    s.ingest_candidate(build_candidate(remoteB(CandidateId(2), CandidateGeneration(1))));
    AlwaysReadyDependency dep; AllowAllPolicy pol; OkExec exec; TestBroker broker;
    PlacementDecision d = s.schedule(req(baseDemand(), SchedulingRequestId(1)), &dep, &pol);
    CHECK(d.producedPlan);
    CHECK(d.plan.selectedCandidate == CandidateId(1));
    auto plan = s.plan(d.plan.planId);
    GenerationFingerprint cur = plan->generations;
    cur.congestion = CongestionGeneration(2);
    RevalidationResult rr = s.revalidate(d.plan.planId, cur);
    CHECK(!rr.current);
    CandidateSpec ap = localA(CandidateId(1), CandidateGeneration(2));
    ap.congestion = 1.0;
    ap.communicationCost = Cost(0.9);
    s.ingest_candidate(build_candidate(ap));
    PlacementDecision d2 = s.schedule(req(baseDemand(), SchedulingRequestId(2)), &dep, &pol);
    CHECK(d2.producedPlan);
    CHECK(d2.plan.selectedCandidate == CandidateId(2));
}

TESTCASE(cancellation_race) {
    Scheduler s(cfg());
    s.ingest_candidate(build_candidate(localA(CandidateId(1), CandidateGeneration(1))));
    AlwaysReadyDependency dep; AllowAllPolicy pol; OkExec exec; TestBroker broker;
    PlacementDecision d = s.schedule(req(baseDemand(), SchedulingRequestId(7)), &dep, &pol);
    CHECK(d.producedPlan);
    s.cancel(SchedulingRequestId(7), "user cancel");
    CHECK(s.plan_state(d.plan.planId) == PlacementLifecycleState::CANCELLED);
    CHECK(!s.commit(d.plan.planId, broker).succeeded);
    CHECK(!s.handoff(d.plan.planId, exec).authorized);
    PlacementDecision d2 = s.schedule(req(baseDemand(), SchedulingRequestId(8)), &dep, &pol);
    CHECK(d2.producedPlan);
    s.cancel(SchedulingRequestId(8));
    CHECK(s.plan_state(d2.plan.planId) == PlacementLifecycleState::CANCELLED);
    CHECK(!s.commit(d2.plan.planId, broker).succeeded);
    CHECK(!s.handoff(d2.plan.planId, exec).authorized);
}

TESTCASE(superseding_request_race) {
    Scheduler s(cfg());
    s.ingest_candidate(build_candidate(localA(CandidateId(1), CandidateGeneration(1))));
    s.ingest_candidate(build_candidate(remoteB(CandidateId(2), CandidateGeneration(1))));
    AlwaysReadyDependency dep; AllowAllPolicy pol; OkExec exec; TestBroker broker;
    PlacementDecision da = s.schedule(req(baseDemand(), SchedulingRequestId(1)), &dep, &pol);
    CHECK(da.producedPlan);
    s.supersede(SchedulingRequestId(1), SchedulingRequestId(2), "newer demand");
    CHECK(s.plan_state(da.plan.planId) == PlacementLifecycleState::SUPERSEDED);
    CHECK(!s.commit(da.plan.planId, broker).succeeded);
    CHECK(!s.handoff(da.plan.planId, exec).authorized);
    DemandSpec d2spec = baseDemand(); d2spec.hostMemory = 8_GiB;
    PlacementDecision db = s.schedule(req(d2spec, SchedulingRequestId(2)), &dep, &pol);
    CHECK(db.producedPlan);
    TestBroker broker2;
    CHECK(s.commit(db.plan.planId, broker2).succeeded);
    CHECK(s.handoff(db.plan.planId, exec).authorized);
    CHECK(!s.commit(da.plan.planId, broker).succeeded);
}

// ---- regression: fenced and refreshed evidence ------------------------------

TESTCASE(fenced_worker_evidence_is_not_placed) {
    Scheduler s(cfg());
    s.ingest_candidate(build_candidate(localA(CandidateId(1), CandidateGeneration(1))));    // worker boot 11
    s.ingest_candidate(build_candidate(remoteB(CandidateId(2), CandidateGeneration(1))));   // source boot 22
    AlwaysReadyDependency dep; AllowAllPolicy pol; OkExec exec; TestBroker broker;
    PlacementDecision ready = s.schedule(req(baseDemand(), SchedulingRequestId(1)), &dep, &pol);
    PlacementDecision active = s.schedule(req(baseDemand(), SchedulingRequestId(2)), &dep, &pol);
    CHECK(ready.plan.selectedCandidate == CandidateId(1));
    CHECK(s.commit(active.plan.planId, broker).succeeded);
    CHECK(s.handoff(active.plan.planId, exec).authorized);
    s.fence_worker(WorkerBootId(11));
    // Every plan relying on the fenced boot's evidence must be revalidated.
    CHECK(s.plan_state(ready.plan.planId) == PlacementLifecycleState::REVALIDATION_REQUIRED);
    CHECK(s.plan_state(active.plan.planId) == PlacementLifecycleState::REVALIDATION_REQUIRED);
    CHECK(!s.commit(ready.plan.planId, broker).succeeded);
    // The next pass places on the live worker and says why the other was skipped.
    PlacementDecision d = s.schedule(req(baseDemand(), SchedulingRequestId(3)), &dep, &pol);
    CHECK(d.producedPlan);
    CHECK(d.plan.selectedCandidate == CandidateId(2));
    bool reported = false;
    for (const auto& r : d.rejections)
        if (r.candidateId == CandidateId(1) && r.status == EligibilityStatus::REVALIDATION_REQUIRED) reported = true;
    CHECK(reported);
    // A fenced source boot is handled the same way.
    s.fence_source(SourceBootId(22));
    CHECK(s.plan_state(d.plan.planId) == PlacementLifecycleState::REVALIDATION_REQUIRED);
    CHECK(!s.schedule(req(baseDemand(), SchedulingRequestId(4)), &dep, &pol).producedPlan);
}

TESTCASE(refreshed_evidence_blocks_commit_and_handoff) {
    Scheduler s(cfg());
    s.ingest_candidate(build_candidate(localA(CandidateId(1), CandidateGeneration(1))));
    AlwaysReadyDependency dep; AllowAllPolicy pol; OkExec exec; TestBroker broker;
    PlacementDecision planned = s.schedule(req(baseDemand(), SchedulingRequestId(1)), &dep, &pol);
    PlacementDecision committed = s.schedule(req(baseDemand(), SchedulingRequestId(2)), &dep, &pol);
    CHECK(s.commit(committed.plan.planId, broker).succeeded);
    s.ingest_candidate(newer_capacity(CandidateId(1)));      // capacity generation 1 -> 2
    CHECK(!s.commit(planned.plan.planId, broker).succeeded);
    CHECK(s.plan_state(planned.plan.planId) == PlacementLifecycleState::REVALIDATION_REQUIRED);
    CHECK(!s.handoff(committed.plan.planId, exec).authorized);
    CHECK(s.plan_state(committed.plan.planId) == PlacementLifecycleState::REVALIDATION_REQUIRED);
    CHECK(broker.commits.load() == 1);
    // A plan built on the current evidence proceeds.
    PlacementDecision fresh = s.schedule(req(baseDemand(), SchedulingRequestId(3)), &dep, &pol);
    CHECK(s.commit(fresh.plan.planId, broker).succeeded);
    CHECK(s.handoff(fresh.plan.planId, exec).authorized);
}

TESTCASE(evidence_fenced_during_evaluation_is_not_planned) {
    Scheduler s(cfg());
    s.ingest_candidate(build_candidate(localA(CandidateId(1), CandidateGeneration(1))));
    s.ingest_candidate(build_candidate(remoteB(CandidateId(2), CandidateGeneration(1))));
    AlwaysReadyDependency dep;
    HookPolicy pol; pol.hook = [&] { s.fence_worker(WorkerBootId(11)); };
    PlacementDecision d = s.schedule(req(baseDemand(), SchedulingRequestId(1)), &dep, &pol);
    CHECK(d.producedPlan);
    CHECK(d.plan.selectedCandidate == CandidateId(2));
}

TESTCASE(evidence_refreshed_during_commit_releases_the_commitment) {
    Scheduler s(cfg());
    s.ingest_candidate(build_candidate(localA(CandidateId(1), CandidateGeneration(1))));
    AlwaysReadyDependency dep; AllowAllPolicy pol;
    PlacementDecision d = s.schedule(req(baseDemand(), SchedulingRequestId(1)), &dep, &pol);
    HookBroker broker; broker.hook = [&] { s.ingest_candidate(newer_capacity(CandidateId(1))); };
    CHECK(!s.commit(d.plan.planId, broker).succeeded);
    CHECK(broker.releases == 1);
    CHECK(s.plan_state(d.plan.planId) == PlacementLifecycleState::REVALIDATION_REQUIRED);
}

// ---- regression: lifecycle guards -------------------------------------------

TESTCASE(revalidation_keeps_a_terminal_plan_terminal) {
    Scheduler s(cfg());
    s.ingest_candidate(build_candidate(localA(CandidateId(1), CandidateGeneration(1))));
    AlwaysReadyDependency dep; AllowAllPolicy pol;
    PlacementDecision d = s.schedule(req(baseDemand(), SchedulingRequestId(1)), &dep, &pol);
    s.cancel(SchedulingRequestId(1));
    GenerationFingerprint moved = s.plan(d.plan.planId)->generations;
    moved.capacity = CapacityGeneration(9);
    const RevalidationResult rr = s.revalidate(d.plan.planId, moved);
    CHECK(!rr.current);
    CHECK(s.plan_state(d.plan.planId) == PlacementLifecycleState::CANCELLED);
    CHECK(rr.resultingState == PlacementLifecycleState::CANCELLED);
}

TESTCASE(supersession_during_handoff_launches_nothing) {
    Scheduler s(cfg());
    s.ingest_candidate(build_candidate(localA(CandidateId(1), CandidateGeneration(1))));
    AlwaysReadyDependency dep; AllowAllPolicy pol; TestBroker broker;
    PlacementDecision d = s.schedule(req(baseDemand(), SchedulingRequestId(1)), &dep, &pol);
    CHECK(s.commit(d.plan.planId, broker).succeeded);
    HookExec exec; exec.hook = [&] { s.supersede(SchedulingRequestId(1), SchedulingRequestId(9), "newer demand"); };
    const ExecutionHandoff h = s.handoff(d.plan.planId, exec);   // the fabric grants authority
    CHECK(!h.authorized);
    CHECK(s.plan_state(d.plan.planId) == PlacementLifecycleState::SUPERSEDED);
}

TESTCASE(refused_handoff_does_not_revive_a_superseded_plan) {
    Scheduler s(cfg());
    s.ingest_candidate(build_candidate(localA(CandidateId(1), CandidateGeneration(1))));
    AlwaysReadyDependency dep; AllowAllPolicy pol; TestBroker broker;
    PlacementDecision d = s.schedule(req(baseDemand(), SchedulingRequestId(1)), &dep, &pol);
    CHECK(s.commit(d.plan.planId, broker).succeeded);
    HookExec exec; exec.grant = false;
    exec.hook = [&] { s.supersede(SchedulingRequestId(1), SchedulingRequestId(9), "newer demand"); };
    CHECK(!s.handoff(d.plan.planId, exec).authorized);
    CHECK(s.plan_state(d.plan.planId) == PlacementLifecycleState::SUPERSEDED);
}

TESTCASE(adapter_failure_does_not_strand_the_plan) {
    Scheduler s(cfg());
    s.ingest_candidate(build_candidate(localA(CandidateId(1), CandidateGeneration(1))));
    AlwaysReadyDependency dep; AllowAllPolicy pol; TestBroker ok;
    PlacementDecision first = s.schedule(req(baseDemand(), SchedulingRequestId(1)), &dep, &pol);
    HookBroker broken; broken.throwOnCommit = true;
    bool threw = false;
    try { (void)s.commit(first.plan.planId, broken); } catch (const std::runtime_error&) { threw = true; }
    CHECK(threw);
    CHECK(s.plan_state(first.plan.planId) == PlacementLifecycleState::REVALIDATION_REQUIRED);
    PlacementDecision second = s.schedule(req(baseDemand(), SchedulingRequestId(2)), &dep, &pol);
    CHECK(s.commit(second.plan.planId, ok).succeeded);
    HookExec unreachable; unreachable.throwOnAuthorize = true;
    threw = false;
    try { (void)s.handoff(second.plan.planId, unreachable); } catch (const std::runtime_error&) { threw = true; }
    CHECK(threw);
    CHECK(s.plan_state(second.plan.planId) == PlacementLifecycleState::REVALIDATION_REQUIRED);
}

// ---- regression: cancellation and supersession ------------------------------

TESTCASE(cancelled_request_gets_no_new_plan) {
    Scheduler s(cfg());
    s.ingest_candidate(build_candidate(localA(CandidateId(1), CandidateGeneration(1))));
    AlwaysReadyDependency dep; AllowAllPolicy pol;
    PlacementDecision d = s.schedule(req(baseDemand(), SchedulingRequestId(1)), &dep, &pol);
    s.cancel(SchedulingRequestId(1));
    CHECK(!s.schedule(req(baseDemand(), SchedulingRequestId(1)), &dep, &pol).producedPlan);
    CHECK(s.plan_state(d.plan.planId) == PlacementLifecycleState::CANCELLED);
    // Cancelled while its candidates are being evaluated.
    HookPolicy cancelling; cancelling.hook = [&] { s.cancel(SchedulingRequestId(2)); };
    CHECK(!s.schedule(req(baseDemand(), SchedulingRequestId(2)), &dep, &cancelling).producedPlan);
    // Still cancelled after persistence and recovery.
    Scheduler recovered(cfg());
    recovered.restore(s.snapshot());
    recovered.ingest_candidate(build_candidate(localA(CandidateId(1), CandidateGeneration(1))));
    CHECK(!recovered.schedule(req(baseDemand(), SchedulingRequestId(1)), &dep, &pol).producedPlan);
}

TESTCASE(newer_plan_supersedes_the_unresolved_plan_of_its_request) {
    Scheduler s(cfg());
    s.ingest_candidate(build_candidate(localA(CandidateId(1), CandidateGeneration(1))));
    AlwaysReadyDependency dep; AllowAllPolicy pol; TestBroker broker;
    PlacementDecision first = s.schedule(req(baseDemand(), SchedulingRequestId(1)), &dep, &pol);
    PlacementDecision second = s.schedule(req(baseDemand(), SchedulingRequestId(1)), &dep, &pol);
    CHECK(first.producedPlan);
    CHECK(second.producedPlan);
    CHECK(s.plan_state(first.plan.planId) == PlacementLifecycleState::SUPERSEDED);
    CHECK(s.plan(first.plan.planId)->supersededBy == second.plan.planId);
    CHECK(!s.commit(first.plan.planId, broker).succeeded);
    CHECK(s.commit(second.plan.planId, broker).succeeded);
    CHECK(broker.commits.load() == 1);
    // A committed plan is resolved: a later plan does not supersede it.
    PlacementDecision third = s.schedule(req(baseDemand(), SchedulingRequestId(1)), &dep, &pol);
    CHECK(third.producedPlan);
    CHECK(s.plan_state(second.plan.planId) == PlacementLifecycleState::COMMITTED);
}

TESTCASE(request_older_than_the_planned_generation_is_stale) {
    Scheduler s(cfg());
    s.ingest_candidate(build_candidate(localA(CandidateId(1), CandidateGeneration(1))));
    AlwaysReadyDependency dep; AllowAllPolicy pol;
    SchedulingRequest newer = req(baseDemand(), SchedulingRequestId(5));
    newer.generation = SchedulingRequestGeneration(2);
    SchedulingRequest older = req(baseDemand(), SchedulingRequestId(5));     // generation 1
    PlacementDecision d = s.schedule(newer, &dep, &pol);
    CHECK(d.producedPlan);
    expect_fabric_error([&]{ (void)s.schedule(older, &dep, &pol); }, ErrorCode::StaleGeneration, "stale request generation");
    CHECK(s.plan_state(d.plan.planId) == PlacementLifecycleState::PLAN_READY);
}

// ---- regression: persistence ------------------------------------------------

TESTCASE(recovered_state_can_be_saved_and_recovered_again) {
    Scheduler s1(cfg());
    s1.ingest_candidate(build_candidate(localA(CandidateId(1), CandidateGeneration(1))));
    AlwaysReadyDependency dep; AllowAllPolicy pol; TestBroker broker;
    PlacementDecision d = s1.schedule(req(baseDemand(), SchedulingRequestId(1)), &dep, &pol);
    CHECK(s1.commit(d.plan.planId, broker).succeeded);
    Scheduler s2(cfg());
    s2.restore(s1.snapshot());
    CHECK(s2.candidate_count() == 0);
    std::ostringstream out(std::ios::binary);
    persist_save(s2.snapshot(), out);
    std::istringstream in(out.str(), std::ios::binary);
    Persisted again = persist_load(in);       // the recovered scheduler's own save must load
    CHECK(again.plans.size() == 1);
    Scheduler s3(cfg());
    s3.restore(again);
    CHECK(s3.plan_state(d.plan.planId) == PlacementLifecycleState::REVALIDATION_REQUIRED);
}

TESTCASE(saved_bytes_do_not_depend_on_cancellation_order) {
    const auto bytes = [](bool reversed) {
        Scheduler s(cfg());
        for (std::uint64_t i = 1; i <= 24; ++i) s.cancel(SchedulingRequestId(reversed ? 25 - i : i));
        std::ostringstream out(std::ios::binary);
        persist_save(s.snapshot(), out);
        return out.str();
    };
    CHECK(bytes(false) == bytes(true));
}

int main() { return testharness::run_all("test_closed_races"); }