#include "test_common.hpp"
#include "fabric_scheduler/scheduler.hpp"
#include "fabric_scheduler/revalidation.hpp"
#include "fabric_scheduler/persistence.hpp"
#include "fabric_scheduler/adapters/execution.hpp"

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

int main() { return testharness::run_all("test_closed_races"); }