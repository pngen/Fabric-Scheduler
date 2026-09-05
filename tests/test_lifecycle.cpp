#include "test_common.hpp"

TESTCASE(transition_table_predicates) {
    CHECK(!placement_can_transition(PlacementLifecycleState::REQUESTED, PlacementLifecycleState::ACTIVE));
    CHECK(!placement_can_transition(PlacementLifecycleState::SUPERSEDED, PlacementLifecycleState::ACTIVE));
    CHECK(!placement_can_transition(PlacementLifecycleState::PLAN_READY, PlacementLifecycleState::COMMITTED));
    CHECK(placement_can_transition(PlacementLifecycleState::COMMITTED, PlacementLifecycleState::COMMITTED));
    CHECK(placement_can_transition(PlacementLifecycleState::ACTIVE, PlacementLifecycleState::ACTIVE));
}

TESTCASE(advance_illegal_transition_throws) {
    PlacementPlan plan;
    std::uint64_t ord = 1;
    bool threw = false;
    try { placement_advance(plan, PlacementLifecycleState::ACTIVE, 0, ord); }
    catch (const FabricError& e) { threw = true; CHECK(e.code() == ErrorCode::IllegalLifecycleTransition); }
    CHECK(threw);
    CHECK(plan.state == PlacementLifecycleState::REQUESTED);
}

TESTCASE(advance_legal_records_history) {
    PlacementPlan plan;
    std::uint64_t ord = 1;
    placement_advance(plan, PlacementLifecycleState::DISCOVERING, 0, ord);
    CHECK(plan.state == PlacementLifecycleState::DISCOVERING);
    CHECK(!plan.stateHistory.empty());
    CHECK(plan.stateHistory.back().first == PlacementLifecycleState::DISCOVERING);
    CHECK(plan.stateHistory.back().second == 1);
}

TESTCASE(advance_self_transition_is_noop) {
    PlacementPlan plan;
    plan.state = PlacementLifecycleState::COMMITTED;
    std::uint64_t ord = 5;
    placement_advance(plan, PlacementLifecycleState::COMMITTED, 0, ord);
    CHECK(plan.state == PlacementLifecycleState::COMMITTED);
    CHECK(plan.stateHistory.empty());
}

TESTCASE(scheduler_lifecycle_pipeline) {
    Scheduler sched(default_config());
    sched.ingest_candidate(build_candidate(CandidateSpec{}));
    AlwaysReadyDependency dep;
    AllowAllPolicy pol;
    auto d = sched.schedule(build_request(DemandSpec{}, SchedulingRequestId(5)), &dep, &pol);
    CHECK(d.producedPlan);
    CHECK(sched.plan_state(d.plan.planId) == PlacementLifecycleState::PLAN_READY);
    TestBroker broker;
    auto cr = sched.commit(d.plan.planId, broker);
    CHECK(cr.succeeded);
    CHECK(sched.plan_state(d.plan.planId) == PlacementLifecycleState::COMMITTED);
    ReferenceExecutionAdapter exec(AuthorityGeneration(1));
    auto h = sched.handoff(d.plan.planId, exec);
    CHECK(h.authorized);
    CHECK(sched.plan_state(d.plan.planId) == PlacementLifecycleState::ACTIVE);
    CHECK(sched.plan_state(PlacementPlanId(9999)) == PlacementLifecycleState::RETIRED);
}

TESTCASE(resource_commit_failure_no_false_commit) {
    Scheduler sched(default_config());
    sched.ingest_candidate(build_candidate(CandidateSpec{}));
    AlwaysReadyDependency dep;
    AllowAllPolicy pol;
    auto d = sched.schedule(build_request(DemandSpec{}, SchedulingRequestId(1)), &dep, &pol);
    PlacementPlanId pid = d.plan.planId;
    TestBroker broker;
    broker.failNext = true;
    auto cr = sched.commit(pid, broker);
    CHECK(!cr.succeeded);
    CHECK(sched.plan_state(pid) != PlacementLifecycleState::COMMITTED);
    CHECK(sched.plan_state(pid) != PlacementLifecycleState::ACTIVE);
    CHECK(sched.plan_state(pid) == PlacementLifecycleState::REVALIDATION_REQUIRED);
    ReferenceExecutionAdapter exec(AuthorityGeneration(1));
    auto h = sched.handoff(pid, exec);
    CHECK(!h.authorized);
}

TESTCASE(stale_authority_after_epoch_bump) {
    Scheduler sched(default_config());
    sched.ingest_candidate(build_candidate(CandidateSpec{}));
    AlwaysReadyDependency dep;
    AllowAllPolicy pol;
    auto d = sched.schedule(build_request(DemandSpec{}, SchedulingRequestId(1)), &dep, &pol);
    PlacementPlanId pid = d.plan.planId;
    CHECK(sched.epoch() == CoordinatorEpoch(1));
    CHECK(sched.current_authority() == AuthorityGeneration(1));
    CHECK_THROWS_CODE(sched.set_epoch(CoordinatorEpoch(0)), ErrorCode::StaleAuthority);
    sched.set_epoch(CoordinatorEpoch(2));
    CHECK(sched.epoch() == CoordinatorEpoch(2));
    CHECK(sched.current_authority() == AuthorityGeneration(2));
    CHECK(sched.plan_state(pid) == PlacementLifecycleState::REVALIDATION_REQUIRED);
    ReferenceExecutionAdapter exec(AuthorityGeneration(2));
    auto h = sched.handoff(pid, exec);
    CHECK(!h.authorized);
    TestBroker broker;
    auto cr = sched.commit(pid, broker);
    CHECK(!cr.succeeded);
}

TESTCASE(fenced_worker_and_source_rejected) {
    Scheduler sched(default_config());
    sched.fence_worker(WorkerBootId(77));
    CandidateSpec cs; cs.workerBootId = WorkerBootId(77);
    CHECK_THROWS_CODE(sched.ingest_candidate(build_candidate(cs)), ErrorCode::StaleAuthority);
    Scheduler s2(default_config());
    s2.fence_source(SourceBootId(88));
    CandidateSpec cs2; cs2.sourceBootId = SourceBootId(88);
    CHECK_THROWS_CODE(s2.ingest_candidate(build_candidate(cs2)), ErrorCode::StaleAuthority);
}

int main() { return testharness::run_all("test_lifecycle"); }
