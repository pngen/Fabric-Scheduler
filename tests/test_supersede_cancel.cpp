#include "test_common.hpp"

TESTCASE(supersede_marks_old_superseded) {
    Scheduler sched(default_config());
    sched.ingest_candidate(build_candidate(CandidateSpec{}));
    AlwaysReadyDependency dep;
    AllowAllPolicy pol;
    auto d1 = sched.schedule(build_request(DemandSpec{}, SchedulingRequestId(1)), &dep, &pol);
    auto d2 = sched.schedule(build_request(DemandSpec{}, SchedulingRequestId(2)), &dep, &pol);
    PlacementPlanId p1 = d1.plan.planId;
    PlacementPlanId p2 = d2.plan.planId;
    sched.supersede(SchedulingRequestId(1), SchedulingRequestId(2), "newer request");
    CHECK(sched.plan_state(p1) == PlacementLifecycleState::SUPERSEDED);
    TestBroker broker;
    auto cr = sched.commit(p1, broker);
    CHECK(!cr.succeeded);
    ReferenceExecutionAdapter exec(AuthorityGeneration(1));
    auto h = sched.handoff(p1, exec);
    CHECK(!h.authorized);
    CHECK(sched.plan_state(p2) == PlacementLifecycleState::PLAN_READY);
    auto pl = sched.plan(p1);
    CHECK(pl.has_value());
    CHECK(pl->supersededBy == p2);
}

TESTCASE(cancel_prevents_commit_and_launch) {
    Scheduler sched(default_config());
    sched.ingest_candidate(build_candidate(CandidateSpec{}));
    AlwaysReadyDependency dep;
    AllowAllPolicy pol;
    auto d = sched.schedule(build_request(DemandSpec{}, SchedulingRequestId(7)), &dep, &pol);
    PlacementPlanId pid = d.plan.planId;
    sched.cancel(SchedulingRequestId(7), "user cancel");
    CHECK(sched.plan_state(pid) == PlacementLifecycleState::CANCELLED);
    TestBroker broker;
    auto cr = sched.commit(pid, broker);
    CHECK(!cr.succeeded);
    ReferenceExecutionAdapter exec(AuthorityGeneration(1));
    auto h = sched.handoff(pid, exec);
    CHECK(!h.authorized);
}

int main() { return testharness::run_all("test_supersede_cancel"); }
