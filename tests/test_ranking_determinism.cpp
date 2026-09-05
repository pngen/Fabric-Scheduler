#include "test_common.hpp"

TESTCASE(insertion_order_independent) {
    Scheduler a(default_config());
    Scheduler b(default_config());
    CandidateSpec base;
    for (int i = 1; i <= 4; ++i) {
        CandidateSpec cs = base; cs.candidateId = CandidateId(i);
        a.ingest_candidate(build_candidate(cs));
    }
    for (int i = 4; i >= 1; --i) {
        CandidateSpec cs = base; cs.candidateId = CandidateId(i);
        b.ingest_candidate(build_candidate(cs));
    }
    AlwaysReadyDependency dep;
    AllowAllPolicy pol;
    auto da = a.schedule(build_request(DemandSpec{}, SchedulingRequestId(9)), &dep, &pol);
    auto db = b.schedule(build_request(DemandSpec{}, SchedulingRequestId(10)), &dep, &pol);
    CHECK(da.producedPlan);
    CHECK(db.producedPlan);
    CHECK(da.eligible == db.eligible);
    CHECK(da.plan.selectedCandidate == db.plan.selectedCandidate);
    CHECK(da.eligible.size() == 4);
    CHECK(da.plan.selectedCandidate == CandidateId(1));
}

TESTCASE(repeat_schedule_identical_ordering) {
    Scheduler sched(default_config());
    for (int i = 1; i <= 4; ++i) {
        CandidateSpec cs; cs.candidateId = CandidateId(i);
        sched.ingest_candidate(build_candidate(cs));
    }
    AlwaysReadyDependency dep;
    AllowAllPolicy pol;
    auto d1 = sched.schedule(build_request(DemandSpec{}, SchedulingRequestId(1)), &dep, &pol);
    auto d2 = sched.schedule(build_request(DemandSpec{}, SchedulingRequestId(2)), &dep, &pol);
    CHECK(d1.eligible == d2.eligible);
    CHECK(d1.plan.selectedCandidate == d2.plan.selectedCandidate);
}

TESTCASE(differing_evidence_ranks_by_score) {
    Scheduler a(default_config());
    Scheduler b(default_config());
    CandidateSpec c1; c1.candidateId = CandidateId(1); c1.vramAvailable = 32_GiB;
    CandidateSpec c2; c2.candidateId = CandidateId(2); c2.vramAvailable = 8_GiB;
    a.ingest_candidate(build_candidate(c1));
    a.ingest_candidate(build_candidate(c2));
    b.ingest_candidate(build_candidate(c2));
    b.ingest_candidate(build_candidate(c1));
    AlwaysReadyDependency dep;
    AllowAllPolicy pol;
    SchedulingRequest req = build_request(DemandSpec{}, SchedulingRequestId(9));
    auto da = a.schedule(req, &dep, &pol);
    auto db = b.schedule(req, &dep, &pol);
    CHECK(da.producedPlan);
    CHECK(db.producedPlan);
    CHECK(da.plan.selectedCandidate == CandidateId(1));
    CHECK(db.plan.selectedCandidate == CandidateId(1));
    CHECK(da.eligible == db.eligible);
    // cross-check the winner against the pure Ranker score.
    Ranker ranker;
    RankOptions ro = default_config().rankOptions;
    auto r1 = ranker.evaluate(req, build_candidate(c1), ro);
    auto r2 = ranker.evaluate(req, build_candidate(c2), ro);
    CHECK(r1.score.value() < r2.score.value());
}

int main() { return testharness::run_all("test_ranking_determinism"); }
