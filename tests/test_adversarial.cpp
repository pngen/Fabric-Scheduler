#include "test_common.hpp"

TESTCASE(nan_throughput_throws_nonfinite) {
    DemandSpec ds;
    ds.throughputObjective = std::numeric_limits<double>::quiet_NaN();
    SchedulingRequest req = build_request(ds, SchedulingRequestId(1));
    expect_fabric_error([&]{ req.demand.validate(); }, ErrorCode::NonFinite, "nan throughput validate");
    Scheduler sched(default_config());
    expect_fabric_error([&]{ (void)sched.schedule(req, nullptr, nullptr); }, ErrorCode::NonFinite, "nan schedule");
}

TESTCASE(minimum_exceeds_maximum_throws) {
    DemandSpec ds;
    ds.minCount = CoreCount(4);
    ds.maxCount = CoreCount(2);
    ds.targetCount = CoreCount(1);
    SchedulingRequest req = build_request(ds, SchedulingRequestId(1));
    expect_fabric_error([&]{ req.demand.validate(); }, ErrorCode::InvalidCountRange, "min>max validate");
}

TESTCASE(ranker_scores_are_finite_nonnegative) {
    DemandSpec ds;
    SchedulingRequest req = build_request(ds, SchedulingRequestId(1));
    Candidate c = build_candidate(CandidateSpec{});
    Ranker ranker;
    RankOptions ro = default_config().rankOptions;
    auto rc = ranker.evaluate(req, c, ro);
    CHECK(rc.score.is_finite());
    CHECK(!rc.score.is_negative());
    CHECK(rc.factors.size() == kRankFactorCount);
    for (const auto& f : rc.factors) {
        CHECK(f.value.is_finite());
        CHECK(!f.value.is_negative());
    }
}

TESTCASE(insufficient_vram_candidate_never_wins) {
    Scheduler sched(default_config());
    CandidateSpec bad; bad.candidateId = CandidateId(1); bad.vramAvailable = 2_GiB;
    CandidateSpec good; good.candidateId = CandidateId(2); good.vramAvailable = 16_GiB;
    sched.ingest_candidate(build_candidate(bad));
    sched.ingest_candidate(build_candidate(good));
    AlwaysReadyDependency dep;
    AllowAllPolicy pol;
    auto d = sched.schedule(build_request(DemandSpec{}, SchedulingRequestId(1)), &dep, &pol);
    CHECK(d.producedPlan);
    CHECK(d.plan.selectedCandidate == CandidateId(2));
    CHECK(d.eligible.size() == 1);
    CHECK(d.eligible[0] == CandidateId(2));
    bool foundDisqualified = false;
    for (const auto id : d.disqualified) { if (id == CandidateId(1)) foundDisqualified = true; }
    CHECK(foundDisqualified);
    bool foundRejection = false;
    for (const auto& rej : d.rejections) {
        if (rej.candidateId == CandidateId(1) && rej.status == EligibilityStatus::REJECT_CAPACITY) foundRejection = true;
    }
    CHECK(foundRejection);
}

int main() { return testharness::run_all("test_adversarial"); }
