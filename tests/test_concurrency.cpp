#include "test_common.hpp"

struct ThreadResult {
    std::vector<CandidateId> eligible;
    CandidateId winner;
    bool produced{false};
    bool commitOk{false};
    bool handoffOk{false};
    bool stateOk{false};
    std::string err;
};

TESTCASE(concurrent_schedule_commit_handoff) {
    Scheduler sched(default_config());
    const int kCandidates = 6;
    for (int i = 1; i <= kCandidates; ++i) {
        CandidateSpec cs; cs.candidateId = CandidateId(i);
        sched.ingest_candidate(build_candidate(cs));
    }
    const int kThreads = 8;
    std::vector<ThreadResult> results(kThreads);
    TestBroker broker;
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&, t]() {
            try {
                AlwaysReadyDependency dep;
                AllowAllPolicy pol;
                SchedulingRequest req = build_request(DemandSpec{}, SchedulingRequestId(100 + t));
                auto d = sched.schedule(req, &dep, &pol);
                if (!d.producedPlan) { results[t].err = "no plan produced"; return; }
                results[t].produced = true;
                results[t].eligible = d.eligible;
                results[t].winner = d.plan.selectedCandidate;
                auto cr = sched.commit(d.plan.planId, broker);
                results[t].commitOk = cr.succeeded;
                ReferenceExecutionAdapter exec(AuthorityGeneration(1));
                auto h = sched.handoff(d.plan.planId, exec);
                results[t].handoffOk = h.authorized;
                results[t].stateOk = (sched.plan_state(d.plan.planId) == PlacementLifecycleState::ACTIVE);
            } catch (const std::exception& e) {
                results[t].err = e.what();
            } catch (...) {
                results[t].err = "unknown exception";
            }
        });
    }
    for (auto& th : threads) th.join();
    for (int t = 0; t < kThreads; ++t) {
        CHECK(results[t].err.empty());
        CHECK(results[t].produced);
        CHECK(results[t].commitOk);
        CHECK(results[t].handoffOk);
        CHECK(results[t].stateOk);
        CHECK(results[t].winner == results[0].winner);
        CHECK(results[t].eligible == results[0].eligible);
    }
    CHECK(broker.commits.load() >= kThreads);
}

int main() { return testharness::run_all("test_concurrency"); }
