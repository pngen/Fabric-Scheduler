# Fabric Scheduler core test-suite specification

Repository: E:\The Journey\Coding\GitHub\production\Fabric-Scheduler (C++20, MSVC, /W4 /WX).

## Environment / build
- Configured VS build dir: E:\The Journey\Coding\GitHub\production\Fabric-Scheduler\build
- Build target: cmake --build "E:\The Journey\Coding\GitHub\production\Fabric-Scheduler\build" --config Release --target <Target>
- Run: "E:\The Journey\Coding\GitHub\production\Fabric-Scheduler\build\tests\Release\<Target>.exe"
- After editing tests/CMakeLists.txt reconfigure:
  cmake -S "E:\The Journey\Coding\GitHub\production\Fabric-Scheduler" -B "E:\The Journey\Coding\GitHub\production\Fabric-Scheduler\build" -G "Visual Studio 17 2022" -A x64 -DFABRIC_BUILD_CUDA=OFF
- Add each test to tests/CMakeLists.txt as:
  add_executable(<name> <file>)
  target_link_libraries(<name> PRIVATE FabricScheduler::FabricScheduler)
  add_test(NAME <name> COMMAND <name>)
- Link against FabricScheduler::FabricScheduler. Use #include "fabric_scheduler/...".

## API (authoritative - read these headers)
- include/fabric_scheduler/scheduler.hpp
- include/fabric_scheduler/request.hpp
- include/fabric_scheduler/candidate.hpp
- include/fabric_scheduler/eligibility.hpp
- include/fabric_scheduler/ranking.hpp
- include/fabric_scheduler/placement.hpp
- include/fabric_scheduler/policy.hpp
- include/fabric_scheduler/revalidation.hpp
- include/fabric_scheduler/core/*.hpp
- include/fabric_scheduler/adapters/*.hpp
- ByteCount literals: 1_MiB, 1_GiB, _B. Duration literals _ms, _s.

## Key API openers
- Scheduler::Config cfg; cfg.policy.generation = PolicyGeneration(1); cfg.rankOptions; cfg.maxCandidatesPerRequest = CoreCount(N); Scheduler sched(cfg);
- sched.ingest_candidate(cand); sched.candidate_count(); sched.ingest_watermark();
- sched.schedule(request, &dep, &policy) -> PlacementDecision { producedPlan, plan, eligible(vector<CandidateId>), disqualified, rejections, summary }
- sched.commit(planId, broker) -> ResourceCommitResult {succeeded, commitmentToken, resourceGeneration, failureReason, provisional}
- sched.handoff(planId, exec) -> ExecutionHandoff {authorized, authorityGeneration, ...} ; ReferenceExecutionAdapter(acceptedAuthority)
- sched.revalidate(planId, fingerprint) -> RevalidationResult {current, changes, resultingState}
- sched.cancel(requestId, reason); sched.supersede(oldId, newId, reason); sched.explain(planId); sched.plan_state(planId); sched.plan(id)->optional<PlacementPlan>; sched.candidate(id)->optional<Candidate>; sched.set_epoch(epoch); sched.epoch(); sched.current_authority(); sched.fence_worker(boot); sched.fence_source(boot)
- PlacementLifecycleState: REQUESTED, DISCOVERING, FILTERING, RANKING, PLAN_READY, AWAITING_RESOURCE_COMMIT, COMMITTED, AWAITING_EXECUTION, ACTIVE, REVALIDATION_REQUIRED, SUPERSEDED, CANCELLED, FAILED, RETIRED
- Scheduler throws fabric::FabricError (has .code(), .what()). schedule() calls request.demand.validate() which throws on malformed demand.
- A candidate must have valid candidateId, candidateGeneration, sourceBootId, workerBootId, and at least one binding (or capacity.vramTotal>0). ingest_candidate throws on duplicate/stale id (refresh only at higher generation) and on fenced worker/source boot.
- HardFilter is a pure functor: eligibilityResult = filter.evaluate(request, candidate, &depView, &policyVeto).

## Required tests (create clearly named executables)
test_eligibility.cpp, test_ranking_determinism.cpp, test_lifecycle.cpp, test_supersede_cancel.cpp, test_concurrency.cpp, test_adversarial.cpp

1. Hard eligibility: for each REJECT_* status build a candidate failing exactly that constraint; assert the status and that it is not eligible. Cover REJECT_CAPABILITY, REJECT_CAPACITY, REJECT_MEMORY, REJECT_CONTIGUITY, REJECT_HEALTH, REJECT_NETWORK, REJECT_STORAGE, REJECT_RESERVATION, REJECT_TOPOLOGY, REJECT_POLICY, REJECT_DEPENDENCY, INSUFFICIENT_EVIDENCE.
2. UNKNOWN never becomes ELIGIBLE: candidate with Unknown capacity/health provenance -> INSUFFICIENT_EVIDENCE.
3. Deterministic ranking: re-ingest same evidence in different insertion order (different candidate ids) -> same winner and same full eligible ordering. Two identical inputs -> identical ordering.
4. A hard-rejected candidate never wins (never appears selected).
5. Lifecycle: placement_can_transition(REQUESTED,ACTIVE)==false, (SUPERSEDED,ACTIVE)==false, (PLAN_READY,COMMITTED)==false, (COMMITTED,COMMITTED)==true. placement_advance throws FabricError(IllegalLifecycleTransition) for illegal transitions.
6. Supersession: schedule req1, supersede(req1,req2); old plan -> SUPERSEDED; commit(oldPlanId) fails; handoff(oldPlanId) unauthorized.
7. Cancellation: schedule, cancel(request); commit fails; handoff unauthorized. Cancelled request must not launch.
8. Resource-commit failure: TestBroker that returns failure -> plan not COMMITTED/ACTIVE (state REVALIDATION_REQUIRED), handoff unauthorized, no false COMMITTED.
9. Generation monotonicity/stale authority: set_epoch(lower) throws; plan built under epoch1 then set_epoch(2) -> plan REVALIDATION_REQUIRED, handoff unauthorized.
10. fenced worker: fence_worker(boot) then ingest candidate with that workerBootId throws FabricError(StaleAuthority).
11. Concurrency: 8 std::threads concurrently scheduling against ONE scheduler (distinct request ids, same candidate set) -> identical winner ordering each; a burst of concurrent commit+handoff; no crashes, no sleeps as correctness.
12. Adversarial: DemandProfile with NaN throughput -> throws NonFinite; min>max accelerator count -> throws InvalidCountRange; Ranker output score.is_finite() and no negative factor values; a candidate with insufficient vram never wins.

## Hard requirements
- Zero warnings under /W4 /WX; unreferenced params cast to (void)param;
- No test timeouts or forced kills anywhere.
- Genuine concurrency (std::thread, std::atomic, std::barrier/latch). No sleep as synchronization.
- Every test PASSES (fix the library only if it is genuinely wrong; prefer fixing the test to match correct semantics for real behavior).
- Build and run in Release AND Debug. Report exact status.
- Do not break existing tests (tests/core_smoke_test.cpp, tests/scheduler_pipeline_test.cpp must still pass).
