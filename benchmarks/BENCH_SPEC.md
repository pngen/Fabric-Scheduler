# Fabric Scheduler benchmark specification

Repository: E:\The Journey\Coding\GitHub\production\Fabric-Scheduler.

## Environment
- Configured build dir: E:\The Journey\Coding\GitHub\production\Fabric-Scheduler\build
- Build: cmake --build "E:\The Journey\Coding\GitHub\production\Fabric-Scheduler\build" --config Release --target bench_candidate
- Run: "E:\The Journey\Coding\GitHub\production\Fabric-Scheduler\build\benchmarks\Release\bench_candidate.exe" [scale]
- Add new benchmark executables to benchmarks/CMakeLists.txt linking FabricScheduler::FabricScheduler. Ensure they compile with /W4 /WX (zero warnings).

## Goals
Measure completed scheduling operations at scales: 100, 1000, 10000, 100000 candidates (fall back to the largest scale that completes in a reasonable wall time, and report which).

Measures per scale, using std::chrono::high_resolution_clock and reporting nanosecond/second timings:
- candidate ingest (N ingests)
- a full scheduling pass: hard filtering + ranking + plan creation (schedule once over the N candidates, reporting its duration)
- revalidation of the produced plan
- explanation generation (explain(planId))
- candidate retrieval (candidate(id))
- concurrent scheduling throughput: e.g., 8 threads each running 100 schedules over the same candidate set; report total wall time and schedules/sec

Also measure a dense-eligibility and a heavy-rejection scenario (e.g., candidates pre-configured so most fail a hard constraint) and report the same core timings.

## Data
- Use deterministic seeded pseudo-random generation (a fixed PRNG, e.g. a small xorshift/splitmix64) so runs are reproducible.
- Each candidate: CandidateId(i+1), CandidateGeneration(1), WorkerBootId, SourceBootId, a Cuda binding, measured capacity with vramAvailable randomized over a range, health.ready true, congestion 0, locality numaDistance/pcieHops random, residency stateLocal random. Provenance Measured. A small set of candidates (e.g., every 7th) should be made hard-ineligible (health.ready=false) for the heavy-rejection scenario.
- Record the exact candidate/evidence dimensions (counts) and the seed in the printed output.

## Requirements
- Zero warnings under /W4 /WX. No test timeouts. No sleeps as correctness.
- The library is already correct; do not modify it unless you find a genuine defect (note it).
- Print a clear table of per-scale timings.
- Return a concise summary of the measured numbers.
