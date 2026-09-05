// Fabric Scheduler benchmark: candidate ingest / scheduling / revalidation /
// explanation / retrieval / concurrent throughput, at a configured scale.
//
// Build:  cmake --build build --config Release --target bench_candidate
// Run:    build\benchmarks\Release\bench_candidate.exe [scale]
//
// Deterministic seeded PRNG (splitmix64) so every run is reproducible.
// Measures per scale:
//   - candidate ingest (N ingests)
//   - one full scheduling pass (hard filter + ranking + plan creation)
//   - plan revalidation
//   - explanation generation (explain(planId))
//   - candidate retrieval (candidate(id))
//   - concurrent scheduling throughput (8 threads x 100 schedules)
// Across a dense-eligibility, a heavy-rejection, and an over-demand scenario.

#include "fabric_scheduler/scheduler.hpp"
#include "fabric_scheduler/revalidation.hpp"
#include "fabric_scheduler/adapters/dependency.hpp"
#include "fabric_scheduler/adapters/resource_broker.hpp"
#include "fabric_scheduler/adapters/execution.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

using namespace fabric;

namespace {

using Clock = std::chrono::high_resolution_clock;

constexpr std::uint64_t kGiB = 1ull << 30;   // binary GiB
constexpr std::uint64_t kMiB = 1ull << 20;   // binary MiB
constexpr std::uint64_t kSeed = 0x5EED0F00Dull;

// ---------------------------------------------------------------------------
// Deterministic splitmix64 PRNG. Reproducible across runs and toolchains.
// ---------------------------------------------------------------------------
class SplitMix64 {
public:
    explicit SplitMix64(std::uint64_t seed) noexcept : s_(seed) {}

    std::uint64_t next() noexcept {
        std::uint64_t z = (s_ += 0x9E3779B97F4A7C15ull);
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBull;
        return z ^ (z >> 31);
    }

    // Uniform value in [lo, hi] inclusive.
    std::uint64_t in_range(std::uint64_t lo, std::uint64_t hi) {
        return lo + (next() % (hi - lo + 1));
    }

private:
    std::uint64_t s_;
};

// ---------------------------------------------------------------------------
// Candidate dimension summary (recorded in output for reproducibility).
// ---------------------------------------------------------------------------
struct CandidateDims {
    std::size_t bindingCount = 1;
    ByteCount vramTotal = ByteCount(96 * kGiB);
    ByteCount vramLo = ByteCount(16 * kGiB);
    ByteCount vramHi = ByteCount(63 * kGiB);
    CoreCount cpuAvailable = CoreCount(16);
    ByteCount storageAvailable = ByteCount(256 * kGiB);
    ByteCount hostMemLo = ByteCount(16 * kGiB);
    ByteCount hostMemHi = ByteCount(128 * kGiB);
    std::uint32_t numaLo = 0;
    std::uint32_t numaHi = 3;
    std::uint32_t pcieLo = 0;
    std::uint32_t pcieHi = 4;
};

const CandidateDims kDims;

// Build one deterministic candidate. When heavyHealthReject, every 7th
// candidate is hard-ineligible (health.ready=false), per the spec's recipe.
// forceEligible guarantees a winning candidate (top VRAM + healthy when asked).
Candidate make_candidate(std::size_t index, SplitMix64& rng,
                         bool heavyHealthReject, bool forceEligible) {
    const std::uint64_t idVal = static_cast<std::uint64_t>(index + 1);

    Candidate c;
    c.candidateId = CandidateId(idVal);
    c.candidateGeneration = CandidateGeneration(1);
    c.workerId = WorkerId(idVal + 1000);
    c.workerBootId = WorkerBootId(idVal + 100000);   // valid boot id required by ingest
    c.sourceId = SourceId(idVal + 5000);
    c.sourceBootId = SourceBootId(idVal + 200000);   // valid boot id required by ingest
    c.provenance = Provenance::Measured;

    // Single Cuda binding.
    DeviceBinding b;
    b.deviceId = DeviceId(idVal);
    b.deviceGeneration = DeviceGeneration(1);
    b.resourceId = ResourceId(idVal);
    b.resourceGeneration = ResourceGeneration(1);
    b.nodeId = NodeId(1);
    b.nodeGeneration = NodeGeneration(1);
    b.memoryDomainId = MemoryDomainId(rng.in_range(1, 4));
    b.memoryDomainGeneration = MemoryDomainGeneration(1);
    b.nicId = NicId(idVal + 100);
    b.nicGeneration = NicGeneration(1);
    b.storageId = StorageEndpointId(1);
    b.storageGeneration = StorageEndpointGeneration(1);
    b.cpuDomainId = CpuDomainId(1);
    b.cpuDomainGeneration = CpuDomainGeneration(1);
    b.capability = AcceleratorClass::Cuda;
    b.cudaCompatibility = 120;
    b.capabilityCurrent = true;
    b.capabilityGeneration = CapabilityGeneration(1);
    b.sharesPcieRoot = false;
    c.bindings.push_back(b);

    // Capacity: measured, VRAM random over [lo, hi] (always above the dense
    // demand), or the top of the range when a winning candidate is forced.
    c.capacity.generation = CapacityGeneration(1);
    c.capacity.vramTotal = kDims.vramTotal;
    const std::uint64_t vramAvail = forceEligible
        ? kDims.vramHi.value()
        : rng.in_range(kDims.vramLo.value(), kDims.vramHi.value());
    c.capacity.vramAvailable = ByteCount(vramAvail);
    c.capacity.hostMemoryAvailable =
        ByteCount(rng.in_range(kDims.hostMemLo.value(), kDims.hostMemHi.value()));
    c.capacity.pinnedMemoryAvailable = ByteCount(8 * kGiB);
    c.capacity.cpuAvailable = kDims.cpuAvailable;
    c.capacity.storageAvailable = kDims.storageAvailable;
    c.capacity.nicBandwidthAvailable = ByteCount(400 * kGiB);
    c.capacity.contiguousMemory = true;
    c.capacity.provenance = Provenance::Measured;

    // Health. Every 7th candidate is hard-ineligible when requested.
    bool ready = true;
    if (heavyHealthReject && (index % 7 == 0)) ready = false;
    if (forceEligible) ready = true;
    c.health.generation = HealthGeneration(1);
    c.health.ready = ready;
    c.health.healthScore = 1.0;
    c.health.provenance = Provenance::Reported;

    c.reservation.generation = ReservationGeneration(1);
    c.reservation.reservedBytes = ByteCount(0);
    c.reservation.provenance = Provenance::Reported;

    c.congestion.generation = CongestionGeneration(1);
    c.congestion.congestion = 0.0;
    c.congestion.residualBandwidth = 1.0;
    c.congestion.provenance = Provenance::Measured;

    c.locality.topologyGeneration = TopologyGeneration(1);
    c.locality.numaDistance =
        static_cast<std::uint32_t>(rng.in_range(kDims.numaLo, kDims.numaHi));
    c.locality.pcieHops =
        static_cast<std::uint32_t>(rng.in_range(kDims.pcieLo, kDims.pcieHi));
    c.locality.nicDistance = static_cast<std::uint32_t>(rng.in_range(0, 3));
    c.locality.storageDistance = static_cast<std::uint32_t>(rng.in_range(0, 3));
    c.locality.topologyGroup = "group-1";
    c.locality.provenance = Provenance::Reported;

    c.communication.planGeneration = CommunicationPlanGeneration(1);
    c.communication.estimatedCost = Cost(0.0);
    c.communication.derivedPathCost = Cost(0.0);
    c.communication.collectiveCost = Cost(0.0);
    c.communication.pathFeasible = true;
    c.communication.provenance = Provenance::Derived;

    // Residency: randomized state-locality.
    c.residency.residencyGeneration = ResidencyGeneration(1);
    c.residency.modelResident = true;
    c.residency.adapterResident = true;
    c.residency.stateLocal = (rng.next() % 2 == 0);
    c.residency.moveBytes = c.residency.stateLocal ? ByteCount(0) : ByteCount(512 * kGiB);
    c.residency.provenance = Provenance::Reported;

    c.policyGeneration = PolicyGeneration(1);
    c.memoryMovementCost = Cost(c.residency.stateLocal ? 0.0 : 0.4);
    return c;
}

// Build the scheduling request. vramPerDevice is the accelerator capacity
// gate; the dense scenario uses a small gate (most pass), an over-demand gate
// makes most/all candidates fail a hard constraint.
SchedulingRequest make_request(SchedulingRequestId id, ByteCount vramPerDevice) {
    SchedulingRequest r;
    r.requestId = id;
    r.generation = SchedulingRequestGeneration(1);
    r.coordinatorEpoch = CoordinatorEpoch(1);
    r.sourceId = SourceId(3);
    r.sourceBootId = SourceBootId(30);
    r.demand.demandId = WorkloadDemandId(1);
    r.demand.demandGeneration = WorkloadDemandGeneration(1);
    r.demand.workloadId = WorkloadId(4);
    r.demand.workloadGeneration = WorkloadGeneration(1);
    r.demand.accelerator.capability = AcceleratorClass::Cuda;
    r.demand.accelerator.minimumCount = CoreCount(1);
    r.demand.accelerator.targetCount = CoreCount(1);
    r.demand.accelerator.maximumCount = CoreCount(1);
    r.demand.accelerator.vramPerDevice = vramPerDevice;
    r.demand.accelerator.aggregateVram = ByteCount(0);
    r.demand.accelerator.capabilityGeneration = CapabilityGeneration(1);
    r.demand.memory.hostMemory = ByteCount(1 * kGiB);
    r.demand.memory.pinnedMemory = ByteCount(256 * kMiB);
    r.demand.cpuRequired = CoreCount(2);
    r.demand.storage.capacity = ByteCount(4 * kGiB);
    r.demand.storage.requireLocal = false;
    r.demand.policyGeneration = PolicyGeneration(1);
    r.demand.requiredTopologyGeneration = TopologyGeneration(1);
    r.demand.latencySensitivity = LatencySensitivity::High;
    return r;
}

}  // namespace

double to_seconds(long long ns) { return static_cast<double>(ns) / 1e9; }

struct ScenarioReport {
    std::size_t n = 0;
    long long ingestNs = 0;
    long long scheduleNs = 0;
    long long revalidateNs = 0;
    long long explainNs = 0;
    long long retrievalNs = 0;
    long long concurrentNs = 0;
    std::size_t eligible = 0;
    std::size_t disqualified = 0;
    bool producedPlan = false;
    CandidateId winner{0};
};

void run_scenario(std::size_t n, bool heavy, ByteCount gateVram, bool runConcurrent, ScenarioReport& out) {
    SplitMix64 rng(kSeed + (heavy ? 977ull : 0ull));

    // Generate the candidate pool.
    std::vector<Candidate> pool;
    pool.reserve(n);
    for (std::size_t i = 0; i < n; ++i) {
        pool.push_back(make_candidate(i, rng, heavy, false));
    }

    Scheduler::Config cfg;
    cfg.policy.generation = PolicyGeneration(1);
    cfg.rankOptions.policyGeneration = PolicyGeneration(1);
    cfg.maxCandidatesPerRequest = CoreCount(256);
    Scheduler sched(cfg);

    // 1. Candidate ingest (N ingests).
    auto t0 = Clock::now();
    for (std::size_t i = 0; i < n; ++i) sched.ingest_candidate(pool[i]);
    auto t1 = Clock::now();
    out.ingestNs = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();

    SchedulingRequest req = make_request(SchedulingRequestId(1), gateVram);
    AlwaysReadyDependency dep;
    AllowAllPolicy pol;

    // 2. Full scheduling pass.
    auto t2 = Clock::now();
    PlacementDecision dec = sched.schedule(req, &dep, &pol);
    auto t3 = Clock::now();
    out.scheduleNs = std::chrono::duration_cast<std::chrono::nanoseconds>(t3 - t2).count();
    out.eligible = dec.eligible.size();
    out.disqualified = dec.disqualified.size();
    out.producedPlan = dec.producedPlan;
    if (out.producedPlan) out.winner = dec.plan.selectedCandidate;

    PlacementPlanId planId(0);
    if (out.producedPlan) planId = dec.plan.planId;

    long long revalidateNs = 0;
    long long explainNs = 0;
    if (out.producedPlan) {
        auto planOpt = sched.plan(planId);
        if (planOpt.has_value()) {
            GenerationFingerprint current = planOpt->generations;  // unchanged
            auto t4 = Clock::now();
            RevalidationResult rr = sched.revalidate(planId, current);
            auto t5 = Clock::now();
            revalidateNs = std::chrono::duration_cast<std::chrono::nanoseconds>(t5 - t4).count();
            (void)rr;
        }

        auto t6 = Clock::now();
        PlacementExplanation ex = sched.explain(planId);
        auto t7 = Clock::now();
        explainNs = std::chrono::duration_cast<std::chrono::nanoseconds>(t7 - t6).count();
        (void)ex;
    }
    out.revalidateNs = revalidateNs;
    out.explainNs = explainNs;

    // 5. Candidate retrieval (candidate(id)) for all N candidates.
    auto t8 = Clock::now();
    for (std::size_t i = 1; i <= n; ++i) {
        auto c = sched.candidate(CandidateId(static_cast<std::uint64_t>(i)));
        (void)c;
    }
    auto t9 = Clock::now();
    out.retrievalNs = std::chrono::duration_cast<std::chrono::nanoseconds>(t9 - t8).count();

    // 6. Concurrent scheduling throughput: 8 threads x 100 schedules.
    if (runConcurrent) {
        constexpr int kThreads = 8;
        constexpr int kPerThread = 100;
        SchedulingRequest concurrentReq =
            make_request(SchedulingRequestId(static_cast<std::uint64_t>(n) + 1), gateVram);
        std::atomic<bool> go{false};
        std::atomic<int> done{0};
        auto tA = Clock::now();
        std::vector<std::thread> threads;
        threads.reserve(kThreads);
        for (int t = 0; t < kThreads; ++t) {
            threads.emplace_back([&]() {
                while (!go.load(std::memory_order_acquire)) { /* spin */ }
                for (int k = 0; k < kPerThread; ++k) {
                    PlacementDecision d = sched.schedule(concurrentReq, &dep, &pol);
                    (void)d;
                }
                done.fetch_add(1, std::memory_order_release);
            });
        }
        go.store(true, std::memory_order_release);
        for (auto& th : threads) th.join();
        while (done.load(std::memory_order_acquire) < kThreads) { /* spin */ }
        auto tB = Clock::now();
        out.concurrentNs = std::chrono::duration_cast<std::chrono::nanoseconds>(tB - tA).count();
    }

    out.n = n;
}

int main(int argc, char** argv) {
    long long scale = 1000;
    if (argc > 1) scale = std::stoll(argv[1]);
    if (scale <= 0) scale = 1000;
    const std::size_t n = static_cast<std::size_t>(scale);

    std::cout << "Fabric Scheduler benchmark\n";
    std::cout << "seed=" << kSeed << "  scale N=" << n << "\n";
    std::cout << "candidate dims: bindings=" << kDims.bindingCount
              << " vramTotal=" << (kDims.vramTotal.value() / kGiB) << "GiB"
              << " vramAvail=[" << (kDims.vramLo.value() / kGiB) << ","
              << (kDims.vramHi.value() / kGiB) << "]GiB"
              << " cpuAvail=" << kDims.cpuAvailable.value()
              << " numa=[" << kDims.numaLo << "," << kDims.numaHi << "]"
              << " pcie=[" << kDims.pcieLo << "," << kDims.pcieHi << "]\n";

    ScenarioReport dense;
    run_scenario(n, false, ByteCount(8 * kGiB), true, dense);   // all candidates eligible
    ScenarioReport heavy;
    run_scenario(n, true, ByteCount(8 * kGiB), true, heavy);    // every 7th hard-ineligible
    ScenarioReport over;
    run_scenario(n, true, ByteCount(48 * kGiB), false, over);   // strict VRAM gate, concurrency skipped

    const int w = 22;
    auto header = [&](const char* label) {
        std::cout << "\n=== " << label << " ===\n";
        std::cout << std::left << std::setw(w) << "measure"
                  << std::setw(16) << "ns"
                  << std::setw(16) << "seconds"
                  << std::setw(16) << "per-op" << "\n"
                  << std::string(70, '-') << "\n";
    };

    auto printReport = [&](const char* label, const ScenarioReport& r) {
        header(label);
        std::cout << std::left << std::setw(w) << "ingest (N)" << std::setw(16) << r.ingestNs
                  << std::setw(16) << std::fixed << std::setprecision(6) << to_seconds(r.ingestNs)
                  << std::setw(16) << "" << "\n";
        std::cout << std::left << std::setw(w) << "schedule pass" << std::setw(16) << r.scheduleNs
                  << std::setw(16) << std::fixed << std::setprecision(6) << to_seconds(r.scheduleNs)
                  << std::setw(16) << "" << "\n";
        std::cout << std::left << std::setw(w) << "revalidate" << std::setw(16) << r.revalidateNs
                  << std::setw(16) << std::fixed << std::setprecision(6) << to_seconds(r.revalidateNs)
                  << std::setw(16) << "" << "\n";
        std::cout << std::left << std::setw(w) << "explain" << std::setw(16) << r.explainNs
                  << std::setw(16) << std::fixed << std::setprecision(6) << to_seconds(r.explainNs)
                  << std::setw(16) << "" << "\n";
        std::cout << std::left << std::setw(w) << "retrieval (N)" << std::setw(16) << r.retrievalNs
                  << std::setw(16) << std::fixed << std::setprecision(6) << to_seconds(r.retrievalNs)
                  << std::setw(16) << "" << "\n";
        std::cout << std::left << std::setw(w) << "concurrent (8x100)" << std::setw(16) << r.concurrentNs
                  << std::setw(16) << std::fixed << std::setprecision(6) << to_seconds(r.concurrentNs)
                  << std::setw(16) << (r.concurrentNs == 0 ? "skipped" : "") << "\n";
        std::cout << std::left << std::setw(w) << "eligible" << std::setw(16) << r.eligible << "\n";
        std::cout << std::left << std::setw(w) << "disqualified" << std::setw(16) << r.disqualified << "\n";
        std::cout << std::left << std::setw(w) << "producedPlan" << std::setw(16) << (r.producedPlan ? "yes" : "no") << "\n";
        if (r.producedPlan) {
            std::cout << std::left << std::setw(w) << "winner" << std::setw(16) << r.winner.value() << "\n";
        }
        if (r.scheduleNs > 0) {
            std::cout << std::left << std::setw(w) << "sched cands/sec"
                      << std::setw(16) << "" << std::setw(16) << "" << std::setw(16) << std::fixed
                      << std::setprecision(0) << (static_cast<double>(r.n) / to_seconds(r.scheduleNs)) << "\n";
        }
        if (to_seconds(r.concurrentNs) > 0.0) {
            std::cout << std::left << std::setw(w) << "concurrent sched/sec"
                      << std::setw(16) << "" << std::setw(16) << "" << std::setw(16) << std::fixed
                      << std::setprecision(2) << (800.0 / to_seconds(r.concurrentNs)) << "\n";
        }
        std::cout << std::left;
    };

    printReport("Dense-eligibility scenario", dense);
    printReport("Heavy-rejection scenario", heavy);
    printReport("Over-demand (most fail) scenario", over);

    // --- Aggregate summary table --------------------------------------
    std::cout << "\n=== Summary per scale ===\n";
    std::cout << std::left << std::setw(14) << "scenario"
              << std::setw(12) << "N"
              << std::setw(16) << "schedule_ms"
              << std::setw(12) << "eligible"
              << std::setw(12) << "rejected"
              << std::setw(18) << "sched_cands/sec"
              << std::setw(20) << "conc_sched/sec\n"
              << std::string(104, '-') << "\n";
    auto sumRow = [&](const char* label, const ScenarioReport& r) {
        const double schedMs = to_seconds(r.scheduleNs) * 1000.0;
        const double candsPerSec = r.scheduleNs > 0 ? (static_cast<double>(r.n) / to_seconds(r.scheduleNs)) : 0.0;
        const bool hasConc = to_seconds(r.concurrentNs) > 0.0;
        const double concPerSec = hasConc ? (800.0 / to_seconds(r.concurrentNs)) : 0.0;
        std::cout << std::left << std::setw(14) << label
                  << std::setw(12) << r.n
                  << std::right << std::setw(16) << std::fixed << std::setprecision(3) << schedMs
                  << std::setw(12) << r.eligible
                  << std::setw(12) << r.disqualified
                  << std::setw(18) << std::setprecision(0) << candsPerSec
                  << std::setw(20) << std::setprecision(2);
        if (hasConc) std::cout << concPerSec; else std::cout << "n/a";
        std::cout << "\n";
    };
    sumRow("dense", dense);
    sumRow("heavy", heavy);
    sumRow("over-demand", over);

    return 0;
}
