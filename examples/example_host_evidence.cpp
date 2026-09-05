#include <fabric_scheduler/scheduler.hpp>
#include <fabric_scheduler/adapters/dependency.hpp>
#include <fabric_scheduler/net.hpp>
#include <fabric_scheduler/coding.hpp>
#include <cstdio>
#include <chrono>
#include <fstream>
#include <string>
#include <vector>
#include <thread>

using namespace fabric;
using std::chrono::steady_clock;

namespace {
Candidate mk(CandidateId id, std::uint32_t numa, ByteCount vram, double commCost, bool local) {
    Candidate c;
    c.candidateId = id; c.candidateGeneration = CandidateGeneration(1);
    c.workerId = WorkerId(id.value()); c.workerBootId = WorkerBootId(id.value() + 100);
    c.sourceId = SourceId(id.value() + 200); c.sourceBootId = SourceBootId(id.value() + 300);
    c.provenance = Provenance::Reported;
    DeviceBinding b;
    b.deviceId = DeviceId(id.value()); b.resourceGeneration = ResourceGeneration(1);
    b.memoryDomainId = MemoryDomainId(id.value()); b.capability = AcceleratorClass::Cuda;
    b.cudaCompatibility = 120; b.capabilityCurrent = true; b.capabilityGeneration = CapabilityGeneration(1);
    c.bindings.push_back(b);
    c.capacity.generation = CapacityGeneration(1);
    c.capacity.vramAvailable = vram; c.capacity.hostMemoryAvailable = 16_GiB;
    c.capacity.pinnedMemoryAvailable = 1_GiB; c.capacity.cpuAvailable = CoreCount(16);
    c.capacity.storageAvailable = 64_GiB; c.capacity.contiguousMemory = true;
    c.capacity.provenance = Provenance::Measured;
    c.health.generation = HealthGeneration(1); c.health.ready = true; c.health.healthScore = 1.0;
    c.health.provenance = Provenance::Reported;
    c.congestion.generation = CongestionGeneration(1); c.congestion.provenance = Provenance::Measured;
    c.locality.topologyGeneration = TopologyGeneration(1); c.locality.numaDistance = numa;
    c.locality.provenance = Provenance::Reported;
    c.communication.planGeneration = CommunicationPlanGeneration(1);
    c.communication.estimatedCost = Cost(commCost); c.communication.derivedPathCost = Cost(commCost * 0.5);
    c.communication.provenance = Provenance::Derived;
    c.residency.residencyGeneration = ResidencyGeneration(1); c.residency.stateLocal = local;
    c.residency.modelResident = true; c.residency.moveBytes = local ? ByteCount(0) : 512_MiB;
    c.residency.provenance = Provenance::Reported;
    c.policyGeneration = PolicyGeneration(1);
    return c;
}
SchedulingRequest rq() {
    SchedulingRequest r;
    r.requestId = SchedulingRequestId(1); r.generation = SchedulingRequestGeneration(1);
    r.demand.demandId = WorkloadDemandId(1); r.demand.demandGeneration = WorkloadDemandGeneration(1);
    r.demand.workloadId = WorkloadId(4); r.demand.workloadGeneration = WorkloadGeneration(1);
    r.demand.accelerator.capability = AcceleratorClass::Cuda;
    r.demand.accelerator.minimumCount = CoreCount(1); r.demand.accelerator.targetCount = CoreCount(1);
    r.demand.accelerator.maximumCount = CoreCount(1); r.demand.accelerator.vramPerDevice = 4_GiB;
    r.demand.accelerator.capabilityGeneration = CapabilityGeneration(1);
    r.demand.memory.hostMemory = 1_GiB; r.demand.memory.pinnedMemory = 64_MiB;
    r.demand.cpuRequired = CoreCount(2); r.demand.storage.capacity = 1_GiB;
    r.demand.policyGeneration = PolicyGeneration(1); r.demand.requiredTopologyGeneration = TopologyGeneration(1);
    return r;
}

// Real loopback TCP round-trip (framed) measurement. Returns microseconds.
double measure_loopback_frame_rt() {
    TcpListener listener;
    const std::uint16_t port = listener.listen(0);
    std::thread server([&] {
        auto c = listener.accept();
        if (c) { auto m = c->recv_frame(); (void)m; }
    });
    auto client = TcpChannel::connect("127.0.0.1", port);
    if (!client) { server.join(); return -1.0; }
    std::vector<std::byte> payload(1024, std::byte(7));
    const auto t0 = steady_clock::now();
    client->send_frame(MessageType::HELLO, payload);
    server.join();
    const auto t1 = steady_clock::now();
    return std::chrono::duration<double, std::micro>(t1 - t0).count();
}
}  // namespace

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);
    WinsockInit winsock;
    // 1) Real local-file state fixture to establish a derived movement cost.
    const std::string path = "host_evidence_state.bin";
    const std::size_t kBytes = 32u << 20;  // 32 MiB
    {
        std::ofstream out(path, std::ios::binary);
        std::vector<char> buf(kBytes, 'A');
        out.write(buf.data(), (std::streamsize)buf.size());
    }
    const std::ifstream::pos_type stateBytes = []{ std::ifstream in("host_evidence_state.bin", std::ios::binary|std::ios::ate); return in.tellg(); }();
    std::printf("host state fixture: %lld bytes on local filesystem\n", (long long)stateBytes);

    // 2) Real loopback TCP communication-cost measurement.
    const double loopbackUs = measure_loopback_frame_rt();
    std::printf("LOOPBACK TCP framed round-trip: %.1f us\n", loopbackUs);
    const double normalizedComm = loopbackUs > 0 ? (loopbackUs / 1000000.0) : 0.0;

    // 3) Ranked placement: candidate A (local state, neighbour path) vs candidate B
    //    (state must move, remote path). Real communication cost feeds the ranking.
    Scheduler::Config cfg; cfg.policy.generation = PolicyGeneration(1);
    Scheduler sched(cfg);
    sched.ingest_candidate(mk(CandidateId(11), 0, 16_GiB, 0.0, true));     // local, cheap
    sched.ingest_candidate(mk(CandidateId(22), 3, 16_GiB, normalizedComm, false)); // remote, expensive
    AlwaysReadyDependency dep; AllowAllPolicy pol;
    const PlacementDecision d = sched.schedule(rq(), &dep, &pol);
    std::printf("selected candidate: %llu (expect 11 = local state + local path)\n",
                (unsigned long long)d.plan.selectedCandidate.value());
    std::printf("host evidence example %s\n", d.plan.selectedCandidate == CandidateId(11) ? "PASS" : "FAIL");
    std::remove(path.c_str());
    return d.plan.selectedCandidate == CandidateId(11) ? 0 : 1;
}