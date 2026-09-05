// worker_main.cpp
// Reference distributed worker for the Fabric Scheduler multiprocess proof.
// Connects to the coordinator over framed loopback TCP, registers, publishes a
// deterministic scenario candidate, then idles (keeping the connection open) so
// the coordinator observes the worker as alive until the process is killed.
#include "fabric_scheduler/net.hpp"
#include "fabric_scheduler/protocol.hpp"
#include "fabric_scheduler/persistence.hpp"
#include "fabric_scheduler/coding.hpp"

#include <chrono>
#include <cstdint>
#include <iostream>
#include <string>
#include <thread>

using namespace fabric;

namespace {

// Deterministic candidate factory.       numa  pcie  vram   stateLocal  congestion
//   A  (local)      :                    0     1    16GiB  true        0.00
//   B  (remote)     :                    2     3     8GiB  false       0.00
//   A2 (local + sat):                    0     1    16GiB  true        0.95, gen 2
Candidate make_scenario_candidate(std::uint64_t boot, const std::string& scenario) {
    const bool isB = (scenario == "B");
    const bool isA2 = (scenario == "A2");

    Candidate c;
    c.candidateId = CandidateId(boot);
    c.candidateGeneration = CandidateGeneration(1);
    c.workerId = WorkerId(boot);
    c.workerBootId = WorkerBootId(boot);
    c.sourceId = SourceId(boot + 5000);
    c.sourceBootId = SourceBootId(100000 + boot);
    c.provenance = Provenance::Reported;

    DeviceBinding b;
    b.deviceId = DeviceId(boot);
    b.deviceGeneration = DeviceGeneration(1);
    b.resourceId = ResourceId(boot);
    b.resourceGeneration = ResourceGeneration(1);
    b.nodeId = NodeId(1);
    b.nodeGeneration = NodeGeneration(1);
    b.memoryDomainId = MemoryDomainId(1);
    b.memoryDomainGeneration = MemoryDomainGeneration(1);
    b.nicId = NicId(boot + 100);
    b.nicGeneration = NicGeneration(1);
    b.storageId = StorageEndpointId(1);
    b.storageGeneration = StorageEndpointGeneration(1);
    b.cpuDomainId = CpuDomainId(1);
    b.cpuDomainGeneration = CpuDomainGeneration(1);
    b.pcieRoot = "root-" + std::to_string(boot);
    b.sharesPcieRoot = false;
    b.capability = AcceleratorClass::Cuda;
    b.cudaCompatibility = 120;
    b.capabilityCurrent = true;
    b.capabilityGeneration = CapabilityGeneration(1);
    c.bindings.push_back(std::move(b));

    c.capacity.generation = CapacityGeneration(1);
    c.capacity.vramTotal = 16_GiB;
    c.capacity.vramUsed = ByteCount(0);
    c.capacity.vramAvailable = isB ? 8_GiB : 16_GiB;
    c.capacity.hostMemoryAvailable = 16_GiB;
    c.capacity.pinnedMemoryAvailable = 4_GiB;
    c.capacity.cpuAvailable = CoreCount(8);
    c.capacity.storageAvailable = 128_GiB;
    c.capacity.nicBandwidthAvailable = 400_GiB;
    c.capacity.contiguousMemory = true;
    c.capacity.provenance = Provenance::Measured;

    c.health.generation = HealthGeneration(1);
    c.health.ready = true;
    c.health.healthScore = 1.0;
    c.health.provenance = Provenance::Reported;

    c.reservation.generation = ReservationGeneration(1);
    c.reservation.provenance = Provenance::Reported;

    c.congestion.generation = isA2 ? CongestionGeneration(2) : CongestionGeneration(1);
    c.congestion.congestion = isA2 ? 0.95 : 0.0;
    c.congestion.residualBandwidth = isA2 ? 0.1 : 1.0;
    c.congestion.bandwidthNominal = ByteCount(0);
    c.congestion.bottleneck.clear();
    c.congestion.provenance = Provenance::Measured;

    c.locality.topologyGeneration = TopologyGeneration(1);
    c.locality.numaDistance = isB ? 2 : 0;
    c.locality.pcieHops = isB ? 3 : 1;
    c.locality.sharesPcieRoot = false;
    c.locality.nicDistance = isB ? 2 : 0;
    c.locality.storageDistance = isB ? 2 : 0;
    c.locality.topologyGroup = "group-1";
    c.locality.provenance = Provenance::Reported;

    c.communication.planGeneration = CommunicationPlanGeneration(1);
    c.communication.estimatedCost = Cost(isB ? 0.3 : 0.0);
    c.communication.derivedPathCost = Cost(isB ? 0.2 : 0.0);
    c.communication.collectiveCost = Cost(0.0);
    c.communication.pathFeasible = true;
    c.communication.planId = "ref";
    c.communication.provenance = Provenance::Derived;

    c.residency.residencyGeneration = ResidencyGeneration(1);
    c.residency.modelResident = true;
    c.residency.adapterResident = false;
    c.residency.stateLocal = !isB;
    c.residency.moveBytes = isB ? 512_MiB : ByteCount(0);
    c.residency.moveSource = isB ? "src" : "";
    c.residency.provenance = Provenance::Reported;

    c.policyGeneration = PolicyGeneration(1);
    c.memoryMovementCost = Cost(isB ? 0.4 : 0.0);
    return c;
}

}  // namespace

int main(int argc, char** argv) {
    std::uint16_t port = 0;
    std::string name = "worker";
    std::uint64_t boot = 0;
    std::string mode;
    std::string scenario;
    std::uint64_t waitMs = 0;

    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        if (a == "--port" && i + 1 < argc) {
            port = static_cast<std::uint16_t>(std::stoul(argv[++i]));
        } else if (a == "--name" && i + 1 < argc) {
            name = argv[++i];
        } else if (a == "--boot" && i + 1 < argc) {
            boot = std::stoull(argv[++i]);
        } else if (a == "--mode" && i + 1 < argc) {
            mode = argv[++i];
        } else if (a == "--scenario" && i + 1 < argc) {
            scenario = argv[++i];
        } else if (a == "--wait-ms" && i + 1 < argc) {
            waitMs = std::stoull(argv[++i]);
        } else if (a == "--help") {
            std::cout << "usage: worker_main --port <p> --name <n> --boot <b> --mode <A|B|A2>\n";
            return 0;
        }
    }

    if (scenario.empty()) scenario = mode;
    if (scenario.empty()) scenario = "A";

    WinsockInit winsock;

    auto channel = TcpChannel::connect("127.0.0.1", port);
    if (!channel) {
        std::cerr << "worker " << name << ": connect to coordinator failed\n";
        return 1;
    }
    TcpChannel ch = std::move(*channel);

    // REGISTER (workerId, bootId, name).
    {
        ByteWriter w;
        w.put_u64(boot);
        w.put_u64(boot);
        w.put_string(name);
        ch.send_frame(MessageType::REGISTER, w.take());
    }

    if (waitMs > 0) std::this_thread::sleep_for(std::chrono::milliseconds(waitMs));

    // Publish the scenario candidate (PUBLISH_RESOURCE carries the whole
    // candidate snapshot).
    {
        Candidate cand = make_scenario_candidate(boot, scenario);
        ByteWriter w;
        encode_candidate_record(w, cand);
        ch.send_frame(MessageType::PUBLISH_RESOURCE, w.take());
    }

    std::cout << "PUBLISHED " << name << std::endl;
    std::cout.flush();

    // Keep the connection open so the coordinator sees the worker as alive.
    while (true) {
        try {
            auto [type, payload] = ch.recv_frame();
            (void)payload;
            if (type == MessageType::SHUTDOWN) break;
            if (type == MessageType::ERROR) {
                std::cerr << "worker " << name << ": received ERROR frame\n";
            }
        } catch (...) {
            break;
        }
    }
    return 0;
}
