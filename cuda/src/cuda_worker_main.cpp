#include "fabric_scheduler/scheduler.hpp"
#include "fabric_scheduler/net.hpp"
#include "fabric_scheduler/protocol.hpp"
#include "fabric_scheduler/persistence.hpp"
#include "fabric_scheduler/coding.hpp"
#include "fabric_cuda.h"
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <thread>

using namespace fabric;

int main(int argc, char** argv) {
    std::uint16_t coordPort = 0;
    std::uint16_t ctrlPort = 0;
    std::uint64_t boot = 100;
    std::string name = "cudaA";
    std::string mode = "A";
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        if (a == "--coord-port" && i + 1 < argc) coordPort = static_cast<std::uint16_t>(std::stoul(argv[++i]));
        else if (a == "--ctrl-port" && i + 1 < argc) ctrlPort = static_cast<std::uint16_t>(std::stoul(argv[++i]));
        else if (a == "--boot" && i + 1 < argc) boot = std::stoull(argv[++i]);
        else if (a == "--name" && i + 1 < argc) name = argv[++i];
        else if (a == "--mode" && i + 1 < argc) mode = argv[++i];
    }

    setvbuf(stdout, nullptr, _IONBF, 0);
    WinsockInit winsock;

    // Discover the physical NVIDIA RTX 5090 (real device/capability evidence).
    if (fs_cuda_count() < 1) { std::printf("ERROR no cuda\n"); return 2; }
    FabricCudaDeviceInfo info;
    if (fs_cuda_probe(0, &info) != 0 || !info.present) { std::printf("ERROR probe\n"); return 3; }

    // Connect to the coordinator and REGISTER.
    auto coord = TcpChannel::connect("127.0.0.1", coordPort);
    if (!coord) { std::printf("ERROR connect-coord\n"); return 4; }
    { ByteWriter w; w.put_u64(boot); w.put_u64(boot); w.put_string(name);
      coord->send_frame(MessageType::REGISTER, w.take()); }
    auto regAck = coord->recv_frame();
    if (regAck.first != MessageType::REGISTER) { std::printf("ERROR register-ack\n"); return 5; }

    // Build a candidate carrying real device/capability/memory evidence.
    Candidate c;
    c.candidateId = CandidateId(boot);
    c.candidateGeneration = CandidateGeneration(1);
    c.workerId = WorkerId(boot);
    c.workerBootId = WorkerBootId(boot);
    c.sourceId = SourceId(boot + 7000);
    c.sourceBootId = SourceBootId(boot + 9000);
    c.provenance = Provenance::Measured;
    DeviceBinding b;
    b.deviceId = DeviceId(1); b.deviceGeneration = DeviceGeneration(1);
    b.resourceId = ResourceId(1); b.resourceGeneration = ResourceGeneration(1);
    b.nodeId = NodeId(1); b.nodeGeneration = NodeGeneration(1);
    b.memoryDomainId = MemoryDomainId(1); b.memoryDomainGeneration = MemoryDomainGeneration(1);
    b.nicId = NicId(1); b.nicGeneration = NicGeneration(1);
    b.storageId = StorageEndpointId(1); b.storageGeneration = StorageEndpointGeneration(1);
    b.cpuDomainId = CpuDomainId(1); b.cpuDomainGeneration = CpuDomainGeneration(1);
    b.capability = AcceleratorClass::Cuda;
    b.cudaCompatibility = info.major * 10 + info.minor;
    b.capabilityCurrent = true;
    b.capabilityGeneration = CapabilityGeneration(1);
    c.bindings.push_back(b);
    c.capacity.generation = CapacityGeneration(1);
    c.capacity.vramTotal = ByteCount(info.totalBytes);
    c.capacity.vramAvailable = ByteCount(mode == "B"
        ? (info.freeBytes > (8ull << 30) ? (8ull << 30) : info.freeBytes)
        : info.freeBytes);
    c.capacity.hostMemoryAvailable = 32_GiB;
    c.capacity.pinnedMemoryAvailable = 1_GiB;
    c.capacity.cpuAvailable = CoreCount(16);
    c.capacity.storageAvailable = 64_GiB;
    c.capacity.nicBandwidthAvailable = 4_GiB;
    c.capacity.contiguousMemory = true;
    c.capacity.provenance = Provenance::Measured;
    c.health.generation = HealthGeneration(1); c.health.ready = true; c.health.healthScore = 1.0;
    c.health.provenance = Provenance::Measured;
    c.reservation.generation = ReservationGeneration(1); c.reservation.provenance = Provenance::Reported;
    c.congestion.generation = CongestionGeneration(1); c.congestion.congestion = 0.0; c.congestion.residualBandwidth = 1.0;
    c.congestion.provenance = Provenance::Measured;
    c.locality.topologyGeneration = TopologyGeneration(1);
    c.locality.numaDistance = (mode == "B") ? 2 : 0;
    c.locality.pcieHops = (mode == "B") ? 3 : 1;
    c.locality.provenance = Provenance::Derived;
    c.communication.planGeneration = CommunicationPlanGeneration(1); c.communication.provenance = Provenance::Derived;
    c.residency.residencyGeneration = ResidencyGeneration(1);
    c.residency.stateLocal = (mode == "B") ? false : true;
    c.residency.modelResident = true;
    c.residency.moveBytes = (mode == "B") ? 512_MiB : ByteCount(0);
    c.residency.provenance = Provenance::Reported;
    c.policyGeneration = PolicyGeneration(1);
    { ByteWriter w; encode_candidate_record(w, c);
      coord->send_frame(MessageType::PUBLISH_RESOURCE, w.take()); }
    auto pubAck = coord->recv_frame();
    if (pubAck.first == MessageType::ERROR) { std::printf("ERROR publish\n"); return 6; }
    std::printf("PUBLISHED %s\n", name.c_str());

    // Connect to the driver control channel and wait for an EXECUTE directive.
    auto ctrl = TcpChannel::connect("127.0.0.1", ctrlPort);
    if (!ctrl) { std::printf("ERROR connect-ctrl\n"); return 7; }
    { ByteWriter w; w.put_u64(boot); w.put_string(name);
      ctrl->send_frame(MessageType::REGISTER, w.take()); }

    while (true) {
        std::pair<MessageType, std::vector<std::byte>> frame;
        try { frame = ctrl->recv_frame(); } catch (...) { break; }
        if (frame.first == MessageType::EXECUTE) {
            ByteReader r(frame.second);
            const std::uint64_t planId = r.get_u64();
            const std::uint64_t authority = r.get_u64();
            const std::uint64_t allocBytes = r.get_u64();
            unsigned long long before = 0; fs_cuda_memfree(0, &before);
            double parity = 0.0;
            const int er = fs_cuda_exec(0, allocBytes, &parity);
            unsigned long long after = 0; fs_cuda_memfree(0, &after);
            ByteWriter w;
            w.put_u64(planId); w.put_u64(authority);
            w.put_double(parity);
            w.put_u64(before); w.put_u64(after);
            w.put_u8(static_cast<std::uint8_t>(er == 0 ? 1 : 0));
            ctrl->send_frame(MessageType::EXECUTION_RESULT, w.take());
        } else if (frame.first == MessageType::SHUTDOWN) { break; }
    }
    return 0;
}