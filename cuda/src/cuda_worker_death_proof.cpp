#include "fabric_scheduler/scheduler.hpp"
#include "fabric_scheduler/net.hpp"
#include "fabric_scheduler/protocol.hpp"
#include "fabric_scheduler/persistence.hpp"
#include "fabric_scheduler/coding.hpp"
#include "fabric_scheduler/adapters/dependency.hpp"
#include "fabric_scheduler/adapters/resource_broker.hpp"
#include "fabric_scheduler/adapters/execution.hpp"

#include <windows.h>
#undef ERROR
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iterator>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

using namespace fabric;
using Clock = std::chrono::steady_clock;

namespace {

struct Child {
    HANDLE hProcess = nullptr;
    HANDLE hThread = nullptr;
    DWORD pid = 0;
    std::wstring outPath;
    std::wstring errPath;
};

bool spawn(const std::wstring& exe, const std::wstring& args,
           const std::wstring& tempdir, const std::wstring& prefix, Child& out) {
    const std::wstring outPath = tempdir + L"\\" + prefix + L".out.log";
    const std::wstring errPath = tempdir + L"\\" + prefix + L".err.log";
    DeleteFileW(outPath.c_str()); DeleteFileW(errPath.c_str());
    SECURITY_ATTRIBUTES sa{sizeof(sa), nullptr, TRUE};
    HANDLE hOut = CreateFileW(outPath.c_str(), GENERIC_WRITE, FILE_SHARE_WRITE | FILE_SHARE_READ, &sa, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
    HANDLE hErr = CreateFileW(errPath.c_str(), GENERIC_WRITE, FILE_SHARE_WRITE | FILE_SHARE_READ, &sa, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
    HANDLE hIn = CreateFileW(L"NUL", GENERIC_READ, FILE_SHARE_READ, &sa, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
    if (hOut == INVALID_HANDLE_VALUE || hErr == INVALID_HANDLE_VALUE || hIn == INVALID_HANDLE_VALUE) {
        if (hOut != INVALID_HANDLE_VALUE) CloseHandle(hOut);
        if (hErr != INVALID_HANDLE_VALUE) CloseHandle(hErr);
        if (hIn != INVALID_HANDLE_VALUE) CloseHandle(hIn);
        return false;
    }
    STARTUPINFOW si{sizeof(si)};
    si.dwFlags = STARTF_USESTDHANDLES;
    si.hStdInput = hIn; si.hStdOutput = hOut; si.hStdError = hErr;
    const std::wstring cmd = L"\"" + exe + L"\" " + args;
    std::vector<wchar_t> cmdBuf(cmd.begin(), cmd.end()); cmdBuf.push_back(L'\0');
    PROCESS_INFORMATION pi{};
    const BOOL ok = CreateProcessW(exe.c_str(), cmdBuf.data(), nullptr, nullptr, TRUE, CREATE_NO_WINDOW, nullptr, nullptr, &si, &pi);
    CloseHandle(hIn); CloseHandle(hOut); CloseHandle(hErr);
    if (!ok) { std::printf("spawn CreateProcessW failed err=%lu exe=%ls\n", (unsigned long)GetLastError(), exe.c_str()); return false; }
    out.hProcess = pi.hProcess; out.hThread = pi.hThread;
    out.pid = GetProcessId(pi.hProcess); out.outPath = outPath; out.errPath = errPath;
    return true;
}

bool file_contains(const std::wstring& path, const std::string& needle) {
    std::ifstream in(path, std::ios::binary);
    if (!in) return false;
    std::string content((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    return content.find(needle) != std::string::npos;
}

void kill(const Child& c) { if (c.hProcess) { TerminateProcess(c.hProcess, 0); WaitForSingleObject(c.hProcess, 5000); } }
void cleanup(const Child& c) { kill(c); if (c.hThread) CloseHandle(c.hThread); if (c.hProcess) CloseHandle(c.hProcess); DeleteFileW(c.outPath.c_str()); DeleteFileW(c.errPath.c_str()); }

bool wait_for(const std::wstring& path, const std::string& needle, int maxMs) {
    const auto t0 = Clock::now();
    while (std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - t0).count() < maxMs) {
        if (file_contains(path, needle)) return true;
        Sleep(5);
    }
    return file_contains(path, needle);
}

bool exists_w(const std::wstring& p) {
    const DWORD attr = GetFileAttributesW(p.c_str());
    return attr != INVALID_FILE_ATTRIBUTES && !(attr & FILE_ATTRIBUTE_DIRECTORY);
}

std::wstring canon_w(const std::wstring& p) {
    DWORD n = GetFullPathNameW(p.c_str(), 0, nullptr, nullptr);
    std::vector<wchar_t> buf(n);
    GetFullPathNameW(p.c_str(), n, buf.data(), nullptr);
    return std::wstring(buf.data());
}

std::wstring self_dir() {
    wchar_t self[MAX_PATH]; GetModuleFileNameW(nullptr, self, MAX_PATH);
    std::wstring d(self); const std::size_t s = d.find_last_of(L"\\/");
    if (s != std::wstring::npos) d = d.substr(0, s);
    return d;
}

std::wstring search_up_exe(std::wstring dir, const std::wstring& sub, const std::wstring& name, const std::wstring& cfg) {
    for (int i = 0; i < 4; ++i) {
        const std::wstring a = canon_w(dir + L"\\" + sub + L"\\" + name);
        if (exists_w(a)) return a;
        const std::wstring b = canon_w(dir + L"\\" + sub + L"\\" + cfg + L"\\" + name);
        if (exists_w(b)) return b;
        const std::size_t s = dir.find_last_of(L"\\/");
        if (s == std::wstring::npos || s < 3) break;
        dir = dir.substr(0, s);
    }
    return std::wstring();
}

std::wstring module_dir() {
    const std::wstring d = self_dir();
    const std::size_t cs = d.find_last_of(L"\\/");
    const std::wstring cfg = (cs != std::wstring::npos) ? d.substr(cs + 1) : L"Release";
    return search_up_exe(d, L"tools", L"coordinator_main.exe", cfg);
}

std::wstring worker_path() {
    const std::wstring d = self_dir();
    const std::size_t cs = d.find_last_of(L"\\/");
    const std::wstring cfg = (cs != std::wstring::npos) ? d.substr(cs + 1) : L"Release";
    return search_up_exe(d, L"cuda", L"cuda_worker.exe", cfg);
}

SchedulingRequest make_request(SchedulingRequestId r, WorkloadDemandId d) {
    SchedulingRequest q;
    q.requestId = r; q.generation = SchedulingRequestGeneration(1);
    q.coordinatorEpoch = CoordinatorEpoch(1);
    q.sourceId = SourceId(3); q.sourceBootId = SourceBootId(30);
    q.demand.demandId = d; q.demand.demandGeneration = WorkloadDemandGeneration(1);
    q.demand.workloadId = WorkloadId(4); q.demand.workloadGeneration = WorkloadGeneration(1);
    q.demand.accelerator.capability = AcceleratorClass::Cuda;
    q.demand.accelerator.minimumCount = CoreCount(1); q.demand.accelerator.targetCount = CoreCount(1);
    q.demand.accelerator.maximumCount = CoreCount(1); q.demand.accelerator.vramPerDevice = 512_MiB;
    q.demand.accelerator.capabilityGeneration = CapabilityGeneration(1);
    q.demand.memory.hostMemory = 1_GiB; q.demand.memory.pinnedMemory = 64_MiB;
    q.demand.cpuRequired = CoreCount(2); q.demand.storage.capacity = 1_GiB;
    q.demand.policyGeneration = PolicyGeneration(1); q.demand.requiredTopologyGeneration = TopologyGeneration(1);
    return q;
}

std::pair<MessageType, std::vector<std::byte>> call(TcpChannel& ch, MessageType t, std::vector<std::byte> payload) {
    ch.send_frame(t, payload);
    return ch.recv_frame();
}

}  // namespace

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);
    std::printf("CUDA worker-death integrated proof\n");
    WinsockInit winsock;

    wchar_t tmpPath[MAX_PATH]; GetTempPathW(MAX_PATH, tmpPath);
    const std::wstring tempdir = std::wstring(tmpPath) + L"fab_cwd_" + std::to_wstring(GetCurrentProcessId());
    CreateDirectoryW(tempdir.c_str(), nullptr);
    const std::wstring stateFile = tempdir + L"\\state.bin";
    const std::wstring coordExe = module_dir();
    const std::wstring workerExe = worker_path();
    if (coordExe.empty() || workerExe.empty()) { std::printf("FAILED: exe path not found\n"); return 2; }

    struct CtrlState {
        TcpListener ctrl;
        std::mutex mu;
        std::unordered_map<std::uint64_t, std::shared_ptr<TcpChannel>> map;
        std::atomic<bool> running{true};
    };
    auto cs = std::make_shared<CtrlState>();
    const std::uint16_t ctrlPort = cs->ctrl.listen(0);
    std::thread ctrlThread([cs] {
        while (cs->running.load()) {
            auto ch = cs->ctrl.accept();
            if (!ch) continue;
            try {
                auto fr = ch->recv_frame();
                if (fr.first == MessageType::REGISTER) {
                    ByteReader r(fr.second);
                    const std::uint64_t boot = r.get_u64();
                    (void)r.get_string();
                    std::lock_guard<std::mutex> g(cs->mu);
                    cs->map[boot] = std::make_shared<TcpChannel>(std::move(*ch));
                }
            } catch (...) { }
        }
    });
    ctrlThread.detach();

    std::printf("tempdir=%ls coordExe=%ls\n", tempdir.c_str(), coordExe.c_str());
    Child coordChild;
    if (!spawn(coordExe, L"--port 0 --state-file " + stateFile, tempdir, L"coord", coordChild)) { std::printf("FAILED: spawn coordinator\n"); return 3; }
    if (!wait_for(coordChild.outPath, "READY", 10000)) { std::printf("FAILED: coordinator not ready\n"); return 4; }
    std::uint16_t coordPort = 0;
    { std::ifstream in(coordChild.outPath, std::ios::binary); std::string c((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
      const std::size_t p = c.find("PORT "); if (p != std::string::npos) coordPort = static_cast<std::uint16_t>(std::stoul(c.substr(p+5))); }
    if (coordPort == 0) { std::printf("FAILED: coord port\n"); return 5; }
    std::printf("coordinator port = %u\n", coordPort);

    auto controller = TcpChannel::connect("127.0.0.1", coordPort);
    if (!controller) { std::printf("FAILED: controller connect\n"); return 6; }

    Child workerA, workerB;
    const std::wstring wAargs = L"--coord-port " + std::to_wstring(coordPort) + L" --ctrl-port " + std::to_wstring(ctrlPort) + L" --boot 100 --name cudaA --mode A";
    const std::wstring wBargs = L"--coord-port " + std::to_wstring(coordPort) + L" --ctrl-port " + std::to_wstring(ctrlPort) + L" --boot 200 --name cudaB --mode B";
    if (!spawn(workerExe, wAargs, tempdir, L"wA", workerA)) { std::printf("FAILED: spawn A\n"); return 7; }
    if (!spawn(workerExe, wBargs, tempdir, L"wB", workerB)) { std::printf("FAILED: spawn B\n"); return 8; }
    if (!wait_for(workerA.outPath, "PUBLISHED cudaA", 10000)) { std::printf("FAILED: A publish\n"); return 9; }
    if (!wait_for(workerB.outPath, "PUBLISHED cudaB", 10000)) { std::printf("FAILED: B publish\n"); return 10; }
    { auto t0 = Clock::now(); bool got=false;
      while (std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now()-t0).count() < 10000) {
        std::lock_guard<std::mutex> g(cs->mu); if (cs->map.count(100) && cs->map.count(200)) { got=true; break; } Sleep(5); }
      if (!got) { std::printf("FAILED: control channels\n"); return 11; } }

    PlacementPlan p1; std::uint64_t auth1 = 0;
    { ByteWriter w; encode_request_record(w, make_request(SchedulingRequestId(1), WorkloadDemandId(1)));
      auto r1 = call(*controller, MessageType::SUBMIT_WORKLOAD, w.take());
      if (r1.first != MessageType::PLACEMENT_PLAN) { std::printf("FAILED: submit1\n"); return 12; }
      ByteReader br(r1.second); (void)br.get_u8(); p1 = decode_plan_record(br);
      if (p1.selectedCandidate != CandidateId(100)) { std::printf("FAILED: A not selected\n"); return 13; }
      ByteWriter cw; cw.put_u64(p1.planId.value()); auto cr = call(*controller, MessageType::COMMIT_REQUEST, cw.take());
      ByteReader cb(cr.second); if (!cb.get_u8()) { std::printf("FAILED: commit1\n"); return 14; }
      ByteWriter hw; hw.put_u64(p1.planId.value()); auto hr = call(*controller, MessageType::EXECUTION_HANDOFF, hw.take());
      ByteReader hb(hr.second); if (!hb.get_u8()) { std::printf("FAILED: handoff1\n"); return 15; }
      auth1 = hb.get_u64();
      std::printf("phase1 A selected, commit ok, handoff authorized\n");
      std::shared_ptr<TcpChannel> c100; { std::lock_guard<std::mutex> g(cs->mu); c100 = cs->map[100]; }
      ByteWriter ew; ew.put_u64(p1.planId.value()); ew.put_u64(auth1); ew.put_u64(256ull<<20);
      c100->send_frame(MessageType::EXECUTE, ew.take());
      auto er = c100->recv_frame();
      ByteReader eb(er.second); (void)eb.get_u64(); (void)eb.get_u64();
      const double parity = eb.get_double(); const std::uint64_t before = eb.get_u64(); const std::uint64_t after = eb.get_u64(); const std::uint8_t success = eb.get_u8();
      if (success != 1 || parity != 1.0) { std::printf("FAILED: A parity %f\n", parity); return 16; }
      if (after + (4ull<<20) < before) { std::printf("FAILED: A mem leak\n"); return 17; }
      std::printf("phase1 A real CUDA parity=1.0 baseline %llu->%llu\n", before, after);
    }

    kill(workerA);
    std::printf("phase2 killed worker A (pid %lu)\n", (unsigned long)workerA.pid);
    bool bWon = false;
    { auto t0 = Clock::now();
      while (std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now()-t0).count() < 15000) {
        ByteWriter w; encode_request_record(w, make_request(SchedulingRequestId(2), WorkloadDemandId(2)));
        auto r = call(*controller, MessageType::SUBMIT_WORKLOAD, w.take());
        if (r.first == MessageType::PLACEMENT_PLAN) {
          ByteReader br(r.second); (void)br.get_u8(); PlacementPlan pl = decode_plan_record(br);
          if (pl.selectedCandidate == CandidateId(200)) { bWon = true; std::printf("phase2 after death B won = 200\n"); break; }
        }
        Sleep(8);
      }
    }
    if (!bWon) { std::printf("FAILED: B did not win after A death\n"); return 18; }

    { Candidate stale; stale.candidateId = CandidateId(100); stale.candidateGeneration = CandidateGeneration(2);
      stale.workerId = WorkerId(100); stale.workerBootId = WorkerBootId(100);
      stale.sourceId = SourceId(77000); stale.sourceBootId = SourceBootId(79000);
      stale.provenance = Provenance::Measured;
      DeviceBinding b; b.deviceId = DeviceId(1); b.deviceGeneration = DeviceGeneration(1);
      b.resourceId = ResourceId(1); b.resourceGeneration = ResourceGeneration(1);
      b.nodeId = NodeId(1); b.nodeGeneration = NodeGeneration(1);
      b.memoryDomainId = MemoryDomainId(1); b.memoryDomainGeneration = MemoryDomainGeneration(1);
      b.nicId = NicId(1); b.nicGeneration = NicGeneration(1); b.storageId = StorageEndpointId(1); b.storageGeneration = StorageEndpointGeneration(1);
      b.cpuDomainId = CpuDomainId(1); b.cpuDomainGeneration = CpuDomainGeneration(1);
      b.capability = AcceleratorClass::Cuda; b.cudaCompatibility = 120; b.capabilityCurrent = true; b.capabilityGeneration = CapabilityGeneration(1);
      stale.bindings.push_back(b);
      stale.capacity.generation = CapacityGeneration(1); stale.capacity.vramAvailable = 6_GiB; stale.capacity.contiguousMemory = true;
      stale.capacity.provenance = Provenance::Measured;
      stale.health.generation = HealthGeneration(1); stale.health.ready = true; stale.health.provenance = Provenance::Measured;
      stale.congestion.generation = CongestionGeneration(1); stale.congestion.provenance = Provenance::Measured;
      stale.locality.topologyGeneration = TopologyGeneration(1); stale.locality.provenance = Provenance::Reported;
      stale.communication.planGeneration = CommunicationPlanGeneration(1); stale.communication.provenance = Provenance::Derived;
      stale.residency.residencyGeneration = ResidencyGeneration(1); stale.residency.provenance = Provenance::Reported;
      stale.policyGeneration = PolicyGeneration(1);
      ByteWriter w; encode_candidate_record(w, stale);
      auto r = call(*controller, MessageType::PUBLISH_RESOURCE, w.take());
      if (r.first != MessageType::ERROR) { std::printf("FAILED: stale replay not rejected\n"); return 19; }
      ByteReader eb(r.second); const std::uint16_t code = eb.get_u16();
      std::printf("phase3 stale worker-boot replay rejected code=%u\n", (unsigned)code);
      if (code != 13) { std::printf("FAILED: expected StaleAuthority\n"); return 20; }
    }

    { ByteWriter hw; hw.put_u64(p1.planId.value()); auto hr = call(*controller, MessageType::EXECUTION_HANDOFF, hw.take());
      if (hr.first == MessageType::EXECUTION_RESULT) { ByteReader hb(hr.second); if (hb.get_u8()) { std::printf("FAILED: stale handoff authorized\n"); return 21; } }
      std::printf("phase4 stale handoff rejected before any CUDA launch\n");
    }

    Child workerA2;
    const std::wstring wA2args = L"--coord-port " + std::to_wstring(coordPort) + L" --ctrl-port " + std::to_wstring(ctrlPort) + L" --boot 300 --name cudaA2 --mode A";
    if (!spawn(workerExe, wA2args, tempdir, L"wA2", workerA2)) { std::printf("FAILED: spawn A2\n"); return 22; }
    if (!wait_for(workerA2.outPath, "PUBLISHED cudaA2", 10000)) { std::printf("FAILED: A2 publish\n"); return 23; }
    { auto t0 = Clock::now(); bool got=false;
      while (std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now()-t0).count() < 10000) {
        std::lock_guard<std::mutex> g(cs->mu); if (cs->map.count(300)) { got=true; break; } Sleep(5); }
      if (!got) { std::printf("FAILED: A2 ctrl\n"); return 24; } }

    { ByteWriter w; encode_request_record(w, make_request(SchedulingRequestId(3), WorkloadDemandId(3)));
      auto r = call(*controller, MessageType::SUBMIT_WORKLOAD, w.take());
      if (r.first != MessageType::PLACEMENT_PLAN) { std::printf("FAILED: submit3\n"); return 25; }
      ByteReader br(r.second); (void)br.get_u8(); PlacementPlan p3 = decode_plan_record(br);
      if (p3.selectedCandidate != CandidateId(300)) { std::printf("FAILED: A2 not selected\n"); return 26; }
      std::printf("phase5 fresh scheduling selected A2 = 300 (fresh authority %llu)\n", (unsigned long long)p3.authorityGeneration.value());
      ByteWriter cw; cw.put_u64(p3.planId.value()); auto cr = call(*controller, MessageType::COMMIT_REQUEST, cw.take());
      ByteReader cb(cr.second); if (!cb.get_u8()) { std::printf("FAILED: commit3\n"); return 27; }
      ByteWriter hw; hw.put_u64(p3.planId.value()); auto hr = call(*controller, MessageType::EXECUTION_HANDOFF, hw.take());
      ByteReader hb(hr.second); const std::uint8_t auth = hb.get_u8(); const std::uint64_t aut2 = hb.get_u64();
      if (!auth) { std::printf("FAILED: handoff3\n"); return 28; }
      std::shared_ptr<TcpChannel> c300; { std::lock_guard<std::mutex> g(cs->mu); c300 = cs->map[300]; }
      ByteWriter ew; ew.put_u64(p3.planId.value()); ew.put_u64(aut2); ew.put_u64(256ull<<20);
      c300->send_frame(MessageType::EXECUTE, ew.take());
      auto er = c300->recv_frame();
      ByteReader eb(er.second); (void)eb.get_u64(); (void)eb.get_u64();
      const double parity = eb.get_double(); const std::uint64_t before = eb.get_u64(); const std::uint64_t after = eb.get_u64(); const std::uint8_t success = eb.get_u8();
      if (success != 1 || parity != 1.0) { std::printf("FAILED: A2 parity %f\n", parity); return 29; }
      if (after + (4ull<<20) < before) { std::printf("FAILED: A2 mem\n"); return 30; }
      std::printf("phase5 A2 real CUDA parity=1.0 baseline %llu->%llu\n", before, after);
    }

    cs->running.store(false);
    controller.reset();
    cleanup(workerA); cleanup(workerB); cleanup(workerA2); cleanup(coordChild);
    DeleteFileW(stateFile.c_str()); RemoveDirectoryW(tempdir.c_str());
    std::printf("INTEGRATED CUDA WORKER-DEATH SCHEDULING: PASS\n");
    return 0;
}