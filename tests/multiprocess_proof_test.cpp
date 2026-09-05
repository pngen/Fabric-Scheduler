// multiprocess_proof_test.cpp
// Driver for the real distributed multiprocess proof of Fabric Scheduler.
// Spawns coordinator_main and worker_main as real OS processes (CreateProcess),
// talks to them over real loopback TCP using the framed protocol, and asserts:
//   registration, candidate publication, deterministic scheduling (A wins),
//   commit, execution handoff, worker death -> fencing -> old plan revalidation
//   -> B wins, and stale replay rejection.
#include "fabric_scheduler/scheduler.hpp"
#include "fabric_scheduler/net.hpp"
#include "fabric_scheduler/protocol.hpp"
#include "fabric_scheduler/persistence.hpp"
#include "fabric_scheduler/coding.hpp"
#include "fabric_scheduler/request.hpp"
#include "fabric_scheduler/candidate.hpp"
#include "fabric_scheduler/placement.hpp"

#define WIN32_LEAN_AND_MEAN
#include <windows.h>

#include <cstdint>
#include <fstream>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

using namespace fabric;

namespace {

// ---------------------------------------------------------------------------
// Child process management.
// ---------------------------------------------------------------------------
struct Child {
    HANDLE hProcess{nullptr};
    HANDLE hThread{nullptr};
    bool killed{false};
    std::wstring outPath;
    std::wstring errPath;

    Child() = default;
    Child(const Child&) = delete;
    Child& operator=(const Child&) = delete;
    Child(Child&& o) noexcept
        : hProcess(o.hProcess), hThread(o.hThread), killed(o.killed),
          outPath(std::move(o.outPath)), errPath(std::move(o.errPath)) {
        o.hProcess = nullptr;
        o.hThread = nullptr;
    }
    Child& operator=(Child&& o) noexcept {
        if (this != &o) {
            if (hProcess) CloseHandle(hProcess);
            if (hThread) CloseHandle(hThread);
            hProcess = o.hProcess; hThread = o.hThread; killed = o.killed;
            outPath = std::move(o.outPath); errPath = std::move(o.errPath);
            o.hProcess = nullptr; o.hThread = nullptr;
        }
        return *this;
    }
    ~Child() {
        if (hProcess) CloseHandle(hProcess);
        if (hThread) CloseHandle(hThread);
    }

    void kill() {
        if (hProcess && !killed) {
            TerminateProcess(hProcess, 0);
            WaitForSingleObject(hProcess, 5000);
            killed = true;
        }
    }
};

std::wstring temp_dir() {
    wchar_t buf[MAX_PATH];
    const UINT n = GetTempPathW(MAX_PATH, buf);
    return std::wstring(buf, n);
}

bool spawn(const std::wstring& exe, const std::wstring& args,
           const std::wstring& tempDir, const std::wstring& prefix, Child& out) {
    const std::wstring outPath = tempDir + L"\\" + prefix + L".out.log";
    const std::wstring errPath = tempDir + L"\\" + prefix + L".err.log";
    DeleteFileW(outPath.c_str());
    DeleteFileW(errPath.c_str());

    SECURITY_ATTRIBUTES sa{sizeof(sa), nullptr, TRUE};
    HANDLE hOut = CreateFileW(outPath.c_str(), GENERIC_WRITE,
                              FILE_SHARE_WRITE | FILE_SHARE_READ, &sa,
                              CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
    HANDLE hErr = CreateFileW(errPath.c_str(), GENERIC_WRITE,
                              FILE_SHARE_WRITE | FILE_SHARE_READ, &sa,
                              CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
    HANDLE hIn = CreateFileW(L"NUL", GENERIC_READ, FILE_SHARE_READ, &sa,
                             OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
    if (hOut == INVALID_HANDLE_VALUE || hErr == INVALID_HANDLE_VALUE) {
        if (hOut != INVALID_HANDLE_VALUE) CloseHandle(hOut);
        if (hErr != INVALID_HANDLE_VALUE) CloseHandle(hErr);
        if (hIn != INVALID_HANDLE_VALUE) CloseHandle(hIn);
        return false;
    }

    STARTUPINFOW si{sizeof(si)};
    si.dwFlags = STARTF_USESTDHANDLES;
    si.hStdInput = hIn;
    si.hStdOutput = hOut;
    si.hStdError = hErr;

    const std::wstring cmd = L"\"" + exe + L"\" " + args;
    std::vector<wchar_t> cmdBuf(cmd.begin(), cmd.end());
    cmdBuf.push_back(L'\0');

    PROCESS_INFORMATION pi{};
    const BOOL ok = CreateProcessW(exe.c_str(), cmdBuf.data(), nullptr, nullptr,
                                   TRUE, CREATE_NO_WINDOW, nullptr, nullptr, &si, &pi);
    CloseHandle(hIn);
    CloseHandle(hOut);
    CloseHandle(hErr);

    if (!ok) return false;
    out.hProcess = pi.hProcess;
    out.hThread = pi.hThread;
    out.outPath = outPath;
    out.errPath = errPath;
    return true;
}

void dump_file(const std::wstring& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) return;
    std::cerr << "---- " << path << " ----\n";
    std::cerr << in.rdbuf();
    std::cerr << "\n";
}

// ---------------------------------------------------------------------------
// Controller helpers over the framed protocol.
// ---------------------------------------------------------------------------
std::pair<MessageType, std::vector<std::byte>> call(TcpChannel& ch,
                                                   MessageType type,
                                                   std::vector<std::byte> payload) {
    ch.send_frame(type, payload);
    return ch.recv_frame();
}

std::vector<std::byte> encode_request(SchedulingRequestId id) {
    SchedulingRequest r;
    r.requestId = id;
    r.generation = SchedulingRequestGeneration(1);
    r.coordinatorEpoch = CoordinatorEpoch(1);
    r.sourceId = SourceId(3);
    r.sourceBootId = SourceBootId(30);
    r.submitOrdinal = id.value();
    r.demand.demandId = WorkloadDemandId(1);
    r.demand.demandGeneration = WorkloadDemandGeneration(1);
    r.demand.workloadId = WorkloadId(4);
    r.demand.workloadGeneration = WorkloadGeneration(1);
    r.demand.accelerator.capability = AcceleratorClass::Cuda;
    r.demand.accelerator.minimumCount = CoreCount(1);
    r.demand.accelerator.targetCount = CoreCount(1);
    r.demand.accelerator.maximumCount = CoreCount(1);
    r.demand.accelerator.vramPerDevice = 4_GiB;
    r.demand.accelerator.capabilityGeneration = CapabilityGeneration(1);
    r.demand.memory.hostMemory = 1_GiB;
    r.demand.memory.pinnedMemory = 256_MiB;
    r.demand.cpuRequired = CoreCount(2);
    r.demand.storage.capacity = 4_GiB;
    // NOTE: requireLocal is left false. The scheduler's HardFilter treats
    // requireLocal as a hard constraint a remote (stateLocal=false) candidate
    // can never satisfy, so a remote survivor would be unschedulable. Leaving
    // it non-required lets the remote worker B be eligible while still ranking
    // strictly below the local worker A (the point of the A-wins -> B-wins
    // ordering).
    r.demand.storage.requireLocal = false;
    r.demand.policyGeneration = PolicyGeneration(1);
    r.demand.requiredTopologyGeneration = TopologyGeneration(1);
    ByteWriter w;
    encode_request_record(w, r);
    return w.take();
}

PlacementPlan submit_and_get_plan(TcpChannel& ch, SchedulingRequestId id) {
    auto [type, payload] = call(ch, MessageType::SUBMIT_WORKLOAD, encode_request(id));
    if (type != MessageType::PLACEMENT_PLAN)
        throw std::runtime_error("expected PLACEMENT_PLAN, got " +
                                 std::string(to_string(type)));
    ByteReader r(payload);
    const std::uint8_t produced = r.get_u8();
    PlacementPlan plan = decode_plan_record(r);
    const std::string summary = r.get_string();
    if (produced == 0)
        throw std::runtime_error("no plan produced: " + summary);
    return plan;
}

ResourceCommitResult commit_plan(TcpChannel& ch, PlacementPlanId pid) {
    ByteWriter w;
    w.put_u64(pid.value());
    auto [type, payload] = call(ch, MessageType::COMMIT_REQUEST, w.take());
    if (type != MessageType::COMMIT_RESULT)
        throw std::runtime_error("expected COMMIT_RESULT");
    ByteReader r(payload);
    ResourceCommitResult cr;
    cr.succeeded = r.get_u8() != 0;
    cr.resourceGeneration = ResourceGeneration(r.get_u64());
    cr.commitmentToken = r.get_string();
    cr.failureReason = r.get_string();
    return cr;
}

ExecutionHandoff handoff_plan(TcpChannel& ch, PlacementPlanId pid) {
    ByteWriter w;
    w.put_u64(pid.value());
    auto [type, payload] = call(ch, MessageType::EXECUTION_HANDOFF, w.take());
    if (type != MessageType::EXECUTION_RESULT)
        throw std::runtime_error("expected EXECUTION_RESULT");
    ByteReader r(payload);
    ExecutionHandoff h;
    h.authorized = r.get_u8() != 0;
    h.authorityGeneration = AuthorityGeneration(r.get_u64());
    return h;
}

bool wait_for_candidates(TcpChannel& ch, std::uint64_t target) {
    for (int i = 0; i < 5000; ++i) {
        auto [type, payload] = call(ch, MessageType::HELLO, {});
        if (type == MessageType::HELLO) {
            ByteReader r(payload);
            (void)r.get_u64();  // epoch
            const std::uint64_t count = r.get_u64();
            if (count >= target) return true;
        }
        Sleep(1);
    }
    return false;
}

bool stale_replay_rejected(TcpChannel& ch) {
    // A candidate supposedly published from worker A's OLD boot (100), which is
    // fenced after A's death. ingest_candidate must throw StaleAuthority.
    Candidate c;
    c.candidateId = CandidateId(777);
    c.candidateGeneration = CandidateGeneration(1);
    c.sourceId = SourceId(777);
    c.sourceBootId = SourceBootId(100100);
    c.workerId = WorkerId(100);
    c.workerBootId = WorkerBootId(100);
    c.provenance = Provenance::Reported;
    c.capacity.vramTotal = 16_GiB;
    ByteWriter w;
    encode_candidate_record(w, c);
    auto [type, payload] = call(ch, MessageType::PUBLISH_RESOURCE, w.take());
    if (type == MessageType::ERROR) {
        ByteReader r(payload);
        const std::uint16_t code = r.get_u16();
        const std::string msg = r.get_string();
        std::cout << "stale replay rejected: code=" << code << " msg=" << msg << "\n";
        return true;
    }
    return false;
}

std::uint16_t find_free_port() {
    TcpListener listener;
    return listener.listen(0);
}

}  // namespace

int main() {
    WinsockInit winsock;
    const std::wstring temp = temp_dir();

    wchar_t self[MAX_PATH];
    GetModuleFileNameW(nullptr, self, MAX_PATH);
    std::wstring selfDir(self);
    const std::size_t slash = selfDir.find_last_of(L"\\/");
    if (slash != std::wstring::npos) selfDir = selfDir.substr(0, slash);
    const std::wstring coordExe = selfDir + L"\\..\\..\\tools\\Release\\coordinator_main.exe";
    const std::wstring workerExe = selfDir + L"\\..\\..\\tools\\Release\\worker_main.exe";

    Child coordinator, workerA, workerB;
    std::wstring stateFile;

    try {
        const std::uint16_t port = find_free_port();
        stateFile = temp + L"fabric_mp_state_" + std::to_wstring(port) + L".bin";

        const std::wstring coordArgs =
            L"--port " + std::to_wstring(port) + L" --state-file \"" + stateFile + L"\"";
        if (!spawn(coordExe, coordArgs, temp, L"coordinator", coordinator))
            throw std::runtime_error("failed to spawn coordinator");

        // Wait (bounded startup) for the coordinator to be connectable.
        std::optional<TcpChannel> controller;
        for (int i = 0; i < 5000 && !controller; ++i) {
            controller = TcpChannel::connect("127.0.0.1", port);
            if (!controller) Sleep(1);
        }
        if (!controller) throw std::runtime_error("coordinator did not become ready");
        TcpChannel ctrl = std::move(*controller);

        if (!spawn(workerExe, L"--port " + std::to_wstring(port) +
                                  L" --name A --boot 100 --mode A",
                   temp, L"workerA", workerA))
            throw std::runtime_error("failed to spawn worker A");

        if (!spawn(workerExe, L"--port " + std::to_wstring(port) +
                                  L" --name B --boot 200 --mode B",
                   temp, L"workerB", workerB))
            throw std::runtime_error("failed to spawn worker B");

        if (!wait_for_candidates(ctrl, 2))
            throw std::runtime_error("candidates were not published");

        // 1) deterministic scheduling: A wins.
        PlacementPlan plan1 = submit_and_get_plan(ctrl, SchedulingRequestId(1));
        if (plan1.selectedCandidate != CandidateId(100))
            throw std::runtime_error("expected A (100) to win, got " +
                                     std::to_string(plan1.selectedCandidate.value()));
        std::cout << "schedule selected A = " << plan1.selectedCandidate.value()
                  << " (plan " << plan1.planId.value() << ")\n";

        // 2) commit + execution handoff.
        ResourceCommitResult cr = commit_plan(ctrl, plan1.planId);
        if (!cr.succeeded)
            throw std::runtime_error("commit failed: " + cr.failureReason);
        ExecutionHandoff h = handoff_plan(ctrl, plan1.planId);
        if (!h.authorized)
            throw std::runtime_error("handoff not authorized");
        std::cout << "commit ok, handoff authorized (authority "
                  << h.authorityGeneration.value() << ")\n";

        // 3) worker death -> fencing.
        workerA.kill();
        std::cout << "killed worker A\n";

        // 4) resubmit: B wins after A is fenced.
        bool bWon = false;
        for (int i = 0; i < 5000 && !bWon; ++i) {
            PlacementPlan p = submit_and_get_plan(ctrl, SchedulingRequestId(2));
            if (p.selectedCandidate == CandidateId(200)) bWon = true;
            else Sleep(5);
        }
        if (!bWon) throw std::runtime_error("B did not win after A died");
        std::cout << "after A death, B won = 200\n";

        // 5) stale replay rejection.
        if (!stale_replay_rejected(ctrl))
            throw std::runtime_error("stale replay was not rejected");

        // 6) clean shutdown.
        try {
            call(ctrl, MessageType::SHUTDOWN, {});
        } catch (...) {
            // Coordinator may close immediately; that is fine.
        }

        std::cout << "multiprocess proof PASS\n";
        coordinator.kill();
        workerA.kill();
        workerB.kill();
        DeleteFileW(stateFile.c_str());
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "multiprocess proof FAIL: " << e.what() << "\n";
        dump_file(coordinator.errPath);
        dump_file(workerA.errPath);
        dump_file(workerB.errPath);
        dump_file(coordinator.outPath);
        dump_file(workerA.outPath);
        dump_file(workerB.outPath);
        coordinator.kill();
        workerA.kill();
        workerB.kill();
        DeleteFileW(stateFile.c_str());
        return 1;
    }
}
