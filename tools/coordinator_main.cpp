// coordinator_main.cpp
// Reference distributed coordinator for the Fabric Scheduler multiprocess proof.
// Runs as a real OS process, serves framed loopback TCP, owns a Scheduler, a
// thread-safe in-memory ResourceBroker, a current-authority ExecutionFabric, and
// a policy veto that refuses to schedule onto workers whose connection dropped.
#include "fabric_scheduler/scheduler.hpp"
#include "fabric_scheduler/net.hpp"
#include "fabric_scheduler/protocol.hpp"
#include "fabric_scheduler/persistence.hpp"
#include "fabric_scheduler/coding.hpp"
#include "fabric_scheduler/adapters/resource_broker.hpp"
#include "fabric_scheduler/adapters/execution.hpp"
#include "fabric_scheduler/adapters/dependency.hpp"
#include "fabric_scheduler/policy.hpp"

#include <atomic>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

using namespace fabric;

namespace {

// ---------------------------------------------------------------------------
// Shared coordinator runtime state, safe to touch from the accept loop and
// every connection-serve thread.
// ---------------------------------------------------------------------------
struct LiveState {
    std::mutex mu;
    std::unordered_set<WorkerBootId> fenced;
};

class InMemoryBroker final : public ResourceBroker {
public:
    std::mutex mu;
    std::atomic<bool> failNext{false};
    std::uint64_t nextToken{0};

    ResourceCommitResult commit(const std::vector<ResourceClaim>& claims) override {
        std::lock_guard<std::mutex> g(mu);
        ResourceCommitResult r;
        r.resourceGeneration = claims.empty() ? ResourceGeneration(1)
                                              : claims.front().resourceGeneration;
        if (failNext.exchange(false)) {
            r.succeeded = false;
            r.failureReason = "capacity changed at commit time";
            return r;
        }
        r.succeeded = true;
        r.commitmentToken = "tok-" + std::to_string(++nextToken);
        return r;
    }
    void release(const std::string&) override {}
};

// A plan is launchable only when its authority generation equals the authority
// the coordinator is currently issuing (i.e. the plan was not produced under a
// superseded epoch/authority).
class CurrentAuthorityExec final : public ExecutionFabric {
public:
    explicit CurrentAuthorityExec(Scheduler& s) : sched_(s) {}
    ExecutionHandoff authorize(const PlacementPlan& plan) override {
        ExecutionHandoff h;
        h.authorityGeneration = plan.authorityGeneration;
        h.authorized = (plan.authorityGeneration == sched_.current_authority());
        return h;
    }

private:
    Scheduler& sched_;
};

// Policy veto: a candidate published by a fenced (dropped) worker boot is never
// scheduled. This is how a dead worker loses the next scheduling pass even
// though its previously-published candidate is still held by the scheduler.
class LiveVeto final : public PolicyVeto {
public:
    explicit LiveVeto(LiveState* live) : live_(live) {}
    bool allows(const Candidate& c) const override {
        std::lock_guard<std::mutex> g(live_->mu);
        return live_->fenced.count(c.workerBootId) == 0;
    }
    std::string reason(const Candidate& c) const override {
        std::lock_guard<std::mutex> g(live_->mu);
        return live_->fenced.count(c.workerBootId) ? "worker boot is fenced"
                                                   : std::string();
    }

private:
    LiveState* live_;
};

void send_error(TcpChannel& ch, ErrorCode code, const std::string& msg) {
    try {
        ByteWriter w;
        w.put_u16(static_cast<std::uint16_t>(code));
        w.put_string(msg);
        ch.send_frame(MessageType::ERROR, w.take());
    } catch (...) {
        // The peer may have gone away; nothing to do.
    }
}

struct Ctx {
    Scheduler* sched;
    InMemoryBroker* broker;
    CurrentAuthorityExec* exec;
    LiveState* live;
    std::atomic<bool>* running;
    std::string stateFile;
    std::mutex workerMu;
    std::unordered_map<std::string, WorkerBootId> workers;
};

// Fence a dropped worker and advance the coordinator epoch monotonically so any
// plan it touched becomes revalidation-required.
void fence_worker_and_bump(Ctx& ctx, WorkerBootId boot) {
    try {
        ctx.sched->fence_worker(boot);
    } catch (...) {
    }
    {
        std::lock_guard<std::mutex> g(ctx.live->mu);
        ctx.live->fenced.insert(boot);
    }
    try {
        const CoordinatorEpoch cur = ctx.sched->epoch();
        ctx.sched->set_epoch(CoordinatorEpoch(cur.value() + 1));
    } catch (...) {
    }
}

void serve_connection(Ctx& ctx, TcpChannel ch) {
    WorkerBootId boot(0);
    bool hasBoot = false;

    while (ctx.running->load()) {
        std::pair<MessageType, std::vector<std::byte>> frame;
        try {
            frame = ch.recv_frame();
        } catch (...) {
            // Connection dropped (or a framing failure): conservative fencing.
            if (hasBoot) fence_worker_and_bump(ctx, boot);
            return;
        }

        try {
            ByteReader r(frame.second);
            switch (frame.first) {
                case MessageType::HELLO: {
                    ByteWriter w;
                    w.put_u64(ctx.sched->epoch().value());
                    w.put_u64(static_cast<std::uint64_t>(ctx.sched->candidate_count()));
                    ch.send_frame(MessageType::HELLO, w.take());
                    break;
                }
                case MessageType::REGISTER: {
                    const std::uint64_t workerId = r.get_u64();
                    const std::uint64_t b = r.get_u64();
                    const std::string name = r.get_string();
                    boot = WorkerBootId(b);
                    hasBoot = true;
                    {
                        std::lock_guard<std::mutex> g(ctx.workerMu);
                        ctx.workers[name] = boot;
                    }
                    (void)workerId;
                    ByteWriter w;
                    w.put_u8(1);
                    ch.send_frame(MessageType::REGISTER, w.take());
                    break;
                }
                case MessageType::PUBLISH_RESOURCE:
                case MessageType::PUBLISH_CAPACITY:
                case MessageType::PUBLISH_CONGESTION:
                case MessageType::PUBLISH_HEALTH: {
                    // Each publish message carries the whole candidate snapshot.
                    Candidate cand = decode_candidate_record(r);
                    try {
                        ctx.sched->ingest_candidate(cand);
                        ByteWriter w;
                        w.put_u8(1);
                        ch.send_frame(frame.first, w.take());
                    } catch (const FabricError& e) {
                        send_error(ch, e.code(), e.what());
                    }
                    break;
                }
                case MessageType::SUBMIT_WORKLOAD: {
                    SchedulingRequest req = decode_request_record(r);
                    AlwaysReadyDependency dep;
                    LiveVeto veto(ctx.live);
                    PlacementDecision d = ctx.sched->schedule(req, &dep, &veto);
                    ByteWriter w;
                    w.put_u8(d.producedPlan ? 1 : 0);
                    encode_plan_record(w, d.plan);
                    w.put_string(d.summary);
                    ch.send_frame(MessageType::PLACEMENT_PLAN, w.take());
                    break;
                }
                case MessageType::COMMIT_REQUEST: {
                    const std::uint64_t pid = r.get_u64();
                    ResourceCommitResult cr =
                        ctx.sched->commit(PlacementPlanId(pid), *ctx.broker);
                    ByteWriter w;
                    w.put_u8(cr.succeeded ? 1 : 0);
                    w.put_u64(cr.resourceGeneration.value());
                    w.put_string(cr.commitmentToken);
                    w.put_string(cr.failureReason);
                    ch.send_frame(MessageType::COMMIT_RESULT, w.take());
                    break;
                }
                case MessageType::EXECUTION_HANDOFF: {
                    const std::uint64_t pid = r.get_u64();
                    ExecutionHandoff h =
                        ctx.sched->handoff(PlacementPlanId(pid), *ctx.exec);
                    ByteWriter w;
                    w.put_u8(h.authorized ? 1 : 0);
                    w.put_u64(h.authorityGeneration.value());
                    ch.send_frame(MessageType::EXECUTION_RESULT, w.take());
                    break;
                }
                case MessageType::REVALIDATE: {
                    const std::uint64_t pid = r.get_u64();
                    GenerationFingerprint fp;
                    fp.topology = TopologyGeneration(r.get_u64());
                    fp.capacity = CapacityGeneration(r.get_u64());
                    fp.reservation = ReservationGeneration(r.get_u64());
                    fp.congestion = CongestionGeneration(r.get_u64());
                    fp.capability = CapabilityGeneration(r.get_u64());
                    fp.health = HealthGeneration(r.get_u64());
                    fp.residency = ResidencyGeneration(r.get_u64());
                    fp.communicationPlan = CommunicationPlanGeneration(r.get_u64());
                    fp.policy = PolicyGeneration(r.get_u64());
                    fp.resource = ResourceGeneration(r.get_u64());
                    fp.dependency = DependencyGeneration(r.get_u64());
                    fp.coordinatorEpoch = CoordinatorEpoch(r.get_u64());
                    RevalidationResult rr =
                        ctx.sched->revalidate(PlacementPlanId(pid), fp);
                    ByteWriter w;
                    w.put_u8(rr.current ? 1 : 0);
                    w.put_u32(static_cast<std::uint32_t>(rr.changes.size()));
                    for (const auto& c : rr.changes) w.put_string(c);
                    ch.send_frame(MessageType::REVALIDATE, w.take());
                    break;
                }
                case MessageType::CANCEL: {
                    const std::uint64_t req = r.get_u64();
                    ctx.sched->cancel(SchedulingRequestId(req));
                    ByteWriter w;
                    w.put_u8(0);
                    ch.send_frame(MessageType::CANCEL, w.take());
                    break;
                }
                case MessageType::SUPERSEDE: {
                    const std::uint64_t oldReq = r.get_u64();
                    const std::uint64_t newReq = r.get_u64();
                    ctx.sched->supersede(SchedulingRequestId(oldReq),
                                         SchedulingRequestId(newReq));
                    ByteWriter w;
                    w.put_u8(0);
                    ch.send_frame(MessageType::SUPERSEDE, w.take());
                    break;
                }
                case MessageType::SAVE: {
                    std::ofstream ofs(ctx.stateFile, std::ios::binary);
                    if (!ofs) {
                        send_error(ch, ErrorCode::Internal, "cannot open state file");
                        break;
                    }
                    persist_save(ctx.sched->snapshot(), ofs);
                    if (!ofs) {
                        send_error(ch, ErrorCode::Internal, "state write failed");
                        break;
                    }
                    ByteWriter w;
                    w.put_u8(1);
                    ch.send_frame(MessageType::SAVE, w.take());
                    break;
                }
                case MessageType::SHUTDOWN: {
                    ch.send_frame(MessageType::SHUTDOWN, {});
                    ctx.running->store(false);
                    return;
                }
                default: {
                    send_error(ch, ErrorCode::ProtocolError, "unhandled message type");
                    break;
                }
            }
        } catch (const FabricError& e) {
            // A decode/processing error on an open connection is reported; a
            // closed connection fences the registered worker.
            if (ch.valid()) {
                send_error(ch, e.code(), e.what());
            } else if (hasBoot) {
                fence_worker_and_bump(ctx, boot);
                return;
            }
        } catch (...) {
            if (ch.valid()) {
                send_error(ch, ErrorCode::Internal, "unexpected error");
            } else if (hasBoot) {
                fence_worker_and_bump(ctx, boot);
                return;
            }
        }
    }
}

}  // namespace

int main(int argc, char** argv) {
    std::uint16_t port = 0;
    std::string stateFile = "coordinator_state.bin";

    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        if (a == "--port" && i + 1 < argc) {
            port = static_cast<std::uint16_t>(std::stoul(argv[++i]));
        } else if (a == "--state-file" && i + 1 < argc) {
            stateFile = argv[++i];
        } else if (a == "--help") {
            std::cout << "usage: coordinator_main --port <p> --state-file <path>\n";
            return 0;
        }
    }

    WinsockInit winsock;

    Scheduler::Config cfg;
    cfg.policy.generation = PolicyGeneration(1);
    cfg.maxCandidatesPerRequest = CoreCount(64);
    Scheduler scheduler(cfg);

    InMemoryBroker broker;
    CurrentAuthorityExec exec(scheduler);
    LiveState live;
    std::atomic<bool> running{true};

    Ctx ctx;
    ctx.sched = &scheduler;
    ctx.broker = &broker;
    ctx.exec = &exec;
    ctx.live = &live;
    ctx.running = &running;
    ctx.stateFile = stateFile;

    TcpListener listener;
    std::uint16_t boundPort = listener.listen(port);
    std::cout << "PORT " << boundPort << std::endl;
    std::cout << "READY" << std::endl;
    std::cout.flush();

    std::vector<std::thread> threads;
    while (running.load()) {
        std::optional<TcpChannel> ch = listener.accept();
        if (!ch) {
            if (!running.load()) break;
            continue;
        }
        threads.emplace_back([&ctx, ch = std::move(*ch)]() mutable {
            serve_connection(ctx, std::move(ch));
        });
    }

    // Process is normally terminated externally (proof driver). Detach so a
    // standalone run can shut down without joining into blocking recv_frame.
    for (auto& t : threads) t.detach();
    return 0;
}
