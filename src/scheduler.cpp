#include "fabric_scheduler/scheduler.hpp"
#include "fabric_scheduler/persistence.hpp"
#include "fabric_scheduler/revalidation.hpp"
#include "fabric_scheduler/adapters/dependency.hpp"
#include "fabric_scheduler/adapters/resource_broker.hpp"
#include "fabric_scheduler/adapters/execution.hpp"
#include <mutex>
#include <unordered_map>
#include <map>
#include <unordered_set>
#include <vector>
#include <algorithm>
#include <cmath>
#include <string>
#include <sstream>
#include <optional>

namespace fabric {

namespace {
constexpr std::uint32_t kMaxPlanHistory = 64;

void advance_local(PlacementPlan& plan, PlacementLifecycleState to, std::uint64_t ordinal) {
    if (plan.state == to) return;
    if (!placement_can_transition(plan.state, to)) {
        throw FabricError(ErrorCode::IllegalLifecycleTransition,
                          "illegal placement lifecycle transition " +
                              std::string(to_string(plan.state)) + " -> " +
                              std::string(to_string(to)));
    }
    plan.state = to;
    if (plan.stateHistory.size() < kMaxPlanHistory) plan.stateHistory.emplace_back(to, ordinal);
}

void force_state(PlacementPlan& plan, PlacementLifecycleState to, std::uint64_t ordinal) {
    if (plan.state == to) return;
    plan.state = to;
    if (plan.stateHistory.size() < kMaxPlanHistory) plan.stateHistory.emplace_back(to, ordinal);
}

std::vector<ResourceClaim> build_claims(const std::vector<DeviceBinding>& bindings) {
    std::vector<ResourceClaim> claims;
    claims.reserve(bindings.size());
    for (const auto& b : bindings) {
        ResourceClaim c;
        c.resourceId = b.resourceId;
        c.resourceGeneration = b.resourceGeneration;
        c.kind = ResourceKind::Accelerator;
        c.amount = ByteCount(1ull);
        c.provisional = true;
        claims.push_back(c);
    }
    return claims;
}

// A plan is resolved once its resources are committed. Until then a newer plan
// for the same request replaces it.
bool is_unresolved(PlacementLifecycleState s) {
    return !is_terminal(s) && s != PlacementLifecycleState::COMMITTED &&
           s != PlacementLifecycleState::AWAITING_EXECUTION &&
           s != PlacementLifecycleState::ACTIVE;
}

CandidateIdentity identity_of(const Candidate& c) {
    CandidateIdentity ci;
    ci.candidateId = c.candidateId;
    ci.candidateGeneration = c.candidateGeneration;
    ci.sourceId = c.sourceId;
    ci.sourceBootId = c.sourceBootId;
    ci.workerId = c.workerId;
    ci.workerBootId = c.workerBootId;
    return ci;
}

// The generations a plan takes from its selected candidate's evidence.
void apply_candidate_generations(GenerationFingerprint& fp, const Candidate& c) {
    fp.topology = c.locality.topologyGeneration;
    fp.capacity = c.capacity.generation;
    fp.reservation = c.reservation.generation;
    fp.congestion = c.congestion.generation;
    fp.resource = c.bindings.empty() ? ResourceGeneration(1) : c.bindings.front().resourceGeneration;
    fp.capability = c.bindings.empty() ? CapabilityGeneration(1) : c.bindings.front().capabilityGeneration;
    fp.health = c.health.generation;
    fp.residency = c.residency.residencyGeneration;
    fp.communicationPlan = c.communication.planGeneration;
}

}  // namespace

class SchedulerState {
public:
    mutable std::mutex mu;
    CoordinatorEpoch epoch{1};
    AuthorityGeneration authority{1};
    PolicyConfig policy;
    RankOptions rankOptions;
    CoreCount maxCandidatesPerRequest{256};
    std::uint64_t nextPlanId{1};
    std::uint64_t nextSubmitOrdinal{1};
    std::uint64_t nextEventOrdinal{1};
    std::uint64_t ingestWatermark{0};

    std::map<CandidateId, Candidate> candidates;
    // Identity of every candidate published or recovered. Live evidence is
    // dropped on recovery; the identities that plans refer to are kept, so a
    // recovered scheduler can still be persisted.
    std::map<CandidateId, CandidateIdentity> knownIdentities;
    std::map<SchedulingRequestId, SchedulingRequest> requests;
    std::map<PlacementPlanId, PlacementPlan> plans;
    std::map<SchedulingRequestId, PlacementPlanId> latestPlanByRequest;
    std::vector<std::string> supersessionHistory;
    std::unordered_set<SchedulingRequestId> cancelledRequests;
    std::unordered_set<WorkerBootId> fencedWorkers;
    std::unordered_set<SourceBootId> fencedSources;
    HardFilter hardFilter;
    Ranker ranker;
};

namespace {

bool publisher_fenced(const SchedulerState& st, const Candidate& c) {
    return st.fencedWorkers.count(c.workerBootId) != 0 || st.fencedSources.count(c.sourceBootId) != 0;
}

// Why the evidence a plan was built on is no longer current, or empty while it
// still is. A plan stays current only while its selected candidate is published
// by a live worker and source boot at the generations the plan recorded.
std::string stale_evidence(const SchedulerState& st, const PlacementPlan& plan) {
    const auto it = st.candidates.find(plan.selectedCandidate);
    if (it == st.candidates.end()) return "selected candidate evidence is not currently published";
    if (publisher_fenced(st, it->second)) return "selected candidate was published by a fenced worker or source boot";
    GenerationFingerprint current = plan.generations;
    apply_candidate_generations(current, it->second);
    const RevalidationResult rr = revalidate_plan(plan, current);
    if (!rr.current) return "selected candidate evidence changed: " + rr.changes.front();
    return {};
}

// A fenced boot no longer vouches for its evidence: every plan still relying on
// a candidate it published must be revalidated.
template <typename FromFencedBoot>
void invalidate_plans(SchedulerState& st, FromFencedBoot fromFencedBoot) {
    for (auto& [id, plan] : st.plans) {
        if (is_terminal(plan.state)) continue;
        const auto it = st.candidates.find(plan.selectedCandidate);
        if (it == st.candidates.end() || !fromFencedBoot(it->second)) continue;
        force_state(plan, PlacementLifecycleState::REVALIDATION_REQUIRED, st.nextEventOrdinal++);
    }
}

}  // namespace

Scheduler::Scheduler(Config config) : state_(std::make_unique<SchedulerState>()) {
    state_->policy = config.policy;
    state_->rankOptions = config.rankOptions;
    state_->maxCandidatesPerRequest = config.maxCandidatesPerRequest;
    state_->authority = config.initialAuthority;
}

Scheduler::~Scheduler() = default;

CoordinatorEpoch Scheduler::epoch() const { std::lock_guard<std::mutex> g(state_->mu); return state_->epoch; }

void Scheduler::set_epoch(CoordinatorEpoch newEpoch) {
    std::lock_guard<std::mutex> g(state_->mu);
    if (state_->epoch == newEpoch) return;
    const bool regression = newEpoch.value() <= state_->epoch.value();
    if (regression) throw FabricError(ErrorCode::StaleAuthority, "coordinator epoch must not move backward");
    state_->epoch = newEpoch;
    state_->authority = state_->authority.next();
    // Fencing: every non-terminal plan becomes REVALIDATION_REQUIRED so a
    // recovered coordinator can never blindly launch a stale plan.
    for (auto& [id, plan] : state_->plans) {
        if (!is_terminal(plan.state)) force_state(plan, PlacementLifecycleState::REVALIDATION_REQUIRED, state_->nextEventOrdinal++);
    }
}

AuthorityGeneration Scheduler::current_authority() const { std::lock_guard<std::mutex> g(state_->mu); return state_->authority; }

// --- candidate evidence -----------------------------------------------
void Scheduler::ingest_candidate(Candidate candidate) {
    if (!candidate.candidateId.is_valid()) throw FabricError(ErrorCode::InvalidArgument, "candidate id missing");
    if (!candidate.candidateGeneration.is_valid()) throw FabricError(ErrorCode::InvalidArgument, "candidate generation missing");
    if (!candidate.sourceBootId.is_valid()) throw FabricError(ErrorCode::InvalidArgument, "candidate source boot id missing");
    if (!candidate.workerBootId.is_valid()) throw FabricError(ErrorCode::InvalidArgument, "candidate worker boot id missing");
    if (candidate.bindings.empty() && candidate.capacity.vramTotal.value() == 0)
        throw FabricError(ErrorCode::InvalidArgument, "candidate carries no binding or capacity evidence");
    // A non-finite value would make the ranking order undefined and could not
    // be persisted with the plan that used it.
    if (!std::isfinite(candidate.health.healthScore) || !std::isfinite(candidate.congestion.congestion) ||
        !std::isfinite(candidate.congestion.residualBandwidth) ||
        !candidate.communication.estimatedCost.is_finite() || !candidate.communication.derivedPathCost.is_finite() ||
        !candidate.communication.collectiveCost.is_finite() || !candidate.memoryMovementCost.is_finite())
        throw FabricError(ErrorCode::NonFinite, "candidate evidence carries a non-finite value");
    std::lock_guard<std::mutex> g(state_->mu);
    if (state_->fencedWorkers.count(candidate.workerBootId))
        throw FabricError(ErrorCode::StaleAuthority, "candidate published by a fenced worker boot");
    if (state_->fencedSources.count(candidate.sourceBootId))
        throw FabricError(ErrorCode::StaleAuthority, "candidate published by a fenced source boot");
    const CandidateIdentity identity = identity_of(candidate);
    const auto it = state_->candidates.find(candidate.candidateId);
    if (it != state_->candidates.end()) {
        // A same-id candidate may refresh only at a strictly higher generation.
        if (candidate.candidateGeneration.value() <= it->second.candidateGeneration.value())
            throw FabricError(ErrorCode::InvalidArgument, "duplicate or stale candidate id (must refresh at higher generation)");
        it->second = std::move(candidate);
        state_->knownIdentities[identity.candidateId] = identity;
        ++state_->ingestWatermark;
        return;
    }
    state_->candidates.emplace(candidate.candidateId, std::move(candidate));
    state_->knownIdentities[identity.candidateId] = identity;
    ++state_->ingestWatermark;
}

std::size_t Scheduler::candidate_count() const { std::lock_guard<std::mutex> g(state_->mu); return state_->candidates.size(); }
std::size_t Scheduler::ingest_watermark() const { std::lock_guard<std::mutex> g(state_->mu); return state_->ingestWatermark; }

namespace {
GenerationFingerprint make_fingerprint(const Candidate& c, const SchedulingRequest& req,
                                     const DependencyView* dep, CoordinatorEpoch epoch) {
    GenerationFingerprint fp;
    apply_candidate_generations(fp, c);
    fp.policy = req.demand.policyGeneration.is_valid() ? req.demand.policyGeneration : PolicyGeneration(1);
    fp.dependency = dep ? dep->generation() : DependencyGeneration(1);
    fp.coordinatorEpoch = epoch;
    return fp;
}
}  // namespace

PlacementDecision Scheduler::schedule(const SchedulingRequest& request,
                                     const DependencyView* dep, const PolicyVeto* policy) {
    request.demand.validate();

    PlacementDecision decision;
    decision.request = request;

    std::vector<Candidate> snapshot;
    std::vector<char> fenced;
    {
        std::lock_guard<std::mutex> g(state_->mu);
        // Cancellation stops new plan work for the request.
        if (state_->cancelledRequests.count(request.requestId)) {
            decision.summary = "request cancelled";
            return decision;
        }
        snapshot.reserve(state_->candidates.size());
        fenced.reserve(state_->candidates.size());
        for (const auto& [id, cand] : state_->candidates) {
            snapshot.push_back(cand);
            fenced.push_back(publisher_fenced(*state_, cand) ? 1 : 0);
        }
    }

    struct Entry { RankedCandidate rank; const Candidate* candidate; };
    std::vector<Entry> eligible;
    eligible.reserve(snapshot.size());

    for (std::size_t i = 0; i < snapshot.size(); ++i) {
        const Candidate& cand = snapshot[i];
        EligibilityResult er;
        if (fenced[i]) {
            // Evidence published by a fenced boot is not current evidence.
            er.status = EligibilityStatus::REVALIDATION_REQUIRED;
            er.reason = "candidate was published by a fenced worker or source boot";
        } else {
            er = state_->hardFilter.evaluate(request, cand, dep, policy);
        }
        if (er.eligible()) {
            eligible.push_back({ state_->ranker.evaluate(request, cand, state_->rankOptions), &cand });
        } else {
            RejectedAlternative ra;
            ra.candidateId = cand.candidateId;
            ra.candidateGeneration = cand.candidateGeneration;
            ra.status = er.status;
            ra.reason = er.reason;
            decision.rejections.push_back(std::move(ra));
            decision.disqualified.push_back(cand.candidateId);
        }
    }

    std::sort(eligible.begin(), eligible.end(),
              [](const Entry& a, const Entry& b) { return RankComparator{}(a.rank, b.rank); });

    for (const auto& e : eligible) decision.eligible.push_back(e.rank.candidateId);

    if (!eligible.empty()) {
        std::lock_guard<std::mutex> g(state_->mu);
        // The request may have been cancelled, and evidence may have been
        // refreshed or fenced, while the candidates were evaluated outside the
        // lock. Only entries that are still current may be planned.
        if (state_->cancelledRequests.count(request.requestId)) {
            decision.summary = "request cancelled";
            return decision;
        }
        std::vector<const Entry*> current;
        current.reserve(eligible.size());
        for (const auto& e : eligible) {
            const auto it = state_->candidates.find(e.rank.candidateId);
            if (it == state_->candidates.end()) continue;
            if (it->second.candidateGeneration != e.rank.candidateGeneration) continue;
            if (publisher_fenced(*state_, it->second)) continue;
            current.push_back(&e);
        }
        if (current.empty()) {
            decision.summary = "candidate evidence changed during scheduling";
            return decision;
        }

        // A newer plan supersedes the older unresolved plan of the same request;
        // a request older than the one already planned is stale.
        PlacementPlan* previous = nullptr;
        const auto prevIt = state_->latestPlanByRequest.find(request.requestId);
        if (prevIt != state_->latestPlanByRequest.end()) {
            const auto pit = state_->plans.find(prevIt->second);
            if (pit != state_->plans.end()) previous = &pit->second;
        }
        if (previous != nullptr && previous->requestGeneration.value() > request.generation.value())
            throw FabricError(ErrorCode::StaleGeneration,
                              "scheduling request generation is older than the latest plan for this request");

        const Entry& winner = *current.front();
        PlacementPlan plan;
        plan.planId = PlacementPlanId(state_->nextPlanId++);
        plan.planGeneration = PlacementPlanGeneration(1);
        plan.requestId = request.requestId;
        plan.requestGeneration = request.generation;
        plan.selectedCandidate = winner.rank.candidateId;
        plan.selectedCandidateGeneration = winner.rank.candidateGeneration;
        const std::size_t maxFallback = state_->policy.maxFallbackCandidates.value() > 0
            ? state_->policy.maxFallbackCandidates.value() : current.size() - 1;
        for (std::size_t i = 1; i < current.size() && i <= maxFallback; ++i)
            plan.fallbackCandidates.push_back(current[i]->rank.candidateId);
        plan.bindings = winner.candidate->bindings;
        plan.claims = build_claims(winner.candidate->bindings);
        plan.communicationPlan = winner.candidate->communication.planGeneration;
        plan.selectedFactors = winner.rank.factors;
        plan.rejectedAlternatives = decision.rejections;
        plan.generations = make_fingerprint(*winner.candidate, request, dep, state_->epoch);
        plan.policyGeneration = request.demand.policyGeneration.is_valid()
            ? request.demand.policyGeneration : state_->policy.generation;
        plan.authorityGeneration = state_->authority;
        plan.coordinatorEpoch = state_->epoch;
        plan.movementBytes = winner.candidate->residency.moveBytes;
        plan.movementSource = winner.candidate->residency.moveSource;
        plan.state = PlacementLifecycleState::REQUESTED;
        std::uint64_t ord = state_->nextEventOrdinal;
        advance_local(plan, PlacementLifecycleState::DISCOVERING, ord++);
        advance_local(plan, PlacementLifecycleState::FILTERING, ord++);
        advance_local(plan, PlacementLifecycleState::RANKING, ord++);
        advance_local(plan, PlacementLifecycleState::PLAN_READY, ord++);
        if (previous != nullptr && is_unresolved(previous->state)) {
            previous->supersededBy = plan.planId;
            previous->supersessionReason = "superseded by a newer plan for the same request";
            force_state(*previous, PlacementLifecycleState::SUPERSEDED, ord++);
        }
        state_->nextEventOrdinal = ord;
        const PlacementPlanId pid = plan.planId;
        state_->plans.emplace(pid, plan);
        state_->latestPlanByRequest[request.requestId] = pid;
        decision.plan = std::move(plan);
        decision.producedPlan = true;
        decision.summary = "selected candidate " + std::to_string(winner.rank.candidateId.value());
    } else {
        std::lock_guard<std::mutex> g(state_->mu);
        state_->requests[request.requestId] = request;
        decision.summary = decision.rejections.empty() ? "no candidates" : "all candidates hard-rejected";
    }

    return decision;
}

ExecutionHandoff make_unauthorized(std::string why) {
    (void)why;
    ExecutionHandoff h;
    h.authorized = false;
    h.executionId = ExecutionId(0);
    h.executionGeneration = ExecutionGeneration(0);
    h.attemptId = AttemptId(0);
    h.attemptGeneration = AttemptGeneration(0);
    h.authorityGeneration = AuthorityGeneration(0);
    return h;
}

ResourceCommitResult Scheduler::commit(PlacementPlanId planId, ResourceBroker& broker) {
    std::vector<ResourceClaim> claims;
    {
        std::lock_guard<std::mutex> g(state_->mu);
        auto it = state_->plans.find(planId);
        if (it == state_->plans.end()) return { false, "", ResourceGeneration(1), "unknown placement plan", false };
        auto& plan = it->second;
        if (plan.state != PlacementLifecycleState::PLAN_READY)
            return { false, "", ResourceGeneration(1), "placement plan is not PLAN_READY", false };
        if (state_->cancelledRequests.count(plan.requestId))
            return { false, "", ResourceGeneration(1), "request cancelled", false };
        if (plan.authorityGeneration != state_->authority)
            return { false, "", ResourceGeneration(1), "stale authority", false };
        // Revalidation before resource commit: stale evidence commits nothing.
        const std::string stale = stale_evidence(*state_, plan);
        if (!stale.empty()) {
            advance_local(plan, PlacementLifecycleState::REVALIDATION_REQUIRED, state_->nextEventOrdinal++);
            return { false, "", ResourceGeneration(1), stale, false };
        }
        claims = plan.claims;
        advance_local(plan, PlacementLifecycleState::AWAITING_RESOURCE_COMMIT, state_->nextEventOrdinal++);
    }

    // Broker is an external owner - never invoked while holding the lock.
    ResourceCommitResult brokerResult;
    try {
        brokerResult = broker.commit(claims);
    } catch (...) {
        // The broker did not answer, so no commitment exists; the plan must not
        // stay in flight.
        std::lock_guard<std::mutex> g(state_->mu);
        const auto it = state_->plans.find(planId);
        if (it != state_->plans.end() && it->second.state == PlacementLifecycleState::AWAITING_RESOURCE_COMMIT)
            advance_local(it->second, PlacementLifecycleState::REVALIDATION_REQUIRED, state_->nextEventOrdinal++);
        throw;
    }

    bool releaseOutside = false;
    std::string releaseToken;
    ResourceCommitResult out;
    {
        std::lock_guard<std::mutex> g(state_->mu);
        auto it = state_->plans.find(planId);
        if (it == state_->plans.end()) {
            releaseOutside = brokerResult.succeeded; releaseToken = brokerResult.commitmentToken;
            out = { false, "", brokerResult.resourceGeneration, "plan vanished during commit", false };
        } else {
            auto& plan = it->second;
            const bool overtaken = state_->cancelledRequests.count(plan.requestId) ||
                                   plan.authorityGeneration != state_->authority ||
                                   plan.state != PlacementLifecycleState::AWAITING_RESOURCE_COMMIT;
            // Verify step: the evidence must still be current once the broker
            // has answered.
            const std::string stale = overtaken ? std::string() : stale_evidence(*state_, plan);
            if (overtaken || !stale.empty()) {
                if (!overtaken) advance_local(plan, PlacementLifecycleState::REVALIDATION_REQUIRED, state_->nextEventOrdinal++);
                releaseOutside = brokerResult.succeeded; releaseToken = brokerResult.commitmentToken;
                out = { false, "", brokerResult.resourceGeneration,
                        !brokerResult.succeeded ? std::string("no commitment")
                            : (stale.empty() ? std::string("stale/cancelled after provisional commit") : stale),
                        false };
            } else if (!brokerResult.succeeded) {
                // No false COMMITTED state; mark for revalidation so a fresh
                // scheduling pass can evaluate fallback under current evidence.
                advance_local(plan, PlacementLifecycleState::REVALIDATION_REQUIRED, state_->nextEventOrdinal++);
                out = { false, "", brokerResult.resourceGeneration, brokerResult.failureReason, false };
            } else {
                plan.hasCommitmentEvidence = true;
                advance_local(plan, PlacementLifecycleState::COMMITTED, state_->nextEventOrdinal++);
                plan.claims = claims;
                out = { true, brokerResult.commitmentToken, brokerResult.resourceGeneration, "", false };
            }
        }
    }
    if (releaseOutside && !releaseToken.empty()) broker.release(releaseToken);
    return out;
}

ExecutionHandoff Scheduler::handoff(PlacementPlanId planId, ExecutionFabric& exec) {
    PlacementPlan snapshot;
    {
        std::lock_guard<std::mutex> g(state_->mu);
        auto it = state_->plans.find(planId);
        if (it == state_->plans.end()) return make_unauthorized("unknown placement plan");
        auto& plan = it->second;
        if (plan.state != PlacementLifecycleState::COMMITTED) return make_unauthorized("plan is not COMMITTED");
        if (state_->cancelledRequests.count(plan.requestId)) return make_unauthorized("request cancelled");
        if (plan.authorityGeneration != state_->authority) return make_unauthorized("stale authority");
        // Revalidation before execution handoff: a stale placement launches nothing.
        const std::string stale = stale_evidence(*state_, plan);
        if (!stale.empty()) {
            advance_local(plan, PlacementLifecycleState::REVALIDATION_REQUIRED, state_->nextEventOrdinal++);
            return make_unauthorized(stale);
        }
        snapshot = plan;
        advance_local(plan, PlacementLifecycleState::AWAITING_EXECUTION, state_->nextEventOrdinal++);
    }

    ExecutionHandoff h;
    try {
        h = exec.authorize(snapshot);
    } catch (...) {
        // No authority was granted; the plan must not stay in flight.
        std::lock_guard<std::mutex> g(state_->mu);
        const auto it = state_->plans.find(planId);
        if (it != state_->plans.end() && it->second.state == PlacementLifecycleState::AWAITING_EXECUTION)
            advance_local(it->second, PlacementLifecycleState::REVALIDATION_REQUIRED, state_->nextEventOrdinal++);
        throw;
    }
    {
        std::lock_guard<std::mutex> g(state_->mu);
        auto it = state_->plans.find(planId);
        if (it == state_->plans.end()) { h.authorized = false; return h; }
        auto& plan = it->second;
        // A plan that was cancelled, superseded, fenced or sent back for
        // revalidation while the execution fabric was consulted launches
        // nothing and keeps the state it was given.
        if (state_->cancelledRequests.count(plan.requestId) || plan.authorityGeneration != state_->authority ||
            plan.state != PlacementLifecycleState::AWAITING_EXECUTION) {
            h.authorized = false;
            return h;
        }
        if (h.authorized && !stale_evidence(*state_, plan).empty()) h.authorized = false;
        if (h.authorized) advance_local(plan, PlacementLifecycleState::ACTIVE, state_->nextEventOrdinal++);
        else advance_local(plan, PlacementLifecycleState::REVALIDATION_REQUIRED, state_->nextEventOrdinal++);
        return h;
    }
}

RevalidationResult Scheduler::revalidate(PlacementPlanId planId, const GenerationFingerprint& current) {
    std::lock_guard<std::mutex> g(state_->mu);
    auto it = state_->plans.find(planId);
    if (it == state_->plans.end()) throw FabricError(ErrorCode::UnknownState, "unknown placement plan");
    auto res = revalidate_plan(it->second, current);
    // A terminal plan stays terminal: revalidation never revives it.
    if (!res.current && !is_terminal(it->second.state))
        force_state(it->second, PlacementLifecycleState::REVALIDATION_REQUIRED, state_->nextEventOrdinal++);
    res.resultingState = it->second.state;
    return res;
}

void Scheduler::cancel(SchedulingRequestId requestId, std::string reason) {
    (void)reason;
    std::lock_guard<std::mutex> g(state_->mu);
    state_->cancelledRequests.insert(requestId);
    for (auto& it : state_->plans) {
        auto& plan = it.second;
        if (plan.requestId == requestId && !is_terminal(plan.state))
            force_state(plan, PlacementLifecycleState::CANCELLED, state_->nextEventOrdinal++);
    }
}

void Scheduler::supersede(SchedulingRequestId oldRequest, SchedulingRequestId newRequest, std::string reason) {
    std::lock_guard<std::mutex> g(state_->mu);
    PlacementPlanId newPlanId(0);
    const auto nit = state_->latestPlanByRequest.find(newRequest);
    if (nit != state_->latestPlanByRequest.end()) newPlanId = nit->second;
    for (auto& it : state_->plans) {
        auto& plan = it.second;
        if (plan.requestId == oldRequest && !is_terminal(plan.state)) {
            plan.supersededBy = newPlanId;
            plan.supersessionReason = reason;
            force_state(plan, PlacementLifecycleState::SUPERSEDED, state_->nextEventOrdinal++);
        }
    }
    std::ostringstream oss;
    oss << "request superseded: " << oldRequest.value() << " by " << newRequest.value() << "; " << reason;
    state_->supersessionHistory.push_back(oss.str());
}

PlacementExplanation Scheduler::explain(PlacementPlanId planId) const {
    std::lock_guard<std::mutex> g(state_->mu);
    PlacementExplanation ex;
    ex.planId = planId;
    const auto it = state_->plans.find(planId);
    if (it == state_->plans.end()) { ex.why = "unknown plan"; return ex; }
    const auto& plan = it->second;
    std::ostringstream wh;
    wh << "selected candidate " << plan.selectedCandidate.value()
       << " (generation " << plan.selectedCandidateGeneration.value()
       << "); state=" << to_string(plan.state);
    ex.winningCandidate = "candidate " + std::to_string(plan.selectedCandidate.value());
    ex.why = wh.str();
    for (const auto& f : plan.selectedFactors) {
        std::ostringstream fs;
        fs << to_string(f.kind) << "=" << f.value.value() << " w=" << f.weight
           << " " << to_string(f.provenance) << " :: " << f.description;
        ex.factors.push_back(fs.str());
    }
    for (const auto& r : plan.rejectedAlternatives) {
        std::ostringstream rs;
        rs << "candidate " << r.candidateId.value() << ": " << to_string(r.status) << " - " << r.reason;
        ex.rejections.push_back(rs.str());
    }
    std::ostringstream dn;
    dn << "plan " << plan.planId.value() << " generation " << plan.planGeneration.value()
       << " authority " << plan.authorityGeneration.value() << " epoch " << plan.coordinatorEpoch.value();
    ex.deterministicNote = dn.str();
    return ex;
}

PlacementLifecycleState Scheduler::plan_state(PlacementPlanId planId) const {
    std::lock_guard<std::mutex> g(state_->mu);
    const auto it = state_->plans.find(planId);
    return it == state_->plans.end() ? PlacementLifecycleState::RETIRED : it->second.state;
}

std::optional<PlacementPlan> Scheduler::plan(PlacementPlanId planId) const {
    std::lock_guard<std::mutex> g(state_->mu);
    const auto it = state_->plans.find(planId);
    if (it == state_->plans.end()) return std::nullopt;
    return it->second;
}

std::optional<Candidate> Scheduler::candidate(CandidateId candidateId) const {
    std::lock_guard<std::mutex> g(state_->mu);
    const auto it = state_->candidates.find(candidateId);
    if (it == state_->candidates.end()) return std::nullopt;
    return it->second;
}



void Scheduler::fence_worker(WorkerBootId bootId) {
    std::lock_guard<std::mutex> g(state_->mu);
    state_->fencedWorkers.insert(bootId);
    invalidate_plans(*state_, [bootId](const Candidate& c) { return c.workerBootId == bootId; });
}

void Scheduler::fence_source(SourceBootId bootId) {
    std::lock_guard<std::mutex> g(state_->mu);
    state_->fencedSources.insert(bootId);
    invalidate_plans(*state_, [bootId](const Candidate& c) { return c.sourceBootId == bootId; });
}


Persisted Scheduler::snapshot() const {
    std::lock_guard<std::mutex> g(state_->mu);
    Persisted p;
    p.epoch = state_->epoch;
    p.authority = state_->authority;
    p.nextPlanId = state_->nextPlanId;
    p.nextSubmitOrdinal = state_->nextSubmitOrdinal;
    p.nextEventOrdinal = state_->nextEventOrdinal;
    p.ingestWatermark = state_->ingestWatermark;
    for (const auto& [id, req] : state_->requests) p.requests.push_back(req);
    for (const auto& [id, plan] : state_->plans) p.plans.push_back(plan);
    for (const auto& [id, identity] : state_->knownIdentities) p.candidateIdentities.push_back(identity);
    p.supersessionHistory = state_->supersessionHistory;
    p.cancelledRequests = state_->cancelledRequests;
    return p;
}

void Scheduler::restore(Persisted p) {
    std::lock_guard<std::mutex> g(state_->mu);
    state_->requests.clear();
    for (auto& req : p.requests) state_->requests.emplace(req.requestId, std::move(req));
    state_->plans.clear();
    state_->latestPlanByRequest.clear();
    for (auto& plan : p.plans) {
        const auto pid = plan.planId;
        state_->plans.emplace(pid, plan);
        auto& lp = state_->latestPlanByRequest;
        const auto it = lp.find(plan.requestId);
        if (it == lp.end() || it->second.value() < pid.value()) lp[plan.requestId] = pid;
    }
    state_->epoch = p.epoch;
    state_->authority = p.authority;
    state_->nextPlanId = p.nextPlanId;
    state_->nextSubmitOrdinal = p.nextSubmitOrdinal;
    state_->nextEventOrdinal = p.nextEventOrdinal;
    state_->ingestWatermark = p.ingestWatermark;
    state_->supersessionHistory = std::move(p.supersessionHistory);
    state_->cancelledRequests = std::move(p.cancelledRequests);
    state_->candidates.clear();      // dynamic evidence is never restored as current.
    state_->knownIdentities.clear(); // the identities recovered plans refer to are kept.
    for (const auto& identity : p.candidateIdentities) state_->knownIdentities[identity.candidateId] = identity;
    state_->fencedWorkers.clear();
    state_->fencedSources.clear();
    // Conservative recovery: any non-terminal plan must be revalidated.
    for (auto& [id, plan] : state_->plans) {
        if (!is_terminal(plan.state)) force_state(plan, PlacementLifecycleState::REVALIDATION_REQUIRED, state_->nextEventOrdinal++);
    }
}
}  // namespace fabric
