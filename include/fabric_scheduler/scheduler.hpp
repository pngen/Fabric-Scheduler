#pragma once
#include "fabric_scheduler/core/generations.hpp"
#include "fabric_scheduler/core/types.hpp"
#include "fabric_scheduler/request.hpp"
#include "fabric_scheduler/candidate.hpp"
#include "fabric_scheduler/eligibility.hpp"
#include "fabric_scheduler/ranking.hpp"
#include "fabric_scheduler/placement.hpp"
#include "fabric_scheduler/policy.hpp"
#include "fabric_scheduler/core/result.hpp"
#include "fabric_scheduler/adapters/resource_broker.hpp"
#include "fabric_scheduler/adapters/execution.hpp"
#include <memory>
#include <string>
#include <vector>
#include <cstdint>
#include <optional>

namespace fabric {

struct PlacementDecision {
    SchedulingRequest request;
    PlacementPlan plan;
    std::vector<CandidateId> eligible;       // eligible candidates, ranked order
    std::vector<CandidateId> disqualified;   // candidates that failed a hard constraint
    std::vector<RejectedAlternative> rejections;
    bool producedPlan{false};
    std::string summary;
};

struct PlacementExplanation {
    PlacementPlanId planId;
    std::string winningCandidate;
    std::string why;
    std::vector<std::string> factors;
    std::vector<std::string> rejections;
    std::string deterministicNote;
};

struct Persisted;
class SchedulerState;

// The Fabric Scheduler core facade.
class Scheduler {
public:
    struct Config {
        PolicyConfig policy;
        RankOptions rankOptions;
        CoreCount maxCandidatesPerRequest{256};
        AuthorityGeneration initialAuthority{1};
    };

    explicit Scheduler(Config config);
    ~Scheduler();
    Scheduler(const Scheduler&) = delete;
    Scheduler& operator=(const Scheduler&) = delete;

    CoordinatorEpoch epoch() const;
    void set_epoch(CoordinatorEpoch newEpoch);
    AuthorityGeneration current_authority() const;

    void fence_worker(WorkerBootId bootId);
    void fence_source(SourceBootId bootId);
    void ingest_candidate(Candidate candidate);
    std::size_t candidate_count() const;
    std::size_t ingest_watermark() const;

    PlacementDecision schedule(const SchedulingRequest& request,
                               const DependencyView* dependency = nullptr,
                               const PolicyVeto* policy = nullptr);

    ResourceCommitResult commit(PlacementPlanId planId, ResourceBroker& broker);
    ExecutionHandoff handoff(PlacementPlanId planId, ExecutionFabric& exec);
    RevalidationResult revalidate(PlacementPlanId planId, const GenerationFingerprint& current);

    void cancel(SchedulingRequestId requestId, std::string reason = {});
    void supersede(SchedulingRequestId oldRequest, SchedulingRequestId newRequest,
                  std::string reason = {});

    // --- persistence -----------------------------------------------------
    Persisted snapshot() const;
    void restore(Persisted snapshot);

    PlacementExplanation explain(PlacementPlanId planId) const;
    PlacementLifecycleState plan_state(PlacementPlanId planId) const;
    std::optional<PlacementPlan> plan(PlacementPlanId planId) const;
    std::optional<Candidate> candidate(CandidateId candidateId) const;

private:
    std::unique_ptr<SchedulerState> state_;
};

}  // namespace fabric