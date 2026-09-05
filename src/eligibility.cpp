#include "fabric_scheduler/eligibility.hpp"
#include "fabric_scheduler/request.hpp"
#include "fabric_scheduler/candidate.hpp"
#include "fabric_scheduler/adapters/dependency.hpp"
#include "fabric_scheduler/policy.hpp"
#include <algorithm>
#include <cstdint>

namespace fabric {

namespace {

// Saturating multiply so a hostile demand cannot wrap into a pass.
ByteCount saturating_mul(ByteCount a, CoreCount b) {
    if (a.value() == 0 || b.value() == 0) return ByteCount(0);
    const std::uint64_t lim = 0xFFFFFFFFFFFFFFFFull;
    if (a.value() > lim / b.value()) return ByteCount(lim);
    return ByteCount(a.value() * b.value());
}

EligibilityResult reject(EligibilityStatus status, std::string reason,
                         RankFactorKind factor, ResourceKind bottleneck) {
    EligibilityResult r;
    r.status = status;
    r.reason = std::move(reason);
    r.rejectedFactor = factor;
    r.bottleneckResource = bottleneck;
    return r;
}

}  // namespace

EligibilityResult HardFilter::evaluate(const SchedulingRequest& request,
                                       const Candidate& candidate,
                                       const DependencyView* dependency,
                                       const PolicyVeto* policy) const {
    const auto& demand = request.demand;
    const auto& accel = demand.accelerator;

    // --- insufficient evidence --------------------------------------------
    // A candidate that cannot prove the demanded compute capability must never
    // be treated as eligible.
    if (accel.minimumCount.value() > 0 && candidate.bindings.empty()) {
        return reject(EligibilityStatus::INSUFFICIENT_EVIDENCE,
                      "candidate exposes no accelerator binding for a demanded compute workload",
                      RankFactorKind::AcceleratorFit, ResourceKind::Accelerator);
    }
    if (accel.minimumCount.value() > 0 &&
        candidate.capacity.provenance == Provenance::Unknown) {
        return reject(EligibilityStatus::INSUFFICIENT_EVIDENCE,
                      "candidate capacity evidence provenance is UNKNOWN",
                      RankFactorKind::ResourceHeadroom, ResourceKind::Accelerator);
    }
    if (candidate.health.provenance == Provenance::Unknown) {
        return reject(EligibilityStatus::INSUFFICIENT_EVIDENCE,
                      "candidate health evidence provenance is UNKNOWN",
                      RankFactorKind::HealthMargin, ResourceKind::CpuCore);
    }

    // --- capability --------------------------------------------------------
    if (accel.minimumCount.value() > 0) {
        bool found = false;
        for (const auto& b : candidate.bindings) {
            if (b.capability == accel.capability && b.capabilityCurrent) { found = true; break; }
        }
        if (!found) {
            return reject(EligibilityStatus::REJECT_CAPABILITY,
                          "candidate does not expose a current required accelerator capability",
                          RankFactorKind::AcceleratorFit, ResourceKind::Accelerator);
        }
    }

    // --- capacity ----------------------------------------------------------
    if (accel.minimumCount.value() > 0) {
        ByteCount requiredVram = accel.aggregateVram.value() > 0
            ? accel.aggregateVram
            : saturating_mul(accel.vramPerDevice, accel.minimumCount);
        if (candidate.capacity.vramAvailable.value() < requiredVram.value()) {
            return reject(EligibilityStatus::REJECT_CAPACITY,
                          "candidate VRAM available is below required VRAM",
                          RankFactorKind::VramHeadroom, ResourceKind::Accelerator);
        }
    }
    if (candidate.capacity.cpuAvailable.value() < demand.cpuRequired.value()) {
        return reject(EligibilityStatus::REJECT_CAPACITY,
                      "candidate CPU capacity is below required CPU count",
                      RankFactorKind::CpuHeadroom, ResourceKind::CpuCore);
    }
    if (candidate.capacity.hostMemoryAvailable.value() < demand.memory.hostMemory.value()) {
        return reject(EligibilityStatus::REJECT_CAPACITY,
                      "candidate host memory available is below required host memory",
                      RankFactorKind::ResourceHeadroom, ResourceKind::HostMemory);
    }

    // --- memory (pinned) ---------------------------------------------------
    if (candidate.capacity.pinnedMemoryAvailable.value() < demand.memory.pinnedMemory.value()) {
        return reject(EligibilityStatus::REJECT_MEMORY,
                      "candidate pinned memory available is below required pinned memory",
                      RankFactorKind::PinnedMemoryHeadroom, ResourceKind::PinnedMemory);
    }

    // --- contiguity --------------------------------------------------------
    if (accel.requiresContiguousMemory && !candidate.capacity.contiguousMemory) {
        return reject(EligibilityStatus::REJECT_CONTIGUITY,
                      "candidate cannot provide contiguous memory",
                      RankFactorKind::ResourceHeadroom, ResourceKind::HostMemory);
    }

    // --- health ------------------------------------------------------------
    if (!candidate.health.ready) {
        return reject(EligibilityStatus::REJECT_HEALTH,
                      "candidate resource is not health-ready" +
                          (candidate.health.degradationReason.empty()
                               ? std::string()
                               : ": " + candidate.health.degradationReason),
                      RankFactorKind::HealthMargin, ResourceKind::CpuCore);
    }

    // --- network -----------------------------------------------------------
    if (demand.network.requireNic) {
        bool hasNic = false;
        for (const auto& b : candidate.bindings) {
            if (b.nicId.is_valid() && b.nicGeneration.is_valid()) { hasNic = true; break; }
        }
        if (!hasNic) {
            return reject(EligibilityStatus::REJECT_NETWORK,
                          "candidate exposes no NIC binding while network is required",
                          RankFactorKind::NicLocality, ResourceKind::Nic);
        }
        if (candidate.capacity.nicBandwidthAvailable.value() < demand.network.bandwidth.value()) {
            return reject(EligibilityStatus::REJECT_NETWORK,
                          "candidate NIC bandwidth is below required bandwidth",
                          RankFactorKind::ResidualBandwidth, ResourceKind::Nic);
        }
    }

    // --- storage -----------------------------------------------------------
    if (candidate.capacity.storageAvailable.value() < demand.storage.capacity.value()) {
        return reject(EligibilityStatus::REJECT_STORAGE,
                      "candidate storage available is below required storage",
                      RankFactorKind::StorageLocality, ResourceKind::Storage);
    }
    if (demand.storage.requireLocal && !candidate.residency.stateLocal) {
        return reject(EligibilityStatus::REJECT_STORAGE,
                      "candidate does not provide local storage for required state",
                      RankFactorKind::StorageLocality, ResourceKind::Storage);
    }

    // --- reservation -------------------------------------------------------
    if (candidate.reservation.protectedResource) {
        return reject(EligibilityStatus::REJECT_RESERVATION,
                      "candidate resource is protected by a live reservation",
                      RankFactorKind::ReservationFit, ResourceKind::Accelerator);
    }

    // --- topology ----------------------------------------------------------
    if (demand.requiredTopologyGeneration.is_valid() &&
        candidate.locality.topologyGeneration != demand.requiredTopologyGeneration) {
        return reject(EligibilityStatus::REJECT_TOPOLOGY,
                      "candidate topology generation does not match required topology",
                      RankFactorKind::TopologyLocality, ResourceKind::LinkBandwidth);
    }

    // --- dependency --------------------------------------------------------
    if (dependency != nullptr && !dependency->ready(demand.demandId)) {
        auto r = reject(EligibilityStatus::REJECT_DEPENDENCY,
                        "dependencies not ready: " + dependency->unsatisfied(demand.demandId),
                        RankFactorKind::PolicyPreference, ResourceKind::CpuCore);
        r.dependencyGeneration = dependency->generation();
        return r;
    }

    // --- policy ------------------------------------------------------------
    if (policy != nullptr && !policy->allows(candidate)) {
        return reject(EligibilityStatus::REJECT_POLICY,
                      "policy veto: " + policy->reason(candidate),
                      RankFactorKind::PolicyPreference, ResourceKind::CpuCore);
    }

    EligibilityResult ok;
    ok.status = EligibilityStatus::ELIGIBLE;
    ok.reason = "candidate meets all hard constraints";
    return ok;
}

}  // namespace fabric
