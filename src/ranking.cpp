#include "fabric_scheduler/ranking.hpp"
#include "fabric_scheduler/request.hpp"
#include "fabric_scheduler/candidate.hpp"
#include <algorithm>
#include <cmath>
#include <cstdint>

namespace fabric {

namespace {

constexpr double kMaxDistanceDivisor = 8.0;   // for hop/count normalization
constexpr double kMaxNuma = 4.0;
constexpr double kMaxHops = 8.0;
constexpr std::uint64_t kMoveNormalizer = 8ull << 30;  // 8 GiB reference movement

double clamp01(double v) {
    if (v < 0.0) return 0.0;
    if (v > 1.0) return 1.0;
    return v;
}

double ratio_or_one(std::uint64_t need, std::uint64_t avail) {
    if (avail == 0) return 1.0;
    return clamp01(static_cast<double>(need) / static_cast<double>(avail));
}

double distance_norm(std::uint32_t d, double divisor) {
    return clamp01(static_cast<double>(d) / divisor);
}

}  // namespace

bool RankComparator::operator()(const RankedCandidate& a, const RankedCandidate& b) const noexcept {
    if (a.score != b.score) return a.score < b.score;                  // lower total cost wins
    if (a.unknownCount != b.unknownCount) return a.unknownCount < b.unknownCount;   // fewer unknowns
    if (a.evidenceStrength != b.evidenceStrength) return a.evidenceStrength > b.evidenceStrength;  // stronger evidence
    if (a.movementCost != b.movementCost) return a.movementCost < b.movementCost;
    if (a.headroomCost != b.headroomCost) return a.headroomCost < b.headroomCost;
    if (a.congestionCost != b.congestionCost) return a.congestionCost < b.congestionCost;
    if (a.localResources != b.localResources) return a.localResources > b.localResources;
    return a.candidateId < b.candidateId;                             // stable strong-ID ordering
}

Cost Ranker::compute_factor(RankFactorKind kind, const SchedulingRequest& request,
                            const Candidate& c, Provenance& outProv,
                            std::string& outDesc) const {
    const auto& demand = request.demand;
    const auto& accel = demand.accelerator;
    const auto& cap = c.capacity;
    const auto& loc = c.locality;
    const auto& comm = c.communication;
    const auto& res = c.residency;
    double v = 0.0;

    switch (kind) {
        case RankFactorKind::AcceleratorFit: {
            const std::uint64_t need = accel.aggregateVram.value() > 0
                ? accel.aggregateVram.value()
                : accel.vramPerDevice.value() * accel.minimumCount.value();
            v = ratio_or_one(need, cap.vramAvailable.value());
            outProv = cap.provenance;
            outDesc = "VRAM fit ratio (need/available)";
            break;
        }
        case RankFactorKind::ResourceHeadroom: {
            const double cpu = ratio_or_one(demand.cpuRequired.value(), cap.cpuAvailable.value());
            const double hm = ratio_or_one(demand.memory.hostMemory.value(), cap.hostMemoryAvailable.value());
            v = std::max(cpu, hm);
            outProv = cap.provenance;
            outDesc = "aggregate CPU/host-memory demand ratio";
            break;
        }
        case RankFactorKind::VramHeadroom: {
            const std::uint64_t need = accel.aggregateVram.value() > 0
                ? accel.aggregateVram.value()
                : accel.vramPerDevice.value() * accel.minimumCount.value();
            v = ratio_or_one(need, cap.vramAvailable.value());
            outProv = cap.provenance;
            outDesc = "VRAM headroom ratio (need/available)";
            break;
        }
        case RankFactorKind::CpuHeadroom: {
            v = ratio_or_one(demand.cpuRequired.value(), cap.cpuAvailable.value());
            outProv = cap.provenance;
            outDesc = "CPU headroom ratio";
            break;
        }
        case RankFactorKind::PinnedMemoryHeadroom: {
            v = ratio_or_one(demand.memory.pinnedMemory.value(), cap.pinnedMemoryAvailable.value());
            outProv = cap.provenance;
            outDesc = "pinned-memory headroom ratio";
            break;
        }
        case RankFactorKind::TopologyLocality: {
            const double d = static_cast<double>(loc.numaDistance) + static_cast<double>(loc.pcieHops);
            v = distance_norm(static_cast<std::uint32_t>(d), kMaxDistanceDivisor);
            v = clamp01(v + (loc.sharesPcieRoot ? 0.25 : 0.0));
            outProv = loc.provenance;
            outDesc = "topology distance (NUMA+PCIe hops)";
            break;
        }
        case RankFactorKind::NumaLocality: {
            v = distance_norm(loc.numaDistance, kMaxNuma);
            outProv = loc.provenance;
            outDesc = "NUMA distance";
            break;
        }
        case RankFactorKind::PcieLocality: {
            v = distance_norm(loc.pcieHops, kMaxHops);
            v = clamp01(v + (loc.sharesPcieRoot ? 0.5 : 0.0));
            outProv = loc.provenance;
            outDesc = "PCIe hops / shared-root penalty";
            break;
        }
        case RankFactorKind::NicLocality: {
            v = distance_norm(loc.nicDistance, kMaxNuma);
            outProv = loc.provenance;
            outDesc = "NIC distance";
            break;
        }
        case RankFactorKind::StorageLocality: {
            v = distance_norm(loc.storageDistance, kMaxNuma);
            outProv = loc.provenance;
            outDesc = "storage distance";
            break;
        }
        case RankFactorKind::CommunicationCost: {
            v = clamp01(comm.estimatedCost.value() + comm.derivedPathCost.value() + comm.collectiveCost.value());
            outProv = comm.provenance;
            outDesc = "communication cost (estimated+derived+collective)";
            break;
        }
        case RankFactorKind::CongestionPenalty: {
            v = clamp01(c.congestion.congestion);
            outProv = c.congestion.provenance;
            outDesc = "congestion penalty";
            break;
        }
        case RankFactorKind::ResidualBandwidth: {
            v = clamp01(1.0 - c.congestion.residualBandwidth);
            outProv = c.congestion.provenance;
            outDesc = "inverse residual bandwidth";
            break;
        }
        case RankFactorKind::ModelResidency: {
            if (res.modelResident) { v = 0.0; }
            else { v = res.moveBytes.value() > 0 ? 1.0 : 0.5; }
            outProv = res.provenance;
            outDesc = "model residency / movement";
            break;
        }
        case RankFactorKind::AdapterResidency: {
            if (res.adapterResident) { v = 0.0; }
            else { v = res.moveBytes.value() > 0 ? 1.0 : 0.5; }
            outProv = res.provenance;
            outDesc = "adapter residency / movement";
            break;
        }
        case RankFactorKind::ReusableStateLocality: {
            v = res.stateLocal ? 0.0 : 1.0;
            outProv = res.provenance;
            outDesc = "reusable state locality";
            break;
        }
        case RankFactorKind::CheckpointMovementCost: {
            const std::uint64_t normalizer = demand.maximumMovementCost.value() > 0
                ? demand.maximumMovementCost.value() : kMoveNormalizer;
            v = clamp01(static_cast<double>(res.moveBytes.value()) / static_cast<double>(normalizer));
            outProv = res.provenance;
            outDesc = "checkpoint/state movement cost";
            break;
        }
        case RankFactorKind::StartupCost: {
            v = res.moveBytes.value() > 0 ? 0.1 : 0.0;
            outProv = res.provenance;
            outDesc = "startup/reload cost";
            break;
        }
        case RankFactorKind::ReservationFit: {
            v = clamp01(static_cast<double>(c.reservation.reservedBytes.value()) /
                        static_cast<double>(std::max<std::uint64_t>(1, cap.vramTotal.value())));
            outProv = c.reservation.provenance;
            outDesc = "reservation pressure";
            break;
        }
        case RankFactorKind::FailureDomainDiversification: {
            v = 0.0;
            outProv = Provenance::Synthetic;
            outDesc = "per-candidate failure-domain cost (plan-level)";
            break;
        }
        case RankFactorKind::HealthMargin: {
            v = clamp01(1.0 - c.health.healthScore);
            outProv = c.health.provenance;
            outDesc = "health margin (1 - health score)";
            break;
        }
        case RankFactorKind::EnergyPower: {
            v = 0.0;
            outProv = Provenance::Unknown;
            outDesc = "energy/power (not modeled; external evidence only)";
            break;
        }
        case RankFactorKind::FragmentationImpact: {
            v = 0.0;
            outProv = Provenance::Synthetic;
            outDesc = "expected fragmentation impact (plan-level)";
            break;
        }
        case RankFactorKind::ExternalPriority: {
            v = 0.0;
            outProv = Provenance::Reported;
            outDesc = "external priority (uniform across candidates)";
            break;
        }
        case RankFactorKind::MigrationRecomputeCost: {
            v = res.moveBytes.value() > 0 ? clamp01(static_cast<double>(res.moveBytes.value()) /
                                                    static_cast<double>(kMoveNormalizer)) : 0.0;
            outProv = res.provenance;
            outDesc = "migration/recompute cost";
            break;
        }
        case RankFactorKind::EstimatedQueueing: {
            v = clamp01(c.congestion.congestion * 0.5);
            outProv = c.congestion.provenance;
            outDesc = "estimated queueing pressure";
            break;
        }
        case RankFactorKind::PolicyPreference: {
            v = 0.0;
            outProv = Provenance::Reported;
            outDesc = "policy preference (uniform per policy gate)";
            break;
        }
    }
    return Cost(v);
}

RankedCandidate Ranker::evaluate(const SchedulingRequest& request,
                                 const Candidate& candidate,
                                 const RankOptions& options) const {
    RankedCandidate out;
    out.candidateId = candidate.candidateId;
    out.candidateGeneration = candidate.candidateGeneration;

    double total = 0.0;
    std::uint8_t minStrength = 255;   // highest is inferred from the weakest provenance
    std::uint32_t unknown = 0;
    double moveCost = 0.0;
    double headroom = 0.0;
    double congestionCost = clamp01(candidate.congestion.congestion);
    std::uint32_t localResources = 0;

    out.factors.reserve(kRankFactorCount);
    for (const auto kind : kRankFactorOrder) {
        Provenance prov = Provenance::Unknown;
        std::string desc;
        const double weight = options.weights.get(kind);
        Cost value = compute_factor(kind, request, candidate, prov, desc);
        RankFactor f;
        f.kind = kind;
        f.value = value;
        f.weight = weight;
        f.provenance = prov;
        f.description = desc;
        out.factors.push_back(f);

        total += weight * value.value();
        if (prov == Provenance::Unknown) ++unknown;

        const std::uint8_t strength = provenance_strength(prov);
        if (strength < minStrength) minStrength = strength;

        const double cv = value.value();
        if (kind == RankFactorKind::CheckpointMovementCost) moveCost += cv;
        if (kind == RankFactorKind::ResourceHeadroom ||
            kind == RankFactorKind::VramHeadroom ||
            kind == RankFactorKind::CpuHeadroom ||
            kind == RankFactorKind::PinnedMemoryHeadroom) headroom += cv;
    }

    out.score = Cost(total);
    out.unknownCount = unknown;
    out.evidenceStrength = (minStrength == 255) ? 0 : minStrength;
    out.movementCost = Cost(moveCost);
    out.headroomCost = Cost(headroom);
    out.congestionCost = Cost(congestionCost);

    for (const auto& b : candidate.bindings) {
        if (b.memoryDomainId.is_valid() && candidate.locality.numaDistance == 0) ++localResources;
    }
    out.localResources = localResources;

    // A run is "deterministic" only if no UNKNOWN evidence influenced it.
    out.deterministic = (unknown == 0);
    return out;
}

}  // namespace fabric
