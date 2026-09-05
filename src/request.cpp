#include "fabric_scheduler/request.hpp"
#include <cmath>
#include <algorithm>

namespace fabric {

namespace {
constexpr std::uint64_t kMaxBytes = 16ull << 40;   // sanity ceiling for any byte quantity
constexpr std::uint64_t kMaxCpuCount = 1u << 20;
constexpr Duration kMaxHorizon(30ull * 24 * 60 * 60 * 1000000000ull);  // 30 days
}

void DemandProfile::validate() const {
    // --- accelerator count range -------------------------------------------
    const auto min = accelerator.minimumCount.value();
    const auto max = accelerator.maximumCount.value();
    const auto target = accelerator.targetCount.value();
    if (max > 0 && min > max) {
        throw FabricError(ErrorCode::InvalidCountRange,
                          "accelerator minimumCount exceeds maximumCount");
    }
    if (target > 0 && (min > target || (max > 0 && target > max))) {
        throw FabricError(ErrorCode::InvalidCountRange,
                          "accelerator targetCount outside [minimumCount, maximumCount]");
    }
    if (min > kMaxCpuCount || max > kMaxCpuCount || target > kMaxCpuCount) {
        throw FabricError(ErrorCode::InvalidCountRange, "accelerator count out of supported range");
    }
    if (min > 0 && accelerator.capability == AcceleratorClass::None) {
        throw FabricError(ErrorCode::InvalidDemand,
                          "accelerator count requested with no accelerator capability class");
    }

    // --- byte quantities ---------------------------------------------------
    if (accelerator.vramPerDevice.value() > kMaxBytes ||
        accelerator.aggregateVram.value() > kMaxBytes ||
        memory.hostMemory.value() > kMaxBytes ||
        memory.pinnedMemory.value() > kMaxBytes ||
        storage.capacity.value() > kMaxBytes ||
        network.bandwidth.value() > kMaxBytes ||
        communicationBytes.value() > kMaxBytes ||
        maximumMovementCost.value() > kMaxBytes) {
        throw FabricError(ErrorCode::Overflow, "a byte quantity exceeds the supported ceiling");
    }

    // aggregate VRAM must cover the minimum device count at per-device VRAM
    if (min > 0 && accelerator.aggregateVram.value() > 0 &&
        accelerator.aggregateVram.value() < accelerator.vramPerDevice.value() * min) {
        throw FabricError(ErrorCode::InvalidDemand,
                          "aggregate VRAM is below per-device VRAM times minimum count");
    }

    // --- duration ----------------------------------------------------------
    if (expectedExecutionDuration.nanoseconds() > kMaxHorizon.nanoseconds()) {
        throw FabricError(ErrorCode::ImpossibleDuration,
                          "expected execution duration exceeds supported horizon");
    }

    // --- throughput objective ---------------------------------------------
    if (throughputObjective != 0.0) {
        if (!std::isfinite(throughputObjective) || throughputObjective < 0.0) {
            throw FabricError(ErrorCode::NonFinite,
                              "throughput objective must be finite and non-negative");
        }
    }

    // --- fallback locality list --------------------------------------------
    std::vector<std::uint8_t> seen(kLocalityKindCount, 0);
    for (const auto lk : fallbackLocalities) {
        const auto idx = static_cast<std::uint8_t>(lk);
        if (idx >= kLocalityKindCount) {
            throw FabricError(ErrorCode::MalformedLocalityConstraint,
                              "unknown locality kind in fallback list");
        }
        if (seen[idx]++) {
            throw FabricError(ErrorCode::MalformedLocalityConstraint,
                              "duplicate locality kind in fallback list");
        }
    }

    // --- identities --------------------------------------------------------
    if (!workloadId.is_valid() || !demandId.is_valid()) {
        throw FabricError(ErrorCode::InvalidDemand, "workload/demand identity missing");
    }
    if (!workloadGeneration.is_valid() || !demandGeneration.is_valid()) {
        throw FabricError(ErrorCode::InvalidDemand, "workload/demand generation missing");
    }
}

}
