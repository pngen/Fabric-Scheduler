#include "fabric_scheduler/persistence.hpp"
#include "fabric_scheduler/coding.hpp"
#include "fabric_scheduler/core/error.hpp"
#include <ostream>
#include <istream>
#include <array>
#include <algorithm>
#include <cstring>

namespace fabric {

namespace {
constexpr std::uint32_t kMaxRecords = (1u << 20);  // 1M entries per list
constexpr std::uint64_t kMaxPayload = (1ull << 30);  // 1 GiB payload bound

std::uint64_t read_checked_len(ByteReader& r) {
    const std::uint32_t n = r.get_u32();
    if (n > kMaxRecords) throw FabricError(ErrorCode::OversizedFrame, "record count exceeds bound");
    return n;
}

void check_bytes(ByteReader& r) {
    if (r.remaining() != 0) throw FabricError(ErrorCode::Corruption, "trailing garbage in record");
}

// --- encode helpers -------------------------------------------------
void enc_string(ByteWriter& w, const std::string& s) { w.put_string(s); }

void enc_demand(ByteWriter& w, const DemandProfile& d) {
    w.put_id(d.demandId); w.put_id(d.demandGeneration);
    w.put_id(d.workloadId); w.put_id(d.workloadGeneration);
    w.put_id(d.accelerator.capabilityGeneration);
    w.put_u32(d.accelerator.minimumCount.value());
    w.put_u32(d.accelerator.targetCount.value());
    w.put_u32(d.accelerator.maximumCount.value());
    w.put_u64(d.accelerator.vramPerDevice.value());
    w.put_u64(d.accelerator.aggregateVram.value());
    w.put_bool(d.accelerator.requiresContiguousMemory);
    w.put_u8(static_cast<std::uint8_t>(d.accelerator.capability));
    w.put_u32(d.cpuRequired.value());
    w.put_u64(d.memory.hostMemory.value());
    w.put_u64(d.memory.pinnedMemory.value());
    w.put_u64(d.storage.capacity.value());
    w.put_bool(d.storage.requireLocal);
    w.put_u64(d.network.bandwidth.value());
    w.put_bool(d.network.requireNic);
    w.put_u8(static_cast<std::uint8_t>(d.communication));
    w.put_u64(d.communicationBytes.value());
    w.put_bool(d.residency.requireModelResident);
    w.put_bool(d.residency.requireAdapterResident);
    w.put_bool(d.residency.requireStateLocal);
    enc_string(w, d.residency.modelPath);
    enc_string(w, d.residency.adapterPath);
    enc_string(w, d.residency.statePath);
    w.put_u32(d.failureDomains.minimumDistinctDomains.value());
    w.put_bool(d.failureDomains.avoidColocation);
    w.put_bool(d.failureDomains.avoidSingleRoot);
    w.put_u64(d.expectedExecutionDuration.nanoseconds());
    w.put_u8(static_cast<std::uint8_t>(d.latencySensitivity));
    w.put_double(d.throughputObjective);
    w.put_i32(d.externalPriority);
    w.put_u64(d.externalPriorityOrdinal);
    w.put_id(d.sloGeneration);
    w.put_id(d.policyGeneration);
    w.put_id(d.priorityGeneration);
    w.put_u64(d.maximumMovementCost.value());
    w.put_u32(static_cast<std::uint32_t>(d.fallbackLocalities.size()));
    for (const auto lk : d.fallbackLocalities) w.put_u8(static_cast<std::uint8_t>(lk));
    w.put_u8(static_cast<std::uint8_t>(d.provenance));
}

void enc_request(ByteWriter& w, const SchedulingRequest& r) {
    w.put_id(r.requestId); w.put_id(r.generation);
    w.put_id(r.coordinatorEpoch);
    w.put_id(r.sourceId); w.put_id(r.sourceBootId);
    w.put_u64(r.submitOrdinal);
    enc_demand(w, r.demand);
}

void enc_binding(ByteWriter& w, const DeviceBinding& b) {
    w.put_id(b.deviceId); w.put_id(b.deviceGeneration);
    w.put_id(b.resourceId); w.put_id(b.resourceGeneration);
    w.put_id(b.nodeId); w.put_id(b.nodeGeneration);
    w.put_id(b.memoryDomainId); w.put_id(b.memoryDomainGeneration);
    w.put_id(b.nicId); w.put_id(b.nicGeneration);
    w.put_id(b.storageId); w.put_id(b.storageGeneration);
    w.put_id(b.cpuDomainId); w.put_id(b.cpuDomainGeneration);
    enc_string(w, b.pcieRoot);
    w.put_bool(b.sharesPcieRoot);
    w.put_u8(static_cast<std::uint8_t>(b.capability));
    w.put_i32(b.cudaCompatibility);
    w.put_bool(b.capabilityCurrent);
    w.put_id(b.capabilityGeneration);
}

void enc_factor(ByteWriter& w, const RankFactor& f) {
    w.put_u16(static_cast<std::uint16_t>(f.kind));
    w.put_double(f.value.value());
    w.put_double(f.weight);
    w.put_u8(static_cast<std::uint8_t>(f.provenance));
    enc_string(w, f.description);
}

void enc_rejected(ByteWriter& w, const RejectedAlternative& r) {
    w.put_id(r.candidateId);
    w.put_id(r.candidateGeneration);
    w.put_u8(static_cast<std::uint8_t>(r.status));
    enc_string(w, r.reason);
}

void enc_claim(ByteWriter& w, const ResourceClaim& c) {
    w.put_id(c.resourceId); w.put_id(c.resourceGeneration);
    w.put_u8(static_cast<std::uint8_t>(c.kind));
    w.put_u64(c.amount.value());
    w.put_bool(c.provisional);
}

void enc_fingerprint(ByteWriter& w, const GenerationFingerprint& g) {
    w.put_id(g.topology); w.put_id(g.capacity); w.put_id(g.reservation); w.put_id(g.congestion);
    w.put_id(g.capability); w.put_id(g.health); w.put_id(g.residency); w.put_id(g.communicationPlan);
    w.put_id(g.policy); w.put_id(g.resource); w.put_id(g.dependency); w.put_id(g.coordinatorEpoch);
}

void enc_plan(ByteWriter& w, const PlacementPlan& p) {
    w.put_id(p.planId); w.put_id(p.planGeneration);
    w.put_id(p.requestId); w.put_id(p.requestGeneration);
    w.put_id(p.selectedCandidate); w.put_id(p.selectedCandidateGeneration);
    w.put_u32(static_cast<std::uint32_t>(p.fallbackCandidates.size()));
    for (const auto c : p.fallbackCandidates) w.put_id(c);
    w.put_u32(static_cast<std::uint32_t>(p.bindings.size()));
    for (const auto& b : p.bindings) enc_binding(w, b);
    w.put_u32(static_cast<std::uint32_t>(p.claims.size()));
    for (const auto& c : p.claims) enc_claim(w, c);
    w.put_id(p.communicationPlan);
    enc_string(w, p.communicationPlanId);
    w.put_u64(p.movementBytes.value());
    enc_string(w, p.movementSource);
    w.put_bool(p.residencyPrerequisite);
    w.put_u32(static_cast<std::uint32_t>(p.selectedFactors.size()));
    for (const auto& f : p.selectedFactors) enc_factor(w, f);
    w.put_u32(static_cast<std::uint32_t>(p.rejectedAlternatives.size()));
    for (const auto& r : p.rejectedAlternatives) enc_rejected(w, r);
    enc_fingerprint(w, p.generations);
    w.put_id(p.policyGeneration);
    w.put_id(p.authorityGeneration);
    w.put_u8(static_cast<std::uint8_t>(p.state));
    w.put_u32(static_cast<std::uint32_t>(p.stateHistory.size()));
    for (const auto& h : p.stateHistory) { w.put_u8(static_cast<std::uint8_t>(h.first)); w.put_u64(h.second); }
    w.put_id(p.coordinatorEpoch);
    w.put_id(p.supersededBy);
    enc_string(w, p.supersessionReason);
    w.put_bool(p.hasCommitmentEvidence);
}

void enc_candidate_id(ByteWriter& w, const CandidateIdentity& c) {
    w.put_id(c.candidateId); w.put_id(c.candidateGeneration);
    w.put_id(c.sourceId); w.put_id(c.sourceBootId);
    w.put_id(c.workerId); w.put_id(c.workerBootId);
}

// --- decode helpers -------------------------------------------------
DemandProfile dec_demand(ByteReader& r) {
    DemandProfile d;
    d.demandId = r.get_id<WorkloadDemandIdTag>();
    d.demandGeneration = r.get_id<WorkloadDemandGenerationTag>();
    d.workloadId = r.get_id<WorkloadIdTag>();
    d.workloadGeneration = r.get_id<WorkloadGenerationTag>();
    d.accelerator.capabilityGeneration = r.get_id<CapabilityGenerationTag>();
    d.accelerator.minimumCount = CoreCount(r.get_u32());
    d.accelerator.targetCount = CoreCount(r.get_u32());
    d.accelerator.maximumCount = CoreCount(r.get_u32());
    d.accelerator.vramPerDevice = ByteCount(r.get_u64());
    d.accelerator.aggregateVram = ByteCount(r.get_u64());
    d.accelerator.requiresContiguousMemory = r.get_bool();
    const auto cap = r.get_u8();
    if (cap > static_cast<std::uint8_t>(AcceleratorClass::NoneRequired)) throw FabricError(ErrorCode::Corruption, "invalid accelerator class");
    d.accelerator.capability = static_cast<AcceleratorClass>(cap);
    d.cpuRequired = CoreCount(r.get_u32());
    d.memory.hostMemory = ByteCount(r.get_u64());
    d.memory.pinnedMemory = ByteCount(r.get_u64());
    d.storage.capacity = ByteCount(r.get_u64());
    d.storage.requireLocal = r.get_bool();
    d.network.bandwidth = ByteCount(r.get_u64());
    d.network.requireNic = r.get_bool();
    const auto comm = r.get_u8();
    if (comm > static_cast<std::uint8_t>(CommunicationPattern::Scatter)) throw FabricError(ErrorCode::Corruption, "invalid communication pattern");
    d.communication = static_cast<CommunicationPattern>(comm);
    d.communicationBytes = ByteCount(r.get_u64());
    d.residency.requireModelResident = r.get_bool();
    d.residency.requireAdapterResident = r.get_bool();
    d.residency.requireStateLocal = r.get_bool();
    d.residency.modelPath = r.get_string();
    d.residency.adapterPath = r.get_string();
    d.residency.statePath = r.get_string();
    d.failureDomains.minimumDistinctDomains = CoreCount(r.get_u32());
    d.failureDomains.avoidColocation = r.get_bool();
    d.failureDomains.avoidSingleRoot = r.get_bool();
    d.expectedExecutionDuration = Duration(r.get_u64());
    const auto ls = r.get_u8();
    if (ls > static_cast<std::uint8_t>(LatencySensitivity::Critical)) throw FabricError(ErrorCode::Corruption, "invalid latency sensitivity");
    d.latencySensitivity = static_cast<LatencySensitivity>(ls);
    d.throughputObjective = r.get_double();
    d.externalPriority = r.get_i32();
    d.externalPriorityOrdinal = r.get_u64();
    d.sloGeneration = r.get_id<SloGenerationTag>();
    d.policyGeneration = r.get_id<PolicyGenerationTag>();
    d.priorityGeneration = r.get_id<PriorityGenerationTag>();
    d.maximumMovementCost = ByteCount(r.get_u64());
    const std::uint64_t n = read_checked_len(r);
    d.fallbackLocalities.reserve(n);
    for (std::uint64_t i = 0; i < n; ++i) {
        const auto lk = r.get_u8();
        if (lk > static_cast<std::uint8_t>(LocalityKind::Topology)) throw FabricError(ErrorCode::Corruption, "invalid locality kind");
        d.fallbackLocalities.push_back(static_cast<LocalityKind>(lk));
    }
    const auto prov = r.get_u8();
    if (prov > static_cast<std::uint8_t>(Provenance::Unknown)) throw FabricError(ErrorCode::Corruption, "invalid provenance");
    d.provenance = static_cast<Provenance>(prov);
    d.validate();
    return d;
}

SchedulingRequest dec_request(ByteReader& r) {
    SchedulingRequest req;
    req.requestId = r.get_id<SchedulingRequestIdTag>();
    req.generation = r.get_id<SchedulingRequestGenerationTag>();
    req.coordinatorEpoch = r.get_id<CoordinatorEpochTag>();
    req.sourceId = r.get_id<SourceIdTag>();
    req.sourceBootId = r.get_id<SourceBootIdTag>();
    req.submitOrdinal = r.get_u64();
    req.demand = dec_demand(r);
    return req;
}

DeviceBinding dec_binding(ByteReader& r) {
    DeviceBinding b;
    b.deviceId = r.get_id<DeviceIdTag>(); b.deviceGeneration = r.get_id<DeviceGenerationTag>();
    b.resourceId = r.get_id<ResourceIdTag>(); b.resourceGeneration = r.get_id<ResourceGenerationTag>();
    b.nodeId = r.get_id<NodeIdTag>(); b.nodeGeneration = r.get_id<NodeGenerationTag>();
    b.memoryDomainId = r.get_id<MemoryDomainIdTag>(); b.memoryDomainGeneration = r.get_id<MemoryDomainGenerationTag>();
    b.nicId = r.get_id<NicIdTag>(); b.nicGeneration = r.get_id<NicGenerationTag>();
    b.storageId = r.get_id<StorageEndpointIdTag>(); b.storageGeneration = r.get_id<StorageEndpointGenerationTag>();
    b.cpuDomainId = r.get_id<CpuDomainIdTag>(); b.cpuDomainGeneration = r.get_id<CpuDomainGenerationTag>();
    b.pcieRoot = r.get_string();
    b.sharesPcieRoot = r.get_bool();
    const auto cap = r.get_u8();
    if (cap > static_cast<std::uint8_t>(AcceleratorClass::NoneRequired)) throw FabricError(ErrorCode::Corruption, "invalid accelerator class");
    b.capability = static_cast<AcceleratorClass>(cap);
    b.cudaCompatibility = r.get_i32();
    b.capabilityCurrent = r.get_bool();
    b.capabilityGeneration = r.get_id<CapabilityGenerationTag>();
    return b;
}

RankFactor dec_factor(ByteReader& r) {
    RankFactor f;
    const auto kind = r.get_u16();
    if (!is_valid_factor(static_cast<RankFactorKind>(kind))) throw FabricError(ErrorCode::Corruption, "invalid rank factor kind");
    f.kind = static_cast<RankFactorKind>(kind);
    f.value = Cost(r.get_double());
    f.weight = r.get_double();
    const auto prov = r.get_u8();
    if (prov > static_cast<std::uint8_t>(Provenance::Unknown)) throw FabricError(ErrorCode::Corruption, "invalid provenance");
    f.provenance = static_cast<Provenance>(prov);
    f.description = r.get_string();
    if (!f.value.is_finite()) throw FabricError(ErrorCode::Corruption, "non-finite factor cost");
    return f;
}

RejectedAlternative dec_rejected(ByteReader& r) {
    RejectedAlternative a;
    a.candidateId = r.get_id<CandidateIdTag>();
    a.candidateGeneration = r.get_id<CandidateGenerationTag>();
    const auto st = r.get_u8();
    if (st > static_cast<std::uint8_t>(EligibilityStatus::UNKNOWN)) throw FabricError(ErrorCode::Corruption, "invalid eligibility status");
    a.status = static_cast<EligibilityStatus>(st);
    a.reason = r.get_string();
    return a;
}

ResourceClaim dec_claim(ByteReader& r) {
    ResourceClaim c;
    c.resourceId = r.get_id<ResourceIdTag>();
    c.resourceGeneration = r.get_id<ResourceGenerationTag>();
    const auto k = r.get_u8();
    if (k > static_cast<std::uint8_t>(ResourceKind::ModelResidency)) throw FabricError(ErrorCode::Corruption, "invalid resource kind");
    c.kind = static_cast<ResourceKind>(k);
    c.amount = ByteCount(r.get_u64());
    c.provisional = r.get_bool();
    return c;
}

GenerationFingerprint dec_fingerprint(ByteReader& r) {
    GenerationFingerprint g;
    g.topology = r.get_id<TopologyGenerationTag>();
    g.capacity = r.get_id<CapacityGenerationTag>();
    g.reservation = r.get_id<ReservationGenerationTag>();
    g.congestion = r.get_id<CongestionGenerationTag>();
    g.capability = r.get_id<CapabilityGenerationTag>();
    g.health = r.get_id<HealthGenerationTag>();
    g.residency = r.get_id<ResidencyGenerationTag>();
    g.communicationPlan = r.get_id<CommunicationPlanGenerationTag>();
    g.policy = r.get_id<PolicyGenerationTag>();
    g.resource = r.get_id<ResourceGenerationTag>();
    g.dependency = r.get_id<DependencyGenerationTag>();
    g.coordinatorEpoch = r.get_id<CoordinatorEpochTag>();
    return g;
}

PlacementPlan dec_plan(ByteReader& r) {
    PlacementPlan p;
    p.planId = r.get_id<PlacementPlanIdTag>();
    p.planGeneration = r.get_id<PlacementPlanGenerationTag>();
    p.requestId = r.get_id<SchedulingRequestIdTag>();
    p.requestGeneration = r.get_id<SchedulingRequestGenerationTag>();
    p.selectedCandidate = r.get_id<CandidateIdTag>();
    p.selectedCandidateGeneration = r.get_id<CandidateGenerationTag>();
    const std::uint64_t nf = read_checked_len(r);
    p.fallbackCandidates.reserve(nf);
    for (std::uint64_t i = 0; i < nf; ++i) p.fallbackCandidates.push_back(r.get_id<CandidateIdTag>());
    const std::uint64_t nb = read_checked_len(r);
    p.bindings.reserve(nb);
    for (std::uint64_t i = 0; i < nb; ++i) p.bindings.push_back(dec_binding(r));
    const std::uint64_t ncl = read_checked_len(r);
    p.claims.reserve(ncl);
    for (std::uint64_t i = 0; i < ncl; ++i) p.claims.push_back(dec_claim(r));
    p.communicationPlan = r.get_id<CommunicationPlanGenerationTag>();
    p.communicationPlanId = r.get_string();
    p.movementBytes = ByteCount(r.get_u64());
    p.movementSource = r.get_string();
    p.residencyPrerequisite = r.get_bool();
    const std::uint64_t ns = read_checked_len(r);
    p.selectedFactors.reserve(ns);
    for (std::uint64_t i = 0; i < ns; ++i) p.selectedFactors.push_back(dec_factor(r));
    const std::uint64_t nr = read_checked_len(r);
    p.rejectedAlternatives.reserve(nr);
    for (std::uint64_t i = 0; i < nr; ++i) p.rejectedAlternatives.push_back(dec_rejected(r));
    p.generations = dec_fingerprint(r);
    p.policyGeneration = r.get_id<PolicyGenerationTag>();
    p.authorityGeneration = r.get_id<AuthorityGenerationTag>();
    const auto st = r.get_u8();
    if (st > static_cast<std::uint8_t>(PlacementLifecycleState::RETIRED)) throw FabricError(ErrorCode::Corruption, "invalid lifecycle state");
    p.state = static_cast<PlacementLifecycleState>(st);
    const std::uint64_t nh = read_checked_len(r);
    p.stateHistory.reserve(nh);
    for (std::uint64_t i = 0; i < nh; ++i) {
        const auto s = r.get_u8();
        if (s > static_cast<std::uint8_t>(PlacementLifecycleState::RETIRED)) throw FabricError(ErrorCode::Corruption, "invalid lifecycle state");
        p.stateHistory.emplace_back(static_cast<PlacementLifecycleState>(s), r.get_u64());
    }
    p.coordinatorEpoch = r.get_id<CoordinatorEpochTag>();
    p.supersededBy = r.get_id<PlacementPlanIdTag>();
    p.supersessionReason = r.get_string();
    p.hasCommitmentEvidence = r.get_bool();
    return p;
}

CandidateIdentity dec_candidate_id(ByteReader& r) {
    CandidateIdentity c;
    c.candidateId = r.get_id<CandidateIdTag>();
    c.candidateGeneration = r.get_id<CandidateGenerationTag>();
    c.sourceId = r.get_id<SourceIdTag>();
    c.sourceBootId = r.get_id<SourceBootIdTag>();
    c.workerId = r.get_id<WorkerIdTag>();
    c.workerBootId = r.get_id<WorkerBootIdTag>();
    return c;
}

}  // namespace

void persist_save(const Persisted& state, std::ostream& out) {
    ByteWriter pw;
    pw.put_id(state.epoch);
    pw.put_id(state.authority);
    pw.put_u64(state.nextPlanId);
    pw.put_u64(state.nextSubmitOrdinal);
    pw.put_u64(state.nextEventOrdinal);
    pw.put_u64(state.ingestWatermark);
    pw.put_u32(static_cast<std::uint32_t>(state.requests.size()));
    for (const auto& r : state.requests) enc_request(pw, r);
    pw.put_u32(static_cast<std::uint32_t>(state.plans.size()));
    for (const auto& p : state.plans) enc_plan(pw, p);
    pw.put_u32(static_cast<std::uint32_t>(state.candidateIdentities.size()));
    for (const auto& c : state.candidateIdentities) enc_candidate_id(pw, c);
    pw.put_u32(static_cast<std::uint32_t>(state.supersessionHistory.size()));
    for (const auto& s : state.supersessionHistory) pw.put_string(s);
    pw.put_u32(static_cast<std::uint32_t>(state.cancelledRequests.size()));
    for (const auto id : state.cancelledRequests) pw.put_id(id);

    const auto payload = pw.take();
    const std::uint64_t sum = fnv1a64_digest(payload.data(), payload.size());

    ByteWriter head;
    head.put_u32(kPersistenceMagic);
    head.put_u32(state.formatVersion);
    head.put_u8(kPersistenceFlags);
    head.put_u64(static_cast<std::uint64_t>(payload.size()));
    const auto hd = head.take();
    out.write(reinterpret_cast<const char*>(hd.data()), static_cast<std::streamsize>(hd.size()));
    out.write(reinterpret_cast<const char*>(payload.data()), static_cast<std::streamsize>(payload.size()));
    ByteWriter foot;
    foot.put_u64(sum);
    const auto fd = foot.take();
    out.write(reinterpret_cast<const char*>(fd.data()), static_cast<std::streamsize>(fd.size()));
}

std::uint64_t fnv1a64_digest(const std::byte* data, std::size_t n) {
    std::uint64_t h = 14695981039346656037ull;
    for (std::size_t i = 0; i < n; ++i) {
        h ^= static_cast<std::uint8_t>(data[i]);
        h *= 1099511628211ull;
    }
    return h;
}

Persisted persist_load(std::istream& in) {
    std::array<std::byte, 17> hdr;
    in.read(reinterpret_cast<char*>(hdr.data()), static_cast<std::streamsize>(hdr.size()));
    if (in.gcount() != static_cast<std::streamsize>(hdr.size())) throw FabricError(ErrorCode::Truncation, "header truncated");
    ByteReader hr(hdr.data(), hdr.size());
    const std::uint32_t magic = hr.get_u32();
    if (magic != kPersistenceMagic) throw FabricError(ErrorCode::Corruption, "bad magic");
    const std::uint32_t version = hr.get_u32();
    if (version == 0 || version > kPersistenceFormatVersion) throw FabricError(ErrorCode::UnsupportedVersion, "unsupported format version");
    (void)hr.get_u8();
    const std::uint64_t payLen = hr.get_u64();
    if (payLen > kMaxPayload) throw FabricError(ErrorCode::OversizedFrame, "payload size exceeds bound");

    std::vector<std::byte> payload(static_cast<std::size_t>(payLen));
    in.read(reinterpret_cast<char*>(payload.data()), static_cast<std::streamsize>(payLen));
    if (in.gcount() != static_cast<std::streamsize>(payLen)) throw FabricError(ErrorCode::Truncation, "payload truncated");

    std::array<std::byte, 8> foot;
    in.read(reinterpret_cast<char*>(foot.data()), static_cast<std::streamsize>(foot.size()));
    if (in.gcount() != static_cast<std::streamsize>(foot.size())) throw FabricError(ErrorCode::Truncation, "checksum truncated");
    ByteReader fr(foot.data(), foot.size());
    const std::uint64_t stored = fr.get_u64();
    const std::uint64_t computed = fnv1a64_digest(payload.data(), payload.size());
    if (stored != computed) throw FabricError(ErrorCode::ChecksumMismatch, "checksum mismatch");

    ByteReader r(payload);
    Persisted st;
    st.formatVersion = version;
    st.epoch = r.get_id<CoordinatorEpochTag>();
    st.authority = r.get_id<AuthorityGenerationTag>();
    st.nextPlanId = r.get_u64();
    st.nextSubmitOrdinal = r.get_u64();
    st.nextEventOrdinal = r.get_u64();
    st.ingestWatermark = r.get_u64();
    if (!st.epoch.is_valid()) throw FabricError(ErrorCode::Corruption, "invalid coordinator epoch");

    const std::uint64_t nr = read_checked_len(r);
    st.requests.reserve(nr);
    { std::unordered_set<SchedulingRequestId> seen;
      for (std::uint64_t i = 0; i < nr; ++i) {
          auto req = dec_request(r);
          if (!seen.insert(req.requestId).second) throw FabricError(ErrorCode::Corruption, "duplicate request id");
          st.requests.push_back(std::move(req));
      } }

    const std::uint64_t np = read_checked_len(r);
    st.plans.reserve(np);
    { std::unordered_set<PlacementPlanId> seen;
      std::uint64_t maxPlan = 0;
      for (std::uint64_t i = 0; i < np; ++i) {
          auto plan = dec_plan(r);
          if (!seen.insert(plan.planId).second) throw FabricError(ErrorCode::Corruption, "duplicate plan id");
          if (plan.planId.value() > maxPlan) maxPlan = plan.planId.value();
          st.plans.push_back(std::move(plan));
      }
      if (st.nextPlanId <= maxPlan) throw FabricError(ErrorCode::Corruption, "plan id generation regression");
    }

    const std::uint64_t nc = read_checked_len(r);
    st.candidateIdentities.reserve(nc);
    { std::unordered_set<CandidateId> seen;
      for (std::uint64_t i = 0; i < nc; ++i) {
          auto c = dec_candidate_id(r);
          if (!seen.insert(c.candidateId).second) throw FabricError(ErrorCode::Corruption, "duplicate candidate id");
          st.candidateIdentities.push_back(std::move(c));
      } }

    const std::uint64_t nh = read_checked_len(r);
    st.supersessionHistory.reserve(nh);
    for (std::uint64_t i = 0; i < nh; ++i) st.supersessionHistory.push_back(r.get_string());

    const std::uint64_t ncan = read_checked_len(r);
    { for (std::uint64_t i = 0; i < ncan; ++i) st.cancelledRequests.insert(r.get_id<SchedulingRequestIdTag>()); }

    // cross-record integrity: selected candidate must be a known identity;
    // a committed/executing/active plan must carry commitment evidence.
    for (const auto& plan : st.plans) {
        bool known = false;
        for (const auto& c : st.candidateIdentities) if (c.candidateId == plan.selectedCandidate) { known = true; break; }
        if (!known) throw FabricError(ErrorCode::Corruption, "selected candidate not in candidate set");
        const auto s = plan.state;
        if ((s == PlacementLifecycleState::COMMITTED || s == PlacementLifecycleState::AWAITING_EXECUTION ||
             s == PlacementLifecycleState::ACTIVE) && !plan.hasCommitmentEvidence)
            throw FabricError(ErrorCode::Corruption, "committed placement without commitment evidence");
    }

    check_bytes(r);
    return st;
}



// --- public model codecs (protocol/worker/coordinator) ---------------
void encode_request_record(ByteWriter& w, const SchedulingRequest& r) { enc_request(w, r); }
SchedulingRequest decode_request_record(ByteReader& r) { return dec_request(r); }
void encode_plan_record(ByteWriter& w, const PlacementPlan& p) { enc_plan(w, p); }
PlacementPlan decode_plan_record(ByteReader& r) { return dec_plan(r); }

void encode_candidate_record(ByteWriter& w, const Candidate& c) {
    w.put_id(c.candidateId); w.put_id(c.candidateGeneration);
    w.put_id(c.sourceId); w.put_id(c.sourceBootId);
    w.put_id(c.workerId); w.put_id(c.workerBootId);
    w.put_u8(static_cast<std::uint8_t>(c.provenance));
    w.put_u32(static_cast<std::uint32_t>(c.bindings.size()));
    for (const auto& b : c.bindings) enc_binding(w, b);
    w.put_id(c.capacity.generation);
    w.put_u64(c.capacity.vramTotal.value()); w.put_u64(c.capacity.vramUsed.value());
    w.put_u64(c.capacity.vramAvailable.value()); w.put_u64(c.capacity.hostMemoryAvailable.value());
    w.put_u64(c.capacity.pinnedMemoryAvailable.value()); w.put_u32(c.capacity.cpuAvailable.value());
    w.put_u64(c.capacity.storageAvailable.value()); w.put_u64(c.capacity.nicBandwidthAvailable.value());
    w.put_bool(c.capacity.contiguousMemory);
    w.put_u8(static_cast<std::uint8_t>(c.capacity.provenance));
    w.put_id(c.health.generation); w.put_bool(c.health.ready);
    w.put_double(c.health.healthScore); enc_string(w, c.health.degradationReason);
    w.put_u8(static_cast<std::uint8_t>(c.health.provenance));
    w.put_id(c.reservation.generation); w.put_bool(c.reservation.protectedResource);
    w.put_u64(c.reservation.reservedBytes.value()); enc_string(w, c.reservation.reservationId);
    w.put_u8(static_cast<std::uint8_t>(c.reservation.provenance));
    w.put_id(c.congestion.generation); w.put_double(c.congestion.congestion);
    w.put_double(c.congestion.residualBandwidth); w.put_u64(c.congestion.bandwidthNominal.value());
    enc_string(w, c.congestion.bottleneck);
    w.put_u8(static_cast<std::uint8_t>(c.congestion.provenance));
    w.put_id(c.locality.topologyGeneration); w.put_u32(c.locality.numaDistance);
    w.put_u32(c.locality.pcieHops); w.put_bool(c.locality.sharesPcieRoot);
    w.put_u32(c.locality.nicDistance); w.put_u32(c.locality.storageDistance);
    enc_string(w, c.locality.topologyGroup);
    w.put_u8(static_cast<std::uint8_t>(c.locality.provenance));
    w.put_id(c.communication.planGeneration); w.put_double(c.communication.estimatedCost.value());
    w.put_double(c.communication.derivedPathCost.value()); w.put_double(c.communication.collectiveCost.value());
    w.put_bool(c.communication.pathFeasible); enc_string(w, c.communication.planId);
    w.put_u8(static_cast<std::uint8_t>(c.communication.provenance));
    w.put_id(c.residency.residencyGeneration); w.put_bool(c.residency.modelResident);
    w.put_bool(c.residency.adapterResident); w.put_bool(c.residency.stateLocal);
    w.put_u64(c.residency.moveBytes.value()); enc_string(w, c.residency.moveSource);
    w.put_u8(static_cast<std::uint8_t>(c.residency.provenance));
    w.put_id(c.policyGeneration);
    w.put_double(c.memoryMovementCost.value());
}

Candidate decode_candidate_record(ByteReader& r) {
    Candidate c;
    c.candidateId = r.get_id<CandidateIdTag>();
    c.candidateGeneration = r.get_id<CandidateGenerationTag>();
    c.sourceId = r.get_id<SourceIdTag>();
    c.sourceBootId = r.get_id<SourceBootIdTag>();
    c.workerId = r.get_id<WorkerIdTag>();
    c.workerBootId = r.get_id<WorkerBootIdTag>();
    const auto prov = r.get_u8();
    if (prov > static_cast<std::uint8_t>(Provenance::Unknown)) throw FabricError(ErrorCode::Corruption, "invalid provenance");
    c.provenance = static_cast<Provenance>(prov);
    const std::uint64_t nb = read_checked_len(r);
    c.bindings.reserve(nb);
    for (std::uint64_t i = 0; i < nb; ++i) c.bindings.push_back(dec_binding(r));
    c.capacity.generation = r.get_id<CapacityGenerationTag>();
    c.capacity.vramTotal = ByteCount(r.get_u64());
    c.capacity.vramUsed = ByteCount(r.get_u64());
    c.capacity.vramAvailable = ByteCount(r.get_u64());
    c.capacity.hostMemoryAvailable = ByteCount(r.get_u64());
    c.capacity.pinnedMemoryAvailable = ByteCount(r.get_u64());
    c.capacity.cpuAvailable = CoreCount(r.get_u32());
    c.capacity.storageAvailable = ByteCount(r.get_u64());
    c.capacity.nicBandwidthAvailable = ByteCount(r.get_u64());
    c.capacity.contiguousMemory = r.get_bool();
    const auto cprov = r.get_u8();
    if (cprov > static_cast<std::uint8_t>(Provenance::Unknown)) throw FabricError(ErrorCode::Corruption, "invalid provenance");
    c.capacity.provenance = static_cast<Provenance>(cprov);
    c.health.generation = r.get_id<HealthGenerationTag>();
    c.health.ready = r.get_bool();
    c.health.healthScore = r.get_double();
    c.health.degradationReason = r.get_string();
    const auto hprov = r.get_u8();
    if (hprov > static_cast<std::uint8_t>(Provenance::Unknown)) throw FabricError(ErrorCode::Corruption, "invalid provenance");
    c.health.provenance = static_cast<Provenance>(hprov);
    c.reservation.generation = r.get_id<ReservationGenerationTag>();
    c.reservation.protectedResource = r.get_bool();
    c.reservation.reservedBytes = ByteCount(r.get_u64());
    c.reservation.reservationId = r.get_string();
    const auto rprov = r.get_u8();
    if (rprov > static_cast<std::uint8_t>(Provenance::Unknown)) throw FabricError(ErrorCode::Corruption, "invalid provenance");
    c.reservation.provenance = static_cast<Provenance>(rprov);
    c.congestion.generation = r.get_id<CongestionGenerationTag>();
    c.congestion.congestion = r.get_double();
    c.congestion.residualBandwidth = r.get_double();
    c.congestion.bandwidthNominal = ByteCount(r.get_u64());
    c.congestion.bottleneck = r.get_string();
    const auto gprov = r.get_u8();
    if (gprov > static_cast<std::uint8_t>(Provenance::Unknown)) throw FabricError(ErrorCode::Corruption, "invalid provenance");
    c.congestion.provenance = static_cast<Provenance>(gprov);
    c.locality.topologyGeneration = r.get_id<TopologyGenerationTag>();
    c.locality.numaDistance = r.get_u32();
    c.locality.pcieHops = r.get_u32();
    c.locality.sharesPcieRoot = r.get_bool();
    c.locality.nicDistance = r.get_u32();
    c.locality.storageDistance = r.get_u32();
    c.locality.topologyGroup = r.get_string();
    const auto lprov = r.get_u8();
    if (lprov > static_cast<std::uint8_t>(Provenance::Unknown)) throw FabricError(ErrorCode::Corruption, "invalid provenance");
    c.locality.provenance = static_cast<Provenance>(lprov);
    c.communication.planGeneration = r.get_id<CommunicationPlanGenerationTag>();
    c.communication.estimatedCost = Cost(r.get_double());
    c.communication.derivedPathCost = Cost(r.get_double());
    c.communication.collectiveCost = Cost(r.get_double());
    c.communication.pathFeasible = r.get_bool();
    c.communication.planId = r.get_string();
    const auto mprov = r.get_u8();
    if (mprov > static_cast<std::uint8_t>(Provenance::Unknown)) throw FabricError(ErrorCode::Corruption, "invalid provenance");
    c.communication.provenance = static_cast<Provenance>(mprov);
    c.residency.residencyGeneration = r.get_id<ResidencyGenerationTag>();
    c.residency.modelResident = r.get_bool();
    c.residency.adapterResident = r.get_bool();
    c.residency.stateLocal = r.get_bool();
    c.residency.moveBytes = ByteCount(r.get_u64());
    c.residency.moveSource = r.get_string();
    const auto sprov = r.get_u8();
    if (sprov > static_cast<std::uint8_t>(Provenance::Unknown)) throw FabricError(ErrorCode::Corruption, "invalid provenance");
    c.residency.provenance = static_cast<Provenance>(sprov);
    c.policyGeneration = r.get_id<PolicyGenerationTag>();
    c.memoryMovementCost = Cost(r.get_double());
    return c;
}
}  // namespace fabric
