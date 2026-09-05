#pragma once
// Versioned, integrity-checked persistence of durable scheduler state.
// Dynamic evidence (live capacity, congestion, worker liveness, resource health)
// is deliberately NOT persisted as current; it recovers conservatively.
#include "fabric_scheduler/core/generations.hpp"
#include "fabric_scheduler/request.hpp"
#include "fabric_scheduler/candidate.hpp"
#include "fabric_scheduler/placement.hpp"
#include <cstdint>
#include <iosfwd>
#include <string>
#include <vector>
#include <unordered_set>

namespace fabric {

inline constexpr std::uint32_t kPersistenceMagic      = 0x4642u;  // "FB" (Fabric)
inline constexpr std::uint32_t kPersistenceFormatVersion = 1;
inline constexpr std::uint8_t  kPersistenceFlags      = 0x00;

// Recoverable identity of a known candidate. Live evidence is not carried.
struct CandidateIdentity {
    CandidateId candidateId;
    CandidateGeneration candidateGeneration;
    SourceId sourceId;
    SourceBootId sourceBootId;
    WorkerId workerId;
    WorkerBootId workerBootId;
};

// The durable, versioned scheduler snapshot that is written to disk.
struct Persisted {
    std::uint32_t formatVersion{kPersistenceFormatVersion};
    CoordinatorEpoch epoch{1};
    AuthorityGeneration authority{1};
    std::uint64_t nextPlanId{1};
    std::uint64_t nextSubmitOrdinal{1};
    std::uint64_t nextEventOrdinal{1};
    std::uint64_t ingestWatermark{0};
    std::vector<SchedulingRequest> requests;
    std::vector<PlacementPlan> plans;
    std::vector<CandidateIdentity> candidateIdentities;
    std::vector<std::string> supersessionHistory;
    std::unordered_set<SchedulingRequestId> cancelledRequests;
};

// Save in the deterministic versioned format. Throws FabricError(Corruption)
// on a malformed snapshot. Writes nothing on failure.
void persist_save(const Persisted& state, std::ostream& out);

// Load and validate. Rejects: bad magic, unsupported version, truncation,
// checksum mismatch, malformed lengths, oversized counts, invalid enum,
// duplicate ids, trailing garbage. Returns the validated snapshot.
Persisted persist_load(std::istream& in);

// Compute the FNV-1a 64-bit digest over a byte span. Used for the integrity
// footer and by the protocol framing.
std::uint64_t fnv1a64_digest(const std::byte* data, std::size_t n);

// Public model codecs reused by the framed protocol and worker/coordinator.
struct ByteWriter;
struct ByteReader;
void encode_request_record(ByteWriter& w, const SchedulingRequest& r);
SchedulingRequest decode_request_record(ByteReader& r);
void encode_candidate_record(ByteWriter& w, const Candidate& c);
Candidate decode_candidate_record(ByteReader& r);
void encode_plan_record(ByteWriter& w, const PlacementPlan& p);
PlacementPlan decode_plan_record(ByteReader& r);

}  // namespace fabric