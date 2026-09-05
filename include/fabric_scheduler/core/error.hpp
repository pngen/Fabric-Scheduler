#pragma once
#include <stdexcept>
#include <string>
#include <cstdint>
#include <utility>

namespace fabric {

enum class ErrorCode : std::uint16_t {
    InvalidArgument,
    InvalidDemand,
    NegativeQuantity,
    NonFinite,
    Overflow,
    ImpossibleTopology,
    EmptyRequiredCandidateSet,
    InvalidCountRange,
    ImpossibleDuration,
    MalformedLocalityConstraint,
    InvalidPolicyGeneration,
    IllegalLifecycleTransition,
    StaleGeneration,
    StaleAuthority,
    UnknownState,
    IntegrityError,
    UnsupportedVersion,
    Corruption,
    ProtocolError,
    MalformedFrame,
    OversizedFrame,
    ChecksumMismatch,
    Truncation,
    ResourceCommitFailure,
    Cancelled,
    Superseded,
    Internal,
    NotImplemented,
    RevalidationRequired
};

inline constexpr const char* to_string(ErrorCode c) noexcept {
    switch (c) {
        case ErrorCode::InvalidArgument:              return "INVALID_ARGUMENT";
        case ErrorCode::InvalidDemand:                return "INVALID_DEMAND";
        case ErrorCode::NegativeQuantity:             return "NEGATIVE_QUANTITY";
        case ErrorCode::NonFinite:                    return "NON_FINITE";
        case ErrorCode::Overflow:                     return "OVERFLOW";
        case ErrorCode::ImpossibleTopology:           return "IMPOSSIBLE_TOPOLOGY";
        case ErrorCode::EmptyRequiredCandidateSet:    return "EMPTY_REQUIRED_CANDIDATE_SET";
        case ErrorCode::InvalidCountRange:            return "INVALID_COUNT_RANGE";
        case ErrorCode::ImpossibleDuration:           return "IMPOSSIBLE_DURATION";
        case ErrorCode::MalformedLocalityConstraint:  return "MALFORMED_LOCALITY";
        case ErrorCode::InvalidPolicyGeneration:      return "INVALID_POLICY_GENERATION";
        case ErrorCode::IllegalLifecycleTransition:   return "ILLEGAL_LIFECYCLE_TRANSITION";
        case ErrorCode::StaleGeneration:              return "STALE_GENERATION";
        case ErrorCode::StaleAuthority:               return "STALE_AUTHORITY";
        case ErrorCode::UnknownState:                 return "UNKNOWN_STATE";
        case ErrorCode::IntegrityError:               return "INTEGRITY_ERROR";
        case ErrorCode::UnsupportedVersion:           return "UNSUPPORTED_VERSION";
        case ErrorCode::Corruption:                   return "CORRUPTION";
        case ErrorCode::ProtocolError:                return "PROTOCOL_ERROR";
        case ErrorCode::MalformedFrame:               return "MALFORMED_FRAME";
        case ErrorCode::OversizedFrame:               return "OVERSIZED_FRAME";
        case ErrorCode::ChecksumMismatch:             return "CHECKSUM_MISMATCH";
        case ErrorCode::Truncation:                   return "TRUNCATION";
        case ErrorCode::ResourceCommitFailure:        return "RESOURCE_COMMIT_FAILURE";
        case ErrorCode::Cancelled:                    return "CANCELLED";
        case ErrorCode::Superseded:                   return "SUPERSEDED";
        case ErrorCode::Internal:                     return "INTERNAL";
        case ErrorCode::NotImplemented:               return "NOT_IMPLEMENTED";
        case ErrorCode::RevalidationRequired:         return "REVALIDATION_REQUIRED";
    }
    return "UNKNOWN";
}

// Thrown for malformed/invalid input and invariant violations. These are fatal
// to the unit of work that produced them.
class FabricError final : public std::runtime_error {
public:
    FabricError(ErrorCode code, std::string message)
        : std::runtime_error(std::move(message)), code_(code) {}
    ErrorCode code() const noexcept { return code_; }
private:
    ErrorCode code_;
};

}  // namespace fabric
