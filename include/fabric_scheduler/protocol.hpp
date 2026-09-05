#pragma once
// Compact versioned framed protocol for the reference distributed coordinator.
// Framing is transport-independent (pure byte-level encode/decode); the TCP
// channel lives in net.hpp. Frames carry magic/version/type, a bounded payload,
// and an FNV-1a integrity digest. Every length is checked; malformed enums,
// malformed lengths, oversized frames, truncation, and checksum mismatch are
// rejected deterministically.
#include <cstdint>
#include <cstddef>
#include <vector>
#include "fabric_scheduler/core/error.hpp"
#include "fabric_scheduler/coding.hpp"
#include "fabric_scheduler/persistence.hpp"

namespace fabric {

inline constexpr std::uint32_t kFrameMagic = 0x46414252u;  // "FABR"
inline constexpr std::uint8_t  kFrameVersion = 1;
inline constexpr std::uint32_t kFrameHeaderSize = 12;  // magic(4) ver(1) type(1) flags(1) reserved(1) len(4)
inline constexpr std::uint32_t kMaxFramePayload = (16u << 20);  // 16 MiB bound

enum class MessageType : std::uint8_t {
    HELLO = 0,
    REGISTER = 1,
    PUBLISH_RESOURCE = 2,
    PUBLISH_TOPOLOGY = 3,
    PUBLISH_CAPACITY = 4,
    PUBLISH_RESERVATION_VIEW = 5,
    PUBLISH_CONGESTION = 6,
    PUBLISH_HEALTH = 7,
    SUBMIT_WORKLOAD = 8,
    QUERY_PLACEMENT = 9,
    PLACEMENT_PLAN = 10,
    COMMIT_REQUEST = 11,
    COMMIT_RESULT = 12,
    REVALIDATE = 13,
    SUPERSEDE = 14,
    CANCEL = 15,
    EXECUTION_HANDOFF = 16,
    EXECUTION_RESULT = 17,
    SAVE = 18,
    SHUTDOWN = 19,
    ERROR = 20,
    EXECUTE = 21
};

inline constexpr const char* to_string(MessageType m) noexcept {
    switch (m) {
        case MessageType::HELLO: return "HELLO";
        case MessageType::REGISTER: return "REGISTER";
        case MessageType::PUBLISH_RESOURCE: return "PUBLISH_RESOURCE";
        case MessageType::PUBLISH_TOPOLOGY: return "PUBLISH_TOPOLOGY";
        case MessageType::PUBLISH_CAPACITY: return "PUBLISH_CAPACITY";
        case MessageType::PUBLISH_RESERVATION_VIEW: return "PUBLISH_RESERVATION_VIEW";
        case MessageType::PUBLISH_CONGESTION: return "PUBLISH_CONGESTION";
        case MessageType::PUBLISH_HEALTH: return "PUBLISH_HEALTH";
        case MessageType::SUBMIT_WORKLOAD: return "SUBMIT_WORKLOAD";
        case MessageType::QUERY_PLACEMENT: return "QUERY_PLACEMENT";
        case MessageType::PLACEMENT_PLAN: return "PLACEMENT_PLAN";
        case MessageType::COMMIT_REQUEST: return "COMMIT_REQUEST";
        case MessageType::COMMIT_RESULT: return "COMMIT_RESULT";
        case MessageType::REVALIDATE: return "REVALIDATE";
        case MessageType::SUPERSEDE: return "SUPERSEDE";
        case MessageType::CANCEL: return "CANCEL";
        case MessageType::EXECUTION_HANDOFF: return "EXECUTION_HANDOFF";
        case MessageType::EXECUTION_RESULT: return "EXECUTION_RESULT";
        case MessageType::SAVE: return "SAVE";
        case MessageType::SHUTDOWN: return "SHUTDOWN";
        case MessageType::ERROR: return "ERROR";
        case MessageType::EXECUTE: return "EXECUTE";
    }
    return "UNKNOWN";
}

// One decoded frame's header metadata (payload follows separately).
struct FrameHeader {
    MessageType type{MessageType::ERROR};
    std::uint32_t payloadLen{0};
    std::uint8_t flags{0};
};

// Pure framing utilities.
class FrameCodec {
public:
    // Build a complete frame: header + payload + digest. Rejects oversized payload.
    static std::vector<std::byte> encode_frame(MessageType type,
                                               const std::vector<std::byte>& payload,
                                               std::uint8_t flags = 0);

    // Parse and validate a 12-byte header. Returns type + payload length.
    static FrameHeader parse_header(const std::byte* header12);

    // Validate the payload length bound (must be called before reading payload).
    static void check_payload_length(std::uint32_t len);

    // Validate the trailing 8-byte FNV-1a digest over [header version..payload].
    static void check_digest(const std::byte* header12, const std::byte* payload,
                             std::uint32_t len, const std::byte* digest8);
};

}  // namespace fabric