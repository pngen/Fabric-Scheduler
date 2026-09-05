#include "fabric_scheduler/protocol.hpp"
#include "fabric_scheduler/core/error.hpp"
#include <cstring>

namespace fabric {

std::vector<std::byte> FrameCodec::encode_frame(MessageType type,
                                                 const std::vector<std::byte>& payload,
                                                 std::uint8_t flags) {
    if (payload.size() > kMaxFramePayload) throw FabricError(ErrorCode::OversizedFrame, "frame payload exceeds bound");
    ByteWriter w;
    w.put_u32(kFrameMagic);
    w.put_u8(kFrameVersion);
    w.put_u8(static_cast<std::uint8_t>(type));
    w.put_u8(flags);
    w.put_u8(0);
    w.put_u32(static_cast<std::uint32_t>(payload.size()));
    const auto hd = w.take();

    std::vector<std::byte> out;
    out.reserve(kFrameHeaderSize + payload.size() + 8);
    out.insert(out.end(), hd.begin(), hd.end());
    out.insert(out.end(), payload.begin(), payload.end());

    // Digest over header[version..len] + payload (starts at byte 4).
    const std::uint64_t digest = fnv1a64_digest(out.data() + 4, out.size() - 4);
    for (int i = 0; i < 8; ++i) out.push_back(static_cast<std::byte>((digest >> (8 * i)) & 0xFF));
    return out;
}

FrameHeader FrameCodec::parse_header(const std::byte* header12) {
    ByteReader r(header12, kFrameHeaderSize);
    const std::uint32_t magic = r.get_u32();
    if (magic != kFrameMagic) throw FabricError(ErrorCode::MalformedFrame, "bad frame magic");
    const std::uint8_t version = r.get_u8();
    if (version != kFrameVersion) throw FabricError(ErrorCode::UnsupportedVersion, "unsupported frame version");
    const std::uint8_t type = r.get_u8();
    if (type > static_cast<std::uint8_t>(MessageType::ERROR)) throw FabricError(ErrorCode::MalformedFrame, "invalid message type");
    FrameHeader h;
    h.type = static_cast<MessageType>(type);
    h.flags = r.get_u8();
    (void)r.get_u8();
    h.payloadLen = r.get_u32();
    check_payload_length(h.payloadLen);
    return h;
}

void FrameCodec::check_payload_length(std::uint32_t len) {
    if (len > kMaxFramePayload) throw FabricError(ErrorCode::OversizedFrame, "frame payload exceeds bound");
}

void FrameCodec::check_digest(const std::byte* header12, const std::byte* payload,
                              std::uint32_t len, const std::byte* digest8) {
    std::vector<std::byte> span;
    span.reserve(8 + len);
    span.insert(span.end(), header12 + 4, header12 + kFrameHeaderSize);
    span.insert(span.end(), payload, payload + len);
    const std::uint64_t computed = fnv1a64_digest(span.data(), span.size());
    ByteReader dr(digest8, 8);
    const std::uint64_t stored = dr.get_u64();
    if (computed != stored) throw FabricError(ErrorCode::ChecksumMismatch, "frame checksum mismatch");
}

}  // namespace fabric