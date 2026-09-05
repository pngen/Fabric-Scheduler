#pragma once
// Deterministic bounded binary coding used by persistence and the protocol.
#include <cstdint>
#include <cstddef>
#include <string>
#include <vector>
#include <stdexcept>
#include <cstring>
#include <utility>
#include "fabric_scheduler/core/error.hpp"
#include "fabric_scheduler/core/id.hpp"

namespace fabric {

// ---------------------------------------------------------------------------
// ByteWriter - append-only little-endian deterministic encoder.
// ---------------------------------------------------------------------------
struct ByteWriter {
public:
    void put_u8(std::uint8_t v) { buf_.push_back(static_cast<std::byte>(v)); }
    void put_u16(std::uint16_t v) {
        put_u8(static_cast<std::uint8_t>(v & 0xFF));
        put_u8(static_cast<std::uint8_t>((v >> 8) & 0xFF));
    }
    void put_u32(std::uint32_t v) {
        put_u8(static_cast<std::uint8_t>(v & 0xFF));
        put_u8(static_cast<std::uint8_t>((v >> 8) & 0xFF));
        put_u8(static_cast<std::uint8_t>((v >> 16) & 0xFF));
        put_u8(static_cast<std::uint8_t>((v >> 24) & 0xFF));
    }
    void put_u64(std::uint64_t v) {
        for (int i = 0; i < 8; ++i) put_u8(static_cast<std::uint8_t>((v >> (8 * i)) & 0xFF));
    }
    void put_i32(std::int32_t v) { put_u32(static_cast<std::uint32_t>(v)); }
    void put_double(double d) {
        std::uint64_t bits = 0;
        static_assert(sizeof(bits) == sizeof(d), "double must be 8 bytes");
        std::memcpy(&bits, &d, sizeof(bits));
        put_u64(bits);
    }
    void put_bool(bool b) { put_u8(b ? 1 : 0); }
    void put_bytes(const void* data, std::size_t n) {
        const auto* p = static_cast<const std::byte*>(data);
        buf_.insert(buf_.end(), p, p + n);
    }
    // Length-prefixed string (u32 length + bytes). Bounded by caller.
    void put_string(const std::string& s) {
        put_u32(static_cast<std::uint32_t>(s.size()));
        put_bytes(s.data(), s.size());
    }
    template<typename T>
    void put_id(const StrongId<T>& id) { put_u64(id.value()); }

    const std::vector<std::byte>& data() const noexcept { return buf_; }
    std::vector<std::byte>&& take() noexcept { return std::move(buf_); }
    std::size_t size() const noexcept { return buf_.size(); }

private:
    std::vector<std::byte> buf_;
};

// ---------------------------------------------------------------------------
// ByteReader - bounds-checked deterministic decoder. Every read validates the
// remaining length and throws FabricError(Corruption) on truncation or on
// malformed length.
// ---------------------------------------------------------------------------
struct ByteReader {
public:
    explicit ByteReader(const std::vector<std::byte>& buf, std::size_t maxLen = 0)
        : buf_(buf), pos_(0), max_(maxLen == 0 ? buf.size() : maxLen) {
        if (max_ > buf_.size()) throw FabricError(ErrorCode::Truncation, "declared length exceeds buffer");
    }
    explicit ByteReader(const std::byte* data, std::size_t n)
        : buf_(data, data + n), pos_(0), max_(n) {}

    std::size_t remaining() const noexcept { return max_ - pos_; }
    std::size_t position() const noexcept { return pos_; }

    std::uint8_t get_u8() {
        require(1);
        return static_cast<std::uint8_t>(buf_[pos_++]);
    }
    std::uint16_t get_u16() {
        require(2);
        std::uint16_t v = static_cast<std::uint8_t>(buf_[pos_]);
        v |= static_cast<std::uint16_t>(static_cast<std::uint8_t>(buf_[pos_ + 1])) << 8;
        pos_ += 2; return v;
    }
    std::uint32_t get_u32() {
        require(4);
        std::uint32_t v = 0;
        for (int i = 0; i < 4; ++i) v |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(buf_[pos_ + i])) << (8 * i);
        pos_ += 4; return v;
    }
    std::uint64_t get_u64() {
        require(8);
        std::uint64_t v = 0;
        for (int i = 0; i < 8; ++i) v |= static_cast<std::uint64_t>(static_cast<std::uint8_t>(buf_[pos_ + i])) << (8 * i);
        pos_ += 8; return v;
    }
    std::int32_t get_i32() { return static_cast<std::int32_t>(get_u32()); }
    double get_double() {
        const std::uint64_t bits = get_u64();
        double d = 0.0;
        std::memcpy(&d, &bits, sizeof(d));
        return d;
    }
    bool get_bool() { return get_u8() != 0; }
    std::string get_string() {
        const std::uint32_t len = get_u32();
        if (len > kMaxString) throw FabricError(ErrorCode::OversizedFrame, "string length exceeds bound");
        require(len);
        std::string s(reinterpret_cast<const char*>(buf_.data() + pos_), len);
        pos_ += len;
        return s;
    }
    template<typename T>
    StrongId<T> get_id() { return StrongId<T>(get_u64()); }

    std::vector<std::byte> get_bytes(std::size_t n) {
        require(n);
        std::vector<std::byte> out(n);
        std::memcpy(out.data(), buf_.data() + pos_, n);
        pos_ += n;
        return out;
    }

    static constexpr std::size_t kMaxString = (1u << 20);  // 1 MiB per string

private:
    void require(std::size_t n) const {
        if (n > remaining()) throw FabricError(ErrorCode::Truncation, "truncated record");
    }
    std::vector<std::byte> buf_;
    std::size_t pos_;
    std::size_t max_;
};

}  // namespace fabric