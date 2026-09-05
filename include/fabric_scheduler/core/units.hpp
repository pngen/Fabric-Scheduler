#pragma once
// Strong units. Quantities that mean different physical things are different
// types, so a byte count can never be silently multiplied by a latency.
#include <cstdint>
#include <compare>
#include <string_view>
#include <functional>
#include <cmath>

namespace fabric {

// Count of discrete objects (cores, devices, replicas). Always integral.
class CoreCount {
public:
    using ValueType = std::uint32_t;
    constexpr CoreCount() noexcept = default;
    constexpr explicit CoreCount(ValueType v) noexcept : value_(v) {}
    constexpr ValueType value() const noexcept { return value_; }
    constexpr explicit operator bool() const noexcept { return value_ != 0; }
    friend constexpr auto operator<=>(const CoreCount&, const CoreCount&) = default;
private:
    ValueType value_ = 0;
};

// A quantity of bytes. All memory/storage sizes use this.
class ByteCount {
public:
    using ValueType = std::uint64_t;
    constexpr ByteCount() noexcept = default;
    constexpr explicit ByteCount(ValueType v) noexcept : value_(v) {}
    constexpr ValueType value() const noexcept { return value_; }
    constexpr explicit operator bool() const noexcept { return value_ != 0; }
    constexpr bool operator==(const ByteCount&) const = default;

    friend constexpr ByteCount operator+(ByteCount a, ByteCount b) noexcept {
        return ByteCount(a.value_ + b.value_);
    }
    friend constexpr ByteCount operator-(ByteCount a, ByteCount b) noexcept {
        return ByteCount(a.value_ - b.value_);
    }
    constexpr bool fits_in(ByteCount capacity) const noexcept { return value_ <= capacity.value_; }
private:
    ValueType value_ = 0;
};

// Bytes with a binary (1 GiB = 2^30) multiplier.
constexpr inline ByteCount operator""_B(std::uint64_t v)  noexcept { return ByteCount(v); }
constexpr inline ByteCount operator""_KiB(std::uint64_t v) noexcept { return ByteCount(v * 1024u); }
constexpr inline ByteCount operator""_MiB(std::uint64_t v) noexcept { return ByteCount(v * 1024u * 1024u); }
constexpr inline ByteCount operator""_GiB(std::uint64_t v) noexcept { return ByteCount(v * 1024ull * 1024ull * 1024ull); }

// Time durations in nanoseconds (latency, duration, horizon).
class Duration {
public:
    using ValueType = std::uint64_t;
    constexpr Duration() noexcept = default;
    constexpr explicit Duration(ValueType ns) noexcept : ns_(ns) {}
    constexpr ValueType nanoseconds() const noexcept { return ns_; }
    constexpr explicit operator bool() const noexcept { return ns_ != 0; }
    friend constexpr auto operator<=>(const Duration&, const Duration&) = default;
    constexpr bool operator==(const Duration&) const = default;
private:
    ValueType ns_ = 0;
};
constexpr inline Duration operator""_ms(std::uint64_t v) noexcept { return Duration(v * 1000000ull); }
constexpr inline Duration operator""_s(std::uint64_t v) noexcept { return Duration(v * 1000000000ull); }

// Normalized dimensionless cost in [0, +inf). Lower is better. Used only after
// all hard/eligibility constraints have passed.
class Cost {
public:
    using ValueType = double;
    constexpr Cost() noexcept = default;
    constexpr explicit Cost(double v) noexcept : v_(v) {}
    constexpr double value() const noexcept { return v_; }
    bool is_finite() const noexcept { return std::isfinite(v_); }
    constexpr bool is_negative() const noexcept { return v_ < 0.0; }
    friend constexpr auto operator<=>(const Cost&, const Cost&) = default;
    constexpr bool operator==(const Cost&) const = default;
    constexpr Cost& operator+=(const Cost& o) noexcept { v_ += o.v_; return *this; }
private:
    double v_ = 0.0;
};

}  // namespace fabric

template<>
struct std::hash<fabric::ByteCount> {
    std::size_t operator()(const fabric::ByteCount& b) const noexcept {
        return std::hash<std::uint64_t>{}(b.value());
    }
};

template<>
struct std::hash<fabric::CoreCount> {
    std::size_t operator()(const fabric::CoreCount& c) const noexcept {
        return std::hash<std::uint32_t>{}(c.value());
    }
};

template<>
struct std::hash<fabric::Duration> {
    std::size_t operator()(const fabric::Duration& d) const noexcept {
        return std::hash<std::uint64_t>{}(d.nanoseconds());
    }
};
