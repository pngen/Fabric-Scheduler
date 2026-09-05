#pragma once
// Strongly typed identities and generations.
//
// Every distinct identity (workload, resource, plan, epoch, ...) is its own
// compile-time type so that semantically distinct IDs and generations can never
// be silently interchanged. The underlying storage is a single u64 but the
// wrapper type is what gives each concept its meaning.
#include <cstdint>
#include <compare>
#include <string_view>
#include <functional>
#include <cstddef>

namespace fabric {

template<typename Tag>
struct IdTraits {};

template<typename Tag>
class StrongId {
public:
    using ValueType = std::uint64_t;

    constexpr StrongId() noexcept = default;
    constexpr explicit StrongId(ValueType v) noexcept : value_(v) {}

    constexpr ValueType value() const noexcept { return value_; }

    // An id is "valid" when it has been assigned a real non-zero ordinal.
    // Zero is reserved as the sentinel for "not yet assigned" / "none".
    constexpr bool is_valid() const noexcept { return value_ != 0; }
    constexpr explicit operator bool() const noexcept { return value_ != 0; }

    constexpr StrongId next() const noexcept { return StrongId(value_ + 1); }

    friend constexpr auto operator<=>(const StrongId&, const StrongId&) = default;

    static constexpr std::string_view type_name() noexcept { return IdTraits<Tag>::name(); }

private:
    ValueType value_ = 0;
};

}  // namespace fabric

template<typename Tag>
struct std::hash<fabric::StrongId<Tag>> {
    std::size_t operator()(const fabric::StrongId<Tag>& id) const noexcept {
        return std::hash<std::uint64_t>{}(id.value());
    }
};

#define FABRIC_STRONG_ID(Name)                                               \
    namespace fabric {                                                     \
    struct Name##Tag;                                                      \
    template<> struct IdTraits<Name##Tag> {                                \
        static constexpr std::string_view name() noexcept {                \
            return #Name;                                                  \
        }                                                                  \
    };                                                                     \
    using Name = StrongId<Name##Tag>;                                      \
    }
