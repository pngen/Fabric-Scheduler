#include "fabric_scheduler/core/generations.hpp"
#include "fabric_scheduler/core/types.hpp"
#include "fabric_scheduler/core/units.hpp"
#include <string_view>
#include <iostream>
using namespace fabric;

// Checks that stay active in every configuration: assert() is compiled out
// under NDEBUG, which left this test without any check in a Release build.
#define REQUIRE(cond)                                                          \
    do {                                                                       \
        if (!(cond)) {                                                         \
            std::cerr << "REQUIRE failed at line " << __LINE__ << ": " #cond "\n"; \
            return 1;                                                          \
        }                                                                      \
    } while (0)
int main() {
    SchedulingRequestId id(1);
    CapacityGeneration cap(7);
    REQUIRE(id.is_valid());
    REQUIRE(cap.value() == 7);
    REQUIRE(cap == CapacityGeneration(7));
    REQUIRE(ByteCount(4_GiB).fits_in(8_GiB));
    REQUIRE(std::string_view(SchedulingRequestId::type_name()) == std::string_view("SchedulingRequestId"));
    REQUIRE(std::string_view(CoordinatorEpoch::type_name()) == std::string_view("CoordinatorEpoch"));
    std::cout << "core smoke ok\n";
    return 0;
}
