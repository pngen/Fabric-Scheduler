#include "fabric_scheduler/core/generations.hpp"
#include "fabric_scheduler/core/types.hpp"
#include "fabric_scheduler/core/units.hpp"
#include <cassert>
#include <string_view>
#include <iostream>
using namespace fabric;
int main() {
    SchedulingRequestId id(1);
    CapacityGeneration cap(7);
    assert(id.is_valid());
    assert(cap.value() == 7);
    assert(cap == CapacityGeneration(7));
    assert(ByteCount(4_GiB).fits_in(8_GiB));
    assert(std::string_view(SchedulingRequestId::type_name()) == std::string_view("SchedulingRequestId"));
    assert(std::string_view(CoordinatorEpoch::type_name()) == std::string_view("CoordinatorEpoch"));
    std::cout << "core smoke ok\n";
    return 0;
}
