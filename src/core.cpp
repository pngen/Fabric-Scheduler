#include "fabric_scheduler/core/id.hpp"
#include "fabric_scheduler/core/generations.hpp"
#include "fabric_scheduler/core/types.hpp"
#include "fabric_scheduler/core/units.hpp"
#include "fabric_scheduler/core/error.hpp"
#include "fabric_scheduler/core/result.hpp"
#include "fabric_scheduler/version.hpp"

namespace fabric::detail {
void core_header_compile_check() noexcept {
    (void)sizeof(StrongId<SchedulingRequestIdTag>);
}
}  // namespace fabric::detail
