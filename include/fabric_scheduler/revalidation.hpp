#pragma once
#include "fabric_scheduler/placement.hpp"
#include <vector>
#include <string>

namespace fabric {

// Revalidate a plan against a current generation fingerprint. Returns which
// generation moved and the resulting lifecycle state. The plan must not be
// silently patched with new evidence - prefer a new generation.
RevalidationResult revalidate_plan(const PlacementPlan& plan,
                                   const GenerationFingerprint& current);

}  // namespace fabric
