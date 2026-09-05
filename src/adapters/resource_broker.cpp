#include "fabric_scheduler/adapters/resource_broker.hpp"
namespace fabric { namespace detail {
// ResourceBroker is an abstract interface injected by the deployment. This
// translation unit keeps the header self-contained for install/CTest.
void resource_broker_tu() noexcept {}
}}
