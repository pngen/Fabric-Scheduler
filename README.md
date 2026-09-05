# Fabric Scheduler

**Fabric Scheduler is an open-source, vendor-neutral C++20 topology-aware scheduling runtime for placing AI workloads across accelerators, CPUs, memory, NICs, storage, and communication fabrics using capacity, locality, congestion, reservations, and communication cost.**

**It answers one systems question:**

**Where should this work run now, given current capacity, topology, reservations, congestion, locality, communication cost, and policy, and which placement decision remains authoritative as infrastructure state changes?**

---

## The problem

Resource availability alone does not determine a good placement. A workload may *technically fit* while still being a poor placement because of remote NUMA memory, weak PCIe locality, congested transfer paths, NIC or storage distance, fragmented accelerator groups, reservations, expected capacity pressure, health degradation, placement interference, incompatible accelerator capability, stale topology or capacity evidence, expensive checkpoint/state movement, poor failure-domain concentration, or resource generations changing between planning and execution.

Fabric Scheduler turns those facts into an explicit, inspectable, generation-bound placement decision. It distinguishes:

**candidate discovery -> eligibility filtering -> cost evaluation -> deterministic ranking -> placement plan -> authority validation -> commit intent -> execution handoff -> revalidation -> supersession**

The core doctrine is:

> **A resource existing does not make it a valid candidate. A candidate being valid does not make it optimal. A placement plan is not a resource reservation. A scheduler decision is not an execution attempt. A high score is not authority. A stale placement must not launch fresh work.**

So scheduling remains **constraint-correct first, deterministic second, cost-aware third, authority-bound always.**

The defining thesis:

> **Scheduling is not choosing the first resource with enough free capacity. It is selecting the generation-valid placement whose compute, memory, locality, topology, communication, congestion, reservation, residency, and failure-domain constraints jointly make the workload safe and worthwhile to run there now.**

---

## Systems boundary

Fabric Scheduler sits above physical/resource evidence and below execution/workload launch. It owns **topology-aware workload placement selection**. Adjacent runtimes own their own concerns:

- **Resource Broker** owns resource arbitration.
- **Reservation Fabric** owns future commitments.
- **Capacity Fabric** models current/future capacity.
- **Fragmentation Governor** governs stranded-capacity remediation.
- **Congestion Fabric** governs live traffic congestion and backpressure evidence.
- topology/NUMA/PCIe/fleet runtimes provide physical facts.
- **Communication Planner** computes communication plans.
- **Collective Scheduler** orders competing collectives.
- **Workload Fabric** owns workload lifecycle.
- **Execution Fabric** owns execution-attempt authority.
- **Preemption Fabric** owns safe interruption.
- **Dependency Fabric** owns dependency readiness.

Fabric Scheduler owns workload placement requests, candidate construction, hard eligibility filtering, topology-aware grouping, locality/congestion/communication cost, reservation and capacity awareness, placement ranking, deterministic tie-breaking, placement-plan generations, placement authority, revalidation, supersession, fallback ordering, explainability, placement-history evidence, and bounded placement intent for adjacent runtimes. It deliberately does **not** reimplement resource arbitration, execution, communication planning, topology crawling, workload management, or global optimization.

---

## Strongly typed identities and generations

Every identity and generation is its own compile-time type (`StrongId<Tag>`), so a `TopologyGeneration` can never be passed where a `CapacityGeneration` is required. The model includes (among others) `SchedulingRequestId/Generation`, `SchedulingDecisionId/Generation`, `PlacementPlanId/Generation`, `CandidateId/Generation`, `WorkloadId/Generation`, `ExecutionId/Generation`, `AttemptId/Generation`, `ResourceId/Generation`, `DeviceId/Generation`, `NodeId/Generation`, `MemoryDomainId/Generation`, `NicId/Generation`, `StorageEndpointId/Generation`, `LinkId/Generation`, `TopologyGeneration`, `CapabilityGeneration`, `HealthGeneration`, `CapacityGeneration`, `ReservationGeneration`, `CongestionGeneration`, `CommunicationPlanGeneration`, `ResidencyGeneration`, `DependencyGeneration`, `PolicyGeneration`, `CoordinatorEpoch`, `WorkerId`, `WorkerBootId`, `SourceId`, `SourceBootId`, `AuthorityGeneration`, and `RevalidationGeneration`.

A stale `TopologyGeneration` does not authorize a placement. A stale `CapacityGeneration` does not prove fit. A stale `CongestionGeneration` does not influence ranking as if fresh. A stale `ReservationGeneration` does not protect or free resources. A stale `WorkerBootId` does not publish current candidate evidence. A stale `PlacementGeneration` does not launch or mutate execution.

---

## Workload scheduling request

A `SchedulingRequest` carries a `DemandProfile` with workload/demand identity and generation, accelerator requirements (capability class, minimum/target/maximum count, per-device and aggregate VRAM, contiguous-memory requirement), CPU and pinned-memory requirements, storage and network requirements, communication pattern and bytes, model/adapter/state residency requirements, failure-domain constraints, expected execution duration, latency sensitivity, throughput objective, external priority/SLO/policy generations, maximum movement cost, and provenance. `DemandProfile::validate()` rejects negative/non-finite/overflowing quantities, impossible topologies, invalid accelerator count ranges, impossible durations, malformed locality constraints, and duplicate locality kinds.

---

## Candidate model

A `Candidate` is a first-class, generation-bound bundle of device/secondary bindings plus the evidence used to decide eligibility and rank. Each candidate binds resource identities, resource generations, the topology/capacity/reservation/congestion/capability/health/residency/communication-plan generations it depends on, and the `WorkerBootId`/`SourceBootId` provenance. Candidates never hold a hidden mutable reference to external evidence.

---

## Hard eligibility filtering

Hard constraints run before ranking and are executed by `HardFilter`. A candidate that fails a hard constraint never survives because of a good score. Outcomes include `ELIGIBLE`, `REJECT_CAPACITY`, `REJECT_MEMORY`, `REJECT_CONTIGUITY`, `REJECT_CAPABILITY`, `REJECT_HEALTH`, `REJECT_TOPOLOGY`, `REJECT_NUMA`, `REJECT_PCIE`, `REJECT_NETWORK`, `REJECT_STORAGE`, `REJECT_RESERVATION`, `REJECT_DEPENDENCY`, `REJECT_COMPATIBILITY`, `REJECT_FAILURE_DOMAIN`, `REJECT_POLICY`, `REVALIDATION_REQUIRED`, `INSUFFICIENT_EVIDENCE`, and `UNKNOWN`. `UNKNOWN` never becomes `ELIGIBLE`; a candidate with unknown capacity/health evidence is `INSUFFICIENT_EVIDENCE`.

---

## Deterministic ranking

Only eligible candidates are ranked. Ranking uses named, inspectable factors (accelerator fit, resource/VRAM/CPU/pinned-memory headroom, topology/NUMA/PCIe/NIC/storage locality, communication cost, congestion penalty, residual bandwidth, model/adapter residency, reusable-state locality, checkpoint movement cost, startup cost, reservation fit, failure-domain diversification, health margin, fragmentation impact, external priority, migration/recompute cost, estimated queueing, policy preference). Each factor carries an explicit value, weight, and provenance (`MEASURED`, `REPORTED`, `DERIVED`, `ESTIMATED`, `FORECAST`, `SYNTHETIC`, `UNKNOWN`); `UNKNOWN` is never treated as a known favorable value.

For equal weighted score the comparator breaks ties by: fewer unknowns, stronger evidence, lower movement cost, more headroom, lower congestion, more local resources, then stable strong-`CandidateId` ordering. No unordered-container iteration is used. Identical authoritative inputs produce identical ordering; insertion-order permutations do not change the result.

---

## Placement plan and lifecycle

A placement decision produces an explicit `PlacementPlan` with its ID/generation, the selected candidate, a deterministic fallback list, exact resource bindings, resource claims, communication-plan reference, movement/residency prerequisites, the generation fingerprint the plan depends on, rejected alternatives and reasons, policy and authority generations, and a guarded lifecycle. States include `REQUESTED`, `DISCOVERING`, `FILTERING`, `RANKING`, `PLAN_READY`, `AWAITING_RESOURCE_COMMIT`, `COMMITTED`, `AWAITING_EXECUTION`, `ACTIVE`, `REVALIDATION_REQUIRED`, `SUPERSEDED`, `CANCELLED`, `FAILED`, and `RETIRED`. Illegal transitions are rejected (`REQUESTED -> ACTIVE`, `SUPERSEDED -> ACTIVE`, `PLAN_READY -> COMMITTED` all fail).

Atomic multi-resource commitment is modeled as **select candidate -> request provisional commitment -> verify -> commit or fail cleanly**. If the final resource claim fails, there is no execution launch, no false `COMMITTED` state, and the plan moves to `REVALIDATION_REQUIRED` so a fresh pass can evaluate fallback under current evidence.

---

## Fallback, revalidation, supersession, cancellation

Fallback candidates are not promoted from an old ranking by assumption; a fallback must be revalidated against current resource/topology/capacity/reservation/congestion/compatibility generations before use. Revalidation is mandatory before resource commit, execution handoff, retry/fallback, migration restart, and recovered-plan activation; a plan whose generation fingerprint diverges becomes `REVALIDATION_REQUIRED` and prefers a new generation over silently patching old evidence. A newer request/plan supersedes older unresolved plans for the same authoritative scope; superseded plans cannot commit resources, launch work, or publish success. Cancellation stops new candidate/plan work, rejects stale commit completion and execution handoff, releases provisional external claims through the owning adapter, and remains cancelled across persistence/recovery.

---

## Adjacent runtime interfaces

Fabric Scheduler consumes narrow interfaces for Resource Broker (atomic bundle commit/release), Reservation view, Capacity view, Congestion view, Communication Planner (path cost/feasibility), Residency/state-locality, Dependency readiness, and Execution Fabric (fresh execution authority). Reference adapters are provided so the runtime can be exercised standalone; they are deterministic and never fabricate a measured value.

---

## Persistence and conservative recovery

Durable scheduler state is persisted in a versioned binary format with magic, format version, flags, a bounded payload, and an FNV-1a 64-bit integrity digest. Loading rejects bad magic, unsupported versions, truncation, checksum mismatch, malformed/oversized lengths, invalid enums, duplicate IDs, plan ID regression, a selected candidate not in the candidate set, a committed placement without commitment evidence, and trailing garbage. Dynamic evidence (live capacity, congestion, health, worker liveness, actual ownership) is deliberately not persisted as current: after recovery, non-terminal plans become `REVALIDATION_REQUIRED`, candidate sets are cleared (requiring re-publication), and a fresh coordinator epoch/authority is enforced.

---

## Distributed reference deployment

A reference multiplexer runs over real framed loopback TCP with independent OS processes: one coordinator, workers, and a request controller. The protocol is compact, versioned, framed with magic/version/type/flags/length, a bounded payload, and an FNV-1a digest, and handles partial reads/writes, malformed frames, oversized payloads, and checksum mismatch. Messages include `HELLO`, `REGISTER`, `PUBLISH_RESOURCE`, `PUBLISH_TOPOLOGY`, `PUBLISH_CAPACITY`, `PUBLISH_RESERVATION_VIEW`, `PUBLISH_CONGESTION`, `PUBLISH_HEALTH`, `SUBMIT_WORKLOAD`, `QUERY_PLACEMENT`, `PLACEMENT_PLAN`, `COMMIT_REQUEST`, `COMMIT_RESULT`, `REVALIDATE`, `SUPERSEDE`, `CANCEL`, `EXECUTION_HANDOFF`, `EXECUTION_RESULT`, `SAVE`, `SHUTDOWN`, and `ERROR`.

The proof exercises registration, candidate publication, deterministic scheduling (local candidate A wins), atomic commit, execution handoff, a real OS worker-death (process killed, worker boot fenced, old plan `REVALIDATION_REQUIRED`, re-schedule picks B), stale `WorkerBootId` replay rejection, and coordinator restart/recovery.

---

## Real hardware, real evidence

**RTX 5090 placement-gated CUDA proof.** On the actual host, Fabric Scheduler discovers the **NVIDIA GeForce RTX 5090** (compute capability 12.0, ~34 GB total / ~32.5 GB free at test time), publishes *measured* device/capability/memory evidence, hard-filters incompatible candidates, selects the physical RTX 5090, acquires a resource-commit, receives a fresh execution handoff, and then runs **real** CUDA work: `cudaMalloc`, H2D, a real kernel, synchronization, D2H, CPU parity verification, and `cudaFree`. Device memory returns to its measured baseline. A stale-placement proof advances the device/capacity generation, rejects the old handoff **before** any CUDA allocation/kernel launch, then produces a fresh decision that runs real CUDA work successfully.

**Host storage / state-locality.** Real local file state is used to establish a derived movement cost; the scheduler prefers a state-local candidate when all hard constraints and policy factors are equal.

**Loopback TCP communication cost.** Real loopback TCP timed transfers supply an honest `LOOPBACK TCP` communication-cost input; remote-path costs use deterministic *synthetic* values for topology that does not exist on this host.

## Synthetic scenarios (explicitly) and limitations

This host exposes one physical GPU and no NVLink/NVSwitch, RDMA, GPUDirect, multi-node, or multi-NIC physical topology. Therefore multi-GPU, remote-topology, PCIe-root contention, and rack/failure-domain scenarios use **deterministic SYNTHETIC** candidates and are clearly labeled as such; they are not presented as measured physical facts. The scheduler never infers physical connectivity from device naming. It does not control CUDA driver placement beyond the explicit application-level device selection used by the proof.

---

## Build, test, install

Requires CMake 3.20+ and a C++20 compiler. On MSVC the build uses `/W4 /WX` (zero warnings required; Debug and Release both pass). AddressSanitizer is available via `-DFABRIC_ENABLE_ASAN=ON`. No test uses a timeout.

```sh
cmake -S . -B build -G "Visual Studio 17 2022" -A x64
cmake --build build --config Release
ctest --test-dir build -C Release
```

CUDA proof binaries are built when a CUDA toolkit is available:

```sh
cmake -S . -B build-cuda -G "NMake Makefiles" -DFABRIC_BUILD_CUDA=ON
cmake --build build-cuda --config Release
```

The public API remains usable without the reference TCP coordinator; transport is a reference deployment mechanism, not the core abstraction.

### Install and consume via find_package

```sh
cmake --install build --config Release --prefix <prefix>
```

A downstream project links the exported target:

```cmake
find_package(FabricScheduler CONFIG REQUIRED)
target_link_libraries(my_app PRIVATE FabricScheduler::FabricScheduler)
```

The repository ships an independent downstream consumer (`examples/consumer`) that configures against the installed package, compiles, links, and runs a placement to receive a deterministic result.

---

## Modules

`include/fabric_scheduler/` and `src/` contain the core (`id`, `units`, `types`, `error`, `result`), request/demand model, candidate model, eligibility, ranking, placement, revalidation, policy, persistence, protocol, net (Winsock TCP), coordinator, worker, adapters, examples, tools (CLI inspection), benchmarks, and CUDA proof binaries.

---

## License

Apache License 2.0. Copyright 2026 Summon Software Labs. No telemetry transmission.