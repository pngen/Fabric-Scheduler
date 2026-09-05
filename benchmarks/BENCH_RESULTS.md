# Fabric Scheduler benchmark results

Repo: E:\The Journey\Coding\GitHub\production\Fabric-Scheduler
Build: cmake --build build --config Release --target bench_candidate  (MSVC Visual Studio 17 2022, x64, /W4 /WX, zero warnings)
Bin:   build\benchmarks\Release\bench_candidate.exe

## Reproducibility
- Seed (splitmix64): 0x5EED0F00D = 25481506829
- Candidate dimensions: bindings=1  vramTotal=96 GiB  vramAvail=[16,63] GiB  cpuAvail=16 cores
  numaDistance=[0,3]  pcieHops=[0,4]  hostMemAvail=[16,128] GiB  storageAvail=256 GiB
  congestion=0  health.ready=true (dense)  stateLocal=random  provenance=Measured
- 3 scenarios per scale: dense-eligibility (all eligible), heavy-rejection (every 7th health.ready=false),
  over-demand (strict 48 GiB/device VRAM gate -> most fail a hard constraint).

## Per-scale timings (ns / seconds)

### Scale N=100
| scenario | N | ingest ns | schedule ns | revalidate ns | explain ns | retrieval ns | concurrent ns (8x100) | eligible | rejected | sched cands/sec | conc sched/sec |
|---|---|---|---|---|---|---|---|---|---|---|---|
| dense | 100 | 47100 | 472300 | 600 | 61900 | 9700 | 44745100 | 100 | 0 | 211730 | 17879 |
| heavy | 100 | 31500 | 326400 | 400 | 62800 | 8700 | 34902800 | 85 | 15 | 306373 | 22921 |
| over-demand | 100 | 24300 | 130200 | 500 | 158900 | 9100 | (skipped) | 29 | 71 | 768049 | n/a |

### Scale N=1000
| scenario | N | ingest ns | schedule ns | revalidate ns | explain ns | retrieval ns | concurrent ns (8x100) | eligible | rejected | sched cands/sec | conc sched/sec |
|---|---|---|---|---|---|---|---|---|---|---|---|
| dense | 1000 | 411000 | 5057300 | 800 | 76500 | 203600 | 431205100 | 1000 | 0 | 197734 | 1855 |
| heavy | 1000 | 320600 | 4594100 | 500 | 131100 | 103500 | 362389400 | 857 | 143 | 217670 | 2208 |
| over-demand | 1000 | 395300 | 1631300 | 400 | 582700 | 127800 | (skipped) | 278 | 722 | 613008 | n/a |

### Scale N=10000
| scenario | N | ingest ns | schedule ns | revalidate ns | explain ns | retrieval ns | concurrent ns (8x100) | eligible | rejected | sched cands/sec | conc sched/sec |
|---|---|---|---|---|---|---|---|---|---|---|---|
| dense | 10000 | 4138300 | 53372100 | 500 | 76000 | 2231400 | 9772706000 | 10000 | 0 | 187364 | 81.86 |
| heavy | 10000 | 4350500 | 50761000 | 700 | 1204000 | 2649200 | 8473299400 | 8571 | 1429 | 197002 | 94.41 |
| over-demand | 10000 | 3590400 | 16906100 | 600 | 4008600 | 1961500 | (skipped) | 2779 | 7221 | 591502 | n/a |

### Scale N=100000
| scenario | N | ingest ns | schedule ns | revalidate ns | explain ns | retrieval ns | concurrent ns (8x100) | eligible | rejected | sched cands/sec | conc sched/sec |
|---|---|---|---|---|---|---|---|---|---|---|---|
| dense | 100000 | 43266700 | 546733000 | 700 | 80300 | 24068100 | 100593248300 | 100000 | 0 | 182905 | 7.95 |
| heavy | 100000 | 41885300 | 557337900 | 600 | 10772200 | 21063400 | 93252111800 | 85714 | 14286 | 179424 | 8.58 |
| over-demand | 100000 | 46567200 | 241288100 | 1000 | 56139800 | 22360300 | (skipped) | 27365 | 72635 | 414442 | n/a |

## Summary (largest completed scale = 100000; all four scales 100/1000/10000/100000 completed)
- Single scheduling pass cost is roughly linear in candidate count: ~4.7 us/candidate at N=1000
  (dense, ~0.47 s at 1000 ... 0.5467 s at 100000) => ~183k-198k candidates/second.
- Over-demand schedules are ~2-3x faster than dense because hard-rejected candidates skip the 27-factor
  ranking pass (only eligible candidates get ranked).
- Concurrent throughput (8 threads x 100 schedules = 800 schedules) degrades with scale because each
  schedule copies and scans the whole candidate set: 17879 sched/sec at N=100 down to 7.95 sched/sec at
  N=100000 (dense), 8.58 sched/sec (heavy).
- Revalidation is O(1) (sub-microsecond). Explanation generation is O(plan rejections): 80 us (dense, 0
  rejections) up to 56 ms (over-demand, 72635 rejections) at N=100000.
- Candidate retrieval is ~240 ns/candidate at N=100000.
- No library modification was required; no genuine defect found. The benchmark is deterministic and
  builds clean with /W4 /WX.
