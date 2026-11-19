# Local benchmark

Run the same workload with:

```sh
python examples/benchmark.py --events 200 --workers 4
```

The script creates a temporary database, saves 200 events sequentially, then drains their deliveries using four workers and a real loopback HTTP receiver. Each event has 512 bytes of synthetic message data. The receiver returns HTTP 204 without business processing. The database uses WAL and `synchronous=FULL`.

## Recorded run — v0.1.1, November 19, 2026

| Measurement | Result |
| --- | --- |
| Environment | Windows, CPython 3.14.3 |
| Processor | Intel64 Family 6 Model 142 Stepping 12 |
| Events / destinations / workers | 200 / 1 / 4 |
| Store acceptance | 146.9 events/second |
| Queue-to-HTTP drain | 32.8 deliveries/second |
| HTTP attempt p95 | 22.8 ms |
| Delivered at completion | 200 / 200 |

Acceptance measures direct store calls, not the HTTP ingestion API. Drain throughput includes database coordination and local HTTP work. Attempt latency starts at the network boundary and excludes queue waiting time. This is a single run with a small dataset and no competing workload. Internet latency, disk speed, larger payloads, slow receivers, and backlog size will change the result.

The script prints its environment and measurements so results can be reproduced and compared. No cloud throughput or availability claim is inferred from this run.

In 0.1.1 only one request to a given endpoint is in flight at a time, regardless of worker count. The previous 0.1.0 run drained this single-destination workload at 51.0 deliveries/second with unrestricted endpoint concurrency. The lower single-destination throughput is a deliberate isolation tradeoff; adding workers is not a way to bypass shared endpoint/project limits.

## Parked backlog

```sh
python examples/benchmark_queue.py --backlog 200000 --mode paused
python examples/benchmark_queue.py --backlog 200000 --mode rate-limited
```

This fixture creates 200,000 distinct events and parked deliveries outside the measured operations. It then samples an empty queue claim, accepts one healthy-project event, claims that delivery, and records completion. Results below use 20 samples on the same Windows/Python environment. The baseline uses the tagged 0.1.0 Store with the same fixture; the 0.1.1 run followed it locally.

| Median operation | v0.1.0, paused | v0.1.1, paused |
| --- | --- | --- |
| Empty claim | 114.54 ms | 3.55 ms |
| Healthy delivery claim | 131.79 ms | 8.23 ms |
| Healthy event acceptance | 25.29 ms | 7.52 ms |

A separate 0.1.1 run with a rate-limited backlog measured a 3.31 ms empty claim and 7.84 ms healthy claim. These are single-run timings, affected by disk and system load; the regression suite instead enforces a SQLite instruction budget and verifies indexed selection so correctness does not depend on machine speed. Endpoint/project count remains a scaling factor.

## Stalled receiver isolation

A local 60-second workload sent one event every four seconds to a receiver that accepted TCP connections without responding, while a second project sent one event every two seconds to a healthy HTTP 204 receiver. With the default four workers, all 30 healthy events completed: average end-to-end latency was about 0.2 seconds and maximum latency about 0.6 seconds. The unhealthy project retained its pending/retrying backlog. This verifies healthy progress during one destination's outage; it does not establish an internet-facing availability guarantee.
