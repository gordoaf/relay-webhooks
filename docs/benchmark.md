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

## Capacity planning

A v0.2.1 stress run used four projects with four hung destinations each, plus one healthy project, with each project submitting every two seconds for 60 seconds. It used the real worker and local sockets, followed by an eight-second drain window and graceful worker shutdown. All 30 healthy deliveries completed: median end-to-end latency 9.79 seconds, mean 11.94 seconds, maximum 34.03 seconds. This run used Windows and CPython 3.14.3 and overlapped other local tests. It shows progress through correlated failures, not low latency during their initial discovery: new destinations can occupy slots before their first timeout identifies them for the shared probe limit.

The v0.2.2 checks below used the same 60-second submission pattern, eight-second drain, four real workers, and local sockets on Windows/CPython 3.14.3. Slow receivers waited 9.5 seconds before returning HTTP 200 or 503: two projects with two destinations each. The hung-receiver case used four projects with four destinations each. All destinations started untested. Each workload included a separate healthy HTTP 204 receiver receiving one event about every two seconds.

| Competing receivers | Healthy deliveries | Median end-to-end | Mean | Maximum |
| --- | --- | --- | --- | --- |
| Slow HTTP 200 | 30 / 30 | 0.21 s | 0.24 s | 0.53 s |
| Slow HTTP 503 | 30 / 30 | 0.21 s | 0.25 s | 0.54 s |
| No HTTP response | 29 / 29 | 0.35 s | 1.34 s | 10.07 s |

These are single local runs, executed concurrently with one another and other checks. The elapsed-time submission loop fit 29 healthy submissions in the hung-receiver run; all completed. The slow-response endpoints kept zero timeout streaks. Their backlog remained because the incoming workload exceeded slow-receiver capacity. The bounded slow/untested lane improves healthy progress without turning a slow HTTP 200 into a failure. A healthy destination's first request can still wait for a first-contact slot, visible in the hung-receiver maximum. Once classified, it can use reserved responsive capacity. These measurements do not establish a latency guarantee or internet throughput claim.

The scheduler still examines enabled endpoints to find eligible work. Thousands of mostly idle endpoints can make that scan a meaningful part of claim latency, even with a small delivery backlog. The parked-backlog benchmark does not measure this workload. Measure endpoint count, fanout, concurrent receiver failures, and queue age on the intended hardware before increasing scale; a durable indexed next-work schedule would be a separate scaling change.

Retention throughput depends on attempts per event, fanout, disk speed, and how many older events remain unfinished. Automatic maintenance uses bounded bursts and catch-up passes instead of one batch every 30 seconds. Each transaction remains limited by `RELAY_CLEANUP_BATCH_SIZE`; pending deliveries and queued recovery members remain protected. A configured retention age is an eligibility rule, not a guarantee that all eligible history has already been removed. Monitor storage and verify cleanup can keep pace with the expected sustained workload.
