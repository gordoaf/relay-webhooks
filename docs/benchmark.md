# Local benchmark

Run the same workload with:

```sh
python examples/benchmark.py --events 200 --workers 4
```

The script creates a temporary database, saves 200 events sequentially, then drains their deliveries using four workers and a real loopback HTTP receiver. Each event has 512 bytes of synthetic message data. The receiver returns HTTP 204 without business processing. The database uses WAL and `synchronous=FULL`.

## Recorded run — November 19, 2026

| Measurement | Result |
| --- | --- |
| Environment | Windows, CPython 3.14.3 |
| Processor | Intel64 Family 6 Model 142 Stepping 12 |
| Events / destinations / workers | 200 / 1 / 4 |
| Store acceptance | 125.8 events/second |
| Queue-to-HTTP drain | 51.0 deliveries/second |
| HTTP attempt p95 | 20.9 ms |
| Delivered at completion | 200 / 200 |

Acceptance measures direct store calls, not the HTTP ingestion API. Drain throughput includes database coordination and local HTTP work. Attempt latency starts at the network boundary and excludes queue waiting time. This is a single run with a small dataset and no competing workload. Internet latency, disk speed, larger payloads, slow receivers, and backlog size will change the result.

The script prints its environment and measurements so results can be reproduced and compared. No cloud throughput or availability claim is inferred from this run.
