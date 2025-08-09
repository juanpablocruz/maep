### MAEP — Merkle Anti-Entropy Protocol (reference implementation)

This repository contains a pragmatic, testable implementation of the Merkle Anti-Entropy Protocol (MAEP).

At a high level, each node maintains an append-only per-key log of operations, materializes Last-Writer-Wins (LWW) state, summarizes that state using Merkle roots, and performs anti-entropy synchronization over a simple framed wire protocol (TCP for sync, UDP/TCP for heartbeats). The implementation supports two summary modes:

- Root+descent: exchange global Merkle roots and recursively descend on mismatching prefixes to produce precise requests.
- Summary+segments: exchange flat leaf summaries and/or per-segment roots to bound request sizes and focus deltas.


### Quick start

- **Requirements**: Go 1.24+ (module declares `go 1.24.2`).

- **Build everything**:

  ```bash
  go build ./...
  ```

- **Run unit/integration tests**:

  ```bash
  go test ./...
  ```


### Executables

- `cmd/sim`: headless simulator that launches an N-node ring on localhost TCP/UDP ports, generates random writes, and records synchronization metrics.
- `cmd/tuidemo`: interactive 2-node TUI for exploring sync behavior, events, wire traffic, and network chaos knobs.
- `cmd/inspect`: small demo that builds a local log, materializes state, and prints the Merkle leaves/root for inspection.


### Simulator (`cmd/sim`)

Launch a ring of nodes that periodically write and synchronize. Outputs CSVs and a summary under `out/` by default.

Example (5 nodes for 60s, 7s sync, 3s writes, descent enabled):

```bash
go run ./cmd/sim \
  -nodes=5 \
  -duration=60s \
  -write-interval=3s \
  -sync-interval=7s \
  -heartbeat=750ms \
  -suspect-timeout=2250ms \
  -delta-chunk-bytes=65536 \
  -delta-window-chunks=1 \
  -retrans-timeout=2s \
  -descent=true \
  -out=out \
  -base-port=9100
```

Chaos knobs (uniform for now):

- `-loss` `-dup` `-reorder` in [0..1]
- `-delay` base one-way delay; `-jitter` (+/-)
- `-failure-period` mean time between random link downs; `-recovery-delay` duration of outage

Quiescing options:

- `-stop-writes-at` stop writers at an offset
- `-quiesce-last` stop writers near the end to measure convergence

Outputs in `out/`:

- `roots.csv`: per-node Merkle root snapshots at 1 Hz
- `sync_rounds.csv`: per-sync round latency and reason
- `summary.txt`: run parameters and aggregate metrics (latency mean/stdev, convergence, throughput, window occupancy, etc.)
- `chunk_rtts.csv`: per-delta-chunk RTTs and retransmit counts

Post-run analysis helper:

```bash
python3 analyze.py out --out out/combined.csv
```


### Interactive TUI (`cmd/tuidemo`)

Start two nodes (A ↔ B) on localhost and interactively issue writes/deletes, toggle link state, and tune chaos parameters:

```bash
go run ./cmd/tuidemo
```

Commands (type then Enter):

- `wa|wb <key> <val>`: write on A or B
- `da|db <key>`: delete on A or B
- `rand [a|b]`: random key write to A (default) or B
- `burst <a|b> <n>`: issue n writes rapidly
- `link <a|b> <up|down>`: toggle node link state
- `pause <a|b> <on|off>`: pause/resume sync engine
- `net <a|b> <tcp|udp|both> <loss|dup|reorder|delay|up|down> [args]`: chaos knobs
- `q`/`quit`/`exit`: leave the TUI

UI shows each node’s materialized state, Merkle roots, differing keys and segments, plus recent events and wire frames.


### Inspect demo (`cmd/inspect`)

Prints an example LWW state, Merkle leaves, and ASCII Merkle tree for a small local log:

```bash
go run ./cmd/inspect
```


### Protocol overview

- **Op log** (`pkg/oplog` + `pkg/model`): per-key append-only log of `Op{Put|Del}` with HLC timestamps and deterministic hashes. Order is MAEP: `(HLCTicks, ActorID, Hash)`.
- **Materialize** (`pkg/materialize`): reduces each key’s log via LWW into visible state; used to derive Merkle leaves.
- **Merkle tree** (`pkg/merkle`): leaves are `(key, hash(value-state))`, root summarizes global state.
- **Segments** (`pkg/segment`, `pkg/syncproto/segad.go`): deterministic key→segment mapping; nodes can advertise per-segment roots and request keys for changed segments only.
- **Descent** (`pkg/syncproto/descent.go`, `pkg/node`): start from `SYNC_ROOT`; if roots differ, recursively request child hashes or leaves for a prefix until precise needs are known.
- **Wire** (`pkg/wire`): simple framing `| 1B type | 4B len | payload |`. Message types include:
  - `SYNC_ROOT`, `DESCENT_REQ`, `DESCENT_RESP`
  - `SYNC_SUMMARY`, `SEG_AD`, `SEG_KEYS_REQ`, `SEG_KEYS`
  - `SYNC_REQ` (list of `Need{Key, From}`)
  - `SYNC_DELTA_CHUNK` + `SYNC_ACK` and `DELTA_NACK`
  - `SYNC_BEGIN`, `SYNC_END`, `PING`, `PONG`

Sync flow (typical):

- Node periodically sends `SYNC_ROOT` (and, if descent is disabled, `SEG_AD` and `SYNC_SUMMARY`).
- Receiver compares against local view and responds with either descent queries or a `SYNC_REQ` of keys with counts.
- Sender streams deltas as chunked, hashed frames with reliable, in-order delivery (`ACK`/`NACK`, windowed, retransmissions).
- Session ends early when equality is detected or explicitly via `SYNC_END`.


### Transport & heartbeats

- **TCP** (`pkg/transport/tcp.go`): framed stream for sync messages.
- **UDP** (`pkg/transport/udp.go`) or TCP: heartbeats (`PING`/`PONG`) to drive suspicion/unpause logic.
- **Chaos** (`pkg/transport/chaos.go`): wrap endpoints to simulate loss/dup/reorder/delay/jitter; used by the simulator and TUI.


### Configuration knobs (selected)

- `DeltaMaxBytes` per chunk; `DeltaWindowChunks` in-flight window; `RetransTimeout` for resends.
- Heartbeat period and miss threshold control suspect state and (auto-)pause during outages.
- `DescentEnabled` toggles root+descent vs summary/segments mode.


### Repository layout

- `cmd/sim`, `cmd/tuidemo`, `cmd/inspect`: runnable examples/tools
- `pkg/model`, `pkg/oplog`: operations and append-only logs
- `pkg/materialize`: LWW reduction into visible state
- `pkg/merkle`: Merkle leaf/root computation
- `pkg/segment`: key segmentation and per-segment roots
- `pkg/syncproto`: protocol data types and codecs (summary, req, delta, chunking, ack/nack, root/descent, segment ads)
- `pkg/node`: node orchestration, sessions, timers, heartbeats, sync logic
- `pkg/transport`: in-memory switch (tests), TCP/UDP endpoints, chaos wrapper
- `pkg/wire`: frame encoding/decoding and message type constants


### Development notes

- Go modules: see `go.mod` (uses `golang.org/x/term` for TUI, `x/sys` indirectly)
- Logging/events: `pkg/node` emits structured events; the simulator aggregates metrics and writes CSVs.
- Tests: protocol roundtrip and unit tests live under `pkg/**/**/*_test.go`.


### Paper

The reference document is included as `Merkle_Anti_Entropy_Protocol__MAEP_.pdf` in the repository root.


### Disclaimer

This is a reference implementation intended for experimentation and evaluation. Production deployments may require further hardening, persistence, batching, flow control, authentication, and compatibility considerations.


