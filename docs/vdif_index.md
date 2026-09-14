# VDIF indexing and export budgets

`vlbimeta-vdif-index` is a diagnostic for closed recorder captures. It builds a
compact, content-hashed index and budgets selection from a schema-2 CSV scan
manifest. It does not decode voltages, copy VDIF, publish products, or change the
controller path. The current full-capture hard links remain the delivered VDIF.

## Build and query

```sh
vlbimeta-vdif-index build \
    --data-dir /data --capture-block-id 1788963931 --stream-name sdp_vdif \
    --frames-per-second 3200 --threads 0 1 2 3 \
    --output-dir /diagnostics/1788963931-index

vlbimeta-vdif-index plan \
    --index-dir /diagnostics/1788963931-index \
    --scan-manifest /diagnostics/scan_manifest.json \
    --read-mib-per-second 250 --write-mib-per-second 250 \
    --output /diagnostics/export-plan.json
```

The rate is **per thread**. Obtain it and the expected thread IDs from the
instrument configuration: EDV 0 headers cannot establish either. The numbers
above describe the local synthetic packet source, not a general instrument
default. The station ID is read from the packets and must stay constant; its
physical station and polarisation/frequency mapping still require the instrument
contract.

Build requires the exact version-1 handoff, including a closed `capture.json` and
matching shard inventory. Output must be a fresh directory outside the raw tree.
It contains `index.sqlite` and `index_report.json`. Each raw shard receives a
SHA-256 digest during the same sequential pass as header inspection. The capture
inventory and file identities, sizes and timestamps are checked again at the end
to detect changes during indexing. This is not a lock against a concurrent writer;
closed raw files must remain immutable.

An index has a `building`, `failed`, or `complete` state. A crash can leave
`building`; planning requires `complete`. Existing directories are rejected,
including failed attempts. Recovery/reuse and controller integration are separate
work. A successful index says its supported headers and unique frame identities
were indexed; it does not certify complete or scientifically valid observations.

Planning opens the index read-only, without reopening raw data. Its report says
`raw_revalidated=false`: a future exporter must verify the indexed input identity
before using byte locations. A supplied manifest capture/stream identity must
match. Diagnostic manifests made from standalone CSVs can lack these fields;
aligning such a manifest with the correct capture remains the caller's duty.

## Index representation and scope

The first profile accepts little-endian, standard 32-byte VDIF headers, version
0 or 1, EDV 0 with zero extension words, and fixed geometry across all threads.
Frame lengths must be between 40 bytes and 4 MiB, including the header. Legacy
headers, extensions, changing geometry/station, truncated shards, unexpected
threads and frame numbers outside the supplied rate fail explicitly. This is
a deliberately restricted profile, not a parser for every legal VDIF variant.
See the [VDIF 1.1.1 specification](https://vlbi.org/wp-content/uploads/2019/03/VDIF_specification_Release_1.1.1.pdf)
for the header and clock definitions.

Each SQLite `runs` row describes one thread in one shard:

```text
time_tick(i) = start_tick + i            0 <= i < stop_tick - start_tick
byte_offset(i) = first_offset + i * stride
```

Ticks are integer multiples of the supplied frame rate on a continuous TAI time
axis. A run splits at a time discontinuity, byte-stride change, invalid-bit
change, or shard boundary. A single-frame run has stride zero. Interleaved and
thread-separated shards both work; filenames do not determine time/thread order.
Runs are checked in time order across shards to reject duplicate identities and
count gaps. Invalid frames are counted separately and retain their byte offsets.

Input chunks are at most 4 MiB; active runs are bounded by the expected thread
count. SQLite has an 8 MiB page-cache target and uses disk for temporary sorting.
There is no Python set/list of every frame. Memory still includes the interpreter,
libraries and shard/scan inventories. Ordered captures compress well; disorder or
alternating invalid bits can produce one row per frame, requiring much more
index space and SQL work. That worst case needs a production disk budget.

Reference epochs are interpreted in the **2000–2031 cycle only**. The six-bit
epoch field cannot resolve the cycle by itself; recordings using the 2032 cycle
need a future explicit cycle adapter. Seconds since an epoch count physical
seconds, including leap seconds. Epoch conversion uses Astropy's installed leap
table with downloads disabled; epoch origins are recorded in the report. Tests
cover the 2016 leap second and cross-epoch duplicate detection. Production must
supply a current leap table; the lab environment emitted an expired-table warning.
Manifest boundaries require aware ISO times with at most microsecond precision;
literal `23:59:60` boundaries are not supported by this initial manifest adapter.

## Selection and cost model

Only `include=true` scans are selected. They require unique `schedule_scan_id`
values; overlapping nominal windows fail until a padding/overlap policy exists.
Each `[start, end)` window selects only whole frames contained in that interval:
the first frame tick is `ceil(start)` and the exclusive stop tick is `floor(end)`.
No partial packet is copied or counted. Sub-frame windows can contain zero frames.

Coverage reports count present, missing and invalid frames for every expected
thread. The byte budget includes invalid frames; their eventual export policy is
still open. `complete_valid_frames` concerns nominal packet coverage only, and is
false for empty windows. Execution stays `unknown`; packet coverage cannot prove
that the telescope tracked the requested target or calibrated successfully.

Let `R` be recorded VDIF bytes and `S` the selected complete-frame bytes:

| Quantity | Budget |
| --- | ---: |
| Index input read | `R` |
| Separate sequential export read | `R` |
| New export storage/write | `S` |
| Raw plus exported VDIF retained | `R + S` |
| Index plus export I/O | `2R + S` |

These are logical data volumes, excluding indexes, filesystem overhead and any
extra verification passes. The model budgets a full export read. A future range
exporter might skip unselected bytes, but interleaving and storage read
amplification need measurement before claiming that saving.

When both assumed bandwidths are supplied, the report gives `R/read + S/write`
for serial I/O and `max(R/read, S/write)` for ideal independent overlapping I/O.
Neither is a measured export time or a guaranteed bound on completion. Shared
disks, seeking, sorting, CPU, fsync, concurrent capture and checksum verification
can change the result. `export_cpu_seconds` is explicitly unknown: no exporter has
been implemented or benchmarked. Omitting bandwidths leaves time estimates absent.

## Initial lab result

The [recorded inputs, measurements and projections](benchmarks/vdif-index-20260911.json)
use the local synthetic loopback capture `1788963931` from 2026-09-09. It has
38,400 frames, four threads, 3,200 frames/s/thread, 5,032-byte frames and 193,228,800
VDIF bytes across two shards. Index coverage agrees with the earlier independent
packet validator: 9,600 frames/thread, no duplicates, missing frames or invalid
frames. The index has eight runs and occupies 20,480 bytes.

| Index build on 2026-09-11, local ZFS scratch2 | First observed run | Repeat |
| --- | ---: | ---: |
| Wall time | 4.291 s | 0.968 s |
| Process CPU time | 0.208 s | 0.188 s |
| Input throughput | 42.94 MiB/s | 190.41 MiB/s |
| Process peak RSS | 61.67 MiB | 61.71 MiB |

Timings cover the indexing function through database closure, including hashing,
but exclude interpreter import and the final report write. Cache state was not
controlled, so neither result establishes sustained disk speed. This small sample
shows that I/O dominated these runs; it is not a capacity test alongside a capture.
The two synthetic one-second scan windows select 128,819,200 bytes with complete
coverage. They were constructed for this diagnostic and are not an executed CSV.
No new VDIF copy was made.

For the supplied N26L2 CSV, eligible scans total 8,460 seconds over a 10,800-second
science span. Assuming 64,000,000 payload bytes/s and the lab frame geometry gives
**695.62 GB raw + 544.91 GB selected = 1.241 TB retained**, using decimal units.
The earlier 541.44 GB figure excludes VDIF headers. This calculation excludes
local calibration before/after that span, recording margins, padding and losses.
It is a projection from the CSV, not a measurement of an N26L2 recording.

At an assumed 250 MiB/s for both reads and writes, the separate export's serial
I/O budget is 78.9 minutes; ideal independent overlap would be 44.2 minutes.
The index adds another full raw read. These assumptions must be replaced by
same-host measurements under concurrent recorder load before setting an execution
budget. The next export work must also settle consumer layout, padding, invalid
and partial-scan handling, and verify byte preservation against an independent
reader. The index itself is not a conventional per-scan delivery product.
