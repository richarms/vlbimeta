# Physical scan-export benchmark, 2026-10-16

The existing flat VDIF example was physically copied to `/scratch` and exported
into per-scan VDIF files on the **same attached storage**. Indexing and export
are measured operations, not calculations from assumed disk bandwidths.

The [machine-readable report](benchmarks/vdif-export-20261016.json) retains input
shard hashes, geometry, software versions/source hashes, timings, process I/O
counters, output hashes, independent validation and eight-hour projections.
Large artifacts remain at `/scratch/richarms/vdif-export-20261016`.

## Input and method

Host: `ing-stage-4.sdp.kat.ac.za`. Storage: `/scratch`, ext4 on `/dev/sdb1`,
reported model `Seagate IronWolf`, non-rotational according to sysfs. Both source
and destination of each timed export were on this filesystem. No intentional
concurrent recording/storage workload was introduced.

The source is the supplied `git_katsdpcontroller/example_vdif` dataset:
67 files, **17,970,278,400 bytes**, 3,571,200 frames spanning **279 seconds** on
2023-10-26. It has four interleaved threads (0–3), 3,200 frames/s/thread,
5,000-byte payloads plus 32-byte standard EDV-0 headers, real 2-bit samples and
station ID 28005 (`me`). This was an existing VDIF example, not a new online
CBF/recorder capture. The `power.csv` companion was not copied or timed.

A synthetic CSV chooses nine 20-second scans separated by gaps and a final
fractional-boundary scan. It selects **65.9496%** of input frames. This tests
scan extraction and original IDs; it is not an assertion that those sky scans
were actually observed. A separate full-retention manifest selects all 279 s.

The implementation performs:

1. The existing full sequential index/hash pass, checking closed v1 inventory,
   geometry, frame identities and coverage.
2. A second full sequential read with source hash revalidation, vectorised
   header-time selection, unchanged frame copying and output SHA-256 hashing.
3. File and directory `fsync`, then final directory rename. These costs are
   included in export wall time; process imports and original input staging are
   excluded. The staging copy took 194.2 s and is not part of postprocessing.

Before every timed phase, clean input pages were released with **per-file**
`POSIX_FADV_DONTNEED`. No global cache settings were changed. `/proc/self/io`
reported 17.970 GB of physical input reads for each phase, confirming the
timings were not simply reads from RAM. Output physical writes match exported
byte counts plus small metadata/alignment overheads.

## Measured results

GB are decimal. MiB/s use 1,048,576 bytes.

| Phase | Input GB | Exported GB | Wall seconds | Process CPU seconds |
| --- | ---: | ---: | ---: | ---: |
| Index + raw SHA-256 | 17.970 | — | 41.38 | 21.20 |
| Ten scans, first export | 17.970 | 11.851 | 71.49 | 26.31 |
| Ten scans, repeat export | 17.970 | 11.851 | 71.99 | 25.96 |
| Full-retention export | 17.970 | 17.970 | 78.54 | 30.33 |

Indexing achieved 414.14 MiB/s. The first scan export achieved 239.72 MiB/s of
input over its complete runtime, including output writes, hashes and flushes.
Its combined logical read/write throughput was 397.82 MiB/s; the full-retention
export achieved 436.42 MiB/s combined. These are pipeline rates, not independent
maximum device read and write speeds.

File/directory `fsync` accounted for 26.30 and 26.63 s in the ten-file exports,
and 11.05 s in the one-file full-retention export. Buffering and overlap make
those figures unsuitable for a simple per-GB extrapolation by themselves.
Peak process RSS was about 526 MiB; index size was 53,248 bytes. Header-time
selection used about 0.23 s of CPU; raw/output hashing used 16.8–19.7 s.

Index plus export took **112.87 / 113.37 s** for the gapped plan, and
**119.92 s** for full retention, against 279 s of recorded data.

## Independent validation

All three exports passed a separate verifier. It uses baseband's VDIF header
decoder and UTC datetime arithmetic to derive source byte ranges independently
of the exporter's TAI tick calculation. It compares SHA-256 over **every output
byte** with the corresponding original source range, including ranges crossing
shard boundaries. It also checks sizes, boundary frame times, station label and
boundary threads. The fractional scan starts at 15:00:36.000312500 UTC and its
last frame starts at 15:00:39.999375000 UTC, preserving whole-frame containment.

The read-back verifier took **34.73 / 36.23 s** for the gapped exports and
**64.82 s** for the full-retention export. These are separate from export timings;
their cache state was not controlled. Production source/output hashes are already
computed during export; a separate physical read-back policy remains a choice.

The vlbimeta test suite passed **199 tests, with 9 optional tests skipped**. New
small-packet tests independently check exact bytes, adjacent/fractional windows,
chunk/shard crossings, unchanged raw input, stale hashes, invalid/missing frames,
ordering failures, path components and refusal to overwrite previous outputs.
Astropy emitted an expired leap-table warning during benchmark phases. The
sample is from 2023, and independent baseband/UTC byte-range checks passed.

## 64 MHz dual-polarisation, eight-hour volumes

Assume the current **2-bit** mode: 64 MHz total bandwidth per polarisation,
represented by two real 32 MHz sidebands per polarisation (four VDIF threads).
Each real thread is sampled at 64 Msamples/s. Thus:

```text
payload = 64 MHz × 2 real samples/Hz × 2 bits × 2 polarisations
        = 512 Mb/s = 64.0000 MB/s
VDIF    = payload × 5032/5000 = 64.4096 MB/s
```

| Quantity | Volume |
| --- | ---: |
| VDIF per hour | 231.875 GB |
| Eight-hour payload | 1.8432 TB |
| Eight-hour VDIF, including headers | **1.8550 TB = 1.6871 TiB** |
| Extra export at the measured 65.95% selection | 1.2234 TB |
| Raw + that export retained | **3.0784 TB** |
| Extra export at 100% selection | 1.8550 TB |
| Raw + full export retained | **3.7100 TB** |

Network/UDP/Ethernet overhead is additional to the recorded VDIF rate. These
figures exclude recording beyond eight hours, filesystem overhead and companion
products. Existing raw hard links do not add another payload allocation.

## Eight-hour time projection

Scale the measured 279-second example by `28800/279`. Using the mean of the two
gapped exports:

| Retention profile | Index | Export | Index + export | With independent read-back |
| --- | ---: | ---: | ---: | ---: |
| 65.95% selected | 1.19 h | 2.06 h | **3.24 h** | 4.26 h |
| 100% selected | 1.19 h | 2.25 h | **3.44 h** | 5.30 h |

These are **linear projections**, not eight-hour endurance tests or guarantees
under concurrent capture. SSD cache behaviour over terabytes, competing I/O,
packet disorder, extra padding and different file layouts may change them.
Independent read-back projections have the additional uncontrolled-cache caveat.
The current two-pass approach does about 4.93–5.56 TB of logical I/O for eight
hours, before optional independent validation. A future fused index/export pass
could avoid a full raw read, but has not been measured here.

## Diagnostic implementation and reproduction

The diagnostic exporter is `python -m vlbimeta.vdif_export` from an installed
environment outside the repository root, where the legacy `vlbimeta.py` launcher
would shadow the package. The benchmark driver below can run directly from the
checkout and selects its `src` package explicitly. Candidate layout:

```text
export/
    export.json
    benchmark.json
    BENCH_me_No0001/BENCH_me_No0001.vdif
    ...
```

Each file contains all four original threads interleaved by frame time/thread.
Frames and payloads are not decoded, re-quantised, renamed internally or padded.
The filename's station label is explicit; recipient layout acceptance remains
to be agreed. The exporter requires strictly ordered input. General reordering,
execution-aware selection, publication/DLM integration and production retry
recovery remain separate work. Missing/invalid nominal coverage fails unless
`--allow-partial` is explicitly supplied; invalid frames are preserved and flagged
in reports. Existing output/staging paths are never adopted or overwritten.

With a suitable Python environment and this repo as the working directory, use
a **new** `/scratch` directory for another run:

```bash
python scripts/benchmark_vdif_export.py prepare \
  --root /scratch/richarms/vdif-export-NEW \
  --source /scratch2/richarms/git_katsdpcontroller/example_vdif
python scripts/benchmark_vdif_export.py index --root /scratch/richarms/vdif-export-NEW
python scripts/benchmark_vdif_export.py export --root /scratch/richarms/vdif-export-NEW
python scripts/benchmark_vdif_export.py export-repeat --root /scratch/richarms/vdif-export-NEW
python scripts/benchmark_vdif_export.py export-full --root /scratch/richarms/vdif-export-NEW
python scripts/benchmark_vdif_export.py verify --root /scratch/richarms/vdif-export-NEW --export-name all
```

The benchmark driver intentionally targets this specific contiguous four-thread
example and its known frame rate. It creates a synthetic schedule. Other
observations need their actual frame rate, instrument mapping and CSV instead.

This tests the VDIF restructuring part of the proposed science-output path.
**ANTAB, UVFLG, calibration correctness and fringe validation are not covered.**
The controller's current raw/hard-link publication path is unchanged. This
benchmark did not exercise live recorder start/stop or scan tagging.
