# Reproducible CAM acquisition smoke test

This test exercises configuration, continuous VDIF recording, closed inventory
handoff and hard-linked pass-through publication. It does not certify ANTAB,
UVFLG, beamformer calibration or per-scan VDIF exports. The intended observer is
`katsdpscripts/observation/vlbi_online.py`, preserving Marcel's existing observing
and calibration procedure. The katobs tracking-only prototype is retired.

Before running that draft live, fix its product-specific capture namespace and
dry-run handling, resolve the phase-up gain initialisation issue, and check the
instrument assumptions with Marcel. Register it through the normal CAM executor
and exercise its deployed katcorelib lifecycle. These preparation tools do not
perform those changes or establish observer readiness.

## Matched images

`scripts/cam_smoke_images.json` pins the four registry digests from the successful
September 9 handoff test. They were checked against local Docker RepoDigests on
September 23. These are historical reproduction inputs, not default images for
a new integration run. Supply an explicitly reviewed image JSON with `--images`.
Source/tag provenance:

| Image | Tag |
| --- | --- |
| katsdpcontroller | `richarms-vlbi-handoff-20260909-3923022` |
| katsdpvlbi | `richarms-vlbi-handoff-20260909-1cadf36-mcastfix2` |
| katsdpcam2telstate | `richarms-vlbi-resampled-20260804` |
| vlbimeta | `richarms-vlbi-handoff-20260909-ad47d07` |

The controller image contains `VLBI_STREAM_NAME` propagation and the vlbimeta
image contains closed raw-v1 inventory validation and hard-link publication.
These are previously built artifacts, not fresh builds of every current branch.
In particular the vlbimeta image predates the strict CSV/index work. It is
appropriate for this explicit `pass_through` test, not acceptance of those new
features. Build and pin a newer vlbimeta image for their subsequent acceptance.
The bridge also predates the reviewed underscore sensor-name correction; use a
matching consumer when testing new bridge images.

## Prepare and preflight

Obtain a **fresh allocated CAM product-config** JSON. Do not replay an old
snapshot's input endpoints. Choose a future UTC start with time for setup and
within the observing script's configured wait limit. From the vlbimeta repo:

```bash
python3 scripts/prepare_cam_smoke.py /path/to/fresh-product-config.json \
  --images=/path/to/reviewed-image-digests.json \
  --start=2026-10-20T12:00:00Z --output-dir=/tmp/vlbi-cam-smoke
```

Replace the example time with the agreed session time. The tool creates a new
directory containing a two-scan tracking CSV (20 s each, 20 s gap) and a config
with the four digest overrides and `config.vlbimeta.mode=pass_through`. It
preserves all input allocations, other outputs and existing service placement.
It checks relevant stream relationships and `vdif://` input syntax, but does
not replace controller schema validation. Review and apply through CAM's normal
configuration path; the preparation tool sends no control requests. A synthetic
azel target at 80 degrees is for acquisition plumbing, not astronomy.

Check the model service from the hosts/containers which will run the product
controller and consumers. `scripts/preflight_models.py` requires
the optional preflight dependencies (`python -m pip install '.[preflight]'`);
it resolves every alias, downloads the final payload
and validates it with the appropriate model class, including checksum checks.
It produces JSON with host, UTC time, alias chain and outcome, and returns
nonzero if any supplied model fails. Example for the previously tested L-band
856/107 MHz product (verify the actual configured geometry first):

```bash
python3 scripts/preflight_models.py \
  rfi=https://sdpmodels.kat.ac.za/rfi_mask/current.alias \
  band=https://sdpmodels.kat.ac.za/band_mask/current/l/nb_ratio=1.alias \
  band=https://sdpmodels.kat.ac.za/band_mask/current/l/nb_ratio=8.alias \
  > model-preflight.json
```

Use the model base URL configured for the deployed controller, including its
scheme. Also include `beam=BASE/primary_beam/current/GROUP/ANT/BAND.alias` for
each actual antenna and both `individual` and `cohort` groups. Ratio is
`round(adc_sample_rate / 2 / bandwidth)`. Passing the three example URLs alone
is not a full product model preflight.

On September 23 at 09:44 UTC these three example model checks passed from
`ing-stage-4.sdpdyn.kat.ac.za`, including payload validation. This does not
establish access from every execution container or explain the September 14
HTTP 502 failures. The lab Mesos snapshot had no active product tasks when
inspected, so no existing CAM product was modified or captured in this work.
The eight individual/cohort L-band beam models for m001–m004 also passed at
09:59 UTC from that host. The initial sandbox DNS failure was resolved by
running this read-only check outside the sandbox.

## CAM acceptance record

1. Complete the observer prerequisites above, then install/register it in the
   normal CAM executor environment. Run its CAM dry run against the new CSV. Confirm
   the actual command, interpreter, selected SDP product and planned times.
2. Apply the prepared configuration using CAM. Retain product-config, model
   report and actual task image digests. Confirm recorder ready, the VDIF input
   protocol, and one multicast group carrying the expected four threads.
3. Execute the short observation. Record **both** wideband and narrowband IDs;
   check the CSV hash in the VLBI product's own capture view. Retain the actual
   labels, activity and script logs; a structured execution journal is still
   pending. Confirm continuous recording and no script error.
4. Confirm capture-stop and capture-done replies/logs separately. katcorelib's
   script success alone does not prove successful teardown.
   Confirm the recorder's closed raw manifest, exact file inventory and sizes.
5. Confirm vlbimeta ran on the recorder's host and published pass-through output.
   Compare source/destination device and inode numbers to establish hard links;
   inspect VDIF header times, station/thread IDs and coverage with the existing
   validator. No payload duplication or scan restructuring is expected here.
6. Retain logs, manifests, CSV and timestamps. In a separate short fresh capture,
   cancel during a scan and check the reported outcome and recorder closure.
   A retry always gets a new capture ID.

The generated CSV contains only tracking rows and includes the observer's
`PCC_VERIFICATION` header. It is an acquisition fixture, not a calibration
acceptance schedule. Marcel's observing procedure supports local calibration
and phase-up rows; use a separately reviewed schedule to test those operations.
Science validation and execution-aware metadata remain later gates. For site
tests, also configure the intended host volume, placement and multicast routes;
the preparation overlay deliberately preserves existing placement.
