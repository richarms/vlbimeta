# Site observing-plan fixtures

These are the six matching CSV files supplied by the user from site on
2026-09-11, copied byte-for-byte from `example_csv/`. They are regression inputs,
not packaged default observing plans. They cover omitted network scans, local
MCAL/MPCC operations, station-offset differences and network calibrator tags.
The expected counts and CSV interval assertions are in
`tests/test_observation_plan.py`. Matching VEX files remain reference material
in the user's `example_vex/`; no production VEX parser is used by these tests.
