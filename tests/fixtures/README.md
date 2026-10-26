# Synthetic GRIB2 fixture

`synthetic.grib2` contains five messages on a 2x2 regular latitude/longitude grid.
All values were invented for this repository; it includes no downloaded weather
data. The generator uses the standard ecCodes `regular_ll_sfc_grib2` sample and
records all dates, variables and cell values in `../create_fixture.py`. ecCodes
is an Apache-2.0 tool; the fixture and generator use this repository's MIT terms.

Reference monthly spatial-message means:

| Variable | January 2024 | February 2024 | Other months |
| --- | ---: | ---: | --- |
| 2 metre temperature (K) | 285 | 293 | NaN |
| Surface pressure (Pa) | 100003 | 100103 | NaN |

Messages deliberately appear out of chronological/variable order.

SHA-256: `228431b7aa0112acc144499e9d3619d5b2e067b59cbf1753d7a7fb4409e8762d`.

To regenerate, install `eccodes` and run `python tests/create_fixture.py`.
