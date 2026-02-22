# Examples

Standalone scripts demonstrating how to work with Slocum glider data
using the `xarray-dbd` package.

Each script is self-contained and can be run from the command line.
Pass `--help` to see usage information.

## load_single_file.py

[Source](load_single_file.py)

Load a single DBD file and print a summary of its dimensions, variables,
units, depth/lat/lon ranges, and DataFrame shape.

```bash
python examples/load_single_file.py -C cache/ file.dbd
```

## load_multiple_files.py

[Source](load_multiple_files.py)

Load and concatenate multiple DBD files. Demonstrates sensor filtering
with `to_keep` and mission filtering with `skip_missions`.

```bash
python examples/load_multiple_files.py -C cache/ *.dbd
```

## advanced_usage.py

[Source](advanced_usage.py)

Advanced data exploration: subsetting by record index, filtering by
depth, computing approximate distance traveled, and printing full
dataset info.

```bash
python examples/advanced_usage.py -C cache/ *.dbd
```

## plot_dive_profile.py

[Source](plot_dive_profile.py)

Plot a glider dive profile as a scatter plot colored by water temperature.

- **X axis** : time (POSIX timestamp converted to UTC datetime)
- **Y axis** : approximate depth in meters (pressure x 10, inverted)
- **Color** : water temperature in degrees C

```bash
python examples/plot_dive_profile.py -C cache/ ~/data/raw/*.e?d
```

The script loads only the three sensors it needs (`sci_m_present_time`,
`sci_water_pressure`, `sci_water_temp`) so it stays fast and
memory-efficient even on multi-million-record deployments.
