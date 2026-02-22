# Examples

Standalone scripts demonstrating how to work with Slocum glider data
using the `xarray-dbd` package.

Each script is self-contained and can be run from the command line.
Pass `--help` or run without arguments to see usage information.

## plot_dive_profile.py

Plot a glider dive profile as a scatter plot colored by water temperature.

- **X axis** : time (POSIX timestamp converted to UTC datetime)
- **Y axis** : approximate depth in meters (pressure x 10, inverted)
- **Color** : water temperature in degrees C

```bash
python examples/plot_dive_profile.py ~/data/raw/*.e?d
```

The script loads only the three sensors it needs (`sci_m_present_time`,
`sci_water_pressure`, `sci_water_temp`) so it stays fast and
memory-efficient even on multi-million-record deployments.
