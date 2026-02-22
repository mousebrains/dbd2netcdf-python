#!/usr/bin/env python3
"""
Plot a Slocum glider dive profile colored by water temperature.

Reads a set of DBD/EBD files, extracts time, pressure (as a proxy for
depth), and temperature, then produces a scatter plot with:

    x-axis : time  (converted from POSIX timestamp to datetime)
    y-axis : approximate depth in meters  (pressure * 10, inverted)
    color  : water temperature in degrees C

Usage:
    python plot_dive_profile.py ~/tpw/mariner/onboard/raw/*.e?d

Requirements:
    pip install xarray-dbd matplotlib
"""

import sys
from datetime import datetime, timezone
from glob import glob
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np

import xarray_dbd as xdbd

# ── Configuration ────────────────────────────────────────────────────────
#
# Change these to match the sensors you want to plot.
#
TIME_VAR = "sci_m_present_time"  # POSIX timestamp (seconds since epoch)
PRESSURE_VAR = "sci_water_pressure"  # pressure in bar
TEMP_VAR = "sci_water_temp"  # temperature in degrees C

PRESSURE_TO_DEPTH = 10.0  # rough conversion: 1 bar ≈ 10 m seawater

# ── Load data ────────────────────────────────────────────────────────────

# Collect file paths from the command line (supports shell globs)
files = sys.argv[1:]
if not files:
    print(__doc__.strip())
    sys.exit(1)

# Expand any unexpanded globs (useful on Windows)
expanded = []
for pattern in files:
    matches = sorted(glob(pattern))
    expanded.extend(matches if matches else [pattern])
files = expanded

print(f"Reading {len(files)} files ...")

# Read all files into a single xarray Dataset.
# Only load the three sensors we need — this is much faster and uses
# less memory than loading everything.
ds = xdbd.open_multi_dbd_dataset(
    files,
    to_keep=[TIME_VAR, PRESSURE_VAR, TEMP_VAR],
    cache_dir=Path(files[0]).parent,  # sensor cache lives alongside data
)

print(f"  {ds.sizes['i']:,} records, {len(ds.data_vars)} variables")

# ── Extract and filter ───────────────────────────────────────────────────

time = ds[TIME_VAR].values  # POSIX seconds
pressure = ds[PRESSURE_VAR].values  # bar
temp = ds[TEMP_VAR].values  # degrees C

# The glider records NaN when a sensor hasn't been updated yet.
# Keep only rows where all three sensors have valid data.
valid = np.isfinite(time) & np.isfinite(pressure) & np.isfinite(temp)
time = time[valid]
pressure = pressure[valid]
temp = temp[valid]

print(f"  {valid.sum():,} valid data points after removing NaN")

# Convert POSIX timestamps to Python datetimes for a readable x-axis
datetimes = [datetime.fromtimestamp(t, tz=timezone.utc) for t in time]

# Approximate depth in meters (positive downward)
depth = pressure * PRESSURE_TO_DEPTH

# ── Plot ─────────────────────────────────────────────────────────────────

fig, ax = plt.subplots(figsize=(14, 6))

scatter = ax.scatter(
    datetimes,
    depth,
    c=temp,
    s=1,  # small dots — there are millions of points
    cmap="RdYlBu_r",  # red = warm, blue = cold
    rasterized=True,  # keeps the PDF/SVG file size manageable
)

# Color bar for temperature
cbar = fig.colorbar(scatter, ax=ax, pad=0.02)
cbar.set_label("Temperature (°C)")

# Invert y-axis so the surface (0 m) is at the top
ax.invert_yaxis()

# Format the time axis
ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
ax.xaxis.set_major_locator(mdates.AutoDateLocator())
fig.autofmt_xdate()  # rotate date labels

ax.set_xlabel("Date (UTC)")
ax.set_ylabel("Approximate Depth (m)")
ax.set_title("Glider Dive Profile — Temperature vs Depth")

plt.tight_layout()
plt.show()
