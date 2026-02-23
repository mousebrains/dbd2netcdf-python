#!/usr/bin/env python3
"""
Performance comparison: xarray-dbd vs dbdreader

Benchmarks wall time and peak memory for reading Slocum glider DBD files.
Runs four test scenarios:
  1. Single file read (all sensors)
  2. Single file read (filtered sensors)
  3. Multi-file read (all sensors)
  4. Multi-file read (filtered sensors)

Usage:
    python scripts/benchmark_comparison.py -C cache_dir file1.dcd file2.dcd ...
    python scripts/benchmark_comparison.py -C dbd_files/cache dbd_files/*.dcd
"""

from __future__ import annotations

import gc
import time
import tracemalloc
from argparse import ArgumentParser
from pathlib import Path

import dbdreader
import numpy as np

import xarray_dbd as xdbd


def fmt_time(seconds: float) -> str:
    if seconds < 1:
        return f"{seconds * 1000:.0f} ms"
    return f"{seconds:.2f} s"


def fmt_mem(nbytes: int) -> str:
    mb = nbytes / 1024 / 1024
    if mb < 1:
        return f"{nbytes / 1024:.0f} KB"
    return f"{mb:.1f} MB"


SENSORS_SUBSET = [
    "m_present_time",
    "m_depth",
    "m_lat",
    "m_lon",
    "m_pitch",
]

parser = ArgumentParser(description="Benchmark xarray-dbd vs dbdreader")
parser.add_argument("files", nargs="+", type=Path, help="DBD files to read")
parser.add_argument(
    "-C",
    "--cache",
    type=Path,
    required=True,
    metavar="directory",
    help="Sensor cache directory",
)
parser.add_argument(
    "-n",
    "--repeat",
    type=int,
    default=3,
    help="Number of repetitions per benchmark (default: 3)",
)
args = parser.parse_args()

files = sorted(args.files)
cache_dir = str(args.cache)
n_repeat = args.repeat

print(f"Files: {len(files)}")
print(f"Cache: {cache_dir}")
print(f"Repeats: {n_repeat}")
print()


# ── Helpers ──────────────────────────────────────────────────────────────


def bench(label: str, fn, repeats: int = n_repeat):
    """Run fn() multiple times, report best wall time and peak memory."""
    times = []
    peak_mem = 0
    for _ in range(repeats):
        gc.collect()
        tracemalloc.start()
        t0 = time.perf_counter()
        result = fn()
        t1 = time.perf_counter()
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        times.append(t1 - t0)
        peak_mem = max(peak_mem, peak)
        del result
        gc.collect()
    best = min(times)
    median = sorted(times)[len(times) // 2]
    print(
        f"  {label:40s}  best={fmt_time(best):>10s}"
        f"  median={fmt_time(median):>10s}"
        f"  peak_mem={fmt_mem(peak_mem):>10s}"
    )
    return best, peak_mem


# ── 1. Single file read (all sensors) ───────────────────────────────────

print("=" * 78)
print("1. Single file — all sensors")
print("=" * 78)

single_file = files[0]
print(f"   File: {single_file.name}")


# xarray-dbd
def xdbd_single_all():
    return xdbd.open_dbd_dataset(single_file, cache_dir=args.cache)


# dbdreader — read all parameters one at a time (re-parses file per sensor)
def dbdreader_single_all_individual():
    dbd = dbdreader.DBD(str(single_file), cacheDir=cache_dir)
    params = [p for p in dbd.parameterNames if p != "m_present_time"]
    data = {}
    for p in params:
        try:
            t, v = dbd.get(p)
            data[p] = (t, v)
        except Exception:
            pass
    dbd.close()
    return data


# dbdreader — read all parameters in a single get() call (one file parse)
def dbdreader_single_all_batch():
    dbd = dbdreader.DBD(str(single_file), cacheDir=cache_dir)
    params = [p for p in dbd.parameterNames if p != "m_present_time"]
    try:
        results = dbd.get(*params)
        data = dict(zip(params, results))
    except Exception:
        data = {}
    dbd.close()
    return data


bench("xarray-dbd (open_dbd_dataset)", xdbd_single_all)
bench("dbdreader (get per sensor)", dbdreader_single_all_individual)
bench("dbdreader (get all at once)", dbdreader_single_all_batch)
print()


# ── 2. Single file read (filtered sensors) ──────────────────────────────

print("=" * 78)
print(f"2. Single file — {len(SENSORS_SUBSET)} sensors only")
print("=" * 78)
print(f"   Sensors: {SENSORS_SUBSET}")


def xdbd_single_filtered():
    return xdbd.open_dbd_dataset(single_file, cache_dir=args.cache, to_keep=SENSORS_SUBSET)


def dbdreader_single_filtered_individual():
    dbd = dbdreader.DBD(str(single_file), cacheDir=cache_dir)
    data = {}
    for p in SENSORS_SUBSET:
        if p == "m_present_time":
            continue
        try:
            t, v = dbd.get(p)
            data[p] = (t, v)
        except Exception:
            pass
    dbd.close()
    return data


def dbdreader_single_filtered_batch():
    dbd = dbdreader.DBD(str(single_file), cacheDir=cache_dir)
    params = [p for p in SENSORS_SUBSET if p != "m_present_time"]
    try:
        results = dbd.get(*params)
        data = dict(zip(params, results))
    except Exception:
        data = {}
    dbd.close()
    return data


bench("xarray-dbd (to_keep)", xdbd_single_filtered)
bench("dbdreader (get per sensor)", dbdreader_single_filtered_individual)
bench("dbdreader (get all at once)", dbdreader_single_filtered_batch)
print()


# ── 3. Multi-file read (all sensors) ────────────────────────────────────

print("=" * 78)
print(f"3. Multi-file — {len(files)} files, all sensors")
print("=" * 78)


def xdbd_multi_all():
    return xdbd.open_multi_dbd_dataset(files, cache_dir=args.cache)


def dbdreader_multi_all_individual():
    try:
        mdbd = dbdreader.MultiDBD(
            filenames=[str(f) for f in files],
            cacheDir=cache_dir,
        )
    except Exception as e:
        print(f"  dbdreader MultiDBD failed: {e}")
        return None
    all_params = mdbd.parameterNames["eng"] + mdbd.parameterNames["sci"]
    params = [p for p in all_params if p != "m_present_time"]
    data = {}
    for p in params:
        try:
            t, v = mdbd.get(p)
            data[p] = (t, v)
        except Exception:
            pass
    mdbd.close()
    return data


def dbdreader_multi_all_batch():
    try:
        mdbd = dbdreader.MultiDBD(
            filenames=[str(f) for f in files],
            cacheDir=cache_dir,
        )
    except Exception as e:
        print(f"  dbdreader MultiDBD failed: {e}")
        return None
    all_params = mdbd.parameterNames["eng"] + mdbd.parameterNames["sci"]
    params = [p for p in all_params if p != "m_present_time"]
    try:
        results = mdbd.get(*params)
        data = dict(zip(params, results))
    except Exception:
        data = {}
    mdbd.close()
    return data


bench("xarray-dbd (open_multi_dbd_dataset)", xdbd_multi_all)
bench("dbdreader (get per sensor)", dbdreader_multi_all_individual)
bench("dbdreader (get all at once)", dbdreader_multi_all_batch)
print()


# ── 4. Multi-file read (filtered sensors) ───────────────────────────────

print("=" * 78)
print(f"4. Multi-file — {len(files)} files, {len(SENSORS_SUBSET)} sensors")
print("=" * 78)


def xdbd_multi_filtered():
    return xdbd.open_multi_dbd_dataset(files, cache_dir=args.cache, to_keep=SENSORS_SUBSET)


def dbdreader_multi_filtered_individual():
    try:
        mdbd = dbdreader.MultiDBD(
            filenames=[str(f) for f in files],
            cacheDir=cache_dir,
        )
    except Exception as e:
        print(f"  dbdreader MultiDBD failed: {e}")
        return None
    data = {}
    for p in SENSORS_SUBSET:
        if p == "m_present_time":
            continue
        try:
            t, v = mdbd.get(p)
            data[p] = (t, v)
        except Exception:
            pass
    mdbd.close()
    return data


def dbdreader_multi_filtered_batch():
    try:
        mdbd = dbdreader.MultiDBD(
            filenames=[str(f) for f in files],
            cacheDir=cache_dir,
        )
    except Exception as e:
        print(f"  dbdreader MultiDBD failed: {e}")
        return None
    params = [p for p in SENSORS_SUBSET if p != "m_present_time"]
    try:
        results = mdbd.get(*params)
        data = dict(zip(params, results))
    except Exception:
        data = {}
    mdbd.close()
    return data


bench("xarray-dbd (to_keep)", xdbd_multi_filtered)
bench("dbdreader (get per sensor)", dbdreader_multi_filtered_individual)
bench("dbdreader (get all at once)", dbdreader_multi_filtered_batch)
print()


# ── 5. Record count verification ────────────────────────────────────────

print("=" * 78)
print("5. Data verification")
print("=" * 78)

ds = xdbd.open_multi_dbd_dataset(files, cache_dir=args.cache, to_keep=SENSORS_SUBSET)

print(f"   xarray-dbd records: {len(ds.i):,}")

try:
    mdbd = dbdreader.MultiDBD(filenames=[str(f) for f in files], cacheDir=cache_dir)
    for sensor in SENSORS_SUBSET:
        if sensor == "m_present_time":
            continue
        xdbd_vals = ds[sensor].values
        xdbd_valid = int(np.sum(np.isfinite(xdbd_vals)))
        try:
            t, v = mdbd.get(sensor)
            dbdr_valid = len(v)
        except Exception:
            dbdr_valid = 0
        print(f"   {sensor:30s}  xarray-dbd valid={xdbd_valid:>8,}  dbdreader={dbdr_valid:>8,}")
    mdbd.close()
except Exception as e:
    print(f"   dbdreader failed to open files: {e}")
    for sensor in SENSORS_SUBSET:
        if sensor == "m_present_time":
            continue
        xdbd_vals = ds[sensor].values
        xdbd_valid = int(np.sum(np.isfinite(xdbd_vals)))
        print(f"   {sensor:30s}  xarray-dbd valid={xdbd_valid:>8,}  dbdreader=     N/A")
