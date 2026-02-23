#!/usr/bin/env python3
"""
Performance comparison: xarray-dbd vs dbdreader

Benchmarks wall time and peak RSS for reading Slocum glider DBD files.
Each scenario runs in an isolated subprocess via /usr/bin/time -l to capture
both wall clock time and peak resident set size (including C/C++ allocations).

Runs four test scenarios:
  1. Single file read (all sensors)
  2. Single file read (filtered sensors)
  3. Multi-file read (all sensors)
  4. Multi-file read (filtered sensors)

Usage:
    python scripts/benchmark_comparison.py -C cache_dir file1.dcd file2.dcd ...
    python scripts/benchmark_comparison.py -C dbd_files/cache dbd_files/*.dcd

Run a single benchmark (used internally by the harness):
    python scripts/benchmark_comparison.py -C cache --run BENCH_NAME file ...
"""

from __future__ import annotations

import re
import shutil
import subprocess
import sys
import tempfile
from argparse import ArgumentParser
from pathlib import Path

# ── Benchmark functions (executed via --run in a subprocess) ──────────────

SENSORS_SUBSET = [
    "m_present_time",
    "m_depth",
    "m_lat",
    "m_lon",
    "m_pitch",
]


def _run_bench(name: str, files: list[Path], cache_dir: str) -> None:
    """Execute a single benchmark by name. Called in a subprocess."""
    single_file = files[0]

    if name == "xdbd_single_all":
        import xarray_dbd as xdbd

        xdbd.open_dbd_dataset(single_file, cache_dir=cache_dir)

    elif name == "dbdreader_single_all_individual":
        import dbdreader

        dbd = dbdreader.DBD(str(single_file), cacheDir=cache_dir)
        params = [p for p in dbd.parameterNames if p != "m_present_time"]
        for p in params:
            try:
                dbd.get(p)
            except Exception:
                pass
        dbd.close()

    elif name == "dbdreader_single_all_batch":
        import dbdreader

        dbd = dbdreader.DBD(str(single_file), cacheDir=cache_dir)
        params = [p for p in dbd.parameterNames if p != "m_present_time"]
        try:
            dbd.get(*params)
        except Exception:
            pass
        dbd.close()

    elif name == "xdbd_dbdr2_single_all":
        from xarray_dbd.dbdreader2 import DBD

        dbd = DBD(str(single_file), cacheDir=cache_dir)
        params = [p for p in dbd.parameterNames if p != dbd.timeVariable]
        dbd.get(*params)
        dbd.close()

    elif name == "xdbd_single_filtered":
        import xarray_dbd as xdbd

        xdbd.open_dbd_dataset(single_file, cache_dir=cache_dir, to_keep=SENSORS_SUBSET)

    elif name == "dbdreader_single_filtered_individual":
        import dbdreader

        dbd = dbdreader.DBD(str(single_file), cacheDir=cache_dir)
        for p in SENSORS_SUBSET:
            if p == "m_present_time":
                continue
            try:
                dbd.get(p)
            except Exception:
                pass
        dbd.close()

    elif name == "dbdreader_single_filtered_batch":
        import dbdreader

        dbd = dbdreader.DBD(str(single_file), cacheDir=cache_dir)
        params = [p for p in SENSORS_SUBSET if p != "m_present_time"]
        try:
            dbd.get(*params)
        except Exception:
            pass
        dbd.close()

    elif name == "xdbd_dbdr2_single_filtered":
        from xarray_dbd.dbdreader2 import DBD

        dbd = DBD(str(single_file), cacheDir=cache_dir)
        params = [p for p in SENSORS_SUBSET if p != dbd.timeVariable]
        dbd.get(*params)
        dbd.close()

    elif name == "xdbd_multi_all":
        import xarray_dbd as xdbd

        xdbd.open_multi_dbd_dataset(files, cache_dir=cache_dir)

    elif name == "dbdreader_multi_all_individual":
        import dbdreader

        mdbd = dbdreader.MultiDBD(filenames=[str(f) for f in files], cacheDir=cache_dir)
        all_params = mdbd.parameterNames["eng"] + mdbd.parameterNames["sci"]
        params = [p for p in all_params if p != "m_present_time"]
        for p in params:
            try:
                mdbd.get(p)
            except Exception:
                pass
        mdbd.close()

    elif name == "dbdreader_multi_all_batch":
        import dbdreader

        mdbd = dbdreader.MultiDBD(filenames=[str(f) for f in files], cacheDir=cache_dir)
        all_params = mdbd.parameterNames["eng"] + mdbd.parameterNames["sci"]
        params = [p for p in all_params if p != "m_present_time"]
        try:
            mdbd.get(*params)
        except Exception:
            pass
        mdbd.close()

    elif name == "xdbd_dbdr2_multi_all":
        from xarray_dbd.dbdreader2 import MultiDBD

        mdbd = MultiDBD(filenames=[str(f) for f in files], cacheDir=cache_dir)
        all_params = mdbd.parameterNames["eng"] + mdbd.parameterNames["sci"]
        params = [p for p in all_params if p != "m_present_time"]
        mdbd.get(*params)
        mdbd.close()

    elif name == "xdbd_multi_filtered":
        import xarray_dbd as xdbd

        xdbd.open_multi_dbd_dataset(files, cache_dir=cache_dir, to_keep=SENSORS_SUBSET)

    elif name == "xdbd_dbdr2_multi_filtered":
        from xarray_dbd.dbdreader2 import MultiDBD

        mdbd = MultiDBD(filenames=[str(f) for f in files], cacheDir=cache_dir)
        params = [p for p in SENSORS_SUBSET if p != "m_present_time"]
        mdbd.get(*params)
        mdbd.close()

    elif name == "dbdreader_multi_filtered_individual":
        import dbdreader

        mdbd = dbdreader.MultiDBD(filenames=[str(f) for f in files], cacheDir=cache_dir)
        for p in SENSORS_SUBSET:
            if p == "m_present_time":
                continue
            try:
                mdbd.get(p)
            except Exception:
                pass
        mdbd.close()

    elif name == "dbdreader_multi_filtered_batch":
        import dbdreader

        mdbd = dbdreader.MultiDBD(filenames=[str(f) for f in files], cacheDir=cache_dir)
        params = [p for p in SENSORS_SUBSET if p != "m_present_time"]
        try:
            mdbd.get(*params)
        except Exception:
            pass
        mdbd.close()

    else:
        print(f"Unknown benchmark: {name}", file=sys.stderr)
        sys.exit(1)


# ── Harness ──────────────────────────────────────────────────────────────

_TIME_RE_REAL = re.compile(r"^\s*([\d.]+)\s+real\b", re.MULTILINE)
_TIME_RE_RSS = re.compile(r"^\s*(\d+)\s+maximum resident set size", re.MULTILINE)


def fmt_time(seconds: float) -> str:
    if seconds < 1:
        return f"{seconds * 1000:.0f} ms"
    return f"{seconds:.2f} s"


def fmt_mem(nbytes: int) -> str:
    mb = nbytes / 1024 / 1024
    if mb < 1:
        return f"{nbytes / 1024:.0f} KB"
    return f"{mb:.1f} MB"


def bench(
    label: str,
    name: str,
    files: list[Path],
    cache_dir: str,
    repeats: int,
    baseline_rss: int,
) -> tuple[float, int]:
    """Run a benchmark in isolated subprocesses via /usr/bin/time -l."""
    file_args = [str(f) for f in files]
    cmd = [
        "/usr/bin/time",
        "-l",
        sys.executable,
        __file__,
        "-C",
        cache_dir,
        "--run",
        name,
        *file_args,
    ]

    times: list[float] = []
    peak_rss = 0

    for _ in range(repeats):
        result = subprocess.run(cmd, capture_output=True, text=True)
        stderr = result.stderr
        if result.returncode != 0:
            print(f"  {label:40s}  FAILED (exit {result.returncode})")
            if stderr:
                print(f"    {stderr.strip()[:200]}")
            return float("inf"), 0

        m_real = _TIME_RE_REAL.search(stderr)
        m_rss = _TIME_RE_RSS.search(stderr)
        if not m_real or not m_rss:
            print(f"  {label:40s}  FAILED (could not parse /usr/bin/time output)")
            return float("inf"), 0

        times.append(float(m_real.group(1)))
        rss = int(m_rss.group(1))
        peak_rss = max(peak_rss, rss)

    best = min(times)
    median = sorted(times)[len(times) // 2]
    delta_rss = max(0, peak_rss - baseline_rss)
    print(
        f"  {label:40s}  best={fmt_time(best):>10s}"
        f"  median={fmt_time(median):>10s}"
        f"  peak_rss={fmt_mem(delta_rss):>10s}"
    )
    return best, delta_rss


def bench_cmd(
    label: str,
    cmd: list[str],
    repeats: int,
    baseline_rss: int,
) -> tuple[float, int]:
    """Run an external command in isolated subprocesses via /usr/bin/time -l."""
    timed_cmd = ["/usr/bin/time", "-l", *cmd]

    times: list[float] = []
    peak_rss = 0

    for _ in range(repeats):
        result = subprocess.run(timed_cmd, capture_output=True, text=True)
        stderr = result.stderr
        if result.returncode != 0:
            print(f"  {label:40s}  FAILED (exit {result.returncode})")
            if stderr:
                print(f"    {stderr.strip()[:200]}")
            return float("inf"), 0

        m_real = _TIME_RE_REAL.search(stderr)
        m_rss = _TIME_RE_RSS.search(stderr)
        if not m_real or not m_rss:
            print(f"  {label:40s}  FAILED (could not parse /usr/bin/time output)")
            return float("inf"), 0

        times.append(float(m_real.group(1)))
        rss = int(m_rss.group(1))
        peak_rss = max(peak_rss, rss)

    best = min(times)
    median = sorted(times)[len(times) // 2]
    delta_rss = max(0, peak_rss - baseline_rss)
    print(
        f"  {label:40s}  best={fmt_time(best):>10s}"
        f"  median={fmt_time(median):>10s}"
        f"  peak_rss={fmt_mem(delta_rss):>10s}"
    )
    return best, delta_rss


def measure_baseline_rss(cache_dir: str, files: list[Path]) -> int:
    """Measure RSS of a no-op subprocess (import overhead only)."""
    cmd = [
        "/usr/bin/time",
        "-l",
        sys.executable,
        "-c",
        "",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    m = _TIME_RE_RSS.search(result.stderr)
    return int(m.group(1)) if m else 0


def main() -> None:
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
    parser.add_argument(
        "--run",
        metavar="BENCH",
        help="(internal) Run a single benchmark by name and exit",
    )
    args = parser.parse_args()

    files = sorted(args.files)
    cache_dir = str(args.cache)

    # Internal: run a single benchmark and exit
    if args.run:
        _run_bench(args.run, files, cache_dir)
        return

    n_repeat = args.repeat
    single_file = files[0]

    print(f"Files: {len(files)}")
    print(f"Cache: {cache_dir}")
    print(f"Repeats: {n_repeat}")

    baseline_rss = measure_baseline_rss(cache_dir, files)
    print(f"Baseline RSS (python -c ''): {fmt_mem(baseline_rss)}")
    print()

    def run(label: str, name: str, bench_files: list[Path] | None = None):
        return bench(
            label, name, bench_files or files, cache_dir, n_repeat, baseline_rss
        )

    # ── 1. Single file — all sensors ─────────────────────────────────
    print("=" * 78)
    print("1. Single file — all sensors")
    print("=" * 78)
    print(f"   File: {single_file.name}")

    run("xarray-dbd (open_dbd_dataset)", "xdbd_single_all", [single_file])
    run("xarray-dbd dbdreader2 (DBD.get)", "xdbd_dbdr2_single_all", [single_file])
    run("dbdreader (get per sensor)", "dbdreader_single_all_individual", [single_file])
    run("dbdreader (get all at once)", "dbdreader_single_all_batch", [single_file])
    print()

    # ── 2. Single file — filtered sensors ────────────────────────────
    print("=" * 78)
    print(f"2. Single file — {len(SENSORS_SUBSET)} sensors only")
    print("=" * 78)
    print(f"   Sensors: {SENSORS_SUBSET}")

    run("xarray-dbd (to_keep)", "xdbd_single_filtered", [single_file])
    run("xarray-dbd dbdreader2 (DBD.get)", "xdbd_dbdr2_single_filtered", [single_file])
    run("dbdreader (get per sensor)", "dbdreader_single_filtered_individual", [single_file])
    run("dbdreader (get all at once)", "dbdreader_single_filtered_batch", [single_file])
    print()

    # ── 3. Multi-file — all sensors ──────────────────────────────────
    print("=" * 78)
    print(f"3. Multi-file — {len(files)} files, all sensors")
    print("=" * 78)

    run("xarray-dbd (open_multi_dbd_dataset)", "xdbd_multi_all")
    run("xarray-dbd dbdreader2 (MultiDBD.get)", "xdbd_dbdr2_multi_all")
    run("dbdreader (get per sensor)", "dbdreader_multi_all_individual")
    run("dbdreader (get all at once)", "dbdreader_multi_all_batch")
    print()

    # ── 4. Multi-file — filtered sensors ─────────────────────────────
    print("=" * 78)
    print(f"4. Multi-file — {len(files)} files, {len(SENSORS_SUBSET)} sensors")
    print("=" * 78)

    run("xarray-dbd (to_keep)", "xdbd_multi_filtered")
    run("xarray-dbd dbdreader2 (MultiDBD.get)", "xdbd_dbdr2_multi_filtered")
    run("dbdreader (get per sensor)", "dbdreader_multi_filtered_individual")
    run("dbdreader (get all at once)", "dbdreader_multi_filtered_batch")
    print()

    # ── 5. NetCDF writer — dbd2netCDF vs xdbd 2nc ──────────────────
    print("=" * 78)
    print(f"5. NetCDF writer — {len(files)} files → NetCDF")
    print("=" * 78)

    dbd2netcdf_bin = shutil.which("dbd2netCDF")
    xdbd_bin = shutil.which("xdbd")
    tmpdir = tempfile.mkdtemp(prefix="bench_nc_")
    file_args = [str(f) for f in files]

    if dbd2netcdf_bin:
        cpp_out = str(Path(tmpdir) / "cpp.nc")
        bench_cmd(
            "C++ dbd2netCDF",
            [dbd2netcdf_bin, "-C", cache_dir, "-o", cpp_out] + file_args,
            n_repeat,
            baseline_rss,
        )
    else:
        print("  dbd2netCDF not found — skipping")

    if xdbd_bin:
        py_out = str(Path(tmpdir) / "py.nc")
        bench_cmd(
            "xdbd 2nc (streaming)",
            [xdbd_bin, "2nc", "-C", cache_dir, "-o", py_out] + file_args,
            n_repeat,
            baseline_rss,
        )
    else:
        print("  xdbd not found — skipping")

    # Show output file sizes
    for label, name in [("C++ dbd2netCDF", "cpp.nc"), ("xdbd 2nc", "py.nc")]:
        p = Path(tmpdir) / name
        if p.exists():
            print(f"  {label:40s}  output={p.stat().st_size / 1024 / 1024:.1f} MB")

    shutil.rmtree(tmpdir, ignore_errors=True)
    print()

    # ── 6. Data verification ─────────────────────────────────────────
    print("=" * 78)
    print("6. Data verification")
    print("=" * 78)

    import numpy as np

    import xarray_dbd as xdbd

    ds = xdbd.open_multi_dbd_dataset(files, cache_dir=cache_dir, to_keep=SENSORS_SUBSET)
    print(f"   xarray-dbd records: {len(ds.i):,}")

    try:
        import dbdreader

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
            print(
                f"   {sensor:30s}"
                f"  xarray-dbd valid={xdbd_valid:>8,}"
                f"  dbdreader={dbdr_valid:>8,}"
            )
        mdbd.close()
    except Exception as e:
        print(f"   dbdreader failed to open files: {e}")
        for sensor in SENSORS_SUBSET:
            if sensor == "m_present_time":
                continue
            xdbd_vals = ds[sensor].values
            xdbd_valid = int(np.sum(np.isfinite(xdbd_vals)))
            print(
                f"   {sensor:30s}"
                f"  xarray-dbd valid={xdbd_valid:>8,}"
                f"  dbdreader=     N/A"
            )


if __name__ == "__main__":
    main()
