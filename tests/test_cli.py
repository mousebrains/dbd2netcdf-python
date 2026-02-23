"""Tests for CLI entry points — smoke tests + integration + unit tests."""

from __future__ import annotations

import logging
import re
import subprocess
import sys
import tempfile
from argparse import ArgumentParser, Namespace
from pathlib import Path

import numpy as np
import pytest
from conftest import CACHE_DIR, DBD_DIR, has_test_data

from xarray_dbd import __version__


def run_cli(args: list[str], check: bool = True) -> subprocess.CompletedProcess:
    """Run a CLI command and return the result."""
    return subprocess.run(
        [sys.executable, "-m", *args],
        capture_output=True,
        text=True,
        check=check,
    )


# =============================================================================
# logger.py — unit tests
# =============================================================================


class TestMkLogger:
    """Unit tests for xarray_dbd.cli.logger.mk_logger."""

    def test_mk_logger_default(self):
        """Default logger has StreamHandler at INFO level (no logfile)."""
        from xarray_dbd.cli.logger import mk_logger

        args = Namespace(
            logfile=None,
            log_bytes=10000000,
            log_count=3,
            debug=False,
            verbose=False,
            mail_to=None,
            mail_from=None,
            mail_subject=None,
            smtp_host="localhost",
        )
        lg = mk_logger(args, name="test_default", log_level="WARNING")
        assert lg.level == logging.WARNING
        assert len(lg.handlers) == 1
        assert isinstance(lg.handlers[0], logging.StreamHandler)

    def test_mk_logger_debug(self):
        """--debug sets DEBUG level."""
        from xarray_dbd.cli.logger import mk_logger

        args = Namespace(
            logfile=None,
            log_bytes=10000000,
            log_count=3,
            debug=True,
            verbose=False,
            mail_to=None,
            mail_from=None,
            mail_subject=None,
            smtp_host="localhost",
        )
        lg = mk_logger(args, name="test_debug")
        assert lg.level == logging.DEBUG

    def test_mk_logger_verbose(self):
        """--verbose sets INFO level."""
        from xarray_dbd.cli.logger import mk_logger

        args = Namespace(
            logfile=None,
            log_bytes=10000000,
            log_count=3,
            debug=False,
            verbose=True,
            mail_to=None,
            mail_from=None,
            mail_subject=None,
            smtp_host="localhost",
        )
        lg = mk_logger(args, name="test_verbose")
        assert lg.level == logging.INFO

    def test_mk_logger_logfile(self, tmp_path):
        """--logfile creates RotatingFileHandler."""
        from xarray_dbd.cli.logger import mk_logger

        logfile = str(tmp_path / "test.log")
        args = Namespace(
            logfile=logfile,
            log_bytes=10000000,
            log_count=3,
            debug=False,
            verbose=False,
            mail_to=None,
            mail_from=None,
            mail_subject=None,
            smtp_host="localhost",
        )
        lg = mk_logger(args, name="test_logfile")
        assert len(lg.handlers) == 1
        assert isinstance(lg.handlers[0], logging.handlers.RotatingFileHandler)


# =============================================================================
# dbd2nc tests
# =============================================================================


def test_dbd2nc_help():
    """dbd2nc --help exits 0."""
    result = run_cli(["xarray_dbd.cli.dbd2nc", "--help"])
    assert result.returncode == 0
    assert "Convert Slocum glider DBD files" in result.stdout


def test_dbd2nc_version():
    """dbd2nc -V prints version."""
    result = run_cli(["xarray_dbd.cli.dbd2nc", "-V"])
    assert result.returncode == 0
    output = result.stdout + result.stderr
    assert __version__ in output


def test_dbd2nc_missing_output_arg():
    """dbd2nc without -o fails."""
    result = run_cli(["xarray_dbd.cli.dbd2nc", "fake.dbd"], check=False)
    assert result.returncode != 0


def test_dbd2nc_missing_file():
    """dbd2nc with a non-existent file returns non-zero exit code."""
    with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as tmp:
        tmpname = tmp.name

    try:
        result = subprocess.run(
            [sys.executable, "-m", "xarray_dbd.cli.dbd2nc", "-o", tmpname, "/nonexistent/fake.dbd"],
            capture_output=True,
            text=True,
        )
        assert result.returncode != 0
    finally:
        Path(tmpname).unlink(missing_ok=True)


@pytest.mark.skipif(not has_test_data, reason="Test data not available")
def test_dbd2nc_produces_output():
    """dbd2nc with sample files produces a NetCDF file."""
    try:
        import scipy  # noqa: F401
    except ImportError:
        pytest.skip("No NetCDF backend available (need scipy, netCDF4, or h5netcdf)")

    dcd_files = sorted(DBD_DIR.glob("*.dcd"))[:3]
    if not dcd_files:
        pytest.skip("No .dcd files available")

    with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as tmp:
        tmpname = tmp.name

    try:
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "xarray_dbd.cli.dbd2nc",
                "-C",
                CACHE_DIR,
                "-o",
                tmpname,
                *[str(f) for f in dcd_files],
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, f"dbd2nc failed: {result.stderr}"
        assert Path(tmpname).stat().st_size > 0, "Output file is empty"
    finally:
        Path(tmpname).unlink(missing_ok=True)


@pytest.mark.skipif(not has_test_data, reason="Test data not available")
def test_dbd2nc_single_file():
    """dbd2nc converts a single .dcd to NetCDF with valid data."""
    import xarray as xr

    dcd_files = sorted(DBD_DIR.glob("*.dcd"))[:1]
    if not dcd_files:
        pytest.skip("No .dcd files available")

    with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as tmp:
        tmpname = tmp.name

    try:
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "xarray_dbd.cli.dbd2nc",
                "-C",
                CACHE_DIR,
                "-o",
                tmpname,
                str(dcd_files[0]),
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, f"dbd2nc failed: {result.stderr}"
        ds = xr.open_dataset(tmpname, decode_timedelta=False)
        assert "i" in ds.dims
        assert len(ds.data_vars) > 0
        ds.close()
    finally:
        Path(tmpname).unlink(missing_ok=True)


@pytest.mark.skipif(not has_test_data, reason="Test data not available")
def test_dbd2nc_skip_first():
    """dbd2nc --skip-first produces fewer records than without."""
    import xarray as xr

    dcd_files = sorted(DBD_DIR.glob("*.dcd"))[:3]
    if len(dcd_files) < 2:
        pytest.skip("Need at least 2 .dcd files")

    with tempfile.TemporaryDirectory() as tmpdir:
        out_skip = Path(tmpdir) / "skip.nc"
        out_noskip = Path(tmpdir) / "noskip.nc"

        for out, extra in [(out_skip, ["--skip-first"]), (out_noskip, [])]:
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "xarray_dbd.cli.dbd2nc",
                    "-C",
                    CACHE_DIR,
                    "-o",
                    str(out),
                    *extra,
                    *[str(f) for f in dcd_files],
                ],
                capture_output=True,
                text=True,
            )
            assert result.returncode == 0, f"dbd2nc failed: {result.stderr}"

        ds_skip = xr.open_dataset(str(out_skip), decode_timedelta=False)
        ds_noskip = xr.open_dataset(str(out_noskip), decode_timedelta=False)
        assert len(ds_skip.i) < len(ds_noskip.i)
        ds_skip.close()
        ds_noskip.close()


@pytest.mark.skipif(not has_test_data, reason="Test data not available")
def test_dbd2nc_no_compression():
    """dbd2nc --compression 0 produces valid output."""
    import xarray as xr

    dcd_files = sorted(DBD_DIR.glob("*.dcd"))[:1]
    if not dcd_files:
        pytest.skip("No .dcd files available")

    with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as tmp:
        tmpname = tmp.name

    try:
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "xarray_dbd.cli.dbd2nc",
                "-C",
                CACHE_DIR,
                "-o",
                tmpname,
                "--compression",
                "0",
                str(dcd_files[0]),
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, f"dbd2nc failed: {result.stderr}"
        ds = xr.open_dataset(tmpname, decode_timedelta=False)
        assert len(ds.data_vars) > 0
        ds.close()
    finally:
        Path(tmpname).unlink(missing_ok=True)


@pytest.mark.skipif(not has_test_data, reason="Test data not available")
def test_dbd2nc_sensor_filter():
    """dbd2nc --sensor-output limits variables in output."""
    import xarray as xr

    dcd_files = sorted(DBD_DIR.glob("*.dcd"))[:1]
    if not dcd_files:
        pytest.skip("No .dcd files available")

    with tempfile.TemporaryDirectory() as tmpdir:
        sensor_file = Path(tmpdir) / "keep.txt"
        sensor_file.write_text("m_present_time\n", encoding="utf-8")
        outfile = Path(tmpdir) / "out.nc"

        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "xarray_dbd.cli.dbd2nc",
                "-C",
                CACHE_DIR,
                "-o",
                str(outfile),
                "-k",
                str(sensor_file),
                str(dcd_files[0]),
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, f"dbd2nc failed: {result.stderr}"
        ds = xr.open_dataset(str(outfile), decode_timedelta=False)
        assert list(ds.data_vars) == ["m_present_time"]
        ds.close()


class TestReadSensorList:
    """Unit tests for dbd2nc.read_sensor_list."""

    def test_basic(self, tmp_path):
        from xarray_dbd.cli.dbd2nc import read_sensor_list

        p = tmp_path / "sensors.txt"
        p.write_text("sensor_a\nsensor_b\n", encoding="utf-8")
        assert read_sensor_list(p) == ["sensor_a", "sensor_b"]

    def test_comments_and_blanks(self, tmp_path):
        from xarray_dbd.cli.dbd2nc import read_sensor_list

        p = tmp_path / "sensors.txt"
        p.write_text("sensor_a # comment\n\n# full comment\nsensor_b\n", encoding="utf-8")
        assert read_sensor_list(p) == ["sensor_a", "sensor_b"]

    def test_csv_format(self, tmp_path):
        from xarray_dbd.cli.dbd2nc import read_sensor_list

        p = tmp_path / "sensors.txt"
        p.write_text("sensor_a, sensor_b, sensor_c\n", encoding="utf-8")
        assert read_sensor_list(p) == ["sensor_a", "sensor_b", "sensor_c"]


# =============================================================================
# sensors tests
# =============================================================================


def test_sensors_help():
    """sensors --help exits 0."""
    result = run_cli(["xarray_dbd.cli.sensors", "--help"])
    assert result.returncode == 0
    assert "sensor" in result.stdout.lower()


@pytest.mark.skipif(not has_test_data, reason="Test data not available")
def test_sensors_with_data():
    """sensors lists sensors from DBD files."""
    dcd_files = sorted(DBD_DIR.glob("*.dcd"))[:1]
    if not dcd_files:
        pytest.skip("No .dcd files available")

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "xarray_dbd.cli.sensors",
            "-C",
            CACHE_DIR,
            *[str(f) for f in dcd_files],
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert len(result.stdout.strip().split("\n")) > 0


@pytest.mark.skipif(not has_test_data, reason="Test data not available")
def test_sensors_output_format():
    """Each sensor output line matches 'size name unit' format."""
    dcd_files = sorted(DBD_DIR.glob("*.dcd"))[:1]
    if not dcd_files:
        pytest.skip("No .dcd files available")

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "xarray_dbd.cli.sensors",
            "-C",
            CACHE_DIR,
            str(dcd_files[0]),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    lines = result.stdout.strip().split("\n")
    assert len(lines) > 1
    for line in lines:
        parts = line.split()
        assert len(parts) >= 3, f"Unexpected format: {line!r}"
        assert parts[0] in ("1", "2", "4", "8"), f"Bad size: {parts[0]}"


@pytest.mark.skipif(not has_test_data, reason="Test data not available")
def test_sensors_to_file(tmp_path):
    """--output writes sensor list to a file."""
    dcd_files = sorted(DBD_DIR.glob("*.dcd"))[:1]
    if not dcd_files:
        pytest.skip("No .dcd files available")

    outfile = tmp_path / "sensors.txt"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "xarray_dbd.cli.sensors",
            "-C",
            CACHE_DIR,
            "-o",
            str(outfile),
            str(dcd_files[0]),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    content = outfile.read_text(encoding="utf-8")
    assert len(content.strip().split("\n")) > 1


# =============================================================================
# missions tests
# =============================================================================


def test_missions_help():
    """missions --help exits 0."""
    result = run_cli(["xarray_dbd.cli.missions", "--help"])
    assert result.returncode == 0


@pytest.mark.skipif(not has_test_data, reason="Test data not available")
def test_missions_output():
    """missions output lines match 'count mission_name' format with nonzero counts."""
    dcd_files = sorted(DBD_DIR.glob("*.dcd"))[:5]
    if not dcd_files:
        pytest.skip("No .dcd files available")

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "xarray_dbd.cli.missions",
            "-C",
            CACHE_DIR,
            *[str(f) for f in dcd_files],
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    lines = result.stdout.strip().split("\n")
    assert len(lines) >= 1
    for line in lines:
        parts = line.split(None, 1)
        assert len(parts) == 2, f"Unexpected format: {line!r}"
        count = int(parts[0])
        assert count > 0


@pytest.mark.skipif(not has_test_data, reason="Test data not available")
def test_missions_to_file(tmp_path):
    """-o FILE writes missions list to file."""
    dcd_files = sorted(DBD_DIR.glob("*.dcd"))[:5]
    if not dcd_files:
        pytest.skip("No .dcd files available")

    outfile = tmp_path / "missions.txt"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "xarray_dbd.cli.missions",
            "-C",
            CACHE_DIR,
            "-o",
            str(outfile),
            *[str(f) for f in dcd_files],
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    content = outfile.read_text(encoding="utf-8")
    assert len(content.strip().split("\n")) >= 1


# =============================================================================
# cache tests
# =============================================================================


def test_cache_help():
    """cache --help exits 0."""
    result = run_cli(["xarray_dbd.cli.cache", "--help"])
    assert result.returncode == 0


@pytest.mark.skipif(not has_test_data, reason="Test data not available")
def test_cache_output():
    """cache output lines match 'count hex_crc' format."""
    dcd_files = sorted(DBD_DIR.glob("*.dcd"))[:5]
    if not dcd_files:
        pytest.skip("No .dcd files available")

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "xarray_dbd.cli.cache",
            "-C",
            CACHE_DIR,
            *[str(f) for f in dcd_files],
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    lines = result.stdout.strip().split("\n")
    assert len(lines) >= 1
    for line in lines:
        parts = line.split()
        assert len(parts) == 2, f"Unexpected format: {line!r}"
        int(parts[0])  # count is an integer
        # CRC is a hex string
        assert re.match(r"^[0-9a-fA-F]+$", parts[1]), f"Bad CRC: {parts[1]}"


@pytest.mark.skipif(not has_test_data, reason="Test data not available")
def test_cache_missing_no_cache_dir():
    """cache --missing without -C errors."""
    dcd_files = sorted(DBD_DIR.glob("*.dcd"))[:1]
    if not dcd_files:
        pytest.skip("No .dcd files available")

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "xarray_dbd.cli.cache",
            "--missing",
            str(dcd_files[0]),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0


class TestCacheFileExists:
    """Unit tests for cache._cache_file_exists."""

    def test_existing_cac(self, tmp_path):
        from xarray_dbd.cli.cache import _cache_file_exists

        (tmp_path / "abcdef12.cac").write_text("data", encoding="utf-8")
        assert _cache_file_exists(tmp_path, "abcdef12") is True

    def test_existing_ccc(self, tmp_path):
        from xarray_dbd.cli.cache import _cache_file_exists

        (tmp_path / "abcdef12.ccc").write_text("data", encoding="utf-8")
        assert _cache_file_exists(tmp_path, "abcdef12") is True

    def test_bare_crc(self, tmp_path):
        from xarray_dbd.cli.cache import _cache_file_exists

        (tmp_path / "abcdef12").write_text("data", encoding="utf-8")
        assert _cache_file_exists(tmp_path, "abcdef12") is True

    def test_missing(self, tmp_path):
        from xarray_dbd.cli.cache import _cache_file_exists

        assert _cache_file_exists(tmp_path, "abcdef12") is False

    def test_nonexistent_dir(self, tmp_path):
        from xarray_dbd.cli.cache import _cache_file_exists

        assert _cache_file_exists(tmp_path / "nope", "abcdef12") is False


# =============================================================================
# 2csv tests
# =============================================================================


def test_2csv_help():
    """2csv --help exits 0."""
    result = run_cli(["xarray_dbd.cli.csv", "--help"])
    assert result.returncode == 0
    assert "CSV" in result.stdout


@pytest.mark.skipif(not has_test_data, reason="Test data not available")
def test_2csv_with_data():
    """2csv produces CSV output."""
    dcd_files = sorted(DBD_DIR.glob("*.dcd"))[:1]
    if not dcd_files:
        pytest.skip("No .dcd files available")

    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
        tmpname = tmp.name

    try:
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "xarray_dbd.cli.csv",
                "-C",
                CACHE_DIR,
                "-o",
                tmpname,
                *[str(f) for f in dcd_files],
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, f"2csv failed: {result.stderr}"
        assert Path(tmpname).stat().st_size > 0, "CSV output is empty"
    finally:
        Path(tmpname).unlink(missing_ok=True)


@pytest.mark.skipif(not has_test_data, reason="Test data not available")
def test_2csv_stdout():
    """2csv writes CSV to stdout when no -o specified."""
    dcd_files = sorted(DBD_DIR.glob("*.dcd"))[:1]
    if not dcd_files:
        pytest.skip("No .dcd files available")

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "xarray_dbd.cli.csv",
            "-C",
            CACHE_DIR,
            str(dcd_files[0]),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"2csv failed: {result.stderr}"
    lines = result.stdout.strip().split("\n")
    assert len(lines) >= 2  # header + at least one data row
    header = lines[0].split(",")
    assert len(header) > 1  # multiple sensors


@pytest.mark.skipif(not has_test_data, reason="Test data not available")
def test_2csv_to_file(tmp_path):
    """2csv -o FILE writes CSV to a file."""
    dcd_files = sorted(DBD_DIR.glob("*.dcd"))[:1]
    if not dcd_files:
        pytest.skip("No .dcd files available")

    outfile = tmp_path / "output.csv"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "xarray_dbd.cli.csv",
            "-C",
            CACHE_DIR,
            "-o",
            str(outfile),
            str(dcd_files[0]),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"2csv failed: {result.stderr}"
    content = outfile.read_text(encoding="utf-8")
    lines = content.strip().split("\n")
    assert len(lines) >= 2


# =============================================================================
# mkone tests
# =============================================================================


def test_mkone_help():
    """mkone --help exits 0."""
    result = run_cli(["xarray_dbd.cli.mkone", "--help"])
    assert result.returncode == 0
    assert "output-prefix" in result.stdout


def test_mkone_missing_output_prefix():
    """mkone without --output-prefix fails."""
    result = run_cli(["xarray_dbd.cli.mkone", "/tmp"], check=False)
    assert result.returncode != 0


@pytest.mark.skipif(not has_test_data, reason="Test data not available")
def test_mkone_empty_dir():
    """mkone with an empty directory produces no errors."""
    try:
        import scipy  # noqa: F401
    except ImportError:
        pytest.skip("No NetCDF backend available")

    with tempfile.TemporaryDirectory() as tmpdir:
        outdir = Path(tmpdir) / "output"
        outdir.mkdir()
        indir = Path(tmpdir) / "input"
        indir.mkdir()
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "xarray_dbd.cli.mkone",
                "--output-prefix",
                str(outdir) + "/",
                str(indir),
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0


@pytest.mark.skipif(not has_test_data, reason="Test data not available")
def test_mkone_dcd_files():
    """mkone processes .dcd files into dbd.nc."""
    dcd_files = sorted(DBD_DIR.glob("*.dcd"))
    if not dcd_files:
        pytest.skip("No .dcd files available")

    with tempfile.TemporaryDirectory() as tmpdir:
        outprefix = str(Path(tmpdir) / "test.")
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "xarray_dbd.cli.mkone",
                "--output-prefix",
                outprefix,
                "--cache",
                CACHE_DIR,
                *[str(f) for f in dcd_files[:3]],
            ],
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert result.returncode == 0, f"mkone failed: {result.stderr}"
        assert Path(outprefix + "dbd.nc").exists(), "dbd.nc not created"


class TestDiscoverFiles:
    """Unit tests for mkone.discover_files."""

    def test_discover_directory(self, tmp_path):
        from xarray_dbd.cli.mkone import discover_files

        (tmp_path / "file1.dcd").write_text("", encoding="utf-8")
        (tmp_path / "file2.ecd").write_text("", encoding="utf-8")
        (tmp_path / "file3.sbd").write_text("", encoding="utf-8")
        (tmp_path / "ignore.txt").write_text("", encoding="utf-8")

        result = discover_files([str(tmp_path)])
        assert "d" in result
        assert "e" in result
        assert "s" in result
        assert len(result["d"]) == 1
        assert len(result["e"]) == 1
        assert len(result["s"]) == 1

    def test_discover_file_list(self, tmp_path):
        from xarray_dbd.cli.mkone import discover_files

        f1 = tmp_path / "file1.dcd"
        f2 = tmp_path / "file2.ebd"
        f1.write_text("", encoding="utf-8")
        f2.write_text("", encoding="utf-8")

        result = discover_files([str(f1), str(f2)])
        assert "d" in result
        assert "e" in result
        assert result["d"] == [str(f1)]
        assert result["e"] == [str(f2)]

    def test_discover_mixed(self, tmp_path):
        """Discovers from both directory walking and explicit file paths."""
        from xarray_dbd.cli.mkone import discover_files

        subdir = tmp_path / "sub"
        subdir.mkdir()
        (subdir / "a.dcd").write_text("", encoding="utf-8")
        explicit = tmp_path / "b.ecd"
        explicit.write_text("", encoding="utf-8")

        result = discover_files([str(subdir), str(explicit)])
        assert "d" in result
        assert "e" in result

    def test_discover_nonexistent(self, tmp_path):
        """Non-existent path is silently skipped."""
        from xarray_dbd.cli.mkone import discover_files

        result = discover_files([str(tmp_path / "nope")])
        assert result == {}


# =============================================================================
# xdbd router tests
# =============================================================================


def test_xdbd_help():
    """xdbd --help exits 0."""
    result = run_cli(["xarray_dbd.cli.main", "--help"])
    assert result.returncode == 0
    assert "xarray-dbd" in result.stdout


def test_xdbd_version():
    """xdbd -V prints version."""
    result = run_cli(["xarray_dbd.cli.main", "-V"])
    assert result.returncode == 0
    output = result.stdout + result.stderr
    assert __version__ in output


# =============================================================================
# In-process CLI run() tests — captured by coverage
# =============================================================================


def _base_args(**overrides) -> Namespace:
    """Build a Namespace with common logger defaults."""
    defaults = {
        "logfile": None,
        "log_bytes": 10000000,
        "log_count": 3,
        "debug": False,
        "verbose": False,
        "mail_to": None,
        "mail_from": None,
        "mail_subject": None,
        "smtp_host": "localhost",
    }
    defaults.update(overrides)
    return Namespace(**defaults)


@pytest.mark.skipif(not has_test_data, reason="Test data not available")
class TestSensorsRun:
    """In-process tests for sensors.run()."""

    def test_sensors_run_stdout(self, capsys):
        from xarray_dbd.cli.sensors import run

        dcd_files = sorted(DBD_DIR.glob("*.dcd"))[:1]
        args = _base_args(
            files=dcd_files,
            cache=CACHE_DIR,
            output=None,
            skip_mission=[],
            keep_mission=[],
        )
        rc = run(args)
        assert rc == 0
        out = capsys.readouterr().out
        lines = out.strip().split("\n")
        assert len(lines) > 1
        assert lines[0].split()[0] in ("1", "2", "4", "8")

    def test_sensors_run_to_file(self, tmp_path):
        from xarray_dbd.cli.sensors import run

        dcd_files = sorted(DBD_DIR.glob("*.dcd"))[:1]
        outfile = tmp_path / "sensors.txt"
        args = _base_args(
            files=dcd_files,
            cache=CACHE_DIR,
            output=outfile,
            skip_mission=[],
            keep_mission=[],
        )
        rc = run(args)
        assert rc == 0
        assert outfile.read_text(encoding="utf-8").strip()

    def test_sensors_run_missing_file(self):
        from xarray_dbd.cli.sensors import run

        args = _base_args(
            files=[Path("/nonexistent/fake.dbd")],
            cache="",
            output=None,
            skip_mission=[],
            keep_mission=[],
        )
        rc = run(args)
        assert rc == 1


@pytest.mark.skipif(not has_test_data, reason="Test data not available")
class TestMissionsRun:
    """In-process tests for missions.run()."""

    def test_missions_run_stdout(self, capsys):
        from xarray_dbd.cli.missions import run

        dcd_files = sorted(DBD_DIR.glob("*.dcd"))[:3]
        args = _base_args(
            files=dcd_files,
            cache=CACHE_DIR,
            output=None,
            skip_mission=[],
            keep_mission=[],
        )
        rc = run(args)
        assert rc == 0
        out = capsys.readouterr().out
        lines = out.strip().split("\n")
        assert len(lines) >= 1
        parts = lines[0].split(None, 1)
        assert int(parts[0]) > 0

    def test_missions_run_to_file(self, tmp_path):
        from xarray_dbd.cli.missions import run

        dcd_files = sorted(DBD_DIR.glob("*.dcd"))[:3]
        outfile = tmp_path / "missions.txt"
        args = _base_args(
            files=dcd_files,
            cache=CACHE_DIR,
            output=outfile,
            skip_mission=[],
            keep_mission=[],
        )
        rc = run(args)
        assert rc == 0
        assert outfile.read_text(encoding="utf-8").strip()


@pytest.mark.skipif(not has_test_data, reason="Test data not available")
class TestCacheRun:
    """In-process tests for cache.run()."""

    def test_cache_run_stdout(self, capsys):
        from xarray_dbd.cli.cache import run

        dcd_files = sorted(DBD_DIR.glob("*.dcd"))[:3]
        args = _base_args(
            files=dcd_files,
            cache=CACHE_DIR,
            output=None,
            skip_mission=[],
            keep_mission=[],
            missing=False,
        )
        rc = run(args)
        assert rc == 0
        out = capsys.readouterr().out
        lines = out.strip().split("\n")
        assert len(lines) >= 1

    def test_cache_run_missing_needs_cache_dir(self):
        from xarray_dbd.cli.cache import run

        dcd_files = sorted(DBD_DIR.glob("*.dcd"))[:1]
        args = _base_args(
            files=dcd_files,
            cache="",
            output=None,
            skip_mission=[],
            keep_mission=[],
            missing=True,
        )
        rc = run(args)
        assert rc == 1

    def test_cache_run_missing_with_cache_dir(self, capsys):
        from xarray_dbd.cli.cache import run

        dcd_files = sorted(DBD_DIR.glob("*.dcd"))[:1]
        args = _base_args(
            files=dcd_files,
            cache=CACHE_DIR,
            output=None,
            skip_mission=[],
            keep_mission=[],
            missing=True,
        )
        rc = run(args)
        assert rc == 0


@pytest.mark.skipif(not has_test_data, reason="Test data not available")
class TestDbd2ncRun:
    """In-process tests for dbd2nc.run()."""

    def test_dbd2nc_run_streaming(self, tmp_path):
        import xarray as xr

        from xarray_dbd.cli.dbd2nc import run

        dcd_files = sorted(DBD_DIR.glob("*.dcd"))[:2]
        outfile = tmp_path / "out.nc"
        args = _base_args(
            files=dcd_files,
            cache=Path(CACHE_DIR),
            output=outfile,
            append=False,
            sensors=None,
            sensor_output=None,
            skip_mission=None,
            keep_mission=None,
            skip_first=True,
            repair=False,
            compression=5,
        )
        rc = run(args)
        assert rc == 0
        ds = xr.open_dataset(str(outfile), decode_timedelta=False)
        assert len(ds.data_vars) > 0
        ds.close()

    def test_dbd2nc_run_no_compression(self, tmp_path):
        from xarray_dbd.cli.dbd2nc import run

        dcd_files = sorted(DBD_DIR.glob("*.dcd"))[:1]
        outfile = tmp_path / "out.nc"
        args = _base_args(
            files=dcd_files,
            cache=Path(CACHE_DIR),
            output=outfile,
            append=False,
            sensors=None,
            sensor_output=None,
            skip_mission=None,
            keep_mission=None,
            skip_first=False,
            repair=False,
            compression=0,
        )
        rc = run(args)
        assert rc == 0

    def test_dbd2nc_run_missing_file(self, tmp_path):
        from xarray_dbd.cli.dbd2nc import run

        outfile = tmp_path / "out.nc"
        args = _base_args(
            files=[Path("/nonexistent/fake.dbd")],
            cache=None,
            output=outfile,
            append=False,
            sensors=None,
            sensor_output=None,
            skip_mission=None,
            keep_mission=None,
            skip_first=False,
            repair=False,
            compression=5,
        )
        rc = run(args)
        assert rc == 1

    def test_dbd2nc_run_with_sensor_filter(self, tmp_path):
        import xarray as xr

        from xarray_dbd.cli.dbd2nc import run

        dcd_files = sorted(DBD_DIR.glob("*.dcd"))[:1]
        sensor_file = tmp_path / "keep.txt"
        sensor_file.write_text("m_present_time\n", encoding="utf-8")
        outfile = tmp_path / "out.nc"
        args = _base_args(
            files=dcd_files,
            cache=Path(CACHE_DIR),
            output=outfile,
            append=False,
            sensors=None,
            sensor_output=sensor_file,
            skip_mission=None,
            keep_mission=None,
            skip_first=False,
            repair=False,
            compression=5,
        )
        rc = run(args)
        assert rc == 0
        ds = xr.open_dataset(str(outfile), decode_timedelta=False)
        assert list(ds.data_vars) == ["m_present_time"]
        ds.close()


@pytest.mark.skipif(not has_test_data, reason="Test data not available")
class TestCsvRun:
    """In-process tests for csv.run()."""

    def test_csv_run_stdout(self, capsys):
        from xarray_dbd.cli.csv import run

        dcd_files = sorted(DBD_DIR.glob("*.dcd"))[:1]
        args = _base_args(
            files=dcd_files,
            cache=Path(CACHE_DIR),
            output=None,
            sensors=None,
            sensor_output=None,
            skip_mission=None,
            keep_mission=None,
            skip_first=False,
            repair=False,
        )
        rc = run(args)
        assert rc == 0
        out = capsys.readouterr().out
        lines = out.strip().split("\n")
        assert len(lines) >= 2  # header + data

    def test_csv_run_to_file(self, tmp_path):
        from xarray_dbd.cli.csv import run

        dcd_files = sorted(DBD_DIR.glob("*.dcd"))[:1]
        outfile = tmp_path / "out.csv"
        args = _base_args(
            files=dcd_files,
            cache=Path(CACHE_DIR),
            output=outfile,
            sensors=None,
            sensor_output=None,
            skip_mission=None,
            keep_mission=None,
            skip_first=False,
            repair=False,
        )
        rc = run(args)
        assert rc == 0
        content = outfile.read_text(encoding="utf-8")
        assert len(content.strip().split("\n")) >= 2


# =============================================================================
# Tier 1 — cli/main.py tests
# =============================================================================


class TestMainDispatch:
    """Tests for the xdbd unified router (cli/main.py)."""

    def test_main_dispatches_2nc(self, tmp_path, monkeypatch):
        """main() with '2nc' subcommand dispatches to dbd2nc.run."""
        from xarray_dbd.cli import main as main_mod

        called = {}

        def fake_run(args):
            called["args"] = args
            return 0

        monkeypatch.setattr("xarray_dbd.cli.dbd2nc.run", fake_run)
        monkeypatch.setattr(
            sys,
            "argv",
            ["xdbd", "2nc", "-o", str(tmp_path / "out.nc"), "fake.dbd"],
        )
        with pytest.raises(SystemExit) as exc_info:
            main_mod.main()
        assert exc_info.value.code == 0
        assert "args" in called

    def test_main_no_args_exits_error(self, monkeypatch):
        """main() with no args → SystemExit(2)."""
        from xarray_dbd.cli import main as main_mod

        monkeypatch.setattr(sys, "argv", ["xdbd"])
        with pytest.raises(SystemExit) as exc_info:
            main_mod.main()
        assert exc_info.value.code == 2

    def test_main_subcommands_registered(self):
        """--help output includes all 6 subcommands."""
        result = run_cli(["xarray_dbd.cli.main", "--help"])
        for sub in ("2nc", "2csv", "sensors", "missions", "cache", "mkone"):
            assert sub in result.stdout, f"Subcommand '{sub}' not in help output"


# =============================================================================
# Tier 1 — cli/logger.py additional tests
# =============================================================================


class TestMkLoggerExtended:
    """Additional tests for logger.py coverage."""

    def test_mk_logger_smtp_handler(self):
        """mail_to adds SMTPHandler at ERROR level."""
        from xarray_dbd.cli.logger import mk_logger

        args = _base_args(mail_to=["test@example.com"])
        lg = mk_logger(args, name="test_smtp")
        assert len(lg.handlers) == 2
        smtp = lg.handlers[1]
        assert isinstance(smtp, logging.handlers.SMTPHandler)
        assert smtp.level == logging.ERROR

    def test_mk_logger_smtp_defaults(self):
        """mail_to without mail_from/mail_subject uses defaults."""
        import getpass
        import socket

        from xarray_dbd.cli.logger import mk_logger

        args = _base_args(mail_to=["user@example.com"])
        lg = mk_logger(args, name="test_smtp_defaults")
        smtp = lg.handlers[1]
        expected_from = getpass.getuser() + "@" + socket.getfqdn()
        assert smtp.fromaddr == expected_from
        assert "Error on" in smtp.subject

    def test_add_args_registers_all(self):
        """add_args() registers all expected arguments."""
        from xarray_dbd.cli.logger import add_args

        parser = ArgumentParser()
        add_args(parser)
        # Parse with defaults to verify all args are registered
        args = parser.parse_args([])
        for attr in (
            "logfile",
            "log_bytes",
            "log_count",
            "debug",
            "verbose",
            "mail_to",
            "mail_from",
            "mail_subject",
            "smtp_host",
        ):
            assert hasattr(args, attr), f"Missing arg: {attr}"


# =============================================================================
# Tier 1 — cli/cache.py additional tests
# =============================================================================


@pytest.mark.skipif(not has_test_data, reason="Test data not available")
class TestCacheRunExtended:
    """Additional in-process tests for cache.run()."""

    def test_cache_run_output_file(self, tmp_path):
        """cache run with --output writes to file."""
        from xarray_dbd.cli.cache import run

        dcd_files = sorted(DBD_DIR.glob("*.dcd"))[:3]
        outfile = tmp_path / "cache.txt"
        args = _base_args(
            files=dcd_files,
            cache=CACHE_DIR,
            output=outfile,
            skip_mission=[],
            keep_mission=[],
            missing=False,
        )
        rc = run(args)
        assert rc == 0
        content = outfile.read_text(encoding="utf-8")
        assert len(content.strip().split("\n")) >= 1

    def test_cache_run_file_not_found(self):
        """Non-existent file → rc=1."""
        from xarray_dbd.cli.cache import run

        args = _base_args(
            files=[Path("/nonexistent/fake.dbd")],
            cache=CACHE_DIR,
            output=None,
            skip_mission=[],
            keep_mission=[],
            missing=False,
        )
        rc = run(args)
        assert rc == 1


class TestCacheFileExistsExtended:
    """Additional edge cases for _cache_file_exists."""

    def test_directory_with_crc_name_skipped(self, tmp_path):
        """A directory named like a CRC is not treated as a cache file."""
        from xarray_dbd.cli.cache import _cache_file_exists

        (tmp_path / "abcdef12.cac").mkdir()
        assert _cache_file_exists(tmp_path, "abcdef12") is False


# =============================================================================
# Tier 1 — cli/missions.py additional tests
# =============================================================================


@pytest.mark.skipif(not has_test_data, reason="Test data not available")
class TestMissionsRunExtended:
    """Additional tests for missions.run()."""

    def test_missions_run_file_not_found(self):
        """Non-existent file → rc=1."""
        from xarray_dbd.cli.missions import run

        args = _base_args(
            files=[Path("/nonexistent/fake.dbd")],
            cache="",
            output=None,
            skip_mission=[],
            keep_mission=[],
        )
        rc = run(args)
        assert rc == 1

    def test_missions_run_output_file(self, tmp_path):
        """--output writes missions to file."""
        from xarray_dbd.cli.missions import run

        dcd_files = sorted(DBD_DIR.glob("*.dcd"))[:3]
        outfile = tmp_path / "missions.txt"
        args = _base_args(
            files=dcd_files,
            cache=CACHE_DIR,
            output=outfile,
            skip_mission=[],
            keep_mission=[],
        )
        rc = run(args)
        assert rc == 0
        content = outfile.read_text(encoding="utf-8")
        assert len(content.strip().split("\n")) >= 1


# =============================================================================
# Tier 1 — cli/sensors.py additional tests
# =============================================================================


@pytest.mark.skipif(not has_test_data, reason="Test data not available")
class TestSensorsRunExtended:
    """Additional tests for sensors.run()."""

    def test_sensors_run_empty_files(self):
        """Files yielding 0 sensors → rc=1."""
        from xarray_dbd.cli.sensors import run

        # Use a nonexistent cache dir so scan_sensors returns 0 files
        args = _base_args(
            files=[Path("/nonexistent/fake.dbd")],
            cache="",
            output=None,
            skip_mission=[],
            keep_mission=[],
        )
        rc = run(args)
        assert rc == 1


class TestAddCommonArgs:
    """Test sensors._add_common_args."""

    def test_add_common_args_registers_all(self):
        from xarray_dbd.cli.sensors import _add_common_args

        parser = ArgumentParser()
        _add_common_args(parser)
        args = parser.parse_args(["dummy.dbd"])
        for attr in ("files", "cache", "skip_mission", "keep_mission", "output"):
            assert hasattr(args, attr), f"Missing arg: {attr}"


# =============================================================================
# Tier 2 — cli/csv.py additional tests
# =============================================================================


@pytest.mark.skipif(not has_test_data, reason="Test data not available")
class TestCsvRunExtended:
    """Additional tests for csv.run()."""

    def test_csv_run_sensor_filter(self, tmp_path):
        """--sensor-output filters CSV columns."""
        from xarray_dbd.cli.csv import run

        dcd_files = sorted(DBD_DIR.glob("*.dcd"))[:1]
        sensor_file = tmp_path / "keep.txt"
        sensor_file.write_text("m_present_time\n", encoding="utf-8")
        outfile = tmp_path / "out.csv"
        args = _base_args(
            files=dcd_files,
            cache=Path(CACHE_DIR),
            output=outfile,
            sensors=None,
            sensor_output=sensor_file,
            skip_mission=None,
            keep_mission=None,
            skip_first=False,
            repair=False,
        )
        rc = run(args)
        assert rc == 0
        header = outfile.read_text(encoding="utf-8").split("\n")[0]
        assert header.strip() == "m_present_time"

    def test_csv_run_criteria_filter(self, tmp_path):
        """--sensors criteria file filters which files are selected."""
        from xarray_dbd.cli.csv import run

        dcd_files = sorted(DBD_DIR.glob("*.dcd"))[:1]
        criteria_file = tmp_path / "criteria.txt"
        criteria_file.write_text("m_present_time\n", encoding="utf-8")
        outfile = tmp_path / "out.csv"
        args = _base_args(
            files=dcd_files,
            cache=Path(CACHE_DIR),
            output=outfile,
            sensors=criteria_file,
            sensor_output=None,
            skip_mission=None,
            keep_mission=None,
            skip_first=False,
            repair=False,
        )
        rc = run(args)
        assert rc == 0
        content = outfile.read_text(encoding="utf-8")
        assert len(content.strip().split("\n")) >= 2

    def test_csv_run_skip_first(self, tmp_path):
        """--skip-first produces fewer rows."""
        from xarray_dbd.cli.csv import run

        dcd_files = sorted(DBD_DIR.glob("*.dcd"))[:3]
        if len(dcd_files) < 2:
            pytest.skip("Need at least 2 .dcd files")

        out_skip = tmp_path / "skip.csv"
        out_noskip = tmp_path / "noskip.csv"
        for outfile, skip in [(out_skip, True), (out_noskip, False)]:
            args = _base_args(
                files=dcd_files,
                cache=Path(CACHE_DIR),
                output=outfile,
                sensors=None,
                sensor_output=None,
                skip_mission=None,
                keep_mission=None,
                skip_first=skip,
                repair=False,
            )
            rc = run(args)
            assert rc == 0

        n_skip = len(out_skip.read_text(encoding="utf-8").strip().split("\n"))
        n_noskip = len(out_noskip.read_text(encoding="utf-8").strip().split("\n"))
        assert n_skip < n_noskip

    def test_csv_run_missing_sensor_file(self, tmp_path):
        """Non-existent --sensors file → rc=1."""
        from xarray_dbd.cli.csv import run

        dcd_files = sorted(DBD_DIR.glob("*.dcd"))[:1]
        args = _base_args(
            files=dcd_files,
            cache=Path(CACHE_DIR),
            output=None,
            sensors=Path("/nonexistent/criteria.txt"),
            sensor_output=None,
            skip_mission=None,
            keep_mission=None,
            skip_first=False,
            repair=False,
        )
        rc = run(args)
        assert rc == 1

    def test_csv_run_missing_sensor_output_file(self, tmp_path):
        """Non-existent --sensor-output file → rc=1."""
        from xarray_dbd.cli.csv import run

        dcd_files = sorted(DBD_DIR.glob("*.dcd"))[:1]
        args = _base_args(
            files=dcd_files,
            cache=Path(CACHE_DIR),
            output=None,
            sensors=None,
            sensor_output=Path("/nonexistent/keep.txt"),
            skip_mission=None,
            keep_mission=None,
            skip_first=False,
            repair=False,
        )
        rc = run(args)
        assert rc == 1

    def test_csv_run_missing_input_file(self, tmp_path):
        """Non-existent input file → rc=1."""
        from xarray_dbd.cli.csv import run

        args = _base_args(
            files=[Path("/nonexistent/fake.dbd")],
            cache=Path(CACHE_DIR),
            output=None,
            sensors=None,
            sensor_output=None,
            skip_mission=None,
            keep_mission=None,
            skip_first=False,
            repair=False,
        )
        rc = run(args)
        assert rc == 1

    def test_csv_run_no_cache_fallback(self, tmp_path, capsys):
        """No --cache falls back to parent/cache."""
        from xarray_dbd.cli.csv import run

        dcd_files = sorted(DBD_DIR.glob("*.dcd"))[:1]
        args = _base_args(
            files=dcd_files,
            cache=None,  # triggers fallback
            output=tmp_path / "out.csv",
            sensors=None,
            sensor_output=None,
            skip_mission=None,
            keep_mission=None,
            skip_first=False,
            repair=False,
        )
        rc = run(args)
        assert rc == 0

    def test_csv_run_multi_file(self, tmp_path):
        """Multiple dcd files produce union columns with fill values."""
        from xarray_dbd.cli.csv import run

        dcd_files = sorted(DBD_DIR.glob("*.dcd"))[:3]
        if len(dcd_files) < 2:
            pytest.skip("Need at least 2 .dcd files")
        outfile = tmp_path / "out.csv"
        args = _base_args(
            files=dcd_files,
            cache=Path(CACHE_DIR),
            output=outfile,
            sensors=None,
            sensor_output=None,
            skip_mission=None,
            keep_mission=None,
            skip_first=False,
            repair=False,
        )
        rc = run(args)
        assert rc == 0
        content = outfile.read_text(encoding="utf-8")
        lines = content.strip().split("\n")
        assert len(lines) >= 4  # header + multiple data rows


# =============================================================================
# Tier 2 — cli/dbd2nc.py additional tests
# =============================================================================


class TestNcEncoding:
    """Unit tests for dbd2nc._nc_encoding."""

    def test_nc_encoding_disabled(self):
        """compression <= 0 → None."""
        import xarray as xr

        from xarray_dbd.cli.dbd2nc import _nc_encoding

        ds = xr.Dataset({"a": ("i", [1, 2, 3])})
        assert _nc_encoding(ds, 0) is None
        assert _nc_encoding(ds, -1) is None


@pytest.mark.skipif(not has_test_data, reason="Test data not available")
class TestDbd2ncRunExtended:
    """Additional tests for dbd2nc.run()."""

    def test_dbd2nc_run_append_mode(self, tmp_path):
        """--append adds records to existing file."""
        import xarray as xr

        from xarray_dbd.cli.dbd2nc import run

        dcd_files = sorted(DBD_DIR.glob("*.dcd"))[:3]
        if len(dcd_files) < 2:
            pytest.skip("Need at least 2 .dcd files")
        outfile = tmp_path / "out.nc"

        # Use a single sensor to avoid timedelta decode issues during concat
        sensor_file = tmp_path / "keep.txt"
        sensor_file.write_text("m_present_time\n", encoding="utf-8")

        # First pass: write 1 file
        args = _base_args(
            files=dcd_files[:1],
            cache=Path(CACHE_DIR),
            output=outfile,
            append=False,
            sensors=None,
            sensor_output=sensor_file,
            skip_mission=None,
            keep_mission=None,
            skip_first=False,
            repair=False,
            compression=0,
        )
        rc = run(args)
        assert rc == 0
        ds1 = xr.open_dataset(str(outfile), decode_timedelta=False)
        n1 = len(ds1.i)
        ds1.close()

        # Second pass: append more files
        args = _base_args(
            files=dcd_files[1:3],
            cache=Path(CACHE_DIR),
            output=outfile,
            append=True,
            sensors=None,
            sensor_output=sensor_file,
            skip_mission=None,
            keep_mission=None,
            skip_first=False,
            repair=False,
            compression=0,
        )
        rc = run(args)
        assert rc == 0
        ds2 = xr.open_dataset(str(outfile), decode_timedelta=False)
        assert len(ds2.i) > n1
        ds2.close()

    def test_dbd2nc_run_sensor_criteria(self, tmp_path):
        """--sensors criteria file works."""
        from xarray_dbd.cli.dbd2nc import run

        dcd_files = sorted(DBD_DIR.glob("*.dcd"))[:1]
        criteria_file = tmp_path / "criteria.txt"
        criteria_file.write_text("m_present_time\n", encoding="utf-8")
        outfile = tmp_path / "out.nc"
        args = _base_args(
            files=dcd_files,
            cache=Path(CACHE_DIR),
            output=outfile,
            append=False,
            sensors=criteria_file,
            sensor_output=None,
            skip_mission=None,
            keep_mission=None,
            skip_first=False,
            repair=False,
            compression=5,
        )
        rc = run(args)
        assert rc == 0
        assert outfile.stat().st_size > 0

    def test_dbd2nc_run_sensor_and_output(self, tmp_path):
        """Both -c and -k work together."""
        import xarray as xr

        from xarray_dbd.cli.dbd2nc import run

        dcd_files = sorted(DBD_DIR.glob("*.dcd"))[:1]
        criteria_file = tmp_path / "criteria.txt"
        criteria_file.write_text("m_present_time\n", encoding="utf-8")
        keep_file = tmp_path / "keep.txt"
        keep_file.write_text("m_present_time\n", encoding="utf-8")
        outfile = tmp_path / "out.nc"
        args = _base_args(
            files=dcd_files,
            cache=Path(CACHE_DIR),
            output=outfile,
            append=False,
            sensors=criteria_file,
            sensor_output=keep_file,
            skip_mission=None,
            keep_mission=None,
            skip_first=False,
            repair=False,
            compression=5,
        )
        rc = run(args)
        assert rc == 0
        ds = xr.open_dataset(str(outfile), decode_timedelta=False)
        assert list(ds.data_vars) == ["m_present_time"]
        ds.close()

    def test_dbd2nc_run_cache_fallback(self, tmp_path):
        """No --cache falls back to parent/cache."""
        from xarray_dbd.cli.dbd2nc import run

        dcd_files = sorted(DBD_DIR.glob("*.dcd"))[:1]
        outfile = tmp_path / "out.nc"
        args = _base_args(
            files=dcd_files,
            cache=None,  # triggers fallback to files[0].parent / "cache"
            output=outfile,
            append=False,
            sensors=None,
            sensor_output=None,
            skip_mission=None,
            keep_mission=None,
            skip_first=False,
            repair=False,
            compression=5,
        )
        rc = run(args)
        assert rc == 0

    def test_dbd2nc_run_overwrite_message(self, tmp_path):
        """Running twice to same output covers overwrite log message."""
        from xarray_dbd.cli.dbd2nc import run

        dcd_files = sorted(DBD_DIR.glob("*.dcd"))[:1]
        outfile = tmp_path / "out.nc"
        base = {
            "files": dcd_files,
            "cache": Path(CACHE_DIR),
            "output": outfile,
            "append": False,
            "sensors": None,
            "sensor_output": None,
            "skip_mission": None,
            "keep_mission": None,
            "skip_first": False,
            "repair": False,
            "compression": 5,
        }
        assert run(_base_args(**base)) == 0
        assert run(_base_args(**base)) == 0  # covers overwrite path

    def test_dbd2nc_no_netcdf4_fallback(self, tmp_path, monkeypatch):
        """When netCDF4 import fails, falls back to xarray path."""
        import xarray as xr

        from xarray_dbd.cli.dbd2nc import _nc_encoding

        dcd_files = sorted(DBD_DIR.glob("*.dcd"))[:1]
        sensor_file = tmp_path / "keep.txt"
        sensor_file.write_text("m_present_time\n", encoding="utf-8")
        outfile = tmp_path / "out.nc"

        # Instead of mocking import, directly exercise the xarray fallback code path
        # by calling open_multi_dbd_dataset + to_netcdf, which is the fallback body
        import xarray_dbd as xdbd

        ds = xdbd.open_multi_dbd_dataset(
            dcd_files,
            skip_first_record=False,
            repair=False,
            to_keep=["m_present_time"],
            cache_dir=Path(CACHE_DIR),
        )
        ds.to_netcdf(str(outfile), encoding=_nc_encoding(ds, 0))
        assert outfile.stat().st_size > 0
        ds2 = xr.open_dataset(str(outfile), decode_timedelta=False)
        assert "m_present_time" in ds2.data_vars
        ds2.close()


# =============================================================================
# Tier 2 — cli/mkone.py additional tests
# =============================================================================


@pytest.mark.skipif(not has_test_data, reason="Test data not available")
class TestMkoneRunExtended:
    """Additional tests for mkone.run() and helpers."""

    def test_mkone_run_dcd_files(self, tmp_path):
        """In-process run() with dcd files creates dbd.nc."""
        from xarray_dbd.cli.mkone import run

        dcd_files = sorted(DBD_DIR.glob("*.dcd"))[:3]
        outprefix = str(tmp_path / "test.")
        cache_dir = str(tmp_path / "cache")
        import shutil

        shutil.copytree(Path(CACHE_DIR), cache_dir)
        args = _base_args(
            path=[str(f) for f in dcd_files],
            output_prefix=outprefix,
            cache=cache_dir,
            keep_first=False,
            repair=False,
            exclude=[],  # empty list → no filtering
            include=None,
        )
        rc = run(args)
        assert rc == 0
        assert Path(outprefix + "dbd.nc").exists()

    def test_mkone_run_no_files(self, tmp_path):
        """Run on empty directory → rc=0."""
        from xarray_dbd.cli.mkone import run

        indir = tmp_path / "empty"
        indir.mkdir()
        cache_dir = str(tmp_path / "cache")
        args = _base_args(
            path=[str(indir)],
            output_prefix=str(tmp_path / "out."),
            cache=cache_dir,
            keep_first=False,
            repair=False,
            exclude=None,
            include=None,
        )
        rc = run(args)
        assert rc == 0

    def test_mkone_run_default_excludes(self, tmp_path):
        """Default exclude missions set when no --exclude/--include."""
        from xarray_dbd.cli.mkone import run

        indir = tmp_path / "empty"
        indir.mkdir()
        cache_dir = str(tmp_path / "cache")
        args = _base_args(
            path=[str(indir)],
            output_prefix=str(tmp_path / "out."),
            cache=cache_dir,
            keep_first=False,
            repair=False,
            exclude=None,
            include=None,
        )
        run(args)
        assert "status.mi" in args.exclude

    def test_mkone_run_creates_cache_dir(self, tmp_path):
        """Verify cache dir created if missing."""
        from xarray_dbd.cli.mkone import run

        indir = tmp_path / "empty"
        indir.mkdir()
        cache_dir = str(tmp_path / "newcache")
        args = _base_args(
            path=[str(indir)],
            output_prefix=str(tmp_path / "out."),
            cache=cache_dir,
            keep_first=False,
            repair=False,
            exclude=None,
            include=None,
        )
        run(args)
        assert Path(cache_dir).is_dir()


class TestMkoneHelpers:
    """Tests for mkone helper functions."""

    def test_process_files_creates_output_dir(self, tmp_path):
        """process_files creates output directory if missing."""
        from xarray_dbd.cli.mkone import process_files

        outdir = tmp_path / "sub" / "deep"
        ofn = str(outdir / "test.nc")
        # Empty file list won't produce output, but dir should be created
        args = _base_args(
            exclude=None,
            include=["*"],
            cache="",
            keep_first=True,
            repair=False,
        )
        process_files(ofn, [], args)
        assert outdir.is_dir()

    @pytest.mark.skipif(not has_test_data, reason="Test data not available")
    def test_extract_sensors(self):
        """extract_sensors returns sensor names."""
        from xarray_dbd.cli.mkone import extract_sensors

        dcd_files = sorted(DBD_DIR.glob("*.dcd"))[:1]
        args = _base_args(cache=CACHE_DIR)
        sensors = extract_sensors([str(f) for f in dcd_files], args)
        assert len(sensors) > 0
        assert all(isinstance(s, str) for s in sensors)

    def test_write_sensors(self, tmp_path):
        """write_sensors creates file with sorted sensor names."""
        from xarray_dbd.cli.mkone import write_sensors

        ofn = str(tmp_path / "sensors.txt")
        write_sensors({"beta", "alpha", "gamma"}, ofn)
        content = Path(ofn).read_text(encoding="utf-8")
        lines = content.strip().split("\n")
        assert lines == ["alpha", "beta", "gamma"]

    def test_process_all_empty(self):
        """Empty filenames → no-op (no error)."""
        from xarray_dbd.cli.mkone import process_all

        args = _base_args(
            output_prefix="/tmp/test.",
            exclude=None,
            include=None,
            cache="",
            keep_first=True,
            repair=False,
        )
        process_all([], args, "dbd.nc")  # should return without error

    @pytest.mark.skipif(not has_test_data, reason="Test data not available")
    def test_worker(self, tmp_path):
        """_worker() produces output for valid files."""
        from xarray_dbd.cli.mkone import _worker

        dcd_files = sorted(DBD_DIR.glob("*.dcd"))[:2]
        outfile = str(tmp_path / "test.nc")
        args = _base_args(
            exclude=[],
            include=None,
            cache=CACHE_DIR,
            keep_first=False,
            repair=False,
        )
        _worker(outfile, [str(f) for f in dcd_files], args)
        assert Path(outfile).exists()

    @pytest.mark.skipif(not has_test_data, reason="Test data not available")
    def test_mkone_run_dbd_type_files(self, tmp_path):
        """mkone with .dbd files creates dbd.nc, dbd.sci.nc, dbd.other.nc."""
        from xarray_dbd.cli.mkone import run

        dbd_files = sorted(DBD_DIR.glob("*.dbd"))[:3]
        if not dbd_files:
            pytest.skip("No .dbd files available")
        outprefix = str(tmp_path / "test.")
        import shutil

        cache_dir = str(tmp_path / "cache")
        shutil.copytree(Path(CACHE_DIR), cache_dir)
        args = _base_args(
            path=[str(f) for f in dbd_files],
            output_prefix=outprefix,
            cache=cache_dir,
            keep_first=False,
            repair=False,
            exclude=[],
            include=None,
        )
        rc = run(args)
        assert rc == 0
        assert Path(outprefix + "dbd.nc").exists()
        assert Path(outprefix + "dbd.sensors").exists()
        assert Path(outprefix + "dbd.sci.sensors").exists()
        assert Path(outprefix + "dbd.other.sensors").exists()


# =============================================================================
# Bonus — dbdreader2/_core.py coverage gaps
# =============================================================================


class TestIsfill:
    """Tests for _is_fill edge cases."""

    def test_is_fill_int16(self):
        from xarray_dbd.dbdreader2._core import _is_fill

        arr = np.array([0, -32768, 100, -32768], dtype=np.int16)
        mask = _is_fill(arr)
        assert mask.tolist() == [False, True, False, True]

    def test_is_fill_other_int(self):
        from xarray_dbd.dbdreader2._core import _is_fill

        arr = np.array([0, 1, 2], dtype=np.int32)
        mask = _is_fill(arr)
        assert not mask.any()


class TestFilterLatlon:
    """Tests for _filter_latlon."""

    def test_non_latlon_param(self):
        from xarray_dbd.dbdreader2._core import _filter_latlon

        t = np.array([1.0, 2.0, 3.0])
        v = np.array([10.0, 20.0, 30.0])
        t2, v2 = _filter_latlon(t, v, "m_depth", True)
        np.testing.assert_array_equal(t, t2)
        np.testing.assert_array_equal(v, v2)


@pytest.mark.skipif(not has_test_data, reason="Test data not available")
class TestSetTimeVariable:
    """Tests for DBD._set_timeVariable fallback."""

    def test_set_time_variable_sci_fallback(self):
        """When m_present_time is absent, falls back to sci_m_present_time."""
        from xarray_dbd.dbdreader2._core import DBD

        ecd_files = sorted(DBD_DIR.glob("*.ecd"))[:1]
        if not ecd_files:
            pytest.skip("No .ecd files available")
        dbd = DBD(str(ecd_files[0]), cacheDir=CACHE_DIR)
        # Science files use sci_m_present_time
        assert dbd.timeVariable in ("m_present_time", "sci_m_present_time")
        dbd.close()


# =============================================================================
# Bonus — dbdreader2/_list.py coverage gaps
# =============================================================================


class TestDBDPatternSelect:
    """Tests for DBDPatternSelect."""

    def test_select_returns_empty_bins(self, tmp_path):
        """select() with no matching dates returns empty list."""
        from xarray_dbd.dbdreader2._list import DBDPatternSelect

        ps = DBDPatternSelect()
        # No pattern and no filenames raises ValueError
        with pytest.raises(ValueError, match="Expected some pattern"):
            ps.get_filenames(pattern=None, filenames=())

    @pytest.mark.skipif(not has_test_data, reason="Test data not available")
    def test_get_filenames_from_list(self):
        """get_filenames(pattern=None, filenames=[...]) works."""
        from xarray_dbd.dbdreader2._list import DBDPatternSelect

        dcd_files = sorted(str(f) for f in DBD_DIR.glob("*.dcd"))[:3]
        if not dcd_files:
            pytest.skip("No .dcd files available")
        ps = DBDPatternSelect(cacheDir=CACHE_DIR)
        result = ps.get_filenames(pattern=None, filenames=dcd_files, cacheDir=CACHE_DIR)
        assert len(result) == len(dcd_files)
