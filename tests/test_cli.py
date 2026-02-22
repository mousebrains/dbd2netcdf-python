"""Smoke tests for CLI entry points."""

from __future__ import annotations

import subprocess
import sys
import tempfile
from pathlib import Path

import pytest
from conftest import CACHE_DIR, DBD_DIR, has_test_data


def run_cli(args: list[str], check: bool = True) -> subprocess.CompletedProcess:
    """Run a CLI command and return the result."""
    return subprocess.run(
        [sys.executable, "-m", *args],
        capture_output=True,
        text=True,
        check=check,
    )


# --- dbd2nc tests ---


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
    assert "0.1.0" in output


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


# --- mkone tests ---


def test_mkone_help():
    """mkone --help exits 0."""
    result = run_cli(["xarray_dbd.cli.mkone", "--help"])
    assert result.returncode == 0
    assert "output-prefix" in result.stdout


def test_mkone_missing_output_prefix():
    """mkone without --outputPrefix fails."""
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


# --- xdbd router tests ---


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
    assert "0.1.0" in output


# --- sensors tests ---


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


# --- missions tests ---


def test_missions_help():
    """missions --help exits 0."""
    result = run_cli(["xarray_dbd.cli.missions", "--help"])
    assert result.returncode == 0


# --- cache tests ---


def test_cache_help():
    """cache --help exits 0."""
    result = run_cli(["xarray_dbd.cli.cache", "--help"])
    assert result.returncode == 0


# --- 2csv tests ---


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
