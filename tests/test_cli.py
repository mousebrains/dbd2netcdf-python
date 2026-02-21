"""Smoke tests for CLI entry points."""

import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

DBD_DIR = Path(__file__).parent.parent / "dbd_files"
CACHE_DIR = str(DBD_DIR / "cache")


def run_cli(args: list[str], check: bool = True) -> subprocess.CompletedProcess:
    """Run a CLI command and return the result."""
    return subprocess.run(
        [sys.executable, "-m", *args],
        capture_output=True,
        text=True,
        check=check,
    )


def test_dbd2nc_help():
    """dbd2nc --help exits 0."""
    result = run_cli(["xarray_dbd.cli.dbd2nc", "--help"])
    assert result.returncode == 0
    assert "Convert Slocum glider DBD files" in result.stdout


def test_mkone_help():
    """mkone --help exits 0."""
    result = run_cli(["xarray_dbd.cli.mkone", "--help"])
    assert result.returncode == 0
    assert "outputPrefix" in result.stdout


def test_dbd2nc_version():
    """dbd2nc -V prints version."""
    result = run_cli(["xarray_dbd.cli.dbd2nc", "-V"])
    assert result.returncode == 0
    # Version string is in stdout or stderr depending on argparse version
    output = result.stdout + result.stderr
    assert "0.1.0" in output


@pytest.mark.skipif(
    not (DBD_DIR / "01330000.dcd").exists(), reason="Test data not available"
)
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
            [sys.executable, "-m", "xarray_dbd.cli.dbd2nc",
             "-C", CACHE_DIR, "-o", tmpname, *[str(f) for f in dcd_files]],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, f"dbd2nc failed: {result.stderr}"
        assert Path(tmpname).stat().st_size > 0, "Output file is empty"
    finally:
        Path(tmpname).unlink(missing_ok=True)


def test_dbd2nc_missing_file():
    """dbd2nc with a non-existent file returns non-zero exit code."""
    with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as tmp:
        tmpname = tmp.name

    try:
        result = subprocess.run(
            [sys.executable, "-m", "xarray_dbd.cli.dbd2nc",
             "-o", tmpname, "/nonexistent/fake.dbd"],
            capture_output=True,
            text=True,
        )
        assert result.returncode != 0
    finally:
        Path(tmpname).unlink(missing_ok=True)
