"""
Setup script for xarray-dbd
"""

from pathlib import Path

from setuptools import find_packages, setup

# Read the README file
readme_file = Path(__file__).parent / "README.md"
if readme_file.exists():
    long_description = readme_file.read_text(encoding="utf-8")
else:
    long_description = "An efficient xarray backend for Dinkum Binary Data (DBD) files"

setup(
    name="xarray-dbd",
    version="0.1",
    author="Based on dbd2netCDF by Pat Welch",
    author_email="pat@mousebrains.com",
    description="Efficient xarray backend for reading glider DBD files",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/mousebrains/dbd2netcdf-python",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering :: Oceanography",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.13",
        "Programming Language :: Python :: 3.14",
    ],
    python_requires=">=3.13",
    install_requires=[
        "numpy>=1.20",
        "xarray>=2022.3.0",
        "lz4>=3.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0",
            "pytest-cov",
            "ruff>=0.8.0",
            "mypy>=1.13",
        ],
    },
    py_modules=["dbd2nc", "mkOne"],
    entry_points={
        "console_scripts": [
            "dbd2nc=dbd2nc:main",
            "mkone=mkOne:main",
        ],
        "xarray.backends": [
            "dbd=xarray_dbd.backend:DBDBackendEntrypoint",
        ],
    },
    include_package_data=True,
    zip_safe=False,
)
