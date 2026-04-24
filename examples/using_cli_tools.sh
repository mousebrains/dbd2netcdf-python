#!/bin/bash
#
# Example: Using the command-line tools
#
# xarray-dbd installs a single entry point, `xdbd`, with subcommands:
#   - xdbd 2nc      Convert DBD files to NetCDF
#   - xdbd 2csv     Convert DBD files to CSV
#   - xdbd mkone    Batch process directories of DBD files
#   - xdbd sensors  List sensors from one or more DBD files
#

# =============================================================================
# xdbd 2nc - Convert individual files to NetCDF
# =============================================================================

echo "=== Using xdbd 2nc ==="

# Basic conversion
xdbd 2nc -o output.nc input.dbd

# Convert multiple files to one NetCDF
xdbd 2nc -o combined.nc file1.dbd file2.dbd file3.dbd

# Use a cache directory (speeds up subsequent reads)
xdbd 2nc -C cache -o output.nc input.dbd

# Filter to specific sensors
# Create a sensor list file
cat > sensors.txt << EOF
m_present_time
m_depth
m_lat
m_lon
m_pitch
m_roll
EOF

xdbd 2nc -C cache -k sensors.txt -o filtered.nc input.dbd

# Skip certain missions
xdbd 2nc -m initial.mi -m status.mi -o output.nc *.dbd

# Keep only certain missions
xdbd 2nc -M science.mi -o output.nc *.dbd

# Skip first record in each file — this is the default.
# --skip-first explicitly confirms it; --keep-first inverts.
xdbd 2nc -o output.nc *.dbd            # default: skips first record of every file
xdbd 2nc -s -o output.nc *.dbd         # explicit; same effect
xdbd 2nc --keep-first -o output.nc *.dbd  # keep the first record of every file

# Verbose output
xdbd 2nc -v -o output.nc input.dbd

# =============================================================================
# xdbd mkone - Batch process different file types
# =============================================================================

echo "=== Using xdbd mkone ==="

# Process all files in a directory
# Creates separate NetCDF files for each type:
#   - dbd.nc (flight data)
#   - ebd.nc (science data)
#   - sbd.nc (decimated flight)
#   - tbd.nc (decimated science)

xdbd mkone --cache cache --output-prefix /output/path/glider_ *.dbd *.ebd *.sbd *.tbd

# This creates:
#   /output/path/glider_dbd.nc
#   /output/path/glider_dbd.sci.nc
#   /output/path/glider_dbd.other.nc
#   /output/path/glider_ebd.nc
#   /output/path/glider_sbd.nc
#   /output/path/glider_tbd.nc

# Exclude certain missions
xdbd mkone --exclude initial.mi --exclude status.mi \
      --cache cache --output-prefix output/glider_ *.dbd

# Include only specific missions
xdbd mkone --include science.mi \
      --cache cache --output-prefix output/glider_ *.dbd

# Verbose output
xdbd mkone --verbose --cache cache --output-prefix output/glider_ *.dbd

# Keep first records (default is to skip)
xdbd mkone --keep-first --cache cache --output-prefix output/glider_ *.dbd

# Repair corrupted files (attempt to read despite errors)
xdbd mkone --repair --cache cache --output-prefix output/glider_ *.dbd

# =============================================================================
# Working with compressed files
# =============================================================================

echo "=== Compressed Files ==="

# Compressed files (.dcd, .ecd, etc.) are automatically detected
xdbd 2nc -C cache -o output.nc input.dcd

# Mix compressed and uncompressed
xdbd 2nc -o combined.nc file1.dbd file2.dcd file3.dbd

# =============================================================================
# Typical Workflow
# =============================================================================

echo "=== Typical Workflow ==="

# 1. Create cache directory
mkdir -p cache

# 2. Convert all flight data
xdbd 2nc -C cache -o flight_data.nc *.dbd

# 3. Convert all science data
xdbd 2nc -C cache -o science_data.nc *.ebd

# 4. Or use mkone for batch processing
xdbd mkone --cache cache --output-prefix processed/mission_ *.dbd *.ebd *.sbd *.tbd

# 5. Filter specific sensors for analysis
cat > nav_sensors.txt << EOF
m_present_time
m_lat
m_lon
m_depth
m_heading
EOF

xdbd 2nc -C cache -k nav_sensors.txt -o navigation.nc *.dbd

# =============================================================================
# Get Help
# =============================================================================

# Show full help for each subcommand
xdbd --help
xdbd 2nc --help
xdbd mkone --help

# Show version
xdbd --version
