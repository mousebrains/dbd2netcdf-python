#!/bin/bash
#
# Example: Using the command-line tools
#
# xarray-dbd provides two command-line tools:
# - dbd2nc: Convert DBD files to NetCDF
# - mkone: Batch process multiple file types
#

# =============================================================================
# dbd2nc - Convert individual files to NetCDF
# =============================================================================

echo "=== Using dbd2nc ==="

# Basic conversion
dbd2nc -o output.nc input.dbd

# Convert multiple files to one NetCDF
dbd2nc -o combined.nc file1.dbd file2.dbd file3.dbd

# Use a cache directory (speeds up subsequent reads)
dbd2nc -C cache -o output.nc input.dbd

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

dbd2nc -C cache -k sensors.txt -o filtered.nc input.dbd

# Skip certain missions
dbd2nc -m initial.mi -m status.mi -o output.nc *.dbd

# Keep only certain missions
dbd2nc -M science.mi -o output.nc *.dbd

# Skip first record in each file (default behavior)
dbd2nc -s -o output.nc *.dbd

# Include first record
dbd2nc -o output.nc *.dbd  # (without -s)

# Verbose output
dbd2nc -v -o output.nc input.dbd

# =============================================================================
# mkone - Batch process different file types
# =============================================================================

echo "=== Using mkone ==="

# Process all files in a directory
# Creates separate NetCDF files for each type:
#   - dbd.nc (flight data)
#   - ebd.nc (science data)
#   - sbd.nc (decimated flight)
#   - tbd.nc (decimated science)

mkone --cache cache --outputPrefix /output/path/glider_ *.dbd *.ebd *.sbd *.tbd

# This creates:
#   /output/path/glider_dbd.nc
#   /output/path/glider_dbd.sci.nc
#   /output/path/glider_dbd.other.nc
#   /output/path/glider_ebd.nc
#   /output/path/glider_sbd.nc
#   /output/path/glider_tbd.nc

# Exclude certain missions
mkone --exclude initial.mi --exclude status.mi \
      --cache cache --outputPrefix output/glider_ *.dbd

# Include only specific missions
mkone --include science.mi \
      --cache cache --outputPrefix output/glider_ *.dbd

# Verbose output
mkone --verbose --cache cache --outputPrefix output/glider_ *.dbd

# Keep first records
mkone --keepFirst --cache cache --outputPrefix output/glider_ *.dbd

# Repair corrupted files (attempt to read despite errors)
mkone --repair --cache cache --outputPrefix output/glider_ *.dbd

# =============================================================================
# Working with compressed files
# =============================================================================

echo "=== Compressed Files ==="

# Compressed files (.dcd, .ecd, etc.) are automatically detected
dbd2nc -C cache -o output.nc input.dcd

# Mix compressed and uncompressed
dbd2nc -o combined.nc file1.dbd file2.dcd file3.dbd

# =============================================================================
# Typical Workflow
# =============================================================================

echo "=== Typical Workflow ==="

# 1. Create cache directory
mkdir -p cache

# 2. Convert all flight data
dbd2nc -C cache -o flight_data.nc *.dbd

# 3. Convert all science data
dbd2nc -C cache -o science_data.nc *.ebd

# 4. Or use mkone for batch processing
mkone --cache cache --outputPrefix processed/mission_ *.dbd *.ebd *.sbd *.tbd

# 5. Filter specific sensors for analysis
cat > nav_sensors.txt << EOF
m_present_time
m_lat
m_lon
m_depth
m_heading
EOF

dbd2nc -C cache -k nav_sensors.txt -o navigation.nc *.dbd

# =============================================================================
# Get Help
# =============================================================================

# Show full help for each tool
dbd2nc --help
mkone --help

# Show version
dbd2nc --version
