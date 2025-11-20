#!/usr/bin/env python3
"""
Benchmark performance comparison between C++ dbd2netCDF and Python dbd2nc
"""

import subprocess
import time
import os
import sys
from pathlib import Path
import psutil

def get_file_size(path):
    """Get file size in MB"""
    return os.path.getsize(path) / (1024 * 1024)

def measure_command(cmd, desc):
    """Run command and measure time and peak memory"""
    print(f"\n{'='*70}")
    print(f"Running: {desc}")
    print(f"Command: {' '.join(cmd)}")
    print(f"{'='*70}")

    # Start process
    start_time = time.time()
    process = psutil.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # Monitor memory usage
    peak_memory = 0
    try:
        while process.poll() is None:
            try:
                mem_info = process.memory_info()
                peak_memory = max(peak_memory, mem_info.rss)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                break
            time.sleep(0.01)

        # Final check
        try:
            mem_info = process.memory_info()
            peak_memory = max(peak_memory, mem_info.rss)
        except:
            pass

    except KeyboardInterrupt:
        process.kill()
        raise

    end_time = time.time()
    stdout, stderr = process.communicate()

    elapsed = end_time - start_time
    peak_memory_mb = peak_memory / (1024 * 1024)

    result = {
        'elapsed': elapsed,
        'peak_memory_mb': peak_memory_mb,
        'returncode': process.returncode,
        'stdout': stdout.decode('utf-8', errors='ignore'),
        'stderr': stderr.decode('utf-8', errors='ignore'),
    }

    print(f"Time: {elapsed:.2f} seconds")
    print(f"Peak Memory: {peak_memory_mb:.2f} MB")
    print(f"Return Code: {process.returncode}")

    if process.returncode != 0:
        print("STDERR:", stderr.decode('utf-8', errors='ignore')[:500])

    return result

def main():
    # File patterns
    dcd_files = sorted(Path("dbd_files").glob("*.dcd"))
    ecd_files = sorted(Path("dbd_files").glob("*.ecd"))

    print("="*70)
    print("PERFORMANCE BENCHMARK: C++ vs Python dbd2nc")
    print("="*70)
    print(f"\nTest files:")
    print(f"  .dcd files: {len(dcd_files)} files")
    dcd_size = sum(f.stat().st_size for f in dcd_files) / (1024*1024)
    print(f"  .dcd total size: {dcd_size:.2f} MB")
    print(f"  .ecd files: {len(ecd_files)} files")
    ecd_size = sum(f.stat().st_size for f in ecd_files) / (1024*1024)
    print(f"  .ecd total size: {ecd_size:.2f} MB")

    # Create output directory
    os.makedirs("/tmp/benchmark_output", exist_ok=True)

    results = {}

    # Test 1: C++ on .dcd files
    cpp_bin = "dbd2netcdf/bin/dbd2netCDF"
    if os.path.exists(cpp_bin):
        cmd = [cpp_bin, "-C", "dbd_files/cache", "-o", "/tmp/benchmark_output/cpp_dcd.nc"] + [str(f) for f in dcd_files]
        results['cpp_dcd'] = measure_command(cmd, "C++ dbd2netCDF on .dcd files")

        if os.path.exists("/tmp/benchmark_output/cpp_dcd.nc"):
            results['cpp_dcd']['output_size_mb'] = get_file_size("/tmp/benchmark_output/cpp_dcd.nc")

    # Test 2: Python on .dcd files
    cmd = ["dbd2nc", "-C", "dbd_files/cache", "-o", "/tmp/benchmark_output/python_dcd.nc"] + [str(f) for f in dcd_files]
    results['python_dcd'] = measure_command(cmd, "Python dbd2nc on .dcd files")

    if os.path.exists("/tmp/benchmark_output/python_dcd.nc"):
        results['python_dcd']['output_size_mb'] = get_file_size("/tmp/benchmark_output/python_dcd.nc")

    # Test 3: C++ on .ecd files
    if os.path.exists(cpp_bin):
        cmd = [cpp_bin, "-C", "dbd_files/cache", "-o", "/tmp/benchmark_output/cpp_ecd.nc"] + [str(f) for f in ecd_files]
        results['cpp_ecd'] = measure_command(cmd, "C++ dbd2netCDF on .ecd files")

        if os.path.exists("/tmp/benchmark_output/cpp_ecd.nc"):
            results['cpp_ecd']['output_size_mb'] = get_file_size("/tmp/benchmark_output/cpp_ecd.nc")

    # Test 4: Python on .ecd files
    cmd = ["dbd2nc", "-C", "dbd_files/cache", "-o", "/tmp/benchmark_output/python_ecd.nc"] + [str(f) for f in ecd_files]
    results['python_ecd'] = measure_command(cmd, "Python dbd2nc on .ecd files")

    if os.path.exists("/tmp/benchmark_output/python_ecd.nc"):
        results['python_ecd']['output_size_mb'] = get_file_size("/tmp/benchmark_output/python_ecd.nc")

    # Print summary
    print("\n" + "="*70)
    print("BENCHMARK SUMMARY")
    print("="*70)

    def print_comparison(file_type, cpp_key, python_key):
        print(f"\n{file_type} files:")
        print("-" * 70)

        if cpp_key in results and python_key in results:
            cpp = results[cpp_key]
            py = results[python_key]

            print(f"{'Metric':<25} {'C++ dbd2netCDF':>20} {'Python dbd2nc':>20}")
            print("-" * 70)
            print(f"{'Time (seconds)':<25} {cpp['elapsed']:>20.2f} {py['elapsed']:>20.2f}")
            print(f"{'Peak Memory (MB)':<25} {cpp['peak_memory_mb']:>20.2f} {py['peak_memory_mb']:>20.2f}")

            if 'output_size_mb' in cpp and 'output_size_mb' in py:
                print(f"{'Output File (MB)':<25} {cpp['output_size_mb']:>20.2f} {py['output_size_mb']:>20.2f}")

            # Calculate ratios
            time_ratio = py['elapsed'] / cpp['elapsed'] if cpp['elapsed'] > 0 else 0
            mem_ratio = py['peak_memory_mb'] / cpp['peak_memory_mb'] if cpp['peak_memory_mb'] > 0 else 0

            print("-" * 70)
            print(f"{'Python/C++ Ratio':<25} {'':<20} {'':<20}")
            print(f"{'  Time':<25} {time_ratio:>20.2f}x")
            print(f"{'  Memory':<25} {mem_ratio:>20.2f}x")

            if time_ratio < 1:
                print(f"\n✓ Python is {1/time_ratio:.2f}x FASTER")
            elif time_ratio > 1:
                print(f"\n  Python is {time_ratio:.2f}x slower")

            if mem_ratio < 1:
                print(f"✓ Python uses {1/mem_ratio:.2f}x LESS memory")
            elif mem_ratio > 1:
                print(f"  Python uses {mem_ratio:.2f}x more memory")

    print_comparison(".dcd", 'cpp_dcd', 'python_dcd')
    print_comparison(".ecd", 'cpp_ecd', 'python_ecd')

    print("\n" + "="*70)

if __name__ == '__main__':
    main()
