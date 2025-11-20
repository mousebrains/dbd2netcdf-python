#!/usr/bin/env python3
"""
Check for changes in the C++ dbd2netCDF repository that may need
to be incorporated into the Python implementation.
"""

import subprocess
import sys
from pathlib import Path

# Files to monitor for changes
CRITICAL_FILES = {
    'src/Decompress.C': 'xarray_dbd/decompression.py',
    'src/Decompress.H': 'xarray_dbd/decompression.py',
    'src/Header.C': 'xarray_dbd/header.py',
    'src/Header.H': 'xarray_dbd/header.py',
    'src/Sensor.C': 'xarray_dbd/sensor.py',
    'src/Sensor.H': 'xarray_dbd/sensor.py',
    'src/Sensors.C': 'xarray_dbd/sensor.py',
    'src/Sensors.H': 'xarray_dbd/sensor.py',
    'src/KnownBytes.C': 'xarray_dbd/reader.py',
    'src/KnownBytes.H': 'xarray_dbd/reader.py',
    'src/Data.C': 'xarray_dbd/reader.py',
    'src/Data.H': 'xarray_dbd/reader.py',
    'src/dbd2netCDF.C': 'dbd2nc.py',
}

IMPORTANT_FILES = {
    'src/SensorsMap.C': 'Sensor cache management',
    'src/SensorsMap.H': 'Sensor cache management',
    'mkOne.py': 'Batch processing script',
}


def run_git(cmd, cwd):
    """Run git command and return output"""
    try:
        result = subprocess.run(
            ['git'] + cmd,
            cwd=cwd,
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Git command failed: {e.stderr}", file=sys.stderr)
        return None


def check_cpp_repo():
    """Check C++ repository for changes"""
    cpp_dir = Path(__file__).parent.parent / 'dbd2netcdf'

    if not cpp_dir.exists():
        print("❌ C++ repository not found at:", cpp_dir)
        print("   Clone it with: git clone https://github.com/mousebrains/dbd2netcdf.git")
        return False

    print("=" * 70)
    print("Checking C++ dbd2netCDF Repository for Changes")
    print("=" * 70)
    print()

    # Get current branch/commit
    current = run_git(['rev-parse', '--short', 'HEAD'], cpp_dir)
    branch = run_git(['rev-parse', '--abbrev-ref', 'HEAD'], cpp_dir)

    print(f"Current branch: {branch}")
    print(f"Current commit: {current}")
    print()

    # Fetch latest
    print("Fetching latest changes...")
    run_git(['fetch', 'origin'], cpp_dir)
    print()

    # Check for new commits
    upstream = f'origin/{branch}' if branch != 'HEAD' else 'origin/master'
    log_cmd = ['log', '--oneline', '--no-merges', f'HEAD..{upstream}']
    new_commits = run_git(log_cmd, cpp_dir)

    if not new_commits:
        print("✓ No new commits upstream")
        print()
        return True

    print("📝 New commits found:")
    print(new_commits)
    print()

    # Check which files changed
    diff_cmd = ['diff', '--name-only', f'HEAD..{upstream}']
    changed_files = run_git(diff_cmd, cpp_dir)

    if not changed_files:
        print("ℹ️  No file changes (commits may be merge/metadata only)")
        return True

    changed_list = changed_files.split('\n')

    # Categorize changes
    critical_changes = []
    important_changes = []
    other_changes = []

    for file in changed_list:
        if file in CRITICAL_FILES:
            critical_changes.append((file, CRITICAL_FILES[file]))
        elif file in IMPORTANT_FILES:
            important_changes.append((file, IMPORTANT_FILES[file]))
        else:
            other_changes.append(file)

    # Report findings
    if critical_changes:
        print("🔴 CRITICAL: Changes to core implementation files:")
        for cpp_file, py_file in critical_changes:
            print(f"   {cpp_file}")
            print(f"      → Update: {py_file}")
        print()

    if important_changes:
        print("🟡 IMPORTANT: Changes to related files:")
        for cpp_file, note in important_changes:
            print(f"   {cpp_file}")
            print(f"      → {note}")
        print()

    if other_changes:
        print("ℹ️  Other changes (may not affect Python):")
        for file in other_changes[:10]:  # Limit to 10
            print(f"   {file}")
        if len(other_changes) > 10:
            print(f"   ... and {len(other_changes) - 10} more")
        print()

    # Show detailed diffs for critical files
    if critical_changes:
        print("=" * 70)
        print("Detailed Changes to Critical Files")
        print("=" * 70)
        print()

        for cpp_file, py_file in critical_changes:
            print(f"\n{'=' * 70}")
            print(f"File: {cpp_file}")
            print(f"{'=' * 70}\n")

            diff_cmd = ['diff', f'HEAD..{upstream}', '--', cpp_file]
            diff = run_git(diff_cmd, cpp_dir)
            if diff:
                # Show first 50 lines of diff
                lines = diff.split('\n')
                for line in lines[:50]:
                    print(line)
                if len(lines) > 50:
                    print(f"\n... ({len(lines) - 50} more lines)")
            print()

    # Recommendations
    print("=" * 70)
    print("Next Steps")
    print("=" * 70)
    print()

    if critical_changes or important_changes:
        print("1. Review the changes above")
        print("2. Update corresponding Python files:")
        for cpp_file, py_file in critical_changes:
            print(f"   - {py_file}")
        print("3. Run tests: python3 tests/test_dbd2nc.py")
        print("4. Update docs/SYNC.md with sync date and notes")
        print()
        print("See docs/SYNC.md for detailed synchronization process")
    else:
        print("No critical changes detected. No action needed.")

    print()
    return True


def main():
    """Main entry point"""
    try:
        success = check_cpp_repo()
        return 0 if success else 1
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        return 130
    except Exception as e:
        print(f"\n❌ Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
