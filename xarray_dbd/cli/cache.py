#
# cache subcommand — list cache CRCs and file counts from DBD file headers
#
# Feb-2026, Pat Welch, pat@mousebrains.com

import logging
import sys
from collections import Counter
from pathlib import Path

import xarray_dbd as xdbd
from xarray_dbd.cli import logger
from xarray_dbd.cli.sensors import _add_common_args


def addArgs(subparsers) -> None:
    """Register the 'cache' subcommand."""
    parser = subparsers.add_parser(
        "cache",
        help="List cache CRCs and file counts from DBD file headers",
        description="Scan DBD file headers and output sensor_list_crc values with file counts",
    )
    _add_common_args(parser)
    parser.add_argument(
        "--missing",
        action="store_true",
        default=False,
        help="Only show CRCs whose cache file is absent from the cache directory",
    )
    parser.set_defaults(func=run)


def _cache_file_exists(cache_dir: Path, crc: str) -> bool:
    """Check if a cache file exists for the given CRC.

    Mirrors C++ Sensors::mkFilename() logic: case-insensitive scan of
    the cache directory for files matching {crc}, {crc}.cac, or {crc}.ccc.
    """
    if not cache_dir.is_dir():
        return False

    crc_lower = crc.lower()
    for entry in cache_dir.iterdir():
        if not entry.is_file():
            continue
        name_lower = entry.name.lower()
        if name_lower in (crc_lower, crc_lower + ".cac", crc_lower + ".ccc"):
            return True
    return False


def run(args) -> int:
    """Execute the cache subcommand."""
    logger.mkLogger(args)

    if args.missing and not args.cache:
        logging.error("--missing requires -C/--cache directory")
        return 1

    filenames = [str(f) for f in args.files]
    for f in args.files:
        if not f.exists():
            logging.error("File not found: %s", f)
            return 1

    result = xdbd.scan_headers(
        filenames,
        skip_missions=args.skipMission,
        keep_missions=args.keepMission,
    )

    crcs = result["sensor_list_crcs"]
    if not crcs:
        logging.warning("No valid files found")
        return 1

    counts = Counter(crcs)

    if args.missing:
        cache_dir = Path(args.cache)
        counts = {crc: n for crc, n in counts.items() if not _cache_file_exists(cache_dir, crc)}

    lines = []
    for crc in sorted(counts):
        lines.append(f"{counts[crc]} {crc}")

    output = "\n".join(lines) + "\n"

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(output, encoding="utf-8")
        logging.info("Wrote cache list to %s", args.output)
    else:
        sys.stdout.write(output)

    return 0
