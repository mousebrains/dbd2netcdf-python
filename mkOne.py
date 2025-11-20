#!/usr/bin/env python3
#
# Modified version of mkOne.py to use the Python xarray-dbd implementation
# instead of the C++ dbd2netCDF
#
# Oct-2023, Pat Welch, pat@mousebrains.com
# Modified Jan-2025 to use xarray-dbd

from argparse import ArgumentParser
import re
import os
import sys
import logging
import threading
import time
import queue
from pathlib import Path

# Add xarray-dbd to path
sys.path.insert(0, str(Path(__file__).parent))
import xarray_dbd as xdbd

class RunCommand(threading.Thread):
    # Class-level queue to collect exceptions from all threads
    __exception_queue = queue.Queue()

    def __init__(self, name:str, ofn:str, filenames:list, args, sensorsFilename:str=None) -> None:
        threading.Thread.__init__(self)
        self.name = name
        self.__ofn = ofn
        self.__filenames = filenames
        self.__args = args
        self.__sensorsFilename = sensorsFilename

    def run(self): # Called on start
        try:
            stime = time.time()
            processFiles(self.__ofn, self.__filenames, self.__args, self.__sensorsFilename)
            logging.info("Took %s wall clock seconds", "{:.2f}".format(time.time() - stime))
        except Exception as e:
            logging.exception("Executing")
            # Push exception to queue so main thread can detect it
            self.__exception_queue.put((self.name, e))

    @classmethod
    def check_exceptions(cls):
        """Check if any thread raised an exception and re-raise it"""
        if not cls.__exception_queue.empty():
            name, exception = cls.__exception_queue.get()
            raise RuntimeError(f"Thread {name} failed") from exception


def processFiles(ofn:str, filenames:list, args:ArgumentParser, sensorsFilename:str=None) -> None:
    """Process files using xarray-dbd"""
    logging.info("%s files", len(filenames))

    # Make sure the directory of the output file exists
    odir = os.path.dirname(ofn)
    if not os.path.isdir(odir):
        logging.info("Creating %s", odir)
        os.makedirs(odir, mode=0o755, exist_ok=True)

    # Read sensor list if provided
    to_keep = None
    if sensorsFilename:
        with open(sensorsFilename, 'r') as f:
            to_keep = [line.strip() for line in f if line.strip()]

    # Prepare arguments for xarray-dbd
    skip_missions = args.exclude if args.exclude else None
    keep_missions = args.include if args.include else None
    cache_dir = Path(args.cache) if args.cache else None

    # Read the files
    ds = xdbd.open_multi_dbd_dataset(
        [Path(f) for f in filenames],
        skip_first_record=not args.keepFirst,
        repair=args.repair,
        to_keep=to_keep,
        skip_missions=skip_missions,
        keep_missions=keep_missions,
        cache_dir=cache_dir
    )

    # Write to NetCDF
    ds.to_netcdf(ofn)
    logging.info("Wrote %s with %d records and %d variables",
                 ofn, len(ds.i), len(ds.data_vars))


def extractSensors(filenames:list, args:ArgumentParser) -> list:
    """Extract unique sensor names from files"""
    all_sensors = set()

    cache_dir = Path(args.cache) if args.cache else None

    for filename in filenames:
        try:
            # Read just the header to get sensors
            reader = xdbd.DBDReader(
                Path(filename),
                skip_first_record=False,
                cache_dir=cache_dir
            )
            sensors = reader.sensors.get_output_sensors()
            all_sensors.update(s.name for s in sensors)
        except Exception as e:
            logging.warning("Error reading %s: %s", filename, e)

    return list(all_sensors)


def processAll(filenames:list, args:ArgumentParser, suffix:str, sensorsFilename:str=None) -> None:
    """Process files into a NetCDF"""
    filenames = list(filenames) # ensure it is a list
    if not filenames: return # Nothing to do

    ofn = args.outputPrefix + suffix # Output filename

    rc = RunCommand(suffix, ofn, filenames, args, sensorsFilename)
    rc.start()


def writeSensors(sensors:set, ofn:str) -> None:
    odir = os.path.dirname(ofn)
    if not os.path.isdir(odir):
        logging.info("Creating %s", odir)
        os.makedirs(odir, mode=0o755, exist_ok=True)

    with open(ofn, "w") as fp:
        fp.write("\n".join(sorted(sensors)))
        fp.write("\n")
    return ofn


def processDBD(filenames:list, args:ArgumentParser) -> None:
    """Process flight Dinkum Binary files"""
    filenames = list(filenames)
    if not filenames: return # Nothing to do

    allSensors = set(extractSensors(filenames, args))
    dbdSensors = set(filter(lambda x: x.startswith("m_") or x.startswith("c_"), allSensors))
    sciSensors = set(filter(lambda x: x.startswith("sci_"), allSensors))
    otroSensors = allSensors.difference(dbdSensors).difference(sciSensors)
    sciSensors.add("m_present_time")
    otroSensors.add("m_present_time")

    allFN = writeSensors(allSensors, args.outputPrefix + "dbd.all.sensors")
    dbdFN = writeSensors(dbdSensors, args.outputPrefix + "dbd.sensors")
    sciFN = writeSensors(sciSensors, args.outputPrefix + "dbd.sci.sensors")
    otroFN = writeSensors(otroSensors, args.outputPrefix + "dbd.other.sensors")

    processAll(filenames, args, "dbd.nc", dbdFN)
    processAll(filenames, args, "dbd.sci.nc", sciFN)
    processAll(filenames, args, "dbd.other.nc", otroFN)


parser = ArgumentParser()
parser.add_argument("filename", type=str, nargs="+", help="Dinkum binary files to convert")

grp = parser.add_argument_group(description="Processing options")
grp.add_argument("--cache", type=str, default="cache", help="Directory for sensor cache files")
grp.add_argument("--verbose", action="store_true", help="Verbose output")
grp.add_argument("--repair", action="store_true", help="Should corrupted files be 'repaired'")
grp.add_argument("--keepFirst", action="store_true",
                 help="Should the first record not be discarded on all the files?")
g = grp.add_mutually_exclusive_group()
g.add_argument("--exclude", type=str, action="append", help="Mission(s) to exclude")
g.add_argument("--include", type=str, action="append", help="Mission(s) to include")

grp = parser.add_argument_group(description="Output related arguments")
grp.add_argument("--outputPrefix", type=str, required=True, help="Output prefix")

args = parser.parse_args()

logging.basicConfig(
        format="%(asctime)s %(threadName)s %(levelname)s: %(message)s",
        level=logging.DEBUG if args.verbose else logging.INFO,
        )

if args.exclude is None and args.include is None:
    args.exclude = ( # Default missions to exclude
            "status.mi",
            "lastgasp.mi",
            "initial.mi",
            "overtime.mi",
            "ini0.mi",
            "ini1.mi",
            "ini2.mi",
            "ini3.mi",
            )

args.cache = os.path.abspath(os.path.expanduser(args.cache))

if not os.path.isdir(args.cache):
    logging.info("Creating %s", args.cache)
    os.makedirs(args.cache, mode=0o755, exist_ok=True)

files = list(map(lambda x: os.path.abspath(os.path.expanduser(x)), args.filename))

processAll(filter(lambda x: re.search(r"[.]s[bc]d", x, re.IGNORECASE), files),
           args, "sbd.nc") # Flight decimated Dinkum Binary files
processAll(filter(lambda x: re.search(r"[.]t[bc]d", x, re.IGNORECASE), files),
           args, "tbd.nc") # Science decimated Dinkum Binary files
processAll(filter(lambda x: re.search(r"[.]m[bc]d", x, re.IGNORECASE), files),
           args, "mbd.nc") # Flight decimated Dinkum Binary files
processAll(filter(lambda x: re.search(r"[.]n[bc]d", x, re.IGNORECASE), files),
           args, "nbd.nc") # Science decimated Dinkum Binary files
processDBD(filter(lambda x: re.search(r"[.]d[bc]d", x, re.IGNORECASE), files),
           args) # Flight Dinkum Binary files
processAll(filter(lambda x: re.search(r"[.]e[bc]d", x, re.IGNORECASE), files),
           args, "ebd.nc") # Science Dinkum Binary files

# Wait for the children to finish
for thrd in threading.enumerate():
    if thrd != threading.main_thread():
        thrd.join()

# Check if any thread failed
RunCommand.check_exceptions()

logging.info("All processing complete!")
