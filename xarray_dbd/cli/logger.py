#
# Logging setup modeled after TPWUtils/Logger.py
#
# Supports rotating file logs, SMTP error emails, and --debug/--verbose flags.
#
# Feb-2026, Pat Welch, pat@mousebrains.com

import getpass
import logging
import logging.handlers
import socket
from argparse import ArgumentParser, Namespace


def addArgs(parser: ArgumentParser) -> None:
    """Add logger-related command line arguments."""
    grp = parser.add_argument_group("Logger Related Options")
    grp.add_argument("--logfile", type=str, metavar="filename", help="Name of logfile")
    grp.add_argument(
        "--logBytes",
        type=int,
        default=10000000,
        metavar="length",
        help="Maximum logfile size in bytes",
    )
    grp.add_argument(
        "--logCount",
        type=int,
        default=3,
        metavar="count",
        help="Number of backup files to keep",
    )
    grp.add_argument(
        "--mailTo",
        action="append",
        metavar="foo@bar.com",
        help="Where to mail errors and exceptions to",
    )
    grp.add_argument(
        "--mailFrom", type=str, metavar="foo@bar.com", help="Who the mail originates from"
    )
    grp.add_argument("--mailSubject", type=str, metavar="subject", help="Mail subject line")
    grp.add_argument(
        "--smtpHost",
        type=str,
        default="localhost",
        metavar="foo.bar.com",
        help="SMTP server to mail to",
    )
    gg = grp.add_mutually_exclusive_group()
    gg.add_argument("--debug", action="store_true", help="Enable very verbose logging")
    gg.add_argument("--verbose", action="store_true", help="Enable verbose logging")


def mkLogger(
    args: Namespace,
    fmt: str | None = None,
    name: str | None = None,
    logLevel: str = "WARNING",
) -> logging.Logger:
    """Construct a logger and return it."""
    logger = logging.getLogger(name)
    logger.handlers.clear()

    if fmt is None:
        fmt = "%(asctime)s %(levelname)s: %(message)s"

    if args.logfile:
        ch = logging.handlers.RotatingFileHandler(
            args.logfile,
            maxBytes=args.logBytes,
            backupCount=args.logCount,
        )
    else:
        ch = logging.StreamHandler()

    logLevel = logging.DEBUG if args.debug else logging.INFO if args.verbose else logLevel
    logger.setLevel(logLevel)
    ch.setLevel(logLevel)

    formatter = logging.Formatter(fmt)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    if args.mailTo is not None:
        frm = (
            args.mailFrom
            if args.mailFrom is not None
            else (getpass.getuser() + "@" + socket.getfqdn())
        )
        subj = (
            args.mailSubject if args.mailSubject is not None else ("Error on " + socket.getfqdn())
        )
        mh = logging.handlers.SMTPHandler(args.smtpHost, frm, args.mailTo, subj)
        mh.setLevel(logging.ERROR)
        mh.setFormatter(formatter)
        logger.addHandler(mh)

    return logger
