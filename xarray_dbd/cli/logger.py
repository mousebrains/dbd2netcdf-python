#
# Logging setup modeled after TPWUtils/Logger.py
#
# Supports rotating file logs, SMTP error emails, and --debug/--verbose flags.
#
# Feb-2026, Pat Welch, pat@mousebrains.com

from __future__ import annotations

import getpass
import logging
import logging.handlers
import socket
from argparse import ArgumentParser, Namespace


def add_args(parser: ArgumentParser) -> None:
    """Add logger-related command line arguments."""
    grp = parser.add_argument_group("Logger Related Options")
    grp.add_argument("--logfile", type=str, metavar="filename", help="Name of logfile")
    grp.add_argument(
        "--log-bytes",
        type=int,
        default=10000000,
        metavar="length",
        help="Maximum logfile size in bytes",
    )
    grp.add_argument(
        "--log-count",
        type=int,
        default=3,
        metavar="count",
        help="Number of backup files to keep",
    )
    grp.add_argument(
        "--mail-to",
        action="append",
        metavar="foo@bar.com",
        help="Where to mail errors and exceptions to",
    )
    grp.add_argument(
        "--mail-from", type=str, metavar="foo@bar.com", help="Who the mail originates from"
    )
    grp.add_argument("--mail-subject", type=str, metavar="subject", help="Mail subject line")
    grp.add_argument(
        "--smtp-host",
        type=str,
        default="localhost",
        metavar="foo.bar.com",
        help="SMTP server to mail to",
    )
    gg = grp.add_mutually_exclusive_group()
    gg.add_argument("--debug", action="store_true", help="Enable very verbose logging")
    gg.add_argument("--verbose", action="store_true", help="Enable verbose logging")


def mk_logger(
    args: Namespace,
    fmt: str | None = None,
    name: str | None = None,
    log_level: str = "WARNING",
) -> logging.Logger:
    """Construct a logger and return it."""
    logger = logging.getLogger(name)
    logger.handlers.clear()

    if fmt is None:
        fmt = "%(asctime)s %(levelname)s: %(message)s"

    ch: logging.Handler
    if args.logfile:
        ch = logging.handlers.RotatingFileHandler(
            args.logfile,
            maxBytes=args.log_bytes,
            backupCount=args.log_count,
        )
    else:
        ch = logging.StreamHandler()

    level: int | str = logging.DEBUG if args.debug else logging.INFO if args.verbose else log_level
    logger.setLevel(level)
    ch.setLevel(level)

    formatter = logging.Formatter(fmt)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    if args.mail_to is not None:
        frm = (
            args.mail_from
            if args.mail_from is not None
            else (getpass.getuser() + "@" + socket.getfqdn())
        )
        subj = (
            args.mail_subject if args.mail_subject is not None else ("Error on " + socket.getfqdn())
        )
        mh = logging.handlers.SMTPHandler(args.smtp_host, frm, args.mail_to, subj)
        mh.setLevel(logging.ERROR)
        mh.setFormatter(formatter)
        logger.addHandler(mh)

    return logger
