#!/usr/bin/env python3
"""ctp: the runner of the cregit token pipeline. The code is in the ctplib package.
ctplib/cli.py defines the subcommands; `./ctp.py --help` lists them."""

import sys

from ctplib.cli import main

if __name__ == "__main__":
    sys.exit(main())
