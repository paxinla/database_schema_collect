#!/usr/bin/env python
# coding=utf-8

from __future__ import absolute_import

import sys
import logging
import traceback
import argparse

from database_schema_collect.core import handler_dispatcher

reload(sys)
sys.setdefaultencoding("utf-8")


logger = logging.getLogger("database_schema_collect")


def parse_input():
    """Provide a command line interface."""
    parser = argparse.ArgumentParser(description="Collect and manage meta data of database.")

    parser.add_argument("--config", dest="conf_path", type=str, required=True,
                        help="Path of the config file.")
    parser.add_argument("--do", dest="action", type=str, required=True,
                        choices=("collect", "ddl", "erd", "dict"),
                        help="""Action will be performed.
                                - collect: collect the meta data of target database and store them.
                                - ddl: generate ddl sql files of objects in the target database.
                                - erd: generate database erd to png file.
                                - dict: genderate data dictionary of database to Excel file. """)

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)

    parsed_args = parser.parse_args()

    return vars(parsed_args)


def main():
    """Entry point of the program."""
    in_args = parse_input()

    handler_dispatcher(in_args["conf_path"],
                       in_args["action"])


if __name__ == "__main__":
    main()
