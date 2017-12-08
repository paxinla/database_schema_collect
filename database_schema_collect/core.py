#!/usr/bin/env python
# coding=utf-8

from __future__ import absolute_import

import sys
import logging

from database_schema_collect.util import load_conf
from database_schema_collect.util import set_log_level
from database_schema_collect.util import set_log_file
from database_schema_collect.Collector import PGCollector
from database_schema_collect.Handler import PGMetadataHandler

reload(sys)
sys.setdefaultencoding("utf-8")


logger = logging.getLogger("database_schema_collect")


def handler_dispatcher(config_filepath, action):
    config_obj = load_conf(config_filepath)
    datasrc_type = config_obj.get("datasource", "type").strip().lower()

    new_log_level = config_obj.get("log", "log_level")
    if new_log_level is not None:
        set_log_level(new_log_level)

    log_file_path = config_obj.get("log", "log_file_path")
    if log_file_path is not None:
        set_log_file(log_file_path)

    logger.debug("Use %s metadata handler." % datasrc_type)
    if datasrc_type == "postgresql":
        handler = PGMetadataHandler(config_obj)
        handler.process(action)
    else:
        raise ValueError("Unsupport data source type %s" % datasrc_type)
