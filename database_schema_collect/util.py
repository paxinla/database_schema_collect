#!/usr/bin/env python
# coding=utf-8

from __future__ import absolute_import

import sys
import os
import logging
import traceback
import ConfigParser

from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.sql.elements import TextClause
from sqlalchemy.exc import IntegrityError

reload(sys)
sys.setdefaultencoding("utf-8")


logFormatter = logging.Formatter('%(asctime)s [%(levelname)s] (%(pathname)s:%(lineno)d@%(funcName)s) -> %(message)s')
logger = logging.getLogger("database_schema_collect")
logger.setLevel(logging.INFO)
consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)


ROOTDIR = os.path.abspath(os.path.dirname(__file__))
LOG_LVL_MAP = {"critical": logging.CRITICAL,
               "error": logging.ERROR,
               "warning": logging.WARNING,
               "info": logging.INFO,
               "debug": logging.DEBUG}
BANK_NAME_PATTERN = "{}.bank.map"


def load_conf(conf_path):
    """Read an ini format configuration file and return an ConfigParser object.
    :param conf_path: Absolute path of the configuration file.
    :type conf_path: str

    :return conf_loader: A ConfigParser object contains all configuration keys and values.
    :rtype conf_loader: ConfigParser
    """
    conf_loader = ConfigParser.ConfigParser()
    conf_loader.read(conf_path)

    logger.debug("Read configuration from %s", conf_path)
    return conf_loader


def set_log_level(log_level):
    if log_level is not None and isinstance(log_level, str):
        log_level = log_level.strip().lower()
        if log_level in LOG_LVL_MAP:
            logger.setLevel(LOG_LVL_MAP[log_level])
        elif log_level == '':
            pass
        else:
            raise ValueError("Unsupport log level: {} !".format(log_level))


def set_log_file(log_filepath):
    file_handler = logging.FileHandler(log_filepath)
    file_handler.setFormatter(logFormatter)
    logger.addHandler(file_handler)


class PGAgent(object):
    """Tool class for manipulate postgresql."""

    def __init__(self, uri):
        self.eng = create_engine(uri, pool_size=20, pool_recycle=300)


    def execute_raw_query(self, sql_str, params=None):
        """Execute raw sql statement and return the result set.

        :param sql_str: String of SQL statement with named parameters.
        :type sql_str: str.
        :param params: Values of parameters.
        :type params: dict.
        :returns: An instance of ResultProxy.
        :raises:
        """
        with self.eng.connect() as conn:
            try:
                if params is not None and isinstance(params, dict):
                    qrs = conn.execute(text(sql_str), params)
                else:
                    qrs = conn.execute(text(sql_str))
                return qrs
            except:
                logger.error("SQL: %s" % sql_str)
                logger.error(traceback.format_exc())
                raise


    def query_one(self, sql_str, params=None):
        rspxy = self.execute_raw_query(sql_str, params)
        rs = rspxy.fetchone()
        return rs


    def query_all(self, sql_str, params=None):
        rspxy = self.execute_raw_query(sql_str, params)
        rs = rspxy.fetchall()
        if rs is not None:
            for each_row in rs:
                yield each_row
        else:
            yield None


def format_pg_col_str(column_obj):
    """Format (column name, column type) pair for PostgreSQL.
    :param column_obj: Column information.
    :type column_obj: An instance of ColumnMetaData.
    
    :returns: A pair of (column name, column type).
    :rtype: tuple.
    """
    if column_obj.column_length is not None:
        col_type = "{}({})".format(column_obj.column_data_type,
                                   column_obj.column_length)
    elif column_obj.column_data_type in ('smaillint', 'integer', 'bigint', 'serial', 'bigserial'):
        col_type = "{}".format(column_obj.column_data_type)
    elif column_obj.column_precision is not None and \
            column_obj.column_scale is not None and column_obj.column_scale > 0:
        col_type = "{}({},{})".format(column_obj.column_data_type,
                                      column_obj.column_precision,
                                      column_obj.column_scale)
    elif column_obj.column_precision is not None and column_obj.column_precision > 0:
        col_type = "{}({})".format(column_obj.column_data_type,
                                   column_obj.column_precision)
    else:
        col_type = "{}".format(column_obj.column_data_type)

    return (column_obj.column_name, col_type)
