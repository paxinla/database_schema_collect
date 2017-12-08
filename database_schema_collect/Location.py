#!/usr/bin/env python
# coding=utf-8

from __future__ import absolute_import

import sys
import os
import shutil
import logging
import traceback
from abc import abstractmethod

import requests

reload(sys)
sys.setdefaultencoding("utf-8")


logger = logging.getLogger("database_schema_collect")


class Location(object):
    """Location for storing meta data files."""

    @abstractmethod
    def move_file_to(self, src_file_path, des_file_path):
        """Move a file from source path to target path.

        :param src_file_path: path of source file.
        :type src_file_path: str.
        :param des_file_path: path of destination.
        :type des_file_path: str.
        """
        raise NotImplementedError()

    @abstractmethod
    def open_file(self, src_file_path):
        """Return content of a file.

        :param src_file_path: path of source file
        :type src_file_path: str
        """
        raise NotImplementedError()


class LocalLocation(Location):
    """Location in local file system."""

    def move_file_to(self, src_file_path, des_file_path):
        shutil.move(src_file_path, des_file_path)


    def open_file(self, src_file_path):
        if not os.path.exists(src_file_path):
            return None

        with open(src_file_path, "rb") as rf:
            return rf.read()


class HDFSLocation(Location):
    """Location in HDFS."""

    def __init__(self, host, port=None):
        self.uri_prefix = "http://{}:{}/webhdfs/v1/".format(host, port if port is not None else 50070)
        self._global_api_session = requests.session()


    def move_file_to(self, src_file_path, des_file_path):
        with open(src_file_path, "rb") as rf:
            resp = self._global_api_session.put("{}{}?op=CREATE".format(self.uri_prefix,
                                                                        des_file_path),
                                                data=rf.read())
            logger.debug(resp.text)


    def open_file(self, src_file_path):
        resp = self._global_api_session.get("{}{}?op=OPEN".format(self.uri_prefix,
                                                                  src_file_path))
        return resp.text
