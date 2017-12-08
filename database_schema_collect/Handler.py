#!/usr/bin/env python
# coding=utf-8

from __future__ import absolute_import

import sys
import os
import shutil
import logging

import cPickle as pickle
from marshmallow import pprint

from database_schema_collect.Collector import PGCollector
from database_schema_collect.Location import LocalLocation
from database_schema_collect.Location import HDFSLocation
from database_schema_collect.MetaDataBank import MetaDataBank
from database_schema_collect.MetaDataBank import DatabaseMetaDataBankSchema
from database_schema_collect.Exporter import PGExporter
from database_schema_collect.util import BANK_NAME_PATTERN

reload(sys)
sys.setdefaultencoding("utf-8")


logger = logging.getLogger("database_schema_collect")


class PGMetadataHandler(object):
    """Meta data handler for PostgreSQL."""

    def __init__(self, conf_obj):
        self.conf = conf_obj


    def collect_metadata(self, db_uri, metabank):
        """Collect meta data of all supported database objects

        :param db_uri: Connect string for PostgreSQL database.
        :type db_uri: str.
        :param metabank: Object contains meta data objects belong to a specific database.
        :param metabank: An instance of MetaDataBank.
        :returns: An instance of MetaDataBank which store collected meta data.
        """
        collector = PGCollector(db_uri, metabank)
        target_database_name = self.conf.get("datasource", "dbname")

        logger.info("Gather meta data for database: %s" % target_database_name)
        collector.get_metadata_database(target_database_name)

        logger.info("Gather meta data of tablespaces.")
        collector.get_metadata_tablespaces()

        logger.info("Gather meta data of foreign servers.")
        collector.get_metadata_foreign_server()

        logger.info("Gather meta data of foreign tables.")
        collector.get_metadata_foreign_table()

        for each_schema in collector.list_schemas_in_database(target_database_name):
            if each_schema is None:
                break
            logger.info("Gather meta data of tables in schema %s" % each_schema)
            collector.link_tables_to_database(each_schema)

            logger.info("Gather meta data of views in schema %s" % each_schema)
            collector.list_views_in_schema(each_schema)

        return collector.get_metadata_bank()


    def store_metadata_to_file(self, metabank, store_loc):
        """Iter metadata objects in metabank and store them into store location.

        :param metabank: Object contains meta data objects belong to a specific database.
        :param metabank: An instance of MetaDataBank.
        :param store_loc: Object represents store location.
        :type store_loc: An instance of Location.
        """
        metabank.iter_and_save_metadata(store_loc,
                                        self.conf.get("storage", "directory"))


    def _get_metabank_from_file(self, store_loc):
        """Deserialize a MetaDataBank object from specific file.

        :param store_loc: Object represents store location.
        :type store_loc: An instance of Location.
        :returns: dict.
        """
        expect_metabank_file_path = os.path.join(self.conf.get("storage", "directory"),
                                                 self.conf.get("datasource", "dbname"),
                                                 BANK_NAME_PATTERN.format(self.conf.get("datasource", "dbname")))
        logger.debug("Try to find metabank file: %s" % expect_metabank_file_path)
        metabank_file_obj = store_loc.open_file(expect_metabank_file_path)
        if metabank_file_obj is None:
            deser_obj = None
        else:
            raw_metabank = pickle.loads(metabank_file_obj)
            deser_obj = DatabaseMetaDataBankSchema().load(raw_metabank).data
        return deser_obj


    def gen_erd(self, metabank, temp_dir, store_loc):
        """Generate ERD for all database objects(those are supported), and
           store them to image files under store directory.

        :param metabank: Object contains meta data objects belong to a specific database.
        :type metabank: dict.
        :param temp_dir: Temporary working directory.
        :type temp_dir: str.
        :param store_loc: Object represents store location.
        :type store_loc: An instance of Location.
        """
        exporter = PGExporter(metabank, temp_dir)
        exporter.exp_db_erd(store_loc,
                            self.conf.get("storage", "erd_directory"))


    def gen_data_dictionary_file(self, metabank, temp_dir, store_loc):
        """Generate data dictionary for all database objects(those are supported), and
           store them to Excel files under store directory.

        :param metabank: Object contains meta data objects belong to a specific database.
        :type metabank: dict.
        :param temp_dir: Temporary working directory.
        :type temp_dir: str.
        :param store_loc: Object represents store location.
        :type store_loc: An instance of Location.
        """
        exporter = PGExporter(metabank, temp_dir)
        exporter.exp_data_dict_excel(store_loc,
                                     self.conf.get("storage", "dict_directory"))


    def gen_ddl_file(self, metabank, temp_dir, store_loc):
        """Generate DDL for all database objects(those are supported), and
           store them to SQL files under store directory.

        :param metabank: Object contains meta data objects belong to a specific database.
        :type metabank: dict.
        :param temp_dir: Temporary working directory.
        :type temp_dir: str.
        :param store_loc: Object represents store location.
        :type store_loc: An instance of Location.
        """
        exporter = PGExporter(metabank, temp_dir)
        exporter.exp_ddl_sql_file(store_loc,
                                  self.conf.get("storage", "ddl_directory"))


    def _choose_location(self):
        """Choose the right location type due to storage type.

        :returns: An instance of Location.
        :raises: ValueError.
        """
        store_type = self.conf.get("storage", "type").strip().lower()

        if store_type == "local":
            return LocalLocation()
        elif store_type == "hdfs":
            return HDFSLocation(host=self.conf.get("storage", "webhdfs_host"),
                                port=self.conf.get("storage", "webhdfs_port"))
        else:
            raise ValueError("Valid storage type are: local|hdfs, got {}".format(store_type))


    def process(self, action):
        """Main process of the handler.

        :param action: Action for handler, only support: collect|erd|ddl|datadict.
        :type action: str.
        :raises: ValueError.
        """
        if action is None or action.strip() == '':
            raise ValueError("Must specify action!")

        tmp_wrk_dir = self.conf.get("local", "temp_work_dir")

        if tmp_wrk_dir is None:
            raise ValueError("Must have a valid temp directory!")

        if not os.path.exists(tmp_wrk_dir):
            os.makedirs(tmp_wrk_dir)

        action = action.strip().lower()
        loc_obj = self._choose_location()

        if action == "collect":
            pg_uri = self.conf.get("datasource", "uri")
            logger.debug("Use DB URI: %s" % pg_uri)
            metabank = self.collect_metadata(pg_uri, MetaDataBank(tmp_wrk_dir))

            self.store_metadata_to_file(metabank, loc_obj)
        elif action == "ddl":
            metabank = self._get_metabank_from_file(loc_obj)
            if metabank is None:
                raise ValueError("No meta data avaibled, must collect them first!")

            self.gen_ddl_file(metabank, tmp_wrk_dir, loc_obj)
        elif action == "erd":
            metabank = self._get_metabank_from_file(loc_obj)
            if metabank is None:
                raise ValueError("No meta data avaibled, must collect them first!")

            self.gen_erd(metabank, tmp_wrk_dir, loc_obj)
        elif action == "dict":
            metabank = self._get_metabank_from_file(loc_obj)
            if metabank is None:
                raise ValueError("No meta data avaibled, must collect them first!")

            self.gen_data_dictionary_file(metabank, tmp_wrk_dir, loc_obj)
        else:
            raise ValueError("Unsupport action: {!s}".format(action))

        if os.path.isdir(tmp_wrk_dir):
            shutil.rmtree(tmp_wrk_dir)
