#!/usr/bin/env python
# coding=utf-8

from __future__ import absolute_import

import sys
import os
import shutil
import logging
import traceback
from abc import abstractmethod
from itertools import groupby

from database_schema_collect.util import format_pg_col_str
from database_schema_collect.ERD import ERD
from database_schema_collect.DataDictionary import DataDictionary

reload(sys)
sys.setdefaultencoding("utf-8")


logger = logging.getLogger("database_schema_collect")



class Exporter(object):
    """Export meta data to files."""

    @abstractmethod
    def exp_ddl_sql_file(self, des_loc, des_dir):
        """Export meta data as DLL to sql file"""
        raise NotImplementedError()


    @abstractmethod
    def exp_data_dict_excel(self, des_loc, des_dir):
        """Export meta data as data dictionary to Excel file."""
        raise NotImplementedError()

    @abstractmethod
    def exp_db_erd(self, des_loc, des_dir):
        """Export database or schema meta data as ERD to image file."""
        raise NotImplementedError()


class PGExporter(Exporter):
    """Exporter for PostgreSQL."""


    def __init__(self, meta_bank_obj, temp_dir):
        """
        :param meta_bank_obj: The meta data container.
        :type meta_bank_obj: dict.
        :param temp_dir: Temporary directory.
        :type temp_dir: str.
        """
        self.bank_obj = meta_bank_obj
        self.temp_dir = temp_dir

        if not os.path.exists(self.temp_dir):
            os.makedirs(self.temp_dir)

        self.tpl_ddl_db = "CREATE DATABASE {dbname}\n" + \
                          "OWNER={dbowner}\n" + \
                          "ENCODING='{dbenc}'\n" + \
                          "LC_COLLATE='{dbcoll}'\n" + \
                          "LC_CTYPE='{dbctype}'\n" + \
                          "TEMPLATE=template0\n" + \
                          "TABLESPACE={dbdtbs}\n" + \
                          "CONNCTION LIMIT = {dbconnlmt};\n" + \
                          "COMMENT ON DATABASE {dbname} IS '{dbcmt}';"

        self.tpl_ddl_tbs = "CREATE TABLESPACE {tbsname} LOCATION '{tbsloc}';\nCOMMENT ON TABLESPACE {tbsname} IS '{tbscmt}'; "
        self.tpl_ddl_tbs_owner = """ALTER TABLESPACE {tbsname} OWNER TO {tbsowner};"""

        self.tpl_ddl_schema = """CREATE SCHEMA IF NOT EXISTS {smname};"""
        self.tpl_ddl_schema_owner = """ALTER SCHEMA {smname} OWNER TO {smowner};"""

        self.tpl_ddl_table_part1 = "DROP TABLE IF EXISTS {fulltbname} CASCADE;\n" + \
                                   "CREATE TABLE IF NOT EXISTS {fulltbname}\n" + \
                                   "("
        self.tpl_ddl_table_part2 = """) TABLESPACE {tbsname}; """
        self.tpl_ddl_table_part3 = """ALTER TABLE {fulltbname} OWNER TO {tbowner}; """

        self.tpl_ddl_pk_part1 = """ALTER TABLE {fulltbname} ADD CONSTRAINT {pk_name} PRIMARY KEY ("""
        self.tpl_ddl_uk_part1 = """ALTER TABLE {fulltbname} ADD CONSTRAINT {uk_name} UNIQUE ("""
        self.tpl_ddl_k_part2 = """) USING INDEX TABLESPACE {tbsname}; """
        self.tpl_ddl_ck = """ALTER TABLE {fulltbname} ADD CONSTRAINT {ck_name} CHECK ( {ck_def} ); """
        self.tpl_ddl_fk = "ALTER TABLE {fulltbname}\n" +\
                          "        ADD CONSTRAINT {fk_name}\n" + \
                          "    FOREIGN KEY ({fk_col}) REFERENCES {fk_ref_tbname} ({fk_ref_colname})\n" + \
                          "         ON UPDATE CASCADE\n" + \
                          "         ON DELETE CASCADE;"

        self.tpl_ddl_comment_table = """COMMENT ON TABLE {fulltbname} IS '{tbcmt}'; """
        self.tpl_ddl_comment_column = """COMMENT ON COLUMN {fulltbname}.{colname} IS '{colcmt}'; """

        self.tpl_ddl_index = """{idxdef} TABLESPACE {idxtbs}; """


    def exp_ddl_sql_file(self, des_loc, des_dir):
        """Export meta data as DLL to sql file.

        :param des_loc: Location of the des_dir.
        :type des_loc: An instance of Location.
        :param des_dir: Directory for storing sql files.
        :type des_dir: str.
        """
        last_dir_name = "ddl"
        dbname = self.bank_obj.database.database_name
        topdir = os.path.join(self.temp_dir, last_dir_name)
        if not os.path.exists(topdir):
            os.makedirs(topdir)

        # DDL for database, not support foreign server, search_path, users, etc.
        with open(os.path.join(topdir, "ddl_database_{}.sql".format(dbname)), "w+") as dwf:
            # tablespaces
            for each_tbs in self.bank_obj.tablespaces:
                dwf.write(self.tpl_ddl_tbs.format(tbsname=each_tbs.tablespace_name,
                                                  tbsloc=each_tbs.tablespace_location,
                                                  tbscmt=each_tbs.tablespace_comment))
                dwf.write('\n')
                if each_tbs.tablespace_owner is not None and \
                   each_tbs.tablespace_owner.strip() != '':
                    dwf.write(self.tpl_ddl_tbs_owner.format(tbsname=each_tbs.tablespace_name,
                                                            tbsowner=each_tbs.tablespace_owner))
                    dwf.write('\n')

            dwf.write('\n\n')

            # database itself
            db_meta = self.bank_obj.database
            if db_meta.database_default_data_tablespace is not None and \
               db_meta.database_default_data_tablespace.strip() != '':
                db_dtbs = 'pg'
            else:
                db_dtbs =db_meta.database_default_data_tablespace
            dwf.write(self.tpl_ddl_db.format(dbname=dbname,
                                             dbowner=db_meta.database_owner,
                                             dbenc=db_meta.database_encoding,
                                             dbcoll=db_meta.database_lc_collate,
                                             dbctype=db_meta.database_lc_ctype,
                                             dbdtbs=db_dtbs,
                                             dbconnlmt=db_meta.database_connection_limit,
                                             dbcmt=db_meta.database_comment))

            dwf.write('\n\n')

            # schemas
            for each_schema in self.bank_obj.schemas:
                dwf.write(self.tpl_ddl_schema.format(smname=each_schema.schema_name))
                dwf.write('\n')
                if each_schema.schema_owner is not None and \
                   each_schema.schema_owner.strip() != '':
                    dwf.write(self.tpl_ddl_schema_owner.format(smname=each_schema.schema_name,
                                                               smowner=each_schema.schema_owner))
                    dwf.write('\n')

        # DDL for tables and views, not support foreign table yet.
        with open(os.path.join(topdir, "ddl_table_{}.sql".format(dbname)), "w+") as twf:
            for each_tb in self.bank_obj.tables:
                fulltbname = "{}.{}".format(each_tb.table_schemaname, each_tb.table_name)
                logger.info("Generate DDL for table %s" % fulltbname)

                if each_tb.table_comment is not None and each_tb.table_comment.strip() != '':
                    head_comment = "-- {} :: {}".format(fulltbname, each_tb.table_comment)
                else:
                    head_comment = "-- {}".format(fulltbname)
                twf.write(head_comment)
                twf.write('\n')

                # table body.
                twf.write(self.tpl_ddl_table_part1.format(fulltbname=fulltbname))
                twf.write('\n')

                col_type_start_pos = each_tb.column_longest_length + 4
                all_cols_in_tb = sorted(each_tb.columns, key=lambda x:x.column_index)
                for each_column in all_cols_in_tb:
                    logger.debug("  -> gen DDL for column %s" % each_column.column_name)

                    _, col_type = format_pg_col_str(each_column)
                    logger.debug("  -> type of column %s is %s" % (each_column.column_name,
                                                                   col_type))

                    if each_column.column_index == 1:
                        col_str = "     {}".format(each_column.column_name)
                    else:
                        col_str = "   , {}".format(each_column.column_name)

                    fill_blank_cnt = col_type_start_pos - len(each_column.column_name)
                    for i in range(fill_blank_cnt):
                        col_str += ' '
                    col_str += "{}".format(col_type)

                    if not each_column.column_is_nullable:
                        col_str += "    NOT NULL"

                    if each_column.column_default_value is not None and \
                            "nextval" not in each_column.column_default_value and \
                            each_column.column_default_value != '':
                        col_str += "    DEFAULT {}".format(each_column.column_default_value)

                    logger.debug("  -> DDL of column %s is %s" % (each_column.column_name, col_str))
                    twf.write(col_str)
                    twf.write('\n')

                twf.write(self.tpl_ddl_table_part2.format(tbsname=each_tb.table_tablespace))
                twf.write('\n')

                if each_tb.table_owner is not None and each_tb.table_owner.strip() != '':
                    logger.debug("Owner of table %s is %s" % (fulltbname, each_tb.table_owner))
                    twf.write(self.tpl_ddl_table_part3.format(fulltbname=fulltbname,
                                                              tbowner=each_tb.table_owner))
                    twf.write('\n')

                twf.write('\n')

                # primary key
                pk_cols = []
                pk_name = ''
                pk_tbs = None
                for each_pk in sorted(each_tb.primary_key, key=lambda x:x.pk_column_index):
                    pk_cols.append(each_pk.pk_column)
                    if each_pk.pk_column_index == 1:
                        pk_name = each_pk.pk_name
                        pk_tbs = each_pk.pk_tablespace

                if pk_name is not None and pk_name.strip() != '':
                    logger.debug("DDL of primary key %s" % pk_name)
                    twf.write(self.tpl_ddl_pk_part1.format(fulltbname=fulltbname,
                                                           pk_name=pk_name))
                    logger.debug("  -> columns of primary key %s are %s" % (pk_name, ','.join(pk_cols)))
                    twf.write(', '.join(pk_cols))
                    if pk_tbs is not None and pk_tbs.strip() != '':
                        logger.debug("  -> tablespace of primary key %s is %s" % (pk_name, pk_tbs))
                        twf.write(self.tpl_ddl_k_part2.format(tbsname=pk_tbs))
                    else:
                        twf.write(");")
                    twf.write('\n')

                # unique keys
                uks = groupby(sorted(each_tb.unique_keys, key=lambda x:x.uk_name), key=lambda x:x.uk_name)
                for each_uk in uks:
                    uk_cols = []
                    uk_name = each_uk[0]
                    uk_tbs = None
                    logger.debug("DDL for unique key %s" % uk_name)
                    for each_uk_col in sorted(each_uk[1], key=lambda x:x.uk_column_index):
                        uk_cols.append(each_uk_col.uk_column)
                        if each_uk_col.uk_column_index == 1:
                            uk_tbs = each_uk_col.uk_tablespace
                    if uk_name is not None and uk_name.strip() != '':
                        twf.write(self.tpl_ddl_uk_part1.format(fulltbname=fulltbname,
                                                               uk_name=uk_name))
                        logger.debug("  -> columns of unique key %s are %s" % (uk_name, ','.join(uk_cols)))
                        twf.write(', '.join(uk_cols))
                        if uk_tbs is not None and uk_tbs.strip() != '':
                            logger.debug("  -> tablespace of unique key %s is %s" % (uk_name, uk_tbs))
                            twf.write(self.tpl_ddl_k_part2.format(tbsname=uk_tbs))
                        else:
                            twf.write(");")
                        twf.write('\n')

                # table comment and column comments.
                if each_tb.table_comment is not None and each_tb.table_comment.strip() != '':
                    logger.debug("Comment of table %s is %s" % (fulltbname, each_tb.table_comment))
                    twf.write('\n')
                    twf.write(self.tpl_ddl_comment_table.format(fulltbname=fulltbname,
                                                                tbcmt=each_tb.table_comment))
                    twf.write('\n')
                for each_column in all_cols_in_tb:
                    if each_column.column_comment is not None and each_column.column_comment.strip() != '':
                        logger.debug("  -> Comment of column %s is %s" % (each_column.column_name, each_column.column_comment))
                        twf.write(self.tpl_ddl_comment_column.format(fulltbname=fulltbname,
                                                                     colname=each_column.column_name,
                                                                     colcmt=each_column.column_comment))
                        twf.write('\n')

                # check constraints
                for each_check in each_tb.checks:
                    logger.debug("DDL of check constraint %s of %s" % (each_check.check_name, fulltbname))
                    twf.write(self.tpl_ddl_ck.format(fulltbname=fulltbname,
                                                     ck_name=each_check.check_name,
                                                     ck_def=each_check.check_define))
                    twf.write('\n')

                # foreign keys.
                fk_1st_line = True
                for each_fk in each_tb.foreign_keys:
                    logger.debug("DDL of foreign key %s of %s" % (each_fk.fk_name, fulltbname))
                    if fk_1st_line:
                        twf.write('\n')
                        fk_1st_line = False
                    twf.write(self.tpl_ddl_fk.format(fulltbname=fulltbname,
                                                     fk_name=each_fk.fk_name,
                                                     fk_col=each_fk.fk_column,
                                                     fk_ref_tbname=each_fk.fk_ref_tablename,
                                                     fk_ref_colname=each_fk.fk_ref_column))
                    twf.write('\n')

                # indexes.
                idx_1st_line = True
                for each_index in each_tb.indexes:
                    logger.debug("DDL of index %s of %s" % (each_index.index_name, fulltbname))
                    if idx_1st_line:
                        twf.write('\n')
                        idx_1st_line = False
                    index_def_stmt = each_index.index_define.replace("ON {}".format(each_tb.table_name),
                                                                     "ON {}".format(fulltbname))
                    twf.write(self.tpl_ddl_index.format(idxdef=index_def_stmt,
                                                        idxtbs=each_index.index_tablespace))
                    twf.write('\n')

                twf.write('\n\n')

        if not os.path.exists(des_dir):
            os.makedirs(des_dir)

        if os.path.exists(os.path.join(des_dir, last_dir_name)):
            shutil.rmtree(os.path.join(des_dir, last_dir_name))

        logger.debug("Move all ddl files from %s to %s" % (topdir, des_dir))
        des_loc.move_file_to(topdir, des_dir)


    def exp_data_dict_excel(self, des_loc, des_dir):
        """Export meta data to Excel file.

        :param des_loc: Location of the des_dir.
        :type des_loc: An instance of Location.
        :param des_dir: Directory for storing sql files.
        :type des_dir: str.
        """
        last_dir_name = "dict"
        dbname = self.bank_obj.database.database_name

        topdir = os.path.join(self.temp_dir, last_dir_name)
        if not os.path.exists(topdir):
            os.makedirs(topdir)

        writer = DataDictionary(dbname, self.bank_obj, topdir)
        logger.info("Generate data dictionary of database %s" % dbname)
        writer.gen_dictionanry()

        if not os.path.exists(des_dir):
            os.makedirs(des_dir)

        if os.path.exists(os.path.join(des_dir, last_dir_name)):
            shutil.rmtree(os.path.join(des_dir, last_dir_name))

        logger.debug("Move data dictionary file from %s to %s" % (topdir, des_dir))
        des_loc.move_file_to(topdir, des_dir)


    def exp_db_erd(self, des_loc, des_dir):
        """Export meta data as ERD to text file.

        :param des_loc: Location of the des_dir.
        :type des_loc: An instance of Location.
        :param des_dir: Directory for storing sql files.
        :type des_dir: str.
        """
        last_dir_name = "erd"
        dbname = self.bank_obj.database.database_name
        drawer = ERD()

        topdir = os.path.join(self.temp_dir, last_dir_name)
        if not os.path.exists(topdir):
            os.makedirs(topdir)

        logger.info("Generate define file of ERD of database %s" % dbname)
        drawer.gen_erd(self.bank_obj, topdir)

        if not os.path.exists(des_dir):
            os.makedirs(des_dir)

        if os.path.exists(os.path.join(des_dir, last_dir_name)):
            shutil.rmtree(os.path.join(des_dir, last_dir_name))

        logger.debug("Move all erd files from %s to %s" % (topdir, des_dir))
        des_loc.move_file_to(topdir, des_dir)
