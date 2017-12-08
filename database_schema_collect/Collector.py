#!/usr/bin/env python
# coding=utf-8

from __future__ import absolute_import

import sys
import logging
from abc import abstractmethod
from collections import namedtuple

from database_schema_collect.util import PGAgent
from database_schema_collect.MetaDataBank import DatabaseMetaData
from database_schema_collect.MetaDataBank import TablespaceMetaData
from database_schema_collect.MetaDataBank import FServerMetaData
from database_schema_collect.MetaDataBank import FTableMetaData
from database_schema_collect.MetaDataBank import SchemaMetaData
from database_schema_collect.MetaDataBank import ColumnMetaData
from database_schema_collect.MetaDataBank import PKMetaData
from database_schema_collect.MetaDataBank import UKMetaData
from database_schema_collect.MetaDataBank import FKMetaData
from database_schema_collect.MetaDataBank import CheckMetaData
from database_schema_collect.MetaDataBank import IndexMetaData
from database_schema_collect.MetaDataBank import TableMetaData
from database_schema_collect.MetaDataBank import ViewMetaData

reload(sys)
sys.setdefaultencoding("utf-8")


logger = logging.getLogger("database_schema_collect")


class Collector(object):
    """Collector get metadata of database objects and store them."""

    @abstractmethod
    def get_metadata_database(self, databasename):
        """Get metadata of a given database."""
        raise NotImplementedError()


    @abstractmethod
    def list_schemas_in_database(self, databasename):
        """List schema names in a given database."""
        raise NotImplementedError()


    @abstractmethod
    def list_tablenames_in_schema(self, schemaname):
        """List table names in a given schema."""
        raise NotImplementedError()


    @abstractmethod
    def get_metadata_table(self, schemaname, tablename):
        """Get metadata of a given table."""
        raise NotImplementedError()


    @abstractmethod
    def list_views_in_schema(self, schemaname):
        """List view names of a given schema."""
        raise NotImplementedError()


    @abstractmethod
    def get_metadata_view(self, schemaname, viewname):
        """Get metadata of a given view."""
        raise NotImplementedError()


    @abstractmethod
    def get_metadata_bank(self):
        """Return metadata bank which store all collected meta data."""
        raise NotImplementedError()


TableName = namedtuple('TableName', ['tablename', 'schemaname', 'owner', 'tablespace', 'comment'])
class PGCollector(Collector):
    """Collect metadata in PostgreSQL."""

    def __init__(self, db_uri, metadata_bank):
        self.pgagent = PGAgent(db_uri)
        self.metadata_bank = metadata_bank

        self.sql_dbinfo = """SELECT d.datname
                                  , p.description
                                  , pg_catalog.pg_get_userbyid(d.datdba)
                                  , pg_encoding_to_char(d.encoding)
                                  , d.datcollate
                                  , d.datctype
                                  , tbs.spcname
                                  , d.datconnlimit
                               FROM pg_database d
                          LEFT JOIN pg_shdescription p
                                 ON p.objoid = d.oid
                          LEFT JOIN pg_tablespace tbs
                                 ON tbs.oid = d.dattablespace
                              WHERE d.datname = :databasename """

        self.sql_tbsinfo = """SELECT t.spcname
                                   , pg_tablespace_location(t.oid)
                                   , pg_catalog.pg_get_userbyid(t.spcowner)
                                   , p.description
                                FROM pg_tablespace t
                           LEFT JOIN pg_shdescription p
                                  ON p.objoid = t.oid
                               WHERE t.spcname NOT LIKE 'pg%' """

        self.sql_sminfo = """SELECT nspname
                                  , pg_catalog.pg_get_userbyid(nspowner)
                               FROM pg_namespace
                              WHERE nspname NOT LIKE 'pg%'
                                AND nspname != 'information_schema' """

        self.sql_tblist = """SELECT t.tablename
                                  , t.schemaname
                                  , t.tableowner
                                  , (SELECT s.spcname
                                       FROM pg_tablespace s
                                      WHERE s.oid = (string_to_array(pg_catalog.pg_relation_filepath((t.schemaname||'.'||t.tablename)::regclass::oid), '/'))[2]::int)
                                  , pg_catalog.obj_description(t.tablename::regclass::oid)
                               FROM pg_tables t
                              WHERE t.schemaname = :schemaname """

        self.sql_colinfo = """SELECT c.column_name
                                   , c.ordinal_position
                                   , pg_catalog.col_description((:fulltablename)::regclass::oid, c.ordinal_position)  AS col_comment
                                   , CASE WHEN c.data_type = 'bigint'
                                           AND substr(c.column_default, 1, 7) = 'nextval'
                                          THEN 'bigserial'
                                          WHEN c.data_type = 'int'
                                           AND substr(c.column_default, 1, 7) = 'nextval'
                                          THEN 'serial'
                                          WHEN c.data_type = 'ARRAY'
                                          THEN regexp_replace(regexp_replace(c.udt_name, '[248]', ''), '^_', '')||'[]'
                                          ELSE replace(c.data_type, '"', '')
                                     END                         AS col_datatype
                                   , c.character_maximum_length  AS col_length
                                   , c.numeric_scale
                                   , c.numeric_precision
                                   , c.column_default
                                   , substring(c.column_default, '\(''(.*)''::')  AS col_auto_inc
                                   , CASE WHEN trim(c.is_nullable) = 'YES'
                                          THEN 'Y'
                                          ELSE 'N'
                                     END                         AS col_is_nullable
                                   , CASE WHEN tc.constraint_type = 'PRIMARY KEY'
                                          THEN '-> ' || tc.constraint_name
                                          ELSE ''
                                     END                         AS col_in_pk
                                   , CASE WHEN tc.constraint_type = 'FOREIGN KEY'
                                          THEN '-> ' || tc.constraint_name
                                          ELSE ''
                                     END                         AS col_in_fk
                                FROM information_schema.columns c
                           LEFT JOIN information_schema.key_column_usage kcu
                                  ON kcu.table_schema = c.table_schema
                                 AND kcu.table_name = c.table_name
                                 AND kcu.column_name = c.column_name
                           LEFT JOIN information_schema.table_constraints tc
                                  ON tc.constraint_schema = kcu.constraint_schema
                                 AND tc.constraint_name = kcu.constraint_name
                               WHERE c.table_schema = :schemaname
                                 AND c.table_name = :tablename """

        self.sql_pkinfo = """SELECT tc.constraint_name
                                  , kcu.column_name
                                  , kcu.ordinal_position
                                  , pis.tablespace
                               FROM information_schema.table_constraints tc
                               JOIN information_schema.key_column_usage kcu
                                 ON kcu.constraint_schema = tc.constraint_schema
                                AND kcu.constraint_name = tc.constraint_name
                          LEFT JOIN pg_indexes pis
                                 ON pis.indexname = tc.constraint_name
                                AND pis.schemaname = tc.constraint_schema
                                                       WHERE tc.table_schema = :schemaname
                                                         AND tc.table_name = :tablename
                                                         AND tc.constraint_type = 'PRIMARY KEY' """

        self.sql_ukinfo = """SELECT tc.constraint_name
                                  , kcu.column_name
                                  , kcu.ordinal_position
                                  , pis.tablespace
                               FROM information_schema.table_constraints tc
                               JOIN information_schema.key_column_usage kcu
                                 ON kcu.constraint_schema = tc.constraint_schema
                                AND kcu.constraint_name = tc.constraint_name
                          LEFT JOIN pg_indexes pis
                                 ON pis.indexname = tc.constraint_name
                                AND pis.schemaname = tc.constraint_schema
                                                       WHERE tc.table_schema = :schemaname
                                                         AND tc.table_name = :tablename
                                                         AND tc.constraint_type = 'UNIQUE' """

        self.sql_ckinfo = """SELECT cc.constraint_name
                                  , cc.check_clause
                               FROM information_schema.table_constraints tc
                               JOIN information_schema.check_constraints cc
                                 ON cc.constraint_schema = tc.constraint_schema
                                AND cc.constraint_name = tc.constraint_name
                                AND tc.constraint_type = 'UNIQUE'
                              WHERE tc.table_schema = :schemaname
                                AND tc.table_name = :tablename
                                AND cc.constraint_name NOT LIKE '%_not_null' """

        self.sql_fkinfo = """ SELECT temp.fk_name
                                   , temp.column_name
                                   , temp.reference_table_name
                                   , temp.reference_column_name
                                FROM (  SELECT tc.constraint_name  AS fk_name
                                             , string_agg(kcu.column_name, ',')
                                               OVER(PARTITION BY tc.constraint_name
                                                               , ccu.table_name
                                                        ORDER BY kcu.ordinal_position ASC
                                                   )               AS column_name
                                             , ccu.table_name      AS reference_table_name
                                             , string_agg(ccu.column_name, ',')
                                               OVER(PARTITION BY tc.constraint_name
                                                               , ccu.table_name
                                                        ORDER BY kcu.ordinal_position ASC
                                                   )               AS reference_column_name
                                             , ROW_NUMBER()
                                               OVER(PARTITION BY tc.constraint_name
                                                               , ccu.table_name
                                                        ORDER BY kcu.ordinal_position ASC
                                                   )               AS rn
                                             , COUNT(1)
                                               OVER(PARTITION BY tc.constraint_name
                                                               , ccu.table_name
                                                   )               AS cnt
                                          FROM information_schema.table_constraints tc
                                          JOIN information_schema.key_column_usage kcu
                                            ON tc.constraint_name = kcu.constraint_name
                                          JOIN information_schema.constraint_column_usage ccu
                                            ON ccu.constraint_name = tc.constraint_name
                                           AND kcu.column_name = ccu.column_name
                                         WHERE tc.constraint_type = 'FOREIGN KEY'
                                           AND tc.table_schema = :schemaname
                                           AND tc.table_name = :tablename
                                     ) temp
                               WHERE temp.cnt = temp.rn """

        self.sql_indexinfo = """SELECT temp.index_name
                                     , temp.index_columns
                                     , temp.index_type
                                     , temp.index_tablespace
                                     , temp.index_define
                                  FROM ( SELECT i.indexname          AS index_name
                                              , string_agg(att.attname, ', ')over(partition by att.attrelid order by att.attnum)  AS index_columns
                                              , a.amname             AS index_type
                                              , i.tablespace         AS index_tablespace
                                              , i.indexdef           AS index_define
                                              , att.attnum
                                              , max(att.attnum)over(partition by att.attrelid)   AS idx_col_last
                                           FROM pg_indexes i
                                           JOIN pg_class c
                                             ON c.relname = i.indexname
                                           JOIN pg_am a
                                             ON a.oid = c.relam
                                           LEFT JOIN pg_attribute att
                                             ON att.attrelid = c.oid
                                          WHERE i.schemaname = :schemaname
                                            AND i.tablename = :tablename
                                            AND i.indexname NOT IN (SELECT u.constraint_name
                                                                      FROM information_schema.constraint_table_usage u 
                                                                     WHERE u.table_schema = :schemaname
                                                                       AND u.table_name = :tablename
                                                                   )
                                       ) temp
                                 WHERE temp.attnum = temp.idx_col_last """

        self.sql_vwinfo = """SELECT viewname
                                  , viewowner
                                  , definition
                                  , pg_catalog.obj_description((schemaname||'.'||viewname)::regclass::oid)
                               FROM pg_views
                              WHERE schemaname = :schemaname """

        self.sql_fsvcinfo = """SELECT fs.srvname       AS fsvc_name
                                    , pg_catalog.pg_get_userbyid(fs.srvowner)   AS fsvc_owner
                                    , w.fdwname        AS wrapper
                                    , fs.srvoptions    AS fsvc_options
                                 FROM pg_foreign_server fs
                                 JOIN pg_foreign_data_wrapper w
                                   ON w.oid = fs.srvfdw """

        self.sql_ftbinfo = """SELECT ft.foreign_table_name
                                   , ft.foreign_table_schema
                                   , ft.foreign_server_name
                                   , fs.foreign_data_wrapper_name
                                FROM information_schema.foreign_tables ft
                                JOIN information_schema.foreign_servers fs
                                  ON fs.foreign_server_catalog = ft.foreign_server_catalog
                                 AND fs.foreign_server_name = ft.foreign_server_name """


    def get_metadata_tablespaces(self):
        tbs_infos = []
        for each_tbs in self.pgagent.query_all(self.sql_tbsinfo):
            if each_tbs is None:
                break
            logger.debug("Got tablespace: %s | location: %s | owner: %s" % (each_tbs[0], each_tbs[1], each_tbs[2]))

            tbs_info = TablespaceMetaData()
            tbs_info.tablespace_name = each_tbs[0] if each_tbs[0] is not None and each_tbs[0].strip() != '' else 'pg_default'
            tbs_info.tablespace_location = each_tbs[1]
            tbs_info.tablespace_owner = each_tbs[2]
            tbs_info.tablespace_comment = each_tbs[3]

            tbs_info.name = tbs_info.tablespace_name

            tbs_infos.append(tbs_info)
            self.metadata_bank.add_tablespace(tbs_info)

        return tbs_infos


    def get_metadata_database(self, databasename):
        if databasename is None or databasename == '':
            raise ValueError("Database name must not be empty!")

        databasename = databasename.strip().lower()

        rs = self.pgagent.query_one(self.sql_dbinfo,
                                    {"databasename": databasename})
        logger.debug("Got database name: %s | owner: %s | comment: %s | default tablespace: %s" % (rs[0], rs[1], rs[2], rs[6]))
        db_meta = DatabaseMetaData()
        db_meta.database_name = rs[0]
        db_meta.database_comment = rs[1]
        db_meta.database_owner = rs[2]
        db_meta.database_encoding = rs[3]
        db_meta.database_lc_collate = rs[4]
        db_meta.database_lc_ctype = rs[5]
        db_meta.database_default_data_tablespace = rs[6]
        db_meta.database_connection_limit = int(rs[7])

        db_meta.name = db_meta.database_name

        self.metadata_bank.set_database(db_meta)


    def list_schemas_in_database(self, databasename):
        schemas = []
        for each_schema in self.pgagent.query_all(self.sql_sminfo):
            if each_schema is None:
                break

            schema_meta = SchemaMetaData()
            schema_meta.schema_name = each_schema[0]
            schema_meta.schema_owner = each_schema[1]

            schema_meta.name = schema_meta.schema_name

            schemas.append(schema_meta.get_name())
            self.metadata_bank.add_schema(schema_meta)

        return schemas


    def list_tablenames_in_schema(self, schemaname):
        if schemaname is None or schemaname == '':
            raise ValueError("Schema name must not be empty!")

        tb_lst = []

        for each_tb in self.pgagent.query_all(self.sql_tblist, {"schemaname": schemaname}):
            if each_tb is None:
                break

            tb_lst.append(TableName(tablename=each_tb[0],
                                    schemaname=each_tb[1],
                                    owner=each_tb[2],
                                    tablespace=each_tb[3],
                                    comment=each_tb[4]))

        return tb_lst


    def get_metadata_table(self, schemaname, tablename):
        tb_meta = TableMetaData(table_name=tablename, table_schemaname=schemaname)
        fulltablename = "{}.{}".format(schemaname, tablename)
        logger.debug("Fetch metadata of table: %s" % fulltablename)

        # Basic table information
        tb_meta.table_schemaname = schemaname
        tb_meta.table_name = tablename

        tb_meta.name = tb_meta.table_name
        tb_meta.column_longest_length = 0

        for each_column in self.pgagent.query_all(self.sql_colinfo,
                                                  {"fulltablename": fulltablename,
                                                   "schemaname": schemaname,
                                                   "tablename": tablename}):
            if each_column is None:
                break
            logger.debug("Got column %s for table %s" % (each_column[0], tablename))

            col_meta = ColumnMetaData()
            col_meta.column_name = each_column[0]
            col_meta.column_index = int(each_column[1])
            col_meta.column_comment = each_column[2]
            col_meta.column_data_type = each_column[3]
            col_meta.column_length = each_column[4]
            col_meta.column_scale = each_column[5]
            col_meta.column_precision = each_column[6]
            col_meta.column_default_value = str(each_column[7]) if each_column[7] is not None else ''
            col_meta.column_auto_increment = each_column[8]
            col_meta.column_is_nullable = True if each_column[9].strip() == 'Y' else False
            col_meta.column_in_pk = each_column[10]
            col_meta.column_in_fk = each_column[11]

            col_meta.name = col_meta.column_name
            if len(col_meta.column_name) > tb_meta.column_longest_length:
                tb_meta.column_longest_length = len(col_meta.column_name)

            tb_meta.columns.append(col_meta)

        # Primary key
        for each_pk_col in self.pgagent.query_all(self.sql_pkinfo,
                                                  {"schemaname": schemaname,
                                                   "tablename": tablename}):
            if each_pk_col is None:
                break
            logger.debug("Got column %s in primary key %s for table %s" % (each_pk_col[1], each_pk_col[0], tablename))

            pk_meta = PKMetaData()
            pk_meta.pk_name = each_pk_col[0]
            pk_meta.pk_column = each_pk_col[1]
            pk_meta.pk_column_index = each_pk_col[2]
            pk_meta.pk_tablespace = each_pk_col[3]

            pk_meta.name = pk_meta.pk_name

            tb_meta.primary_key.append(pk_meta)

        # Unique key
        for each_uk_col in self.pgagent.query_all(self.sql_ukinfo,
                                                  {"schemaname": schemaname,
                                                   "tablename": tablename}):
            if each_uk_col is None:
                break
            logger.debug("Got column %s in unique key %s for table %s" % (each_uk_col[1], each_uk_col[0], tablename))

            uk_meta = UKMetaData()
            uk_meta.uk_name = each_uk_col[0]
            uk_meta.uk_column = each_uk_col[1]
            uk_meta.uk_column_index = each_uk_col[2]
            uk_meta.uk_tablespace = each_uk_col[3]

            uk_meta.name = uk_meta.uk_name

            tb_meta.unique_keys.append(uk_meta)

        # Check constraint
        for each_check in self.pgagent.query_all(self.sql_ckinfo,
                                                 {"schemaname": schemaname,
                                                  "tablename": tablename}):
            if each_check is None:
                break
            logger.debug("Got check [%s] for table %s: %s" % (each_check[0], tablename, each_check[1]))

            ck_meta = CheckMetaData()
            ck_meta.check_name = each_check[0]
            ck_meta.check_define = each_check[1]

            ck_meta.name = ck_meta.check_name

            tb_meta.checks.append(ck_meta)

        # Foreign key
        for each_fk in self.pgagent.query_all(self.sql_fkinfo,
                                              {"schemaname": schemaname,
                                               "tablename": tablename}):
            if each_fk is None:
                break
            logger.debug("Got column %s in foreign key %s for table %s" % (each_fk[1], each_fk[0], tablename))
            fk_meta = FKMetaData()
            fk_meta.fk_name = each_fk[0]
            fk_meta.fk_column = each_fk[1]
            fk_meta.fk_ref_tablename = each_fk[2]
            fk_meta.fk_ref_column = each_fk[3]

            fk_meta.name = fk_meta.fk_name

            tb_meta.foreign_keys.append(fk_meta)

        # Index
        for each_idx in self.pgagent.query_all(self.sql_indexinfo,
                                               {"schemaname": schemaname,
                                                "tablename": tablename}):
            if each_idx is None:
                break
            logger.debug("Got index %s for table %s" % (each_idx[0], tablename))

            idx_meta = IndexMetaData()
            idx_meta.index_name = each_idx[0]
            idx_meta.index_columns = each_idx[1]
            idx_meta.index_type = each_idx[2]
            idx_meta.index_tablespace = each_idx[3]
            idx_meta.index_define = each_idx[4]

            idx_meta.name = idx_meta.index_name

            tb_meta.indexes.append(idx_meta)

        return tb_meta


    def link_tables_to_database(self, schemaname):
        """Add all tables to the table list of database.

        :param schemaname: schema name in the target database.
        :type schemaname: str.
        """
        table_list = self.list_tablenames_in_schema(schemaname)
        for each_table in table_list:
            if each_table is None:
                break
            table_meta = self.get_metadata_table(schemaname, each_table.tablename)
            table_meta.table_owner = each_table.owner
            table_meta.table_tablespace = each_table.tablespace
            table_meta.table_comment = each_table.comment

            self.metadata_bank.add_table(table_meta)


    def list_views_in_schema(self, schemaname):
        for each_view in self.pgagent.query_all(self.sql_vwinfo, {"schemaname": schemaname}):
            if each_view is None:
                break
            logger.debug("Got view %s in schema %s" % (each_view[0], schemaname))

            vw_meta = ViewMetaData()
            vw_meta.view_name = each_view[0]
            vw_meta.view_schemaname = schemaname
            vw_meta.view_owner = each_view[1]
            vw_meta.view_define = each_view[2].replace('\n', ' ')
            vw_meta.view_comment = each_view[3]

            vw_meta.name = vw_meta.view_name

            self.metadata_bank.add_view(vw_meta)


    def get_metadata_view(self, schemaname, viewname):
        """Already get meta data for view in method list_views_in_schema."""
        pass


    def get_metadata_foreign_server(self):
        """Get meta data of foreign_server(s) in the database."""
        for each_fsvc in self.pgagent.query_all(self.sql_fsvcinfo):
            if each_fsvc is None:
                break
            logger.debug("Got foreign server %s" % each_fsvc[0])

            fsvc_meta = FServerMetaData()
            fsvc_meta.foreign_servername = each_fsvc[0]
            fsvc_meta.foreign_server_owner = each_fsvc[1]
            fsvc_meta.foreign_server_wrapper = each_fsvc[2]
            fsvc_meta.foreign_server_option = ', '.join(each_fsvc[3]) if each_fsvc[3] is not None and each_fsvc[3] != [] else ''

            fsvc_meta.name = fsvc_meta.foreign_servername

            self.metadata_bank.add_foreign_server(fsvc_meta)


    def get_metadata_foreign_table(self):
        """Get meta data of a foreign_table in the database."""
        for each_ftb in self.pgagent.query_all(self.sql_ftbinfo):
            if each_ftb is None:
                break
            logger.debug("Got foreign table %s" % each_ftb[0])

            ftb_meta = FTableMetaData()
            ftb_meta.foreign_tablename = each_ftb[0]
            ftb_meta.foreign_schemaname = each_ftb[1]
            ftb_meta.foreign_server = each_ftb[2]
            ftb_meta.foreign_data_wrapper = each_ftb[3]

            ftb_meta.name = ftb_meta.foreign_tablename

            self.metadata_bank.add_foreign_table(ftb_meta)

    def get_metadata_bank(self):
        return self.metadata_bank
