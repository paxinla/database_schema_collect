#!/usr/bin/env python
# coding=utf-8

from __future__ import absolute_import

import sys
import os
import shutil
import logging
import traceback

import simplejson as json
import cPickle as pickle
from marshmallow import Schema, fields, post_load

from database_schema_collect.util import BANK_NAME_PATTERN

reload(sys)
sys.setdefaultencoding("utf-8")


logger = logging.getLogger("database_schema_collect")


def convert_field_type(obj_field, init_class):
    """Convert deserialized field from dict to init_class object.

    :param obj_field: Target field.
    :type obj_field: list.
    :param init_class: Target class.
    :type init_class: Subclass of marshmallow.Schema.
    :returns: Modified obj_field.
    """
    if isinstance(obj_field, list):
        tmp = []
        for i in obj_field:
            tmp.append(init_class(**i))
        obj_field = tmp
    elif isinstance(obj_field, dict):
        obj_field = init_class(**obj_field)
    else:
        obj_field = None
    return obj_field


class MetaData(object):

    def __init__(self):
        self.name = None


    def get_name(self, jsonfile=False):
        """Return name of the object.

        :param jsonfile: if return json file name of this object.
        :type jsonfile: boolean.
        :returns: name of column.
        :rtype: str
        """
        if jsonfile:
            return "{}.json".format(self.name)
        else:
            return self.name


class ColumnMetaData(MetaData):
    """Container of meta data of a column in a table."""

    def __init__(self, name=None, column_name=None, column_index=None, column_data_type=None,
                 column_length=None, column_scale=None, column_precision=None,
                 column_comment=None, column_default_value=None,
                 column_auto_increment=None, column_is_nullable=None,
                 column_is_unique=None, column_in_pk=None, column_in_fk=None):
        self.column_name = column_name
        self.name = name
        self.column_index = column_index
        self.column_data_type = column_data_type
        self.column_length = column_length
        self.column_scale = column_scale
        self.column_precision = column_precision
        self.column_comment = column_comment
        self.column_default_value = column_default_value
        # For PostgreSQL, this should be the name of sequence.
        self.column_auto_increment = column_auto_increment
        self.column_is_nullable = column_is_nullable
        self.column_is_unique = column_is_unique
        # Format like "-> NameOfPrimaryKey"
        self.column_in_pk = column_in_pk
        # Format like "-> NameOfForeignKey"
        self.column_in_fk = column_in_fk


class ColumnMetaDataSchema(Schema):
    """Model of meta data of a column in a table."""

    column_name = fields.Str(required=True)
    name = fields.Str(allow_none=True)
    column_index = fields.Int(required=True)
    column_data_type = fields.Str(required=True)
    column_length = fields.Int(allow_none=True)
    column_scale = fields.Int(allow_none=True)
    column_precision = fields.Int(allow_none=True)
    column_comment = fields.Str(allow_none=True)
    column_default_value = fields.Str(allow_none=True)
    # For PostgreSQL, this should be the name of sequence.
    column_auto_increment = fields.Str(allow_none=True)
    column_is_nullable = fields.Bool(allow_none=True)
    column_is_unique = fields.Bool(allow_none=True)
    # Format like "-> NameOfPrimaryKey"
    column_in_pk = fields.Str(allow_none=True)
    # Format like "-> NameOfForeignKey"
    column_in_fk = fields.Str(allow_none=True)


class PKMetaData(MetaData):
    """Container of meta data of a primary key of a table."""

    def __init__(self, name=None, pk_name=None, pk_column=None, pk_column_index=None, pk_tablespace=None):
        self.pk_name = pk_name
        self.name = name
        self.pk_column = pk_column
        self.pk_column_index = pk_column_index
        self.pk_tablespace = pk_tablespace


class PKMetaDataSchema(Schema):
    """Model of meta data of a primary key of a table."""

    pk_name = fields.Str(required=True)
    name = fields.Str(allow_none=True)
    pk_column = fields.Str(required=True)
    pk_column_index = fields.Int(required=True)
    pk_tablespace = fields.Str(allow_none=True)


class FKMetaData(MetaData):
    """Container of meta data of a foreign key of a table."""

    def __init__(self, name=None, fk_name=None, fk_column=None, fk_ref_tablename=None, fk_ref_column=None):
        self.fk_name = fk_name
        self.name = name
        self.fk_column = fk_column
        self.fk_ref_tablename = fk_ref_tablename
        self.fk_ref_column = fk_ref_column


class FKMetaDataSchema(Schema):
    """Model of meta data of a foreign key of a table."""

    fk_name = fields.Str(required=True)
    name = fields.Str(allow_none=True)
    fk_column = fields.Str(required=True)
    fk_ref_tablename = fields.Str(required=True)
    fk_ref_column = fields.Str(required=True)


class UKMetaData(MetaData):
    """Container of meta data of a unique key of a table."""

    def __init__(self, name=None, uk_name=None, uk_column=None, uk_column_index=None, uk_tablespace=None):
        self.uk_name = uk_name
        self.name = name
        self.uk_column = uk_column
        self.uk_column_index = uk_column_index
        self.uk_tablespace = uk_tablespace


class UKMetaDataSchema(Schema):
    """Model of meta data of a unique key of a table."""

    uk_name = fields.Str(required=True)
    name = fields.Str(allow_none=True)
    uk_column = fields.Str(required=True)
    uk_column_index = fields.Int(required=True)
    uk_tablespace = fields.Str(allow_none=True)


class CheckMetaData(MetaData):
    """Container of meta data of a check constraint of a table."""

    def __init__(self, name=None, check_name=None, check_define=None):
        self.check_name = check_name
        self.name = name
        self.check_define = check_define


class CheckMetaDataSchema(Schema):
    """Model of meta data of a check constraint of a table."""

    check_name = fields.Str(required=True)
    name = fields.Str(allow_none=True)
    check_define = fields.Str(allow_none=True)


class IndexMetaData(MetaData):
    """Container of meta data of an index of a table."""

    def __init__(self, name=None, index_name=None, index_columns=None, index_type=None,
                 index_tablespace=None, index_define=None):
        self.index_name = index_name
        self.name = name
        # Column names should be seperated by comma.
        self.index_columns = index_columns
        self.index_type = index_type
        self.index_tablespace = index_tablespace
        self.index_define = index_define


class IndexMetaDataSchema(Schema):
    """Model of meta data of an index of a table."""

    index_name = fields.Str(required=True)
    name = fields.Str(allow_none=True)
    # Column names should be seperated by comma.
    index_columns = fields.Str(required=True)
    index_type = fields.Str(allow_none=True)
    index_tablespace = fields.Str(allow_none=True)
    index_define = fields.Str(allow_none=True)


class TableMetaData(MetaData):
    """Container of meta data of a table."""

    def __init__(self, name=None, table_name=None, table_schemaname=None, columns=None,
                 column_longest_length=None,table_comment=None, table_tablespace=None, table_owner=None,
                 primary_key=None, foreign_keys=None, unique_keys=None,
                 indexes=None, checks=None):
        self.table_name = table_name
        self.name = name
        self.table_schemaname = table_schemaname
        self.columns = [] if columns is None else columns
        self.column_longest_length = column_longest_length
        self.table_comment = table_comment
        self.table_tablespace = table_tablespace
        self.table_owner = table_owner
        self.primary_key = [] if primary_key is None else primary_key
        self.foreign_keys = [] if foreign_keys is None else foreign_keys
        self.unique_keys = [] if unique_keys is None else unique_keys
        self.indexes = [] if indexes is None else indexes
        self.checks = [] if checks is None else checks


class TableMetaDataSchema(Schema):
    """Model of meta data of a table."""

    table_name = fields.Str(required=True)
    name = fields.Str(allow_none=True)
    table_schemaname = fields.Str(required=True)
    columns = fields.Nested("ColumnMetaDataSchema", many=True)
    column_longest_length = fields.Int(allow_none=True)
    table_comment = fields.Str(allow_none=True)
    table_tablespace = fields.Str(allow_none=True)
    table_owner = fields.Str(allow_none=True)
    primary_key = fields.Nested("PKMetaDataSchema", many=True, allow_none=True)
    foreign_keys = fields.Nested("FKMetaDataSchema", many=True, allow_none=True)
    unique_keys = fields.Nested("UKMetaDataSchema", many=True, allow_none=True)
    indexes = fields.Nested("IndexMetaDataSchema", many=True, allow_none=True)
    checks = fields.Nested("CheckMetaDataSchema", many=True, allow_none=True)

    def make_table(self, data):
        """For deserialize table metadata object."""
        try:
            table_obj = TableMetaData(**data)

            table_obj.columns = convert_field_type(table_obj.columns, ColumnMetaData)
            table_obj.primary_key = convert_field_type(table_obj.primary_key, PKMetaData)
            table_obj.foreign_keys = convert_field_type(table_obj.foreign_keys, FKMetaData)
            table_obj.unique_keys = convert_field_type(table_obj.unique_keys, UKMetaData)
            table_obj.indexes = convert_field_type(table_obj.indexes, IndexMetaData)
            table_obj.checks = convert_field_type(table_obj.checks, CheckMetaData)
        except:
            logger.debug(traceback.format_exc())

        return table_obj


class ViewMetaData(MetaData):
    """Container of meta data of a view."""

    def __init__(self, name=None, view_name=None, view_schemaname=None, view_owner=None, view_define=None, view_comment=None):
        self.view_name = view_name
        self.name = name
        self.view_schemaname = view_schemaname
        self.view_owner = view_owner
        self.view_define = view_define
        self.view_comment = view_comment


class ViewMetaDataSchema(Schema):
    """Model of meta data of a view."""

    view_name = fields.Str(required=True)
    name = fields.Str(allow_none=True)
    view_schemaname = fields.Str(required=True)
    view_owner = fields.Str(allow_none=True)
    view_define = fields.Str(allow_none=True)
    view_comment = fields.Str(allow_none=True)


class SchemaMetaData(MetaData):
    """Container of meta data of a schema."""

    def __init__(self, name=None, schema_name=None, schema_owner=None):
        self.schema_name = schema_name
        self.name = name
        self.schema_owner = schema_owner


class SchemaMetaDataSchema(Schema):
    """Model of meta data of a schema."""

    schema_name = fields.Str(required=True)
    name = fields.Str(allow_none=True)
    schema_owner = fields.Str(allow_none=True)


class TablespaceMetaData(MetaData):
    """Container of meta data of a tablespace of a table."""

    def __init__(self, name=None, tablespace_name=None, tablespace_location=None,
                 tablespace_owner=None, tablespace_comment=None):
        self.tablespace_name = tablespace_name
        self.name = name
        self.tablespace_location = tablespace_location
        self.tablespace_owner = tablespace_owner
        self.tablespace_comment = tablespace_comment


class TablespaceMetaDataSchema(Schema):
    """Model of meta data of a tablespace of a table."""

    tablespace_name = fields.Str(required=True)
    name = fields.Str(allow_none=True)
    tablespace_location = fields.Str(allow_none=True)
    tablespace_owner = fields.Str(allow_none=True)
    tablespace_comment = fields.Str(allow_none=True)


class FServerMetaData(MetaData):
    """Container of meta data of a foreign server."""

    def __init__(self, name=None, foreign_servername=None, foreign_server_owner=None,
                 foreign_server_wrapper=None, foreign_server_option=None):
        self.foreign_servername = foreign_servername
        self.name = name
        self.foreign_server_owner = foreign_server_owner
        self.foreign_server_wrapper = foreign_server_wrapper
        self.foreign_server_option = foreign_server_option


class FServerMetaDataSchema(Schema):
    """Model of meta data of a foreign server."""

    foreign_servername = fields.Str(required=True)
    name = fields.Str(allow_none=True)
    foreign_server_owner = fields.Str(allow_none=True)
    foreign_server_wrapper = fields.Str(allow_none=True)
    foreign_server_option = fields.Str(allow_none=True)


class FTableMetaData(MetaData):
    """Container of meta data of a foreign table."""

    def __init__(self, name=None, foreign_tablename=None, foreign_schemaname=None,
                 foreign_server=None, foreign_data_wrapper=None):
        self.foreign_tablename = foreign_tablename
        self.name = name
        self.foreign_schemaname = foreign_schemaname
        self.foreign_server = foreign_server
        self.foreign_data_wrapper = foreign_data_wrapper


class FTableMetaDataSchema(Schema):
    """Model of meta data of a foreign table."""

    foreign_tablename = fields.Str(required=True)
    name = fields.Str(allow_none=True)
    foreign_schemaname = fields.Str(required=True)
    foreign_server = fields.Str(required=True)
    foreign_data_wrapper = fields.Str(required=True)


class DatabaseMetaData(MetaData):
    """Container of meta data of a database."""

    def __init__(self, name=None, database_name=None, database_encoding=None, database_comment=None,
                 database_owner=None, database_lc_collate=None,
                 database_lc_ctype=None, database_default_data_tablespace=None,
                 database_default_temp_tablespace=None,
                 database_connection_limit=-1):
        self.database_name = database_name
        self.name = name
        self.database_encoding = database_encoding
        self.database_comment = database_comment
        self.database_owner = database_owner
        self.database_lc_collate = database_lc_collate
        self.database_lc_ctype = database_lc_ctype
        self.database_default_data_tablespace = database_default_data_tablespace
        self.database_default_temp_tablespace = database_default_temp_tablespace
        self.database_connection_limit = database_connection_limit


class DatabaseMetaDataSchema(Schema):
    """Model of meta data of a database."""

    database_name = fields.Str(required=True)
    name = fields.Str(allow_none=True)
    database_encoding = fields.Str(required=True)
    database_comment = fields.Str(allow_none=True)
    database_owner = fields.Str(allow_none=True)
    database_lc_collate = fields.Str(allow_none=True)
    database_lc_ctype = fields.Str(allow_none=True)
    database_default_data_tablespace = fields.Str(allow_none=True)
    database_default_temp_tablespace = fields.Str(allow_none=True)
    database_connection_limit = fields.Int(allow_none=True)


class DatabaseMetaDataBank(object):
    """Container of meta data of a meta data bank."""

    def __init__(self, database=None, tablespaces=None, schemas=None,
                 tables=None, views=None, foreign_servers=None,
                 foreign_tables=None):
        self.database = database
        self.tablespaces = [] if tablespaces is None else tablespaces
        self.schemas = [] if schemas is None else schemas
        self.tables = [] if tables is None else tables
        self.views = [] if views is None else views
        self.foreign_servers = [] if foreign_servers is None else foreign_servers
        self.foreign_tables = [] if foreign_tables is None else foreign_tables


class DatabaseMetaDataBankSchema(Schema):
    """Model of meta data of a meta data bank."""

    database = fields.Nested("DatabaseMetaDataSchema")
    tablespaces = fields.Nested("TablespaceMetaDataSchema", many=True, allow_none=True)
    schemas = fields.Nested("SchemaMetaDataSchema", many=True)
    tables = fields.Nested("TableMetaDataSchema", many=True)
    views = fields.Nested("ViewMetaDataSchema", many=True, allow_none=True)
    foreign_servers = fields.Nested("FServerMetaDataSchema", many=True, allow_none=True)
    foreign_tables = fields.Nested("FTableMetaDataSchema", many=True, allow_none=True)


    @post_load
    def make_bank(self, data):
        """Deserialized database meta data bank object.

        :param data: Deserialized data.
        :type data: dict.
        """
        try:
            bank_obj = DatabaseMetaDataBank(**data)

            bank_obj.database = convert_field_type(bank_obj.database, DatabaseMetaData)
            bank_obj.tablespaces = convert_field_type(bank_obj.tablespaces, TablespaceMetaData)
            bank_obj.schemas = convert_field_type(bank_obj.schemas, SchemaMetaData)
            bank_obj.views = convert_field_type(bank_obj.views, ViewMetaData)
            bank_obj.foreign_servers = convert_field_type(bank_obj.foreign_servers, FServerMetaData)
            bank_obj.foreign_tables = convert_field_type(bank_obj.foreign_tables, FTableMetaData)

            tmp = []
            for t in bank_obj.tables:
                tmp.append(TableMetaDataSchema().make_table(t))
            bank_obj.tables = tmp
        except:
            logger.error(traceback.format_exc())

        return bank_obj


SCHEMA_MAP = {ColumnMetaData: ColumnMetaDataSchema,
              PKMetaData: PKMetaDataSchema,
              FKMetaData: FKMetaDataSchema,
              UKMetaData: UKMetaDataSchema,
              CheckMetaData: CheckMetaDataSchema,
              IndexMetaData: IndexMetaDataSchema,
              TableMetaData: TableMetaDataSchema,
              ViewMetaData: ViewMetaDataSchema,
              SchemaMetaData: SchemaMetaDataSchema,
              TablespaceMetaData: TablespaceMetaDataSchema,
              FServerMetaData: FServerMetaDataSchema,
              FTableMetaData: FTableMetaDataSchema,
              DatabaseMetaData: DatabaseMetaDataSchema,
              DatabaseMetaDataBank: DatabaseMetaDataBankSchema}


def to_struct(metadata_obj):
    for k, v in SCHEMA_MAP.items():
        if isinstance(metadata_obj, k):
            return v().dump(metadata_obj).data
    return None


class MetaDataBank(object):
    """Container holds meta data of all objects in a database."""

    def __init__(self, temp_dir):
        self._db_metadatas = DatabaseMetaDataBank()
        self.temp_dir = temp_dir


    def set_database(self, db_metadata):
        """Set database to the bank.

        :param db_metadata: Database metadata object.
        :type db_metadata: An instance of DatabaseMetaData.
        :raises: TypeError.
        """
        if isinstance(db_metadata, DatabaseMetaData):
            self._db_metadatas.database = db_metadata
            self.temp_dir = os.path.join(self.temp_dir, db_metadata.get_name())
        else:
            raise TypeError("Wrong type of database metadata, expect DatabaseMetaData, Got {}".format(type(db_metadata)))


    def add_tablespace(self, tbs_metadata):
        """Add tablespace in the database to the bank.

        :param tbs_metadata: Tablespace metadata object.
        :type tbs_metadata: An instance of TablespaceMetaData.
        :raises: TypeError.
        """
        if isinstance(tbs_metadata, TablespaceMetaData):
            self._db_metadatas.tablespaces.append(tbs_metadata)
        else:
            raise TypeError("Wrong type of tablespace metadata, expect TablespaceMetaData, Got {}".format(type(tbs_metadata)))


    def add_schema(self, sm_metadata):
        """Add schema in the database to the bank.

        :param sm_metadata: Schema metadata object.
        :type sm_metadata: An instance of SchemaMetaData.
        :raises: TypeError.
        """
        if isinstance(sm_metadata, SchemaMetaData):
            self._db_metadatas.schemas.append(sm_metadata)
        else:
            raise TypeError("Wrong type of schema metadata, expect SchemaMetaData, Got {}".format(type(sm_metadata)))


    def add_table(self, tb_metadata):
        """Add table in the database to the bank.

        :param tb_metadata: Table metadata object.
        :type tb_metadata: An instance of TableMetaData.
        :raises: TypeError.
        """
        if isinstance(tb_metadata, TableMetaData):
            self._db_metadatas.tables.append(tb_metadata)
        else:
            raise TypeError("Wrong type of table metadata, expect TableMetaData, Got {}".format(type(tb_metadata)))


    def add_view(self, vw_metadata):
        """Add view in the database to the bank.

        :param vw_metadata: View metadata object.
        :type vw_metadata: An instance of ViewMetaData.
        :raises: TypeError.
        """
        if isinstance(vw_metadata, ViewMetaData):
            self._db_metadatas.views.append(vw_metadata)
        else:
            raise TypeError("Wrong type of view metadata, expect ViewMetaData, Got {}".format(type(vw_metadata)))


    def add_foreign_server(self, fsvc_metadata):
        """Add foreign server in the database to the bank.

        :param fsvc_metadata: Foreign server metadata object.
        :type fsvc_metadata: An instance of FServerMetaData.
        :raises: TypeError.
        """
        if isinstance(fsvc_metadata, FServerMetaData):
            self._db_metadatas.foreign_servers.append(fsvc_metadata)
        else:
            raise TypeError("Wrong type of foreign server metadata, expect FServerMetaData, Got {}".format(type(fsvc_metadata)))


    def add_foreign_table(self, ftb_metadata):
        """Add foreign table in the database to the bank.

        :param ftb_metadata: Foreign table metadata object.
        :type ftb_metadata: An instance of FTableMetaData.
        :raises: TypeError.
        """
        if isinstance(ftb_metadata, FTableMetaData):
            self._db_metadatas.foreign_tables.append(ftb_metadata)
        else:
            raise TypeError("Wrong type of foreign table metadata, expect FTableMetaData, Got {}".format(type(ftb_metadata)))


    def iter_and_save_metadata(self, des_loc, des_root_dir):
        """Iter metadata objects in metabank and store them into store location.

        :param des_loc: Store media of the des_path.
        :type des_loc: An instance of Location.
        :param des_root_dir: Store root directory.
        :type des_root_dir: str.
        """
        db_name = self._db_metadatas.database.get_name()
        logger.debug("Use target root directory: %s" % des_root_dir)
        logger.debug("Use temporary stage directory: %s" % self.temp_dir)

        topdir = os.path.join(self.temp_dir, db_name)
        logger.debug("Use temporary database directory: %s" % topdir)
        if os.path.isdir(topdir):
            shutil.rmtree(topdir)
        os.makedirs(topdir)

        # database its self meta data
        self.save_metadata_to_location(des_loc, self._db_metadatas.database, topdir)

        # tablespaces in the database.
        for each_tbs in self._db_metadatas.tablespaces:
            self.save_metadata_to_location(des_loc, each_tbs, topdir)

        # foreign servers in the database.
        for each_fsvc in self._db_metadatas.foreign_servers:
            self.save_metadata_to_location(des_loc, each_fsvc, topdir)

        # schemas in the database.
        for each_schema in self._db_metadatas.schemas:
            each_schema_name = each_schema.get_name()
            each_schema_dir = os.path.join(topdir, each_schema_name)
            os.makedirs(each_schema_dir)

            # tables belong to this schema.
            for each_table in (t for t in self._db_metadatas.tables if t.table_schemaname == each_schema_name):
                self.save_metadata_to_location(des_loc, each_table, each_schema_dir)

            # views belong to this schema.
            for each_view in (v for v in self._db_metadatas.views if v.view_schemaname == each_schema_name):
                self.save_metadata_to_location(des_loc, each_view, each_schema_dir)

            # foreign tables belong to this schema
            for each_ftables in (f for f in self._db_metadatas.foreign_tables if f.foreign_schemaname == each_schema_name):
                self.save_metadata_to_location(des_loc, each_ftables, each_schema_dir)

        # this meta data bank
        this_meta_bank = os.path.join(topdir, BANK_NAME_PATTERN.format(db_name))
        logger.debug("Save this meta data bank to %s" % this_meta_bank)
        with open(this_meta_bank, "wb") as wf:
            pickle.dump(to_struct(self._db_metadatas), wf)

        try:
            des_db_dir = os.path.join(des_root_dir, db_name)
            logger.info("Finally move %s to %s" % (topdir, des_db_dir))
            des_loc.move_file_to(topdir, des_db_dir)
        except:
            logger.error(traceback.format_exc())
        finally:
            if os.path.isdir(topdir):
                shutil.rmtree(topdir)


    def save_metadata_to_location(self, des_loc, metadata_obj, des_path):
        """Save metadata as json file, and put it to the target location.

        :param des_loc: Store media of the des_path.
        :type des_loc: An instance of Location.
        :param metadata_obj: Metadata object.
        :type metadata_obj: An instance of *MetaData.
        :param des_path: Target location for saving the json files.
        :type des_path: str.
        """
        objname = metadata_obj.get_name(True)
        tempname = os.path.join(self.temp_dir, objname)
        finalname = os.path.join(des_path, objname)

        logger.debug("Save %s to %s ." % (objname, tempname))
        try:
            with open(tempname, "wb") as wf:
                json.dump(to_struct(metadata_obj), wf)

            logger.debug("Move %s to %s ." % (tempname, finalname))
            des_loc.move_file_to(tempname, finalname)
        except:
            logger.error(traceback.format_exc())
        finally:
            if os.path.exists(tempname):
                os.remove(tempname)
