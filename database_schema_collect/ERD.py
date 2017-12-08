#!/usr/bin/env python
# coding=utf-8

from __future__ import absolute_import

import sys
import os
import shutil
import logging
import traceback

from graphviz import Digraph

from database_schema_collect.util import format_pg_col_str

reload(sys)
sys.setdefaultencoding("utf-8")


logger = logging.getLogger("database_schema_collect")


class ERD(object):
    """Template of ERD elements."""


    def __init__(self):
        self.out_image_type = "png"
        self.graph_layout = "dot"


    def element_table(self, table_obj):
        """An entity Expression for a table.

        :param table_obj: Table information.
        :type table_obj: An instance of TableMetaData

        :returns tpl: Expression of an entity.
        :rtype tpl: str.
        """
        logger.debug("Define table %s" % table_obj.table_name)
        tpl = '<<table border="0" cellborder="1" cellspacing="0" cellpadding="4">'
        tpl += '<tr><td bgcolor="lightblue">{}</td></tr>\n'.format(table_obj.table_name)
        for each_column in table_obj.columns:
            column_name, column_type = format_pg_col_str(each_column)
            tpl += '<tr><td align="left">{}: {}</td></tr>\n'.format(column_name,
                                                                    column_type)
        tpl += '</table>>'

        return tpl


    def element_view(self, view_obj):
        """An entity Expression for a view.

        :param view_obj: View information.
        :type view_obj: An instance of ViewMetaData

        :returns tpl: Expression of an entity.
        :rtype tpl: str.
        """
        logger.debug("Define view %s" % view_obj.view_name)
        tpl = '<<table border="0" cellborder="1" cellspacing="0" cellpadding="4">'
        tpl += '<tr><td bgcolor="orange">{}</td></tr>\n'.format(view_obj.view_name)
        tpl += '</table>>'

        return tpl


    def element_schema(self, schema_name, gv_path, r_path, other_defines):
        """Scope of a schema.

        :param schema_name: Schema name.
        :type schema_name: str.
        :param gv_path: Directory for storing definition file of erd.
        :type gv_path: str.
        :param r_path: Directory for storing image file of erd.
        :type r_path: str.
        :param other_defines: List of expressoins.
        :type other_defines: list.

        :returns: Definition of tables and views in a schema in graphviz format.
        :rtype: An instance of Digraph.
        """
        logger.debug("Define schema %s" % schema_name)

        node_param = {"shape": "record",
                      "margin": "0"}
        edge_param = {"arrowhead": "crow",
                      "arrowtail": "none",
                      "dir": "both"}
        g = Digraph(schema_name,
                    filename=gv_path,
                    directory=r_path,
                    node_attr=node_param,
                    edge_attr=edge_param,
                    format=self.out_image_type,
                    engine=self.graph_layout)

        g.attr(lable=schema_name)
        g.attr(lablelloc="top")
        g.attr(style="filled")
        g.attr(color="lightgrey")
        g.attr(rankdir="LR")

        for each_elt in other_defines:
            elt_type, elt_obj = each_elt
            if elt_type == "table":
                g.node(elt_obj.table_name,
                        self.element_table(elt_obj))
            elif elt_type == "view":
                g.node(elt_obj.view_name,
                        self.element_view(elt_obj))
            else:
                from_name, end_name = elt_obj
                g.edge(from_name, end_name)

        return g


    def gen_erd(self, bank_obj, erd_path):
        """Generate ERD definition text and image files.

        :param bank_obj: The meta data container.
        :type bank_obj: dict.
        :param erd_path: Directory for storing erd definition text files and image files.
        :type erd_path: str.
        """
        schema_reg_tables = {}
        dbname = bank_obj.database.database_name

        if os.path.exists(erd_path):
            logger.debug("Destination [%s] exists, remove!" % erd_path)
            shutil.rmtree(erd_path)

        for each_tb in bank_obj.tables:
            schema_name = each_tb.table_schemaname
            schema_reg_tables.setdefault(schema_name, [])
            schema_reg_tables[schema_name].append(("table", each_tb))

            for each_fk in each_tb.foreign_keys:
                schema_reg_tables[schema_name].append(("fk",
                                                       (each_tb.table_name,each_fk.fk_ref_tablename)))

        for each_vw in bank_obj.views:
            schema_name = each_vw.view_schemaname
            schema_reg_tables.setdefault(schema_name, [])
            schema_reg_tables[schema_name].append(("view", each_vw))

        for smname, objs_in_sm in schema_reg_tables.items():
            gv_filepath = os.path.join(erd_path, "erd_{}_{}".format(dbname, smname))
            g = self.element_schema(smname, gv_filepath, erd_path, objs_in_sm)
            logger.debug("Save dot file [{fn}] and render png file [{fn}.{fs}]".format(fn=gv_filepath,
                                                                                       fs=self.out_image_type))
            try:
                g.render()
            except:
                logger.error(traceback.format_exc())
                logger.debug(g.source)
