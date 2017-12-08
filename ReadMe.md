# database_schema_collect
------

database_schema_collect is a little tool to collect and manage metadata of tables in databases.

## Requirements
------
  - Only tested with Python 2.7.
  - Only tested with PostgreSQL 9.6.
  - graphviz.

## Feature
------
  - Collect metadata of tables in PostgreSQL.
  - Collect metadata of tables in Hive.(Now only support metadata database in PostgreSQL and MySQL.)
  - Store metadata as json file in local file system or hdfs.
  - Show collected metadata in table like style.
  - Generate ERD from collected metadata.
  - Generate data dictionary from collected metadata.

## Examples
------
 
  config file example:
  ```
  [storage]
  storage_medium_type=hdfs
  storage_medium_location=http://NAMENODE_IP:
  
  [source]
  database_type=postgresql
  database_uri=postgresql://USERNAME:PASSWORD@DBHOST:PORT/DATABASENAME

  [log]
  log_level=info
  log_file_location=/tmp/dsc_run.log
  ```
