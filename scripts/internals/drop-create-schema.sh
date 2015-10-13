#!/bin/bash
# Author: Salman Niazi 2015
# Run all the damn benchmarks

#load config parameters
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $DIR/deployment.properties
temp_file=/tmp/connection.properties
connection_file=$DIR/hop_conf/hdfs_configs/hops-ndb-config.properties

cat hop_conf/hdfs_configs/hops-ndb-config.properties | awk -f read-prop.awk > $temp_file
source $temp_file


command="mysql -u$io_hops_metadata_ndb_mysqlserver_username -p$io_hops_metadata_ndb_mysqlserver_password -h$io_hops_metadata_ndb_mysqlserver_host -P$io_hops_metadata_ndb_mysqlserver_port "

echo "Attempting to drop the database"
$command -e "drop schema $com_mysql_clusterj_database"
echo "Attempting to create the database"
$command -e "create database   $com_mysql_clusterj_database"
echo "Attempting to import the database"
$command  $com_mysql_clusterj_database < $DIR/../../hops-metadata-dal-impl-ndb/schema/schema.sql 
echo "Schema dropped and recreated."


#create an additional table for BR Benchmarks
if [ $BenchMark = "BR" ]; then
 echo "Attempting to drop and create table for Block Reporting"
 $command  $com_mysql_clusterj_database < $DIR/br.sql
 echo "Truncate and create table Finished"

 sed -i 's|br.persist.database=.*|br.persist.database='$io_hops_metadata_ndb_mysqlserver_host:$io_hops_metadata_ndb_mysqlserver_port:$com_mysql_clusterj_database'|g'   $DIR/../master.properties
fi
