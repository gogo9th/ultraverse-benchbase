#!/bin/bash



benchmarks=(
"epinions"
"resourcestresser"
"tatp"
"seats"
"tpcc"
)

epinions=(
"review"
"review_rating"
"trust"
"useracct"
"item2"
)

resourcestresser=(
"cputable"
"iotable"
"iotablesmallrow"
"locktable"
)

tatp=(
"access_info"
"call_forwarding"
"special_facility"
"subscriber"
)

seats=(
"config_profile"
"reservation"
"flight"
"frequent_flyer"
"airline"
"customer2"
"config_histograms"
"airport_distance"
"airport"
"country"
)

tpcc=(
"order_line"
"new_order"
"oorder"
"history"
"customer"
"stock"
"district"
"item"
"warehouse"
)




function instantiate_table()
{
	echo "[CMD] $benchmark: Instantiating tables"
	for table in "${tables[@]}"; do
      echo "use benchbase; drop table if exists ${table}; create table ${table} like ${table}_initial; insert into ${table} select * from ${table}_initial;" 
		echo "use benchbase; drop table if exists ${table}; create table ${table} like ${table}_initial; insert into ${table} select * from ${table}_initial;" | mysql -uadmin -ppassword
	done
}

if [ $# -ne 2 ] && [ $# -ne 3 ] ; then
   echo "Usage: $0 <epinions|resourcestresser|tatp|seats|tpcc> <1m|10m|100m|1b> (execute)"
   exit
fi

benchmark="$1"
size="$2"

ARR="$(eval echo \${${benchmark}[@]})"
tables=($ARR)


if [ "$3" != 'execute' ]; then

   START_TIME=$SECONDS
	echo "[CMD] $benchmark: Creating initial tables"
   echo "DROP DATABASE IF EXISTS benchbase_initial; create database benchbase_initial;" | mysql -uadmin -ppassword
	./run-mariadb $benchmark mariadb $size prepare
   echo "[CMD] $benchmark: Creating initial tables Done ($(($SECONDS - $START_TIME)) seconds)"

	#echo "[CMD] $benchmark: Cloning the database"
   #mysqldump -uroot -p123456 benchbase > dump.sql
   #mysql -uroot -p123456 benchbase_initial < dump.sql

   START_TIME=$SECONDS
	echo "[CMD] $benchmark: Snapshoting initial tables"
	for table in "${tables[@]}"; do
		echo "use benchbase; drop table if exists  ${table}_initial, ${table}_done;" | mysql -uadmin -ppassword
	done
	for table in "${tables[@]}"; do
		echo "use benchbase; rename table ${table} to ${table}_initial;" | mysql -uadmin -ppassword
	done
   echo "[CMD] $benchmark: Snapshoting initial tables Done ($(($SECONDS - $START_TIME)) seconds)"
fi

instantiate_table
START_TIME=$SECONDS
echo "[CMD] $benchmark: Playing MariaDB"
./run-mariadb $benchmark mariadb $size execute
echo "[CMD] $benchmark: Playing MariaDB Done ($(($SECONDS - $START_TIME)) seconds)"

 



