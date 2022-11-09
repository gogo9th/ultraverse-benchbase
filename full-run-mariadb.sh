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


function sync_table()
{
   local updated_table="$1"
   local primary_key="$2"
   local cluster_key_value_count=$3
   local is_string=$4
   echo "[CMD] $benchmark: Syncing ${updated_table} table"
	if [ $cluster_key_value_count -eq 0 ]; then
			echo "use benchbase; drop table if exists ${updated_table}_updated; create table ${updated_table}_updated like ${updated_table}; insert into ${updated_table}_updated select ${updated_table}_done.* from  ${updated_table} join ${updated_table}_done on ${updated_table}.$primary_key = ${updated_table}_done.$primary_key;" | mysql -uroot -p123456
	else
		for (( i=$cluster_key_value_count; i > 0; i-- )); do
			cluster_key_value="$(echo "use benchbase; select $primary_key from $updated_table limit 1;" | mysql -uroot -p123456 | sed 1d)"
			#echo "$cluster_key_value"
			if [ $is_string -eq 1 ]; then
				cluster_key_value="'$cluster_key_value'"
			fi
			echo "use benchbase; drop table if exists ${updated_table}_updated; create table ${updated_table}_updated like ${updated_table}; insert into ${updated_table}_updated select ${updated_table}_done.* from  ${updated_table} join ${updated_table}_done on ${updated_table}.$primary_key = $cluster_key_value and  ${updated_table}.$primary_key = ${updated_table}_done.$primary_key;" | mysql -uroot -p123456
		done
	fi
}

function sync_table_two_primary()
{
   local updated_table="$1"
   local primary_key="$2"
   local primary_key2="$3"
   local cluster_key_value_count=$4
   local is_string=$5
   echo "[CMD] $benchmark: Syncing ${updated_table} table"
	if [ $cluster_key_value_count -eq 0 ]; then
      echo "use benchbase; drop table if exists ${updated_table}_updated; create table ${updated_table}_updated like ${updated_table}; insert into ${updated_table}_updated select ${updated_table}_done.* from  ${updated_table} join ${updated_table}_done on ${updated_table}.$primary_key = ${updated_table}_done.$primary_key and ${updated_table}.$primary_key2 = ${updated_table}_done.$primary_key2;" | mysql -uroot -p123456
	else
		for (( i=$cluster_key_value_count; i > 0; i-- )); do
			cluster_key_value="$(echo "use benchbase; select $primary_key from $updated_table limit 1;" | mysql -uroot -p123456 | sed 1d)"
			#echo "$cluster_key_value"
			if [ $is_string -eq 1 ]; then
				cluster_key_value="'$cluster_key_value'"
			fi
			echo "use benchbase; drop table if exists ${updated_table}_updated; create table ${updated_table}_updated like ${updated_table}; insert into ${updated_table}_updated select ${updated_table}_done.* from  ${updated_table} join ${updated_table}_done on  ${updated_table}.$primary_key = $cluster_key_value and ${updated_table}.$primary_key = ${updated_table}_done.$primary_key and ${updated_table}.$primary_key2 = ${updated_table}_done.$primary_key2;" | mysql -uroot -p123456
		done
	fi
}

function sync_table_three_primary()
{
   local updated_table="$1"
   local primary_key="$2"
   local primary_key2="$3"
   local primary_key3="$4"
   local cluster_key_value_count=$5
   local is_string=$6
   echo "[CMD] $benchmark: Syncing ${updated_table} table"
	if [ $cluster_key_value_count -eq 0 ]; then
			echo "use benchbase; drop table if exists ${updated_table}_updated; create table ${updated_table}_updated like ${updated_table}; insert into ${updated_table}_updated select ${updated_table}_done.* from  ${updated_table} join ${updated_table}_done on ${updated_table}.$primary_key = ${updated_table}_done.$primary_key and ${updated_table}.$primary_key2 = ${updated_table}_done.$primary_key2 and ${updated_table}.$primary_key3 = ${updated_table}_done.$primary_key3;" | mysql -uroot -p123456
	else
		for (( i=$cluster_key_value_count; i > 0; i-- )); do
			cluster_key_value="$(echo "use benchbase; select $primary_key from $updated_table limit 1;" | mysql -uroot -p123456 | sed 1d)"
			#echo "$cluster_key_value"
			if [ $is_string -eq 1 ]; then
				cluster_key_value="'$cluster_key_value'"
			fi
			echo "use benchbase; drop table if exists ${updated_table}_updated; create table ${updated_table}_updated like ${updated_table}; insert into ${updated_table}_updated select ${updated_table}_done.* from  ${updated_table} join ${updated_table}_done on ${updated_table}.$primary_key = $cluster_key_value and ${updated_table}.$primary_key = ${updated_table}_done.$primary_key and ${updated_table}.$primary_key2 = ${updated_table}_done.$primary_key2 and ${updated_table}.$primary_key3 = ${updated_table}_done.$primary_key3;" | mysql -uroot -p123456
		done
	fi
}

function sync_table_four_primary()
{
   local updated_table="$1"
   local primary_key="$2"
   local primary_key2="$3"
   local primary_key3="$4"
   local primary_key4="$5"
   local cluster_key_value_count=$6
   local is_string=$7
   echo "[CMD] $benchmark: Syncing ${updated_table} table"
	if [ $cluster_key_value_count -eq 0 ]; then
		echo "use benchbase; drop table if exists ${updated_table}_updated; create table ${updated_table}_updated like ${updated_table}; insert into ${updated_table}_updated select ${updated_table}_done.* from  ${updated_table} join ${updated_table}_done on ${updated_table}.$primary_key = ${updated_table}_done.$primary_key and ${updated_table}.$primary_key2 = ${updated_table}_done.$primary_key2 and ${updated_table}.$primary_key3 = ${updated_table}_done.$primary_key3  and ${updated_table}.$primary_key4 = ${updated_table}_done.$primary_key4;" | mysql -uroot -p123456
	else
		for (( i=$cluster_key_value_count; i > 0; i-- )); do
			cluster_key_value="$(echo "use benchbase; select $primary_key from $updated_table limit 1;" | mysql -uroot -p123456 | sed 1d)"
			#echo "$cluster_key_value"
			if [ $is_string -eq 1 ]; then
				cluster_key_value="'$cluster_key_value'"
			fi
			echo "use benchbase; drop table if exists ${updated_table}_updated; create table ${updated_table}_updated like ${updated_table}; insert into ${updated_table}_updated select ${updated_table}_done.* from  ${updated_table} join ${updated_table}_done on ${updated_table}.$primary_key = $cluster_key_value and ${updated_table}.$primary_key = ${updated_table}_done.$primary_key and ${updated_table}.$primary_key2 = ${updated_table}_done.$primary_key2 and ${updated_table}.$primary_key3 = ${updated_table}_done.$primary_key3  and ${updated_table}.$primary_key4 = ${updated_table}_done.$primary_key4;" | mysql -uroot -p123456
		done
	fi
}



function instantiate_table()
{
	echo "[CMD] $benchmark: Instantiating tables"
	for table in "${tables[@]}"; do
      echo "use benchbase; drop table if exists ${table}; create table ${table} like ${table}_initial; insert into ${table} select * from ${table}_initial;" 
		echo "use benchbase; drop table if exists ${table}; create table ${table} like ${table}_initial; insert into ${table} select * from ${table}_initial;" | mysql -uroot -p123456
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
		echo "use benchbase; drop table if exists  ${table}_initial, ${table}_done;" | mysql -uroot -p123456
	done
	for table in "${tables[@]}"; do
		echo "use benchbase; rename table ${table} to ${table}_initial;" | mysql -uroot -p123456
	done
   echo "[CMD] $benchmark: Snapshoting initial tables Done ($(($SECONDS - $START_TIME)) seconds)"
fi

instantiate_table
START_TIME=$SECONDS
echo "[CMD] $benchmark: Replaying MariaDB"
./run-mariadb $benchmark mariadb $size execute
echo "[CMD] $benchmark: Replaying MariaDB Done ($(($SECONDS - $START_TIME)) seconds)"

START_TIME=$SECONDS
echo "[CMD] $benchmark: Snapshoting '_done' tables"
for table in "${tables[@]}"; do
	echo "use benchbase; drop table if exists ${table}_done; rename table ${table} to ${table}_done;" | mysql -uroot -p123456
done
echo "[CMD] $benchmark: Snapshoting '_done' tables Done ($(($SECONDS - $START_TIME)) seconds)"


instantiate_table
START_TIME=$SECONDS
echo "[CMD] $benchmark: Replaying StateDB1"
./run-mariadb $benchmark statedb1 $size execute
echo "[CMD] $benchmark: Replaying StateDB1 Done ($(($SECONDS - $START_TIME)) seconds)"
   
START_TIME=$SECONDS
if [ "$benchmark" == "epinions" ]; then
   sync_table "item2" "i_id" 1 0
elif [ "$benchmark" == "resourcestresser" ]; then
   sync_table "iotablesmallrow" "empid" 50 0
elif [ "$benchmark" == "tatp" ]; then
   sync_table "subscriber" "s_id" 1 0
elif [ "$benchmark" == "seats" ]; then
	sync_table "customer2" "c_id" 5 1
	sync_table "flight" "f_id" 5 1
	sync_table_three_primary "reservation" "r_id" "r_c_id" "r_f_id" 5 1
	sync_table_two_primary "frequent_flyer" "ff_c_id" "ff_al_id" 5 1
elif [ "$benchmark" == "tpcc" ]; then
   sync_table "warehouse" "w_id" 1 0
   sync_table_two_primary "stock" "s_w_id" "s_i_id" 1 0
   sync_table_two_primary "district" "d_w_id" "d_id" 1 0
   sync_table_three_primary "customer" "c_w_id" "c_d_id" "c_id" 1 0
   sync_table_four_primary "history" "h_c_id" "h_c_d_id" "h_c_w_id" "h_w_id" 1 0
   sync_table_three_primary "oorder" "o_w_id" "o_d_id" "o_id" 1 0
   sync_table_three_primary "new_order" "no_w_id" "no_d_id" "no_o_id" 1 0
   sync_table_four_primary "order_line" "ol_w_id" "ol_d_id" "ol_o_id" "ol_number" 1 0
fi
echo "[CMD] $benchmark: Syncing StateDB1 Done ($(($SECONDS - $START_TIME)) seconds)"

if [ "$size" == "10sf" ] ||  [ "$size" == "100sf" ]; then
   exit
fi


# State DB 2
instantiate_table
START_TIME=$SECONDS
echo "[CMD] $benchmark: Replaying StateDB2"
./run-mariadb $benchmark statedb2 $size execute
echo "[CMD] $benchmark: Replaying StateDB2 Done ($(($SECONDS - $START_TIME)) seconds)"
   
START_TIME=$SECONDS
if [ "$benchmark" == "epinions" ]; then
   sync_table "review" "u_id" 1 0
elif [ "$benchmark" == "resourcestresser" ]; then
   sync_table "iotable" "empid" 50 0
elif [ "$benchmark" == "tatp" ]; then
   sync_table_three_primary "call_forwarding" "s_id" "sf_type" "start_time" 1 0
elif [ "$benchmark" == "seats" ]; then
	sync_table "customer2" "c_id" 5 1
	sync_table "flight" "f_id" 5 1
	sync_table_three_primary "reservation" "r_id" "r_c_id" "r_f_id" 5 1
	sync_table_two_primary "frequent_flyer" "ff_c_id" "ff_al_id" 5 1
elif [ "$benchmark" == "tpcc" ]; then
   sync_table "warehouse" "w_id" 1 0
   sync_table_two_primary "stock" "s_w_id" "s_i_id" 1 0
   sync_table_two_primary "district" "d_w_id" "d_id" 1 0
   sync_table_three_primary "customer" "c_w_id" "c_d_id" "c_id" 1 0
   sync_table_four_primary "history" "h_w_id" "h_c_d_id" "h_c_w_id" "h_c_id" 1 0
   sync_table_three_primary "oorder" "o_w_id" "o_d_id" "o_id" 1 0
   sync_table_three_primary "new_order" "no_w_id" "no_d_id" "no_o_id" 1 0
   sync_table_four_primary "order_line" "ol_w_id" "ol_d_id" "ol_o_id" "ol_number" 1 0
fi
echo "[CMD] $benchmark: Syncing StateDB2 Done ($(($SECONDS - $START_TIME)) seconds)"



