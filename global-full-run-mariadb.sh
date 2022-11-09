#!/bin/bash

./full-run-mariadb.sh epinions 1m execute
./full-run-mariadb.sh resourcestresser 1m execute
./full-run-mariadb.sh tatp 1m execute
./full-run-mariadb.sh seats 1m execute
./full-run-mariadb.sh tpcc 1m execute


./full-run-mariadb.sh epinions 10m execute
./full-run-mariadb.sh resourcestresser 10m execute
./full-run-mariadb.sh tatp 10m execute
./full-run-mariadb.sh seats 10m execute
./full-run-mariadb.sh tpcc 10m execute

./full-run-mariadb.sh epinions 100m execute
./full-run-mariadb.sh resourcestresser 100m execute
./full-run-mariadb.sh tatp 100m execute
./full-run-mariadb.sh seats 100m execute
./full-run-mariadb.sh tpcc 100m execute

