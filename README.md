## BenchBase


### MySQL Initial setup

Add a user to MySQL: ID admin, PASSWORD password
```bash
sudo mysql
> CREATE USER 'admin'@'localhost' IDENTIFIED BY 'password';
> GRANT ALL PRIVILEGES ON *.* TO 'admin'@'localhost';
```


### BenchBase Installation

- Reference: https://github.com/cmu-db/benchbase (The actual installation should be this repository's modified one)

```bash
$ sudo apt install openjdk-17-jdk maven  # must install Java v17 or higher
$ cd ultraverse-benchbase
$ ./make-mariadb # compilation. Must run it whenever editting the Java source code

## BenchBase Execution

  # Usage: ./forward-play-mariadb.sh <epinions|resourcestresser|tatp|seats|tpcc> <1m|10m|100m|1b> (execute)

  # epinions tables are initialized and 1 million transactions get executed
$ ./forward-play-mariadb.sh epinions 1m 

  # Run 1 milllion transaction queries by using the previously generated and initialized epinions tables
$ ./forward-play-mariadb.sh epinions 1m execute 

  # Run 10 milllion transaction queries by using the previously generated and initialized epinions tables
$ ./forward-play-mariadb.sh epinions 10m execute 

  # Run 100 milllion transaction queries by using the previously generated and initialized epinions tables
$ ./forward-play-mariadb.sh epinions 100m execute 

  # Run 1 billion transaction queries by using the previously generated and initialized epinions tables
$ ./forward-play-mariadb.sh epinions 1b execute 

   # The generated tables get stored in the tables named <TABLE NAME>_initial,   
  # and whenever executed, each <TABLE NAME>_iniital get cloned to <TABLE NAME> and executes
```

Once the execution is done, the general log file gets stored in the `/var/log/mysql/mylog` file
