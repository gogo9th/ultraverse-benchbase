## BenchBase


### MySQL 초기 설정

MySQL에 유저 추가: 아이디 admin, 패스워드 123456
```bash
sudo mysql
> CREATE USER 'admin'@'localhost' IDENTIFIED BY 'password';
> GRANT ALL PRIVILEGES ON *.* TO 'admin'@'localhost';
```


### BenchBase 설치

- 참고: https://github.com/cmu-db/benchbase (오직 참고용. 실제 설치는 우리가 일부 변경한 이 Github 코드를 설치해야 함.)

```bash
$ sudo apt install openjdk-17-jdk maven  # must install Java v17 or higher
$ cd ultraverse-benchbase
$ ./make-mariadb # 컴파일. Java 소스코드를 수정할 때마다 실행해주어야 함.

## BenchBase 실행

  # 사용법: ./forward-play-mariadb.sh <epinions|resourcestresser|tatp|seats|tpcc> <1m|10m|100m|1b> (execute)

  # epinions 테이블들을 초기화 후 1 million 트랜젝션 쿼리들을 실행
$ ./forward-play-mariadb.sh epinions 1m 

  # 기존 생성한 epinions 초기 테이블들을 사용하여 1 million 트랜젝션 쿼리들을 실행
$ ./forward-play-mariadb.sh epinions 1m execute 

  # 기존 생성한 epinions 초기 테이블들을 사용하여 10 million 트랜젝션 쿼리들을 실행
$ ./forward-play-mariadb.sh epinions 10m execute 

  # 기존 생성한 epinions 초기 테이블들을 사용하여 100 million 트랜젝션 쿼리들을 실행
$ ./forward-play-mariadb.sh epinions 100m execute 

  # 기존 생성한 epinions 초기 테이블들을 사용하여 1 billion 트랜젝션 쿼리들을 실행
$ ./forward-play-mariadb.sh epinions 1b execute 

   # 생성된 초기 테이블들은 각각 <테이블명>_initial 테이블에 저장되며,   
  # execute할 때마다 각 <테이블명>_iniital을 <테이블명>으로 clone한 후 execute 실행
```

실행이 완료되면  `/var/log/mysql/mylog` 파일 안에 general 로그 파일이 생성됨.
