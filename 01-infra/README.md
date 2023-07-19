# Data Pipeline Setting

### Introduction
이 애플리케이션은 첨부된 스키마 JSON 파일을 읽어 AVRO 스키마, Kafka 토픽, 및 MySQL 테이블을 자동으로 생성합니다. 
데이터 파이프라인 설치 과정을 간소화하고 AVRO 스키마, Kafka 토픽 및 MySQL 테이블 간의 일관성을 보장할 수 있습니다.
이를 통해 새로운 데이터 파이프라인을 세팅하는 시간을 절약하고 오류 위험을 줄이며 데이터 인프라를 유지 관리하고 확장하기 쉬워집니다.

### Features
- 테스트를 위한 Kafka, Schema-Registry, MySQL 등 Docker-compose 로 세팅
- 첨부된 Json 파일을 읽어 Avro 스키마 포맷의 Json 파일로 변환하여 덤프 및 재사용
- Avro 스키마 데이터를 파싱하여 Kafka 토픽, MySQL 테이블을 동적으로 생성
- AVRO 스키마, Kafka 토픽, MySql 테이블은 1:1:1의 관계로 구성


# Getting Started
### Prerequisites
- Java JDK 11
- Apache Maven 3.9.0
- Apache Kafka 2.8.1
- MySQL 8.0
- Docker 20.10.17
- Docker-compose 1.29.2


### Infra Setting 
- Kafka, Schema-Registry, MySQL 생성 및 실행 
- AVRO 스키마, Kafka 토픽, MySql 테이블 생성

```bash
$ docker-compose up -d 
Starting mysql     ... done
Starting zookeeper ... done
Starting kafka2    ... done
Starting kafka1    ... done
Starting kafka3    ... done
Starting schema-registry ... done
Starting kafdrop         ... done

$ docker ps
CONTAINER ID   IMAGE                                   COMMAND                  CREATED        STATUS        PORTS                                                  NAMES
b0ef08a377b2   obsidiandynamics/kafdrop                "/kafdrop.sh"            13 hours ago   Up 13 hours   0.0.0.0:9000->9000/tcp                                 kafdrop
9a23d9188cf1   confluentinc/cp-schema-registry:7.0.0   "/etc/confluent/dock…"   13 hours ago   Up 13 hours   0.0.0.0:8081->8081/tcp                                 schema-registry
c37a1e5ab2ba   confluentinc/cp-kafka:7.0.0             "/etc/confluent/dock…"   13 hours ago   Up 13 hours   9092/tcp, 0.0.0.0:9093->9093/tcp                       kafka3
a9e70028d48e   confluentinc/cp-kafka:7.0.0             "/etc/confluent/dock…"   13 hours ago   Up 13 hours   0.0.0.0:9091->9091/tcp, 9092/tcp                       kafka1
2161366c7af8   confluentinc/cp-kafka:7.0.0             "/etc/confluent/dock…"   13 hours ago   Up 13 hours   0.0.0.0:9092->9092/tcp                                 kafka2
e4378c3e42ca   mysql:8.0                               "docker-entrypoint.s…"   2 days ago     Up 13 hours   0.0.0.0:3306->3306/tcp, 33060/tcp                      mysql
9a4e488a016e   zookeeper:3.7                           "/docker-entrypoint.…"   2 days ago     Up 13 hours   2888/tcp, 3888/tcp, 0.0.0.0:2181->2181/tcp, 8080/tcp   zookeeper

# Avro 스키마 Json 파일 
$ cd schema
$ ls 
schema_before.json
- 첨부된 스키마 Json 파일 위치  

# Kafka UI
http://localhost:9000/
- 브로커 상태 및 토픽 파티션의 메시지 확인 가능 

# Kafka Console
docker exec -it kafka1 /bin/bash

# MySQL Console
$ docker exec -it mysql /bin/bash
$ mysql -u infra -p  # pw: infra1!
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 46663
Server version: 8.0.32 MySQL Community Server - GPL
```


### Configuration
프로퍼티 파일에 Kafka Consumer 애플리케이션에 필요한 구성 설정을 합니다.
```properties
# Json
before.json.file.path=../schema/schema_before.json
after.json.file.path=../schema/schema_avro.json

# Kafka
kafka.bootstrap.servers=localhost:9091,localhost:9092,localhost:9093
kafka.topic.partition.count=3
kafka.topic.replica-factor.count=3

# MySQL
mysql.url=jdbc:mysql://localhost:3306/bank
mysql.username=infra
mysql.password=infra1!
```

### Application Start
1. properties 파일을 세팅합니다.
2. Maven을 사용하여 프로젝트를 빌드합니다.
3. Maven을 사용하여 Consumer 애플리케이션을 실행합니다.
```bash
$ mvn clean compile 
$ mvn exec:java

[INFO] --- exec:3.0.0:java (default-cli) @ infra ---
[Main.main()] INFO Main - data pipeline setting start...
[Main.main()] INFO com.exam.worker.DataPipeline - Avro schema created succeessfully ... dataset1
[Main.main()] INFO com.exam.worker.DataPipeline - Avro schema created succeessfully ... dataset2
[Main.main()] INFO com.exam.worker.DataPipeline - Avro schema created succeessfully ... dataset3
[Main.main()] INFO com.exam.worker.DataPipeline - Avro schema Json dumped succeessfully ...

# Avro 스키마 Json 파일 
$ cd schema
$ ls 
schema_avro.json        schema_before.json
- 정상적으로 Avro 스키마 Json 파일이 생성되었는지 확인한다. 

# Kafka UI
http://localhost:9000/
- Kafka UI와 Console에서 정상적으로 토픽이 생성되었는지 확인한다. 

# Kafka Console
$ docker exec -it kafka1 /bin/bash
$ cd /usr/bin
$ kafka-topics --bootstrap-server kafka1:19091,kafka2:19092,kafka3:19093 --list
__consumer_offsets
_schemas
dataset1
dataset2
dataset3

# MySQL Console
$ docker exec -it mysql /bin/bash
$ mysql -u infra -p  # pw: infra1!
- 정상적으로 MySQL 데이터베이스와 테이블이 생성되었는지 확인한다. 
mysql> use bank;
mysql> show tables;
+----------------+
| Tables_in_bank |
+----------------+
| dataset1       |
| dataset2       |
| dataset3       |
| kafka_offsets  |
+----------------+
4 rows in set (0.00 sec)
```

### Application Deploy
1. properties 파일을 세팅합니다.
2. Maven을 사용하여 프로젝트를 패키징합니다.
3. Java를 사용하여 백그라운드로 Producer 애플리케이션을 실행합니다.
```bash
$ mvn clean compile
$ nohup java -jar target/producer-1.0-jar-with-dependencies.jar &

[1]  + exit 1     nohup java -jar target/producer-1.0-jar-with-dependencies.jar

$ ps -ef |grep infra
  501 13165 10580   0  1:56PM ttys018    0:01.93 /usr/bin/java -jar target/infra-1.0-jar-with-dependencies.jar
  501 13181 10580   0  1:56PM ttys018    0:00.00 grep infra
```

### Application Stop
Mac이나 리눅스 기반의 OS에서는 Shell 파일을 이용해서 애플리케이션을 실행하거나 중지할 수 있습니다.
```bash
$ chmod 755 start.sh stop.sh
$ ./stop.sh

[1]  + killed     nohup java -jar target/infra-1.0-jar-with-dependencies.jar
```

