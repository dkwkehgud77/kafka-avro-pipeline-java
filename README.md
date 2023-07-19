# Realtime Data Pipeline 

## Introduction
이 프로젝트는 대용량 데이터와 트래픽을 안정적으로 처리할 수 있는 실시간 데이터 파이프라인을 구축합니다. 
Avro 스키마를 기반으로 대량의 메시지를 생산하며, Kafka 를 통해 메시지를 소비하여 MySQL 에 저장합니다.
데이터 파이프라인 설치 과정을 간소화하고, Avro 스키마를 활용하여 테이블 및 데이터 타입 변환을 자동화하였습니다.

Kafka 토픽의 다수 파티션과 멀티 컨슈머 스레드를 활용하여 데이터를 분산 병렬 처리하여 메시지 처리량을 최대화합니다. 
Kafka 프로듀서는 메시지 Key 값에 따라 해싱 처리하여 파티션에 분배 저장함으로써 데이터 순서를 보장합니다.

Kafka 컨슈머는 정상/비정상 종료 후 재실행 시 데이터 유실 없이 Exactly-Once Delivery 를 보장합니다. 
Backpressure 기능을 지원하여 블록킹 Queue 를 통해 컨슈머가 메시지 처리 속도를 제어할 수 있습니다. 
이를 통해 대량의 데이터 처리와 트래픽에 대응할 수 있는 안정적인 실시간 데이터 파이프라인을 제공합니다.


## Features
#### Infra
- 테스트를 위한 Kafka, Schema-Registry, MySQL 등 Docker-compose 로 세팅
- 첨부된 Json 파일을 읽어 Avro 스키마 포맷의 Json 파일로 변환하여 덤프 및 재사용
- Avro 스키마 데이터를 파싱하여 Kafka 토픽, MySQL 테이블을 동적으로 생성
- AVRO 스키마, Kafka 토픽, MySql 테이블은 1:1:1의 관계로 구성

#### Producer
- 멀티스레드를 활용하여 대량의 Avro 스키마 메시지를 병렬로 생산
- Kafka 토픽의 다수 파티션을 통해 대량의 메시지를 분산 저장
- 메시지 Key 값에 따라 동일한 파티션에 생산하여 순서를 보장
- Avro 스키마 레지스트리를 활용한 메시지 시리얼라이즈

#### Consumer
- Kafka Topic 멀티 파티션-스레드 대량의 메시지 분산 병렬 처리
- 장애 복구 시 데이터 유실 없이 Exactly-Once Delivery 보장
- 블록킹 Queue 를 활용한 Backpressure 메시지 처리 속도 제어
- Avro 스키마 디시리얼라이즈, 동적으로 SQL 생성 및 데이터 삽입

## Consumer Explanation
#### Kafka Topic 멀티 파티션-스레드 대량의 메시지 분산 병렬 처리
- 1개의 토픽의 N개의 파티션의 개수만큼 N개의 Thread 를 할당하여 메시지 분산 병렬 처리합니다.
- 1개의 토픽의 N개의 파티션을 컨슈밍하고 있는 Thread 들을 1개의 컨슈머 그룹으로 지정합니다.
- Backpressure 기능을 위해 컨슈머 1개당 데이터베이스 처리하는 Thread 1개를 추가로 생성합니다.
- 1개의 토픽 > N개의 파티션 > N개의 컨슈머 Thread > N개의 DB처리 Thread 로 구성합니다.

#### 장애 복구 시 데이터 유실 없이 Exactly-Once Delivery 보장
- MySQL 데이터베이스에 Kafka 의 Consumer Offset 정보를 저장하여 관리합니다.
- 애플리케이션 정상/비정상 종료 후 재실행 시 MySQL 에서 Offset 을 읽어와서 메시지를 소비합니다.
- 1개의 파티션의 메시지를 처리하는 스레드를 1개로 제한하여 파티션의 메시지 처리 순서를 보장합니다.
- Kafka-MySQL 구간에서 장애가 발생하면 트랜잭션을 rollback 하여 Exactly-Once 를 보장합니다.

#### 블록킹 Queue 를 활용한 Backpressure 메시지 처리 속도 제어
- Kafka 토픽을 구독하는 컨슈머 Thread 1개 당 DB처리 Thread 1개를 별도로 생성합니다.
- 토픽에서 컨슈밍한 메시지를 처리하기 전에 블록킹 Queue 에 추가하고 바로 다음 메시지를 컨슈밍합니다.
- DB처리 Thread 에서 메시지를 블록킹 Queue 에서 꺼내어 MySQL 에 처리하는 작업을 수행합니다.
- 블록킹 Queue 사이즈를 초과하면 Backpressure 기능이 작동하여 메시지 처리 속도를 제어합니다.
- 블록킹 Queue 개수가 줄어들 때까지 컨슈머 Thread 의 메시지 소비 작업을 대기하도록 합니다.

#### Avro 스키마 디시리얼라이즈, 동적으로 SQL 생성 및 데이터 삽입
- Avro 스키마를 사용하여 Kafka 토픽에서 가져온 데이터를 Java 객체로 디시리얼라이즈합니다.
- 레코드 객체에 포함된 Avro 스키마를 사용하여 동적으로 SQL 문을 작성하고, MySQL 테이블에 저장합니다.
- Avro 스키마 레지스트리를 활용하여 스키마 및 버전을 관리하여 데이터 형식 변경에 유연하게 대처합니다.


# Getting Started
### Prerequisites
- Java JDK 11
- Apache Maven 3.9.0
- Apache Kafka 2.8.1
- MySQL 8.0
- Docker 20.10.17
- Docker-compose 1.29.2


### 1. Infra Setting
```bash
cd ./01-infra
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

### 2. Infra Application Start
```bash
cd ./01-infra
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

### 3. Producer Application Start
```bash
# Producer Application
$ cd ./02-producer
$ mvn clean compile
$ mvn exec:java

[INFO] --- compiler:3.1:compile (default-compile) @ consumer ---
[INFO] Changes detected - recompiling the module!
[INFO] Compiling 4 source files to /Users/dohyung/ailab/realtime-pipeline/03-consumer/target/classes
[INFO] 
[INFO] --- exec:3.0.0:java (default-cli) @ consumer ---
[Main.main()] INFO Main - kafka consumer application start...

# Kafka UI
http://localhost:9000/topic/dataset1
http://localhost:9000/topic/dataset2
http://localhost:9000/topic/dataset3
- 정상적으로 토픽의 파티션에 메시지가 잘 분배되어 들어갔는지 확인한다.
```

### 3. Consumer Application Start
```bash
$ cd ./03-consumer
$ mvn clean compile
$ mvn exec:java

[INFO] --- compiler:3.1:compile (default-compile) @ consumer ---
[INFO] Changes detected - recompiling the module!
[INFO] Compiling 4 source files to /Users/dohyung/ailab/realtime-pipeline/03-consumer/target/classes
[INFO] 
[INFO] --- exec:3.0.0:java (default-cli) @ consumer ---
[Main.main()] INFO Main - kafka consumer application start...

# MySQL Console
$ docker exec -it mysql /bin/bash
$ mysql -u consumer -p  # pw: consumer1!

- 정상적으로 MySQL dataset 테이블에 데이터가 잘 들어갔는지 확인한다. 
mysql> use bank;
mysql> SELECT * FROM dataset1 limit 5;
+------------+------------------+---------------+---------------+
| key_field1 | timestamp_field1 | double_field1 | string_field1 |
+------------+------------------+---------------+---------------+
| OlJooL     |    1679818410755 |         0.718 | hjxXOO        |
| fsVcuB     |    1679818411274 |      0.402082 | McPNhz        |
| eGTOEZ     |    1679818411253 |      0.204099 | mNvnZz        |
| vtLfJG     |    1679818411255 |      0.114452 | vDUwiB        |
| ftAUsp     |    1679818411262 |      0.617986 | jkRYLj        |
+------------+------------------+---------------+---------------+
5 rows in set (0.00 sec)

- 정상적으로 MySQL kafka_offsets 테이블에 offset 정보가 업데이트 되었는지 확인한다.
mysql> select * from kafka_offsets;
+----------+-----------+----------------+--------+
| topic    | partition | consumer_group | offset |
+----------+-----------+----------------+--------+
| dataset1 |         0 | group-dataset1 |    532 |
| dataset1 |         1 | group-dataset1 |    493 |
| dataset1 |         2 | group-dataset1 |    475 |
| dataset2 |         0 | group-dataset2 |    502 |
| dataset2 |         1 | group-dataset2 |    504 |
| dataset2 |         2 | group-dataset2 |    494 |
| dataset3 |         0 | group-dataset3 |    513 |
| dataset3 |         1 | group-dataset3 |    534 |
| dataset3 |         2 | group-dataset3 |    453 |
+----------+-----------+----------------+--------+
9 rows in set (0.01 sec)

# Kafka UI
- Kafka 토픽 파티션의 offset과 MySQL kafka_offsets 테이블을 비교한다. 
- 데이터 유실 없이 Exactly Once가 보장되는지 데이터 정합성을 확인한다. 
http://localhost:9000/topic/dataset1
http://localhost:9000/topic/dataset2
http://localhost:9000/topic/dataset3

```

### Exactly Once
```bash
1. Consumer 애플리케이션이 실행되고 있는 상태에서, Producer 애플리케이션을 실행한다. 
2. Producer 가 대량의 메시지를 생산하고, Consumer 애플리케이션 메시지를 처리하기 시작한다.   
3. 메시지가 처리하고 있는 도중에 Consumer 애플리케이션을 중지시킨다. -> 장애상황이라고 가정
4. Consumer 애플리케이션 재시작하고 나서, 모든 메시지가 처리되기 까지 대기한다. 
5. Kafka 토픽 파티션의 offset과 MySQL kafka_offsets 테이블을 비교한다.
6. 장애 복구 후에도 데이터 유실 없이 Exactly Once가 보장되는지 데이터 정합성을 확인한다. 

# Consumer Stop
$ pkill -9 -f consumer

# Kafka UI
http://localhost:9000/topic/dataset1
http://localhost:9000/topic/dataset2
http://localhost:9000/topic/dataset3

# MySQL Console
mysql> select * from kafka_offsets;
+----------+-----------+----------------+--------+
| topic    | partition | consumer_group | offset |
+----------+-----------+----------------+--------+
| dataset1 |         0 | group-dataset1 |   1013 |
| dataset1 |         1 | group-dataset1 |    992 |
| dataset1 |         2 | group-dataset1 |    995 |
| dataset2 |         0 | group-dataset2 |   1052 |
| dataset2 |         1 | group-dataset2 |   1002 |
| dataset2 |         2 | group-dataset2 |    946 |
| dataset3 |         0 | group-dataset3 |   1041 |
| dataset3 |         1 | group-dataset3 |   1003 |
| dataset3 |         2 | group-dataset3 |    956 |
+----------+-----------+----------------+--------+
9 rows in set (0.03 sec)

```

### Backpressure 
```bash
# Consumer 애플리케이션을 패키징하여 백드라운드로 실행시킨다. 
$ cd ./03-consumer
$ mvn clean package
$ nohup java -jar target/consumer-1.0-jar-with-dependencies.jar &
$ ps -ef |grep consumer
  501 51542     1   0  7:59PM ttys023    0:11.92 /usr/bin/java -jar target/consumer-1.0-jar-with-dependencies.jar
  501 51561 16576   0  7:59PM ttys023    0:00.00 grep consumer

# 아래의 경고 메시지가 로그에 찍히면 블록킹 Queue 의 사이즈를 초과하였다는 의미이고,
# Backpressure 를 작동하여 메시지 처리 속도를 제어하고 있는 것을 확인할 수 있다.  
$ tail -f nohup.out| grep WARN
[pool-1-thread-4] WARN com.exam.worker.AvroConsumer - Backpressure activated and message processing stopped...
[pool-1-thread-9] WARN com.exam.worker.AvroConsumer - Backpressure activated and message processing stopped...
[pool-1-thread-1] WARN com.exam.worker.AvroConsumer - Backpressure activated and message processing stopped...
[pool-1-thread-4] WARN com.exam.worker.AvroConsumer - Backpressure has been resolved. Resuming message processing...
[pool-1-thread-9] WARN com.exam.worker.AvroConsumer - Backpressure has been resolved. Resuming message processing...
[pool-1-thread-4] WARN com.exam.worker.AvroConsumer - Backpressure activated and message processing stopped...
[pool-1-thread-3] WARN com.exam.worker.AvroConsumer - Backpressure activated and message processing stopped...
[pool-1-thread-1] WARN com.exam.worker.AvroConsumer - Backpressure has been resolved. Resuming message processing...
```

