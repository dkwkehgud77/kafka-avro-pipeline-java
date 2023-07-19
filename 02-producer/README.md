
# Kafka Producer Application

### Introduction
이 프로젝트는 Avro 스키마를 기반으로 병렬로 대량의 메시지를 생산하는 애플리케이션입니다.
Kafka 토픽의 다수 파티션과 멀티스레드를 활용하여 병렬로 메시지 생산하고 분산 저장합니다.
데이터의 순서를 보장하기 위해 Key 값에 따라 동일한 파티션에 메시지를 생산하도록 합니다.
이를 위해 Key 값을 해싱 처리하고 파티션 개수로 나누어 파티션 Id를 할당하여 메시지를 전송합니다. 

### Features
- 멀티스레드를 활용하여 대량의 Avro 스키마 메시지를 병렬로 생산
- Kafka 토픽의 다수 파티션을 통해 대량의 메시지를 분산 처리
- 메시지 Key 값에 따라 동일한 파티션에 생산하여 순서를 보장
- Avro 스키마 레지스트리를 활용한 메시지 시리얼라이즈

# Getting Started
### Prerequisites
- Java JDK 11
- Apache Maven 3.9.0
- Apache Kafka 2.8.1


### Configuration
프로퍼티 파일에 Kafka Producer 애플리케이션에 필요한 구성 설정을 합니다.
```properties
# Json
avro.json.file.path=../schema/schema_avro.json

# Thread
thread.count.per.producer=5

# Message
message.count.per.topic=300

# Kafka
kafka.bootstrap.servers=localhost:9091,localhost:9092,localhost:9093
kafka.schema.registry.url=http://localhost:8081

```

### Application Start
1. properties 파일을 세팅합니다.
2. Maven을 사용하여 프로젝트를 빌드합니다.
3. Maven을 사용하여 Consumer 애플리케이션을 실행합니다.
```bash
$ mvn clean compile exec:java

[INFO] --- compiler:3.1:compile (default-compile) @ producer ---
[INFO] Changes detected - recompiling the module!
[INFO] Compiling 2 source files to /Users/dohyung/ailab/realtime-pipeline/02-producer/target/classes
[INFO] 
[INFO] --- exec:3.0.0:java (default-cli) @ producer ---
[Main.main()] INFO Main - kafka producer application start...

# Kafka UI
http://localhost:9000/topic/dataset1
http://localhost:9000/topic/dataset2
http://localhost:9000/topic/dataset3
- 정상적으로 토픽의 파티션에 메시지가 잘 분배되어 들어갔는지 확인한다.
```

### Application Deploy
1. properties 파일을 세팅합니다.
2. Maven을 사용하여 프로젝트를 패키징합니다.
3. Java를 사용하여 백그라운드로 Producer 애플리케이션을 실행합니다.
```bash
$ mvn clean compile
$ nohup java -jar target/producer-1.0-jar-with-dependencies.jar &

$ ps -ef |grep producer
  501 15323 10877   0  2:10PM ttys019    0:05.73 /usr/bin/java -jar target/producer-1.0-jar-with-dependencies.jar

```

### Application Stop
Mac이나 리눅스 기반의 OS에서는 Shell 파일을 이용해서 애플리케이션을 실행하거나 중지할 수 있습니다.
```bash
$ chmod 755 start.sh stop.sh
$ ./stop.sh

[1] + killed     nohup java -jar target/producer-1.0-jar-with-dependencies.jar
```

