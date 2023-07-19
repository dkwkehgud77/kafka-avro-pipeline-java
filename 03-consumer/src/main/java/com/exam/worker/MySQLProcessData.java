package com.exam.worker;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.stream.Collectors;

class MySQLProcessData {

    private static final ResourceBundle RESOURCE_BUNDLE = ResourceBundle.getBundle("config");
    private static final Logger logger = LoggerFactory.getLogger(MySQLProcessData.class);

    public MySQLConnectionPool createConnPool()  {
        MySQLConnectionPool dbPool = new MySQLConnectionPool(
                    RESOURCE_BUNDLE.getString("mysql.url"),
                    RESOURCE_BUNDLE.getString("mysql.username"),
                    RESOURCE_BUNDLE.getString("mysql.password"),
                    Integer.parseInt(RESOURCE_BUNDLE.getString("mysql.maxPoolSize")));
        return dbPool;
    }

    public Connection getConnMySQL(MySQLConnectionPool dbPool) {
        try {
            if(dbPool == null)
                dbPool = createConnPool();
            return dbPool.getConnection();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    public Map<TopicPartition, OffsetAndMetadata> readOffsetsFromDB(Connection conn, String topicName, int partitionId, String consumerGroupId){
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

        // DB 에서 컨슈머가 구독하고 있는 토픽-파티션의 Offset 값을 읽어옵니다.
        try {
            String query = String.format("SELECT * FROM kafka_offsets " +
                            "where topic='%s' and `partition`=%d and consumer_group='%s'" ,topicName, partitionId, consumerGroupId);
            System.out.println(query);
            PreparedStatement stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                String topic = rs.getString("topic");
                int partition = rs.getInt("partition");
                long offset = rs.getLong("offset");
                offsets.put(new TopicPartition(topic, partition), new OffsetAndMetadata(offset));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return offsets;
    }

    public void saveOffsetToDB(Connection conn, Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId) throws SQLException {
        // 처리한 offset 정보를 데이터베이스 kafka_offset 테이블에 업데이트 합니다.
        String sql = "INSERT INTO kafka_offsets (offset, topic, `partition`, consumer_group)\n" +
                "VALUES (?, ?, ?, ?)\n" +
                "ON DUPLICATE KEY UPDATE\n" +
                "    offset = ?,\n" +
                "    topic = ?,\n" +
                "    `partition` = ?,\n" +
                "    consumer_group = ?";

        PreparedStatement stmt = conn.prepareStatement(sql);
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            TopicPartition tp = entry.getKey();
            long offset = entry.getValue().offset();
            stmt.setLong(1, offset);
            stmt.setString(2, tp.topic());
            stmt.setInt(3, tp.partition());
            stmt.setString(4, consumerGroupId);
            stmt.setLong(5, offset);
            stmt.setString(6, tp.topic());
            stmt.setInt(7, tp.partition());
            stmt.setString(8, consumerGroupId);

//            logger.debug(String.format("offsets updated successfuly... topic:%s partition:%d",tp.topic(), tp.partition()));
            stmt.executeUpdate();
        }

    }

    public int insertRecordToDB(Connection conn, ConsumerRecord<String, GenericData.Record> record) throws SQLException {
        // 전달된 Record 의  Avro Schema 에 포함된 테이블 포맷에 맞게 동적으로 삽입 합니다.
        GenericData.Record recordValue =  record.value();
        Schema schema = recordValue.getSchema();
        String tableName = record.topic();

        // 필드 이름을 추출하고 문자열 리스트로 변환 ->  ',' 로 이어 스트링으로 만듭니다.
        List<Schema.Field> fields = schema.getFields();

        List<String> fieldNames = fields.stream()
                .map(Schema.Field::name)
                .collect(Collectors.toList());
        String fieldString = String.join(", ", fieldNames);

        // `?`를 필드명 개수만큼 반복해서 문자열을 구성합니다.
        String placeholders = fieldNames.stream().map(s -> "?").collect(Collectors.joining(", "));

        String insert_sql = String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, fieldString, placeholders);
//        logger.info(insert_sql);
        PreparedStatement stmt = conn.prepareStatement(insert_sql);

        // MySQL 테이블에 Kafka Record 를 삽입합니다.
        for( int i=0; i < fields.size(); i++){
            int idx = i+1;
            Schema.Field field = fields.get(i);
            String fieldName = field.name();
            String fieldType = field.schema().getType().name();
            Object value = recordValue.get(fieldName);
            stmt = setParameter(stmt, idx, value, fieldType);
        }

        int result = stmt.executeUpdate();
//        if(result > 0)
//            logger.info(String.format("Record inserted successfuly... Table %s ", tableName));

        return result;
    }

    public PreparedStatement setParameter(PreparedStatement stmt, int parameterIndex, Object value, String sqlType) throws SQLException {
        switch (sqlType) {
            case "STRING":
                stmt.setString(parameterIndex, value.toString());
                return stmt;

            case "INT":
                stmt.setInt(parameterIndex, (Integer) value);
                return stmt;

            case "LONG":
                stmt.setLong(parameterIndex, (Long) value);
                return stmt;

            case "DOUBLE":
                stmt.setDouble(parameterIndex, (Double) value);
                return stmt;

            default:
                stmt.setObject(parameterIndex, value);
                return stmt;
//                throw new SQLException("Unknown SQL type: " + sqlType);
        }

    }


}