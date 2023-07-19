import com.exam.worker.DataPipeline;
import org.apache.avro.Schema;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.ResourceBundle;


public class Main {
    private static final ResourceBundle RESOURCE_BUNDLE = ResourceBundle.getBundle("config");
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        logger.info("data pipeline setting start...");
        // AVRO 스키마, Kafka 토픽 및 MySql 테이블 생성 작업을 수행하는 DataPipeline 객체를 생성합니다.
        DataPipeline pipeline = new DataPipeline();

        // 첨부된 스키마 JSON 파일 읽어서 JSONArray 객체로 파싱합니다.
        String jsonFilePath = RESOURCE_BUNDLE.getString("before.json.file.path");
        try(FileReader reader = new FileReader(jsonFilePath)){
            JSONParser parser = new JSONParser();
            JSONArray beforeJsonArray = (JSONArray) parser.parse(reader);

            // JSONArray 으로 Avro 스키마 Json 파일을 생성하고, Schema 리스트로 반환합니다.
            List<Schema> avroSchemaList = pipeline.createSchema(beforeJsonArray);

            // Schema 리스트를 기반으로 Kafka 토픽과 MySql 테이블 생성 작업을 수행합니다.
            pipeline.createTopic(avroSchemaList);
            pipeline.createTable(avroSchemaList);

        } catch (IOException | ParseException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        logger.info("data pipeline setting stop...");
        // 모든 작업이 완료되면 애플리케이션을 종료합니다.
        System.exit(0);
    }

}
