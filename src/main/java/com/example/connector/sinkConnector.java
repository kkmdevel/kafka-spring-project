package com.example.connector;

import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

@Service
public class sinkConnector {

    private final RestTemplate restTemplate;

    public sinkConnector(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    public void createJDBCSinkConnector(String connectorName, String topicName, String DBName, String tableName, String keyColumn) {
        String url = "http://192.168.56.101:8083/connectors";

        Map<String, Object> config = new HashMap<>();
        config.put("connector.class", "io.confluent.connect.jdbc.JdbcSinkConnector");
        config.put("tasks.max", "1");
        config.put("topics", topicName);
        config.put("connection.url", "jdbc:mysql://localhost:3306/"+DBName);
        config.put("connection.user", "test");
        config.put("connection.password", "test");
        config.put("table.name.format", DBName+"."+tableName);
        config.put("insert.mode", "upsert");
        config.put("pk.fields", keyColumn);
        config.put("pk.mode", "record_key");
        config.put("delete.enabled", "true");
        config.put("key.converter", "io.confluent.connect.avro.AvroConverter");
        config.put("value.converter", "io.confluent.connect.avro.AvroConverter");
        config.put("key.converter.schema.registry.url", "http://localhost:8081");
        config.put("value.converter.schema.registry.url", "http://localhost:8081");

        config.put("db.timezone", "Asia/Seoul");
        config.put("transforms", "convertTS");
        config.put("transforms.convertTS.type", "org.apache.kafka.connect.transforms.TimestampConverter$Value");
        config.put("transforms.convertTS.field", "VST_DTM");
        config.put("transforms.convertTS.format", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        config.put("transforms.convertTS.target.type", "Timestamp");


        Map<String, Object> request = new HashMap<>();
        request.put("name", connectorName);
        request.put("config", config);

        restTemplate.postForEntity(url, request, String.class);
    }
}
