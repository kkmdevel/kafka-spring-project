package com.example.service;

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.confluent.ksql.api.client.ExecuteStatementResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Service
public class dropColumn {

    @Value("${ksqldb.server.host}")
    private String ksqlDbHost;

    @Value("${ksqldb.server.port}")
    private int ksqlDbPort;

    private Client client;

    @Autowired
    private describe describeService;

    @PostConstruct
    public void init() {
        ClientOptions options = ClientOptions.create()
                .setHost(ksqlDbHost)
                .setPort(ksqlDbPort);
        client = Client.create(options);
    }

    public void dropColumn(String sourceStreamName, String newStreamName, String topicName, String excludedColumnName) {
        try {
            List<String> schema = describeService.describeStream(sourceStreamName);
            System.out.println("Original Stream Schema: \n" + String.join(", ", schema));

            String newStreamKsql = createNewStreamKsql(sourceStreamName, newStreamName, topicName, schema, excludedColumnName);
            System.out.println("New Stream KSQL: \n" + newStreamKsql);

            executeKsqlCommand(newStreamKsql);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    private String createNewStreamKsql(String sourceStreamName, String newStreamName, String topicName, List<String> schema, String excludedColumnName) {
        List<String> columns = schema.stream()
                .filter(columnName -> !columnName.equals(excludedColumnName))
                .collect(Collectors.toList());

        String columnsList = String.join(", ", columns);

        return "CREATE STREAM " + newStreamName + " WITH (KAFKA_TOPIC='" + topicName + "', KEY_FORMAT='AVRO', VALUE_FORMAT='AVRO', PARTITIONS=1) AS " +
                "SELECT " + columnsList + " FROM " + sourceStreamName + " EMIT CHANGES;";
    }

    private void executeKsqlCommand(String ksqlCommand) throws InterruptedException, ExecutionException {
        ExecuteStatementResult result = client.executeStatement(ksqlCommand).get();
        System.out.println("Stream created: " + result.queryId());
    }
}
