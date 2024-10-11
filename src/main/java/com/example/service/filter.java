package com.example.service;

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.confluent.ksql.api.client.ExecuteStatementResult;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
public class filter {

    @Value("${ksqldb.server.host}")
    private String ksqlDbHost;

    @Value("${ksqldb.server.port}")
    private int ksqlDbPort;

    private Client client;

    @PostConstruct
    public void init() {
        ClientOptions options = ClientOptions.create()
                .setHost(ksqlDbHost)
                .setPort(ksqlDbPort);
        client = Client.create(options);
    }

    public void streamsCondition( String sourceStreamName, String streamName, String topicName, String columnName, String condition, String operation, boolean isNumeric, boolean withPartitions) {
        String filterStreamKsql = buildKsqlStatement(streamName, topicName, columnName, condition, operation, isNumeric, withPartitions, sourceStreamName);

        try {
            ExecuteStatementResult result = client.executeStatement(filterStreamKsql).get(30, TimeUnit.SECONDS);
            System.out.println("Filtered stream created and data inserted into new topic: " + result.queryId());
            System.out.println("Statement result: " + result);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            e.printStackTrace();
        }
    }

    private String buildKsqlStatement(String streamName, String topicName, String columnName, String condition, String operation, boolean isNumeric, boolean withPartitions, String sourceStreamName) {
        String partitions = withPartitions ? ", PARTITIONS=1" : "";
        String formattedCondition = isNumeric ? condition : "'" + condition + "'";
        return String.format(
                "CREATE STREAM %s WITH (KAFKA_TOPIC='%s', KEY_FORMAT='AVRO', VALUE_FORMAT='AVRO'%s) AS SELECT * FROM %s WHERE %s %s %s EMIT CHANGES;",
                streamName, topicName, partitions, sourceStreamName, columnName, operation, formattedCondition
        );
    }
}
