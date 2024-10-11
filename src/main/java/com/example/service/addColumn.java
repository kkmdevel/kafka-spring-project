package com.example.service;

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.confluent.ksql.api.client.ExecuteStatementResult;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.concurrent.ExecutionException;

@Service
public class addColumn {

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

    public void streamsAdd(String sourceStreamName,String streamName, String topicName, String columnName, String dataType, boolean withPartitions) {
        String partitions = withPartitions ? ", PARTITIONS=1" : "";

        String createStreamKsql = "CREATE STREAM " + streamName + " WITH (KAFKA_TOPIC='" + topicName + "', KEY_FORMAT='AVRO', VALUE_FORMAT='AVRO'" + partitions + ") AS " +
                "SELECT *, CAST(NULL AS " + dataType + ") AS " + columnName + " FROM " + sourceStreamName + " EMIT CHANGES;";
        try {
            ExecuteStatementResult result = client.executeStatement(createStreamKsql).get();
            System.out.println("Stream created and data inserted into new topic: " + result.queryId());
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }
}
