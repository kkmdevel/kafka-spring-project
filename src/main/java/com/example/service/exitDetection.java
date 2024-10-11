package com.example.service;

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.confluent.ksql.api.client.ExecuteStatementResult;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.concurrent.ExecutionException;

@Service
public class exitDetection {

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

    public void streamsExitDetection(String sourceStreamName, String streamName, String topicName) {

        String createTableKsql = "CREATE STREAM " + streamName + " WITH (KAFKA_TOPIC='" + topicName + "', KEY_FORMAT='AVRO', VALUE_FORMAT='AVRO', PARTITIONS=1) AS " +
                "SELECT " +
                "ssn_id, " +
                "lusr_id, " +
                "bunc_yn, " +
                "vst_dtm, " +
                "page_dwll_tscd, " +
                "ssn_id + lusr_id AS exit_id " +
                "FROM " + sourceStreamName + " " +
                "WHERE bunc_yn = '0' " +
                "PARTITION BY ssn_id + lusr_id;";
        try {
            ExecuteStatementResult result = client.executeStatement(createTableKsql).get();
            System.out.println("Table created and data inserted into new topic: " + result.queryId());
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }
}
