package com.example.connector;

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.confluent.ksql.api.client.ExecuteStatementResult;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Service
public class sourceConnector {

    private final RestTemplate restTemplate;
    private Client client;

    @Value("${ksqldb.server.host}")
    private String ksqlDbHost;

    @Value("${ksqldb.server.port}")
    private int ksqlDbPort;

    public sourceConnector(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    @PostConstruct
    public void init() {
        ClientOptions options = ClientOptions.create()
                .setHost(ksqlDbHost)
                .setPort(ksqlDbPort);
        client = Client.create(options);
    }

    public void createDebeziumSourceConnector(String connectorName, Integer serverID, String serverName, String DBName, String tableName, String rawStreamName) {
        String url = "http://192.168.56.101:8083/connectors";

        Map<String, Object> config = new HashMap<>();
        config.put("connector.class", "io.debezium.connector.mysql.MySqlConnector");
        config.put("tasks.max", "1");
        config.put("database.hostname", "localhost");
        config.put("database.port", "3306");
        config.put("database.user", "test");
        config.put("database.password", "test");
        config.put("database.server.id", serverID);
        config.put("database.server.name", serverName);
        config.put("database.include.list", DBName);
        config.put("table.include.list", tableName);
        config.put("database.history.kafka.bootstrap.servers", "localhost:9092");
        config.put("database.history.kafka.topic", "schema-changes.mysql.oc");
        config.put("database.allowPublicKeyRetrieval", "true");
        config.put("time.precision.mode", "connect");
        config.put("database.connectionTimezone", "Asia/Seoul");
        config.put("key.converter", "io.confluent.connect.avro.AvroConverter");
        config.put("value.converter", "io.confluent.connect.avro.AvroConverter");
        config.put("key.converter.schema.registry.url", "http://localhost:8081");
        config.put("value.converter.schema.registry.url", "http://localhost:8081");
        config.put("transforms", "unwrap");
        config.put("transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState");
        config.put("transforms.unwrap.drop.tombstones", "false");

        Map<String, Object> request = new HashMap<>();
        request.put("name", connectorName);
        request.put("config", config);

        restTemplate.postForEntity(url, request, String.class);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // 성공적으로 커넥터를 생성한 후 스트림 생성 메서드 호출
        createWebLogStream(rawStreamName,serverName,tableName);
    }

    public void createWebLogStream(String rawStreamName,String severName, String tableName) {
        String createStreamKsql = "CREATE STREAM "+ rawStreamName +"("
                + "ssn_id struct<ssn_id varchar> key, "
                + "vstor_id VARCHAR, "
                + "infrd_vstor_uid VARCHAR, "
                + "cntn_grp_id VARCHAR, "
                + "cntn_id VARCHAR, "
                + "vym_cd VARCHAR, "
                + "vst_dt DATE, "
                + "vst_dtm VARCHAR, "
                + "vtmd_cd VARCHAR, "
                + "vst_dtm_tscd BIGINT, "
                + "vst_dywk_cd VARCHAR, "
                + "wkn_vst_yn VARCHAR, "
                + "vst_wknbr_cd VARCHAR, "
                + "vst_srno INT, "
                + "lusr_id VARCHAR, "
                + "cust_id VARCHAR, "
                + "nwvst_yn VARCHAR, "
                + "vst_ipadr VARCHAR, "
                + "cooki_actv_yn VARCHAR, "
                + "adid_actv_yn VARCHAR, "
                + "bwsr_nm VARCHAR, "
                + "bwsr_vrsn_nm VARCHAR, "
                + "bwsr_lng_nm VARCHAR, "
                + "acdvc_tpcl_nm VARCHAR, "
                + "acdvc_mfc_nm VARCHAR, "
                + "acdvc_prd_nm VARCHAR, "
                + "acdvc_os_nm VARCHAR, "
                + "acdvc_os_vrsn_nm VARCHAR, "
                + "acdvc_dprsl VARCHAR, "
                + "acrgn_nm VARCHAR, "
                + "st_page_yn VARCHAR, "
                + "epgdm_nm VARCHAR, "
                + "epght_nm VARCHAR, "
                + "epgpt_nm VARCHAR, "
                + "epgpar_nm VARCHAR, "
                + "epgurl VARCHAR, "
                + "epg_nm VARCHAR, "
                + "epgex_nm VARCHAR, "
                + "epgtl_nm VARCHAR, "
                + "page_dwll_tscd INT, "
                + "cvrs_yn VARCHAR, "
                + "fnevt_yn VARCHAR, "
                + "bunc_yn VARCHAR"
                + ") WITH ("
                + "KAFKA_TOPIC='"+severName+"."+tableName +"', "
                + "VALUE_FORMAT='AVRO', "
                + "KEY_FORMAT='AVRO'"
                + ");";
        try {
            ExecuteStatementResult result = client.executeStatement(createStreamKsql).get();
            System.out.println("Stream created: " + result.queryId());
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }
}
