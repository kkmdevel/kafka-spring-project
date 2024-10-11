package com.example.service;

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.confluent.ksql.api.client.ExecuteStatementResult;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.concurrent.ExecutionException;

@Service
public class createStream {

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

    public void createWebLogStream(String streamName, String topicName,String rawStreamName) {
        String createStreamKsql = "CREATE STREAM "+streamName+" WITH ("
                + "KAFKA_TOPIC='"+topicName+"', "
                + "VALUE_FORMAT='AVRO', "
                + "KEY_FORMAT='AVRO', "
                + "PARTITIONS=1"
                + ") AS SELECT "
                + "ssn_id->ssn_id AS ssn_id, "
                + "vstor_id, "
                + "infrd_vstor_uid, "
                + "cntn_grp_id, "
                + "cntn_id, "
                + "vym_cd, "
                + "vst_dt, "
                + "vst_dtm, "
                + "vtmd_cd, "
                + "vst_dtm_tscd, "
                + "vst_dywk_cd, "
                + "wkn_vst_yn, "
                + "vst_wknbr_cd, "
                + "vst_srno, "
                + "lusr_id, "
                + "cust_id, "
                + "nwvst_yn, "
                + "vst_ipadr, "
                + "cooki_actv_yn, "
                + "adid_actv_yn, "
                + "bwsr_nm, "
                + "bwsr_vrsn_nm, "
                + "bwsr_lng_nm, "
                + "acdvc_tpcl_nm, "
                + "acdvc_mfc_nm, "
                + "acdvc_prd_nm, "
                + "acdvc_os_nm, "
                + "acdvc_os_vrsn_nm, "
                + "acdvc_dprsl, "
                + "acrg_nm, "
                + "st_page_yn, "
                + "epgdm_nm, "
                + "epght_nm, "
                + "epgpt_nm, "
                + "epgpar_nm, "
                + "epgurl, "
                + "epg_nm, "
                + "epgex_nm, "
                + "epgtl_nm, "
                + "page_dwll_tscd, "
                + "cvrs_yn, "
                + "fnevt_yn, "
                + "bunc_yn "
                + "FROM "+rawStreamName+" "
                + "PARTITION BY ssn_id->ssn_id;";
        try {
            ExecuteStatementResult result = client.executeStatement(createStreamKsql).get();
            System.out.println("Stream created and data inserted into new topic: " + result.queryId());
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }
}
