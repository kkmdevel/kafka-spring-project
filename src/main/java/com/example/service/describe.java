package com.example.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;

@Service
public class describe {

    @Value("${ksqldb.server.url}")
    private String ksqlDbUrl;

    private WebClient webClient;
    private ObjectMapper objectMapper;

    @PostConstruct
    public void init() {
        this.webClient = WebClient.builder()
                .baseUrl(ksqlDbUrl)
                .build();
        this.objectMapper = new ObjectMapper();
    }

    public List<String> describeStream(String streamName) {
        String ksqlStatement = String.format("DESCRIBE %s;", streamName);
        String requestBody = String.format("{\"ksql\": \"%s\", \"streamsProperties\": {}}", ksqlStatement);

        try {
            String response = webClient.post()
                    .uri("/ksql")
                    .header("Content-Type", "application/vnd.ksql.v1+json")
                    .bodyValue(requestBody)
                    .retrieve()
                    .bodyToMono(String.class)
                    .block();

            // JSON 응답을 파싱하여 필드 이름을 리스트로 반환
            JsonNode jsonResponse = objectMapper.readTree(response);
            List<String> fieldNames = new ArrayList<>();

            if (jsonResponse.isArray() && jsonResponse.size() > 0) {
                JsonNode fieldsNode = jsonResponse.get(0).get("sourceDescription").get("fields");
                System.out.println("Field Names in Stream Schema:");
                if (fieldsNode != null && fieldsNode.isArray()) {
                    for (JsonNode field : fieldsNode) {
                        String fieldName = field.get("name").asText();
                        System.out.println(fieldName);
                        fieldNames.add(fieldName);
                    }
                } else {
                    System.out.println("No fields found in the stream schema.");
                }
            } else {
                System.out.println("Unexpected response format: " + response);
            }

            return fieldNames;

        } catch (WebClientResponseException e) {
            throw new RuntimeException("Failed to describe stream: " + streamName, e);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse response: " + streamName, e);
        }
    }
}
