package com.example.controller;

import com.example.service.exitDetection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import com.example.service.*;
import com.example.connector.*;

@RestController
@RequestMapping("/kafka/")
public class kafkaController {

    private final createStream create;
    private final addColumn add;
    private final dropColumn drop;
    private final filter filterCondition;
    private final exitDetection exitDetection;
    private final describe describe;
    private final sinkConnector sinkConnector;
    private final sourceConnector sourceConnector;

    @Autowired
    public kafkaController(createStream create, addColumn add, dropColumn drop, filter filterCondition ,
                          exitDetection exitDetection, describe describe,
                          sinkConnector sinkConnector, sourceConnector sourceConnector) {
        this.create = create;
        this.add = add;
        this.drop = drop;
        this.filterCondition = filterCondition;
        this.exitDetection = exitDetection;
        this.describe = describe;
        this.sinkConnector = sinkConnector;
        this.sourceConnector = sourceConnector;

    }

    @PostMapping("/createStream/{streamName}/{topicName}/{rawStreamName}")
    public void createStream(@PathVariable String streamName, @PathVariable String topicName,
                          @PathVariable String rawStreamName) {
       create.createWebLogStream(streamName, topicName, rawStreamName);
    }

    @PostMapping("/addColumn/{sourceStreamName}/{streamName}/{topicName}/{columnName}/{dataType}/{withPartitions}")
    public void addColumn(@PathVariable String sourceStreamName, @PathVariable String streamName,
                          @PathVariable String topicName, @PathVariable String columnName,
                          @PathVariable String dataType, @PathVariable boolean withPartitions) {
        add.streamsAdd(sourceStreamName, streamName, topicName, columnName, dataType, withPartitions);
    }

    @PostMapping("/dropColumn/{sourceStreamName}/{newStreamName}/{topicName}/{excludedColumnName}")
    public void dropColumn(@PathVariable String sourceStreamName, @PathVariable String newStreamName,
                             @PathVariable String topicName, @PathVariable String excludedColumnName) {
        drop.dropColumn(sourceStreamName, newStreamName, topicName, excludedColumnName);
    }

    @PostMapping("/filter/{sourceStreamName}/{streamName}/{topicName}/{columnName}/{condition}/{operation}/{isNumeric}/{withPartitions}")
    public void filterCondition(@PathVariable String streamName, @PathVariable String topicName,
                                @PathVariable String columnName, @PathVariable String condition,
                                @PathVariable String operation, @PathVariable boolean isNumeric,
                                @PathVariable boolean withPartitions, @PathVariable String sourceStreamName) {
        filterCondition.streamsCondition(sourceStreamName, streamName, topicName, columnName, condition, operation, isNumeric, withPartitions);
    }

    @PostMapping("/exitDetection/{sourceStreamName}/{streamName}/{topicName}")
    public void exitDetection(@PathVariable String sourceStreamName, @PathVariable String streamName,
                              @PathVariable String topicName) {
        exitDetection.streamsExitDetection(sourceStreamName, streamName, topicName);
    }

    @PostMapping("/describe/{sourceStreamName}")
    public void describe(@PathVariable String sourceStreamName) {
        describe.describeStream(sourceStreamName);
    }

    @PostMapping("/sinkConnector/{connectorName}/{topicName}/{DBName}/{tableName}/{keyColumn}")
    public void createJDBCSinkConnector(@PathVariable String connectorName, @PathVariable String topicName,
                                        @PathVariable String DBName, @PathVariable String tableName,
                                        @PathVariable String keyColumn) {
        sinkConnector.createJDBCSinkConnector(connectorName, topicName, DBName, tableName, keyColumn);
    }

    @PostMapping("/sourceConnector/{connectorName}/{serverID}/{serverName}/{DBName}/{tableName}/{rawStreamName}")
    public void createDebeziumSourceConnector(@PathVariable String connectorName, @PathVariable Integer serverID,
                                              @PathVariable String serverName,@PathVariable String DBName,
                                              @PathVariable String tableName, @PathVariable String rawStreamName) {
        sourceConnector.createDebeziumSourceConnector(connectorName, serverID, serverName, DBName, tableName,rawStreamName);
    }
}
