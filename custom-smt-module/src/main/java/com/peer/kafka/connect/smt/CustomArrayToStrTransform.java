package com.peer.kafka.connect.smt;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.apache.kafka.connect.transforms.util.*;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class CustomArrayToStrTransform<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final String FIELD_CONFIG = "field";
    private static final String ARRAY_CONFIG = "array";

    private String field;
    private String array;

    @Override
    public void configure(Map<String, ?> configs) {
        SimpleConfig config = new SimpleConfig(configDef(), configs);
        field = config.getString(FIELD_CONFIG);
        array = config.getString(ARRAY_CONFIG);
    }

    @Override
    public ConnectRecord apply(ConnectRecord record) {
        Object value = record.value();

        if (value instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) value;
            Object assignedToObject = map.get(field);
            if (assignedToObject instanceof Map) {
                Map<String, Object> assignedToMap = (Map<String, Object>) assignedToObject;
                Object referencesObject = assignedToMap.get(array);
                if (referencesObject instanceof List) {
                    List<String> referencesList = (List<String>) referencesObject;
                    assignedToMap.put(array, String.join(",", referencesList));
                }
            }
            Object originatorObject = map.get(field);
            if (originatorObject instanceof Map) {
                Map<String, Object> originatorMap = (Map<String, Object>) originatorObject;
                Object referencesObject = originatorMap.get(array);
                if (referencesObject instanceof List) {
                    List<String> referencesList = (List<String>) referencesObject;
                    originatorMap.put(array, String.join(",", referencesList));
                }
            }
        }

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                record.valueSchema(),
                value,
                record.timestamp()
        );
    }

    @Override
    public void close() {
        // No-op
    }

    @Override
    public ConfigDef config() {
        return configDef();
    }

    private static ConfigDef configDef() {
        return new ConfigDef()
                .define(FIELD_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                        "The name of the field containing the object with the array to convert.")
                .define(ARRAY_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                        "The name of the array to convert to a string.");
    }


}

