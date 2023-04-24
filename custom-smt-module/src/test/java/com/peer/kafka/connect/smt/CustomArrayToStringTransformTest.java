package com.peer.kafka.connect.smt;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Before;
import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;


import java.io.IOException;
import java.util.*;
public class CustomArrayToStringTransformTest {

    private CustomArraytoStringTransform<SourceRecord> transform;
    private String subdoc1Name;
    private String subdoc2Name;
    private String array1FieldName;
    private String array2FieldName;

   /* @Before
    public void setup() {
        transform = new CustomArraytoStringTransform<>();
        subdoc1Name = "assignedTo";
        subdoc2Name = "originator";
        array1FieldName = "references";
        array2FieldName = "references";
        Map<String, String> props = new HashMap<>();
        props.put("array1FieldName", array1FieldName);
        props.put("array2FieldName", array2FieldName);
        props.put("subdoc1Name", subdoc1Name);
        props.put("subdoc2Name", subdoc2Name);
        transform.configure(props);
    }

    */

    @Test
    public void testApply() {

        transform = new CustomArraytoStringTransform<>();
        subdoc1Name = "assignedTo";
        subdoc2Name = "originator";
        array1FieldName = "references";
        array2FieldName = "references";
        Map<String, String> props = new HashMap<>();
        props.put("array1FieldName", array1FieldName);
        props.put("array2FieldName", array2FieldName);
        props.put("subdoc1Name", subdoc1Name);
        props.put("subdoc2Name", subdoc2Name);
        transform.configure(props);

        // Create a test record with a JSON string value
        String jsonString = "{\n" +
                "    \"id\": \"MzlhZmM5YmQtOTgyNi01MzhmLTllMGUtYWY2ODQyOTY0ZTBjOmU2OThhM2YzLWUzYzctNGUxZi05NmQ2LTdjZTQ5MGM1OWRmMToxNjA5ODQ3MTA0OTAxOg==\",\n" +
                "        \"type\": \"spm-security-breach\",\n" +
                "            \"status\": \"RESOLVED\",\n" +
                "                \"severity\": \"high\",\n" +
                "                    \"products\": [\n" +
                "                        \"dxmonitor\"\n" +
                "                    ],\n" +
                "                        \"userGroups\": [\n" +
                "                            \"521065b0-bf94-454b-87d7-fdef44893e75\",\n" +
                "                            \"a18943db-7236-4338-9e2f-76d05bd13d60\"\n" +
                "                        ],\n" +
                "                            \"broadcast\": true,\n" +
                "                                \"firstEventTime\": \"2021-01-05T11:45:04.901Z\",\n" +
                "                                    \"reportableByRuleGroupId\": \"spm-security-breach\",\n" +
                "                                        \"resolvableByRuleGroupId\": \"spm-in-normal-operation-mode\",\n" +
                "                                            \"resolvedBy\": \"92444c9c-5bd4-11eb-ae93-0242ac130002\",\n" +
                "                                                \"resolvedTime\": \"2021-02-24T05:44:24.8904952Z\",\n" +
                "                                                    \"properties\": {\n" +
                "        \"deviceType\": \"\"\n" +
                "    },\n" +
                "    \"assignedTo\": {\n" +
                "        \"scope\": null,\n" +
                "            \"provider\": \"site\",\n" +
                "                \"references\": [\n" +
                "                    \"aa654da3-7c0e-436c-966f-19d17cda7f79\"\n" +
                "                ]\n" +
                "    },\n" +
                "    \"originator\": {\n" +
                "        \"scope\": null,\n" +
                "            \"provider\": \"device\",\n" +
                "                \"references\": [\n" +
                "                    \"39afc9bd-9826-538f-9e0e-af6842964e0c\"\n" +
                "                ]\n" +
                "    }\n" +
                "}";
        Schema valueSchema = SchemaBuilder.string().build();
        SourceRecord record = new SourceRecord(null, null, "test", null, null, valueSchema, jsonString);

        // Apply the transform
        SourceRecord result = transform.apply(record);

        // Verify the transformed value
        String expected = "{\"severity\":\"high\",\"broadcast\":true,\"resolvedTime\":\"2021-02-24T05:44:24.8904952Z\",\"resolvedBy\":\"92444c9c-5bd4-11eb-ae93-0242ac130002\",\"resolvableByRuleGroupId\":\"spm-in-normal-operation-mode\",\"originator\":{\"references\":\"39afc9bd-9826-538f-9e0e-af6842964e0c\",\"provider\":\"device\",\"scope\":null},\"type\":\"spm-security-breach\",\"assignedTo\":{\"references\":\"aa654da3-7c0e-436c-966f-19d17cda7f79\",\"provider\":\"site\",\"scope\":null},\"products\":[\"dxmonitor\"],\"userGroups\":[\"521065b0-bf94-454b-87d7-fdef44893e75\",\"a18943db-7236-4338-9e2f-76d05bd13d60\"],\"reportableByRuleGroupId\":\"spm-security-breach\",\"id\":\"MzlhZmM5YmQtOTgyNi01MzhmLTllMGUtYWY2ODQyOTY0ZTBjOmU2OThhM2YzLWUzYzctNGUxZi05NmQ2LTdjZTQ5MGM1OWRmMToxNjA5ODQ3MTA0OTAxOg==\",\"firstEventTime\":\"2021-01-05T11:45:04.901Z\",\"properties\":{\"deviceType\":\"\"},\"status\":\"RESOLVED\"}";
        assertEquals(expected, result.value());


        // Create a test record with a JSON string value
        String jsonWithoutArrayString = "{\n" +
                "    \"id\": \"MzlhZmM5YmQtOTgyNi01MzhmLTllMGUtYWY2ODQyOTY0ZTBjOmU2OThhM2YzLWUzYzctNGUxZi05NmQ2LTdjZTQ5MGM1OWRmMToxNjA5ODQ3MTA0OTAxOg==\",\n" +
                "        \"type\": \"spm-security-breach\",\n" +
                "            \"status\": \"RESOLVED\",\n" +
                "                \"severity\": \"high\",\n" +
                "                    \"products\": [\n" +
                "                        \"dxmonitor\"\n" +
                "                    ],\n" +
                "                        \"userGroups\": [\n" +
                "                            \"521065b0-bf94-454b-87d7-fdef44893e75\",\n" +
                "                            \"a18943db-7236-4338-9e2f-76d05bd13d60\"\n" +
                "                        ],\n" +
                "                            \"broadcast\": true,\n" +
                "                                \"firstEventTime\": \"2021-01-05T11:45:04.901Z\",\n" +
                "                                    \"reportableByRuleGroupId\": \"spm-security-breach\",\n" +
                "                                        \"resolvableByRuleGroupId\": \"spm-in-normal-operation-mode\",\n" +
                "                                            \"resolvedBy\": \"92444c9c-5bd4-11eb-ae93-0242ac130002\",\n" +
                "                                                \"resolvedTime\": \"2021-02-24T05:44:24.8904952Z\",\n" +
                "                                                    \"properties\": {\n" +
                "        \"deviceType\": \"\"\n" +
                "    },\n" +
                "    \"assignedTo\": {\n" +
                "        \"scope\": null,\n" +
                "            \"provider\": \"site\"\n" +
                "    }\n" +
                "}";
        Schema valuejsonWithoutArraySchema = SchemaBuilder.string().build();
        SourceRecord recordJsonWithoutArrayString = new SourceRecord(null, null, "test", null, null, valuejsonWithoutArraySchema, jsonWithoutArrayString);

        // Apply the transform
        SourceRecord resultWithoutArray = transform.apply(recordJsonWithoutArrayString);

        // Verify the transformed value
        String expectedWithoutArray = "{\"severity\":\"high\",\"broadcast\":true,\"resolvedTime\":\"2021-02-24T05:44:24.8904952Z\",\"resolvedBy\":\"92444c9c-5bd4-11eb-ae93-0242ac130002\",\"resolvableByRuleGroupId\":\"spm-in-normal-operation-mode\",\"type\":\"spm-security-breach\",\"assignedTo\":{\"provider\":\"site\",\"scope\":null},\"products\":[\"dxmonitor\"],\"userGroups\":[\"521065b0-bf94-454b-87d7-fdef44893e75\",\"a18943db-7236-4338-9e2f-76d05bd13d60\"],\"reportableByRuleGroupId\":\"spm-security-breach\",\"id\":\"MzlhZmM5YmQtOTgyNi01MzhmLTllMGUtYWY2ODQyOTY0ZTBjOmU2OThhM2YzLWUzYzctNGUxZi05NmQ2LTdjZTQ5MGM1OWRmMToxNjA5ODQ3MTA0OTAxOg==\",\"firstEventTime\":\"2021-01-05T11:45:04.901Z\",\"properties\":{\"deviceType\":\"\"},\"status\":\"RESOLVED\"}";
        assertEquals(expectedWithoutArray, resultWithoutArray.value());

    }


}
