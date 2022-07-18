/*
 *
 *  Copyright (c) 2022 [The original author]
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 */

package com.flipkart.varidhi.core.utils;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;

import java.io.IOException;
import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.TimeZone;

public final class JsonUtil {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static Gson gson =
        new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
            .setDateFormat("yyyy-MM-dd'T'HH:mm:ss").create();

    static {
        OBJECT_MAPPER.setPropertyNamingStrategy(
            PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);
        OBJECT_MAPPER.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        OBJECT_MAPPER.setSerializationInclusion(Include.NON_NULL);
        OBJECT_MAPPER.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
        OBJECT_MAPPER.setTimeZone(TimeZone.getTimeZone("Asia/Calcutta"));
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        OBJECT_MAPPER.configure(DeserializationFeature.READ_ENUMS_USING_TO_STRING, true);
        OBJECT_MAPPER.configure(SerializationFeature.WRITE_ENUMS_USING_TO_STRING, true);
    }

    public static final Gson createGson() {
        return new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
            .setDateFormat("yyyy-MM-dd HH:mm:ss").create();
    }

    public static final Gson createGsonNew() {
        return gson;
    }

    public static final Gson createGson(Type type, JsonDeserializer<Map> deserializer) {
        return new GsonBuilder().excludeFieldsWithoutExposeAnnotation()
            .registerTypeAdapter(type, deserializer)
            .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).create();
    }

    public static final <T> T deserializeJson(String json, Class<T> type)
        throws IOException {
        ObjectMapper objectMapper = ObjectMapperProvider.INSTANCE.get();
        //For UTs
        if (null == objectMapper) {
            objectMapper = OBJECT_MAPPER;
        }
        return objectMapper.readValue(json, type);
    }

    public static final <T> T deserializeJson(String json, TypeReference<T> valueTypeRef)
        throws IOException {
        ObjectMapper objectMapper = ObjectMapperProvider.INSTANCE.get();
        //For UTs
        if (null == objectMapper) {
            objectMapper = OBJECT_MAPPER;
        }
        return objectMapper.readValue(json, valueTypeRef);
    }

    public static final String serializeJson(Object object) throws JsonProcessingException {
        ObjectMapper objectMapper = ObjectMapperProvider.INSTANCE.get();
        //For UTs
        if (null == objectMapper) {
            objectMapper = OBJECT_MAPPER;
        }
        return objectMapper.writeValueAsString(object);
    }

    public static final String serializeJsonQuietlyUsingGson(Object object) {
        try {
            return gson.toJson(object);
        } catch (Exception e) {
            return "";
        }
    }
}
