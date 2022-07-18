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

package com.flipkart.turbo.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

/**
 * @author shah.dhruvik
 * @date 08/08/19.
 */

@Getter
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
public class AlertzConfig {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static AlertzConfig defaultAlertzConfig = new AlertzConfig();
    private int severity = 0;
    private Integer pendingMessage;
    private Integer sidelinedMessages;
    private Integer rowsAvailability;

    public int getPendingMessage() {
        return pendingMessage != null ? pendingMessage : 10000;
    }

    public int getSidelinedMessages() {
        return sidelinedMessages != null ? sidelinedMessages : 0;
    }

    public int getRowsAvailability() {
        return rowsAvailability != null ? rowsAvailability : 300000;
    }

    public int getSeverity() {
        if(severity < 0 || severity > 3) {
            severity = 0;
        }
        return severity;
    }

    public static String getSeverityString(int severity) {
        switch (severity) {
            case 0:
                return "SEV0";
            case 1:
                return "SEV1";
            case 2:
                return "SEV2";
            case 3:
                return "SEV3";
            default:
                return "SEV0";
        }
    }


    public static List<AlertzConfig> parseOrDefault(List<String> config) {
        if (config == null) {
//            log.warn("Alertz config is Null, Using Default Config");
            return defaultAlertzConfigList();
        }
        try {
            List<AlertzConfig> alertzConfigs = OBJECT_MAPPER.convertValue(config, new TypeReference<List<AlertzConfig>>() {
            });
            if (alertzConfigs == null || alertzConfigs.isEmpty()) {
//                log.warn("Alertz config is Empty, Using Default Config", config);
                return defaultAlertzConfigList();
            }
            return alertzConfigs;
        } catch (Exception e) {
//            log.error("Using Default Config... Exception while parsing Alertz Config: " + config, e);
            return defaultAlertzConfigList();
        }
    }

    public static List<AlertzConfig> defaultAlertzConfigList() {
        List<AlertzConfig> alertzConfigs = new ArrayList<>();
        alertzConfigs.add(defaultAlertzConfig);
        return alertzConfigs;
    }
}