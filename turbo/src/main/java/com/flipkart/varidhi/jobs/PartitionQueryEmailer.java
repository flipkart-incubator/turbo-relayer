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

package com.flipkart.varidhi.jobs;

import com.flipkart.varidhi.config.ApplicationConfiguration;
import com.flipkart.varidhi.config.RelayerConfiguration;
import com.flipkart.varidhi.core.utils.EmailNotifier;
import com.flipkart.varidhi.repository.TRPartitionQueriesLogger;
import com.flipkart.varidhi.utils.Splitable;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;

public class PartitionQueryEmailer implements TRPartitionQueriesLogger {
    private static final Logger logger = LoggerFactory.getLogger(PartitionQueryEmailer.class);
    private final EmailNotifier emailNotifier;
    private ApplicationConfiguration appConfig;
    protected Set<PartitionEvent> partitionEventSet;
    private Set<String> errors;


    public PartitionQueryEmailer(EmailNotifier emailNotifier, ApplicationConfiguration appConfig) {
        this.emailNotifier = emailNotifier;
        this.appConfig = appConfig;
        init();
    }

    @Override
    public void init() {
        partitionEventSet = new LinkedHashSet<>();
        errors = new HashSet<>();
    }

    @Override
    public void collect(String tableName, RelayerConfiguration relayerConfiguration, DBTYPE dbtype, String query) {
        partitionEventSet.add(new PartitionEvent(tableName, relayerConfiguration, dbtype, query));
    }

    @Override
    public void collectException(String exceptionMessage) {
        this.errors.add(exceptionMessage);
    }

    @AllArgsConstructor
    static class PartitionEvent{
        final String tableName;
        final RelayerConfiguration relayerConfiguration;
        final DBTYPE dbtype;
        final String query;

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof PartitionEvent) {
                PartitionEvent other = (PartitionEvent) obj;
                EqualsBuilder builder = new EqualsBuilder();
                builder.append(this.tableName, other.tableName);
                builder.append(this.dbtype, other.dbtype);
                builder.append(this.query, other.query);
                return builder.isEquals();
            }
            return false;
        }

        @Override
        public int hashCode() {
            HashCodeBuilder builder = new HashCodeBuilder();
            builder.append(tableName);
            builder.append(dbtype);
            builder.append(query);
            return builder.toHashCode();
        }
    }

    @Override
    public void finish() {
        try {
            Splitable.split(this.partitionEventSet, 100)
                    .stream()
                    .forEach(list -> emailNotifier.newNotification(getBody(list), "[CRITICAL] Partition management required for TR: " + appConfig.getAppName(), appConfig.getEmailConfiguration().getToAddress()));

        } catch (RuntimeException runTimeException) {
            logger.error("Email failure:" + runTimeException.getMessage(), runTimeException);
        }
    }

    public String getBody(List<PartitionEvent> partitionEventList) {
        Map<RelayerConfiguration, Map<String, Map<String, List<PartitionEvent>>>> grouping = partitionEventList.stream().collect(
                groupingBy(p -> p.relayerConfiguration,
                        groupingBy(q -> q.dbtype.name(),
                                groupingBy(r -> r.tableName, Collectors.toList()))));
        StringBuilder stringBuilder = new StringBuilder();
        for (RelayerConfiguration r : grouping.keySet()) {
            stringBuilder.append("<br/><u><b>Relayer Name :" + r.getRelayerId() + "</b></u><br/>");
            Map<String, Map<String, List<PartitionEvent>>> dbtypeMap = grouping.get(r);
            for (String dbtype : dbtypeMap.keySet()) {
                stringBuilder.append("<br/><b> Database Type: " + dbtype + "</b><br/>");
                String dBUrl;
                if (dbtype == DBTYPE.TR.name()) {
                    dBUrl = getTRDbHost(appConfig, r);
                } else {
                    dBUrl = getAppDbHost(appConfig, r);
                }
                stringBuilder.append("DB connection URl:" + dBUrl + "<br/>");
                Map<String, List<PartitionEvent>> listMap = dbtypeMap.get(dbtype);
                for (String table : listMap.keySet()) {
                    List<PartitionEvent> partitionEvents = listMap.get(table);
                    stringBuilder.append("<b>Table Name: " + table + "</b><br/>");
                    for (PartitionEvent p : partitionEvents) {
                        stringBuilder.append("<i>" + p.query + ";</i><br/>");
                    }
                }
            }
        }

        if(!errors.isEmpty()){
            stringBuilder.append("<b>errors: </b><br/>");
        }
        for(String error : errors){
            stringBuilder.append(error).append("<br/>");
        }
        return stringBuilder.toString();
    }

    private String getAppDbHost(ApplicationConfiguration config, RelayerConfiguration relayerConfiguration) {
        return config.getMysql().get(relayerConfiguration.getAppDbRef().getId()).getProperty("hibernate.connection.url");
    }

    private String getTRDbHost(ApplicationConfiguration config, RelayerConfiguration relayerConfiguration) {
        return config.getMysql().get(relayerConfiguration.getOutboundDbRef().getId()).getProperty("hibernate.connection.url");
    }

}
