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

package com.flipkart.restbus.turbo.config;

import com.flipkart.restbus.turbo.shard.ExchangeBasedTurboShardStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.error.YAMLException;
import org.yaml.snakeyaml.representer.Representer;

import java.io.InputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class TurboConfig implements Serializable {
    private static final String HIBERNATE_CONNECTION_PASSWORD = "hibernate.connection.password";
    Boolean multiDbWriteEnabled = false;
    Boolean singleDbWriteEnabled = true;
    Boolean turboOutboundWithoutTrxEnabled = false;
    Properties mysql;
    public boolean sharding;
    String appDbType;
    Map<String, Properties> db_shards;
    public ExchangeBasedTurboShardStrategy.Config queue_shard_strategy;
    private Long payloadLength ;
    private static final Logger logger = LoggerFactory.getLogger(TurboConfig.class);

    public static TurboConfig parseDocument(InputStream yaml) {

        TurboConfig config;
        try {
            Representer representer = new Representer();
            representer.getPropertyUtils().setSkipMissingProperties(true);
            Yaml snake = new Yaml(new Constructor(TurboConfig.class),representer);
            config = (TurboConfig) snake.load(yaml);
        } catch (YAMLException e) {
            e.printStackTrace();
            config = null;
        }

        return config;
    }

    public Boolean isTurboOutboundWithoutTrxEnabled() {
        return turboOutboundWithoutTrxEnabled;
    }

    public void setTurboOutboundWithoutTrxEnabled(Boolean turboOutboundWithoutTrxEnabled) {
        this.turboOutboundWithoutTrxEnabled = turboOutboundWithoutTrxEnabled;
    }

    public Boolean getMultiDbWriteEnabled() {
        return multiDbWriteEnabled;
    }

    public void setMultiDbWriteEnabled(Boolean multiDbWriteEnabled) {
        this.multiDbWriteEnabled = multiDbWriteEnabled;
    }

    public Boolean getSingleDbWriteEnabled() {
        return singleDbWriteEnabled;
    }

    public void setSingleDbWriteEnabled(Boolean singleDbWriteEnabled) {
        this.singleDbWriteEnabled = singleDbWriteEnabled;
    }

    public Properties getMysql() {
        return mysql;
    }

    public void setMysql(Properties mysql) {
        this.mysql = mysql;
    }

    public Properties getDbShard(String dbShard) {
        if (dbShard == null) {
            return null;
        }
        return db_shards.get(dbShard);
    }

    public Map<String, Properties> getDbShards() {
        return db_shards;
    }

    public void setDb_shards(Map<String, Properties> db_shards) {
        this.db_shards = db_shards;
        if (db_shards != null) {
            Map<String, Properties> propertiesMap = new HashMap<String, Properties>();
            for (Map.Entry<String, Properties> entry : db_shards.entrySet()) {
                Properties properties = new Properties();
                properties.putAll(entry.getValue());
                propertiesMap.put(entry.getKey(), properties);
            }
            this.db_shards = propertiesMap;
        }
    }

    public String getAppDbType() {
        return appDbType;
    }

    public void setAppDbType(String appDbType) {
        this.appDbType = appDbType;
    }

    public Long getPayloadLength() {
        return payloadLength;
    }

    public void setPayloadLength(Long payloadLength) {
        this.payloadLength = payloadLength;
    }
}
