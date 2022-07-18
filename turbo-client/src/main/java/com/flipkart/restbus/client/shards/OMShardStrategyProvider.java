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

package com.flipkart.restbus.client.shards;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

/**
 * Created by saurabh.agrawal on 05/09/14.
 */
public class OMShardStrategyProvider {
    private static final Logger logger = LoggerFactory.getLogger(OMShardStrategyProvider.class);

    private static final String TYPE_TIME_BASED = "time_based";
    private static final String TYPE_QUEUE_BASED = "queue_based";
    private static final String TYPE_MIXED = "mixed";

    private static OMShardStrategy strategy = null;

    private static final String DEFAULT_SHARD_CONFIG_FILE_PATH = "/etc/fk-sc-mq/config.yml";

    private static OMShardStrategy createShardStrategy(OMShardConfig config) {
        OMShardStrategy shardStrategy = null;
        if (config != null && config.sharding) {
            String strategyType = StringUtils.defaultIfEmpty(config.sharding_strategy, TYPE_TIME_BASED);

            String timeConfig = StringUtils.defaultIfEmpty(config.time_shard_strategy, TimeBasedOMShardStrategy.WEEKLY);

            if (TYPE_TIME_BASED.equals(strategyType)) {
                shardStrategy = new TimeBasedOMShardStrategy(timeConfig);
            } else if (TYPE_QUEUE_BASED.equals(strategyType)) {
                shardStrategy = new ExchangeBasedOMShardStrategy(config.queue_shard_strategy);
            } else if (TYPE_MIXED.equals(strategyType)) {
                shardStrategy = new MixedShardStrategy(
                        new TimeBasedOMShardStrategy(timeConfig),
                        new ExchangeBasedOMShardStrategy(config.queue_shard_strategy)
                );
            }
        }

        return shardStrategy;
    }

    private static OMShardConfig getShardConfig() {
        OMShardConfig config = null;

        String configFilePath = System.getProperty("SC_MQ_CONFIG");
        if (configFilePath == null || configFilePath.isEmpty())
            configFilePath = DEFAULT_SHARD_CONFIG_FILE_PATH;

        InputStream document = null;
        try {
            document = new FileInputStream(new File(configFilePath));
            config = OMShardConfig.parseDocument(document);
            logger.info("Outbound Message sharding config loaded from " + configFilePath);
        }  catch (FileNotFoundException e) {
            if (DEFAULT_SHARD_CONFIG_FILE_PATH.equals(configFilePath)) {
                logger.debug("Default shard config at " + DEFAULT_SHARD_CONFIG_FILE_PATH + " not found.");
            } else {
                logger.error(configFilePath + " file not found.");
            }
        }

        return config;
    }

    public static OMShardStrategy getStrategy() {
        if (strategy == null) {
            OMShardConfig config = getShardConfig();
            strategy = getStrategy(config);
            logger.info("Using shard strategy: " + strategy);
        }
        return strategy;
    }

    // adding this method to make createShardStrategy() unit testable
    public static OMShardStrategy getStrategy(OMShardConfig config) {
        return createShardStrategy(config);
    }
}
