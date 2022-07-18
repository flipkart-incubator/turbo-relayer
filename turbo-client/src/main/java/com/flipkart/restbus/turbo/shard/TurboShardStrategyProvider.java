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

package com.flipkart.restbus.turbo.shard;

import com.flipkart.restbus.turbo.config.TurboConfig;
import com.flipkart.restbus.turbo.config.TurboConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * *
 * Author: abhinavp
 * Date: 21-Sep-2015
 *
 */
public class TurboShardStrategyProvider
{
    private static final Logger logger = LoggerFactory.getLogger(TurboShardStrategyProvider.class);

    //private static final String TYPE_TIME_BASED = "time_based";
    private static final String TYPE_QUEUE_BASED = "queue_based";
   // private static final String TYPE_MIXED = "mixed";

    private static TurboShardStrategy strategy = null;

   // private static final String DEFAULT_SHARD_CONFIG_FILE_PATH = "/etc/fk-sc-mq/config.yml";

    private static TurboShardStrategy createShardStrategy(TurboConfig config) {    // Only queue based sharding supported.
        TurboShardStrategy shardStrategy = null;
        if (config != null && config.sharding) {
          //  String strategyType = StringUtils.defaultIfEmpty(config.sharding_strategy, TYPE_QUEUE_BASED);
            if(config.queue_shard_strategy!=null)
                shardStrategy = new ExchangeBasedTurboShardStrategy(config.queue_shard_strategy);

        }

        return shardStrategy;
    }


    public static TurboShardStrategy getStrategy() {
        if (strategy == null) {
        //    OMShardConfig config = getShardConfig();
            TurboConfig turboConfig = TurboConfigProvider.getConfig();
            strategy = getStrategy(turboConfig);
            logger.info("Using shard strategy: " + strategy);
        }
        return strategy;
    }

    // adding this method to make createShardStrategy() unit testable
    public static TurboShardStrategy getStrategy(TurboConfig config) {
        return createShardStrategy(config);
    }
}
