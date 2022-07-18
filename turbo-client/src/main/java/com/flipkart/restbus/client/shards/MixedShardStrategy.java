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

import com.flipkart.restbus.client.entity.OutboundMessage;
import org.apache.commons.lang.StringUtils;

/**
 * Created by saurabh.agrawal on 04/09/14.
 */
public class MixedShardStrategy extends OMShardStrategy {

    private TimeBasedOMShardStrategy timeStrategy;
    private ExchangeBasedOMShardStrategy exchangeStrategy;

    public MixedShardStrategy(TimeBasedOMShardStrategy timeStrategy,
                              ExchangeBasedOMShardStrategy exchangeStrategy) {
        this.timeStrategy = timeStrategy;
        this.exchangeStrategy = exchangeStrategy;
    }

    @Override
    public String getShardSuffix(OutboundMessage message) {
        String timeSuffix = timeStrategy.getShardSuffix(message);
        String exchangeSuffix = exchangeStrategy.getShardSuffix(message);

        if (valid(timeSuffix) && valid(exchangeSuffix)) {
            return exchangeSuffix + SHARD_SEP + timeSuffix;
        } else if (valid(exchangeSuffix)) {
            return exchangeSuffix;
        } else {
            return timeSuffix;
        }
    }

    private boolean valid(String s) {
        return StringUtils.isNotBlank(s);
    }
}
