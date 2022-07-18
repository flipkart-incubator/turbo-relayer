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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by saurabh.agrawal on 04/09/14.
 */
public class TimeBasedOMShardStrategy extends OMShardStrategy {
    public static final String HOURLY = "hourly";
    public static final String DAILY = "daily";
    public static final String WEEKLY = "weekly";
    public static final String MONTHLY = "monthly";

    private Frequency frequency;

    public TimeBasedOMShardStrategy(String config) {
        if (HOURLY.equals(config))
            frequency = FrequencyBuilder.build(FrequencyBuilder.HOURLY);
        else if (DAILY.equals(config))
            frequency = FrequencyBuilder.build(FrequencyBuilder.DAILY);
        else if (WEEKLY.equals(config))
            frequency = FrequencyBuilder.build(FrequencyBuilder.WEEKLY);
        else if (MONTHLY.equals(config))
            frequency = FrequencyBuilder.build(FrequencyBuilder.MONTHLY);
        else
            throw new IllegalArgumentException(String.format("TimeBasedOMShardStrategy config '%s' is invalid", config));
    }

    @Override
    public String getShardSuffix(OutboundMessage message) {
        return frequency.getSuffix(message.getCreatedAt());
    }

    public static class Frequency {
        private String prefix;
        private String pattern;
        private DateFormat dateFormat;

        Frequency(String prefix, String pattern) {
            this.prefix = prefix;
            this.pattern = pattern;
            this.dateFormat = new SimpleDateFormat(pattern);
        }

        public String getSuffix(Date date) {
            return prefix + SHARD_SEP + dateFormat.format(date);
        }
    }

    public static class FrequencyBuilder {
        public static final int HOURLY = 0;
        public static final int DAILY = 1;
        public static final int WEEKLY = 2;
        public static final int MONTHLY = 3;

        public static Frequency build(int frequency) {
            String prefix;
            String dateFormat;

            switch (frequency) {
                case HOURLY:
                    dateFormat = "yyyyMMddHH";
                    prefix = "h";
                    break;
                case DAILY:
                    dateFormat = "yyyyMMdd";
                    prefix = "d";
                    break;
                case MONTHLY:
                    dateFormat = "yyyyMM";
                    prefix = "m";
                    break;
                case WEEKLY:
                default:
                    dateFormat = "yyyyww";
                    prefix = "w";
                    break;
            }

            return new Frequency(prefix, dateFormat);
        }
    }
}
