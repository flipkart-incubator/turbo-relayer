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

import com.flipkart.restbus.client.entity.OutboundMessage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
 * *
 * Author: abhinavp
 * Date: 22-Sep-2015
 *
 */
public class ExchangeBasedTurboShardStrategy extends TurboShardStrategy
{
    private Config config;

    private Map<String, String> patternToCluster;

   public static class Config {

        public String default_suffix;

        public Map<String, List<String>> clusters;
    }

    public ExchangeBasedTurboShardStrategy(Config config) {
        this.config = config;
        generateReverseMapping();
    }

    private void generateReverseMapping() {
        patternToCluster = new HashMap<String, String>();
        if (config != null && config.clusters != null) {
            for (String clusterName : config.clusters.keySet()) {
                List<String> patterns = config.clusters.get(clusterName);
                for (String pattern : patterns) {
                    patternToCluster.put(pattern, clusterName);
                }
            }
        }
    }

    @Override
    public String getShardPrefix(OutboundMessage message) {
        String exchangeName = message.getExchangeName();

        String clusterName = null;
        for (Map.Entry<String, String> entry : patternToCluster.entrySet()) {
            String pattern = entry.getKey();
            String cluster = entry.getValue();
            if (exchangeName.matches(pattern)) {
                clusterName = cluster;
                break;
            }
        }

        // TODO config could be null. NPE possible
        return (clusterName != null) ? clusterName : config.default_suffix;
    }
}
