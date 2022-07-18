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

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.error.YAMLException;

import java.io.InputStream;

/**
 * Created by saurabh.agrawal on 04/09/14.
 */
public class OMShardConfig {

    public boolean sharding;

    public String sharding_strategy;

    public String time_shard_strategy;

    public ExchangeBasedOMShardStrategy.Config queue_shard_strategy;

    public static OMShardConfig parseDocument(InputStream yaml) {

        OMShardConfig config;

        try {
            Yaml snake = new Yaml(new Constructor(OMShardConfig.class));
            config = (OMShardConfig) snake.load(yaml);
        } catch (YAMLException e) {
            config = null;
        }

        return config;
    }
}
