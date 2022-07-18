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

package com.flipkart.restbus.client.entity;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by harshit.gangal on 16/12/16.
 */
public class TDSOutboundMessage extends OutboundMessage{
    private static final Logger LOGGER = LoggerFactory.getLogger(TDSOutboundMessage.class);

    private String shardKey;

    public TDSOutboundMessage(String shardKey) {
        if(null == shardKey) {
            throw new IllegalArgumentException("Shard Key cannot be null");
        }
        this.shardKey = shardKey;
    }

    public String getShardKey() {
        return shardKey;
    }
}
