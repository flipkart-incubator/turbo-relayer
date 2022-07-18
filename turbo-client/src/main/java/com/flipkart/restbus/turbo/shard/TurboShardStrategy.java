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
import com.flipkart.restbus.client.shards.OMShardConfig;
import com.flipkart.restbus.client.shards.Shard;

/*
 * *
 * Author: abhinavp
 * Date: 22-Sep-2015
 *
 */
public abstract class TurboShardStrategy implements DynamicShardStrategy<OutboundMessage>
{

    private static final String OUTBOUND_TABLE_SUFFIX = "messages";

    protected static final String SHARD_SEP = "_";

    private OMShardConfig config;

    public abstract String getShardPrefix(OutboundMessage message);

    public Shard resolve(OutboundMessage message) {
        String prefix = getShardPrefix(message);
        if(prefix!=null)
            return new Shard( prefix + SHARD_SEP + OUTBOUND_TABLE_SUFFIX  );
        else
            return new Shard(OUTBOUND_TABLE_SUFFIX);
    }

    public Shard resolve(String suffix, OutboundMessage message) {
        String prefix = getShardPrefix(message);
        //return new Shard(prefix + SHARD_SEP + suffix);
        if(prefix!=null)
            return new Shard(prefix+SHARD_SEP+suffix);
        else
            return new Shard(suffix);
    }
}
