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

import com.flipkart.restbus.client.shards.Shard;

/*
 * *
 * Author: abhinavp
 * Date: 23-Sep-2015
 *
 */
public class DummyTurboShardStrategy<T> implements DynamicShardStrategy<T>
{
    private static final String OUTBOUND_TABLE_SUFFIX = "messages";

    @Override
    public Shard resolve(String suffix, T entity)
    {
        return new Shard(suffix);
    }

    @Override
    public Shard resolve(T entity)
    {
        return new Shard(OUTBOUND_TABLE_SUFFIX);
    }
    //    private static final String OUTBOUND_TABLE_PREFIX = "messages";
//
//    protected static final String SHARD_SEP = "_";
//
//    private OMShardConfig config;
//
//    public abstract String getShardSuffix(OutboundMessage message);
//
//    public Shard resolve(OutboundMessage message) {
//        String suffix = getShardSuffix(message);
//        return new Shard( suffix + SHARD_SEP + OUTBOUND_TABLE_PREFIX  );
//    }
//
//    public Shard resolve(String prefix, OutboundMessage message) {
//        String suffix = getShardSuffix(message);
//        //return new Shard(prefix + SHARD_SEP + suffix);
//        return new Shard(suffix+SHARD_SEP+prefix);
//    }
}
