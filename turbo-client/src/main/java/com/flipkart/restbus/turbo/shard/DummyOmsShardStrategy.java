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
import com.flipkart.restbus.client.shards.ShardStrategy;

/*
 * *
 * Author: abhinavp
 * Date: 23-Sep-2015
 *
 */
public class DummyOmsShardStrategy<T> implements ShardStrategy<T>
{
    private static final String OUTBOUND_TABLE_PREFIX = "outbound_messages";

    @Override
    public Shard resolve(T entity)
    {
        return new Shard(OUTBOUND_TABLE_PREFIX);
    }
}
