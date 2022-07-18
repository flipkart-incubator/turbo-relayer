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

package com.flipkart.restbus.hibernate.client;

import com.flipkart.restbus.client.entity.OutboundMessage;
import com.flipkart.restbus.client.shards.OMShardStrategyProvider;
import com.flipkart.restbus.client.shards.ShardStrategy;
import com.flipkart.restbus.hibernate.utils.Constants;
import com.flipkart.restbus.turbo.config.TurboConfigProvider;
import com.flipkart.restbus.turbo.shard.DummyOmsShardStrategy;
import com.flipkart.restbus.turbo.shard.DummyTurboShardStrategy;
import com.flipkart.restbus.turbo.shard.DynamicShardStrategy;
import com.flipkart.restbus.turbo.shard.TurboShardStrategyProvider;
import org.hibernate.Session;

public class OutboundMessageRepositoryFactory {

    /**
     * Get OutboundMessageRepository based on app session
     * @param session
     * @return
     */
    public static OutboundMessageRepository getOutboundMessageRepository(Session session) {
        if(session == null){
            throw new RuntimeException("Session can't be null while getting outboundMessageRepository");
        }
        return prepareOutboundMessageRepository(session);
    }

    /**
     * This method returns a repository which only writes to turbo outbound table
     * @return OutboundMessageRepository
     */
    public static OutboundMessageRepository getOutboundMessageRepositoryWithoutTrx(){
        return prepareOutboundMessageRepository(null);
    }

    /**
     * Get TDSOutboundMessageRepository based on app session
     * @param session
     * @return
     */
    public static TDSOutboundMessageRepository getTDSOutboundMessageRepository(Session session) {
        if(session == null){
            throw new RuntimeException("Session can't be null while getting outboundMessageRepository");
        }
        return prepareTDSOutboundMessageRepository(session);
    }

    /**
     * This method returns a repository which only writes to turbo outbound table
     * @return TDSOutboundMessageRepository
     */
    public static TDSOutboundMessageRepository getTDSOutboundMessageRepositoryWithoutTrx(){
        return prepareTDSOutboundMessageRepository(null);
    }

    private static OutboundMessageRepository prepareOutboundMessageRepository(Session session) {

        ShardStrategy<OutboundMessage> shardStrategy = OMShardStrategyProvider.getStrategy();
        Boolean isMultiDbWriteEnabled = TurboConfigProvider.getConfig()
            .getMultiDbWriteEnabled();  // For Turbo. Sharding is not supported.
        Boolean isSingleDbWriteEnabled = TurboConfigProvider.getConfig()
            .getSingleDbWriteEnabled();  // For Turbo. Sharding is not supported.
        boolean turboOutboundWithoutTrxEnabled = TurboConfigProvider.getConfig().isTurboOutboundWithoutTrxEnabled() == null ? false :
                                        TurboConfigProvider.getConfig().isTurboOutboundWithoutTrxEnabled();

        Boolean appOnTDS = Constants.APP_DB_TYPE_TDS
            .equalsIgnoreCase(TurboConfigProvider.getConfig().getAppDbType());


        if (isMultiDbWriteEnabled) {
            DynamicShardStrategy<OutboundMessage> dynamicShardStrategy =
                TurboShardStrategyProvider.getStrategy();

            if (shardStrategy == null && dynamicShardStrategy == null) {
                if(appOnTDS){
                    throw new IllegalArgumentException("use prepareTDSOutboundMessageRepository to construct repository");
                }
                return new TurboHibernateOutboundMessageRepository(session, true, isSingleDbWriteEnabled, turboOutboundWithoutTrxEnabled);
            } else {
                if (shardStrategy == null)
                    shardStrategy = new DummyOmsShardStrategy<OutboundMessage>();
                if (dynamicShardStrategy == null)
                    dynamicShardStrategy = new DummyTurboShardStrategy<OutboundMessage>();
                if(appOnTDS){
                    throw new IllegalArgumentException("use prepareTDSOutboundMessageRepository to construct repository");
                }
                return new TurboHibernateOutboundMessageShardRepository(session, shardStrategy,
                        dynamicShardStrategy, true, isSingleDbWriteEnabled,turboOutboundWithoutTrxEnabled);
            }
        } else {
            if (shardStrategy == null)
                return new HibernateOutboundMessageRepository(session);
            return new HibernateOutboundMessageShardRepository(session, shardStrategy);
        }
    }

    private static TDSOutboundMessageRepository prepareTDSOutboundMessageRepository(Session session) {

        ShardStrategy<OutboundMessage> shardStrategy = OMShardStrategyProvider.getStrategy();
        Boolean isMultiDbWriteEnabled = TurboConfigProvider.getConfig()
                .getMultiDbWriteEnabled();  // For Turbo. Sharding is not supported.
        Boolean isSingleDbWriteEnabled = TurboConfigProvider.getConfig()
                .getSingleDbWriteEnabled();  // For Turbo. Sharding is not supported.
        boolean turboOutboundWithoutTrxEnabled = TurboConfigProvider.getConfig().isTurboOutboundWithoutTrxEnabled() == null ? false :
                TurboConfigProvider.getConfig().isTurboOutboundWithoutTrxEnabled();

        Boolean appOnTDS = Constants.APP_DB_TYPE_TDS
                .equalsIgnoreCase(TurboConfigProvider.getConfig().getAppDbType());
        if(!appOnTDS){
            throw new IllegalArgumentException("use prepareOutboundMessageRepository to construct repository");
        }

        if (isMultiDbWriteEnabled) {
            DynamicShardStrategy<OutboundMessage> dynamicShardStrategy =
                    TurboShardStrategyProvider.getStrategy();

            if (shardStrategy == null && dynamicShardStrategy == null) {
                return new TDSTurboHibernateOutboundMessageRepository(session, true, isSingleDbWriteEnabled, turboOutboundWithoutTrxEnabled);
            } else {
                if (shardStrategy == null)
                    shardStrategy = new DummyOmsShardStrategy<OutboundMessage>();
                if (dynamicShardStrategy == null)
                    dynamicShardStrategy = new DummyTurboShardStrategy<OutboundMessage>();
                    return new TDSTurboHibernateOutboundMessageShardRepository(session, true, isSingleDbWriteEnabled, turboOutboundWithoutTrxEnabled,shardStrategy,
                            dynamicShardStrategy);
            }
        } else {
            throw new IllegalArgumentException("use prepareOutboundMessageRepository to construct repository");
        }
    }
}
