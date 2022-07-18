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

package com.flipkart.varidhi.relayer.distributor;


import com.flipkart.varidhi.core.RelayerMetrics;
import com.flipkart.varidhi.relayer.distributor.taskExecutors.DistributorTaskExecutor;
import com.flipkart.varidhi.relayer.distributor.taskExecutors.SequentialDistributionTaskExecutor;
import com.flipkart.varidhi.relayer.distributor.tasks.DistributorTask;
import com.flipkart.varidhi.relayer.distributor.tasks.SequentialDistributionTask;

/*
 * *
 * Author: abhinavp
 * Date: 14-Jul-2015
 *
 */
public class DistributorTaskExecutorProvider {

    public static DistributorTaskExecutor getTaskExecutor(DistributorTask distributorTask, RelayerMetrics relayerMetrics) {
        if (distributorTask instanceof SequentialDistributionTask) {
            return new SequentialDistributionTaskExecutor(relayerMetrics);
        } else
            return null;
    }
}
