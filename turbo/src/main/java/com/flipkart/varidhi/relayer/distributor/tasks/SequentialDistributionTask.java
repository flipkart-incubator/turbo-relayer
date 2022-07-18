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

package com.flipkart.varidhi.relayer.distributor.tasks;

import com.flipkart.varidhi.relayer.distributor.tasks.output.DistributorOutput;

import java.util.concurrent.BlockingQueue;

/*
 * *
 * Author: abhinavp
 * Date: 06-Jul-2015
 *
 */
public class SequentialDistributionTask<T> extends DistributorTask {
    BlockingQueue<T> queue;
    DistributorOutput distributorOutput;

    public SequentialDistributionTask(BlockingQueue<T> queue, DistributorOutput distributorOutput) {
        this.queue = queue;
        this.distributorOutput = distributorOutput;
    }

    public BlockingQueue<T> getQueue() {
        return queue;
    }

    public DistributorOutput getDistributorOutput() {
        return distributorOutput;
    }

}
