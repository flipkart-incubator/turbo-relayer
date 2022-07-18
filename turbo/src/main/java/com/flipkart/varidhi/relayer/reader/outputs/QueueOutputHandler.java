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

package com.flipkart.varidhi.relayer.reader.outputs;

import com.flipkart.varidhi.relayer.reader.models.BaseReadDomain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;

/*
 * *
 * Author: abhinavp
 * Date: 05-Jul-2015
 *
 */
public class QueueOutputHandler<T> extends OutputHandler {
    private static Logger logger = LoggerFactory.getLogger(QueueOutputHandler.class);

    OutputTaskFactory outputTaskFactory;
    BlockingQueue queue;

    public QueueOutputHandler(OutputTaskFactory<T> outputTaskFactory, BlockingQueue<T> queue) {
        this.outputTaskFactory = outputTaskFactory;
        this.queue = queue;
    }

    @Override public void submit(BaseReadDomain readDomain) {
        T task = (T) outputTaskFactory.getTask(readDomain);
        try {
            queue.put(task);
        } catch (InterruptedException | ClassCastException | NullPointerException | IllegalArgumentException e) {
            logger.error(
                "Stopping Relayer: submit:Error in submitting task to reader queue: " + e.getClass()
                    .toString() + " : " + e.getMessage(), e);
        }
    }
}
