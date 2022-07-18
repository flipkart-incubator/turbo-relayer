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

package com.flipkart.varidhi.relayer.processor.subProcessor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/*
 * *
 * Author: abhinavp
 * Date: 30-Jun-2015
 *
 */
public class SubProcessorWaitPolicy implements RejectedExecutionHandler {
    private final long time;
    private final TimeUnit timeUnit;
    String namespace;
    Logger logger;
        // = LoggerFactory.getLogger(SubProcessorWaitPolicy.class.getCanonicalName() + " " + namespace);

    public SubProcessorWaitPolicy(String namespace) {
        this(namespace, 1, TimeUnit.SECONDS);
    }

    public SubProcessorWaitPolicy(String namespace, long time, TimeUnit timeUnit) {
        super();
        this.time = (time < 0 ? Long.MAX_VALUE : time);
        this.timeUnit = timeUnit;
        this.namespace = namespace;
        logger = LoggerFactory
            .getLogger(SubProcessorWaitPolicy.class.getCanonicalName() + " " + namespace);

    }

    @Override public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
        try {
            logger.info("Queue full. Stopping distribution");
            while (!e.isShutdown() && !e.getQueue().offer(r, time, timeUnit)) {
                Thread.sleep(100);
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RejectedExecutionException(ie);
        }
    }


}
