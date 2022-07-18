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

package com.flipkart.varidhi.core;

import org.slf4j.Logger;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/*
 * *
 * Author: abhinavp
 * Date: 21-Jun-2015
 *
 */
public class ThreadPoolExecutorProvider {
    // final static AtomicLong counter = new AtomicLong();
    // final static AtomicLong executorCounter = new AtomicLong();

    public static ThreadPoolExecutor provideExecutor(final String threadGroup,
        final String executorGroup, int corePoolSize, int maxPoolSize, long keepAlivetime,
        int queueSize, RejectedExecutionHandler rejectedExecutionHandler, final Logger logger) {
        ThreadPoolExecutor threadPoolExecutor =
            new ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAlivetime, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>(queueSize), new ThreadFactory() {

                ThreadGroup group = new ThreadGroup(threadGroup);

                @Override public Thread newThread(Runnable runnable) {
                    return new Thread(group, runnable, executorGroup + "_" + threadGroup);
                }
            }, rejectedExecutionHandler) {
                @Override protected void afterExecute(Runnable r, Throwable t) {
                    super.afterExecute(r, t);
                    if (t == null && r instanceof Future<?>) {
                        try {
                            ((Future<?>) r).get();
                        } catch (CancellationException ce) {
                            t = ce;
                        } catch (ExecutionException ee) {
                            t = ee.getCause();
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt(); // ignore/reset
                        }
                    }
                    if (t != null) {
                        logger.error("Stopping Relayer::System.exit::Error in ThreadPoolExecutor: " + t.toString() + " ->" + t.getMessage(), t);
                        System.exit(-1);
                    }
                }
            };
        threadPoolExecutor.prestartAllCoreThreads();
        return threadPoolExecutor;
    }
}
