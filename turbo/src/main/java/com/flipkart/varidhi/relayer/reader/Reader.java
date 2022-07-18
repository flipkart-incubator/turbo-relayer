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

package com.flipkart.varidhi.relayer.reader;

import com.flipkart.varidhi.config.RelayerConfiguration;
import com.flipkart.varidhi.relayer.Relayer;
import com.flipkart.varidhi.relayer.reader.outputs.OutputHandler;
import com.flipkart.varidhi.relayer.reader.repository.ReaderApplicationRepository;
import com.flipkart.varidhi.relayer.reader.repository.ReaderOutboundRepository;
import com.flipkart.varidhi.relayer.reader.taskExecutors.ReaderTaskExecutor;
import com.flipkart.varidhi.relayer.reader.taskExecutors.ReaderTaskExecutorProvider;
import com.flipkart.varidhi.relayer.reader.tasks.ReaderTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;

/*
 * *
 * Author: abhinavp
 * Date: 05-Jul-2015
 *
 */
public class Reader implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(Reader.class);

    String readerId;
    int readerParallelismDegree;
    ReaderTaskExecutor readerTaskExecutor;
    ReaderTask readerTask;
    OutputHandler outputHandler;
    RelayerConfiguration relayerConfiguration;

    public Reader(String readerId, int readerParallelismDegree, ReaderTask readerTask,
                  OutputHandler outputHandler, ReaderApplicationRepository readerApplicationRepository,
                  ReaderOutboundRepository readerOutboundRepository, RelayerConfiguration relayerConfiguration,@NotNull Relayer relayer) {
        this.readerId = readerId;
        this.readerParallelismDegree = readerParallelismDegree;
        this.readerTask = readerTask;
        this.outputHandler = outputHandler;
        this.readerTaskExecutor =
            new ReaderTaskExecutorProvider(readerId, readerParallelismDegree,
                readerApplicationRepository, readerOutboundRepository, outputHandler, relayerConfiguration,
                    relayer).getExecutor(readerTask);
        this.relayerConfiguration = relayerConfiguration;
    }

    @Override public void run() {
        readerTaskExecutor.execute(readerTask);
    }

    public void stop() {
        logger.info("Stopping Reader :" + readerId);
        readerTaskExecutor.stopExecution();
        logger.info("Reader successfully stopped :" + readerId);

    }

    public void halt() {
        logger.info("Halting Reader :" + readerId);
        readerTaskExecutor.haltExecution();
        logger.info("Reader successfully halted :" + readerId);

    }
}
