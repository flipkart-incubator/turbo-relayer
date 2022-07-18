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

package com.flipkart.varidhi.relayer.reader.taskExecutors;


import com.flipkart.varidhi.config.RelayerConfiguration;
import com.flipkart.varidhi.relayer.Relayer;
import com.flipkart.varidhi.relayer.reader.outputs.OutputHandler;
import com.flipkart.varidhi.relayer.reader.repository.ReaderApplicationRepository;
import com.flipkart.varidhi.relayer.reader.repository.ReaderOutboundRepository;
import com.flipkart.varidhi.relayer.reader.tasks.BatchReadMessagesTask;
import com.flipkart.varidhi.relayer.reader.tasks.ReaderTask;

import javax.validation.constraints.NotNull;

/*
 * *
 * Author: abhinavp
 * Date: 14-Jul-2015
 *
 */
public class ReaderTaskExecutorProvider {
    ReaderApplicationRepository readerApplicationRepository;
    ReaderOutboundRepository readerOutboundRepository;
    OutputHandler outputHandler;
    int readerParallelismDegree;
    String readerId;
    RelayerConfiguration relayerConfiguration;
    Relayer relayer;

    public ReaderTaskExecutorProvider(String readerId, int readerParallelismDegree,
                                      ReaderApplicationRepository readerApplicationRepository,
                                      ReaderOutboundRepository readerOutboundRepository, OutputHandler outputHandler,
                                      RelayerConfiguration relayerConfiguration,@NotNull Relayer relayer) {
        this.readerId = readerId;
        this.readerParallelismDegree = readerParallelismDegree;
        this.readerApplicationRepository = readerApplicationRepository;
        this.readerOutboundRepository = readerOutboundRepository;
        this.outputHandler = outputHandler;
        this.relayerConfiguration = relayerConfiguration;
        this.relayer = relayer;
    }

    public ReaderTaskExecutor getExecutor(ReaderTask readerTask) {
        if (readerTask instanceof BatchReadMessagesTask) {
            return new BatchReadTaskExecutor(readerId, readerParallelismDegree,
                    readerApplicationRepository, readerOutboundRepository, outputHandler, relayerConfiguration,relayer);
        } else
            return null;
    }
}
