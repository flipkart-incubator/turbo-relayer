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
import com.flipkart.varidhi.relayer.reader.outputs.OutputHandler;
import com.flipkart.varidhi.relayer.reader.repository.ReaderApplicationRepository;
import com.flipkart.varidhi.relayer.reader.repository.ReaderOutboundRepository;

import java.util.concurrent.ThreadPoolExecutor;


public class ReaderFactory {

    public static MessageReader getMessageReader(TurboReadMode readMode, String executorId, int readerMaxParallelismDegree,
                                                 ReaderApplicationRepository readerApplicationRepository, ReaderOutboundRepository readerOutboundRepository,
                                                 OutputHandler outputHandler, ThreadPoolExecutor readerExecutor, RelayerConfiguration relayerConfiguration){
        switch (readMode){
            case OUTBOUND_READER:
                return new OutboundMessageReader(executorId, readerMaxParallelismDegree,
                 readerOutboundRepository, outputHandler, readerExecutor,relayerConfiguration);
            case SEQUENCE_READER:
                return new SequenceMessageReader(executorId, readerMaxParallelismDegree,
                    readerApplicationRepository, readerOutboundRepository, outputHandler, readerExecutor,relayerConfiguration);
            default:
                return new DefaultMessageReader(executorId, readerMaxParallelismDegree,
                        readerApplicationRepository, readerOutboundRepository, outputHandler, readerExecutor,relayerConfiguration);

        }
    }


}
