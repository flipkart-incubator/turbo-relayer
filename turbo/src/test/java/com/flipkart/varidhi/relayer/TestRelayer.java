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

package com.flipkart.varidhi.relayer;

import com.flipkart.varidhi.RelayerModule;
import com.flipkart.varidhi.config.ApplicationConfiguration;
import com.flipkart.varidhi.config.RelayerConfiguration;
import com.flipkart.varidhi.core.SessionFactoryContainer;
import com.flipkart.turbo.tasks.ProcessorTask;
import com.flipkart.varidhi.relayer.reader.MessageReader;
import com.flipkart.varidhi.relayer.reader.outputs.OutputHandler;
import com.flipkart.varidhi.relayer.reader.outputs.QueueOutputHandler;
import com.flipkart.varidhi.relayer.reader.repository.ReaderApplicationRepository;
import com.flipkart.varidhi.relayer.reader.repository.ReaderOutboundRepository;
import com.flipkart.varidhi.relayer.reader.taskExecutors.BatchReadTaskExecutor;
import com.flipkart.varidhi.repository.ApplicationRepositoryImpl;
import com.flipkart.varidhi.repository.ExchangeTableNameProvider;
import com.flipkart.varidhi.repository.OutboundRepositoryImpl;
import com.flipkart.varidhi.utils.DBBaseTest;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.mockito.Mockito.mock;

/**
 * Created by harshit.gangal on 29/09/16.
 */
public class TestRelayer extends DBBaseTest {

    BatchReadTaskExecutor readTaskExecutor;
    @Before
    public void setUp() {
        ApplicationConfiguration configuration = InitializeRelayer.getApplicationConfigurationFromResourceFile("test.yml");
        RelayerModule relayerModule = new RelayerModule();
        SessionFactoryContainer sessionFactoryContainer =
            relayerModule.provideSessionFactoryContainer(configuration);
        ExchangeTableNameProvider exchangeTableNameProvider =
            new ExchangeTableNameProvider(configuration.getRelayers().get(0).getName());

        ReaderApplicationRepository readerApplicationRepository = new ApplicationRepositoryImpl(
            sessionFactoryContainer
                .getSessionFactory(configuration.getRelayers().get(0).getAppDbRef().getId()),
            exchangeTableNameProvider);
        ReaderOutboundRepository readerOutboundRepository = new OutboundRepositoryImpl(
            sessionFactoryContainer
                .getSessionFactory(configuration.getRelayers().get(0).getOutboundDbRef().getId()),
            exchangeTableNameProvider);

        BlockingQueue<ProcessorTask> mainQueue = new LinkedBlockingQueue<ProcessorTask>(
            configuration.getRelayers().get(0).getReaderMainQueueSize());
        OutputHandler outputHandler = new QueueOutputHandler<ProcessorTask>(
            new ProcessorTaskFactory(configuration.getRelayers().get(0).getDestinationServer(), null),
            mainQueue);
        RelayerConfiguration relayerConfiguration = new RelayerConfiguration();
        relayerConfiguration.setTurboReadMode(null);
        relayerConfiguration.setReaderSleepTime(1000);
        relayerConfiguration.setPreventReRelayOnStartUp(true);
        Relayer relayer = mock(Relayer.class);
        readTaskExecutor = new BatchReadTaskExecutor("topic", 10, readerApplicationRepository,
            readerOutboundRepository, outputHandler, relayerConfiguration, relayer);
    }

    @Test
    public void testRelayer() throws Exception {
        Field field = readTaskExecutor.getClass().getDeclaredField("messageReader");
        field.setAccessible(true);
        MessageReader messageReader = (MessageReader) field.get(readTaskExecutor);
        messageReader.getOutboundMessagesInParallel(1L, 5,0);
    }

}
