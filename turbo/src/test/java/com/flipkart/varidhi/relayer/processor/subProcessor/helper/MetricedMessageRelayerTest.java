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

package com.flipkart.varidhi.relayer.processor.subProcessor.helper;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.flipkart.varidhi.config.RelayerConfiguration;
import com.flipkart.turbo.tasks.BaseRelayMessageTask;
import com.google.common.collect.ImmutableList;
import com.ning.http.client.Response;

import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MetricedMessageRelayerTest {
    static ImmutableList<Integer> codes = ImmutableList.of(200, 400, 500);

    public static void main(String args[]) throws Exception {
        MessageRelayer mRelayer = new MessageRelayer() {
            @Override
            public Response relayMessage(BaseRelayMessageTask relayMessageTask) throws Exception {
                Response response = mock(Response.class);
                int randomIndex = (int) (Math.random() * codes.size());
                when(response.getStatusCode()).thenReturn(codes.get(randomIndex) + ((int) (Math.random() * 100)));
                Thread.sleep((long) (Math.random() * 100));
                return response;
            }

            @Override
            public RelayerConfiguration getRelayerConfiguration() {
                return null;
            }
        };
        MetricRegistry registry = new MetricRegistry();
        final ConsoleReporter reporter = ConsoleReporter.forRegistry(registry)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.start(1, TimeUnit.SECONDS);
        MetricedMessageRelayer slayer = new MetricedMessageRelayer("slayer", mRelayer);
        IntStream.iterate(1, i -> i++).forEach(i -> {
            try {
                slayer.relayMessage(mock(BaseRelayMessageTask.class));
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        });
    }

}