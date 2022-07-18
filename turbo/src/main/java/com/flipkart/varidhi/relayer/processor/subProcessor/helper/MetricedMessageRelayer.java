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

import com.flipkart.varidhi.config.RelayerConfiguration;
import com.flipkart.turbo.tasks.BaseRelayMessageTask;
import com.ning.http.client.Response;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.TimerContext;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class MetricedMessageRelayer implements MessageRelayer {

    private final MessageRelayer messageRelayer;
    private final Counter serviceErrors;
    private final Counter timeoutErrors;
    private final Counter internalSystemErrors;
    private final com.yammer.metrics.core.Timer responses;

    public MetricedMessageRelayer(String relayerId, MessageRelayer messageRelayer) {
        this.messageRelayer = messageRelayer;
        final String prefix = "relayer.exchange." + relayerId;
        serviceErrors = Metrics.newCounter(MessageRelayer.class, prefix, "error.4XX");
        timeoutErrors = Metrics.newCounter(MessageRelayer.class, prefix, "error.499");
        internalSystemErrors = Metrics.newCounter(MessageRelayer.class, prefix, "error.5XX");
        responses = Metrics.newTimer(MessageRelayer.class, prefix, "varidhi.response_times");
    }

    @Override
    public Response relayMessage(BaseRelayMessageTask message) throws Exception {
        Response response;
        final TimerContext timerContext = responses.time();
        try {
            response = this.messageRelayer.relayMessage(message);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TimeoutException) {
                timeoutErrors.inc();
            }
            throw e;
        } finally {
            timerContext.stop();
        }
        if (response.getStatusCode() / 100 == 4) {
            serviceErrors.inc();
        } else if (response.getStatusCode() / 100 == 5) {
            internalSystemErrors.inc();
        }
        return response;
    }

    @Override
    public RelayerConfiguration getRelayerConfiguration(){
        return this.messageRelayer.getRelayerConfiguration();
    }

}
