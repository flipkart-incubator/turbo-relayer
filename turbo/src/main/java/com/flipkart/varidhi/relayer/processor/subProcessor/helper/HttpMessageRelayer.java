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

import com.fasterxml.jackson.core.type.TypeReference;
import com.flipkart.turbo.config.HttpAuthConfig;
import com.flipkart.varidhi.config.MockConfig;
import com.flipkart.varidhi.config.RelayerConfiguration;
import com.flipkart.varidhi.core.HttpAuthenticationService;
import com.flipkart.varidhi.core.utils.HttpClient;
import com.flipkart.varidhi.core.utils.JsonUtil;
import com.flipkart.varidhi.relayer.common.Exceptions.InsufficientRelayerParametersException;
import com.flipkart.turbo.tasks.BaseRelayMessageTask;
import com.flipkart.varidhi.utils.LoggerUtil;
import com.ning.http.client.Response;
import com.ning.http.client.providers.netty.NettyResponse;
import com.ning.http.client.providers.netty.ResponseStatus;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static com.flipkart.varidhi.utils.Constants.MOCK_HEADER;

/*
 * *
 * Author: abhinavp
 * Date: 03-Aug-2015
 *
 */
@Getter
public class HttpMessageRelayer implements MessageRelayer {

    private String appUserName;
    private boolean enableCustomRelay;
    private RelayerConfiguration relayerConfiguration;
    private MockConfig mockConfig;
    private HttpAuthConfig httpAuthConfig;
    private HttpAuthenticationService httpAuthenticationService;
    private static Logger logger = LoggerFactory.getLogger(HttpMessageRelayer.class);

    public HttpMessageRelayer(String appUserName, boolean enableCustomRelay,
                              @NotNull RelayerConfiguration relayerConfiguration, MockConfig mockConfig,
                              HttpAuthConfig httpAuthConfig,
                              HttpAuthenticationService httpAuthenticationService
    ) {
        this.appUserName = appUserName;
        this.enableCustomRelay = enableCustomRelay;
        this.relayerConfiguration = relayerConfiguration;
        this.mockConfig = mockConfig;
        this.httpAuthConfig = httpAuthConfig;
        this.httpAuthenticationService = httpAuthenticationService;
    }

    private Map<String, String> createHeaders(BaseRelayMessageTask relayMessageTask, String appUserName)
            throws IOException {

        Map<String, String> headers = httpAuthenticationService.getDefaultHeaders(httpAuthConfig.getHttpAuthMethodName(), relayMessageTask, appUserName);
        if (relayMessageTask.getCustomHeaders() == null || relayMessageTask.getCustomHeaders()
                .equals("{}"))
            return headers;
        else {
            Map<String, String> customHeaders = JsonUtil.deserializeJson(relayMessageTask.getCustomHeaders(),
                    new TypeReference<HashMap<String, String>>() {
                    });
            headers.putAll(customHeaders);
        }
        return headers;
    }

    @Override
    public Response relayMessage(BaseRelayMessageTask relayMessageTask) throws Exception {
        String uri;

        Map<String, String> headers = createHeaders(relayMessageTask, this.appUserName);
        if (enableCustomRelay) {
            if (StringUtils.isEmpty(relayMessageTask.getHttpUri())) {
                logger.error(LoggerUtil.generateLogInLogSvcFormat(relayMessageTask.getMessageId(), relayMessageTask.getGroupId()
                        , "Relaying", "Stopped", relayMessageTask.getMessageData()));
                throw new InsufficientRelayerParametersException("http_uri can't be null when custom relay is enabled");
            }
            uri = relayMessageTask.getHttpUri();
        } else {
            uri = relayMessageTask.getDestinationServer() + relayMessageTask.getExchangeType() + "s/"
                    + relayMessageTask.getExchangeName() + "/messages";

            try {
                headers.putAll(httpAuthenticationService.getAuthHeaders(httpAuthConfig.getHttpAuthMethodName(), relayerConfiguration.getVaradhiAuthTargetClientID()));
            } catch (Exception e) {
                logger.error("System.Exit:: Error while fetching Auth Token for messageID: " + relayMessageTask.getMessageId() + ". Exiting relayer.",e);
                System.exit(-1);
            }

        }
        if (isMockingRequest(headers)) {
            logger.warn(LoggerUtil.generateLogInLogSvcFormat(relayMessageTask.getMessageId(), relayMessageTask.getGroupId()
                    , "Mocking", "Mocked", relayMessageTask.getMessageData()));
            Thread.sleep(mockConfig.getAvgLatencyInMillis());
            return getMockedResponse(uri);
        }
        if (relayerConfiguration.isEnableCustomRelay()) {
            return HttpClient.INSTANCE.executeRequest(relayMessageTask.getHttpMethod(), uri, relayMessageTask.getMessageData(), headers, this.getRelayerConfiguration().getCharacterEncoding());
        }
        return HttpClient.INSTANCE.executePost(uri, relayMessageTask.getMessageData(), headers, this.getRelayerConfiguration().getCharacterEncoding());
    }

    @Override
    public RelayerConfiguration getRelayerConfiguration() {
        return relayerConfiguration;
    }

    private boolean isMockingRequest(Map<String, String> headers) {
        return headers.containsKey(MOCK_HEADER) && "true".equalsIgnoreCase(headers.get(MOCK_HEADER));
    }

    private Response getMockedResponse(String uri) throws URISyntaxException {
        double num = Math.random();
        if (num < mockConfig.getErrorPercent()) {
            return getResponseForStatus(uri, HttpResponseStatus.INTERNAL_SERVER_ERROR);
        }
        return getResponseForStatus(uri, HttpResponseStatus.OK);
    }

    private Response getResponseForStatus(String uri, HttpResponseStatus responseStatus) throws URISyntaxException {
        return new NettyResponse(new ResponseStatus(new URI(uri),
                new DefaultHttpResponse(new HttpVersion("HTTP/1.0"), responseStatus), null), null, new ArrayList<>());
    }

}
