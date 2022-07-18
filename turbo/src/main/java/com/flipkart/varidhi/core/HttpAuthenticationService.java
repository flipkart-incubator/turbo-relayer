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

import com.flipkart.turbo.config.HttpAuthConfig;
import com.flipkart.turbo.provider.HttpAuthenticationProvider;
import com.flipkart.turbo.tasks.BaseRelayMessageTask;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by rahul.agrawal on 05/07/22.
 */
public class HttpAuthenticationService  implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(AuthenticationService.class);

    private final Map<String, HttpAuthenticationProvider> providers = Maps.newHashMap();

    public HttpAuthenticationService(HttpAuthConfig httpAuthConfig) {
        try {
            if(httpAuthConfig != null && httpAuthConfig.getHttpAuthProviderClass() != null &&
                    !httpAuthConfig.getHttpAuthProviderClass().isEmpty()) {
                HttpAuthenticationProvider provider = (HttpAuthenticationProvider) Class.forName(httpAuthConfig.getHttpAuthProviderClass()).newInstance();
                providers.put(provider.getAuthMethodName(), provider);
            }
        } catch (Throwable e) {
            logger.warn("Error in Loading a Provider class ", e);
            throw new RuntimeException("Failed to load an authentication provider.", e);
        }
    }

    public Map<String, String> getAuthHeaders(String authMethodName, String clientID) throws InterruptedException {
        HttpAuthenticationProvider provider = providers.get(authMethodName);
        if (provider != null) {
            return provider.getAuthHeaders(clientID);
        } else {
            return new HashMap<>();
        }
    }

    public Map<String, String> getDefaultHeaders(String authMethodName, BaseRelayMessageTask relayMessageTask, String appUserName) {
        HttpAuthenticationProvider provider = providers.get(authMethodName);
        if (provider != null) {
            return provider.getDefaultHeaders(relayMessageTask, appUserName);
        } else {
            return new HashMap<>();
        }
    }

    @Override
    public void close() throws IOException {

    }
}
