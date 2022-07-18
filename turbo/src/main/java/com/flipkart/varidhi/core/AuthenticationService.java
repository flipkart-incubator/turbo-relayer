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

import com.flipkart.turbo.config.AuthConfig;
import com.flipkart.turbo.provider.AuthenticationProvider;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

/**
 * Created by rahul.agrawal on 05/07/22.
 */
public class AuthenticationService implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(AuthenticationService.class);

    private final Map<String, AuthenticationProvider> providers = Maps.newHashMap();

    public AuthenticationService(AuthConfig authConfig) {
        try {
            String className = authConfig.getAuthProviderClass();
            if(className != null && !className.isEmpty()) {
                AuthenticationProvider provider = (AuthenticationProvider) Class.forName(className).newInstance();
                providers.put(provider.getAuthMethodName(), provider);
            }
        } catch (Throwable e) {
            logger.warn("Error in Loading a Provider class ", e);
            throw new RuntimeException("Failed to load an authentication provider.", e);
        }
    }

    public void authenticate(AuthConfig authConfig) {
        AuthenticationProvider provider = providers.get(authConfig.getAuthMethodName());
        if (provider != null) {
            provider.authenticate(authConfig);
        }
    }

    @Override
    public void close() throws IOException {
        for (AuthenticationProvider provider : providers.values()) {
            provider.close();
        }
    }
}
