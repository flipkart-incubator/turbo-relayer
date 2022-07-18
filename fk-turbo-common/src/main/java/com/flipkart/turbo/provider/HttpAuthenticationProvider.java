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

package com.flipkart.turbo.provider;

import com.flipkart.turbo.tasks.BaseRelayMessageTask;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

/**
 * Created by rahul.agrawal on 05/07/22.
 */
public interface HttpAuthenticationProvider extends Closeable{
    String getAuthMethodName();
    Map<String,String> getAuthHeaders(String clientID) throws InterruptedException;

    void close() throws IOException;

    Map<String,String> getDefaultHeaders(BaseRelayMessageTask relayMessageTask, String appUserName);

    Map<String,String> getDefaultHeaders();
}
