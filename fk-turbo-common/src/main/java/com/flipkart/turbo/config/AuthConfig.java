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

package com.flipkart.turbo.config;import lombok.Getter;
import lombok.Setter;
import org.eclipse.jetty.util.StringUtil;

import javax.validation.constraints.NotNull;

/**
 * @author shah.dhruvik
 * @date 11/10/19.
 */

@Getter
public class AuthConfig {
    @NotNull
    private String authnUrl;
    @NotNull
    private String clientId;
    @NotNull
    private String clientSecret;
    private String authProviderClass;
    private String authMethodName;

    public boolean isValid(){
        if(authnUrl == null || clientId == null || clientSecret == null || authProviderClass == null){
            return false;
        }
        return !(StringUtil.isBlank(String.valueOf(authnUrl)) ||
                StringUtil.isBlank(String.valueOf(clientId)) ||
                StringUtil.isBlank(String.valueOf(clientSecret)) ||
                StringUtil.isBlank(String.valueOf(authProviderClass)));
    }
}
