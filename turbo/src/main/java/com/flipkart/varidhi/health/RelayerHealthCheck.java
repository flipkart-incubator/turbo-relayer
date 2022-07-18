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

package com.flipkart.varidhi.health;

import ru.vyarus.dropwizard.guice.module.installer.feature.health.NamedHealthCheck;


/**
 * @author apoorv.s
 */
public class RelayerHealthCheck extends NamedHealthCheck {

    @Override protected Result check() throws Exception {
        return Result.healthy("Service is up and kickin'!");
    }

    @Override public String getName() {
        // TODO Auto-generated method stub
        return "Relayer Health check";
    }

}		
