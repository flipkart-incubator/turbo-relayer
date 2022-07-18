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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/*
 * *
 * Author: abhinavp
 * Date: 03-Sep-2015
 *
 */
public class MapMetricContainer extends HashMap<String, RelayerMetrics>
    implements RelayerMetricHandleContainer {
    @Override public RelayerMetrics getRelayerMetricsHandle(String namespace) {
        return this.get(namespace);
    }

    @Override public void addRelayerMetrics(String namespace, RelayerMetrics relayerMetrics) {
        this.put(namespace, relayerMetrics);
    }

    @Override public boolean isRelayerMetricsAvailable(String namespace) {
        return this.containsKey(namespace);
    }

    @Override public List<RelayerMetrics> getAllRelayerMetrcis() {
        return new ArrayList<>(this.values());
    }
}
