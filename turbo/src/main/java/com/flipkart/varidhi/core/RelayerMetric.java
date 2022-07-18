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
import com.flipkart.varidhi.relayer.Relayer;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by manmeet.singh on 11/02/16.
 */

public class RelayerMetric<T> {
    @Getter
    private Histogram histogram;
    private T metric;
    private boolean hasHistogram;
    private Logger logger = LoggerFactory.getLogger(RelayerMetric.class.getCanonicalName());
    private String scope;

    public RelayerMetric(String relayerId, String scope, boolean hasHistogram, boolean hasGauge) {
        this(Relayer.class,"relayer.exchange." + relayerId, scope, hasHistogram, hasGauge);
    }

    public RelayerMetric(String relayerId, String scope, boolean hasGauge) {
        this(Relayer.class,"relayer.exchange." + relayerId, scope, false, hasGauge);
    }

    public RelayerMetric(Class klassOwner, String name, String scope, boolean hasGauge) {
        this(klassOwner,name,scope,false,hasGauge);
    }

    public RelayerMetric(Class klassOwner, String name, String scope, boolean hasHistogram, boolean hasGauge) {
        this.scope = scope;
        this.hasHistogram = hasHistogram;
        if (hasHistogram) {
            histogram = Metrics.newHistogram(klassOwner, name, scope + ".histogram");
        }
        if (hasGauge) {
            Metrics.newGauge(klassOwner, name, scope, new Gauge<T>() {
                @Override
                public T value() {
                    return metric;
                }
            });
        }
    }

    public void updateMetric(T newValue) {
        logger.debug("Publishing value {} for the metric {}", newValue, scope);
        if (hasHistogram) {
            if (newValue instanceof Integer) {
                getHistogram().update((Integer) newValue);
            } else if (newValue instanceof Long) {
                getHistogram().update((Long) newValue);
            }
        }
        metric = newValue;
    }

}
