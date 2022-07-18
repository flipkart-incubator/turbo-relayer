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

package com.flipkart.varidhi.jobs;

import com.flipkart.varidhi.config.ApplicationConfiguration;
import com.flipkart.varidhi.config.HashingAlgorithm;
import com.flipkart.varidhi.config.PartitionMode;
import com.flipkart.varidhi.relayer.InitializeRelayer;
import org.junit.Assert;
import org.junit.Test;

public class ConfigFileLoadTest {
    @Test
    public void test() {
        try {
            // TODO : remove relayer_observer.yml and use test.yml
            ApplicationConfiguration applicationConfiguration = InitializeRelayer.prepareConfiguration("relayer_observer.yml");
            Assert.assertEquals(PartitionMode.OBSERVER, applicationConfiguration.getPartitionConfiguration().getMode());
            Assert.assertEquals(new Long(120000), applicationConfiguration.getPartitionConfiguration().getScheduleJobRetryTime());
            Assert.assertEquals(HashingAlgorithm.JAVA_HASHCODE, applicationConfiguration.getHashingAlgorithm());
        } catch (Throwable throwable) {
            throwable.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testDefaultHashingAlgorithm() {
        try {
            ApplicationConfiguration applicationConfiguration = InitializeRelayer.prepareConfiguration("relayer_observer.yml");
            HashingAlgorithm hashingAlgorithm = applicationConfiguration.getHashingAlgorithm();
            Assert.assertEquals(HashingAlgorithm.JAVA_HASHCODE, hashingAlgorithm);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
            Assert.fail();
        }
    }

}