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

import org.hibernate.Session;
import org.junit.Test;

import static org.mockito.Mockito.mock;

public class MonitorDeadlockTest {
    @Test(timeout=10000)
    public void test() throws InterruptedException {
        MonitorDeadlock monitorDeadlock = new MonitorDeadlock("", () -> mock(Session.class), "x", 0L, 0L){
            @Override
            protected void removeDeadlock(Session session) {
            }
        };
        monitorDeadlock.start();
        Thread.sleep(1000);
        monitorDeadlock.stop();
        monitorDeadlock.monitorDeadlockThread.join();
    }
}