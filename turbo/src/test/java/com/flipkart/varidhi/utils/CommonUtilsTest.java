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

package com.flipkart.varidhi.utils;

import org.junit.Test;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.junit.Assert.*;

public class CommonUtilsTest {

    public static final long HOUR = 3600*1000;
    public static final long MINUTE = 60 *1000;

    @Test
    /**
     * check generated cron on https://www.freeformatter.com/cron-expression-generator-quartz.html
     */
    public void convertUnixToQuartzCron() {
        try {
            CommonUtils.convertUnixToQuartzCron(null);
            fail();
        } catch (NullPointerException e){}

        try {
            CommonUtils.convertUnixToQuartzCron("");
            fail();
        } catch (IllegalArgumentException e){}

        assertEquals("0 10 * * * ? *",CommonUtils.convertUnixToQuartzCron("10 * * * *"));
        assertEquals("0 * * * * ? *",CommonUtils.convertUnixToQuartzCron("* * * * *"));
        assertEquals("0 */2 * * * ? *",CommonUtils.convertUnixToQuartzCron("*/2 * * * *"));

    }

    @Test
    public void getFutureInitialDelay() {
        DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
        long initialDelay = CommonUtils.getInitialDelay(dateFormat.format(new Date().getTime() + HOUR));
        assertTrue(initialDelay > (58 * MINUTE) && initialDelay < (62 * MINUTE));
    }

    @Test
    public void getPastInitialDelay() {
        DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
        long initialDelay = CommonUtils.getInitialDelay(dateFormat.format(new Date().getTime() - HOUR));
        assertTrue(initialDelay > (22 * HOUR + 58 * MINUTE) && initialDelay < (23 * HOUR + 2 * MINUTE));
    }

    @Test
    public void getFuturenextExecution() {
        DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
        Date nextExecution = CommonUtils.getNextExecution(dateFormat.format(new Date().getTime() + HOUR));
        assertTrue(nextExecution.after(new Date(new Date().getTime() + 58 * MINUTE)) &&
                nextExecution.before(new Date(new Date().getTime() + 62 * MINUTE)));
    }

    @Test
    public void getPastnextExecution() {
        DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
        Date nextExecution = CommonUtils.getNextExecution(dateFormat.format(new Date().getTime() - HOUR));
        assertTrue(nextExecution.after(new Date(new Date().getTime() + 22 * HOUR + 58 * MINUTE)) &&
                nextExecution.before(new Date(new Date().getTime() + 23 * HOUR + 2 * MINUTE)));
    }

}