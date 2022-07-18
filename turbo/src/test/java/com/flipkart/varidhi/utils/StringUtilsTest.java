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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * @author shah.dhruvik
 * @date 16/11/19.
 */

public class StringUtilsTest {

    @Test
    public void formatIds() {
        List<Integer> ids = new ArrayList();
        ids.add(1);
        ids.add(2);
        ids.add(3);
        String formatedIds = StringUtils.formatIds(ids);
        assertEquals("'1','2','3'",formatedIds);
    }

    @Test
    public void loadStreamTest() {
        String stringInput = "String that is to be converted to InputStream.\nWe would ensure that we get the string back.\n";
        String string = stringInput;
        InputStream inputStream = new ByteArrayInputStream(string.getBytes(Charset.forName("UTF-8")));
        try {
            String outputString = StringUtils.loadStream(inputStream);
            assertEquals(stringInput,outputString);
        } catch (Exception e) {
            fail();
        }
    }
}