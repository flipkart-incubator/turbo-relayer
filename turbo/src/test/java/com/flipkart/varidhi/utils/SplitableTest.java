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

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class SplitableTest {
    @Test
    public void test() {
        List<Integer> integerList = new ArrayList<>();
        for (int i = 1; i <= 1005; i++) {
            integerList.add(i);
        }
        List<List<Integer>> splits = Splitable.split(integerList, 10);
        Assert.assertEquals(101, splits.size());
        Assert.assertEquals(5, splits.get(splits.size()-1).size());
    }

    @Test
    public void testSize(){
        List<Integer> integerList = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            integerList.add(i);
        }
        List<List<Integer>> splits = Splitable.split(integerList, 15);
        Assert.assertEquals(1, splits.size());
    }

    @Test
    public void testEmptyList(){
        List<List<Object>> split = Splitable.split(new ArrayList<>(), 10);
        Assert.assertEquals(1, split.size());
    }
}