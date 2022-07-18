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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public interface Splitable<T> {
    List<T> split(int groups);

    static <T> List<List<T>> split(Collection<T> collection, int batchSize) {
        ArrayList<T> list= new ArrayList<>(collection);
        int groups = list.size() / batchSize + 1;
        List<List<T>> splits = new ArrayList<>();
        for (int i = 1; i < groups; i++) {
            List<T> subList = list.subList((i - 1) * batchSize, i * batchSize);
            splits.add(subList);
        }
        List<T> subList = list.subList((groups - 1) * batchSize, list.size());
        splits.add(subList);
        return splits;
    }
}
