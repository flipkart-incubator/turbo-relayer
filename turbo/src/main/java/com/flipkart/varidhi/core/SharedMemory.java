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

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/*
 * *
 * Author: abhinavp
 * Date: 03-Jul-2015
 *
 */
public class SharedMemory<K, V> {
    Map<K, V> map = new ConcurrentHashMap<>();


    public int size() {
        return map.size();
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

    public boolean containsKey(K key) {
        return map.containsKey(key);
    }

    public boolean containsValue(V value) {
        return map.containsValue(value);
    }

    public V get(K key) {
        return map.get(key);
    }

    public V put(K key, V value) {
        return map.put(key, value);
    }

    public V remove(K key) {
        return map.remove(key);
    }

    public void putAll(Map m) {
        map.putAll(m);
    }

    public void clear() {
        map.clear();
    }

    public Set keySet() {
        return map.keySet();
    }

    public Collection values() {
        return map.values();
    }

    //    public Set<Entry> entrySet()
    //    {
    //        return null;
    //    }
}
