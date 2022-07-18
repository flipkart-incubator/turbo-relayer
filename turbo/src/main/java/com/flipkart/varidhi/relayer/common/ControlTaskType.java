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

package com.flipkart.varidhi.relayer.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/*
 * *
 * Author: abhinavp
 * Date: 04-Aug-2015
 *
 */
public enum ControlTaskType {
    UNSIDELINE_GROUP,
    UNSIDELINE_MESSAGE,
    UNSIDELINE_ALL_UNGROUPED,
    MANAGE_PARTITION;

    @JsonCreator public static ControlTaskType valueOfIgnoreCase(String value) {
        return ControlTaskType.valueOf(value.toUpperCase());
    }

    @Override @JsonValue public String toString() {
        return this.name().toUpperCase();
    }
}
