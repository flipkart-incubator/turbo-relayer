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

package com.flipkart.turbo.tasks;

import lombok.Getter;
import lombok.Setter;

/*
 * *
 * Author: abhinavp
 * Date: 10-Aug-2015
 *
 */
@Getter @Setter public abstract class BaseGroupBasedSequencingTask extends ProcessorTask {
    protected String groupId;

    protected BaseGroupBasedSequencingTask(String groupId) {
        this.groupId = groupId;
    }

    protected String getAlternateGroupId() {
        return String.valueOf(hashCode());
    }

    public String sequencingKey() {
        // using messageid for ungrouped messages and groupid for grouped messages as a sequencing key.
        // Reason -: Since we are now skipping the messages  whose ids is less than the maxprocessed seq id
        // (situation occurs when the relayer restarts and hashing algo and the number of the threads are not changed).
        // So we wanted to make sure that the mapping of the messages and threads remain same .
        // In the previous version we were using the method hashCode() for the messages that does not have a group id .
        // hashCode() func generates new random value at every new instance of the relayer .
        // Which was the reason for mapping inconsistency .
        if(groupId == null) {
            return String.valueOf(getAlternateGroupId());
        }
        return String.valueOf(groupId);
    }

}
