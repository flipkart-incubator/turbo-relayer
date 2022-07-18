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

package com.flipkart.restbus.turbo.utils;

import com.flipkart.restbus.hibernate.models.TurboOutboundMessageEntity;

import java.util.ArrayList;
import java.util.List;

/*
 * *
 * Author: abhinavp
 * Date: 28-Sep-2015
 *
 */
public class TurboHelper
{
    public static List<Class<?>> getAnnotatedClasses() {
        List<Class<?>> classes = new ArrayList<Class<?>>();
        classes.add(TurboOutboundMessageEntity.class);
        return classes;
    }

    public static boolean isBlank(String str) {
        int strLen;
        if (str != null && (strLen = str.length()) != 0) {
            for(int i = 0; i < strLen; ++i) {
                if (!Character.isWhitespace(str.charAt(i))) {
                    return false;
                }
            }

            return true;
        } else {
            return true;
        }
    }
}
