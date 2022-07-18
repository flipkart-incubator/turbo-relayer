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

package com.flipkart.varidhi.core.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/*
 * *
 * Author: abhinavp
 * Date: 22-Jan-2014
 *
 */
public class DateHelper {
    public static final SimpleDateFormatFactory dateFormatFactory = new SimpleDateFormatFactory();

    public static Date getFormattedDate(String dateStr, DateFormat originalFormat)
        throws ParseException {
        return originalFormat.parse(dateStr);
    }

    /**
     * Returns the formatted date with default format which is yyyy-MM-dd HH:mm:ss
     *
     * @param dateStr date string in the format yyyy-MM-dd HH:mm:ss
     * @return date object
     */
    public static Date getFormattedDate(String dateStr) throws ParseException {
        return getFormattedDate(dateStr, dateFormatFactory.get("yyyy-MM-dd HH:mm:ss"));
    }

    public static int difference(Date d1, Date d2, TimeUnit timeUnit) {
        if (null == d1 || null == d2) {
            return 0;
        }
        long millis = d1.getTime() - d2.getTime();
        return (int) timeUnit.convert(millis, TimeUnit.MILLISECONDS);
    }

}
