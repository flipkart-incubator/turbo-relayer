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

import com.cronutils.mapper.CronMapper;
import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.StringUtils;

import javax.ws.rs.core.MultivaluedMap;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.flipkart.varidhi.utils.StringUtils.loadStream;

public class CommonUtils {

    private static final Logger logger = LoggerFactory.getLogger(CommonUtils.class);

    private static Long machineHostWithRemovedDots = -1L;

    public static String generateRelayerUUID(){
        return fetchMachineHostname() + ":"+ UUID.randomUUID().toString();
    }

    public static String getRelayerIPFromUUID(String relayerUUID){
        if(StringUtils.isBlank(relayerUUID)){
            return null;
        }
        String[] splitString = relayerUUID.split(":");
        if(splitString.length < 2){
            return null;
        }
        return splitString[0];
    }

    public static String fetchMachineHostname(){
        return fetchMachineHostnameUsingBash();
    }

    private static String fetchMachineHostnameUsingBash() {
        String hostname = "";
        try {
            ProcessBuilder pb = new ProcessBuilder("/bin/bash", "-c", "export HOSTNAME=`hostname -I`" + " && " + "printenv");
            Process p = pb.start();
            int rc = p.waitFor();
            if (rc == 0) {
                Properties properties = new Properties();
                properties.load(p.getInputStream());
                hostname = properties.getProperty("HOSTNAME") != null ? properties.getProperty("HOSTNAME").trim() : "";
            } else {
                logger.error("Unable to fetch HOSTNAME :" + loadStream(p.getErrorStream()));
            }
        } catch (Exception e) {
            logger.error("Failed to get fetch HOSTNAME " + e);
        }
        return hostname;
    }

    private static Long parseAndRemoveDotsFromIps(String machineHostname) {
        if(StringUtils.isEmpty(machineHostname)){
            return null;
        }
        Long ip = null;
        try {
            ip = Long.parseLong(machineHostname.replace(".", ""));
        }  catch (Exception e) {
            logger.error("exception occured", e);
        }
        return ip;
    }

    public static Long getDotRemovedIp() {
        if(machineHostWithRemovedDots == -1L){
            machineHostWithRemovedDots = parseAndRemoveDotsFromIps(fetchMachineHostname());
        }
        return machineHostWithRemovedDots;
    }

    public static Date getNextYearDate() {
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("IST"));
        cal.add(Calendar.YEAR, 1);
        return cal.getTime();
    }

    public static Date getPreviousYearDate() {
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("IST"));
        cal.add(Calendar.YEAR, -1);
        return cal.getTime();
    }

    public static String convertUnixToQuartzCron(String unixCronExpression){
        CronParser cronParser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX));
        Cron unixCron = cronParser.parse(unixCronExpression);
        String quartzCronExpression = CronMapper.fromUnixToQuartz().map(unixCron).asString();
        logger.info(String.format("Cron Parser original unix cron : '%s', parsed cron : '%s', quartz cron : '%s'",
                unixCronExpression, unixCron.asString(),quartzCronExpression));
        return quartzCronExpression;
    }

    public static long getInitialDelay(String jobTime) {
        Date jobDate = getNextExecution(jobTime);
        long initialDelay = jobDate.getTime() - new Date().getTime();
        return initialDelay;
    }

    public static Date getNextExecution(String jobTime) {
        String currentDate = new SimpleDateFormat("yyyy-dd-MM").format(new Date());
        Date jobDate;
        try {
            jobDate = new SimpleDateFormat("yyyy-dd-MM HH:mm:SS").parse(currentDate + " " + jobTime);
        } catch (ParseException e) {
            logger.error("Could not parse configured jobTime " + e, e);
            throw new RuntimeException(e);
        }
        if (jobDate.before(new Date())) {
            jobDate = new Date(jobDate.getTime() + 86400000);
        }
        return jobDate;
    }

    public static Map<String, String> multiMapToMap(MultivaluedMap<String, String> queryParameters) {
        Map<String, String> parameters = new HashMap<>();
        for (String str : queryParameters.keySet()) {
            parameters.put(str, queryParameters.getFirst(str));
        }
        return parameters;
    }
}
