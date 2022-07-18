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

package com.flipkart.turbo.utils;

import org.apache.commons.lang3.StringUtils;

/**
 * @author shah.dhruvik
 * @date 05/08/19.
 */
public enum ZoneType {

    IN_HYDERABAD_1("cosmos-hyd"),
    IN_CHENNAI_1("cosmos"),
    IN_MUMBAI_PROD("cosmos-nm.in-mumbai-prod");

    private String alertSource;

    ZoneType(String alertSource) {
        this.alertSource = alertSource;
    }

    public String getAlertSource() {
        return alertSource;
    }

    public static ZoneType getZoneType(String zone) {
        if(StringUtils.isBlank(zone)){
            return null;
        }
        switch (zone) {
            case "in-hyderabad-1":
                return IN_HYDERABAD_1;
            case "in-chennai-1":
                return IN_CHENNAI_1;
            case "in-mumbai-prod":
                return IN_MUMBAI_PROD;
            default:
                return null;
        }
    }
}
