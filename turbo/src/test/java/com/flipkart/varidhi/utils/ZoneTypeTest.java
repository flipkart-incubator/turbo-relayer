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
import com.flipkart.turbo.utils.*;

import static org.junit.Assert.*;

/**
 * @author shah.dhruvik
 * @date 18/11/19.
 */

public class ZoneTypeTest {

    @Test
    public void zoneType() {
        String zone = "in-hyderabad-1";
        assertEquals(ZoneType.IN_HYDERABAD_1,ZoneType.getZoneType(zone));
        assertEquals("cosmos-hyd",ZoneType.getZoneType(zone).getAlertSource());
        zone = "in-chennai-1";
        assertEquals(ZoneType.IN_CHENNAI_1,ZoneType.getZoneType(zone));
        assertEquals("cosmos",ZoneType.getZoneType(zone).getAlertSource());
        zone = "in-mumbai-prod";
        assertEquals(ZoneType.IN_MUMBAI_PROD,ZoneType.getZoneType(zone));
        assertEquals("cosmos-nm.in-mumbai-prod",ZoneType.getZoneType(zone).getAlertSource());
        zone = "in-mumbai-preprod";
        assertNull(ZoneType.getZoneType(zone));
    }
}