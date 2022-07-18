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

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.flipkart.varidhi.relayer.Relayer;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Histogram;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class RelayerMetricTest {

    @Test
    public void testUpdateMetricWithNull() {
        //Test Case to check when 'null' value is being passed to the updateMetric method works or not.
        Long c = null;
        RelayerMetric relayerMetric = new RelayerMetric<Long>( "12323", "messages.sidelined", true, true);
        try {
            relayerMetric.updateMetric(c);
        }
        catch (Throwable throwable){
            //throw error if the updateMetric does not accepts null.
            throwable.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testUpdateMetricWithInt(){
        //Test Case to check when Integer type is being passed to the updateMetric method works or not.
        int  c = 12;
        RelayerMetric relayerMetric = new RelayerMetric<Long>( "12323", "messages.sidelined", true, true);
        try {
            relayerMetric.updateMetric(c);
        }
        catch (Throwable throwable){
            //throw error if the updateMetric does not accepts null.
            throwable.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testUpdateMetricWithDouble(){
        //Test Case to check when Double type is being passed to the updateMetric method works or not.
        double c = 12.2;
        RelayerMetric relayerMetric = new RelayerMetric<Long>( "12323", "messages.sidelined", true, true);
        try {
            relayerMetric.updateMetric(c);
        }
        catch (Throwable throwable){
            //throw error if the updateMetric does not accepts null.
            throwable.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testHistogramUpdateWithNull(){
        // Initializing  the variable that need to be passed to the target fucntion.
        Long c = null;

        //Creating the mock obejcts for testing
        RelayerMetric relayerMetric = Mockito.spy(new RelayerMetric<Long>( "12323", "messages.sidelined", true, true));
        Histogram hist =  Metrics.newHistogram(Relayer.class, "relayer.exchange.12323", "messages.sidelined.histogram");
        Histogram histogramObject = spy(hist);
        doReturn(histogramObject).when(relayerMetric).getHistogram();

        //Supplying the value of the variable to the Object containing the target function to be tested.
        relayerMetric.updateMetric(c);

        //'verify' method which checks how many times the target function is called with the given value.
        //using anyLong() instead of 'c' because the data type accepted by 'update()' method of histogram is 'Long'.
        verify(histogramObject, times(0)).update(anyLong());

    }

    @Test
    public void testHistogramUpdateWithInt(){
        // Initializing  the integer variable that need to be passed to the target fucntion.
        Integer c = 1;

        //Creating the mock obejcts for testing
        RelayerMetric relayerMetric = Mockito.spy(new RelayerMetric<Long>( "12323", "messages.sidelined", true, true));
        Histogram hist =  Metrics.newHistogram(Relayer.class, "relayer.exchange.12323", "messages.sidelined.histogram");
        Histogram histogramObject = spy(hist);
        doReturn(histogramObject).when(relayerMetric).getHistogram();

        //Supplying the value of the variable to the Object containing the target function to be tested.
        relayerMetric.updateMetric(c);

        //'verify' method which checks how many times the target function is called with the given value.
        //using anyLong() instead of 'c' because the data type accepted by 'update()' method of histogram is 'Long'.
        verify(histogramObject, times(1 )).update(c);

    }


    //Method to test the constructors of test relayMetric.
    @Test
    public void testRelayMetricConstructor(){
        try {
            //Creating new objects for Testing
            new RelayerMetric<Long>(null,"1",true,true);
            new RelayerMetric<Long>("1",null,true,true);
            new RelayerMetric<Long>("1","1",false,true);
            new RelayerMetric<Long>("1","1",true,false);
        }
        catch ( Exception e){
            //Throw an error if something goes wrong in the try part
            fail(e.getMessage());
        }
    }


}