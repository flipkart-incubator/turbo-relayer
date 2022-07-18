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

package com.flipkart.varidhi.config;

import io.dropwizard.validation.ValidationMethod;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.validator.constraints.NotBlank;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

/**
 * Created by manmeet.singh on 04/03/16.
 */
@Getter
@Setter
public class PartitionConfiguration {
    @NotNull
    private Long size;
    private Integer noOfDaysToPreserve;
    private Integer noOfHoursToPreserve;
    @NotNull
    private Integer noOfExtraPartitions;
    private String archivalPath;
    private String archivalDestination;
    private Long scheduleJobRetryTime;
    private Integer scheduleJobTries;
    @NotBlank
    private String jobTime;
    private String cronSchedule;
    @NotNull
    private Integer frequencyInHrs;
    @NotNull
    private PartitionMode mode;
    private Long monitorThreadSleepTime;
    private Long deadlockQueryExecutionTime;
    @Valid
    private RepositoryConfiguration appPartitionDbRef;
    private static final long DEFAULT_SCHEDULE_JOB_RETRY_TIME = 120000;
    private static final int DEFAULT_SCHEDULE_JOB_TRIES = 5;
    private static final long DEFAULT_MONITOR_THREAD_SLEEP_TIME = 5000;
    private static final long DEFAULT_DEADLOCK_QUERY_EXECUTION_TIME = 30;
    private static final Logger logger = LoggerFactory.getLogger(PartitionConfiguration.class);

    public boolean isInObserverMode() {
        return mode == PartitionMode.OBSERVER;
    }

    public void setDefaultPartitionConfigurations(PartitionConfiguration defaultPartitionConfiguration,RelayerConfiguration relayerConfiguration) {

        if (this.getMode() == null) {
            this.setMode(defaultPartitionConfiguration.getMode());
        }
        if (this.getMode() != PartitionMode.INACTIVE) {
            if (defaultPartitionConfiguration != null) {
                if (this.getSize() == null) {
                    if (defaultPartitionConfiguration.getSize() != null) {
                        this.setSize(new Long(defaultPartitionConfiguration.getSize()));
                    } else {
                        logger.error("Partition Size is Empty");
                        throw new IllegalArgumentException("Partition Size cannot be Empty");
                    }
                }


                if (this.getNoOfHoursToPreserve() == null) {
                    if (defaultPartitionConfiguration.getNoOfHoursToPreserve() != null) {
                        this.setNoOfHoursToPreserve(new Integer(defaultPartitionConfiguration.getNoOfHoursToPreserve()));
                    }
                }

                if (this.getNoOfDaysToPreserve() == null) {
                    if (defaultPartitionConfiguration.getNoOfDaysToPreserve() != null) {
                        this.setNoOfDaysToPreserve(new Integer(defaultPartitionConfiguration.getNoOfDaysToPreserve()));
                    } else {
                        if(this.getNoOfHoursToPreserve() == null) {
                            logger.error("No of Days/Hours to preserve Size is Empty");
                            throw new IllegalArgumentException("Number of days and hours to preserve is empty, atleast one is required.");
                        }
                    }
                }



                if (this.getNoOfExtraPartitions() == null) {
                    if (defaultPartitionConfiguration.getNoOfExtraPartitions() != null) {
                        this.setNoOfExtraPartitions(new Integer(defaultPartitionConfiguration.getNoOfExtraPartitions()));
                    } else {
                        logger.error("No of Extra Partitions is Empty");
                        throw new IllegalArgumentException("NoOfExtraPartitions cannot be Empty");
                    }
                }
                if (this.getAppPartitionDbRef() == null) {
                    if (defaultPartitionConfiguration.getAppPartitionDbRef() != null) {
                        this.setAppPartitionDbRef(defaultPartitionConfiguration.getAppPartitionDbRef());
                    } else {
                        this.setAppPartitionDbRef(relayerConfiguration.getAppDbRef());
                    }
                }
                if (this.getArchivalPath() == null) {
                    this.setArchivalPath(defaultPartitionConfiguration.getArchivalPath());
                }
                if (this.getArchivalDestination() == null) {
                    this.setArchivalDestination(defaultPartitionConfiguration.getArchivalDestination());
                }
                if (this.getScheduleJobRetryTime() == null && defaultPartitionConfiguration.getScheduleJobRetryTime() != null) {
                    this.setScheduleJobRetryTime(new Long(defaultPartitionConfiguration.getScheduleJobRetryTime()));
                }
                if (this.getScheduleJobTries() == null && defaultPartitionConfiguration.getScheduleJobTries() != null) {
                    this.setScheduleJobTries(new Integer(defaultPartitionConfiguration.getScheduleJobTries()));
                }
                if (this.getJobTime() == null) {
                    this.setJobTime(defaultPartitionConfiguration.getJobTime());
                }
                if (this.getCronSchedule() == null) {
                    this.setCronSchedule(defaultPartitionConfiguration.getCronSchedule());
                }
                if (this.getFrequencyInHrs() == null) {
                    if (defaultPartitionConfiguration.getFrequencyInHrs() != null) {
                        this.setFrequencyInHrs(new Integer(defaultPartitionConfiguration.getFrequencyInHrs()));
                    } else {
                        logger.error("Partition Frequency in Hrs is Empty");
                        throw new IllegalArgumentException("FrequencyInHrs cannot be Empty");
                    }
                }
                if (this.getMonitorThreadSleepTime() == null && defaultPartitionConfiguration.getMonitorThreadSleepTime() != null) {
                    this.setMonitorThreadSleepTime(new Long(defaultPartitionConfiguration.getMonitorThreadSleepTime()));
                }
                if (this.getDeadlockQueryExecutionTime() == null && defaultPartitionConfiguration.getDeadlockQueryExecutionTime() != null) {
                    this.setDeadlockQueryExecutionTime(new Long(defaultPartitionConfiguration.getDeadlockQueryExecutionTime()));

                }
            }
            setDefaultsForNull();
        }
    }

    @ValidationMethod(message="NoOfHours / NoOfDays to Preserve Configuration not right")
    public boolean isPartitionPreserveTimingCorrect(){
        if(
                (noOfDaysToPreserve == null && noOfHoursToPreserve == null)
                || (noOfHoursToPreserve != null && noOfHoursToPreserve <= 0)
                || (noOfHoursToPreserve == null && noOfDaysToPreserve <= 0)
        ) {
            logger.error("noOfDaysToPreserve/noOfHoursToPreserve is invalid/null");
            return false;
        }
        return true;
    }


    private void setDefaultsForNull() {
        if(this.scheduleJobRetryTime == null) {
            this.scheduleJobRetryTime = DEFAULT_SCHEDULE_JOB_RETRY_TIME;
        }
        if(this.scheduleJobTries == null) {
            this.scheduleJobTries = DEFAULT_SCHEDULE_JOB_TRIES;
        }
        if(this.monitorThreadSleepTime == null) {
            this.monitorThreadSleepTime = DEFAULT_MONITOR_THREAD_SLEEP_TIME;
        }
        if(this.deadlockQueryExecutionTime == null) {
            this.deadlockQueryExecutionTime = DEFAULT_DEADLOCK_QUERY_EXECUTION_TIME;
        }
    }

    @Override
    public String toString() {
        return "Partition size:" + size +
                "\nno of days to preserve :" + noOfDaysToPreserve +
                "\nno of hours to preserve :" + noOfHoursToPreserve +
                "\nno of extra partitions :" + noOfExtraPartitions +
                "\narchivalDestination :" + archivalDestination +
                "\nscheduleJobRetryTime :" + scheduleJobRetryTime +
                "\nscheduleJobTries :" + scheduleJobTries +
                "\njobTime :" + jobTime +
                "\nfrequencyInHrs :" + frequencyInHrs +
                "\nmode :" + mode;
    }
}
