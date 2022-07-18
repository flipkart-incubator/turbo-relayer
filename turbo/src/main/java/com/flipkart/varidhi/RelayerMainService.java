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

package com.flipkart.varidhi;

import com.flipkart.varidhi.config.ApplicationConfiguration;
import com.flipkart.varidhi.config.PartitionConfiguration;
import com.flipkart.varidhi.config.PartitionMode;
import com.flipkart.varidhi.config.RelayerConfiguration;
import com.flipkart.varidhi.core.RelayerHandleContainer;
import com.flipkart.varidhi.core.SessionFactoryContainer;
import com.flipkart.varidhi.core.utils.EmailNotifier;
import com.flipkart.varidhi.health.RelayerHealthCheck;
import com.flipkart.varidhi.jobs.ControlTaskDeletionJob;
import com.flipkart.varidhi.jobs.PartitionManagementJob;
import com.flipkart.varidhi.jobs.PartitionManagementQuartzJob;
import com.flipkart.varidhi.jobs.PartitionQueryEmailer;
import com.flipkart.varidhi.jobs.UnsidelineMessagesQuartzJob;
import com.flipkart.varidhi.relayer.Relayer;
import com.flipkart.varidhi.relayer.processor.ForwardingFilterImpl;
import com.flipkart.varidhi.repository.PartitionAlterMonitor;
import com.flipkart.varidhi.utils.CommonUtils;
import io.dropwizard.Application;
import io.dropwizard.Configuration;
import io.dropwizard.lifecycle.setup.ScheduledExecutorServiceBuilder;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.vyarus.dropwizard.guice.GuiceBundle;

import javax.validation.ConstraintViolation;
import javax.validation.Validator;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.quartz.SimpleScheduleBuilder.simpleSchedule;

/*
 * *
 * Author: abhinavp
 * Date: 30-Jun-2015
 *
 */
public class RelayerMainService extends Application<Configuration> {
    private static final Logger logger = LoggerFactory.getLogger(RelayerMainService.class);
    private static final int BUFFER_BETWEEN_PARTITION_MANAGEMENT_IN_MS = 300000;
    private static final int DEFAULT_PARTITION_DELAY_DURING_STARTUP = 15;
    private static GuiceBundle guiceBundle;

    public static void main(String[] args) throws Exception {
        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                logger.error("Uncaught Exception: " + t.getName(), e);
            }
        });
        new RelayerMainService().run(args);
    }

    public static <T> T getInstance(Class<T> c) {
        return guiceBundle.getInjector().getInstance(c);
    }

    @Override
    public void initialize(Bootstrap<Configuration> bootstrap) {
        guiceBundle = GuiceBundle.<Configuration>builder().modules(new RelayerModule())
                .enableAutoConfig(getClass().getPackage().getName()).build();
        bootstrap.addBundle(guiceBundle);
        bootstrap.getObjectMapper().setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
        bootstrap.getObjectMapper().setTimeZone(TimeZone.getTimeZone("Asia/Calcutta"));
    }

    @Override
    public void run(Configuration configuration,
                    Environment environment) {
        try {
//            environment.healthChecks().register("Relayer Health check", new RelayerHealthCheck());
            environment.jersey().register(new ForwardingFilterImpl());
            triggerJobs(environment);
        }catch (Exception e){
            logger.error("Exception occurred during initialization",e);
            System.exit(1);
        }
    }

    private void triggerJobs(Environment environment) throws SchedulerException {
        RelayerModule relayerModule = getInstance(RelayerModule.class);
        ApplicationConfiguration applicationConfiguration = getInstance(ApplicationConfiguration.class);
        verifyConsumersCountForRelayers(applicationConfiguration.getRelayers());
        SessionFactoryContainer sessionFactoryContainer = getInstance(SessionFactoryContainer.class);
        RelayerHandleContainer relayerHandleContainer = getInstance(RelayerHandleContainer.class);
        ScheduledExecutorServiceBuilder executorServiceBuilder =
                environment.lifecycle().scheduledExecutorService("SchedulerThread");

        /*
            1 executor service thread for each relayer: Control task Deletion & Partition Management on startup
            and 2 quartz task: unsideline cron task & Partition Management for each via quartz
         */
        executorServiceBuilder.threads(applicationConfiguration.getRelayers().size());
        ScheduledExecutorService executorService = executorServiceBuilder.build();
        Properties quartzProperties = new Properties();
        quartzProperties.setProperty("org.quartz.threadPool.threadCount",
                String.valueOf(relayerHandleContainer.getAllRelayers().size()) );

        SchedulerFactory schedulerFactory = new StdSchedulerFactory(quartzProperties);
        Scheduler scheduler = schedulerFactory.getScheduler();
        scheduler.getContext().put(RelayerHandleContainer.class.getCanonicalName(),relayerHandleContainer);

        for (Relayer relayer : relayerHandleContainer.getAllRelayers()) {
            RelayerConfiguration relayerConfiguration = relayer.getRelayerConfiguration();

            //Control Task Job
            long controlTaskDeletionDelay =  CommonUtils.getInitialDelay(relayerConfiguration.getControlTaskDeletionJobTime());
            long controlTaskDeletionFrequency =  TimeUnit.HOURS.toMillis(relayerConfiguration.getControlTaskDeletionFrequencyInHrs());
            Runnable controlTaskDeletionJob = new ControlTaskDeletionJob(relayer,sessionFactoryContainer);
            executorService.scheduleAtFixedRate(controlTaskDeletionJob, controlTaskDeletionDelay, controlTaskDeletionFrequency, TimeUnit.MILLISECONDS);
            //Unsideline Cron Job
            scheduleUnsidelineQuartzCronJobs(relayer,scheduler);
            //Partition Management Cron
            PartitionConfiguration partitionConfiguration = applicationConfiguration.getPartitionConfigForRelayerConfig(relayerConfiguration);
            if (PartitionMode.INACTIVE == partitionConfiguration.getMode()) {
                logger.warn("PartitionManagement is disabled for relayer: " + relayerConfiguration.getRelayerId());
                continue;
            }
            validatePartitionConfig(environment.getValidator(), partitionConfiguration);
            PartitionAlterMonitor partitionAlterMonitor = new PartitionAlterMonitor(partitionConfiguration.getMonitorThreadSleepTime(),
                    partitionConfiguration.getDeadlockQueryExecutionTime(),
                    partitionConfiguration.getScheduleJobTries(), partitionConfiguration.getScheduleJobRetryTime());
            EmailNotifier emailNotifier = new EmailNotifier(applicationConfiguration.getEmailConfiguration());
            PartitionManagementJob partitionManagementJob = new PartitionManagementJob(relayer, sessionFactoryContainer,
                    new PartitionQueryEmailer(emailNotifier, applicationConfiguration), partitionAlterMonitor);
            relayerModule.setPartitionManagementJobForRelayerContainer(relayerConfiguration, partitionManagementJob);

            Date firstExecution;
            if(partitionConfiguration.getCronSchedule() != null) {
                firstExecution = scheduleQuartzPartitionJobs(relayer.getRelayerId(), partitionConfiguration.getCronSchedule(), scheduler);
            } else {
                firstExecution = scheduleQuartzPartitionJobs(relayer.getRelayerId(), partitionConfiguration.getJobTime(), partitionConfiguration.getFrequencyInHrs(), scheduler);
            }
            //Partition Management on Startup
            if(((firstExecution.getTime() - new Date().getTime()) > BUFFER_BETWEEN_PARTITION_MANAGEMENT_IN_MS)
                    && Boolean.valueOf(System.getProperty("relayer.partitionManagementOnStartup"))) {
                schedulePartitionManagementOnStartup(executorService, relayer, partitionManagementJob);
            }
        }
        scheduler.start();

    }

    private void scheduleUnsidelineQuartzCronJobs(Relayer relayer, Scheduler scheduler) throws SchedulerException{
        String cronExpression = CommonUtils.convertUnixToQuartzCron(relayer.getRelayerConfiguration().getCronScheduleUnsideline());

        JobDetail jobDetail = JobBuilder.newJob(UnsidelineMessagesQuartzJob.class)
                .withIdentity("job_"+UnsidelineMessagesQuartzJob.class.getCanonicalName(), relayer.getRelayerId())
                .withDescription("unsideline job for relayer: "+relayer.getRelayerId())
                .build();

        CronTrigger trigger = TriggerBuilder.newTrigger()
                .withIdentity("trigger_"+UnsidelineMessagesQuartzJob.class.getCanonicalName(), relayer.getRelayerId())
                .withSchedule(CronScheduleBuilder.cronSchedule(cronExpression).withMisfireHandlingInstructionFireAndProceed())
                .build();
        scheduler.scheduleJob(jobDetail,trigger);
    }

    private Date scheduleQuartzPartitionJobs(String relayerId, String jobTime, int frequency,
                                             Scheduler scheduler) throws SchedulerException{
        logger.info("Scheduling Partition Managemnt via jobTime & frequency for relayer: " + relayerId);
        JobDetail jobDetail = getPartitionJobDetail(relayerId);
        SimpleScheduleBuilder simpleScheduleBuilder = simpleSchedule()
                .withIntervalInHours(frequency)
                .repeatForever()
                .withMisfireHandlingInstructionIgnoreMisfires();
        Date nextExecution = CommonUtils.getNextExecution(jobTime);
        SimpleTrigger trigger = TriggerBuilder.newTrigger()
                .withIdentity("trigger_" + PartitionManagementQuartzJob.class.getCanonicalName(), relayerId)
                .startAt(nextExecution)
                .withSchedule(simpleScheduleBuilder)
                .build();
        scheduler.scheduleJob(jobDetail,trigger);
        return nextExecution;
    }

    private Date scheduleQuartzPartitionJobs(String relayerId, String cron, Scheduler scheduler) throws SchedulerException{
        logger.info("Scheduling Partition Managemnt via cron expression for relayer: " + relayerId + " & cron: " + cron);
        String cronExpression = CommonUtils.convertUnixToQuartzCron(cron);
        JobDetail jobDetail = getPartitionJobDetail(relayerId);
        CronTrigger trigger = TriggerBuilder.newTrigger()
                .withIdentity("trigger_"+PartitionManagementQuartzJob.class.getCanonicalName(), relayerId)
                .withSchedule(CronScheduleBuilder.cronSchedule(cronExpression).withMisfireHandlingInstructionFireAndProceed())
                .build();
        scheduler.scheduleJob(jobDetail,trigger);

        return trigger.getFireTimeAfter(new Date());
    }

    private JobDetail getPartitionJobDetail(String relayerId) {
        return JobBuilder.newJob(PartitionManagementQuartzJob.class)
                .withIdentity("job_" + PartitionManagementQuartzJob.class.getCanonicalName(), relayerId)
                .withDescription("Partition Management job for relayer: "+relayerId)
                .build();
    }

    private void schedulePartitionManagementOnStartup(ScheduledExecutorService executorService, Relayer relayer, PartitionManagementJob partitionManagementJob) {
        long initialPartitionDelay = DEFAULT_PARTITION_DELAY_DURING_STARTUP;
        if(relayer.getRelayerConfiguration().isLeaderElectionEnabled()) {
            //This is required to insure leader election has happened before scheduling partition management
            initialPartitionDelay = relayer.getRelayerConfiguration().getLeaderElectionExpiryInterval() * 2;
        }
        logger.info("Scheduling Partition Managemnt on Startup for relayer: " + relayer.getRelayerId() +
                " after " + initialPartitionDelay + " seconds.");
        executorService.schedule(partitionManagementJob,initialPartitionDelay, TimeUnit.SECONDS);
    }

    private void validatePartitionConfig(Validator validator, PartitionConfiguration partitionConfiguration) {
        Set<ConstraintViolation<PartitionConfiguration>> validate = validator.validate(partitionConfiguration);
        if (validate.size() > 0) {
            logger.error("Constraint Violation in Partition Config:");
            for (ConstraintViolation constraintViolation : validate) {
                logger.error(constraintViolation.getPropertyPath() + " " + constraintViolation.getMessage());
            }
            System.exit(1);
        }
    }

    private void verifyConsumersCountForRelayers(
            List<RelayerConfiguration> relayerConfigurationList) {
        for (RelayerConfiguration relayerConfiguration : relayerConfigurationList) {
            if (!isPrime(relayerConfiguration.getProcessorParallelismDegree())) {
                logger.error("Number of SubProcessors for Relayer: " + relayerConfiguration.getRelayerId()
                        + " is not prime. Stopping Relayer :: System.exit");
                System.exit(-1);
            }
        }
    }

    boolean isPrime(int n) {
        if (n < 2) {
            return false;
        }
        if (n == 2 || n == 3) {
            return true;
        }
        if (n % 2 == 0 || n % 3 == 0) {
            return false;
        }
        int sqrt = (int) Math.sqrt(n) + 1;
        for (int i = 6; i <= sqrt; i += 6) {
            if (n % (i - 1) == 0 || n % (i + 1) == 0) {
                return false;
            }
        }
        return true;
    }

}
