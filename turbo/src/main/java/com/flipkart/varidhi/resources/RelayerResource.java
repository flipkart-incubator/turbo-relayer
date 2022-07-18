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

package com.flipkart.varidhi.resources;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.varidhi.RelayerMainService;
import com.flipkart.varidhi.config.AlertCreationService;
import com.flipkart.varidhi.core.RelayerHandleContainer;
import com.flipkart.varidhi.core.SessionFactoryContainer;
import com.flipkart.varidhi.core.utils.HttpClient;
import com.flipkart.varidhi.core.utils.JsonUtil;
import com.flipkart.varidhi.jobs.PartitionManagementJob;
import com.flipkart.varidhi.jobs.PartitionQueryFetcher;
import com.flipkart.varidhi.partitionManager.PartitionQueryModel;
import com.flipkart.varidhi.relayer.Relayer;
import com.flipkart.varidhi.relayer.common.Exceptions.RequestForwardingException;
import com.flipkart.varidhi.relayer.processor.ForwardingFilter;
import com.flipkart.varidhi.relayer.reader.models.RelayerResponse;
import com.flipkart.varidhi.relayer.schemavalidator.models.DBValidatorResult;
import com.flipkart.varidhi.utils.CommonUtils;
import com.flipkart.varidhi.utils.Constants;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.dropwizard.jackson.JsonSnakeCase;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/*
 * *
 * Author: abhinavp
 * Date: 14-Jul-2015
 *
 */


@Path("/relayer")
@JsonSnakeCase
@Produces({"application/json"})
@Singleton
public class RelayerResource {

    private static Logger logger = LoggerFactory.getLogger(RelayerResource.class);
    private RelayerHandleContainer relayerHandleContainer;
    private AlertCreationService alertCreationService;

    @Inject
    public RelayerResource(RelayerHandleContainer relayerHandleContainer, AlertCreationService alertCreationService) {
        this.relayerHandleContainer = relayerHandleContainer;
        this.alertCreationService = alertCreationService;
    }

    public RelayerHandleContainer getRelayerHandleContainer() {
        return relayerHandleContainer;
    }

    @GET
    @Path("/{namespace}/stop")
    @Produces(MediaType.APPLICATION_JSON)
    public Response stopRelayer(
            @PathParam("namespace") String namespace) {
        RelayerResponse response = new RelayerResponse();
        Relayer relayer = relayerHandleContainer.getRelayerHandle(namespace);
        if(relayer == null){
            response.setStatus(Response.Status.BAD_REQUEST);
            response.setMessage("relayer not found");
            logger.error("relayer not found: "+namespace);
            return buildResponse(response);
        }
        try {
            relayer.markRelayerInactive();
            relayer.stop();
            response.setMessage("relayer stopped successfully :"+namespace);
        } catch (Exception e) {
            response.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            response.setMessage("stopRelayer::Error in this method::" + e.getMessage()+ e);
            logger.error("stopRelayer::Error in this method::" + e.getMessage(), e);
        }
        return buildResponse(response);
    }

    @GET
    @Path("/{namespace}/start")
    @Produces(MediaType.APPLICATION_JSON)
    public Response startRelayer(
            @PathParam("namespace") String namespace) {
        RelayerResponse response = new RelayerResponse();
        Relayer relayer = relayerHandleContainer.getRelayerHandle(namespace);
        if(relayer == null){
            response.setMessage("relayer not found");
            response.setStatus(Response.Status.BAD_REQUEST);
            logger.error("relayer not found: "+namespace);
            return buildResponse(response);
        }
        try {
            relayer.markRelayerActive();
            if(!relayer.getRelayerConfiguration().isLeaderElectionEnabled()){
                relayer.init();
            }
            response.setMessage("relayer initialized successfully :"+namespace);

        } catch (Exception e) {
            response.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            response.setMessage("startRelayer::Error in this method::" + e.getMessage()+ e);
            logger.error("startRelayer::Error in this method::" + e.getMessage(), e);
        }
        return buildResponse(response);
    }

    @GET
    @Path("/getRelayersInfo")
    @Produces(MediaType.APPLICATION_JSON)
    public RelayerResponse getRelayersInfo() {
        RelayerResponse response = new RelayerResponse();
        @AllArgsConstructor
        class RelayerInfo{
            @JsonProperty("uuid")
            String uuid;
            @JsonProperty("isActive")
            boolean isActive;
            @JsonProperty("isRunning")
            boolean isRunning;
        }
        Map<String ,RelayerInfo> relayerIdToUUID = new HashMap<>();
        for ( Relayer relayer : relayerHandleContainer.getAllRelayers()){
            relayerIdToUUID.put(relayer.getRelayerId(),
                    new RelayerInfo(relayer.getRelayerUUID(),relayer.isActive(),relayer.isRunning()));
        }
        response.setData(relayerIdToUUID);
        return response;
    }

    @POST
    @Path("/{namespace}/control_task/group/{groupId}")
    @ForwardingFilter
    @Produces(MediaType.APPLICATION_JSON)
    public boolean createControlGroupTask(
            @PathParam("namespace") String namespace, @PathParam("groupId") String groupId) {
        try {
            Relayer relayer = relayerHandleContainer.getRelayerHandle(namespace);
            relayer.createUnsidelineGroupTask(groupId);
            return true;
        } catch (Exception e) {
            logger.error("createControlGroupTask::Error in this method::" + e.getMessage(), e);
        }
        return false;
    }


    @POST
    @Path("/control_task/relay_message/{namespace}")
    @ForwardingFilter
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, String> relayByMessageIDs(@NonNull @PathParam("namespace") String namespace, @NonNull List<String> messageIDs) {
        Map<String,String> status = new HashMap<>();
        String createStatus;
        logger.info("Got request to relayMessages for relayer: " + namespace + " with messageIds: " + messageIDs);
        try {
            Relayer relayer = relayerHandleContainer.getRelayerHandle(namespace);
            createStatus = relayer.createForcedMessageRelayerTask(messageIDs.stream().distinct().collect(Collectors.toList()));
        } catch (Exception e) {
            logger.error("Error in RelayMessage ControlTask: " + e.getMessage(), e);
            createStatus = "Failed to create Task: " + e.getMessage();
        }
        status.put("Status",createStatus);
        return status;
    }

    @POST
    @Path("/{namespace}/create_alerts/all")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String,String> createAllScheduledAlerts(@PathParam("namespace") String namespace, @Context final HttpHeaders httpHeaders) {
        logger.info("Got request to create all alerts for relayer: " + namespace);
        Map<String, String> results = new HashMap<>();
        try {
            Relayer relayer = relayerHandleContainer.getRelayerHandle(namespace);
            if (!validateRequest(relayer)) {
                String message = String.format("Relayer %s is invalid/inactive, skipping alert creation ",namespace);
                logger.error(message);
                results.put(namespace,message);
                return results;
            }
            if(relayer.getRelayerConfiguration().isLeaderElectionEnabled() && !relayer.isRunning()){
                com.ning.http.client.Response response = forwardRequest(relayer,
                        "POST","relayer/"+namespace+"/create_alerts/all",null,
                        CommonUtils.multiMapToMap(httpHeaders.getRequestHeaders()));
                results.put(namespace,response.getResponseBody());
            } else {
                if(relayer.getAlertProviderConfig() != null) {
                    results.putAll(alertCreationService.createAlertsForRelayer(relayer, relayer.getAlertProviderConfig().getAlertMethodName()));
                }
            }
        } catch (Exception e) {
            logger.error("Error in RelayMessage ControlTask: " + e.getMessage(), e);
            results.put(namespace,"Failure in Alert Creation"+ e.getMessage());
        }

        return results;
    }

    @POST
    @Path("/create_alerts/all")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String,Map<String,String>> createScheduledAlertsForAllRelayers(@Context final HttpHeaders httpHeaders) {
        logger.info("Got request to create all alerts for all relayers");
        Map<String,Map<String,String>> results = new HashMap<>();
        List<Relayer> allRelayers = relayerHandleContainer.getAllRelayers();
        try {
            for(Relayer relayer: allRelayers) {
                results.put(relayer.getRelayerId(),createAllScheduledAlerts(relayer.getRelayerId(),httpHeaders));
            }
        } catch (Exception e) {
            logger.error("Error in RelayMessage ControlTask: " + e.getMessage(), e);
        }
        return results;
    }


    @POST
    @Path("/control_task/partition/{namespace}")
    @Produces(MediaType.APPLICATION_JSON)
    public boolean createControlPartitionTask(@PathParam("namespace") String namespace,@Context final HttpHeaders httpHeaders) {
        logger.info("Got request for Partition Management Task for relayer: " + namespace);
        Relayer relayer = relayerHandleContainer.getRelayerHandle(namespace);
        if(!validateRequest(relayer)){
            logger.error("Relayer not found/isInactive",namespace);
            return false;
        }
        try {
            if(relayer.getRelayerConfiguration().isLeaderElectionEnabled() && !relayer.isRunning()){
                com.ning.http.client.Response response = forwardRequest(relayer,
                        "POST","relayer/control_task/partition/"+namespace,null,
                        CommonUtils.multiMapToMap(httpHeaders.getRequestHeaders()));
                return Boolean.valueOf(response.getResponseBody());
            }
            return relayer.createPartitionManagementTask();
        } catch (Exception e) {
            logger.error("Error in CreateControlPartitionTask: " + e.getMessage(), e);
        }
        return false;
    }

    @POST
    @Path("/control_task/partition/all")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Boolean> createControlPartitionTask(@Context final HttpHeaders httpHeaders) {
        logger.info("Got request for Partition Management Task for all relayers");
        List<Relayer> allRelayers = relayerHandleContainer.getAllRelayers();
        Map<String, Boolean> relayerControlTaskStatus = new HashMap<>();
            for (Relayer relayer : allRelayers) {
                try {
                    relayerControlTaskStatus.put(relayer.getRelayerId(), createControlPartitionTask(relayer.getRelayerId(),httpHeaders));
                } catch (Exception e) {
                    logger.error("Error in CreateControlPartitionTask: " + e.getMessage(), e);
                    relayerControlTaskStatus.put(relayer.getRelayerId(), false);
                }
            }
        return relayerControlTaskStatus;
    }

    @GET
    @Path("/{namespace}/partitions")
    @Produces(MediaType.APPLICATION_JSON)
    public PartitionQueryModel getPartitionManagementQueries(@PathParam("namespace") String namespace,@Context final HttpHeaders httpHeaders) {
        logger.info("Got request to get getPartitionManagementQueries for relayer: " + namespace);
        PartitionQueryFetcher partitionQueryFetcher = new PartitionQueryFetcher();
        try {
            Relayer relayer = relayerHandleContainer.getRelayerHandle(namespace);
            if(relayer == null){
                throw new RuntimeException("relayer not found "+namespace);
            }

            if(relayer.getRelayerConfiguration().isLeaderElectionEnabled() && !relayer.isRunning()){
                com.ning.http.client.Response response = forwardRequest(relayer,
                        "GET",String.format("relayer/%s/partitions",namespace),null,
                        CommonUtils.multiMapToMap(httpHeaders.getRequestHeaders()));
                return JsonUtil.deserializeJson(response.getResponseBody(),PartitionQueryModel.class);
            }
            SessionFactoryContainer sessionFactoryContainer = RelayerMainService.getInstance(SessionFactoryContainer.class);
            PartitionManagementJob partitionManagementJob = new PartitionManagementJob(relayer,
                    sessionFactoryContainer,partitionQueryFetcher, null);
            partitionManagementJob.invokePartitionManager(relayer, true);
        } catch (Exception e) {
            logger.error("Error while fetching Partitions : " + e.getMessage(), e);
            partitionQueryFetcher.getPartitionQueryModel().getErrors().add(e.getMessage());
            partitionQueryFetcher.collectException(e.getMessage());
        }
        return partitionQueryFetcher.getPartitionQueryModel();
    }

    @GET
    @Path("/partitions")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, PartitionQueryModel> getPartitionManagementQueries(@Context final HttpHeaders httpHeaders) {
        logger.info("Got request to get Partitions");
        Map<String, PartitionQueryModel> partitionQueryModelForEachRelayer = new HashMap<>();
        List<Relayer> allRelayers = relayerHandleContainer.getAllRelayers();
        for (Relayer relayer : allRelayers) {
            partitionQueryModelForEachRelayer.put(
                    relayer.getRelayerId(),
                    getPartitionManagementQueries(relayer.getRelayerId(), httpHeaders)
            );
        }
        return partitionQueryModelForEachRelayer;
    }

    @POST
    @Path("/{namespace}/control_task/message/{messageId}")
    @ForwardingFilter
    @Produces(MediaType.APPLICATION_JSON)
    public boolean createControlMessageTask(
            @PathParam("namespace") String namespace, @PathParam("messageId") String messageId) {
        try {
            Relayer relayer = relayerHandleContainer.getRelayerHandle(namespace);
            relayer.createUnsidelineMessageTask(messageId);
            return true;
        } catch (Exception e) {
            logger.error("createControlMessageTask::Error in this method::" + e.getMessage(), e);
        }
        return false;
    }


    @POST
    @Path("/{namespace}/control_task/ungrouped")
    @ForwardingFilter
    @Produces(MediaType.APPLICATION_JSON)
    public boolean createControlUngroupedMessageTask(
            @PathParam("namespace") String namespace, ControlTaskInput controlTaskInput) {
        try {
            controlTaskInput = ControlTaskInput.getValidOrDefault(controlTaskInput);
            Relayer relayer = relayerHandleContainer.getRelayerHandle(namespace);
            relayer.createUnsidelineAllUngroupedMessageTask(controlTaskInput.getFromDate(),
                    controlTaskInput.getToDate());
            return true;
        } catch (Exception e) {
            logger.error("createControlUngroupedMessageTask::Error in this method::" + e.getMessage(), e);
        }
        return false;
    }

    @POST
    @Path("/{namespace}/control_task/all")
    @ForwardingFilter
    @Produces(MediaType.APPLICATION_JSON)
    public boolean createControlAllMessageTask(
            @PathParam("namespace") String namespace, ControlTaskInput controlTaskInput) {
        controlTaskInput = ControlTaskInput.getValidOrDefault(controlTaskInput);
        try {
            Relayer relayer = relayerHandleContainer.getRelayerHandle(namespace);
            createControlUngroupedMessageTask(namespace, controlTaskInput);
            relayer.createUnsidelineMessagesBetweenDatesTask(controlTaskInput.getFromDate(),
                    controlTaskInput.getToDate());
            return true;
        } catch (Exception e) {
            logger.error("createControlAllMessageTask::Error in this method::" + e.getMessage(), e);
        }
        return false;
    }

    @GET
    @Path("/db_schema_validate/all")
    @Produces(MediaType.APPLICATION_JSON)
    public RelayerResponse validateDBSchemaForAllRelayers() {
        RelayerResponse response = new RelayerResponse();
        try {
            List<Relayer> allRelayers = relayerHandleContainer.getAllRelayers();
            List<DBValidatorResult> responseList =  new ArrayList<>();
            for (Relayer relayer : allRelayers) {
                if(validateRequest(relayer)) {
                    responseList.add(relayer.validateDBSchema());
                }
            }
            response.setData(responseList);
            response.setMessage("Schema validation complete, if there is any schema difference fix it with reference " +
                    "to expected schema. And make sure all columns have same charset and collation. " +
                    "For more details follow 'Table Creation scripts' in turbo docs.");
        } catch (Exception e) {
            response.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            response.setMessage("error while validating schema " + e.getMessage()+ e);
            logger.error("error while validating schema " + e.getMessage(), e);
        }
        return response;
    }

    private boolean validateRequest(Relayer relayer){
        if(relayer == null || !relayer.isActive()){
            return false;
        }
        return true;
    }

    private Response buildResponse(RelayerResponse response) {
        return Response.status(response.getStatus()).entity(response).build();
    }

    private com.ning.http.client.Response forwardRequest(Relayer relayer,String method,String urlPath, String body, Map<String, String> headers) throws Exception {
        String leaderIP = relayer.getActiveRunningRelayerIP();
        if (StringUtils.isBlank(leaderIP)) {
            throw new RequestForwardingException("can't determine leader ip " + leaderIP);
        }
        if(CommonUtils.fetchMachineHostname().equalsIgnoreCase(leaderIP)){
            throw new RequestForwardingException("can't forward request to self");
        }
        String url = Constants.TURBO_REDIRECT_URL.replace(Constants.HOST,leaderIP);
        url = url.replace(Constants.URL_PATH,urlPath);
        return HttpClient.INSTANCE.executeRequestWithForwardingHeader(method, url, body, headers);
    }

}
