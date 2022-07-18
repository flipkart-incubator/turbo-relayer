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

package com.flipkart.varidhi.relayer.processor;

import com.flipkart.varidhi.core.RelayerHandleContainer;
import com.flipkart.varidhi.core.utils.HttpClient;
import com.flipkart.varidhi.relayer.Relayer;
import com.flipkart.varidhi.relayer.common.Exceptions.RequestForwardingException;
import com.flipkart.varidhi.relayer.reader.models.RelayerResponse;
import com.flipkart.varidhi.resources.RelayerResource;
import com.flipkart.varidhi.utils.CommonUtils;
import com.flipkart.varidhi.utils.Constants;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

@Provider
@ForwardingFilter
public class ForwardingFilterImpl implements ContainerRequestFilter {
    private static Logger logger = LoggerFactory.getLogger(ForwardingFilterImpl.class);

    @Context
    private ResourceInfo resourceInfo;

    @Override
    public void filter(ContainerRequestContext context) {
        try {
            logger.debug("filtering request " + context.toString());
            Method method = resourceInfo.getResourceMethod();
            ForwardingFilter forwardingFilter = method.getAnnotation(ForwardingFilter.class);
            logger.debug("filter type " + forwardingFilter);



            // namespace filter
            MultivaluedMap<String, String> paramsMap = context.getUriInfo().getPathParameters();
            String namespace = paramsMap.getFirst("namespace");
            if (StringUtils.isEmpty(namespace)) {
                context.abortWith(Response.status(Response.Status.BAD_REQUEST).entity("Invalid relayer name").build());
                return;
            }
            if(CollectionUtils.isEmpty(context.getUriInfo().getMatchedResources())){
                logger.error("couldn't find matched resource " + context.toString());
                return;
            }
            RelayerResource relayerResource = (RelayerResource)context.getUriInfo().getMatchedResources().get(0);
            RelayerHandleContainer relayerHandleContainer = relayerResource.getRelayerHandleContainer();
            Relayer relayer = relayerHandleContainer.getRelayerHandle(namespace);
            if (relayer == null) {
                logger.error("Relayer not found: " + namespace);
                RelayerResponse response = new RelayerResponse();
                response.setMessage("Relayer not found: " + namespace);
                context.abortWith(Response.status(Response.Status.BAD_REQUEST).entity(response).build());
                return;
            }


            // active filter
            if (!relayer.isActive()) {
                logger.error("Relayer is Inactive: "+namespace);
                RelayerResponse response = new RelayerResponse();
                response.setMessage("Relayer isInactive: "+namespace);
                context.abortWith(Response.status(Response.Status.BAD_REQUEST).entity(response).build());
                return;
            }


            // forwarding filter
            if (relayer.getRelayerConfiguration().isLeaderElectionEnabled() && !relayer.isRunning()) {
                String leaderIP = relayer.getActiveRunningRelayerIP();
                if (StringUtils.isBlank(leaderIP)) {
                    RelayerResponse response = new RelayerResponse();
                    response.setMessage("can't determine leader ip " + leaderIP);
                    context.abortWith(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(response).build());
                    return;
                }
                if(CommonUtils.fetchMachineHostname().equalsIgnoreCase(leaderIP)){
                    throw new RequestForwardingException("can't forward request to self");
                }
                String url = Constants.TURBO_REDIRECT_URL.replace(Constants.HOST,leaderIP);
                url = url.replace(Constants.URL_PATH,context.getUriInfo().getPath());
                String body = null;
                if(context.getMediaType() != null && context.getMediaType().toString().contains("application/json")){
                    body = IOUtils.toString(context.getEntityStream(), Charsets.UTF_8);
                }
                com.ning.http.client.Response response = HttpClient.INSTANCE.executeRequestWithForwardingHeader(
                        context.getMethod(), url, body, CommonUtils.multiMapToMap(context.getHeaders()));
                context.abortWith(Response.status(response.getStatusCode()).entity(response.getResponseBody()).build());
            }
        } catch (Exception e) {
            logger.error("error occurred while processing request, ", e);
            RelayerResponse response = new RelayerResponse();
            response.setMessage("error occurred while processing request, " + e.getMessage());
            context.abortWith(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(response).build());
        }

    }


}
