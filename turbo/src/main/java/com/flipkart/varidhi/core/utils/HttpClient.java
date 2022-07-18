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

import com.flipkart.varidhi.relayer.common.Exceptions.RequestForwardingException;
import com.ning.http.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
/*
 * *
 * Author: abhinavp
 * Date: 03-Aug-2015
 *
 */


public enum HttpClient {
    INSTANCE;

    private final AsyncHttpClient asyncHttpClient;
    private Logger logger = LoggerFactory.getLogger(HttpClient.class);

    HttpClient() {
        asyncHttpClient = new AsyncHttpClient();
    }

    public Response executeGet(String url) throws Exception {
        final Map<String, String> headers = new HashMap<>();
        final Map<String, String> params = new HashMap<>();
        return executeGet(url, params, headers);

    }

    public Response executeGet(String url, Map<String, String> params, Map<String, String> headers)
        throws Exception {
        final AsyncHttpClient.BoundRequestBuilder requestBuilder = asyncHttpClient.prepareGet(url);
        if (params != null) {
            for (Map.Entry<String, String> entry : params.entrySet()) {
                requestBuilder.addQueryParameter(entry.getKey(), entry.getValue());
            }
        }
        if (headers != null) {
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                requestBuilder.addHeader(entry.getKey(), entry.getValue());
            }
        }
        return requestBuilder.execute().get();
    }

    public Response executePost(String url, String body, Map<String, String> headers, String characterEncoding)
            throws Exception {

        final FluentCaseInsensitiveStringsMap map = new FluentCaseInsensitiveStringsMap();
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            map.add(entry.getKey(), entry.getValue());
        }

        if(logger.isDebugEnabled()){
            logger.debug("Charset Encoding is : {}", characterEncoding);
            logger.debug(" headers :" + map + " , Url " + url + " , body " + body);
        }

        return asyncHttpClient.preparePost(url).setBody(body).setBodyEncoding(characterEncoding).setHeaders(map).execute().get();
    }

    public Response executePost(String url, String body, Map<String, String> headers)
        throws Exception {

        return executePost(url, body, headers, null);
    }

    public Response executePut(String url, String body, Map<String, String> headers)
        throws Exception {

        final FluentCaseInsensitiveStringsMap map = new FluentCaseInsensitiveStringsMap();
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            map.add(entry.getKey(), entry.getValue());
        }

        if(logger.isDebugEnabled())
            logger.debug(" headers :" + map + " , Url " + url + " , body " + body);

        return asyncHttpClient.preparePut(url).setBody(body).setHeaders(map).execute().get();
    }

    public Response executeRequest(String method,String url, String body, Map<String, String> headers, String characterEncoding)
            throws Exception {

        final FluentCaseInsensitiveStringsMap map = new FluentCaseInsensitiveStringsMap();
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            map.add(entry.getKey(), entry.getValue());
        }

        if(logger.isDebugEnabled()){
            logger.debug("Charset Encoding is : {}", characterEncoding);
            logger.debug(" headers :" + map + " , Url " + url + " , body " + body);
        }

        Request request = new RequestBuilder(method).setUrl(url).build();
        return asyncHttpClient.prepareRequest(request).setBody(body).setBodyEncoding(characterEncoding).setHeaders(map).execute().get();
    }

    public Response executeRequest(String method,String url, String body, Map<String, String> headers)
            throws Exception {

        return executeRequest(method, url, body, headers, null);

    }

    public Response executeRequestWithForwardingHeader(String method,String url, String body, Map<String, String> headers)
            throws Exception{
        headers = headers != null ? headers : new HashMap<>();
        String forwardHeader = "X_TURBO_FORWARD_NUMBER";
        int requestForwardNo = Integer.valueOf(headers.getOrDefault(forwardHeader,String.valueOf(1)));
        if(requestForwardNo > 5){
            throw new RequestForwardingException("request forwarded too many times, aborting");
        }
        headers.put(forwardHeader,String.valueOf(++requestForwardNo));
        return executeRequest(method,url,body,headers);
    }

}

