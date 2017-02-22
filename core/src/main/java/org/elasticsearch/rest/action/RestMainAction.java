/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.rest.action;

import org.elasticsearch.action.main.MainAction;
import org.elasticsearch.action.main.MainRequest;
import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.io.ReleasableBytesStream;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArray;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.TcpTransport;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.HEAD;

public class RestMainAction extends BaseRestHandler {
    public RestMainAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(GET, "/", this);
        controller.registerHandler(HEAD, "/", this);
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        return channel -> client.execute(MainAction.INSTANCE, new MainRequest(), new RestBuilderListener<MainResponse>(channel) {
            @Override
            public RestResponse buildResponse(MainResponse mainResponse, XContentBuilder builder) throws Exception {
                return convertMainResponse(mainResponse, request, builder);
            }
        });
    }

    static BytesRestResponse convertMainResponse(MainResponse response, RestRequest request, XContentBuilder builder) throws IOException {
        RestStatus status = response.isAvailable() ? RestStatus.OK : RestStatus.SERVICE_UNAVAILABLE;

        final Map<ReleasableBytesStream, TcpTransport.Thing> tcp;
        final Map<RestChannel, RestController.Thing> http;
        final Map<BigArray, BigArrays.Thing> arrays;
        synchronized (TcpTransport.lock) {
            synchronized (RestController.lock) {
                synchronized (BigArrays.lock) {
                    tcp = new HashMap<>(TcpTransport.RELEASABLES);
                    http = new HashMap<>(RestController.MAP);
                    arrays = new HashMap<>(BigArrays.MAP);
                }
            }
        }

        // Default to pretty printing, but allow ?pretty=false to disable
        if (request.hasParam("pretty") == false) {
            builder.prettyPrint().lfAtEnd();
        }
        builder.startObject();
        {
            builder.startObject("tcp_transport_releasables");
            {
                builder.startArray("releasables");
                {
                    for (Map.Entry<ReleasableBytesStream, TcpTransport.Thing> entry : tcp.entrySet()) {
                        builder.startObject();
                        {
                            builder.field("action", entry.getValue().getAction());
                            builder.field("bytes", entry.getValue().getBytes());
                            builder.field("request", entry.getValue().isRequest());
                        }
                        builder.endObject();
                    }
                }
                builder.endArray();
            }
            builder.endObject();

            builder.startObject("http_requests");
            {
                builder.startArray("requests");
                {
                    for (Map.Entry<RestChannel, RestController.Thing> entry : http.entrySet()) {
                        builder.startObject();
                        {
                            builder.field("uri", entry.getValue().getUri());
                            builder.field("length", entry.getValue().getLength());
                        }
                        builder.endObject();
                    }
                }
                builder.endArray();
            }
            builder.endObject();

            builder.startObject("arrays");
            {
                builder.startArray("entries");
                {
                    for (Map.Entry<BigArray, BigArrays.Thing> entry : arrays.entrySet()) {
                        builder.startObject();
                        {
                            builder.field("bytes", entry.getValue().getBytes());
                            builder.array("stack_trace", Arrays.stream(entry.getValue().getStackTraceElements()).map(StackTraceElement::toString).collect(Collectors.toList()).toArray(new String[0]));
                        }
                        builder.endObject();
                    }
                }
                builder.endArray();
            }
            builder.endObject();
        }
        builder.endObject();

        return new BytesRestResponse(status, builder);
    }
}
