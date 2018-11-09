/*
 * Copyright (c) 2016 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.message.stat;

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.config.NodeInfo;
import com.couchbase.client.core.message.cluster.GetClusterConfigRequest;
import com.couchbase.client.core.message.cluster.GetClusterConfigResponse;
import com.couchbase.client.core.message.kv.StatRequest;
import com.couchbase.client.core.message.kv.StatResponse;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.utils.NetworkAddress;
import rx.Observable;
import rx.functions.Func0;
import rx.functions.Func1;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class to handle STAT calls logic.
 *
 * <pre>
 * {@code
 *  Stat.call(core, bucket.name(), "")
 *      .toBlocking()
 *      .forEach(r -> System.out.printf("%s %s %s\n", r.hostname(), r.key(), r.value()));
 * }
 * </pre>
 *
 * @author Sergey Avseyev
 * @since 1.2.1
 */
public class Stat {
    public static Observable<StatResponse> call(final ClusterFacade core, final String bucket, final String key) {
        return sendStatRequests(core, bucket, key);
    }

    private static Observable<StatResponse> sendStatRequests(final ClusterFacade core, final String bucket, final String key) {
        return Observable.defer(new Func0<Observable<StatResponse>>() {
            @Override
            public Observable<StatResponse> call() {
                return core
                        .<GetClusterConfigResponse>send(new GetClusterConfigRequest())
                        .map(new Func1<GetClusterConfigResponse, List<NetworkAddress>>() {
                            @Override
                            public List<NetworkAddress> call(GetClusterConfigResponse response) {
                                CouchbaseBucketConfig conf =
                                        (CouchbaseBucketConfig) response.config().bucketConfig(bucket);
                                List<NetworkAddress> hostnames = new ArrayList<NetworkAddress>();
                                for (NodeInfo nodeInfo : conf.nodes()) {
                                    if (nodeInfo.services().containsKey(ServiceType.BINARY)) {
                                        hostnames.add(nodeInfo.hostname());
                                    }
                                }
                                return hostnames;
                            }
                        })
                        .flatMap(new Func1<List<NetworkAddress>, Observable<StatResponse>>() {
                            @Override
                            public Observable<StatResponse> call(List<NetworkAddress> hostnames) {
                                List<Observable<StatResponse>> stats = new ArrayList<Observable<StatResponse>>();
                                for (NetworkAddress hostname : hostnames) {
                                    stats.add(core.<StatResponse>send(new StatRequest(key, hostname, bucket)));
                                }
                                if (stats.size() == 1) {
                                    return stats.get(0);
                                } else {
                                    return Observable.mergeDelayError(Observable.from(stats));
                                }
                            }
                        });
            }
        });
    }
}