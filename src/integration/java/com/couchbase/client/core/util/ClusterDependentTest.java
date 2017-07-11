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
package com.couchbase.client.core.util;

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.CouchbaseCore;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.DefaultCoreEnvironment;
import com.couchbase.client.core.message.cluster.DisconnectRequest;
import com.couchbase.client.core.message.cluster.GetClusterConfigRequest;
import com.couchbase.client.core.message.cluster.GetClusterConfigResponse;
import com.couchbase.client.core.message.cluster.OpenBucketRequest;
import com.couchbase.client.core.message.cluster.OpenBucketResponse;
import com.couchbase.client.core.message.cluster.SeedNodesRequest;
import com.couchbase.client.core.message.cluster.SeedNodesResponse;
import com.couchbase.client.core.message.config.ClusterConfigRequest;
import com.couchbase.client.core.message.config.ClusterConfigResponse;
import com.couchbase.client.core.message.config.FlushRequest;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.util.ResourceLeakDetector;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import com.couchbase.mock.CouchbaseMock;
import com.couchbase.mock.JsonUtils;
import org.junit.AfterClass;
import org.junit.Assume;
import rx.Observable;
import rx.functions.Func1;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * Base test class for tests that need a working cluster reference.
 *
 * @author Michael Nitschinger
 */
public class ClusterDependentTest {

    static {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
        System.setProperty("com.couchbase.xerrorEnabled", "true");
    }

    private static final String seedNode = TestProperties.seedNode();
    private static final String bucket = TestProperties.bucket();
    private static volatile String username = TestProperties.username();
    private static volatile String password = TestProperties.password();
    private static final String adminUser = TestProperties.adminUser();
    private static final String adminPassword = TestProperties.adminPassword();
    private static final CouchbaseMock couchbaseMock = TestProperties.couchbaseMock();
    private static CoreEnvironment env;

    protected static final int KEEPALIVE_INTERVAL = 1000;

    private static ClusterFacade cluster;

    protected static String sendGetHttpRequestToMock(String path, Map<String, String> parameters) throws Exception {
        URIBuilder builder = new URIBuilder();
        builder.setScheme("http").setHost("localhost").setPort(mock().getHttpPort()).setPath(path);
        for (Map.Entry<String, String> entry: parameters.entrySet()) {
            builder.setParameter(entry.getKey(), entry.getValue());
        }
        HttpGet request = new HttpGet(builder.build());
        HttpClient client = HttpClientBuilder.create().build();
        HttpResponse response = client.execute(request);
        int status = response.getStatusLine().getStatusCode();
        if (status != 200) {
            throw new ClientProtocolException("Unexpected response status: " + status);
        }
        return EntityUtils.toString(response.getEntity());
    }

    private static int getCarrierPortInfo() throws Exception {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("idx", "0");
        parameters.put("bucket", bucket());
        String rawBody = sendGetHttpRequestToMock("mock/get_mcports", parameters);
        com.google.gson.JsonObject respObject = JsonUtils.GSON.fromJson(rawBody, com.google.gson.JsonObject.class);
        com.google.gson.JsonArray portsArray = respObject.getAsJsonArray("payload");
        return portsArray.get(0).getAsInt();
    }

    /**
     * Helper (hacked together) method to grab a config from a bucket without having to initialize the
     * client first - this helps with pre-bootstrap decisions like credentials for RBAC.
     */
    private static int[] minClusterVersion() throws Exception {
        URIBuilder builder = new URIBuilder();
        builder.setScheme("http").setHost(seedNode).setPort(8091).setPath("/pools/default/buckets/" + bucket)
            .setParameter("bucket", bucket);
        HttpGet request = new HttpGet(builder.build());
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(new AuthScope(seedNode, 8091), new UsernamePasswordCredentials(adminUser, adminPassword));
        HttpClient client = HttpClientBuilder.create().setDefaultCredentialsProvider(credentialsProvider).build();
        HttpResponse response = client.execute(request);
        int status = response.getStatusLine().getStatusCode();
        if (status < 200 || status > 300) {
            throw new ClientProtocolException("Unexpected response status: " + status);
        }
        String rawConfig = EntityUtils.toString(response.getEntity());
        return minNodeVersionFromConfig(rawConfig);
    }


    public static void connect(boolean useMock) throws Exception {

        /*
         * If we are running under RBAC, set the user and password to the admin
         * credentials which will always work. This hopefully makes the test suite
         * forwards and backwards compat.
         *
         * Also, the mock currently doesn't support RBAC so ignore it if set.
         */
        if (minClusterVersion()[0] >= 5 && !useMock) {
            username = adminUser;
            password = adminPassword;
        }

        DefaultCoreEnvironment.Builder envBuilder = DefaultCoreEnvironment
                .builder();

        if (useMock) {
            int httpBootstrapPort = couchbaseMock.getHttpPort();
            try {
                int carrierBootstrapPort = getCarrierPortInfo();
                envBuilder
                        .bootstrapHttpDirectPort(httpBootstrapPort)
                        .bootstrapCarrierDirectPort(carrierBootstrapPort)
                        .socketConnectTimeout(30000);
            } catch (Exception ex) {
                throw new RuntimeException("Unable to get port info" + ex.getMessage(), ex);
            }

        }
        env = envBuilder.dcpEnabled(true)
                .dcpConnectionBufferSize(1024)          // 1 kilobyte
                .dcpConnectionBufferAckThreshold(0.5)   // should trigger BUFFER_ACK after 512 bytes
                .mutationTokensEnabled(true)
                .keepAliveInterval(KEEPALIVE_INTERVAL)
                .build();

        cluster = new CouchbaseCore(env);
        cluster.<SeedNodesResponse>send(new SeedNodesRequest(seedNode)).flatMap(
                new Func1<SeedNodesResponse, Observable<OpenBucketResponse>>() {
                    @Override
                    public Observable<OpenBucketResponse> call(SeedNodesResponse response) {
                        return cluster.send(new OpenBucketRequest(bucket, username, password));
                    }
                }
        ).toBlocking().single();
        cluster.send(new FlushRequest(bucket, username, password)).toBlocking().single();
    }

    @AfterClass
    public static void disconnect() throws InterruptedException {
        cluster.send(new DisconnectRequest()).toBlocking().first();
    }

    public static String password() {
        return password;
    }

    public static ClusterFacade cluster() {
        return cluster;
    }

    public static String bucket() {
        return bucket;
    }

    public static String username() {
        return username;
    }

    public static CoreEnvironment env() {
        return env;
    }

    public static CouchbaseMock mock() { return couchbaseMock; }

    /**
     * Checks based on the cluster node versions if DCP is available.
     *
     * @return true if all nodes in the cluster are version 3 or later.
     */
    public static boolean isDCPEnabled() throws Exception {
        return minNodeVersion()[0] >= 3;
    }

    public static boolean isMutationMetadataEnabled() throws Exception {
        return minNodeVersion()[0] >= 4;
    }

    /**
     * Perform an {@link Assume assumption} in order to ignore a surrounding test if the cluster's lowest
     * Couchbase version is under the provided major+minor.
     */
    public static void assumeMinimumVersionCompatible(int major, int minor) throws Exception {
        int[] version = minNodeVersion();
        Assume.assumeTrue("Detected Couchbase " + version[0] + "." + version[1] + ", needed " + major + "." + minor,
               version[0] > major || (version[0] == major && version[1] >= minor));
    }

    /**
     * @return the major.minor minimum version in the cluster, as an int[2].
     * @throws Exception
     */
    public static int[] minNodeVersion() throws Exception {
        ClusterConfigResponse response = cluster()
                .<ClusterConfigResponse>send(new ClusterConfigRequest(adminUser, adminPassword))
                .toBlocking()
                .single();
        return minNodeVersionFromConfig(response.config());
    }

    /**
     * @return the major.minor minimum version in the cluster, as an int[2].
     */
    private static int[] minNodeVersionFromConfig(String rawConfig) throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        JavaType type = mapper.getTypeFactory().constructMapType(Map.class, String.class, Object.class);
        Map<String, Object> result = mapper.readValue(rawConfig, type);

        List<Object> nodes = (List<Object>) result.get("nodes");
        int[] min = { 99, 99, 99 };
        for (Object n : nodes) {
            Map<String, Object> node = (Map<String, Object>) n;
            String stringVersion = (String) node.get("version");
            int[] version = extractVersion(stringVersion);

            if (version[0] < min[0]
                    || (version[0] == min[0] && version[1] < min[1])) {
                min = version;
            }
        }
        return min;
    }

    protected static int[] extractVersion(String stringVersion) {
        String[] splitVersion = stringVersion.split("[^\\d]+");
        int[] version = new int[2];

        version[0] = Integer.parseInt(splitVersion[0]); //major
        version[1] = splitVersion.length < 2 ? 0 : Integer.parseInt(splitVersion[1]); //minor
        return version;
    }


    protected int numberOfPartitions() {
        GetClusterConfigResponse res = cluster().<GetClusterConfigResponse>send(new GetClusterConfigRequest()).toBlocking().single();
        CouchbaseBucketConfig config = (CouchbaseBucketConfig) res.config().bucketConfig(bucket());
        return config.numberOfPartitions();
    }

    protected short calculateVBucketForKey(String key) {
        CRC32 crc32 = new CRC32();
        try {
            crc32.update(key.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        long rv = (crc32.getValue() >> 16) & 0x7fff;
        return (short) ((int) rv & numberOfPartitions() - 1);
    }
}