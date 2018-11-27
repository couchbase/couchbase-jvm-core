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

package com.couchbase.client.core;

import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.config.ConfigurationProvider;
import com.couchbase.client.core.config.ProposedBucketConfigContext;
import com.couchbase.client.core.endpoint.ResponseStatusConverter;
import com.couchbase.client.core.endpoint.kv.KeyValueStatus;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.DefaultCoreEnvironment;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.kv.GetRequest;
import com.couchbase.client.core.message.kv.GetResponse;
import com.couchbase.client.core.message.kv.InsertRequest;
import com.couchbase.client.core.message.kv.InsertResponse;
import com.couchbase.client.core.util.TestProperties;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import rx.subjects.Subject;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies functionality of the {@link ResponseHandler}.
 *
 * @author Michael Nitschinger
 * @since 1.0.3
 */
public class ResponseHandlerTest {

    private static final CoreEnvironment ENVIRONMENT = DefaultCoreEnvironment.create();

    @Before
    public void check() {
        assumeFalse(TestProperties.isCi());
    }

    @Test
    public void shouldSendProposedConfigToProvider() throws Exception {
        ClusterFacade clusterMock = mock(ClusterFacade.class);
        ConfigurationProvider providerMock = mock(ConfigurationProvider.class);
        ResponseHandler handler = new ResponseHandler(ENVIRONMENT, clusterMock, providerMock);
        ByteBuf config = Unpooled.copiedBuffer("{\"json\": true}", CharsetUtil.UTF_8);

        ResponseEvent retryEvent = new ResponseEvent();
        retryEvent.setMessage(new InsertResponse(ResponseStatus.RETRY, KeyValueStatus.ERR_TEMP_FAIL.code(),
                0, "bucket", config, null, mock(InsertRequest.class)));
        retryEvent.setObservable(mock(Subject.class));
        handler.onEvent(retryEvent, 1, true);

        ProposedBucketConfigContext ctx = new ProposedBucketConfigContext("bucket", "{\"json\": true}", null);
        verify(providerMock, times(1)).proposeBucketConfig(ctx);
        assertEquals(0, config.refCnt());
        assertNull(retryEvent.getMessage());
        assertNull(retryEvent.getObservable());
    }

    @Test
    public void shouldIgnoreInvalidConfig() throws Exception {
        ClusterFacade clusterMock = mock(ClusterFacade.class);
        ConfigurationProvider providerMock = mock(ConfigurationProvider.class);
        ResponseHandler handler = new ResponseHandler(ENVIRONMENT, clusterMock, providerMock);
        ByteBuf config = Unpooled.copiedBuffer("Not my Vbucket", CharsetUtil.UTF_8);

        ResponseEvent retryEvent = new ResponseEvent();
        retryEvent.setMessage(new InsertResponse(ResponseStatus.RETRY, KeyValueStatus.ERR_TEMP_FAIL.code(),
                0, "bucket", config, null, mock(InsertRequest.class)));
        retryEvent.setObservable(mock(Subject.class));
        handler.onEvent(retryEvent, 1, true);

        ProposedBucketConfigContext ctx = new ProposedBucketConfigContext("bucket", "Not my Vbucket", null);
        verify(providerMock, never()).proposeBucketConfig(ctx);
        assertEquals(0, config.refCnt());
        assertNull(retryEvent.getMessage());
        assertNull(retryEvent.getObservable());
    }

    @Test
    public void shouldDispatchFirstNMVImmediately() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        ClusterFacade clusterMock = mock(ClusterFacade.class);
        when(clusterMock.send(any(CouchbaseRequest.class))).then(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                latch.countDown();
                return null;
            }
        });

        ConfigurationProvider providerMock = mock(ConfigurationProvider.class);
        ClusterConfig clusterConfig = mock(ClusterConfig.class);
        BucketConfig bucketConfig = mock(BucketConfig.class);
        when(providerMock.config()).thenReturn(clusterConfig);
        when(clusterConfig.bucketConfig("bucket")).thenReturn(bucketConfig);
        when(bucketConfig.hasFastForwardMap()).thenReturn(true);

        ResponseHandler handler = new ResponseHandler(ENVIRONMENT, clusterMock, providerMock);

        GetRequest request = new GetRequest("key", "bucket");
        GetResponse response = new GetResponse(ResponseStatus.RETRY, (short) 0, 0L ,0, "bucket", Unpooled.EMPTY_BUFFER,
                request);

        ResponseEvent retryEvent = new ResponseEvent();
        retryEvent.setMessage(response);
        retryEvent.setObservable(request.observable());

        handler.onEvent(retryEvent, 1, true);

        long start = System.nanoTime();
        latch.await(5, TimeUnit.SECONDS);
        long end = System.nanoTime();

        // assert immediate dispatch
        assertTrue(TimeUnit.NANOSECONDS.toMillis(end - start) < 10);
    }

    @Test
    public void shouldDispatchFirstNMVBWithDelayIfNoFFMap() throws Exception {

        final CountDownLatch latch = new CountDownLatch(1);
        ClusterFacade clusterMock = mock(ClusterFacade.class);
        when(clusterMock.send(any(CouchbaseRequest.class))).then(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                latch.countDown();
                return null;
            }
        });

        ConfigurationProvider providerMock = mock(ConfigurationProvider.class);
        ClusterConfig clusterConfig = mock(ClusterConfig.class);
        BucketConfig bucketConfig = mock(BucketConfig.class);
        when(providerMock.config()).thenReturn(clusterConfig);
        when(clusterConfig.bucketConfig("bucket")).thenReturn(bucketConfig);
        when(bucketConfig.hasFastForwardMap()).thenReturn(false);

        ResponseHandler handler = new ResponseHandler(ENVIRONMENT, clusterMock, providerMock);

        GetRequest request = new GetRequest("key", "bucket");
        GetResponse response = new GetResponse(ResponseStatus.RETRY, (short) 0, 0L ,0, "bucket", Unpooled.EMPTY_BUFFER,
                request);

        ResponseEvent retryEvent = new ResponseEvent();
        retryEvent.setMessage(response);
        retryEvent.setObservable(request.observable());

        handler.onEvent(retryEvent, 1, true);

        long start = System.nanoTime();
        latch.await(5, TimeUnit.SECONDS);
        long end = System.nanoTime();

        // assert delayed dispatch
        assertTrue(TimeUnit.NANOSECONDS.toMillis(end - start) >= 100);
    }

    @Test
    public void shouldDispatchSecondNMVWithDelay() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        ClusterFacade clusterMock = mock(ClusterFacade.class);
        when(clusterMock.send(any(CouchbaseRequest.class))).then(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                latch.countDown();
                return null;
            }
        });

        ConfigurationProvider providerMock = mock(ConfigurationProvider.class);
        ClusterConfig clusterConfig = mock(ClusterConfig.class);
        BucketConfig bucketConfig = mock(BucketConfig.class);
        when(providerMock.config()).thenReturn(clusterConfig);
        when(clusterConfig.bucketConfig("bucket")).thenReturn(bucketConfig);
        when(bucketConfig.hasFastForwardMap()).thenReturn(true);

        ResponseHandler handler = new ResponseHandler(ENVIRONMENT, clusterMock, providerMock);

        GetRequest request = new GetRequest("key", "bucket");
        request.incrementRetryCount(); // pretend its at least once retried!
        GetResponse response = new GetResponse(ResponseStatus.RETRY, (short) 0, 0L ,0, "bucket", Unpooled.EMPTY_BUFFER,
            request);

        ResponseEvent retryEvent = new ResponseEvent();
        retryEvent.setMessage(response);
        retryEvent.setObservable(request.observable());

        handler.onEvent(retryEvent, 1, true);

        long start = System.nanoTime();
        latch.await(5, TimeUnit.SECONDS);
        long end = System.nanoTime();

        // assert delayed dispatch
        assertTrue(TimeUnit.NANOSECONDS.toMillis(end - start) >= 100);
    }

}
