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

import com.couchbase.client.core.config.ConfigurationProvider;
import com.couchbase.client.core.endpoint.ResponseStatusConverter;
import com.couchbase.client.core.endpoint.kv.KeyValueStatus;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.DefaultCoreEnvironment;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.kv.InsertRequest;
import com.couchbase.client.core.message.kv.InsertResponse;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import org.junit.Test;
import rx.subjects.Subject;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Verifies functionality of the {@link ResponseHandler}.
 *
 * @author Michael Nitschinger
 * @since 1.0.3
 */
public class ResponseHandlerTest {

    private static final CoreEnvironment ENVIRONMENT = DefaultCoreEnvironment.create();

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

        verify(providerMock, times(1)).proposeBucketConfig("bucket", "{\"json\": true}");
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

        verify(providerMock, never()).proposeBucketConfig("bucket", "Not my Vbucket");
        assertEquals(0, config.refCnt());
        assertNull(retryEvent.getMessage());
        assertNull(retryEvent.getObservable());
    }

}
