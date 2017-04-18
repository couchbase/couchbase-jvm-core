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
package com.couchbase.client.core.cluster;

import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.view.ViewQueryRequest;
import com.couchbase.client.core.message.view.ViewQueryResponse;
import com.couchbase.client.core.util.ClusterDependentTest;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import rx.functions.Action1;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Verifies basic functionality of view operations.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class ViewMessageTest extends ClusterDependentTest {

    @BeforeClass
    public static void setup() throws Exception {
        connect(false);
    }

    @Test
    public void shouldQueryNonExistentView() {
        ViewQueryResponse single = cluster()
            .<ViewQueryResponse>send(new ViewQueryRequest("designdoc", "foobar", false, "debug=true", null, bucket(), password()))
            .toBlocking()
            .single();
        assertEquals(ResponseStatus.NOT_EXISTS, single.status());
        String error = single.error().toBlocking().singleOrDefault(null);
        assertNotNull(error);

        single.rows().toBlocking().forEach(new Action1<ByteBuf>() {
            @Override
            public void call(ByteBuf byteBuf) {
                ReferenceCountUtil.releaseLater(byteBuf);
            }
        });
    }

}
