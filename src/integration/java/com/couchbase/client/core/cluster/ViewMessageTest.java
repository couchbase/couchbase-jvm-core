/**
 * Copyright (C) 2014 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */
package com.couchbase.client.core.cluster;

import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.view.ViewQueryRequest;
import com.couchbase.client.core.message.view.ViewQueryResponse;
import com.couchbase.client.core.util.ClusterDependentTest;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
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
