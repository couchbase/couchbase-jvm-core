/**
 * Copyright (c) 2015 Couchbase, Inc.
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
package com.couchbase.client.core.config;

import com.couchbase.client.core.service.ServiceType;
import org.junit.Test;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Verifies the functionality of the {@link AbstractBucketConfig}.
 *
 * @author Michael Nitschinger
 * @since 1.2.0
 */
public class AbstractBucketConfigTest {

    private static final String NAME = "name";
    private static final BucketNodeLocator LOCATOR = BucketNodeLocator.VBUCKET;
    private static final String URI = "http://foobar:8091/foo";
    private static final String STREAMING_URI = "http://foobar:8091/foo";

    @Test
    public void shouldCheckIfServiceIsEnabled() throws Exception {
        List<NodeInfo> nodeInfos = new ArrayList<NodeInfo>();

        Map<ServiceType, Integer> direct = new HashMap<ServiceType, Integer>();
        Map<ServiceType, Integer> ssl = new HashMap<ServiceType, Integer>();

        direct.put(ServiceType.BINARY, 1234);
        direct.put(ServiceType.CONFIG, 1235);
        ssl.put(ServiceType.BINARY, 4567);

        nodeInfos.add(new DefaultNodeInfo(InetAddress.getByName("127.0.0.1"), direct, ssl));

        BucketConfig bc = new SampleBucketConfig(nodeInfos, null);

        assertTrue(bc.serviceEnabled(ServiceType.BINARY));
        assertTrue(bc.serviceEnabled(ServiceType.CONFIG));
        assertFalse(bc.serviceEnabled(ServiceType.QUERY));
        assertFalse(bc.serviceEnabled(ServiceType.VIEW));
    }

    static class SampleBucketConfig extends AbstractBucketConfig {

        public SampleBucketConfig(List<NodeInfo> nodeInfos, List<PortInfo> portInfos) {
            super(NAME, LOCATOR, URI, STREAMING_URI, nodeInfos, portInfos);
        }

        @Override
        public boolean tainted() {
            return false;
        }

        @Override
        public long rev() {
            return 0;
        }

        @Override
        public BucketType type() {
            return null;
        }


    }

}