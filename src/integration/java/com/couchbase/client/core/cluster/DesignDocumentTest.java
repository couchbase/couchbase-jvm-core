package com.couchbase.client.core.cluster;

import com.couchbase.client.core.message.config.GetDesignDocumentsRequest;
import com.couchbase.client.core.message.config.GetDesignDocumentsResponse;
import com.couchbase.client.core.message.view.GetDesignDocumentRequest;
import com.couchbase.client.core.message.view.GetDesignDocumentResponse;
import com.couchbase.client.core.util.ClusterDependentTest;
import io.netty.util.CharsetUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class DesignDocumentTest extends ClusterDependentTest {

    @Test
    public void shouldGetDesignDocument() {
        GetDesignDocumentRequest req = new GetDesignDocumentRequest("beer", true, bucket(), password());
        GetDesignDocumentResponse response = cluster().<GetDesignDocumentResponse>send(req).toBlocking().single();

        assertEquals("beer", response.name());
        assertEquals(true, response.development());
        assertTrue(response.status().isSuccess());
        assertNotNull(response.content().toString(CharsetUtil.UTF_8));
    }

    @Test
    public void shouldListDesignDocuments() {
        GetDesignDocumentsRequest req = new GetDesignDocumentsRequest(bucket(), password());
        GetDesignDocumentsResponse response = cluster().<GetDesignDocumentsResponse>send(req).toBlocking().single();

        assertTrue(response.status().isSuccess());
        assertNotNull(response.content());
    }
}
