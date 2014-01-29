package com.couchbase.client.java.bucket;

import com.couchbase.client.java.bucket.Bucket;

public interface BucketCallback {

    void execute(Bucket bucket);

}
