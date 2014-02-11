package com.couchbase.client.java.bucket;

/**
 * A wrapper containing both the actual document and meta information associated.
 */
public interface Document {

    String getId();

    Object getContent();

    long getCas();

    int getExpiration();

    String getGroupId();

    Document setId(String id);

    Document setContent(Object content);

    Document setCas(long cas);

    Document setExpiration(int expiration);

    Document setGroupId(String groupId);

}
