package com.couchbase.client.core.config;

import com.fasterxml.jackson.annotation.JsonCreator;

public enum BucketNodeLocator {

    VBUCKET,
    KETAMA;

    @JsonCreator
    public static BucketNodeLocator fromConfig(String text) {
        if (text == null) {
            return null;
        }
        return valueOf(text.toUpperCase());
    }

}
