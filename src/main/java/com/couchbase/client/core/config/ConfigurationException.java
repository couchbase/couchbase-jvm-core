package com.couchbase.client.core.config;

import com.couchbase.client.core.CouchbaseException;

/**
 * Created by michael on 26/05/14.
 */
public class ConfigurationException extends CouchbaseException {

    public ConfigurationException() {
    }

    public ConfigurationException(String message) {
        super(message);
    }

    public ConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }

    public ConfigurationException(Throwable cause) {
        super(cause);
    }
}
